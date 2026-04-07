#!/usr/bin/env python3
"""Build a promotion gate folder with tier-policy verification_status (no synthetic placeholder).

Reads a gate ``manual_review_queue.csv`` where fill-candidates were marked
``SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF``, and rewrites them per
``docs/domain_mapping_rules.md`` (fill-candidate triage by ``qa_tier``):

  - standard          -> auto_accepted_standard
  - informational     -> auto_accepted_informational
  - critical          -> auto_accepted_critical_sample_ok

**Guardrails:** Aborts if any synthetic row is not ``existing_missing_fill_candidate``,
if ``source_domain`` is unknown to the registry, or if any tier is ``debug``.

Copies ``promotion_scorecard.csv``, ``schema_validation.csv``, and
``concordance_summary.csv`` from ``--source-gate-dir``.

Writes ``promotion_review_decisions.csv`` (append-only payload for ``126``) with
non-empty ``evidence_ref`` pointing at the generated memo.

Usage:
  .venv/bin/python scripts/128_mrq_tier_policy_gate_build.py \\
      --source-gate-dir studies/20260409_final_master_release/mrq_hydrate_gate \\
      --dest-gate-dir studies/20260407_tier_policy_review_gate \\
      --release-batch-id 20260407_tier_policy
"""

from __future__ import annotations

import argparse
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

SYNTHETIC = "SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF"
FILL_ONLY = "existing_missing_fill_candidate"

STATUS_BY_TIER = {
    "standard": "auto_accepted_standard",
    "informational": "auto_accepted_informational",
    "critical": "auto_accepted_critical_sample_ok",
}

import sys

sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source-gate-dir", type=Path, required=True)
    p.add_argument("--dest-gate-dir", type=Path, required=True)
    p.add_argument(
        "--release-batch-id",
        default="20260407_tier_policy",
        help="Batch id embedded in memo / decisions (126 also stamps decision_batch_id).",
    )
    p.add_argument(
        "--reviewer",
        default="tier_policy_governance_20260407",
        help="Recorded reviewer id for bulk tier acceptance.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    src = args.source_gate_dir.resolve()
    dest = args.dest_gate_dir.resolve()
    if not src.is_dir():
        raise SystemExit(f"Source gate not found: {src}")

    reg = load_registry()
    dest.mkdir(parents=True, exist_ok=True)

    for name in ("promotion_scorecard.csv", "schema_validation.csv", "concordance_summary.csv"):
        p = src / name
        if p.is_file():
            shutil.copy2(p, dest / name)

    mrq_in = src / "manual_review_queue.csv"
    if not mrq_in.is_file():
        raise SystemExit(f"Missing {mrq_in}")

    df = pd.read_csv(mrq_in)
    if "algorithm_comparison_status" not in df.columns or "verification_status" not in df.columns:
        raise SystemExit("manual_review_queue.csv missing required columns")

    for col in ("reviewer_id", "reviewer_decision_at", "reviewer_comment", "promotion_approved"):
        if col in df.columns:
            df[col] = df[col].astype("object")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tier_counts: Counter[str] = Counter()

    mask_syn = df["verification_status"].astype(str).str.strip() == SYNTHETIC
    if mask_syn.any():
        sub = df.loc[mask_syn]
        bad_alg = sub["algorithm_comparison_status"].astype(str).str.strip() != FILL_ONLY
        if bad_alg.any():
            raise SystemExit(
                f"Refusing tier policy rewrite: {int(bad_alg.sum())} synthetic row(s) are not {FILL_ONLY!r}."
            )
        for idx in sub.index:
            dom = str(df.at[idx, "source_domain"]).strip()
            spec = reg.domains.get(dom)
            if spec is None:
                raise SystemExit(f"Unknown domain {dom!r} in MRQ row index {idx}")
            tier = spec.qa_tier
            if tier == "debug":
                raise SystemExit(f"Domain {dom!r} has qa_tier=debug; cannot auto tier-policy.")
            new_status = STATUS_BY_TIER.get(tier)
            if new_status is None:
                raise SystemExit(f"No status mapping for qa_tier={tier!r} (domain {dom!r}).")
            df.at[idx, "verification_status"] = new_status
            df.at[idx, "promotion_approved"] = "true"
            df.at[idx, "reviewer_id"] = args.reviewer
            df.at[idx, "reviewer_decision_at"] = now
            df.at[idx, "reviewer_comment"] = (
                f"Tier bulk fill-candidate acceptance batch={args.release_batch_id}; "
                f"qa_tier={tier}; policy=docs/domain_mapping_rules.md#fill-candidate-triage-policy"
            )
            tier_counts[tier] += 1

    mrq_out = dest / "manual_review_queue.csv"
    df.to_csv(mrq_out, index=False)

    memo_path = dest / "TIER_POLICY_BATCH_MEMO.md"
    memo = "\n".join(
        [
            "# Tier-policy batch — fill-candidate MRQ",
            "",
            f"**Batch id:** `{args.release_batch_id}`",
            f"**Generated (UTC):** {now}",
            "",
            "## Scope",
            "",
            f"- Rewrote **{int(mask_syn.sum()):,}** MRQ rows from `{SYNTHETIC}` to registry tier "
            "`verification_status` values.",
            "- **Algorithm gate:** only rows with `algorithm_comparison_status = existing_missing_fill_candidate` "
            "(enrichment / fill). **No** synthetic discordant rows were present in source.",
            "",
            "## Counts by `qa_tier` (registry)",
            "",
        ]
        + [f"- **{t}:** {n:,} row(s) → `{STATUS_BY_TIER[t]}`" for t, n in sorted(tier_counts.items())]
        + [
            "",
            "## Governance",
            "",
            "- Authoritative policy: `docs/domain_mapping_rules.md` (Fill-Candidate Triage Policy).",
            "- Critical-tier rows here are **fill-candidates only** (not value discordance). "
            "Organization batch acceptance is recorded with `auto_accepted_critical_sample_ok` and this memo "
            "as `evidence_ref` on `qa.promotion_review_decisions`.",
            "",
        ]
    )
    memo_path.write_text(memo, encoding="utf-8")

    ev_ref = str(memo_path.relative_to(ROOT))

    decisions = pd.DataFrame(
        [
            {
                "review_id": None,
                "run_label": dest.name,
                "llm_entity_id": None,
                "research_id": None,
                "domain": "_tier_batch_summary",
                "entity_type": None,
                "algorithm_status": FILL_ONLY,
                "verification_status": "policy_batch_adjudication",
                "reviewer": args.reviewer,
                "reviewed_at": now,
                "waiver_reason": (
                    f"tier_policy batch {args.release_batch_id}; counts={dict(tier_counts)}; memo={ev_ref}"
                ),
                "source_object_id": None,
                "evidence_ref": ev_ref,
            }
        ]
    )
    decisions.to_csv(dest / "promotion_review_decisions.csv", index=False)

    print(f"  [128] Wrote gate: {dest}")
    print(f"  [128] MRQ rows rewritten: {int(mask_syn.sum()):,}")
    print(f"  [128] Tier counts: {dict(tier_counts)}")
    print(f"  [128] Memo: {memo_path}")


if __name__ == "__main__":
    main()
