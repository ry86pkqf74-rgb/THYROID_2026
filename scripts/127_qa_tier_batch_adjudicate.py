#!/usr/bin/env python3
"""127_qa_tier_batch_adjudicate.py — Tiered bulk acceptance for fill-candidates (MotherDuck).

Implements docs/domain_mapping_rules.md: standard + informational fill-candidates may be
bulk-accepted with auto_accepted_* statuses. Discordant rows are never updated.

Critical-tier fill-candidates are NOT bulk-updated unless --apply --include-critical-after-sample
(a governance flag; run critical sample review first per policy).

Default is --dry-run (prints SQL and counts only).

Usage:
  .venv/bin/python scripts/127_qa_tier_batch_adjudicate.py --md --md-sa
  .venv/bin/python scripts/127_qa_tier_batch_adjudicate.py --md --md-sa --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402

DEFAULT_DB = ROOT / "thyroid_master.duckdb"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", required=True)
    p.add_argument("--md-sa", action="store_true")
    p.add_argument("--db-path", default=str(DEFAULT_DB))
    p.add_argument("--run-label", default="formalization_20260406_v3")
    p.add_argument("--apply", action="store_true", help="Execute updates (default is print-SQL only).")
    p.add_argument(
        "--include-critical-after-sample",
        action="store_true",
        help="Also bulk-accept critical-tier fill-candidates (only after sample policy sign-off).",
    )
    p.add_argument("--reviewer", default="tier_policy_script_127")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dry = not args.apply

    reg = load_registry()
    standard_domains = {k for k, v in reg.v2_domains.items() if v.qa_tier == "standard" and v.canonical_output}
    info_domains = {k for k, v in reg.v2_domains.items() if v.qa_tier == "informational" and v.canonical_output}
    critical_domains = {k for k, v in reg.v2_domains.items() if v.qa_tier == "critical" and v.canonical_output}

    def qlist(domains: set[str]) -> str:
        return ", ".join(f"'{d}'" for d in sorted(domains))

    con = connect_md_or_file(
        Path(args.db_path),
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
        custom_user_agent="THYROID_2026_qa_tier_batch/1.0",
        motherduck_session_hint="rc_tier_batch_adjudicate",
    )
    try:
        # Pending discordant (must remain for manual)
        disc = con.execute(
            f"""
            SELECT COUNT(*) FROM qa.manual_review_queue
            WHERE run_label = '{args.run_label}'
              AND verification_status IS NULL
              AND algorithm_status = 'discordant_existing'
            """
        ).fetchone()[0]
        pend_fill = con.execute(
            f"""
            SELECT COUNT(*) FROM qa.manual_review_queue
            WHERE run_label = '{args.run_label}'
              AND verification_status IS NULL
              AND algorithm_status = 'existing_missing_fill_candidate'
            """
        ).fetchone()[0]
        print(f"  Pending discordant_existing: {disc}")
        print(f"  Pending fill candidates: {pend_fill}")

        batch_id = f"rc_tier_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        now_sql = "CURRENT_TIMESTAMP"

        std_sql = f"""
UPDATE qa.manual_review_queue
SET verification_status = 'auto_accepted_standard',
    promotion_approved = 'true',
    reviewer = '{args.reviewer}',
    reviewed_at = {now_sql},
    reviewer_comment = 'Bulk acceptance per docs/domain_mapping_rules.md (standard tier, fill-candidate). batch={batch_id}'
WHERE run_label = '{args.run_label}'
  AND verification_status IS NULL
  AND algorithm_status = 'existing_missing_fill_candidate'
  AND domain IN ({qlist(standard_domains)})
"""
        info_sql = f"""
UPDATE qa.manual_review_queue
SET verification_status = 'auto_accepted_informational',
    promotion_approved = 'true',
    reviewer = '{args.reviewer}',
    reviewed_at = {now_sql},
    reviewer_comment = 'Bulk acceptance per docs/domain_mapping_rules.md (informational tier). batch={batch_id}'
WHERE run_label = '{args.run_label}'
  AND verification_status IS NULL
  AND algorithm_status = 'existing_missing_fill_candidate'
  AND domain IN ({qlist(info_domains)})
"""
        crit_sql = f"""
UPDATE qa.manual_review_queue
SET verification_status = 'auto_accepted_critical_sample_ok',
    promotion_approved = 'true',
    reviewer = '{args.reviewer}',
    reviewed_at = {now_sql},
    reviewer_comment = 'CRITICAL tier bulk acceptance ONLY after sample policy; batch={batch_id}'
WHERE run_label = '{args.run_label}'
  AND verification_status IS NULL
  AND algorithm_status = 'existing_missing_fill_candidate'
  AND domain IN ({qlist(critical_domains)})
"""

        decisions_sql = f"""
INSERT INTO qa.promotion_review_decisions (
    review_id, run_label, research_id, domain, entity_type, algorithm_status,
    verification_status, reviewer, waiver_reason, decision_batch_id
)
VALUES (
    NULL, '{args.run_label}', NULL, '_tier_batch_summary', NULL, 'existing_missing_fill_candidate',
    'policy_batch', '{args.reviewer}', 'domain_mapping_rules.md tier bulk (127)', '{batch_id}'
)
"""

        print("\n-- Standard tier bulk update:\n", std_sql)
        print("\n-- Informational tier bulk update:\n", info_sql)
        if args.include_critical_after_sample:
            print("\n-- Critical tier bulk (requires governance sign-off):\n", crit_sql)
        print("\n-- Decision audit row:\n", decisions_sql)

        if dry:
            print("\n  [dry-run] no changes applied.")
            return

        con.execute(std_sql)
        con.execute(info_sql)
        if args.include_critical_after_sample:
            con.execute(crit_sql)
        try:
            con.execute(decisions_sql)
        except Exception as e:
            print(f"  [warn] promotion_review_decisions insert: {e}")

        left = con.execute(
            f"""
            SELECT COUNT(*) FROM qa.manual_review_queue
            WHERE run_label = '{args.run_label}' AND verification_status IS NULL
            """
        ).fetchone()[0]
        print(f"\n  [apply] remaining NULL verification_status for run: {left}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
