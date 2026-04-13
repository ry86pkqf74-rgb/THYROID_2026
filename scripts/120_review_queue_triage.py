#!/usr/bin/env python3
"""Export read-only triage bundle for qa.manual_review_queue (MotherDuck or local).

Produces timestamped artifacts under exports/review_queue_triage_<UTC_YYYYMMDDHHMMSS>/
(or ``--output-root``/study folder) for burning down pending manual review without
touching promotion logic.

Read-only: SELECT only against qa.manual_review_queue.
Does not export raw clinical note text: review_reason is omitted; other text fields
are truncated.

Adds ``counts_manuscript_quality_tiers.csv`` to separate structural queue completion
(non-NULL ``verification_status``) from manuscript sign-off posture (synthetic
placeholders, automation-only tier policy, human reviewer identity present).

Usage:
  .venv/bin/python scripts/120_review_queue_triage.py --md
  .venv/bin/python scripts/120_review_queue_triage.py --read-scaling --output-root exports
  .venv/bin/python scripts/120_review_queue_triage.py --db-path thyroid_master.duckdb
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry  # noqa: E402
from utils.md_connect import connect_md_or_file, connect_read_scaling_fail_closed  # noqa: E402
from utils.publication_governance import mrq_synthetic_placeholder_where_sql  # noqa: E402

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORTS = ROOT / "exports"

# Read-scaling defaults (override via MOTHERDUCK_CUSTOM_USER_AGENT / MOTHERDUCK_SESSION_HINT / --session-hint).
DEFAULT_READ_SCALING_UA = "THYROID_2026_review_queue_triage_rs/1.0"
DEFAULT_READ_SCALING_SESSION_HINT = "THYROID_2026_review_queue_triage_rs_v1"

# PHI-adjacent / note-derived: omit from worklists entirely.
EXCLUDED_WORKLIST_COLS = frozenset({"review_reason"})

# Max lengths for any retained free-text columns.
TRUNC_ENTITY = 64
TRUNC_COMMENT = 96
TRUNC_EVIDENCE = 72

WORKLIST_SELECT_COLS = """
    review_row_id,
    run_label,
    research_id,
    domain,
    entity_type,
    entity_value_norm,
    algorithm_status,
    verification_status,
    promotion_approved,
    reviewer,
    reviewed_at,
    loaded_at,
    reason_code,
    reviewer_comment,
    reviewer_evidence_span
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Query MotherDuck (fail-closed; requires RW token).")
    p.add_argument(
        "--read-scaling",
        action="store_true",
        help="MotherDuck read-scaling token only (MD_READ_SCALING_TOKEN); least-privilege after REFRESH DATABASE.",
    )
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN (with --md only).")
    p.add_argument(
        "--md-env",
        default=None,
        choices=["dev", "qa", "prod"],
        help="MotherDuck catalog for --read-scaling (default: MOTHERDUCK_ENV or prod).",
    )
    p.add_argument(
        "--session-hint",
        default=None,
        help="Override MOTHERDUCK_SESSION_HINT / MD_READ_SCALING_SESSION_HINT for MotherDuck query attribution.",
    )
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Local DuckDB path (when not using --md or --read-scaling).")
    p.add_argument(
        "--output-root",
        default=str(EXPORTS),
        help="Parent directory for review_queue_triage_* folders (default: exports/).",
    )
    p.add_argument("--oldest-limit", type=int, default=200, help="Max rows in oldest_pending.csv.")
    p.add_argument(
        "--run-label",
        default=None,
        help="Optional filter: only rows with this qa.manual_review_queue.run_label.",
    )
    return p.parse_args()


def _resolved_session_hint(cli: str | None, *, read_scaling: bool) -> str | None:
    import os

    if cli and str(cli).strip():
        return str(cli).strip()
    env = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip()
    if env:
        return env
    if read_scaling:
        return DEFAULT_READ_SCALING_SESSION_HINT
    return None


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    import os

    if args.md:
        ua = os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "THYROID_2026_review_queue_triage/1.0"
        hint = _resolved_session_hint(args.session_hint, read_scaling=False)
        return connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=args.md_sa,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
        )
    if args.read_scaling:
        ua = os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or DEFAULT_READ_SCALING_UA
        cli = args.session_hint.strip() if args.session_hint and str(args.session_hint).strip() else None
        hint = _resolved_session_hint(cli, read_scaling=True)
        return connect_read_scaling_fail_closed(
            md_env=args.md_env,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
        )
    return duckdb.connect(str(args.db_path))


def _domain_slug(name: str) -> str:
    s = re.sub(r"[^\w\-.]+", "_", name.strip(), flags=re.UNICODE)
    return s.strip("_") or "unknown"


def _qa_tier_for_domain(reg, domain: str | None) -> str:
    if not domain:
        return "unknown"
    spec = reg.domains.get(domain)
    if spec is None:
        return "unknown"
    return spec.qa_tier


def truncate_field(val: object, max_len: int) -> str | None:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (ValueError, TypeError):
        pass
    t = str(val).strip()
    if not t or t.lower() == "nan":
        return None
    if len(t) <= max_len:
        return t
    return t[: max_len - 3] + "..."


def sanitize_worklist_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop excluded columns; truncate evidence-like fields."""
    out = df.drop(columns=[c for c in df.columns if c in EXCLUDED_WORKLIST_COLS], errors="ignore")
    if "entity_value_norm" in out.columns:
        out["entity_value_norm"] = out["entity_value_norm"].map(lambda x: truncate_field(x, TRUNC_ENTITY))
    if "reviewer_comment" in out.columns:
        out["reviewer_comment"] = out["reviewer_comment"].map(lambda x: truncate_field(x, TRUNC_COMMENT))
    if "reviewer_evidence_span" in out.columns:
        out["reviewer_evidence_span"] = out["reviewer_evidence_span"].map(
            lambda x: truncate_field(x, TRUNC_EVIDENCE)
        )
    return out


def _run_label_filter(run_label: str | None) -> str:
    if not run_label:
        return ""
    safe = run_label.replace("'", "''")
    return f" AND run_label = '{safe}' "


def run_triage(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    *,
    registry,
    oldest_limit: int = 200,
    run_label: str | None = None,
) -> dict[str, int | str]:
    """Execute triage queries and write bundle files. Returns summary stats for testing."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rf = _run_label_filter(run_label)

    # ── counts_by_domain.csv
    dom_sql = f"""
        SELECT domain,
               COUNT(*) AS n_rows,
               COUNT(*) FILTER (WHERE verification_status IS NULL) AS n_pending,
               COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS n_reviewed
        FROM qa.manual_review_queue
        WHERE 1=1 {rf}
        GROUP BY 1
        ORDER BY n_pending DESC, n_rows DESC
    """
    dom_df = con.execute(dom_sql).df()
    dom_df.to_csv(out_dir / "counts_by_domain.csv", index=False)

    # ── counts_by_verification_status.csv
    ver_sql = f"""
        SELECT CASE
                 WHEN verification_status IS NULL THEN '(pending_null)'
                 ELSE COALESCE(NULLIF(TRIM(CAST(verification_status AS VARCHAR)), ''), '(empty_string)')
               END AS verification_status,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE 1=1 {rf}
        GROUP BY 1
        ORDER BY n_rows DESC
    """
    ver_df = con.execute(ver_sql).df()
    ver_df.to_csv(out_dir / "counts_by_verification_status.csv", index=False)

    # ── counts_manuscript_quality_tiers.csv — separates structural completion vs sign-off quality
    syn_pred = mrq_synthetic_placeholder_where_sql("verification_status")
    tier_sql = f"""
        SELECT manuscript_quality_tier, COUNT(*) AS n_rows
        FROM (
            SELECT CASE
                WHEN verification_status IS NULL THEN 'A_pending_blocks_structural_release'
                WHEN {syn_pred} THEN 'B_synthetic_placeholder_not_manuscript_signoff'
                WHEN LOWER(TRIM(CAST(verification_status AS VARCHAR))) LIKE 'auto_accepted%'
                    THEN 'C_automation_tier_policy_only'
                WHEN reviewer IS NOT NULL
                     AND TRIM(CAST(reviewer AS VARCHAR)) <> ''
                     AND reviewed_at IS NOT NULL
                    THEN 'D_human_review_identity_present'
                ELSE 'E_reviewed_status_without_reviewer_timestamp'
            END AS manuscript_quality_tier
            FROM qa.manual_review_queue
            WHERE 1=1 {rf}
        ) t
        GROUP BY 1
        ORDER BY 1
    """
    tier_df = con.execute(tier_sql).df()
    tier_df.to_csv(out_dir / "counts_manuscript_quality_tiers.csv", index=False)

    # ── counts_mrq_three_bucket_signoff.csv — manuscript governance lens (3 buckets)
    # Order matches counts_manuscript_quality_tiers: auto_accepted* must precede
    # reviewer+reviewed_at so tier-policy service identities are not counted as human-reviewed.
    three_sql = f"""
        SELECT signoff_bucket, COUNT(*) AS n_rows
        FROM (
            SELECT CASE
                WHEN verification_status IS NULL THEN 'unresolved_pending'
                WHEN {syn_pred} THEN 'synthetic_automation_only'
                WHEN LOWER(TRIM(CAST(verification_status AS VARCHAR))) LIKE 'auto_accepted%'
                    THEN 'automation_tier_or_incomplete_non_human'
                WHEN reviewer IS NOT NULL
                     AND TRIM(CAST(reviewer AS VARCHAR)) <> ''
                     AND reviewed_at IS NOT NULL
                    THEN 'true_human_reviewed'
                ELSE 'automation_tier_or_incomplete_non_human'
            END AS signoff_bucket
            FROM qa.manual_review_queue
            WHERE 1=1 {rf}
        ) t
        GROUP BY 1
        ORDER BY 1
    """
    three_df = con.execute(three_sql).df()
    three_df.to_csv(out_dir / "counts_mrq_three_bucket_signoff.csv", index=False)

    # ── counts_promotable_blocking.csv — aligned with scripts/119 strict G7:
    #    rows with verification_status IS NULL are "pending" and block release-mode validation.
    block_sql = f"""
        SELECT category, n_rows FROM (
        SELECT 'blocking_release_pending_verification' AS category,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL {rf}
        UNION ALL
        SELECT 'cleared_has_verification_status' AS category,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE verification_status IS NOT NULL {rf}
        UNION ALL
        SELECT 'pending_discordant_existing' AS category,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL
          AND algorithm_status = 'discordant_existing' {rf}
        UNION ALL
        SELECT 'pending_fill_candidate' AS category,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL
          AND algorithm_status = 'existing_missing_fill_candidate' {rf}
        UNION ALL
        SELECT 'pending_other_algorithm_status' AS category,
               COUNT(*) AS n_rows
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL
          AND COALESCE(algorithm_status, '') NOT IN ('discordant_existing', 'existing_missing_fill_candidate') {rf}
        ) q
        ORDER BY category
    """
    block_df = con.execute(block_sql).df()
    block_df.to_csv(out_dir / "counts_promotable_blocking.csv", index=False)

    # ── domains_highest_pending_volume.csv
    vol_df = dom_df.copy()
    vol_df.rename(columns={"n_pending": "pending_count", "n_rows": "total_rows"}, inplace=True)
    vol_df.to_csv(out_dir / "domains_highest_pending_volume.csv", index=False)

    # ── oldest_pending_rows.csv
    old_sql = f"""
        SELECT review_row_id, run_label, research_id, domain, entity_type,
               algorithm_status, loaded_at
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL {rf}
        ORDER BY loaded_at ASC NULLS LAST
        LIMIT {int(oldest_limit)}
    """
    old_df = con.execute(old_sql).df()
    old_df.to_csv(out_dir / "oldest_pending_rows.csv", index=False)

    # ── Worklists: pending rows only, split by domain × qa_tier
    wl_sql = f"""
        SELECT {WORKLIST_SELECT_COLS.strip()}
        FROM qa.manual_review_queue
        WHERE verification_status IS NULL {rf}
    """
    pending_df = con.execute(wl_sql).df()
    pending_df = sanitize_worklist_df(pending_df)
    work_root = out_dir / "worklists"
    work_root.mkdir(exist_ok=True)

    if not pending_df.empty:
        pending_df["qa_tier"] = pending_df["domain"].map(lambda d: _qa_tier_for_domain(registry, d))
        for (_dom, _tier), g in pending_df.groupby(["domain", "qa_tier"], dropna=False):
            dslug = _domain_slug(str(_dom) if _dom is not None else "unknown")
            tslug = _domain_slug(str(_tier) if _tier is not None else "unknown")
            path = work_root / f"worklist__{dslug}__tier_{tslug}.csv"
            g.drop(columns=["qa_tier"], errors="ignore").to_csv(path, index=False)

    # summary.md
    total = int(con.execute(f"SELECT COUNT(*) FROM qa.manual_review_queue WHERE 1=1 {rf}").fetchone()[0])
    n_pending = int(
        con.execute(
            f"SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL {rf}"
        ).fetchone()[0]
    )
    n_rev = total - n_pending
    run_labels = con.execute(
        f"SELECT run_label, COUNT(*) AS n FROM qa.manual_review_queue WHERE 1=1 {rf} GROUP BY 1 ORDER BY n DESC"
    ).fetchall()
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    top_domains = dom_df.head(10).to_dict("records") if len(dom_df) else []

    lines = [
        "# Manual review queue triage bundle",
        "",
        f"Generated (UTC): `{utc_now}`",
        "",
        "## Scope",
        "",
        "- Source table: `qa.manual_review_queue` (read-only export).",
        "- **No raw note text**: `review_reason` / note-adjacent snippets are **not** exported; ",
        "  `entity_value_norm`, `reviewer_comment`, and `reviewer_evidence_span` are **truncated**.",
        "- **Promotion semantics unchanged**: this script does not UPDATE MotherDuck or alter gate logic.",
        "",
    ]
    if run_label:
        lines.append(f"- **Filtered** to `run_label = {run_label!r}`.")
        lines.append("")
    lines.extend(
        [
            "## Headline counts",
            "",
            f"- Total rows (after filter): **{total:,}**",
            f"- `verification_status` present (reviewed): **{n_rev:,}**",
            f"- Pending (`verification_status` NULL) — **blocks strict `119 --release-mode`**: **{n_pending:,}**",
            "",
            "## Files",
            "",
            "| File | Purpose |",
            "|------|---------|",
            "| `counts_by_domain.csv` | Rows / pending / reviewed by `domain` |",
            "| `counts_by_verification_status.csv` | Histogram of `verification_status` |",
            "| `counts_manuscript_quality_tiers.csv` | Pending vs synthetic vs automation vs human identity (sign-off posture) |",
            "| `counts_mrq_three_bucket_signoff.csv` | **Governance:** `unresolved_pending` / `synthetic_automation_only` / `true_human_reviewed` / `automation_tier_or_incomplete_non_human` |",
            "| `counts_promotable_blocking.csv` | Blocking vs cleared + pending algorithm breakdown |",
            "| `domains_highest_pending_volume.csv` | Domains ranked by pending count |",
            "| `oldest_pending_rows.csv` | Stale pending rows by `loaded_at` |",
            "| `worklists/worklist__<domain>__tier_<tier>.csv` | Pending-only slices for reviewers |",
            "",
            "## Run labels in this export",
            "",
        ]
    )
    for rl, n in run_labels[:20]:
        lines.append(f"- `{rl}`: {n:,} rows")
    if len(run_labels) > 20:
        lines.append(f"- … and {len(run_labels) - 20} more")
    lines.extend(["", "## Top domains by pending volume", ""])
    for row in top_domains:
        lines.append(
            f"- **{row.get('domain')}**: pending {row.get('n_pending', 0):,} "
            f"(total {row.get('n_rows', 0):,})"
        )
    lines.extend(
        [
            "",
            "## Manuscript sign-off quality tiers",
            "",
            "Counts are **mutually exclusive** (first matching branch wins). "
            "`119 --release-mode` **fails** CHECK **5b** when tier **B** (synthetic placeholders) "
            "has **any** rows. When B is empty, automation tier **C** can still be "
            "**non-manuscript** from a governance lens (see `counts_mrq_three_bucket_signoff.csv`). "
            "Tier **D** is the conservative \"human reviewer identity\" bucket (non-empty `reviewer` + `reviewed_at`).",
            "",
            "| Tier | n_rows |",
            "|------|--------:|",
        ]
    )
    for _, tr in tier_df.iterrows():
        lines.append(f"| `{tr['manuscript_quality_tier']}` | {int(tr['n_rows']):,} |")
    lines.append("")
    lines.extend(
        [
            "## Three-bucket manuscript sign-off (governance)",
            "",
            "| Bucket | n_rows |",
            "|--------|--------:|",
        ]
    )
    for _, tr in three_df.iterrows():
        lines.append(f"| `{tr['signoff_bucket']}` | {int(tr['n_rows']):,} |")
    lines.append("")

    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    return {
        "total_rows": total,
        "pending": n_pending,
        "reviewed": n_rev,
        "worklist_files": len(list(work_root.glob("worklist__*.csv"))) if work_root.exists() else 0,
        "manuscript_tier_rows": len(tier_df),
        "three_bucket_rows": len(three_df),
    }


def main() -> None:
    args = parse_args()
    if args.md and args.read_scaling:
        print("FATAL: pass at most one of --md and --read-scaling.")
        sys.exit(1)
    reg = load_registry()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.output_root)
    out_dir = out_root / f"review_queue_triage_{stamp}"
    con = get_connection(args)
    try:
        stats = run_triage(
            con,
            out_dir,
            registry=reg,
            oldest_limit=args.oldest_limit,
            run_label=args.run_label,
        )
        print(f"  [triage] Wrote bundle: {out_dir}")
        print(f"  [triage] total={stats['total_rows']:,} pending={stats['pending']:,} reviewed={stats['reviewed']:,}")
        print(f"  [triage] worklist CSVs: {stats['worklist_files']}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
