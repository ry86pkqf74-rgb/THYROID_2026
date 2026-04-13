#!/usr/bin/env python3
"""Read-only export for specimen/genomic review burden tables (MotherDuck or local).

Exports **qa.specimen_genomic_link_review_v1** (with optional join to
``main.specimen_genomic_assay_v1`` for ``linkage_confidence_tier``) and summary rows from
**qa.v_diag_specimen_review_burden_v1**.

Safety:
  - No clinical note bodies (source tables do not include ``note_text``).
  - ``conflict_summary``, ``reason_codes``, and ``source_row_key`` are truncated for CSVs.
  - Batched CSVs by ``linkage_confidence_tier`` (when join succeeds) × ``review_status`` ×
    ``source_table`` × age bucket (queued_at).

Connection modes (exactly one — same pattern as ``scripts/141_fhir_specimen_json_export.py``):

* ``--md`` — fail-closed MotherDuck with RW token.
* ``--read-scaling`` — ``MD_READ_SCALING_TOKEN`` only; refresh readers after writer snapshot
  (``scripts/136_md_read_scaling_snapshot_refresh.py reader`` or ``REFRESH DATABASE``).
* Neither flag — local ``--db-path`` file DuckDB.

Usage:
  .venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --md --output-root exports
  .venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --read-scaling --output-root exports
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file, connect_read_scaling_fail_closed  # noqa: E402

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORTS = ROOT / "exports"

DEFAULT_READ_SCALING_UA = "THYROID_2026_specimen_genomic_review_export_rs/1.0"
DEFAULT_READ_SCALING_SESSION_HINT = "THYROID_2026_specimen_genomic_review_export_rs_v1"

TRUNC_CONFLICT = 160
TRUNC_REASON = 120
TRUNC_ROW_KEY = 96


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
        help="Override MOTHERDUCK_SESSION_HINT for MotherDuck query attribution.",
    )
    p.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Local DuckDB path (when not using --md or --read-scaling).",
    )
    p.add_argument(
        "--output-root",
        default=str(EXPORTS),
        help="Parent directory for specimen_genomic_review_* folders (default: exports/).",
    )
    return p.parse_args()


def validate_connection_args(args: argparse.Namespace) -> None:
    if args.md and args.read_scaling:
        print("FATAL: pass at most one of --md and --read-scaling (or neither for local --db-path).")
        sys.exit(1)
    if getattr(args, "md_sa", False) and not args.md:
        print("FATAL: --md-sa is only valid with --md.")
        sys.exit(1)


def _resolved_session_hint(cli: str | None, *, read_scaling: bool) -> str | None:
    import os

    if cli and str(cli).strip():
        return str(cli).strip()
    if read_scaling:
        rs = (os.environ.get("MD_READ_SCALING_SESSION_HINT") or "").strip()
        if rs:
            return rs
    env = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip()
    if env:
        return env
    if read_scaling:
        return DEFAULT_READ_SCALING_SESSION_HINT
    return None


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    import os

    if args.md:
        ua = os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "THYROID_2026_specimen_genomic_review_export/1.0"
        cli = args.session_hint.strip() if args.session_hint and str(args.session_hint).strip() else None
        hint = _resolved_session_hint(cli, read_scaling=False)
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


def _trunc(val: object, max_len: int) -> str | None:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    t = str(val).strip()
    if not t or t.lower() == "nan":
        return None
    if len(t) <= max_len:
        return t
    return t[: max_len - 3] + "..."


def _age_bucket_sql() -> str:
    return """
    CASE
      WHEN r.queued_at IS NULL THEN 'unknown_age'
      WHEN date_diff('day', CAST(r.queued_at AS DATE), current_date) <= 30 THEN 'age_0_30d'
      WHEN date_diff('day', CAST(r.queued_at AS DATE), current_date) <= 90 THEN 'age_31_90d'
      WHEN date_diff('day', CAST(r.queued_at AS DATE), current_date) <= 365 THEN 'age_91_365d'
      ELSE 'age_gt_365d'
    END
    """


def run_export(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, int | str]:
    out_dir.mkdir(parents=True, exist_ok=True)

    burden_sql = """
        SELECT queue_key, review_status, n_rows
        FROM qa.v_diag_specimen_review_burden_v1
        ORDER BY queue_key, review_status
    """
    try:
        burden_df = con.execute(burden_sql).df()
    except Exception as exc:
        burden_df = pd.DataFrame({"error": [str(exc)]})
    burden_df.to_csv(out_dir / "v_diag_specimen_review_burden_v1.csv", index=False)

    detail_sql = f"""
        SELECT
          r.review_queue_id,
          r.genomic_assay_id,
          r.research_id,
          r.molecular_episode_id,
          r.reason_codes,
          r.conflict_summary,
          r.source_table,
          r.source_row_key,
          r.queued_at,
          r.review_status,
          g.linkage_confidence_tier,
          {_age_bucket_sql().strip()} AS age_bucket
        FROM qa.specimen_genomic_link_review_v1 r
        LEFT JOIN main.specimen_genomic_assay_v1 g
          ON g.genomic_assay_id = r.genomic_assay_id
    """
    try:
        raw = con.execute(detail_sql).df()
    except Exception as exc:
        (out_dir / "specimen_genomic_link_review_ERROR.txt").write_text(str(exc), encoding="utf-8")
        raw = pd.DataFrame()

    n_rows = len(raw)
    if not raw.empty:
        for col, lim in (
            ("conflict_summary", TRUNC_CONFLICT),
            ("reason_codes", TRUNC_REASON),
            ("source_row_key", TRUNC_ROW_KEY),
        ):
            if col in raw.columns:
                raw[col] = raw[col].map(lambda x, L=lim: _trunc(x, L))

    raw.to_csv(out_dir / "specimen_genomic_link_review_v1_all.csv", index=False)

    worklists = out_dir / "worklists"
    worklists.mkdir(exist_ok=True)
    batch_cols = ["linkage_confidence_tier", "review_status", "source_table", "age_bucket"]
    present = [c for c in batch_cols if c in raw.columns]
    n_batches = 0
    if present and not raw.empty:
        for keys, g in raw.groupby(present, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            parts = []
            for col, val in zip(present, keys):
                slug = str(val if val is not None else "null").replace("/", "_")[:80]
                parts.append(f"{col}_{slug}")
            fname = "__".join(parts) + ".csv"
            if len(fname) > 200:
                fname = fname[:197] + ".csv"
            g.to_csv(worklists / fname, index=False)
            n_batches += 1

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_lines = [
        "# Specimen / genomic review queue export",
        "",
        f"Generated (UTC): `{utc_now}`",
        "",
        "## Source",
        "",
        "- `qa.specimen_genomic_link_review_v1` (detail + batched worklists)",
        "- `qa.v_diag_specimen_review_burden_v1` (counts by queue_key / review_status)",
        "",
        "## Safety",
        "",
        "- No raw note text (not present on these objects).",
        f"- Truncation: conflict_summary ≤{TRUNC_CONFLICT}, reason_codes ≤{TRUNC_REASON}, "
        f"source_row_key ≤{TRUNC_ROW_KEY} chars.",
        "",
        "## Counts",
        "",
        f"- `specimen_genomic_link_review_v1` rows exported: **{n_rows:,}**",
        f"- Batched worklist files: **{n_batches}**",
        "",
    ]
    (out_dir / "summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    return {"detail_rows": n_rows, "batch_files": n_batches}


def main() -> None:
    args = parse_args()
    validate_connection_args(args)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.output_root)
    out_dir = out_root / f"specimen_genomic_review_{stamp}"
    con = get_connection(args)
    try:
        stats = run_export(con, out_dir)
        print(f"  [151] Wrote: {out_dir}")
        print(f"  [151] detail_rows={stats['detail_rows']:,} batch_files={stats['batch_files']}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
