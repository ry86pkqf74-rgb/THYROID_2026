#!/usr/bin/env python3
"""128_canonical_extraction_run_id_backfill.py — Fill extraction_run_id on MotherDuck canonical.

Use when `main.canonical_extracted_fact_long_v2` has NULL/blank `extraction_run_id`
but regenerating parquet + `103_fact_lineage_materialize.py --md` is not practical.
Resolution rules match `utils/extraction_run_id_resolve.py` (and view `125`).

Append-only: replaces the canonical table from a computed frame (same columns); does not
touch quarantine or domain tables.

Usage:
  .venv/bin/python scripts/128_canonical_extraction_run_id_backfill.py --md --dry-run
  .venv/bin/python scripts/128_canonical_extraction_run_id_backfill.py --md --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.extraction_run_id_resolve import backfill_extraction_run_id_column  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"
TABLE = "canonical_extracted_fact_long_v2"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument("--db-path", default=str(DB_PATH))
    p.add_argument("--md-user-agent", default=None)
    p.add_argument("--md-session-hint", default=None)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Print counts only; no writes.")
    g.add_argument("--apply", action="store_true", help="CREATE OR REPLACE canonical table from resolved frame.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("  ERROR: --md required (cloud canonical backfill only).")
        sys.exit(2)

    ua = args.md_user_agent or os.environ.get(
        "MOTHERDUCK_CUSTOM_USER_AGENT",
        "THYROID_2026_extraction_run_backfill/1.0",
    )
    con = connect_md_or_file(
        Path(args.db_path),
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
        custom_user_agent=ua,
        motherduck_session_hint=args.md_session_hint,
    )
    try:
        df = con.execute(f"SELECT * FROM main.{TABLE}").fetchdf()
        runs = con.execute("SELECT * FROM main.note_extraction_runs").fetchdf()
        before = int(
            (
                df["extraction_run_id"].isna()
                | (df["extraction_run_id"].astype(str).str.strip() == "")
            ).sum()
        )
        fixed = backfill_extraction_run_id_column(df, runs)
        after = int(
            (
                fixed["extraction_run_id"].isna()
                | (fixed["extraction_run_id"].astype(str).str.strip() == "")
            ).sum()
        )
        print(f"  rows={len(df):,}  blank_before={before:,}  blank_after={after:,}")
        if args.dry_run:
            print("  dry-run: no table write")
            return
        con.register("_canonical_fixed", fixed)
        con.execute(f"CREATE OR REPLACE TABLE main.{TABLE} AS SELECT * FROM _canonical_fixed")
        con.unregister("_canonical_fixed")
        chk = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE extraction_run_id IS NULL OR "
            f"trim(cast(extraction_run_id AS VARCHAR)) = '') FROM main.{TABLE}"
        ).fetchone()[0]
        print(f"  [apply] main.{TABLE} replaced; remaining blank={int(chk):,}")
        if int(chk) > 0:
            sys.exit(1)
    finally:
        con.close()


if __name__ == "__main__":
    main()
