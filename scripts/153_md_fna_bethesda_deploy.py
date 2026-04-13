#!/usr/bin/env python3
"""Deploy Phase 7 per-patient FNA Bethesda table to MotherDuck (and optional local DB).

Creates ``extracted_fna_bethesda_v1`` from ``build_fna_by_source_sql()`` in
``llm_extraction/extraction_audit_engine_v5.py`` (union of ``fna_cytology``,
``fna_episode_master_v2``, ``molecular_testing``), then mirrors to
``md_extracted_fna_bethesda_v1`` for RO-share parity with script 26 patterns.

Run:
  .venv/bin/python scripts/153_md_fna_bethesda_deploy.py --md
  .venv/bin/python scripts/153_md_fna_bethesda_deploy.py --local
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.extraction_audit_engine_v5 import build_fna_by_source_sql  # noqa: E402
from utils.md_connect import connect_md_fail_closed  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy extracted_fna_bethesda_v1 to DuckDB")
    parser.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Local thyroid_master.duckdb file",
    )
    args = parser.parse_args()
    if not args.md and not args.local:
        args.md = True

    db_path = ROOT / "thyroid_master.duckdb"
    if args.md:
        con = connect_md_fail_closed(db_path)
    else:
        import duckdb

        con = duckdb.connect(str(db_path))

    sql = build_fna_by_source_sql().strip()
    if not sql.endswith(";"):
        sql += ";"
    con.execute(sql)
    con.execute(
        """
        CREATE OR REPLACE TABLE md_extracted_fna_bethesda_v1 AS
        SELECT * FROM extracted_fna_bethesda_v1;
        """
    )
    row = con.execute("SELECT COUNT(*) FROM extracted_fna_bethesda_v1").fetchone()
    row_m = con.execute("SELECT COUNT(*) FROM md_extracted_fna_bethesda_v1").fetchone()
    if row is None or row_m is None:
        raise RuntimeError("COUNT(*) query returned no row")
    n, nm = int(row[0]), int(row_m[0])
    con.close()
    print(f"extracted_fna_bethesda_v1 rows: {n}")
    print(f"md_extracted_fna_bethesda_v1 rows: {nm}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
