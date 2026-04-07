#!/usr/bin/env python3
"""Deploy qa.v_diag_* specimen/FHIR diagnostic views only (no full 138 rebuild).

Uses fail-closed MotherDuck connection with custom_user_agent=specimen_fhir_release_ops_v1.

Usage:
  .venv/bin/python scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md
  .venv/bin/python scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_PATH = ROOT / "scripts" / "sql" / "142_specimen_fhir_qa_diagnostics_ddl.sql"
UA = "specimen_fhir_release_ops_v1"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy specimen/FHIR QA diagnostic views.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB when not --md.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ddl = DDL_PATH.read_text(encoding="utf-8")
    if args.md:
        from utils.md_connect import connect_md_or_file

        hint = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip() or None
        con = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            custom_user_agent=UA,
            motherduck_session_hint=hint,
        )
    else:
        import duckdb

        con = duckdb.connect(str(args.db_path))
    try:
        con.execute(ddl)
    finally:
        con.close()
    print(f"OK — applied {DDL_PATH.name} (UA={UA if args.md else 'local'})")


if __name__ == "__main__":
    main()
