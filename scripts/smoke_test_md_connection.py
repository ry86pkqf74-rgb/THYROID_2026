#!/usr/bin/env python3
"""Smoke test: verify MotherDuck connection layer.

Usage:
    .venv/bin/python scripts/smoke_test_md_connection.py          # local file
    .venv/bin/python scripts/smoke_test_md_connection.py --md      # MotherDuck
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "thyroid_master.duckdb"


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test DB connection layer")
    ap.add_argument("--md", action="store_true", help="Connect via MotherDuck")
    args = ap.parse_args()

    from utils.md_connect import connect_md_or_file

    con = connect_md_or_file(DB_PATH, md=args.md)

    try:
        info = con.execute("PRAGMA version").fetchone()
        db_list = [
            r[0]
            for r in con.execute("SHOW DATABASES").fetchall()
        ]
    except Exception as exc:
        print(f"FAIL: query error — {exc}")
        return 1

    version = info[0] if info else "unknown"
    is_md = any(d.startswith("md:") or d == "my_db" for d in db_list)

    print(f"DuckDB version : {version}")
    print(f"Databases       : {db_list}")
    print(f"Connection type : {'MotherDuck (cloud)' if is_md else 'Local file'}")
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
