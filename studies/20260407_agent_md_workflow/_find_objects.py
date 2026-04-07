#!/usr/bin/env python3
"""Find thyroseq / molecular_testing objects across schemas."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import duckdb
from motherduck_client import get_token
from urllib.parse import quote_plus
import os


def main() -> None:
    token = get_token(prefer_service_account=False)
    qs = f"motherduck_token={quote_plus(token)}"
    con = duckdb.connect(f"md:?{qs}&custom_user_agent={quote_plus('THYROID_2026_find/1')}")
    try:
        con.execute('USE "Thyroid 2026"')
        q = """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND (
            table_name ILIKE '%molecular_testing%'
            OR table_name ILIKE '%thyroseq%'
            OR table_name ILIKE '%molecular_results%'
          )
        ORDER BY 1, 2
        """
        for r in con.execute(q).fetchall():
            print(r)
    finally:
        con.close()


if __name__ == "__main__":
    main()
