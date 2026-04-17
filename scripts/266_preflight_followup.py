#!/usr/bin/env python3
"""Preflight follow-up — surface divergence detail for Script 266 v2."""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

OUT = HERE / "output" / "266_preflight_followup.json"


def main() -> int:
    con = connect_locked()
    out: dict = {}

    out["ws_breakdown_detail"] = con.execute(f"""
        SELECT table_name, table_type FROM information_schema.tables
         WHERE table_catalog='{PUBLICATION_DB}'
           AND table_schema='manuscript_workspace'
         ORDER BY table_type, table_name
    """).fetchall()

    out["conventions_full"] = con.execute(f"""
        SELECT *
          FROM {PUBLICATION_DB}.manuscript_workspace.__conventions
         ORDER BY 1
    """).fetchall()
    out["conventions_columns"] = [
        c[0] for c in con.execute(f"""
            SELECT column_name FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='manuscript_workspace'
               AND table_name='__conventions'
             ORDER BY ordinal_position
        """).fetchall()
    ]

    out["archive_databases_attached"] = [
        r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    ]

    try:
        out["archive_schemas"] = [
            r[0] for r in con.execute("""
                SELECT schema_name FROM information_schema.schemata
                 WHERE catalog_name='Thyroid 2026 UPdated'
                 ORDER BY 1
            """).fetchall()
        ]
    except Exception as e:
        out["archive_schemas_error"] = str(e)

    try:
        out["archive_pub_v1_0_tables"] = [
            r[0] for r in con.execute("""
                SELECT table_name FROM information_schema.tables
                 WHERE table_catalog='Thyroid 2026 UPdated'
                   AND table_schema='archive_pub_v1_0'
                 ORDER BY 1
            """).fetchall()
        ]
    except Exception as e:
        out["archive_pub_v1_0_tables_error"] = str(e)

    try:
        out["archive_main_tables_count"] = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='Thyroid 2026 UPdated'
               AND table_schema='main'
        """).fetchone()[0]
    except Exception as e:
        out["archive_main_tables_count_error"] = str(e)

    try:
        out["raw_source_tagged_tables"] = [
            r[0] for r in con.execute(f"""
                SELECT DISTINCT table_name
                  FROM information_schema.tables
                 WHERE table_catalog='{PUBLICATION_DB}'
                   AND table_schema='main'
                   AND COALESCE(table_comment,'') ILIKE '%raw_source%'
                 ORDER BY 1
            """).fetchall()
        ]
    except Exception as e:
        out["raw_source_tables_error"] = str(e)

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
