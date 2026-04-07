#!/usr/bin/env python3
"""Read-only MotherDuck audit vs README claims. No DDL/DML; never prints tokens."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> None:
    import io
    import contextlib

    from utils.md_connect import connect_md_or_file

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        con = connect_md_or_file(
            Path("thyroid_master.duckdb"),
            md=True,
            fail_closed=True,
        )
    (OUT / "connection_log.txt").write_text(buf.getvalue(), encoding="utf-8")

    def qcsv(name: str, sql: str) -> None:
        rel = OUT / name
        con.execute(f"COPY ({sql}) TO '{rel.as_posix()}' (HEADER, DELIMITER ',')")

    # 1
    qcsv(
        "01_current_database.csv",
        "SELECT current_database() AS current_database",
    )

    # 2 — all schemas (exclude system)
    qcsv(
        "02_all_schemas.csv",
        """
        SELECT DISTINCT schema_name AS schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY 1
        """,
    )

    # 3
    qcsv(
        "03_release_schemas.csv",
        """
        SELECT DISTINCT schema_name AS schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'release_%'
        ORDER BY 1
        """,
    )

    # 4 — latest manifest rows (order by release_tag desc if present)
    try:
        cols = con.sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'qa' AND table_name = 'release_manifest'"
        ).fetchall()
        cnames = [c[0] for c in cols]
    except Exception:
        cnames = []

    if "release_tag" in cnames:
        order = "release_tag DESC"
    elif "created_at" in cnames:
        order = "created_at DESC"
    else:
        order = "1"

    qcsv(
        "04_release_manifest_latest.csv",
        f"SELECT * FROM qa.release_manifest ORDER BY {order} LIMIT 25",
    )

    # 5
    qcsv(
        "05_manual_review_queue_counts.csv",
        """
        SELECT
          COUNT(*) AS total_rows,
          COUNT(*) FILTER (WHERE verification_status IS NULL) AS pending_null_status,
          COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS reviewed_non_null_status
        FROM qa.manual_review_queue
        """,
    )

    # 6
    qcsv(
        "06_load_inventory_totals.csv",
        """
        SELECT
          COUNT(*) AS total_rows,
          COUNT(*) FILTER (WHERE NOT COALESCE(row_match, FALSE)) AS mismatch_count
        FROM v2_stage.load_inventory
        """,
    )

    # 7
    qcsv(
        "07_table_counts_by_schema.csv",
        """
        SELECT
          table_schema AS schema_name,
          table_type AS table_type,
          COUNT(*) AS n_objects
        FROM information_schema.tables
        WHERE table_schema IN ('v2_stage', 'main', 'qa')
        GROUP BY table_schema, table_type
        ORDER BY table_schema, table_type
        """,
    )

    con.close()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"audit failed: {e}", file=sys.stderr)
        sys.exit(1)
