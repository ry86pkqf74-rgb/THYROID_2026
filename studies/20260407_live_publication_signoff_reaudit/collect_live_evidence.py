#!/usr/bin/env python3
"""Collect live MotherDuck SQL evidence for publication signoff re-audit.

Run from repository root (fail-closed --md):
  export MOTHERDUCK_CUSTOM_USER_AGENT=...
  export MOTHERDUCK_SESSION_HINT=...
  .venv/bin/python studies/20260407_live_publication_signoff_reaudit/collect_live_evidence.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

OUT_DIR = Path(__file__).resolve().parent / "live_sql_exports"
DEFAULT_DB = ROOT / "thyroid_master.duckdb"


def _write_df(name: str, con, sql: str) -> None:
    import pandas as pd

    p = OUT_DIR / f"{name}.csv"
    try:
        df = con.execute(sql).df()
        df.to_csv(p, index=False)
        print(f"OK {name} -> {p} ({len(df)} rows)")
    except Exception as e:
        err = OUT_DIR / f"{name}_ERROR.txt"
        err.write_text(f"{type(e).__name__}: {e}\nSQL:\n{sql}\n", encoding="utf-8")
        print(f"FAIL {name}: {e}")


def main() -> None:
    import os

    from utils.md_connect import connect_md_fail_closed

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    prefer_sa = bool((os.environ.get("MD_SA_TOKEN") or "").strip())
    con = connect_md_fail_closed(
        DEFAULT_DB,
        prefer_service_account=prefer_sa,
    )
    try:
        _write_df("00_current_database", con, "SELECT current_database() AS current_database")
        # MotherDuck catalog introspection (may fail if token lacks permission)
        for info_name, sql in (
            (
                "01_md_information_schema_databases",
                "SELECT * FROM md_information_schema.databases ORDER BY 1",
            ),
            (
                "02_md_information_schema_database_snapshots",
                "SELECT * FROM md_information_schema.database_snapshots ORDER BY 1",
            ),
            (
                "03_schemas",
                """
                SELECT schema_name
                FROM information_schema.schemata
                ORDER BY schema_name
                """,
            ),
            (
                "04_release_schemas",
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name LIKE 'release_%'
                ORDER BY schema_name
                """,
            ),
            (
                "05_qa_release_manifest_latest",
                """
                SELECT *
                FROM qa.release_manifest
                ORDER BY created_at DESC NULLS LAST
                LIMIT 20
                """,
            ),
            (
                "06_manual_review_queue_totals",
                """
                SELECT
                  COUNT(*) AS total_rows,
                  COUNT(*) FILTER (WHERE verification_status IS NULL) AS pending_null_status,
                  COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS reviewed_non_null_status
                FROM qa.manual_review_queue
                """,
            ),
            (
                "07_manual_review_queue_verification_status",
                """
                SELECT verification_status, COUNT(*) AS n
                FROM qa.manual_review_queue
                GROUP BY 1
                ORDER BY n DESC
                """,
            ),
            (
                "08_promotion_review_decisions_counts",
                """
                SELECT COUNT(*) AS total_rows FROM qa.promotion_review_decisions
                """,
            ),
            (
                "09_promotion_review_decisions_recent",
                """
                SELECT *
                FROM qa.promotion_review_decisions
                LIMIT 50
                """,
            ),
            (
                "10_longitudinal_lab_canonical_v1_analyte",
                """
                SELECT COALESCE(analyte_group, '(null)') AS analyte_group, COUNT(*) AS n
                FROM main.longitudinal_lab_canonical_v1
                GROUP BY 1
                ORDER BY n DESC
                """,
            ),
            (
                "11_longitudinal_lab_deduped_v_analyte",
                """
                SELECT COALESCE(analyte_group, '(null)') AS analyte_group, COUNT(*) AS n
                FROM main.longitudinal_lab_deduped_v
                GROUP BY 1
                ORDER BY n DESC
                """,
            ),
            (
                "12_molecular_results_count",
                "SELECT COUNT(*) AS n FROM main.molecular_results",
            ),
        ):
            _write_df(info_name, con, sql)

        _write_df(
            "13_v_diag_specimen_review_burden_v1",
            con,
            "SELECT * FROM qa.v_diag_specimen_review_burden_v1 ORDER BY queue_key",
        )
        _write_df(
            "14_specimen_merge_review_queue_open_pending",
            con,
            """
            SELECT review_status, COUNT(*) AS n
            FROM qa.specimen_merge_review_queue_v1
            GROUP BY 1
            ORDER BY n DESC
            """,
        )
        _write_df(
            "15_query_history_sample",
            con,
            """
            SELECT query_id, user_agent, session_name, query_text, end_time, execution_time
            FROM md_information_schema.query_history
            ORDER BY end_time DESC NULLS LAST
            LIMIT 100
            """,
        )
        _write_df(
            "16_recent_queries_sample",
            con,
            """
            SELECT *
            FROM md_information_schema.recent_queries
            LIMIT 100
            """,
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
