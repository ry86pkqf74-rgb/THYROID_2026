#!/usr/bin/env python3
"""Emit staging QC rows for MotherDuck v2_stage (fail-closed when --md).

Writes reports/motherduck_stage_counts.csv. Does not print secrets.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

DEFAULT_SESSION = "THYROID_2026"


def _table_exists(con, schema: str, name: str) -> bool:
    q = """
    SELECT COUNT(*) FROM duckdb_tables()
    WHERE database_name = current_database()
      AND schema_name = ?
      AND table_name = ?
    """
    try:
        return int(con.execute(q, [schema, name]).fetchone()[0]) > 0
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--md", action="store_true", help="Query MotherDuck (required).")
    p.add_argument(
        "--out",
        default=str(ROOT / "reports" / "motherduck_stage_counts.csv"),
        help="Output CSV path.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("  [error] use --md", file=sys.stderr)
        sys.exit(2)

    os.environ.setdefault("MOTHERDUCK_SESSION_HINT", DEFAULT_SESSION)
    os.environ.setdefault(
        "MOTHERDUCK_CUSTOM_USER_AGENT",
        "THYROID_2026_molecular/142_staging_qc;kind=staging_qc",
    )

    db_path = ROOT / "thyroid_master.duckdb"
    con = connect_md_fail_closed(
        db_path,
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
    )
    rows: list[dict[str, object]] = []

    tbl_df = con.execute(
        """
        SELECT DISTINCT table_name
        FROM duckdb_tables()
        WHERE schema_name = 'v2_stage'
          AND database_name = current_database()
        ORDER BY table_name
        """
    ).fetchdf()
    for t in tbl_df["table_name"].tolist():
        n = con.execute(f'SELECT COUNT(*) FROM v2_stage."{t}"').fetchone()[0]
        rows.append(
            {
                "check": "row_count",
                "object": f"v2_stage.{t}",
                "metric": "n_rows",
                "value": int(n),
                "status": "",
            }
        )

    if _table_exists(con, "v2_stage", "canonical_extracted_fact_long_v2"):
        v2_null_rid = con.execute(
            """
            SELECT COUNT(*) FROM v2_stage.canonical_extracted_fact_long_v2
            WHERE research_id IS NULL
            """
        ).fetchone()[0]
        rows.append(
            {
                "check": "null_keys",
                "object": "v2_stage.canonical_extracted_fact_long_v2",
                "metric": "null_research_id",
                "value": int(v2_null_rid),
                "status": "FAIL" if v2_null_rid else "PASS",
            }
        )

        dup_groups = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT fact_id
                FROM v2_stage.canonical_extracted_fact_long_v2
                GROUP BY fact_id
                HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()[0]
        rows.append(
            {
                "check": "duplicate_keys",
                "object": "v2_stage.canonical_extracted_fact_long_v2",
                "metric": "duplicate_fact_id_groups",
                "value": int(dup_groups),
                "status": "FAIL" if dup_groups else "PASS",
            }
        )
    else:
        rows.append(
            {
                "check": "null_keys",
                "object": "v2_stage.canonical_extracted_fact_long_v2",
                "metric": "null_research_id",
                "value": "",
                "status": "missing",
            }
        )

    if _table_exists(con, "v2_stage", "canonical_fact_quarantine_v2"):
        q1 = con.execute(
            "SELECT COUNT(*) FROM v2_stage.canonical_fact_quarantine_v2"
        ).fetchone()[0]
        rows.append(
            {
                "check": "quarantine",
                "object": "v2_stage.canonical_fact_quarantine_v2",
                "metric": "n_rows",
                "value": int(q1),
                "status": "",
            }
        )
    else:
        rows.append(
            {
                "check": "quarantine",
                "object": "v2_stage.canonical_fact_quarantine_v2",
                "metric": "n_rows",
                "value": "",
                "status": "missing",
            }
        )

    if _table_exists(con, "v2_stage", "canonical_extracted_fact_long_v2"):
        dom = con.execute(
            """
            SELECT fact_domain, COUNT(*) AS n
            FROM v2_stage.canonical_extracted_fact_long_v2
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchdf()
        for _, r in dom.iterrows():
            rows.append(
                {
                    "check": "domain_coverage",
                    "object": str(r.iloc[0]),
                    "metric": "n_rows_clean_v2",
                    "value": int(r.iloc[1]),
                    "status": "",
                }
            )

    if _table_exists(con, "v2_stage", "load_inventory"):
        lm = con.execute(
            """
            SELECT BOOL_AND(row_match) AS all_match, COUNT(*) AS n_rows, MAX(load_id) AS max_load_id
            FROM v2_stage.load_inventory
            """
        ).fetchone()
        rows.append(
            {
                "check": "load_inventory",
                "object": "v2_stage.load_inventory",
                "metric": "all_row_match",
                "value": str(lm[0]),
                "status": "PASS" if lm[0] else "FAIL",
            }
        )
    else:
        rows.append(
            {
                "check": "load_inventory",
                "object": "v2_stage.load_inventory",
                "metric": "all_row_match",
                "value": "",
                "status": "missing",
            }
        )

    mm_candidates = (
        "imaging_fna_linkage_mm_v1",
        "multimodal_contract_mm_v1",
        "imaging_fna_linkage_mm_v1_validation",
    )
    for name in mm_candidates:
        q = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT table_name
                FROM duckdb_tables()
                WHERE schema_name = 'main'
                  AND database_name = current_database()
                  AND table_name = ?
            ) t
            """,
            [name],
        ).fetchone()[0]
        rows.append(
            {
                "check": "multimodal_main",
                "object": f"main.{name}",
                "metric": "duckdb_tables_hit",
                "value": int(q),
                "status": "INFO",
            }
        )

    con.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"  [ok] wrote {out}")


if __name__ == "__main__":
    main()
