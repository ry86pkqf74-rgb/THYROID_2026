#!/usr/bin/env python3
"""
Profile MotherDuck note_entities tables: schemas, null rates, date-in-evidence gaps,
and histology/RAI/genetics table inventory.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from motherduck_client import MotherDuckClient, MotherDuckConfig

SHARE_RO = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
SHARE_RW = "md:_share/thyroid_research_2026/4839c7d2-67ce-48cb-952d-98b25f85d989"

NOTE_TABLES = [
    "note_entities_genetics",
    "note_entities_procedures",
    "note_entities_staging",
    "note_entities_complications",
    "note_entities_medications",
    "note_entities_problem_list",
]

DB_PREFIX = "thyroid_research_2026"


def section(title: str) -> None:
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def run() -> None:
    client = MotherDuckClient()
    con = client.connect_rw()

    try:
        con.execute(f"ATTACH '{SHARE_RO}' AS thyroid_ro_")
    except Exception:
        pass
    try:
        con.execute(f"ATTACH '{SHARE_RW}' AS thyroid_share")
    except Exception:
        pass

    # ── 1. SHOW ALL TABLES ──
    section("1. ALL TABLES INVENTORY")
    rows = con.execute("SHOW ALL TABLES").fetchall()
    desc = con.execute("SHOW ALL TABLES").description
    col_names = [d[0] for d in desc]
    print(f"{'database':<30} {'schema':<15} {'table_name':<45} {'column_count':<15} {'row_count'}")
    print("-" * 120)
    db_idx = col_names.index("database") if "database" in col_names else 0
    schema_idx = col_names.index("schema") if "schema" in col_names else 1
    name_idx = col_names.index("name") if "name" in col_names else 2
    for r in rows:
        cols = [str(c) for c in r]
        db = r[db_idx] if db_idx < len(r) else ""
        schema = r[schema_idx] if schema_idx < len(r) else ""
        name = r[name_idx] if name_idx < len(r) else ""
        col_count = r[col_names.index("column_count")] if "column_count" in col_names else ""
        row_count = r[col_names.index("estimated_size")] if "estimated_size" in col_names else ""
        print(f"{str(db):<30} {str(schema):<15} {str(name):<45} {str(col_count):<15} {str(row_count)}")

    # ── 2. DESCRIBE clinical_notes_long + note_entities_* ──
    section("2. TABLE SCHEMAS")
    tables_to_describe = ["clinical_notes_long"] + NOTE_TABLES
    for tbl in tables_to_describe:
        print(f"\n--- DESCRIBE {DB_PREFIX}.{tbl} ---")
        try:
            rows = con.execute(f"DESCRIBE {DB_PREFIX}.{tbl}").fetchall()
            print(f"  {'column_name':<35} {'column_type':<25} {'null':<8} {'key':<8} {'default'}")
            for r in rows:
                print(f"  {str(r[0]):<35} {str(r[1]):<25} {str(r[2]):<8} {str(r[3]):<8} {str(r[4]) if len(r) > 4 else ''}")
        except Exception as e:
            print(f"  ERROR: {e}")

    # ── 3. NULL-RATE PROFILING ──
    section("3. NULL-RATE PROFILING")
    print(f"{'table_name':<35} {'total_rows':>12} {'note_date_nulls':>16} {'entity_date_nulls':>18} {'entity_date_null%':>20}")
    print("-" * 105)
    for tbl in NOTE_TABLES:
        sql = f"""
        SELECT
            '{tbl}' AS table_name,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN note_date IS NULL THEN 1 ELSE 0 END) AS note_date_nulls,
            SUM(CASE WHEN entity_date IS NULL THEN 1 ELSE 0 END) AS entity_date_nulls,
            ROUND(100.0 * SUM(CASE WHEN entity_date IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS entity_date_null_pct
        FROM {DB_PREFIX}.{tbl}
        """
        try:
            r = con.execute(sql).fetchone()
            print(f"{r[0]:<35} {r[1]:>12,} {r[2]:>16,} {r[3]:>18,} {r[4]:>19.2f}%")
        except Exception as e:
            print(f"{tbl:<35} ERROR: {e}")

    # ── 4. DATE IN EVIDENCE BUT MISSING IN COLUMNS ──
    section("4. DATE-IN-EVIDENCE BUT MISSING entity_date")

    # First find the right column name for evidence text
    evidence_col = None
    for candidate in ["evidence_span", "evidence_text", "evidence", "text_span", "span"]:
        try:
            con.execute(f"SELECT {candidate} FROM {DB_PREFIX}.{NOTE_TABLES[0]} LIMIT 1")
            evidence_col = candidate
            break
        except Exception:
            continue

    if evidence_col is None:
        # Fall back to inspecting columns
        cols = con.execute(f"DESCRIBE {DB_PREFIX}.{NOTE_TABLES[0]}").fetchall()
        text_cols = [c[0] for c in cols if "VARCHAR" in str(c[1]).upper() or "TEXT" in str(c[1]).upper()]
        print(f"  Could not find evidence column. Text columns available: {text_cols}")
    else:
        print(f"  Using evidence column: {evidence_col}")
        date_regex = r"(?i)\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b"
        print(f"\n{'table_name':<35} {'n_date_like_missing_entity_date':>33} {'pct_of_table':>14}")
        print("-" * 85)
        for tbl in NOTE_TABLES:
            sql = f"""
            SELECT
                COUNT(*) AS n_date_like_but_missing_entity_date,
                ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM {DB_PREFIX}.{tbl}), 0), 2) AS pct_of_table
            FROM {DB_PREFIX}.{tbl}
            WHERE entity_date IS NULL
              AND regexp_matches({evidence_col}, '{date_regex}')
            """
            try:
                r = con.execute(sql).fetchone()
                print(f"{tbl:<35} {r[0]:>32} {r[1]:>13.2f}%")
            except Exception as e:
                print(f"{tbl:<35} ERROR: {e}")

    # ── 5. HISTOLOGY / RAI / GENETICS LOCATOR ──
    section("5. HISTOLOGY / RAI / GENETICS TABLE LOCATOR")
    sql = """
    SELECT table_catalog, table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_catalog LIKE '%thyroid%'
      AND (lower(table_name) LIKE '%histo%'
        OR lower(table_name) LIKE '%path%'
        OR lower(table_name) LIKE '%thyroseq%'
        OR lower(table_name) LIKE '%afirma%'
        OR lower(table_name) LIKE '%rai%'
        OR lower(table_name) LIKE '%genetic%'
        OR lower(table_name) LIKE '%tumor%'
        OR lower(table_name) LIKE '%biopsy%')
    ORDER BY table_name
    """
    rows = con.execute(sql).fetchall()
    print(f"{'catalog':<35} {'schema':<15} {'table_name':<45} {'type'}")
    print("-" * 105)
    for r in rows:
        print(f"{str(r[0]):<35} {str(r[1]):<15} {str(r[2]):<45} {str(r[3])}")

    # Describe each discovered table
    if rows:
        section("5b. SCHEMAS OF DISCOVERED TABLES")
        for r in rows:
            catalog, schema, tname = r[0], r[1], r[2]
            qualified = f"{catalog}.{schema}.{tname}" if schema != "main" else f"{catalog}.{tname}"
            print(f"\n--- DESCRIBE {qualified} ---")
            try:
                desc_rows = con.execute(f"DESCRIBE {qualified}").fetchall()
                print(f"  {'column_name':<35} {'column_type':<25} {'null':<8} {'key':<8}")
                for dr in desc_rows:
                    print(f"  {str(dr[0]):<35} {str(dr[1]):<25} {str(dr[2]):<8} {str(dr[3]):<8}")
            except Exception as e:
                print(f"  ERROR: {e}")

    con.close()
    print("\n\nDone.")


if __name__ == "__main__":
    run()
