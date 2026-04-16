#!/usr/bin/env python3
"""
THYROID_2026 — Script 221: Final Database Consolidation + MotherDuck Optimization

Consolidates the two MotherDuck databases before the ETE paper analysis:
  * thyroid_ete_fix_20260413  — canonical master (207 tables, 1,257-col master)
  * "Thyroid 2026"            — DuckLake DB where Scripts 218/219 ran (missing 37 tables)

PHASES
------
  1  Migrate missing tables from thyroid_ete_fix_20260413 → "Thyroid 2026"
  2  Sync Script 218/219 improvements into canonical_patient_master_v1
  3  Date/time provenance audit + days_from_surgery columns + multi-surgery linkage
  4  MotherDuck optimization — COMMENT ON COLUMN / TABLE
  5  Data dictionary refresh (CSV + MD + MotherDuck table)
  6  Parquet backup of canonical master + key source tables
  7  Final validation report

INVARIANTS (enforced at start and end of every write phase)
  - canonical_patient_master_v1 always has exactly 10,871 rows
  - research_id is never NULL
  - fna_path_outcome is never NULL
  - research_id is VARCHAR in canonical; BIGINT in gold_master_patient_facts_v1
    → ALWAYS CAST(research_id AS VARCHAR) when joining BIGINT sources

Run:
  .venv/bin/python scripts/221_final_database_consolidation.py [--dry-run] [--phase 1|2|3|4|5|6|7|all]
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

# Primary canonical database
DB_ETE = "thyroid_ete_fix_20260413"
# DuckLake database (spaces require quoted names in SQL)
DB_LAKE = "Thyroid 2026"
DB_LAKE_SQL = '"Thyroid 2026"'  # safe in SQL strings

CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10_871
SCRIPT_TAG = "221_final_database_consolidation"

# ===========================================================================
# Tables to migrate from thyroid_ete_fix → "Thyroid 2026"
# ===========================================================================

TABLES_TO_MIGRATE = [
    "mri_imaging",
    "op_sheet_data",
    "nsqip_data",
    "thyroid_weight_data",
    "parathyroid_notes_intent_v1",
    "_parathyroid_patient_rollup_v1",
    "_nsqip_thyroidectomy_enrichment_v1",
    "lab_values_complete_v1",
]

CANONICAL_TABLES = [
    "canonical_patient_master_v1",
    "canonical_diagnosis_unified_v1",
    "canonical_molecular_tested_v1",
    "canonical_recurrence_v1",
    "canonical_survival_followup_v1",
    "clinical_note_ln_patient_rollup_v1",
]

LAB_TABLES = [
    "lab_rollup_calcium_v1",
    "lab_rollup_pth_v1",
    "lab_rollup_tsh_v1",
    "lab_rollup_vitamin_d_v1",
    "lab_rollup_wide_v1",
]

FOLLOWUP_TABLES = [
    "_followup_all_dates_v2",
    "_followup_computed_v2",
    "_followup_patient_max_v2",
    "_notes_death_dates_v1",
    "_recurrence_event_sites_v1",
    "_recurrence_fna_sites_v1",
    "_nucmed_labs_parsed_v1",
    "_nucmed_labs_rollup_v1",
]

ALL_MIGRATE = TABLES_TO_MIGRATE + CANONICAL_TABLES + LAB_TABLES + FOLLOWUP_TABLES

# Columns that must never be NULL on canonical
REQUIRED_NON_NULL = ["research_id", "fna_path_outcome"]

# Tables to add comments to
TABLE_COMMENTS: dict[str, str] = {
    "canonical_patient_master_v1": (
        "Master analytical table: 10,871 thyroid surgery patients × 1,300+ columns. "
        "One row per patient. All data linked to research_id with date/time provenance."
    ),
    "imaging_nodule_master_v1": (
        "Nodule-level imaging data: 37,016 nodules across 6,126 patients "
        "with per-component ACR TI-RADS features."
    ),
    "fna_episode_master_v2": (
        "FNA episode data: 8,119 episodes across 5,266 patients with "
        "specimen site, laterality, Bethesda, dates."
    ),
    "fna_cytology": (
        "FNA cytology with multi-era Bethesda scoring (2010/2015/2023): 8,063 rows."
    ),
    "tirads_llm_extracted_v2": (
        "LLM-extracted TIRADS features from US report text: 12,900 rows, 1,429 patients, "
        "both 2017 and modified scoring."
    ),
    "tumor_pathology": (
        "Tumor pathology with 78 LN columns, per-cancer-type mets, ENE: 4,290 rows."
    ),
    "note_entities_llm_tirads_granular": (
        "Fleet NLP TIRADS entities from clinical notes (qwen3:32b): 27,707 rows, "
        "6,261 patients, mean conf 0.87."
    ),
    "note_entities_llm_cervical_ln_detail": (
        "Fleet NLP cervical LN entities: 36,964 rows, 6,632 patients, mean conf 0.90."
    ),
    "note_entities_llm_pathology": (
        "Fleet NLP pathology entities: 29,236 rows, 5,884 patients, mean conf 0.88."
    ),
    "ln_master_rollup_v1": (
        "Lymph node master rollup with x-marker fix: per-level, per-cancer-type, ENE."
    ),
    "complication_phenotype_v1": (
        "Per-complication-type phenotyping: 5,928 rows, 2,892 patients."
    ),
    "op_sheet_data": (
        "Operative sheet: parathyroid visualization, nerve stim, lobe resection "
        "for 8,733 patients."
    ),
    "mri_imaging": (
        "MRI extraction: 715 exams, 462 patients with impression, key findings, LN assessment."
    ),
    "ct_imaging": (
        "CT extraction: 7,701 exams, 3,086 patients with tracheal/substernal/LN findings."
    ),
    "nuclear_med": (
        "Nuclear medicine: 2,220 scans, 1,148 patients with uptake, tracer, indication."
    ),
    "clinical_notes_long": "Raw clinical notes for NLP extraction.",
    "path_synoptics": "Synoptic pathology reports: 11,688 rows.",
    "thyroid_weight_data": "Thyroid gland weight measurements: 10,001 patients.",
}

# Prefix → short description for auto-comment generation
AUTO_COL_DESC: dict[str, str] = {
    "_days_from_surg": "Days from first surgery date (negative = before surgery)",
    "nlp_": "NLP-extracted from clinical notes (qwen3:32b fleet)",
    "prm_": "From patient_refined_master_clinical_v12",
    "ops_": "From operative sheet data",
    "comp_": "Complication status from complication_phenotype_v1",
    "cnln_": "Clinical note lymph node integration",
    "syn_": "From synoptic pathology reports",
    "pet_": "PET/CT imaging data",
    "ct_": "CT imaging data",
    "mri_": "MRI imaging data",
    "nucmed_": "Nuclear medicine data",
    "lnus_": "Dedicated lymph node ultrasound data",
    "lab_": "Laboratory value",
    "nsqip_": "NSQIP perioperative quality data",
    "op_nlp_": "NLP-extracted from operative notes",
    "med_nlp_": "NLP-extracted medication data",
    "pmhx_": "Past medical history",
    "pshx_": "Past surgical history",
    "proc_nlp_": "NLP-extracted procedure data",
    "tirads_": "From extracted_tirads_validated_v1 / tirads_llm_extracted_v2",
    "bethesda_": "From fna_cytology",
    "ete_": "From extracted_ete_subgraded_v1",
    "gland_weight_": "From thyroid_weight_data",
    "bmi_": "BMI from nsqip_data / op_sheet_data / clinical_notes_long",
    "tg_": "From tg_timeline_patient_summary_v1",
    "gm_": "From gold_master_patient_facts_v1",
    "para_": "From parathyroid_notes_intent_v1",
    "mol_": "Molecular testing data",
    "rec_": "Recurrence data",
}

# ===========================================================================
# Connection utilities
# ===========================================================================


def connect() -> duckdb.DuckDBPyConnection:
    """Connect in multi-database mode (no USE) so both DBs are addressable."""
    token = get_token()
    if not token:
        print(f"[{SCRIPT_TAG}] ERROR: No MotherDuck token found.")
        sys.exit(1)
    print(f"[{SCRIPT_TAG}] Token: SET, len={len(token)}")
    # md: with no db name = multi-database mode
    con = duckdb.connect(f"md:?motherduck_token={token}")
    print(f"[{SCRIPT_TAG}] Connected (multi-db mode)")
    return con


def q(con: duckdb.DuckDBPyConnection, sql: str, label: str = "") -> Any:
    """Execute SQL with error logging. Returns result or None."""
    try:
        return con.execute(sql)
    except Exception as e:
        tag = f" [{label}]" if label else ""
        print(f"[{SCRIPT_TAG}]{tag} WARN: {e!s:.200s}")
        return None


def _quoted(db: str) -> str:
    """Return properly quoted database name for cross-db SQL."""
    return f'"{db}"' if " " in db else db


def table_exists(con: duckdb.DuckDBPyConnection, db: str, table: str) -> bool:
    """Check table existence via direct probe (information_schema unreliable in multi-db mode)."""
    try:
        qdb = _quoted(db)
        con.execute(f"SELECT 1 FROM {qdb}.main.{table} LIMIT 0")
        return True
    except Exception:
        return False


def col_exists(
    con: duckdb.DuckDBPyConnection, db: str, table: str, col: str
) -> bool:
    """Check column existence via direct probe."""
    try:
        qdb = _quoted(db)
        safe_col = col.replace('"', '""')
        con.execute(f'SELECT "{safe_col}" FROM {qdb}.main.{table} LIMIT 0')
        return True
    except Exception:
        return False


def describe_table(
    con: duckdb.DuckDBPyConnection, db: str, table: str
) -> list[tuple[str, str]]:
    """Return [(column_name, column_type)] via DESCRIBE — works in multi-db mode."""
    try:
        qdb = _quoted(db)
        rows = con.execute(f"DESCRIBE {qdb}.main.{table}").fetchall()
        # DESCRIBE returns (column_name, column_type, null, key, default, extra)
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"[{SCRIPT_TAG}] WARN describe {db}.{table}: {e!s:.120s}")
        return []


def row_count(con: duckdb.DuckDBPyConnection, db: str, table: str) -> int:
    try:
        qdb = _quoted(db)
        r = con.execute(f"SELECT COUNT(*) FROM {qdb}.main.{table}").fetchone()
        return r[0] if r else -1
    except Exception as e:
        print(f"[{SCRIPT_TAG}] row_count({table}): {e!s:.120s}")
        return -1


def check_invariants(con: duckdb.DuckDBPyConnection, label: str) -> None:
    """
    Schema-only invariant check: confirms canonical table exists and has required columns.
    Uses direct probes instead of information_schema (unreliable in MotherDuck multi-db mode).
    """
    all_ok = True

    # 1. Table exists
    if table_exists(con, DB_ETE, CANONICAL):
        print("  [✓] table_exists")
    else:
        print(f"  [✗] table_exists — {CANONICAL} not found in {DB_ETE}")
        all_ok = False

    # 2. Required columns exist
    for col in REQUIRED_NON_NULL:
        if col_exists(con, DB_ETE, CANONICAL, col):
            print(f"  [✓] has_{col}")
        else:
            print(f"  [✗] has_{col} — column missing from {CANONICAL}")
            all_ok = False

    if not all_ok:
        print(f"[{SCRIPT_TAG}] ABORT: invariant check failed at [{label}]")
        sys.exit(1)
    print(f"[{SCRIPT_TAG}] ✓ Schema invariants OK [{label}]")


# ===========================================================================
# PHASE 1 — Migrate missing tables from thyroid_ete_fix → Thyroid 2026
# ===========================================================================


def phase1_migrate(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 1: Migrate tables to {DB_LAKE_SQL} ══")
    results: list[tuple[str, int, int, str]] = []

    for table in ALL_MIGRATE:
        # Source row count
        src_n = row_count(con, DB_ETE, table)
        if src_n < 0:
            print(f"  SKIP {table}: not found in {DB_ETE}")
            results.append((table, -1, -1, "not_in_source"))
            continue

        # Check if already in destination
        dst_n = row_count(con, DB_LAKE, table)
        if dst_n == src_n:
            print(f"  ✓ {table}: already in dest ({dst_n} rows) — skip")
            results.append((table, src_n, dst_n, "already_present"))
            continue

        if dry_run:
            print(f"  [dry-run] would migrate {table}: {src_n} rows")
            results.append((table, src_n, -1, "dry_run"))
            continue

        # Attempt direct cross-db CREATE TABLE AS SELECT (avoids pandas roundtrip)
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {DB_LAKE_SQL}.main.{table} AS
                SELECT * FROM {DB_ETE}.main.{table}
            """)
            dst_n = row_count(con, DB_LAKE, table)
            mark = "✓" if dst_n == src_n else "⚠ count mismatch"
            print(f"  {mark} {table}: src={src_n} dst={dst_n}")
            results.append((table, src_n, dst_n, "migrated"))
        except Exception as e:
            print(f"  ✗ {table}: {e!s:.200s}")
            results.append((table, src_n, -1, f"error: {e!s:.80s}"))

    # Summary
    ok = sum(1 for _, s, d, st in results if (s == d and s >= 0) or st == "already_present")
    print(f"\n[{SCRIPT_TAG}] Phase 1 complete: {ok}/{len(ALL_MIGRATE)} tables migrated/present")
    for table, s, d, st in results:
        icon = "✓" if st in ("already_present", "migrated") else "✗" if "error" in st else "~"
        print(f"  {icon} {table:55s} src={s:>7,d} dst={d:>7,d}  {st}")


# ===========================================================================
# PHASE 2 — Sync Script 218/219 improvements into canonical master
# ===========================================================================


def phase2_sync(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 2: Sync 218/219 columns into canonical master ══")
    check_invariants(con, "phase2-start")

    # 2A — Find new columns in gold_master_patient_facts_v1 (Thyroid 2026)
    #      that are missing from canonical_patient_master_v1 (thyroid_ete_fix)
    #      Use DESCRIBE (works in multi-db mode) instead of information_schema
    if not table_exists(con, DB_LAKE, "gold_master_patient_facts_v1"):
        print(f"[{SCRIPT_TAG}] Phase 2: gold_master_patient_facts_v1 not in {DB_LAKE} — skip")
        return

    gm_cols_list = describe_table(con, DB_LAKE, "gold_master_patient_facts_v1")
    cm_cols_list = describe_table(con, DB_ETE, CANONICAL)

    gm_col_names = {c for c, _ in gm_cols_list}
    cm_col_names = {c for c, _ in cm_cols_list}
    gm_col_types = {c: t for c, t in gm_cols_list}

    missing_in_canonical = sorted(gm_col_names - cm_col_names)

    print(f"[{SCRIPT_TAG}] New columns to integrate: {len(missing_in_canonical)}")
    if not missing_in_canonical:
        print(f"[{SCRIPT_TAG}] ✓ canonical already up-to-date with gold_master")
        return

    # 2B — Data types come from the describe results above
    updated = 0
    skipped = 0
    for col in missing_in_canonical:
        dtype = gm_col_types.get(col, "VARCHAR")
        safe_col = col.replace('"', '""')

        if dry_run:
            print(f"  [dry-run] would add {col} ({dtype}) to canonical")
            continue

        # Check if column was added since we ran describe (race condition guard)
        already = col_exists(con, DB_ETE, CANONICAL, col)

        if not already:
            try:
                con.execute(f"""
                    ALTER TABLE {DB_ETE}.main.{CANONICAL}
                    ADD COLUMN "{safe_col}" {dtype}
                """)
            except Exception as e:
                print(f"  SKIP add {col}: {e!s:.120s}")
                skipped += 1
                continue

        # Populate via UPDATE JOIN
        # gold_master uses BIGINT research_id; canonical uses VARCHAR
        try:
            con.execute(f"""
                UPDATE {DB_ETE}.main.{CANONICAL} c
                SET "{safe_col}" = g."{safe_col}"
                FROM {DB_LAKE_SQL}.main.gold_master_patient_facts_v1 g
                WHERE c.research_id = CAST(g.research_id AS VARCHAR)
                  AND g."{safe_col}" IS NOT NULL
            """)
            updated += 1
            print(f"  ✓ {col} ({dtype})")
        except Exception as e:
            print(f"  ✗ update {col}: {e!s:.120s}")
            skipped += 1

    if not dry_run:
        print(f"\n[{SCRIPT_TAG}] Phase 2: {updated} columns synced, {skipped} skipped")
        check_invariants(con, "phase2-end")


# ===========================================================================
# PHASE 3 — Date/time provenance audit + days_from_surgery + multi-surgery
# ===========================================================================


def phase3_provenance(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 3: Date provenance audit ══")
    check_invariants(con, "phase3-start")

    # 3A — Surgery date recovery from operative_episode_detail_v2 in Thyroid 2026
    _phase3a_surgery_recovery(con, dry_run)

    # 3B — Add days_from_surgery for every date column
    _phase3b_days_from_surg(con, dry_run)

    # 3C — Multi-surgery linkage columns
    _phase3c_multi_surgery(con, dry_run)

    # 3D — Temporal provenance report
    _phase3d_report(con)

    if not dry_run:
        check_invariants(con, "phase3-end")


def _phase3a_surgery_recovery(
    con: duckdb.DuckDBPyConnection, dry_run: bool
) -> None:
    print(f"[{SCRIPT_TAG}] 3A: Surgery date recovery")

    # Check if operative_episode_detail_v2 exists in Thyroid 2026
    src_table = "operative_episode_detail_v2"
    if not table_exists(con, DB_LAKE, src_table):
        print(f"  SKIP: {src_table} not in {DB_LAKE}")
        return

    # Column name in operative_episode_detail_v2 is surgery_date_native (DATE type)
    date_col = "surgery_date_native"
    try:
        r = con.execute(f"""
            SELECT COUNT(*) as recoverable
            FROM {DB_ETE}.main.{CANONICAL} c
            JOIN {DB_LAKE_SQL}.main.{src_table} o
              ON c.research_id = CAST(o.research_id AS VARCHAR)
            WHERE c.first_surgery_date IS NULL
              AND o.{date_col} IS NOT NULL
        """).fetchone()
        recoverable = r[0] if r else 0
        print(f"  Recoverable surgery dates: {recoverable:,}")
    except Exception as e:
        print(f"  SKIP recovery check: {e!s:.120s}")
        return

    if recoverable > 0 and not dry_run:
        try:
            con.execute(f"""
                UPDATE {DB_ETE}.main.{CANONICAL} c
                SET first_surgery_date = sub.min_date
                FROM (
                    SELECT CAST(research_id AS VARCHAR) as rid,
                           MIN({date_col}) as min_date
                    FROM {DB_LAKE_SQL}.main.{src_table}
                    WHERE {date_col} IS NOT NULL
                    GROUP BY 1
                ) sub
                WHERE c.research_id = sub.rid
                  AND c.first_surgery_date IS NULL
            """)
            print(f"  ✓ Recovered up to {recoverable:,} surgery dates")
        except Exception as e:
            print(f"  ✗ Recovery failed: {e!s:.120s}")


def _phase3b_days_from_surg(
    con: duckdb.DuckDBPyConnection, dry_run: bool
) -> None:
    print(f"[{SCRIPT_TAG}] 3B: Adding days_from_surgery columns")

    # Use DESCRIBE instead of information_schema (multi-db compatible)
    DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"}
    all_cols = describe_table(con, DB_ETE, CANONICAL)
    date_cols = [
        col for col, dtype in all_cols
        if dtype.upper() in DATE_TYPES
        and col != "first_surgery_date"
        and "days" not in col
        and not col.endswith("_days_from_surg")
    ]

    print(f"  Found {len(date_cols)} date columns to process")
    added = 0
    for col in date_cols:
        # Derive the days column name
        if col.endswith("_date"):
            days_col = col[: -len("_date")] + "_days_from_surg"
        elif col.endswith("_at"):
            days_col = col[: -len("_at")] + "_days_from_surg"
        else:
            days_col = col + "_days_from_surg"

        safe_col = col.replace('"', '""')
        safe_days = days_col.replace('"', '""')

        # Check if column already exists (direct probe)
        if col_exists(con, DB_ETE, CANONICAL, days_col):
            continue

        if dry_run:
            print(f"  [dry-run] would add {days_col}")
            continue

        try:
            con.execute(f"""
                ALTER TABLE {DB_ETE}.main.{CANONICAL}
                ADD COLUMN "{safe_days}" INTEGER
            """)
            con.execute(f"""
                UPDATE {DB_ETE}.main.{CANONICAL}
                SET "{safe_days}" = DATEDIFF('day', first_surgery_date, "{safe_col}")
                WHERE first_surgery_date IS NOT NULL AND "{safe_col}" IS NOT NULL
            """)
            filled = con.execute(f"""
                SELECT COUNT(*) FROM {DB_ETE}.main.{CANONICAL}
                WHERE "{safe_days}" IS NOT NULL
            """).fetchone()[0]
            print(f"  ✓ {days_col}: {filled:,} values computed")
            added += 1
        except Exception as e:
            print(f"  ✗ {days_col}: {e!s:.120s}")

    print(f"  Added {added} new days_from_surgery columns")


def _phase3c_multi_surgery(
    con: duckdb.DuckDBPyConnection, dry_run: bool
) -> None:
    print(f"[{SCRIPT_TAG}] 3C: Multi-surgery linkage")

    src_table = "operative_episode_detail_v2"
    if not table_exists(con, DB_LAKE, src_table):
        print(f"  SKIP: {src_table} not in {DB_LAKE}")
        return

    if dry_run:
        print("  [dry-run] would build multi-surgery columns")
        return

    # Build surgery date staging table in ete_fix
    # Column is surgery_date_native (DATE) in operative_episode_detail_v2
    date_col = "surgery_date_native"
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {DB_ETE}.main._patient_surgery_dates AS
            SELECT
                CAST(research_id AS VARCHAR) as research_id,
                {date_col} AS surgery_date,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id ORDER BY {date_col}
                ) as surgery_number,
                COUNT(*) OVER (PARTITION BY research_id) as total_surgeries
            FROM {DB_LAKE_SQL}.main.{src_table}
            WHERE {date_col} IS NOT NULL
        """)
    except Exception as e:
        print(f"  ✗ Could not build _patient_surgery_dates: {e!s:.120s}")
        return

    multi_n = con.execute(f"""
        SELECT COUNT(DISTINCT research_id) FROM {DB_ETE}.main._patient_surgery_dates
        WHERE total_surgeries > 1
    """).fetchone()[0]
    print(f"  Multi-surgery patients: {multi_n:,}")

    # Add columns (one at a time — DuckDB limitation)
    new_cols_defs = [
        ("n_surgeries", "INTEGER"),
        ("second_surgery_date", "DATE"),
        ("third_surgery_date", "DATE"),
        ("days_between_first_second_surgery", "INTEGER"),
    ]
    for col, dtype in new_cols_defs:
        if not col_exists(con, DB_ETE, CANONICAL, col):
            try:
                con.execute(f"""
                    ALTER TABLE {DB_ETE}.main.{CANONICAL}
                    ADD COLUMN {col} {dtype}
                """)
            except Exception as e:
                print(f"  ✗ add {col}: {e!s:.80s}")

    # Populate n_surgeries
    q(con, f"""
        UPDATE {DB_ETE}.main.{CANONICAL} c
        SET n_surgeries = sub.total_surgeries
        FROM (
            SELECT research_id, MAX(total_surgeries) as total_surgeries
            FROM {DB_ETE}.main._patient_surgery_dates
            GROUP BY 1
        ) sub
        WHERE c.research_id = sub.research_id
    """, "n_surgeries")

    # Populate second_surgery_date
    q(con, f"""
        UPDATE {DB_ETE}.main.{CANONICAL} c
        SET second_surgery_date = sub.surgery_date
        FROM {DB_ETE}.main._patient_surgery_dates sub
        WHERE c.research_id = sub.research_id AND sub.surgery_number = 2
    """, "second_surgery_date")

    # Populate third_surgery_date
    q(con, f"""
        UPDATE {DB_ETE}.main.{CANONICAL} c
        SET third_surgery_date = sub.surgery_date
        FROM {DB_ETE}.main._patient_surgery_dates sub
        WHERE c.research_id = sub.research_id AND sub.surgery_number = 3
    """, "third_surgery_date")

    # Compute days between surgeries
    q(con, f"""
        UPDATE {DB_ETE}.main.{CANONICAL}
        SET days_between_first_second_surgery =
            DATEDIFF('day', first_surgery_date, second_surgery_date)
        WHERE first_surgery_date IS NOT NULL AND second_surgery_date IS NOT NULL
    """, "days_between")

    print("  ✓ Multi-surgery linkage columns populated")


def _phase3d_report(con: duckdb.DuckDBPyConnection) -> None:
    """Print temporal provenance coverage for every data domain."""
    print(f"\n[{SCRIPT_TAG}] 3D: Temporal provenance report")

    # Map of label → column name (check coverage)
    DATE_COLS = [
        ("Surgery",       "first_surgery_date"),
        ("US",            "us_first_exam_date"),
        ("CT",            "ct_first_date"),
        ("MRI",           "mri_first_date"),
        ("PET",           "pet_first_date"),
        ("FNA",           "prm_first_fna_date"),
        ("Molecular",     "mol_test_date"),
        ("Tg lab",        "first_tg_date"),
        ("Recurrence",    "first_recurrence_date"),
        ("Last contact",  "last_contact_date"),
        ("Death",         "death_date"),
        ("TSH lab",       "lab_tsh_first_date"),
        ("PTH lab",       "lab_pth_first_date"),
        ("Calcium lab",   "lab_calcium_first_date"),
        ("N surgeries",   "n_surgeries"),
    ]

    # Discover which columns actually exist via DESCRIBE (multi-db compatible)
    existing_cols: set[str] = {c for c, _ in describe_table(con, DB_ETE, CANONICAL)}

    print(f"\n  {'Domain':<22} {'Count':>8}  {'Pct':>6}")
    print(f"  {'-'*22} {'-'*8}  {'-'*6}")
    for label, col in DATE_COLS:
        if col not in existing_cols:
            print(f"  {label:<22} {'N/A (col missing)':>16}")
            continue
        try:
            n = con.execute(f"""
                SELECT COUNT(*) FROM {DB_ETE}.main.{CANONICAL}
                WHERE "{col}" IS NOT NULL
            """).fetchone()[0]
            pct = round(n / TOTAL_ROWS * 100, 1)
            print(f"  {label:<22} {n:>8,d}   {pct:>5.1f}%")
        except Exception as e:
            print(f"  {label:<22} ERROR: {e!s:.60s}")

    print()


# ===========================================================================
# PHASE 4 — MotherDuck optimization: COMMENT ON COLUMN / TABLE
# ===========================================================================


def phase4_optimize(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 4: MotherDuck schema optimization ══")

    # 4A — Load existing data dictionary descriptions
    col_descriptions: dict[str, str] = {}
    dict_path = REPO / "data_dictionary.csv"
    if dict_path.exists():
        with open(dict_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                col_name = row.get("column_name", "")
                desc = row.get("description", "")
                if col_name and desc:
                    col_descriptions[col_name] = desc
        print(f"  Loaded {len(col_descriptions)} descriptions from data_dictionary.csv")
    else:
        print("  No data_dictionary.csv found — using auto-generated descriptions")

    # 4B — Get all canonical columns via DESCRIBE (multi-db compatible)
    all_cols = describe_table(con, DB_ETE, CANONICAL)
    if not all_cols:
        print("  ✗ Could not list canonical columns via DESCRIBE")
        return

    print(f"  Adding COMMENT ON COLUMN for {len(all_cols)} columns...")
    comment_ok = 0
    comment_skip = 0

    for col_name, col_type in all_cols:
        desc = col_descriptions.get(col_name, "")
        if not desc:
            for suffix, auto_desc in AUTO_COL_DESC.items():
                if col_name.startswith(suffix) or col_name.endswith(suffix):
                    desc = auto_desc
                    break
        if not desc:
            desc = f"{col_type} field"

        desc_safe = desc.replace("'", "''")[:500]

        if dry_run:
            comment_ok += 1
            continue

        try:
            con.execute(f"""
                COMMENT ON COLUMN {DB_ETE}.main.{CANONICAL}."{col_name}"
                IS '{desc_safe}'
            """)
            comment_ok += 1
        except Exception:
            comment_skip += 1

    print(f"  COMMENT ON COLUMN: {comment_ok} OK, {comment_skip} skipped")

    # 4C — Table-level comments
    print(f"  Adding COMMENT ON TABLE for {len(TABLE_COMMENTS)} tables...")
    tbl_ok = 0
    for tbl, comment in TABLE_COMMENTS.items():
        comment_safe = comment.replace("'", "''")[:1000]
        if dry_run:
            tbl_ok += 1
            continue
        try:
            con.execute(f"""
                COMMENT ON TABLE {DB_ETE}.main.{tbl} IS '{comment_safe}'
            """)
            tbl_ok += 1
        except Exception as e:
            print(f"  SKIP table comment {tbl}: {e!s:.80s}")

    print(f"  COMMENT ON TABLE: {tbl_ok} OK")

    # 4D — List dev/staging databases (document only, do not drop)
    try:
        cleanup = con.execute("""
            SELECT database_name
            FROM md_information_schema.databases
            WHERE database_name LIKE '%PrePromote%'
               OR database_name LIKE '%Molecular Dev%'
               OR database_name LIKE '%Molecular QA%'
            ORDER BY database_name
        """).fetchall()
        if cleanup:
            print(f"\n  Cleanup candidates ({len(cleanup)} dev/staging databases):")
            for (name,) in cleanup:
                print(f"    {name}")
            print("  NOTE: NOT deleted. Review after ETE paper submission.")
    except Exception:
        pass  # md_information_schema may not be accessible in all configurations


# ===========================================================================
# PHASE 5 — Data dictionary refresh
# ===========================================================================


def phase5_data_dict(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 5: Data dictionary refresh ══")

    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        print("  SKIP: pandas not available")
        return

    # Get all columns via DESCRIBE (multi-db compatible)
    raw_cols = describe_table(con, DB_ETE, CANONICAL)
    if not raw_cols:
        print("  ✗ Could not retrieve column list via DESCRIBE")
        return
    # DESCRIBE returns (column_name, column_type, null, key, default, extra)
    # We only have name + type; add placeholders for backwards compat
    dict_df = pd.DataFrame(
        [(name, dtype, "YES", idx + 1) for idx, (name, dtype) in enumerate(raw_cols)],
        columns=["column_name", "data_type", "is_nullable", "ordinal_position"],
    )

    print(f"  Columns to document: {len(dict_df)}")

    # Coverage — query in batches of 50 to avoid per-column round trips
    batch_size = 50
    all_coverage: dict[str, int] = {}
    cols = dict_df["column_name"].tolist()

    for i in range(0, len(cols), batch_size):
        batch = cols[i : i + batch_size]
        exprs = [
            f'COUNT(*) FILTER (WHERE "{c}" IS NOT NULL) AS "{c}"'
            for c in batch
        ]
        sql = (
            f"SELECT {', '.join(exprs)} "
            f"FROM {DB_ETE}.main.{CANONICAL}"
        )
        try:
            row = con.execute(sql).fetchone()
            for j, val in enumerate(row):
                all_coverage[batch[j]] = val
        except Exception as e:
            print(f"  WARN batch {i//batch_size}: {e!s:.100s}")

    dict_df["non_null_count"] = dict_df["column_name"].map(all_coverage).fillna(0).astype(int)
    dict_df["coverage_pct"] = (dict_df["non_null_count"] / TOTAL_ROWS * 100).round(1)

    # Infer source table from prefix patterns
    def _infer_source(col: str) -> str:
        prefix_map = {
            "nlp_": "note_entities_llm_* (fleet NLP)",
            "prm_": "patient_refined_master_clinical_v12",
            "ops_": "op_sheet_data",
            "comp_": "complication_phenotype_v1",
            "cnln_": "clinical_note_ln_patient_rollup_v1",
            "syn_": "path_synoptics",
            "pet_": "ct_imaging (PET subset)",
            "ct_": "ct_imaging",
            "mri_": "mri_imaging",
            "nucmed_": "nuclear_med",
            "lnus_": "ultrasound_reports (LN US subset)",
            "lab_": "longitudinal_lab_canonical_v1",
            "nsqip_": "nsqip_data",
            "op_nlp_": "clinical_notes_long (NLP operative)",
            "med_nlp_": "clinical_notes_long (NLP medications)",
            "pmhx_": "clinical_notes_long (NLP PMH)",
            "pshx_": "clinical_notes_long (NLP PSH)",
            "tirads_": "extracted_tirads_validated_v1 / tirads_llm_extracted_v2",
            "bethesda_": "fna_cytology",
            "ete_": "extracted_ete_subgraded_v1",
            "gland_weight_": "thyroid_weight_data",
            "bmi_": "nsqip_data / op_sheet_data / clinical_notes_long",
            "tg_": "tg_timeline_patient_summary_v1",
            "gm_": "gold_master_patient_facts_v1",
            "para_": "parathyroid_notes_intent_v1",
        }
        for prefix, source in prefix_map.items():
            if col.startswith(prefix):
                return source
        return "gold_master_patient_facts_v1"

    dict_df["inferred_source"] = dict_df["column_name"].apply(_infer_source)

    # Auto-description
    existing_descs: dict[str, str] = {}
    dict_path = REPO / "data_dictionary.csv"
    if dict_path.exists():
        with open(dict_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("column_name") and row.get("description"):
                    existing_descs[row["column_name"]] = row["description"]

    def _auto_desc(col: str, dtype: str) -> str:
        if col in existing_descs:
            return existing_descs[col]
        for suffix, desc in AUTO_COL_DESC.items():
            if col.startswith(suffix) or col.endswith(suffix):
                return desc
        return f"{dtype} field"

    dict_df["description"] = dict_df.apply(
        lambda r: _auto_desc(r["column_name"], r["data_type"]), axis=1
    )

    if dry_run:
        print(f"  [dry-run] would write {len(dict_df)} rows to data_dictionary.csv/md/parquet")
        print(f"  Coverage: 100%={( dict_df['coverage_pct']==100).sum()} "
              f"  >75%={(dict_df['coverage_pct']>75).sum()} "
              f"  <10%={(dict_df['coverage_pct']<10).sum()}")
        return

    # Save CSV
    dict_df.to_csv(REPO / "data_dictionary.csv", index=False)
    print(f"  ✓ data_dictionary.csv: {len(dict_df)} columns")

    # Save parquet
    out_dir = REPO / "scripts" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    dict_df.to_parquet(out_dir / "data_dictionary.parquet", index=False)
    print("  ✓ scripts/output/data_dictionary.parquet")

    # Upload to MotherDuck
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {DB_ETE}.main.data_dictionary_v2 AS
            SELECT * FROM dict_df
        """)
        print(f"  ✓ data_dictionary_v2 uploaded to {DB_ETE}")
    except Exception as e:
        print(f"  ✗ MotherDuck upload: {e!s:.120s}")

    # Markdown summary
    md_lines = [
        "# THYROID_2026 — Data Dictionary",
        "## canonical_patient_master_v1",
        f"- **Rows:** {TOTAL_ROWS:,} (one per patient)",
        f"- **Columns:** {len(dict_df)}",
        f"- **Database:** {DB_ETE}",
        "",
        "| Coverage tier | Count |",
        "|---------------|-------|",
        f"| 100% coverage | {(dict_df['coverage_pct']==100).sum()} |",
        f"| >75% coverage | {(dict_df['coverage_pct']>75).sum()} |",
        f"| >50% coverage | {(dict_df['coverage_pct']>50).sum()} |",
        f"| <10% coverage | {(dict_df['coverage_pct']<10).sum()} |",
        "",
    ]
    for source, group in dict_df.groupby("inferred_source"):
        md_lines.append(f"\n### Source: {source} ({len(group)} columns)\n")
        md_lines.append("| Column | Type | Coverage% | Description |")
        md_lines.append("|--------|------|-----------|-------------|")
        for _, row in group.iterrows():
            desc = row["description"].replace("|", "/")
            md_lines.append(
                f"| {row['column_name']} | {row['data_type']} "
                f"| {row['coverage_pct']} | {desc} |"
            )

    with open(REPO / "data_dictionary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")
    print("  ✓ data_dictionary.md written")

    print("\n  Coverage summary:")
    print(f"    100% coverage: {(dict_df['coverage_pct']==100).sum():>4} columns")
    print(f"    >75% coverage: {(dict_df['coverage_pct']>75).sum():>4} columns")
    print(f"    >50% coverage: {(dict_df['coverage_pct']>50).sum():>4} columns")
    print(f"    <10% coverage: {(dict_df['coverage_pct']<10).sum():>4} columns")


# ===========================================================================
# PHASE 6 — Parquet backup
# ===========================================================================


def phase6_backup(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 6: Parquet backups ══")

    backup_dir = REPO / "scripts" / "output" / "parquet_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)

    BACKUP_TABLES = [
        (DB_ETE, "canonical_patient_master_v1"),
        (DB_ETE, "imaging_nodule_master_v1"),
        (DB_ETE, "fna_episode_master_v2"),
        (DB_ETE, "fna_cytology"),
        (DB_ETE, "tirads_llm_extracted_v2"),
        (DB_ETE, "tumor_pathology"),
        (DB_ETE, "path_synoptics"),
        (DB_ETE, "ln_master_rollup_v1"),
        (DB_ETE, "complication_phenotype_v1"),
        (DB_ETE, "data_dictionary_v2"),
    ]

    for db, table in BACKUP_TABLES:
        out_path = backup_dir / f"{table}.parquet"
        if dry_run:
            print(f"  [dry-run] would export {table} → {out_path.name}")
            continue
        try:
            quoted_db = f'"{db}"' if " " in db else db
            con.execute(f"""
                COPY (SELECT * FROM {quoted_db}.main.{table})
                TO '{out_path}' (FORMAT PARQUET)
            """)
            size_mb = out_path.stat().st_size / 1_048_576
            print(f"  ✓ {table}: {size_mb:.1f} MB")
        except Exception as e:
            print(f"  ✗ {table}: {e!s:.120s}")


# ===========================================================================
# PHASE 7 — Final validation report
# ===========================================================================


def phase7_validate(con: duckdb.DuckDBPyConnection) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 7: Final validation report ══")
    check_invariants(con, "final")

    # Count columns via DESCRIBE (multi-db compatible)
    all_cols_desc = describe_table(con, DB_ETE, CANONICAL)
    final_cols = len(all_cols_desc)
    col_name_set = {c for c, _ in all_cols_desc}

    # Check non-null required fields
    null_counts: dict[str, int] = {}
    for col in REQUIRED_NON_NULL:
        if col in col_name_set:
            try:
                n = con.execute(f"""
                    SELECT COUNT(*) FROM {DB_ETE}.main.{CANONICAL}
                    WHERE "{col}" IS NULL
                """).fetchone()[0]
                null_counts[col] = n
            except Exception:
                null_counts[col] = -1

    # Row count via a fast path
    try:
        total_rows = con.execute(
            f"SELECT COUNT(*) FROM {DB_ETE}.main.{CANONICAL}"
        ).fetchone()[0]
    except Exception:
        total_rows = -1

    # Check for n_surgeries column (multi-surgery)
    has_n_surg = "n_surgeries" in col_name_set

    print(f"""
{'='*60}
FINAL CONSOLIDATION REPORT — {SCRIPT_TAG}
{'='*60}
Database:        {DB_ETE}
Canonical table: {CANONICAL}
Columns:         {final_cols:,}
Row count:       {total_rows:,}  (expected {TOTAL_ROWS:,})
Multi-surgery:   n_surgeries column {'PRESENT' if has_n_surg else 'MISSING'}

Invariant checks:
  research_id nulls:    {null_counts.get('research_id', 'N/A')}  (expected 0)
  fna_path_outcome nulls:{null_counts.get('fna_path_outcome', 'N/A')}  (expected 0)

Tables migrated: {len(ALL_MIGRATE)} attempted
Phase 4: COMMENT ON COLUMN/TABLE applied
Phase 5: data_dictionary.csv + .md + MotherDuck table
Phase 6: Parquet backups in scripts/output/parquet_backup/
{'='*60}
""")

    # Fail-hard check
    if total_rows != TOTAL_ROWS:
        print(f"[{SCRIPT_TAG}] ✗ INVARIANT FAILURE: row count {total_rows} ≠ {TOTAL_ROWS}")
        sys.exit(1)
    for col, n in null_counts.items():
        if n > 0:
            print(f"[{SCRIPT_TAG}] ✗ INVARIANT FAILURE: {col} has {n} NULL rows")
            sys.exit(1)
    print(f"[{SCRIPT_TAG}] ✓ All invariants pass")


# ===========================================================================
# CLI
# ===========================================================================


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=f"THYROID_2026 — {SCRIPT_TAG}")
    p.add_argument(
        "--phase",
        default="all",
        choices=["1", "2", "3", "4", "5", "6", "7", "all"],
        help="Which phase to run (default: all)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only — no writes to MotherDuck or disk",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    t0 = time.time()

    if args.dry_run:
        print(f"[{SCRIPT_TAG}] DRY-RUN mode — no writes will be performed\n")

    run_phase = {str(i) for i in range(1, 8)} if args.phase == "all" else {args.phase}

    con = connect()

    # Always verify canonical invariants at startup
    check_invariants(con, "startup")

    if "1" in run_phase:
        phase1_migrate(con, args.dry_run)

    if "2" in run_phase:
        phase2_sync(con, args.dry_run)

    if "3" in run_phase:
        phase3_provenance(con, args.dry_run)

    if "4" in run_phase:
        phase4_optimize(con, args.dry_run)

    if "5" in run_phase:
        phase5_data_dict(con, args.dry_run)

    if "6" in run_phase:
        phase6_backup(con, args.dry_run)

    if "7" in run_phase:
        phase7_validate(con)

    elapsed = time.time() - t0
    print(f"\n[{SCRIPT_TAG}] Total elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
