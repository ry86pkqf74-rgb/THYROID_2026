#!/usr/bin/env python3
"""
THYROID_2026 — Script 218: Follow-Up Recovery, Nuclear Med Labs, Tg Gap Fix,
                           Recurrence Sites + Cross-Validation

Database: thyroid_ete_fix_20260413

Tasks:
  1: Follow-up time recovery from 20+ source tables (affects 67% of patients)
  2: Nuclear med TSH/Tg parsing from lab_summary field
  3: Tg coverage gap fix (193 patients, BIGINT/VARCHAR join bug)
  4: Recurrence site recovery from FNA specimen_location
  5: Complication cross-validation (OP Sheet vs phenotype — QC report only)
  6: Canonical rebuild + final validation

Run:
  .venv/bin/python scripts/218_followup_recovery.py [--dry-run] [--phase 1|2|3|4|5|6|all]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

# Retargeted by Script 233 (2026-04-16) from the stale thyroid_ete_fix_20260413
# to the canonical publication DB so rebuilds always land in the clean master.
DB = "thyroid_canonical_publication_v1_0"
CANONICAL = "canonical_patient_master"
TOTAL_ROWS = 10871

# Overridden by --db / --canonical / --token-from-toml CLI args at runtime


# ======================================================================
# Connection + utilities
# ======================================================================

def connect(db: str | None = None, token: str | None = None) -> duckdb.DuckDBPyConnection:
    if token is None:
        token = get_token()
    if not token:
        print("[218] ERROR: No MotherDuck token found.")
        sys.exit(1)
    target = db or DB
    # Databases with spaces need the USE pattern
    if " " in target:
        con = duckdb.connect(f"md:?motherduck_token={token}")
        con.execute(f'USE "{target}"')
        return con
    return duckdb.connect(f"md:{target}?motherduck_token={token}")


def check_invariants(
    con: duckdb.DuckDBPyConnection, table: str, label: str
) -> bool:
    inv = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT research_id) AS distinct_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM {table}
    """).fetchone()
    print(f"[218] {label}: {inv[0]} rows, {inv[1]} distinct RIDs, "
          f"{inv[2]} null RIDs, {inv[3]} null fna_path_outcome")
    errors = []
    if inv[0] != TOTAL_ROWS:
        errors.append(f"Row count {inv[0]} != {TOTAL_ROWS}")
    if inv[0] != inv[1]:
        errors.append(f"Duplicate research_ids: {inv[0] - inv[1]}")
    if inv[2] > 0:
        errors.append(f"NULL research_ids: {inv[2]}")
    for e in errors:
        print(f"[218] ERROR: {e}")
    return len(errors) == 0


def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


def safe_add_column(
    con: duckdb.DuckDBPyConnection, col: str, dtype: str
) -> None:
    try:
        con.execute(f'ALTER TABLE {CANONICAL} ADD COLUMN "{col}" {dtype}')
    except Exception:
        pass


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    r = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = '{name}' AND table_schema = 'main'
    """).fetchone()
    return r[0] > 0


def resolve_col(
    con: duckdb.DuckDBPyConnection, preferred: str, *fallbacks: str
) -> str:
    """Return the first column name that exists in CANONICAL, else preferred (to be added)."""
    existing = get_existing_columns(con)
    for col in (preferred, *fallbacks):
        if col in existing:
            return col
    return preferred  # will be added by ALTER TABLE


def build_col_map(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Detect column name variants between old canonical and new canonical schemas."""
    return {
        # Recurrence columns
        "recurrence_confirmed": resolve_col(
            con, "recurrence_confirmed", "any_recurrence_flag"
        ),
        "recurrence_site": resolve_col(
            con, "recurrence_site", "recurrence_site_primary"
        ),
        # Follow-up columns (may not exist yet — will be added)
        "last_contact_date": resolve_col(con, "last_contact_date"),
        "followup_years": resolve_col(con, "followup_years"),
        "followup_days": resolve_col(con, "followup_days"),
        "followup_category": resolve_col(con, "followup_category"),
        "last_contact_source": resolve_col(con, "last_contact_source"),
        # RAI / lab columns
        "rai_stimulated_tsh": resolve_col(con, "rai_stimulated_tsh"),
        "rai_stimulated_tg": resolve_col(con, "rai_stimulated_tg"),
    }


# ======================================================================
# TASK 1: FOLLOW-UP TIME RECOVERY
# ======================================================================

FOLLOWUP_SOURCES_SQL = """
CREATE OR REPLACE TABLE _followup_all_dates_v2 AS

-- Structured imaging
SELECT CAST(research_id AS VARCHAR) AS research_id,
    MAX(TRY_CAST(date_of_exam AS DATE)) AS max_date, 'ct_imaging' AS source_table
FROM ct_imaging WHERE date_of_exam IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(date_of_exam AS DATE)), 'mri_imaging'
FROM mri_imaging WHERE date_of_exam IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(ultrasound_date AS DATE)), 'ultrasound_reports'
FROM raw.ultrasound_reports WHERE ultrasound_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(scandate AS DATE)), 'nuclear_med'
FROM nuclear_med WHERE scandate IS NOT NULL GROUP BY 1

-- Labs
UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(lab_date AS DATE)), 'longitudinal_lab'
FROM longitudinal_lab_canonical_v1 WHERE lab_date IS NOT NULL GROUP BY 1

-- FNA
UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(fna_date AS DATE)), 'fna_cytology'
FROM fna_cytology WHERE fna_date IS NOT NULL GROUP BY 1

-- RAI episodes
UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(resolved_rai_date AS DATE)), 'rai_episodes'
FROM rai_treatment_episode_v2 WHERE resolved_rai_date IS NOT NULL GROUP BY 1

-- OP Sheet
UNION ALL
SELECT research_id,
    MAX(TRY_CAST(ops_surg_date AS DATE)), 'op_sheet'
FROM op_sheet_data WHERE ops_surg_date IS NOT NULL GROUP BY 1

-- NLP entity tables (note_date = dated clinical encounter)
UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_cervical_ln'
FROM note_entities_llm_cervical_ln_detail WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_tirads'
FROM note_entities_llm_tirads_granular WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_pathology'
FROM note_entities_llm_pathology WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_survival_followup'
FROM note_entities_llm_survival_followup WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_recurrence'
FROM note_entities_llm_recurrence WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_labs'
FROM note_entities_llm_labs WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_imaging'
FROM note_entities_llm_imaging WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_rai_detailed'
FROM note_entities_llm_rai_detailed WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_pmhx'
FROM note_entities_llm_past_medical_hx WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'nlp_pshx'
FROM note_entities_llm_past_surgical_hx WHERE note_date IS NOT NULL GROUP BY 1

UNION ALL
SELECT CAST(research_id AS VARCHAR),
    MAX(TRY_CAST(note_date AS DATE)), 'clinical_note_ln'
FROM clinical_note_ln_extracted_v1 WHERE note_date IS NOT NULL GROUP BY 1
"""

FOLLOWUP_ROLLUP_SQL = """
CREATE OR REPLACE TABLE _followup_patient_max_v2 AS
WITH ranked AS (
    SELECT
        research_id,
        max_date,
        source_table,
        ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY max_date DESC) AS rn
    FROM _followup_all_dates_v2
    WHERE max_date IS NOT NULL
      AND max_date > DATE '1990-01-01'
      AND max_date <= CURRENT_DATE
)
SELECT
    r.research_id,
    r.max_date AS last_contact_date_v2,
    r.source_table AS last_contact_source_v2,
    agg.all_contact_sources,
    agg.n_contact_sources
FROM ranked r
JOIN (
    SELECT
        research_id,
        STRING_AGG(DISTINCT source_table, '; ' ORDER BY source_table) AS all_contact_sources,
        COUNT(DISTINCT source_table) AS n_contact_sources
    FROM _followup_all_dates_v2
    WHERE max_date IS NOT NULL
      AND max_date > DATE '1990-01-01'
      AND max_date <= CURRENT_DATE
    GROUP BY research_id
) agg ON r.research_id = agg.research_id
WHERE r.rn = 1
"""


def task1_followup_recovery(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 1: FOLLOW-UP TIME RECOVERY")
    print("=" * 70)

    results: dict[str, Any] = {}

    # Step 1.1: Build comprehensive last-contact-date table
    print("\n  Step 1.1: Collecting dates from all source tables...")
    if not dry_run:
        # Build incrementally — skip tables that don't exist
        source_sqls = []
        source_table_checks = [
            ("ct_imaging", "date_of_exam", "CAST(research_id AS VARCHAR)"),
            ("mri_imaging", "date_of_exam", "CAST(research_id AS VARCHAR)"),
            ("ultrasound_reports", "ultrasound_date", "CAST(research_id AS VARCHAR)"),
            ("nuclear_med", "scandate", "CAST(research_id AS VARCHAR)"),
            ("longitudinal_lab_canonical_v1", "lab_date", "CAST(research_id AS VARCHAR)"),
            ("fna_cytology", "fna_date", "CAST(research_id AS VARCHAR)"),
            ("rai_treatment_episode_v2", "resolved_rai_date", "CAST(research_id AS VARCHAR)"),
            ("op_sheet_data", "ops_surg_date", "research_id"),
            ("note_entities_llm_cervical_ln_detail", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_tirads_granular", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_pathology", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_survival_followup", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_recurrence", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_labs", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_imaging", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_rai_detailed", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_past_medical_hx", "note_date", "CAST(research_id AS VARCHAR)"),
            ("note_entities_llm_past_surgical_hx", "note_date", "CAST(research_id AS VARCHAR)"),
            ("clinical_note_ln_extracted_v1", "note_date", "CAST(research_id AS VARCHAR)"),
            # Sources added by Script 233 (2026-04-16) during follow-up recovery.
            # Keeping them in 218 guarantees the next rebuild produces the same
            # last-contact coverage as the in-place 233 fix.
            ("tg_postop_surveillance_windows_v1", "window_last_date", "CAST(research_id AS VARCHAR)"),
        ]

        for tbl, date_col, rid_expr in source_table_checks:
            if table_exists(con, tbl):
                label = tbl.replace("note_entities_llm_", "nlp_")
                sql = (
                    f"SELECT {rid_expr} AS research_id, "
                    f"MAX(TRY_CAST({date_col} AS DATE)) AS max_date, "
                    f"'{label}' AS source_table "
                    f"FROM {tbl} WHERE {date_col} IS NOT NULL GROUP BY 1"
                )
                source_sqls.append(sql)
                print(f"    + {tbl}")
            else:
                print(f"    - {tbl} (not found, skipping)")

        if not source_sqls:
            print("  ERROR: No source tables found!")
            return results

        union_sql = "\nUNION ALL\n".join(source_sqls)
        con.execute(f"CREATE OR REPLACE TABLE _followup_all_dates_v2 AS\n{union_sql}")

        n_rows = con.execute("SELECT COUNT(*) FROM _followup_all_dates_v2").fetchone()[0]
        n_sources = con.execute(
            "SELECT COUNT(DISTINCT source_table) FROM _followup_all_dates_v2"
        ).fetchone()[0]
        n_patients = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM _followup_all_dates_v2"
        ).fetchone()[0]
        print(f"  Built _followup_all_dates_v2: {n_rows:,} rows, "
              f"{n_sources} sources, {n_patients:,} patients")
        results["all_dates_rows"] = n_rows
        results["n_sources"] = n_sources
        results["n_patients_with_dates"] = n_patients

        # Per-source breakdown
        print("\n  Source breakdown:")
        breakdown = con.execute("""
            SELECT source_table,
                   COUNT(DISTINCT research_id) AS n_pts,
                   MIN(max_date) AS earliest,
                   MAX(max_date) AS latest
            FROM _followup_all_dates_v2
            WHERE max_date IS NOT NULL
            GROUP BY 1
            ORDER BY n_pts DESC
        """).fetchall()
        for src, n, earliest, latest in breakdown:
            print(f"    {src:40s} {n:>6,} pts  ({earliest} → {latest})")

    else:
        print("  [DRY RUN] Would collect dates from 19+ source tables")
        return results

    # Step 1.2: Roll up to one row per patient
    print("\n  Step 1.2: Rolling up to patient-level max date...")
    con.execute(FOLLOWUP_ROLLUP_SQL)
    n_rollup = con.execute("SELECT COUNT(*) FROM _followup_patient_max_v2").fetchone()[0]
    print(f"  _followup_patient_max_v2: {n_rollup:,} patients")
    results["rollup_patients"] = n_rollup

    # Top sources
    top = con.execute("""
        SELECT last_contact_source_v2, COUNT(*) AS n
        FROM _followup_patient_max_v2
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    print("\n  Top last-contact sources:")
    for src, n in top:
        print(f"    {src:40s} {n:>6,}")

    # Step 1.3: Cross-validate against existing follow-up (only if columns exist)
    print("\n  Step 1.3: Cross-validation against existing follow-up...")
    existing_now = get_existing_columns(con)
    fu_cols_exist = "followup_years" in existing_now and "last_contact_date" in existing_now
    if fu_cols_exist:
        concordance = con.execute(f"""
            SELECT
                CASE
                    WHEN ABS(DATEDIFF('day', c.last_contact_date, f.last_contact_date_v2)) <= 30
                        THEN 'concordant_30d'
                    WHEN f.last_contact_date_v2 > c.last_contact_date THEN 'new_is_later'
                    WHEN f.last_contact_date_v2 < c.last_contact_date THEN 'new_is_earlier'
                    ELSE 'other'
                END AS comparison,
                COUNT(*) AS n
            FROM {CANONICAL} c
            JOIN _followup_patient_max_v2 f ON CAST(c.research_id AS VARCHAR) = f.research_id
            WHERE c.followup_years > 0 AND c.last_contact_date IS NOT NULL
            GROUP BY 1 ORDER BY n DESC
        """).fetchall()
        print("  Concordance (patients WITH existing follow-up):")
        total_compared = 0
        new_earlier_count = 0
        for comp, n in concordance:
            print(f"    {comp:25s} {n:>6,}")
            total_compared += n
            if comp == "new_is_earlier":
                new_earlier_count = n
        if total_compared > 0 and new_earlier_count / total_compared > 0.05:
            print(f"  WARNING: {new_earlier_count / total_compared:.1%} of cases have "
                  f"new date EARLIER than old — investigating")
        results["concordance"] = dict(concordance)
    else:
        print("  Skipped — follow-up columns not yet in canonical (will be added in Task 6)")

    # Recovery stats for patients with zero / no follow-up
    recovery_sql = f"""
        SELECT
            {'c.last_contact_source AS old_source'
             if 'last_contact_source' in existing_now else "'no_prior_source' AS old_source"},
            CASE WHEN f.last_contact_date_v2 IS NOT NULL THEN 'recovered' ELSE 'still_no_date' END AS status,
            COUNT(*) AS n
        FROM {CANONICAL} c
        LEFT JOIN _followup_patient_max_v2 f ON CAST(c.research_id AS VARCHAR) = f.research_id
        {'WHERE c.followup_years = 0 OR c.followup_years IS NULL'
         if 'followup_years' in existing_now else 'WHERE f.last_contact_date_v2 IS NULL OR 1=1'}
        GROUP BY 1, 2 ORDER BY 1, 2
    """
    recovery = con.execute(recovery_sql).fetchall()
    print("\n  Recovery for zero-followup patients:")
    total_recovered = 0
    total_still_missing = 0
    for old_src, status, n in recovery:
        print(f"    {str(old_src):25s} {status:15s} {n:>6,}")
        if status == "recovered":
            total_recovered += n
        else:
            total_still_missing += n
    print(f"  TOTAL recovered: {total_recovered:,}, still missing: {total_still_missing:,}")
    results["recovered"] = total_recovered
    results["still_missing"] = total_still_missing

    # Step 1.4: Compute new follow-up years
    print("\n  Step 1.4: Computing new follow-up years...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _followup_computed_v2 AS
        SELECT
            f.research_id,
            f.last_contact_date_v2,
            f.last_contact_source_v2,
            f.all_contact_sources,
            f.n_contact_sources,
            c.first_surgery_date,
            ROUND(DATEDIFF('day', c.first_surgery_date, f.last_contact_date_v2) / 365.25, 2)
                AS followup_years_v2,
            DATEDIFF('day', c.first_surgery_date, f.last_contact_date_v2)
                AS followup_days_v2,
            CASE
                WHEN f.last_contact_date_v2 IS NULL THEN 'no_contact'
                WHEN DATEDIFF('day', c.first_surgery_date, f.last_contact_date_v2) < 0
                    THEN 'pre_surgery_only'
                WHEN DATEDIFF('day', c.first_surgery_date, f.last_contact_date_v2) < 365
                    THEN 'short_term'
                WHEN DATEDIFF('day', c.first_surgery_date, f.last_contact_date_v2) < 1825
                    THEN 'medium_term'
                ELSE 'long_term'
            END AS followup_category_v2
        FROM _followup_patient_max_v2 f
        JOIN {CANONICAL} c ON f.research_id = c.research_id
        WHERE f.last_contact_date_v2 IS NOT NULL
    """)

    # Sanity checks
    neg = con.execute(
        "SELECT COUNT(*) FROM _followup_computed_v2 WHERE followup_years_v2 < 0"
    ).fetchone()[0]
    over30 = con.execute(
        "SELECT COUNT(*) FROM _followup_computed_v2 WHERE followup_years_v2 > 30"
    ).fetchone()[0]
    print(f"  Sanity: {neg} negative follow-up (pre-surgery only), {over30} > 30 years")
    if neg > 0:
        print("  NOTE: Negative follow-up patients will NOT update canonical — "
              "last contact predates surgery")
    if over30 > 0:
        print(f"  WARNING: {over30} patients show > 30 years follow-up — capping at 30")

    # Category distribution
    cats = con.execute("""
        SELECT followup_category_v2, COUNT(*) AS n,
               ROUND(AVG(followup_years_v2), 2) AS mean_fu,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY followup_years_v2), 2) AS median_fu
        FROM _followup_computed_v2
        WHERE followup_years_v2 >= 0
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print("\n  Follow-up category distribution (non-negative only):")
    for cat, n, mean_fu, median_fu in cats:
        print(f"    {cat:20s} {n:>6,}  mean={mean_fu:.2f}y  median={median_fu:.2f}y")
    results["followup_categories"] = {c: n for c, n, *_ in cats}

    overall_median = con.execute("""
        SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY followup_years_v2), 2)
        FROM _followup_computed_v2 WHERE followup_years_v2 > 0
    """).fetchone()[0]
    print(f"\n  Overall median follow-up (positive only): {overall_median} years")
    results["median_followup_v2"] = overall_median

    return results


# ======================================================================
# TASK 2: NUCLEAR MED TSH / Tg PARSING
# ======================================================================

NUCMED_PARSE_SQL = """
CREATE OR REPLACE TABLE _nucmed_labs_parsed_v1 AS
SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    scan_index,
    scandate,
    scantype,
    radiotracer,
    CAST(lab_summary AS VARCHAR) AS lab_summary_raw,

    TRY_CAST(
        REGEXP_EXTRACT(LOWER(CAST(lab_summary AS VARCHAR)),
            'tsh:\\s*([0-9]+\\.?[0-9]*)', 1)
    AS DOUBLE) AS nucmed_tsh_value,

    TRY_CAST(
        REGEXP_EXTRACT(LOWER(CAST(lab_summary AS VARCHAR)),
            'thyroglobulin:\\s*([0-9]+\\.?[0-9]*)', 1)
    AS DOUBLE) AS nucmed_tg_value,

    TRY_CAST(
        REGEXP_EXTRACT(LOWER(CAST(lab_summary AS VARCHAR)),
            '(?:tg antibod|anti.?tg|tgab)(?:y)?:?\\s*([0-9]+\\.?[0-9]*)', 1)
    AS DOUBLE) AS nucmed_tgab_value,

    CASE
        WHEN LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%mciu%' THEN 'mIU/mL'
        WHEN LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%uiu%' THEN 'uIU/mL'
        ELSE 'mIU/mL'
    END AS tsh_unit,

    CASE
        WHEN LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%ngml%'
          OR LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%ng/ml%' THEN 'ng/mL'
        ELSE 'ng/mL'
    END AS tg_unit,

    CASE
        WHEN TRY_CAST(
            REGEXP_EXTRACT(LOWER(CAST(lab_summary AS VARCHAR)),
                'tsh:\\s*([0-9]+\\.?[0-9]*)', 1) AS DOUBLE) > 30
        THEN TRUE ELSE FALSE
    END AS tsh_is_stimulated,

    'nuclear_med.lab_summary' AS source_field,
    '218_nucmed_lab_parsing' AS parse_script

FROM nuclear_med
WHERE CAST(lab_summary AS VARCHAR) IS NOT NULL
  AND LENGTH(CAST(lab_summary AS VARCHAR)) > 5
  AND (LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%tsh%'
    OR LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%thyroglobulin%')
"""


def task2_nucmed_labs(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 2: NUCLEAR MED TSH / Tg PARSING")
    print("=" * 70)

    results: dict[str, Any] = {}

    if not table_exists(con, "nuclear_med"):
        print("  ERROR: nuclear_med table not found")
        return results

    # Check available lab_summary data
    pre = con.execute("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE CAST(lab_summary AS VARCHAR) IS NOT NULL
                   AND LENGTH(CAST(lab_summary AS VARCHAR)) > 5) AS has_summary,
               COUNT(*) FILTER (WHERE LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%tsh%') AS has_tsh,
               COUNT(*) FILTER (WHERE LOWER(CAST(lab_summary AS VARCHAR)) LIKE '%thyroglobulin%') AS has_tg
        FROM nuclear_med
    """).fetchone()
    print(f"  nuclear_med: {pre[0]:,} total rows, {pre[1]:,} with lab_summary, "
          f"{pre[2]:,} mentioning TSH, {pre[3]:,} mentioning Tg")
    results["nm_total"] = pre[0]
    results["nm_with_summary"] = pre[1]

    if dry_run:
        print("  [DRY RUN] Would parse TSH/Tg from lab_summary")
        return results

    # Step 2.1: Parse
    print("\n  Step 2.1: Parsing lab_summary values...")
    con.execute(NUCMED_PARSE_SQL)

    # Step 2.2: Validate
    print("\n  Step 2.2: Validating parsed values...")
    val = con.execute("""
        SELECT
            COUNT(*) AS total_parsed,
            COUNT(*) FILTER (WHERE nucmed_tsh_value IS NOT NULL) AS has_tsh,
            COUNT(*) FILTER (WHERE nucmed_tg_value IS NOT NULL) AS has_tg,
            COUNT(*) FILTER (WHERE nucmed_tgab_value IS NOT NULL) AS has_tgab,
            COUNT(*) FILTER (WHERE nucmed_tsh_value < 0 OR nucmed_tsh_value > 500)
                AS tsh_out_of_range,
            COUNT(*) FILTER (WHERE nucmed_tg_value < 0 OR nucmed_tg_value > 100000)
                AS tg_out_of_range,
            COUNT(*) FILTER (WHERE tsh_is_stimulated = TRUE) AS stimulated_tsh_count,
            COUNT(DISTINCT research_id) FILTER (WHERE nucmed_tsh_value IS NOT NULL)
                AS tsh_patients,
            COUNT(DISTINCT research_id) FILTER (WHERE nucmed_tg_value IS NOT NULL)
                AS tg_patients,
            ROUND(AVG(nucmed_tsh_value) FILTER (WHERE tsh_is_stimulated = TRUE), 1)
                AS mean_stim_tsh,
            ROUND(AVG(nucmed_tg_value) FILTER (WHERE nucmed_tg_value IS NOT NULL), 2)
                AS mean_tg
        FROM _nucmed_labs_parsed_v1
    """).fetchone()

    print(f"  Parsed rows: {val[0]:,}")
    print(f"  TSH values: {val[1]:,} ({val[7]:,} patients)")
    print(f"  Tg values:  {val[2]:,} ({val[8]:,} patients)")
    print(f"  TgAb values: {val[3]:,}")
    print(f"  TSH out of range (>500 or <0): {val[4]}")
    print(f"  Tg out of range: {val[5]}")
    print(f"  Stimulated TSH (>30): {val[6]:,}, mean={val[9]}")
    print(f"  Mean Tg: {val[10]}")
    results["tsh_patients"] = val[7]
    results["tg_patients"] = val[8]
    results["tsh_out_of_range"] = val[4]
    results["tg_out_of_range"] = val[5]
    results["stimulated_count"] = val[6]

    if val[4] > 0 or val[5] > 0:
        print("  Flagging out-of-range values (will NOT integrate)...")
        con.execute("""
            DELETE FROM _nucmed_labs_parsed_v1
            WHERE (nucmed_tsh_value IS NOT NULL AND (nucmed_tsh_value < 0 OR nucmed_tsh_value > 500))
               OR (nucmed_tg_value IS NOT NULL AND (nucmed_tg_value < 0 OR nucmed_tg_value > 100000))
        """)

    # Step 2.3: Cross-validate against existing Tg
    print("\n  Step 2.3: Cross-validating against longitudinal Tg...")
    if table_exists(con, "longitudinal_lab_canonical_v1"):
        xval = con.execute("""
            SELECT COUNT(DISTINCT nm.research_id) AS overlap_patients
            FROM (
                SELECT research_id FROM _nucmed_labs_parsed_v1
                WHERE nucmed_tg_value IS NOT NULL
            ) nm
            JOIN (
                SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
                FROM longitudinal_lab_canonical_v1
                WHERE LOWER(lab_name_standardized) = 'thyroglobulin'
                  AND value_numeric IS NOT NULL
            ) tl ON nm.research_id = tl.research_id
        """).fetchone()
        print(f"  Overlap patients (have both nucmed Tg and longitudinal Tg): {xval[0]:,}")
        results["tg_overlap"] = xval[0]

    # Step 2.4: Roll up to patient level
    print("\n  Step 2.4: Rolling up to patient level...")
    con.execute("""
        CREATE OR REPLACE TABLE _nucmed_labs_rollup_v1 AS
        WITH per_scan AS (
            SELECT
                research_id,
                nucmed_tsh_value,
                nucmed_tg_value,
                nucmed_tgab_value,
                tsh_is_stimulated,
                scandate,
                ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY scandate DESC) AS rn
            FROM _nucmed_labs_parsed_v1
            WHERE nucmed_tsh_value IS NOT NULL OR nucmed_tg_value IS NOT NULL
        )
        SELECT
            research_id,
            MAX(nucmed_tsh_value) FILTER (WHERE tsh_is_stimulated = TRUE)
                AS rai_stimulated_tsh_v2,
            MAX(nucmed_tg_value) AS nucmed_tg_max,
            MIN(nucmed_tg_value) AS nucmed_tg_min,
            COUNT(*) FILTER (WHERE nucmed_tsh_value IS NOT NULL) AS nucmed_n_tsh_values,
            COUNT(*) FILTER (WHERE nucmed_tg_value IS NOT NULL) AS nucmed_n_tg_values,
            COUNT(*) FILTER (WHERE nucmed_tgab_value IS NOT NULL) AS nucmed_n_tgab_values,
            MIN(scandate) AS nucmed_first_scan_with_labs,
            MAX(scandate) AS nucmed_last_scan_with_labs,
            'nuclear_med.lab_summary' AS nucmed_lab_source
        FROM per_scan
        GROUP BY research_id
    """)

    rollup = con.execute("""
        SELECT COUNT(*) AS n,
               COUNT(*) FILTER (WHERE rai_stimulated_tsh_v2 IS NOT NULL) AS has_stim_tsh,
               COUNT(*) FILTER (WHERE nucmed_tg_max IS NOT NULL) AS has_tg
        FROM _nucmed_labs_rollup_v1
    """).fetchone()
    print(f"  Rollup: {rollup[0]:,} patients total, "
          f"{rollup[1]:,} with stimulated TSH, {rollup[2]:,} with Tg")
    results["rollup_patients"] = rollup[0]
    results["stimulated_tsh_patients"] = rollup[1]
    results["nucmed_tg_patients"] = rollup[2]

    return results


# ======================================================================
# TASK 3: Tg COVERAGE GAP FIX
# ======================================================================

def task3_tg_gap_fix(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 3: Tg COVERAGE GAP FIX")
    print("=" * 70)

    results: dict[str, Any] = {}

    if not table_exists(con, "tg_timeline_patient_summary_v1"):
        print("  ERROR: tg_timeline_patient_summary_v1 not found")
        return results

    # Step 3.1: Identify gap patients
    # CAST both sides to VARCHAR for cross-type compatibility (BIGINT vs VARCHAR)
    gap = con.execute(f"""
        SELECT COUNT(*) AS gap_patients
        FROM tg_timeline_patient_summary_v1 t
        WHERE CAST(t.research_id AS VARCHAR) IN (
            SELECT CAST(research_id AS VARCHAR) FROM {CANONICAL}
            WHERE tg_n_measurements IS NULL OR tg_n_measurements = 0
        )
    """).fetchone()[0]
    print(f"  Gap patients (in Tg summary but 0 in canonical): {gap}")
    results["gap_patients"] = gap

    # Step 3.2: Check columns
    tg_cols = con.execute("""
        SELECT DISTINCT column_name FROM information_schema.columns
        WHERE table_name = 'tg_timeline_patient_summary_v1' AND table_schema = 'main'
        ORDER BY column_name
    """).fetchall()
    col_list = [r[0] for r in tg_cols]
    print(f"  Tg summary columns ({len(col_list)}): {', '.join(col_list[:15])}...")

    if dry_run:
        print("  [DRY RUN] Would fill Tg data for gap patients")
        return results

    # Step 3.3: Cross-validate — check that gap patients have real data
    # NOTE: tg_timeline uses n_tg_measurements; canonical uses tg_n_measurements
    print("\n  Step 3.3: Cross-validating gap patient Tg data...")
    sample = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE t.n_tg_measurements > 0) AS has_measurements,
            COUNT(*) FILTER (WHERE t.n_tg_measurements IS NULL OR t.n_tg_measurements = 0)
                AS no_measurements,
            ROUND(AVG(t.n_tg_measurements) FILTER (WHERE t.n_tg_measurements > 0), 1)
                AS avg_measurements
        FROM tg_timeline_patient_summary_v1 t
        WHERE CAST(t.research_id AS VARCHAR) IN (
            SELECT CAST(research_id AS VARCHAR) FROM {CANONICAL}
            WHERE tg_n_measurements IS NULL OR tg_n_measurements = 0
        )
    """).fetchone()
    print(f"  Gap patients with Tg measurements in summary: {sample[0]}")
    print(f"  Gap patients with 0 measurements in summary:  {sample[1]}")
    if sample[0] > 0:
        print(f"  Avg measurements among those with data: {sample[2]}")
    results["gap_with_data"] = sample[0]

    # Step 3.4: Fill the gap
    # Map column names: tg_timeline → canonical
    # tg_timeline uses: n_tg_measurements, n_tgab_measurements, tg_nadir, tg_peak, tg_mean, ...
    # canonical uses:   tg_n_measurements, n_tgab_measurements, tg_nadir, tg_peak, tg_mean, ...
    existing = get_existing_columns(con)
    tg_summary_cols_set = set(col_list)

    # Exact-match columns (same name in both)
    shared_cols = sorted(existing & tg_summary_cols_set - {"research_id"})
    # Handle the renamed column: n_tg_measurements → tg_n_measurements
    rename_map = {"n_tg_measurements": "tg_n_measurements"}

    print(f"\n  Shared columns ({len(shared_cols)}): {shared_cols}")
    print(f"  Renamed columns: {rename_map}")

    set_clauses = []
    select_parts = []
    for col in shared_cols:
        set_clauses.append(f'"{col}" = COALESCE(c."{col}", t."{col}")')
        select_parts.append(f'"{col}"')
    for src_col, dst_col in rename_map.items():
        if src_col in tg_summary_cols_set and dst_col in existing:
            set_clauses.append(f'"{dst_col}" = COALESCE(c."{dst_col}", t."{src_col}")')
            select_parts.append(f'"{src_col}"')

    if set_clauses:
        update_sql = f"""
            UPDATE {CANONICAL} AS c
            SET {', '.join(set_clauses)}
            FROM (
                SELECT CAST(research_id AS VARCHAR) AS research_id,
                       {', '.join(select_parts)}
                FROM tg_timeline_patient_summary_v1
            ) t
            WHERE CAST(c.research_id AS VARCHAR) = t.research_id
              AND (c.tg_n_measurements IS NULL OR c.tg_n_measurements = 0)
        """
        print(f"  Updating {len(set_clauses)} Tg columns for gap patients...")
        con.execute(update_sql)
    else:
        print("  WARNING: No mappable Tg columns found — skipping")

    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE tg_n_measurements > 0) FROM {CANONICAL}
    """).fetchone()[0]
    print(f"  Post-fix: {post:,} patients with Tg data")
    results["post_fix_tg_patients"] = post

    return results


# ======================================================================
# TASK 4: RECURRENCE SITE RECOVERY
# ======================================================================

def task4_recurrence_sites(
    con: duckdb.DuckDBPyConnection, dry_run: bool,
    col_map: dict[str, str] | None = None
) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 4: RECURRENCE SITE RECOVERY")
    print("=" * 70)

    results: dict[str, Any] = {}
    if col_map is None:
        col_map = build_col_map(con)

    rec_col = col_map["recurrence_confirmed"]
    site_col = col_map["recurrence_site"]
    print(f"  Using: recurrence_flag='{rec_col}', site_col='{site_col}'")

    # How many confirmed recurrences have no site?
    rec_stats = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE {rec_col} = TRUE) AS total_confirmed,
            COUNT(*) FILTER (WHERE {rec_col} = TRUE AND {site_col} IS NOT NULL)
                AS has_site,
            COUNT(*) FILTER (WHERE {rec_col} = TRUE AND {site_col} IS NULL)
                AS no_site
        FROM {CANONICAL}
    """).fetchone()
    print(f"  Confirmed recurrences: {rec_stats[0]}")
    print(f"  With site: {rec_stats[1]}, Without site: {rec_stats[2]}")
    results["total_confirmed"] = rec_stats[0]
    results["pre_has_site"] = rec_stats[1]

    if rec_stats[2] == 0:
        print("  All confirmed recurrences already have sites — skipping")
        return results

    if dry_run:
        print("  [DRY RUN] Would extract recurrence sites from FNA specimen_location")
        return results

    # Step 4.1: Extract from FNA specimen_location
    print("\n  Step 4.1: Extracting from FNA specimen_location...")
    # recurrence_type may be named differently in new schemas
    _existing_now = get_existing_columns(con)
    if "recurrence_type" in _existing_now:
        rec_type_col = "recurrence_type"
    elif "recurrence_type_primary" in _existing_now:
        rec_type_col = "recurrence_type_primary"
    else:
        rec_type_col = "NULL"
    con.execute(f"""
        CREATE OR REPLACE TABLE _recurrence_fna_sites_v1 AS
        WITH recurrence_fnas AS (
            SELECT
                c.research_id,
                c.recurrence_date,
                {rec_type_col} AS recurrence_type,
                f.specimen_location,
                f.fna_date,
                CASE
                    WHEN LOWER(f.specimen_location) LIKE '%lymph node%'
                      OR LOWER(f.specimen_location) LIKE '%ln%'
                      OR LOWER(f.specimen_location) LIKE '%level%'
                        THEN 'cervical_lymph_node'
                    WHEN LOWER(f.specimen_location) LIKE '%thyroid%'
                      OR LOWER(f.specimen_location) LIKE '%fossa%'
                        THEN 'thyroid_bed'
                    WHEN LOWER(f.specimen_location) LIKE '%neck%'
                      AND LOWER(f.specimen_location) LIKE '%mass%'
                        THEN 'neck_mass'
                    WHEN LOWER(f.specimen_location) LIKE '%neck%' THEN 'cervical'
                    WHEN LOWER(f.specimen_location) LIKE '%lung%'
                      OR LOWER(f.specimen_location) LIKE '%pulmonary%'
                        THEN 'lung'
                    WHEN LOWER(f.specimen_location) LIKE '%bone%'
                      OR LOWER(f.specimen_location) LIKE '%vertebr%'
                        THEN 'bone'
                    WHEN LOWER(f.specimen_location) LIKE '%mediastin%'
                        THEN 'mediastinal'
                    ELSE 'other'
                END AS recurrence_site_standardized,
                CASE
                    WHEN LOWER(f.specimen_location) LIKE '%right%' THEN 'right'
                    WHEN LOWER(f.specimen_location) LIKE '%left%' THEN 'left'
                    WHEN LOWER(f.specimen_location) LIKE '%midline%'
                      OR LOWER(f.specimen_location) LIKE '%central%' THEN 'midline'
                    WHEN LOWER(f.specimen_location) LIKE '%bilateral%' THEN 'bilateral'
                    ELSE NULL
                END AS recurrence_laterality,
                f.specimen_location AS recurrence_site_raw,
                'fna_specimen_location' AS recurrence_site_source,
                ROW_NUMBER() OVER (
                    PARTITION BY c.research_id ORDER BY f.fna_date DESC
                ) AS rn
            FROM {CANONICAL} c
            JOIN fna_cytology f ON CAST(f.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
            WHERE {rec_col} = TRUE
              AND f.specimen_location IS NOT NULL
              AND LENGTH(TRIM(CAST(f.specimen_location AS VARCHAR))) > 3
        )
        SELECT * FROM recurrence_fnas WHERE rn = 1
    """)

    fna_sites = con.execute(
        "SELECT COUNT(*) FROM _recurrence_fna_sites_v1"
    ).fetchone()[0]
    print(f"  FNA-derived recurrence sites: {fna_sites}")
    results["fna_sites"] = fna_sites

    # Step 4.2: Check recurrence_event_clean for remaining
    print("\n  Step 4.2: Checking recurrence_event_clean_v1 for additional sites...")
    if table_exists(con, "recurrence_event_clean_v1"):
        con.execute(f"""
            CREATE OR REPLACE TABLE _recurrence_event_sites_v1 AS
            SELECT DISTINCT ON (CAST(r.research_id AS VARCHAR))
                CAST(r.research_id AS VARCHAR) AS research_id,
                r.recurrence_site AS recurrence_site_standardized,
                r.recurrence_site AS recurrence_site_raw,
                NULL AS recurrence_laterality,
                'recurrence_event_clean' AS recurrence_site_source
            FROM recurrence_event_clean_v1 r
            WHERE CAST(r.research_id AS VARCHAR) IN (
                SELECT CAST(research_id AS VARCHAR) FROM {CANONICAL}
                WHERE {rec_col} = TRUE
                  AND {site_col} IS NULL
            )
            AND CAST(r.research_id AS VARCHAR) NOT IN (
                SELECT CAST(research_id AS VARCHAR) FROM _recurrence_fna_sites_v1
            )
            AND r.recurrence_site IS NOT NULL
            AND LENGTH(TRIM(CAST(r.recurrence_site AS VARCHAR))) > 2
        """)
        event_sites = con.execute(
            "SELECT COUNT(*) FROM _recurrence_event_sites_v1"
        ).fetchone()[0]
        print(f"  Event-derived additional sites: {event_sites}")
        results["event_sites"] = event_sites
    else:
        event_sites = 0
        print("  recurrence_event_clean_v1 not found — skipping")

    # Step 4.3: Cross-validate — site distribution
    print("\n  Step 4.3: Site distribution:")
    dist = con.execute("""
        SELECT recurrence_site_standardized, recurrence_laterality, COUNT(*) AS n
        FROM _recurrence_fna_sites_v1
        GROUP BY 1, 2 ORDER BY 3 DESC
    """).fetchall()
    for site, lat, n in dist:
        print(f"    {str(site):25s} {str(lat):12s} {n:>4}")
    results["site_distribution"] = {f"{s}|{l}": n for s, l, n in dist}

    # Step 4.4: Update canonical
    print("\n  Step 4.4: Updating canonical recurrence columns...")
    existing = get_existing_columns(con)

    # Determine if site_col is a VARCHAR or INTEGER column (use safe write col)
    site_col_dtype_r = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND column_name = '{site_col}'
        AND table_schema = 'main' LIMIT 1
    """).fetchone()
    site_col_is_int = site_col_dtype_r and "INT" in (site_col_dtype_r[0] or "").upper()
    # If the existing site column is integer, write to a new text column instead
    write_site_col = "recurrence_site_text" if site_col_is_int else site_col

    for col, dtype in [
        (write_site_col, "VARCHAR"),
        ("recurrence_site_raw", "VARCHAR"),
        ("recurrence_laterality", "VARCHAR"),
        ("recurrence_site_source", "VARCHAR"),
    ]:
        if col not in existing:
            safe_add_column(con, col, dtype)
            existing.add(col)

    # Update from FNA
    con.execute(f"""
        UPDATE {CANONICAL} AS c
        SET
            {write_site_col} = f.recurrence_site_standardized,
            recurrence_site_raw = f.recurrence_site_raw,
            recurrence_laterality = f.recurrence_laterality,
            recurrence_site_source = f.recurrence_site_source
        FROM _recurrence_fna_sites_v1 f
        WHERE CAST(c.research_id AS VARCHAR) = f.research_id
          AND {write_site_col} IS NULL
    """)

    # Update from event clean
    if event_sites > 0:
        con.execute(f"""
            UPDATE {CANONICAL} AS c
            SET
                {write_site_col} = e.recurrence_site_standardized,
                recurrence_site_raw = e.recurrence_site_raw,
                recurrence_site_source = e.recurrence_site_source
            FROM _recurrence_event_sites_v1 e
            WHERE CAST(c.research_id AS VARCHAR) = e.research_id
              AND {write_site_col} IS NULL
        """)

    # Post check
    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {write_site_col} IS NOT NULL)
        FROM {CANONICAL} WHERE {rec_col} = TRUE
    """).fetchone()[0]
    print(f"  Post-update: {post} of {rec_stats[0]} confirmed recurrences have site ({write_site_col})")
    results["post_has_site"] = post

    return results


# ======================================================================
# TASK 5: COMPLICATION CROSS-VALIDATION (QC report only)
# ======================================================================

def task5_complication_crossval(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 5: COMPLICATION CROSS-VALIDATION")
    print("=" * 70)

    results: dict[str, Any] = {}

    if not table_exists(con, "op_sheet_data") or not table_exists(con, "complication_phenotype_v1"):
        missing = []
        if not table_exists(con, "op_sheet_data"):
            missing.append("op_sheet_data")
        if not table_exists(con, "complication_phenotype_v1"):
            missing.append("complication_phenotype_v1")
        print(f"  Missing tables: {', '.join(missing)} — skipping")
        return results

    if dry_run:
        print("  [DRY RUN] Would generate concordance report")
        return results

    # Step 5.1: Identify discordances
    print("\n  Step 5.1: OP Sheet has complications but phenotype doesn't...")
    ops_only = con.execute("""
        SELECT COUNT(DISTINCT o.research_id) AS n
        FROM op_sheet_data o
        WHERE o.ops_periop_complications IS NOT NULL
          AND LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) > 3
          AND o.research_id NOT IN (
              SELECT CAST(research_id AS VARCHAR) FROM complication_phenotype_v1
          )
    """).fetchone()[0]
    print(f"  OP Sheet-only complication patients: {ops_only}")
    results["ops_only"] = ops_only

    pheno_only = con.execute("""
        SELECT COUNT(DISTINCT CAST(cp.research_id AS VARCHAR)) AS n
        FROM complication_phenotype_v1 cp
        LEFT JOIN op_sheet_data o ON CAST(cp.research_id AS VARCHAR) = o.research_id
        WHERE cp.confirmed_flag = TRUE
          AND (o.ops_periop_complications IS NULL
               OR LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) < 3)
    """).fetchone()[0]
    print(f"  Phenotype-confirmed but OP Sheet empty: {pheno_only}")
    results["pheno_only"] = pheno_only

    both = con.execute("""
        SELECT COUNT(DISTINCT o.research_id) AS n
        FROM op_sheet_data o
        JOIN complication_phenotype_v1 cp ON CAST(cp.research_id AS VARCHAR) = o.research_id
        WHERE o.ops_periop_complications IS NOT NULL
          AND LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) > 3
    """).fetchone()[0]
    print(f"  Both sources agree (have complications): {both}")
    results["both_agree"] = both

    # Step 5.2: Store report
    print("\n  Step 5.2: Storing concordance report...")
    con.execute("""
        CREATE OR REPLACE TABLE complication_crossval_report_v1 AS
        SELECT 'ops_has_comp_pheno_missing' AS discordance_type,
            (SELECT COUNT(DISTINCT o.research_id) FROM op_sheet_data o
             WHERE o.ops_periop_complications IS NOT NULL
               AND LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) > 3
               AND o.research_id NOT IN (
                   SELECT CAST(research_id AS VARCHAR) FROM complication_phenotype_v1
               )) AS n_patients
        UNION ALL
        SELECT 'pheno_confirmed_ops_missing',
            (SELECT COUNT(DISTINCT CAST(cp.research_id AS VARCHAR))
             FROM complication_phenotype_v1 cp
             LEFT JOIN op_sheet_data o ON CAST(cp.research_id AS VARCHAR) = o.research_id
             WHERE cp.confirmed_flag = TRUE
               AND (o.ops_periop_complications IS NULL
                    OR LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) < 3))
        UNION ALL
        SELECT 'both_agree',
            (SELECT COUNT(DISTINCT o.research_id) FROM op_sheet_data o
             JOIN complication_phenotype_v1 cp ON CAST(cp.research_id AS VARCHAR) = o.research_id
             WHERE o.ops_periop_complications IS NOT NULL
               AND LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) > 3)
    """)
    print("  Saved complication_crossval_report_v1 (QC report — no canonical changes)")

    # Show top OP Sheet complication descriptions for context
    print("\n  Sample OP Sheet complication descriptions (first 10):")
    samples = con.execute("""
        SELECT o.research_id, LEFT(CAST(o.ops_periop_complications AS VARCHAR), 80)
        FROM op_sheet_data o
        WHERE o.ops_periop_complications IS NOT NULL
          AND LENGTH(TRIM(CAST(o.ops_periop_complications AS VARCHAR))) > 3
        LIMIT 10
    """).fetchall()
    for rid, desc in samples:
        print(f"    {rid}: {desc}")

    return results


# ======================================================================
# TASK 6: CANONICAL REBUILD + INTEGRATION + VALIDATION
# ======================================================================

def task6_canonical_rebuild(
    con: duckdb.DuckDBPyConnection, dry_run: bool,
    col_map: dict[str, str] | None = None
) -> dict[str, Any]:
    print("\n" + "=" * 70)
    print("[218] TASK 6: CANONICAL REBUILD + VALIDATION")
    print("=" * 70)

    results: dict[str, Any] = {}
    if col_map is None:
        col_map = build_col_map(con)

    fu_col = col_map["followup_years"]
    lc_col = col_map["last_contact_date"]
    src_col = col_map["last_contact_source"]
    fd_col = col_map["followup_days"]
    fc_col = col_map["followup_category"]
    rec_col = col_map["recurrence_confirmed"]
    site_col = col_map["recurrence_site"]
    tsh_col = col_map["rai_stimulated_tsh"]
    tg_col = col_map["rai_stimulated_tg"]

    if dry_run:
        print("  [DRY RUN] Would integrate follow-up, nucmed labs, and recurrence sites")
        return results

    existing = get_existing_columns(con)

    # ── 6A: Follow-up integration ─────────────────────────────────────────
    print("\n  6A: Integrating follow-up recovery...")

    # Add all follow-up columns if they don't exist (handles new canonical schema)
    fu_new_cols: list[tuple[str, str]] = [
        (lc_col, "DATE"),
        (src_col, "VARCHAR"),
        (fd_col, "INTEGER"),
        (fu_col, "DOUBLE"),
        (fc_col, "VARCHAR"),
        ("followup_n_contact_sources", "INTEGER"),
        ("followup_all_sources", "VARCHAR"),
        ("followup_recovery_method", "VARCHAR"),
    ]
    for col, dtype in fu_new_cols:
        if col not in existing:
            safe_add_column(con, col, dtype)
            existing.add(col)

    if table_exists(con, "_followup_computed_v2"):
        con.execute(f"""
            UPDATE {CANONICAL} AS c
            SET
                {lc_col} = CASE
                    WHEN f.followup_years_v2 >= 0
                      AND (c.{lc_col} IS NULL
                           OR f.last_contact_date_v2 > c.{lc_col})
                    THEN f.last_contact_date_v2
                    ELSE c.{lc_col}
                END,
                {src_col} = CASE
                    WHEN f.followup_years_v2 >= 0
                      AND (c.{lc_col} IS NULL
                           OR f.last_contact_date_v2 > c.{lc_col})
                    THEN f.last_contact_source_v2
                    ELSE c.{src_col}
                END,
                {fd_col} = CASE
                    WHEN f.followup_years_v2 >= 0
                      AND (c.{lc_col} IS NULL
                           OR f.last_contact_date_v2 > c.{lc_col})
                    THEN f.followup_days_v2
                    ELSE c.{fd_col}
                END,
                {fu_col} = CASE
                    WHEN f.followup_years_v2 >= 0
                      AND (c.{lc_col} IS NULL
                           OR f.last_contact_date_v2 > c.{lc_col})
                    THEN LEAST(f.followup_years_v2, 30.0)
                    ELSE c.{fu_col}
                END,
                {fc_col} = CASE
                    WHEN f.followup_years_v2 >= 0
                      AND (c.{lc_col} IS NULL
                           OR f.last_contact_date_v2 > c.{lc_col})
                    THEN f.followup_category_v2
                    ELSE c.{fc_col}
                END,
                followup_n_contact_sources = f.n_contact_sources,
                followup_all_sources = f.all_contact_sources,
                followup_recovery_method = CASE
                    WHEN c.{fu_col} IS NOT NULL AND c.{fu_col} > 0 THEN 'original'
                    ELSE f.last_contact_source_v2
                END
            FROM _followup_computed_v2 f
            WHERE CAST(c.research_id AS VARCHAR) = f.research_id
        """)

        fu_stats = con.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE {fu_col} > 0) AS positive_fu,
                COUNT(*) FILTER (WHERE {fu_col} = 0 OR {fu_col} IS NULL) AS zero_fu,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {fu_col})
                    FILTER (WHERE {fu_col} > 0), 2) AS median_fu
            FROM {CANONICAL}
        """).fetchone()
        print(f"  Follow-up: {fu_stats[0]:,} positive, {fu_stats[1]:,} zero/null, "
              f"median={fu_stats[2]} years")
        results["followup_positive"] = fu_stats[0]
        results["followup_zero"] = fu_stats[1]
        results["followup_median"] = fu_stats[2]
    else:
        print("  WARNING: _followup_computed_v2 not found — skipping follow-up integration")

    # ── 6B: Nuclear med lab integration ───────────────────────────────────
    print("\n  6B: Integrating nuclear med labs...")

    for col, dtype in [
        ("nucmed_tg_max", "DOUBLE"),
        ("nucmed_tg_min", "DOUBLE"),
        ("nucmed_n_tsh_values", "INTEGER"),
        ("nucmed_n_tg_values", "INTEGER"),
        ("nucmed_n_tgab_values", "INTEGER"),
        ("nucmed_first_scan_with_labs", "VARCHAR"),
        ("nucmed_last_scan_with_labs", "VARCHAR"),
        ("nucmed_lab_source", "VARCHAR"),
    ]:
        if col not in existing:
            safe_add_column(con, col, dtype)
            existing.add(col)

    # Ensure RAI stimulated columns exist (may be new in this canonical)
    for col, dtype in [(tsh_col, "DOUBLE"), (tg_col, "DOUBLE")]:
        if col not in existing:
            safe_add_column(con, col, dtype)
            existing.add(col)

    if table_exists(con, "_nucmed_labs_rollup_v1"):
        con.execute(f"""
            UPDATE {CANONICAL} AS c
            SET
                {tsh_col} = COALESCE(c.{tsh_col}, nm.rai_stimulated_tsh_v2),
                {tg_col} = COALESCE(c.{tg_col}, nm.nucmed_tg_max),
                nucmed_tg_max = nm.nucmed_tg_max,
                nucmed_tg_min = nm.nucmed_tg_min,
                nucmed_n_tsh_values = nm.nucmed_n_tsh_values,
                nucmed_n_tg_values = nm.nucmed_n_tg_values,
                nucmed_n_tgab_values = nm.nucmed_n_tgab_values,
                nucmed_first_scan_with_labs = nm.nucmed_first_scan_with_labs,
                nucmed_last_scan_with_labs = nm.nucmed_last_scan_with_labs,
                nucmed_lab_source = nm.nucmed_lab_source
            FROM _nucmed_labs_rollup_v1 nm
            WHERE CAST(c.research_id AS VARCHAR) = nm.research_id
        """)

        stim_tsh = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE {tsh_col} IS NOT NULL) FROM {CANONICAL}
        """).fetchone()[0]
        stim_tg = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE {tg_col} IS NOT NULL) FROM {CANONICAL}
        """).fetchone()[0]
        print(f"  RAI stimulated TSH: {stim_tsh:,} patients")
        print(f"  RAI stimulated Tg:  {stim_tg:,} patients")
        results["stim_tsh"] = stim_tsh
        results["stim_tg"] = stim_tg
    else:
        print("  WARNING: _nucmed_labs_rollup_v1 not found — skipping")

    # ── 6C: Final validation ──────────────────────────────────────────────
    print("\n  6C: Final validation suite...")

    ok = check_invariants(con, CANONICAL, "POST-REBUILD")
    results["invariants_pass"] = ok

    # Column count
    n_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name) FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    print(f"  Total columns: {n_cols}")
    results["n_columns"] = n_cols

    # Tg patients
    tg_pts = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE tg_n_measurements > 0) FROM {CANONICAL}
    """).fetchone()[0]
    print(f"  Tg patients: {tg_pts:,}")
    results["tg_patients"] = tg_pts

    # Recurrence sites (check both the original and text column)
    text_site_col = "recurrence_site_text" if "recurrence_site_text" in get_existing_columns(con) \
        else site_col
    rec_sites = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {text_site_col} IS NOT NULL)
        FROM {CANONICAL} WHERE {rec_col} = TRUE
    """).fetchone()[0]
    total_rec = con.execute(f"""
        SELECT COUNT(*) FROM {CANONICAL} WHERE {rec_col} = TRUE
    """).fetchone()[0]
    print(f"  Recurrence sites: {rec_sites} of {total_rec} confirmed")
    results["recurrence_sites"] = rec_sites
    results["recurrence_total"] = total_rec

    # Provenance check
    print("\n  Provenance verification:")
    prov = con.execute(f"""
        SELECT
            'followup' AS domain,
            COUNT(*) FILTER (WHERE {src_col} IS NOT NULL) AS has_source,
            COUNT(*) FILTER (WHERE {lc_col} IS NOT NULL) AS has_date,
            COUNT(*) FILTER (WHERE followup_n_contact_sources IS NOT NULL) AS has_n_sources
        FROM {CANONICAL}
        UNION ALL
        SELECT 'nucmed_tsh',
            COUNT(*) FILTER (WHERE nucmed_lab_source IS NOT NULL
                AND {tsh_col} IS NOT NULL),
            COUNT(*) FILTER (WHERE nucmed_first_scan_with_labs IS NOT NULL
                AND {tsh_col} IS NOT NULL),
            COUNT(*) FILTER (WHERE nucmed_n_tsh_values IS NOT NULL
                AND {tsh_col} IS NOT NULL)
        FROM {CANONICAL}
        UNION ALL
        SELECT 'recurrence_site',
            COUNT(*) FILTER (WHERE recurrence_site_source IS NOT NULL
                AND {text_site_col} IS NOT NULL),
            COUNT(*) FILTER (WHERE recurrence_date IS NOT NULL
                AND {text_site_col} IS NOT NULL),
            0
        FROM {CANONICAL}
    """).fetchall()
    for domain, has_src, has_date, has_n in prov:
        print(f"    {domain:20s} source={has_src:>6,}  date={has_date:>6,}  "
              f"n_sources={has_n:>6,}")

    return results


# ======================================================================
# MAIN
# ======================================================================

def main() -> None:
    global DB, CANONICAL
    parser = argparse.ArgumentParser(
        description="Script 218: Follow-up recovery + nuclear med labs + "
                    "Tg gap + recurrence sites"
    )
    parser.add_argument(
        "--phase", default="all",
        choices=["1", "2", "3", "4", "5", "6", "all"],
        help="Run a specific task or all"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    parser.add_argument(
        "--db", default=None,
        help=f"MotherDuck database name (default: {DB})"
    )
    parser.add_argument(
        "--canonical", default=None,
        help=f"Canonical table name (default: {CANONICAL})"
    )
    parser.add_argument(
        "--token-from-toml", action="store_true",
        help="Force-load token from motherduck.local.toml (bypass env var)"
    )
    args = parser.parse_args()

    # Override module-level constants if CLI args provided
    if args.db:
        DB = args.db
    if args.canonical:
        CANONICAL = args.canonical

    # Token resolution
    explicit_token: str | None = None
    if args.token_from_toml:
        from motherduck_client import _load_toml_path
        toml_data = _load_toml_path(REPO / "motherduck.local.toml")
        for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
            val = toml_data.get(key)
            if val and str(val).strip():
                explicit_token = str(val).strip()
                print(f"  Token: loaded from TOML ({key})")
                break
        if not explicit_token:
            print("[218] ERROR: --token-from-toml set but no token in motherduck.local.toml")
            sys.exit(1)

    print("=" * 70)
    print("[218] THYROID_2026 — Follow-Up Recovery + Nuclear Med Labs + "
          "Tg Gap + Recurrence Sites")
    print(f"  Database:  {DB}")
    print(f"  Canonical: {CANONICAL}")
    print(f"  Phase: {args.phase}")
    print(f"  Dry run: {args.dry_run}")
    print("=" * 70)

    t0 = time.time()
    con = connect(db=DB, token=explicit_token)
    print(f"  Connected to MotherDuck in {time.time() - t0:.1f}s")

    # Pre-flight invariant check
    check_invariants(con, CANONICAL, "PRE-FLIGHT")

    # Build column name map (handles old/new canonical schema differences)
    col_map = build_col_map(con)
    print(f"  Column map: recurrence_flag='{col_map['recurrence_confirmed']}', "
          f"site='{col_map['recurrence_site']}'")

    all_results: dict[str, Any] = {}
    run_all = args.phase == "all"

    if run_all or args.phase == "1":
        all_results["task1"] = task1_followup_recovery(con, args.dry_run)

    if run_all or args.phase == "2":
        all_results["task2"] = task2_nucmed_labs(con, args.dry_run)

    if run_all or args.phase == "3":
        all_results["task3"] = task3_tg_gap_fix(con, args.dry_run)

    if run_all or args.phase == "4":
        all_results["task4"] = task4_recurrence_sites(con, args.dry_run, col_map=col_map)

    if run_all or args.phase == "5":
        all_results["task5"] = task5_complication_crossval(con, args.dry_run)

    if run_all or args.phase == "6":
        all_results["task6"] = task6_canonical_rebuild(con, args.dry_run, col_map=col_map)

    elapsed = time.time() - t0
    print("\n" + "=" * 70)
    print(f"[218] COMPLETE in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    print("=" * 70)

    # Summary
    print("\n  SUMMARY:")
    if "task1" in all_results:
        r = all_results["task1"]
        rec = r.get("recovered", "?")
        print(f"    Follow-up recovered: {rec} patients")
        print(f"    Median follow-up: {r.get('median_followup_v2', '?')} years")
    if "task2" in all_results:
        r = all_results["task2"]
        print(f"    Nuclear med TSH: {r.get('stimulated_tsh_patients', '?')} patients")
        print(f"    Nuclear med Tg:  {r.get('nucmed_tg_patients', '?')} patients")
    if "task3" in all_results:
        r = all_results["task3"]
        print(f"    Tg gap fixed: {r.get('post_fix_tg_patients', '?')} total patients with Tg")
    if "task4" in all_results:
        r = all_results["task4"]
        print(f"    Recurrence sites: {r.get('post_has_site', '?')} of "
              f"{r.get('total_confirmed', '?')} confirmed")
    if "task6" in all_results:
        r = all_results["task6"]
        print(f"    Invariants: {'PASS' if r.get('invariants_pass') else 'FAIL'}")
        print(f"    Columns: {r.get('n_columns', '?')}")

    con.close()


if __name__ == "__main__":
    main()
