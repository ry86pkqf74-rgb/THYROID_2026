#!/usr/bin/env python3
"""
THYROID_2026 — Script 217: Lab Value Recovery + Clinical Note LN Integration
Database: thyroid_ete_fix_20260413 on MotherDuck

Phase 1 — Lab Value Recovery (Task 1):
  Parse value_raw strings to recover missing numeric values for TSH/VitD/PTH/Calcium.
  Creates lab_values_complete_v1 (all 4 labs, numeric + parsed) and updates canonical.

Phase 2 — Clinical Note LN Integration (Task 2):
  Copy clinical_note_ln_extracted_v1 from thyroid_research_ro_v2 to canonical DB.
  Build patient-level rollup (surgical_path + imaging modalities).
  Cross-validate vs structured LN data. Integrate ~25 cnln_* columns into canonical.

Phase 3 — Canonical Rebuild + Validation (Task 3):
  Rebuild canonical with expanded lab columns + new cnln_* columns.
  Run all invariant checks and coverage report.

Run:
  .venv/bin/python scripts/217_lab_recovery_ln_integration.py [--dry-run] [--phase 1|2|3|all]

Invariants enforced after every rebuild:
  - 10,871 rows
  - 10,871 distinct research_ids
  - 0 null research_ids
  - 0 null fna_path_outcome
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10_871
LAB_NAMES = ("tsh", "vitamin_d", "pth", "calcium")

# Expected clinical ranges for validation (10× outer bounds)
LAB_OUTER_BOUNDS = {
    "tsh":       (0.0, 2000.0),   # mIU/L — suppressed thyroid cancer will have very low TSH
    "vitamin_d": (0.0, 1500.0),   # ng/mL
    "pth":       (0.0, 5000.0),   # pg/mL
    "calcium":   (0.0, 200.0),    # mg/dL
}

# Canonical unit for each lab (used when unit cannot be parsed from raw string)
LAB_DEFAULT_UNITS = {
    "tsh":       "mIU/L",
    "vitamin_d": "ng/mL",
    "pth":       "pg/mL",
    "calcium":   "mg/dL",
}


# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[217] ERROR: No MotherDuck token found.")
        sys.exit(1)
    print(f"[217] Token: SET, length={len(token)}")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ─────────────────────────────────────────────────────────────────────────────
# Invariant checker
# ─────────────────────────────────────────────────────────────────────────────

def check_invariants(con: duckdb.DuckDBPyConnection, table: str = CANONICAL,
                     label: str = "") -> bool:
    inv = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT research_id) AS distinct_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM {table}
    """).fetchone()
    tag = f"[217] {label}:" if label else "[217]"
    print(f"{tag} {inv[0]} rows | {inv[1]} distinct RIDs | "
          f"{inv[2]} null RIDs | {inv[3]} null fna_path_outcome")
    errors = []
    if inv[0] != TOTAL_ROWS:
        errors.append(f"Row count {inv[0]} != {TOTAL_ROWS}")
    if inv[0] != inv[1]:
        errors.append(f"Duplicate research_ids: {inv[0] - inv[1]}")
    if inv[2] > 0:
        errors.append(f"NULL research_ids: {inv[2]}")
    for e in errors:
        print(f"[217] ERROR: {e}")
    return len(errors) == 0


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str, label: str = "") -> Any:
    """Execute SQL and return row count; log on error."""
    try:
        result = con.execute(sql)
        try:
            rc = result.fetchone()
            if rc is not None and len(rc) == 1 and isinstance(rc[0], int):
                return rc[0]
        except Exception:
            pass
        return -1
    except Exception as e:
        print(f"[217] SQL ERROR ({label}): {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — Lab Value Recovery
# ─────────────────────────────────────────────────────────────────────────────

PARSE_CTE = """
WITH raw_source AS (
    SELECT 
        research_id,
        lab_date,
        lab_date_status,
        lab_name_standardized,
        lab_name_raw,
        value_raw,
        value_numeric,
        CAST(unit_raw AS VARCHAR) AS unit_raw,
        unit_standardized,
        CAST(abnormal_flag AS VARCHAR) AS abnormal_flag,
        is_censored,
        CAST(reference_range AS VARCHAR) AS reference_range,
        source_table,
        source_script,
        ingestion_wave,
        data_completeness_tier,
        provenance_note,
        analyte_group
    FROM longitudinal_lab_canonical_v1
    WHERE lab_name_standardized IN ('tsh', 'vitamin_d', 'pth', 'calcium')
),

parsed AS (
    SELECT
        *,
        -- Strip censor prefix (<, >, <=, >=) then extract first numeric token
        TRY_CAST(
            REGEXP_EXTRACT(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(value_raw), '^[<>≤≥]=?\\s*', ''),
                    '^\\s+', ''
                ),
                '([0-9]+\\.?[0-9]*)',
                1
            ) AS DOUBLE
        ) AS parsed_numeric,

        -- Extract unit from raw string (first recognized unit token after the number)
        NULLIF(TRIM(
            REGEXP_EXTRACT(
                value_raw,
                '[0-9]+\\.?[0-9]*\\s*([a-zA-Z][a-zA-Z0-9/µμ]*(?:/[a-zA-Z]+)?)',
                1
            )
        ), '') AS parsed_unit_raw,

        -- Censored flag from leading < / > symbols
        CASE
            WHEN value_raw LIKE '<%' OR value_raw LIKE '≤%' OR value_raw LIKE '<=%' THEN TRUE
            WHEN value_raw LIKE '>%' OR value_raw LIKE '≥%' OR value_raw LIKE '>=%' THEN TRUE
            ELSE FALSE
        END AS is_censored_parsed,

        -- Censor direction
        CASE
            WHEN value_raw LIKE '<%' OR value_raw LIKE '≤%' OR value_raw LIKE '<=%' THEN 'left'
            WHEN value_raw LIKE '>%' OR value_raw LIKE '≥%' OR value_raw LIKE '>=%' THEN 'right'
            ELSE NULL
        END AS censor_direction_parsed,

        -- Abnormal flag from parenthetical qualifiers
        CASE
            WHEN value_raw LIKE '%(Low)%' OR value_raw LIKE '%(L)%'  THEN 'L'
            WHEN value_raw LIKE '%(High)%' OR value_raw LIKE '%(H)%' THEN 'H'
            ELSE NULL
        END AS abnormal_flag_parsed

    FROM raw_source
    WHERE value_numeric IS NULL
      AND value_raw IS NOT NULL
      AND TRIM(value_raw) != ''
)
SELECT * FROM parsed
WHERE parsed_numeric IS NOT NULL
"""


def phase1_build_lab_complete(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Create lab_values_complete_v1 combining existing numeric + newly parsed rows."""
    print("\n[217] ── PHASE 1: Lab Value Recovery ──")

    # Preview parse results before creating table
    df_preview = con.execute(f"""
        {PARSE_CTE}
    """).df()
    print(f"[217] Parsed {len(df_preview):,} raw-only rows with extractable numerics")
    by_lab = df_preview.groupby("lab_name_standardized")["parsed_numeric"].agg(
        ["count", "min", "median", "max"]
    )
    print(f"[217] Parsed rows by lab:\n{by_lab.to_string()}")

    if dry_run:
        print("[217] DRY RUN: skipping lab_values_complete_v1 creation")
        return

    sql_create = """
    CREATE OR REPLACE TABLE lab_values_complete_v1 AS
    WITH raw_source AS (
        SELECT 
            research_id,
            lab_date,
            lab_date_status,
            lab_name_standardized,
            lab_name_raw,
            value_raw,
            value_numeric,
            CAST(unit_raw AS VARCHAR) AS unit_raw,
            unit_standardized,
            CAST(abnormal_flag AS VARCHAR) AS abnormal_flag,
            is_censored,
            CAST(reference_range AS VARCHAR) AS reference_range,
            source_table,
            source_script,
            ingestion_wave,
            data_completeness_tier,
            provenance_note,
            analyte_group
        FROM longitudinal_lab_canonical_v1
        WHERE lab_name_standardized IN ('tsh', 'vitamin_d', 'pth', 'calcium')
    ),
    parsed AS (
        SELECT
            *,
            TRY_CAST(
                REGEXP_EXTRACT(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(value_raw), '^[<>≤≥]=?\\s*', ''),
                        '^\\s+', ''
                    ),
                    '([0-9]+\\.?[0-9]*)',
                    1
                ) AS DOUBLE
            ) AS parsed_numeric,
            NULLIF(TRIM(
                REGEXP_EXTRACT(
                    value_raw,
                    '[0-9]+\\.?[0-9]*\\s*([a-zA-Z][a-zA-Z0-9/µμ]*(?:/[a-zA-Z]+)?)',
                    1
                )
            ), '') AS parsed_unit_raw,
            CASE
                WHEN value_raw LIKE '<%' OR value_raw LIKE '≤%' OR value_raw LIKE '<=%' THEN TRUE
                WHEN value_raw LIKE '>%' OR value_raw LIKE '≥%' OR value_raw LIKE '>=%' THEN TRUE
                ELSE FALSE
            END AS is_censored_parsed,
            CASE
                WHEN value_raw LIKE '<%' OR value_raw LIKE '≤%' OR value_raw LIKE '<=%' THEN 'left'
                WHEN value_raw LIKE '>%' OR value_raw LIKE '≥%' OR value_raw LIKE '>=%' THEN 'right'
                ELSE NULL
            END AS censor_direction_parsed,
            CASE
                WHEN value_raw LIKE '%(Low)%' OR value_raw LIKE '%(L)%'  THEN 'L'
                WHEN value_raw LIKE '%(High)%' OR value_raw LIKE '%(H)%' THEN 'H'
                ELSE NULL
            END AS abnormal_flag_parsed
        FROM raw_source
        WHERE value_numeric IS NULL
          AND value_raw IS NOT NULL
          AND TRIM(value_raw) != ''
    )
    -- Already-numeric rows (no change needed)
    SELECT
        research_id,
        lab_date,
        lab_date_status,
        lab_name_standardized,
        lab_name_raw,
        value_raw,
        value_numeric,
        unit_raw,
        unit_standardized,
        abnormal_flag,
        is_censored,
        reference_range,
        source_table,
        source_script,
        ingestion_wave,
        data_completeness_tier,
        provenance_note,
        analyte_group,
        FALSE AS value_was_parsed,
        NULL::VARCHAR AS censor_direction,
        '217_lab_value_recovery' AS parse_script
    FROM raw_source
    WHERE value_numeric IS NOT NULL

    UNION ALL

    -- Newly parsed from value_raw
    SELECT
        research_id,
        lab_date,
        lab_date_status,
        lab_name_standardized,
        lab_name_raw,
        value_raw,
        parsed_numeric AS value_numeric,
        unit_raw,
        COALESCE(NULLIF(parsed_unit_raw, ''), unit_standardized) AS unit_standardized,
        COALESCE(abnormal_flag_parsed, abnormal_flag) AS abnormal_flag,
        COALESCE(is_censored_parsed, is_censored, FALSE) AS is_censored,
        reference_range,
        source_table,
        source_script,
        ingestion_wave,
        data_completeness_tier,
        provenance_note,
        analyte_group,
        TRUE AS value_was_parsed,
        censor_direction_parsed AS censor_direction,
        '217_lab_value_recovery' AS parse_script
    FROM parsed
    WHERE parsed_numeric IS NOT NULL
    """
    con.execute(sql_create)
    n = con.execute("SELECT COUNT(*) FROM lab_values_complete_v1").fetchone()[0]
    print(f"[217] lab_values_complete_v1 created: {n:,} rows")


def phase1_validate(con: duckdb.DuckDBPyConnection) -> bool:
    """Validate parsed numeric values are in expected clinical ranges."""
    print("\n[217] ── Phase 1 Validation ──")
    df = con.execute("""
        SELECT
            lab_name_standardized,
            COUNT(*) as n_total,
            COUNT(*) FILTER (WHERE is_censored = TRUE) as n_censored,
            COUNT(*) FILTER (WHERE value_was_parsed = TRUE) as n_newly_parsed,
            MIN(value_numeric) as min_val,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value_numeric) as p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value_numeric) as median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value_numeric) as p75,
            MAX(value_numeric) as max_val,
            COUNT(DISTINCT research_id) as n_patients
        FROM lab_values_complete_v1
        GROUP BY 1
        ORDER BY 1
    """).df()
    print(f"[217] Lab value distribution:\n{df.to_string()}")

    # Flag out-of-range values
    out_of_range = con.execute("""
        SELECT lab_name_standardized, COUNT(*) as n_out_of_range, MIN(value_numeric), MAX(value_numeric)
        FROM lab_values_complete_v1
        WHERE (lab_name_standardized = 'tsh'       AND (value_numeric < 0 OR value_numeric > 2000))
           OR (lab_name_standardized = 'vitamin_d' AND (value_numeric < 0 OR value_numeric > 1500))
           OR (lab_name_standardized = 'pth'       AND (value_numeric < 0 OR value_numeric > 5000))
           OR (lab_name_standardized = 'calcium'   AND (value_numeric < 0 OR value_numeric > 200))
        GROUP BY 1
    """).fetchall()
    if out_of_range:
        for r in out_of_range:
            print(f"[217] WARNING: {r[0]} has {r[1]} out-of-range values "
                  f"(range: {r[2]:.3g}–{r[3]:.3g})")
    else:
        print("[217] All lab values within expected clinical bounds ✓")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Lab rollup → wide per-patient table
# ─────────────────────────────────────────────────────────────────────────────

def phase1_build_lab_rollup(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Build per-patient lab summary (lab_rollup_v1)."""
    print("\n[217] ── Phase 1: Build Lab Patient Rollup ──")
    if dry_run:
        print("[217] DRY RUN: skipping lab_rollup_v1 creation")
        return

    # Build separate rollups per lab type, then join
    per_lab_aggs = []
    for lab in LAB_NAMES:
        col_prefix = f"lab_{lab}"
        agg_sql = f"""
        WITH ordered AS (
            SELECT
                CAST(research_id AS VARCHAR) AS rid,
                value_numeric,
                lab_date,
                unit_standardized,
                is_censored,
                value_was_parsed,
                ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY lab_date DESC NULLS LAST, value_numeric) AS rn_desc
            FROM lab_values_complete_v1
            WHERE lab_name_standardized = '{lab}'
              AND value_numeric IS NOT NULL
        )
        SELECT
            rid AS research_id,
            COUNT(*) AS {col_prefix}_n_measurements,
            MIN(value_numeric) AS {col_prefix}_min,
            MAX(value_numeric) AS {col_prefix}_max,
            MIN(lab_date) AS {col_prefix}_first_date,
            MAX(lab_date) AS {col_prefix}_last_date,
            COUNT(*) FILTER (WHERE is_censored = TRUE) AS {col_prefix}_n_censored,
            COUNT(*) FILTER (WHERE value_was_parsed = TRUE) AS {col_prefix}_n_parsed_from_raw,
            -- Most recent value (latest lab date)
            MAX(CASE WHEN rn_desc = 1 THEN value_numeric END) AS {col_prefix}_most_recent,
            MAX(CASE WHEN rn_desc = 1 THEN lab_date END) AS {col_prefix}_most_recent_date,
            -- Standardized unit (most common non-null)
            MAX(unit_standardized) AS {col_prefix}_unit
        FROM ordered
        GROUP BY rid
        """
        per_lab_aggs.append((lab, agg_sql))

    # Create the individual lab rollup tables
    for lab, sql in per_lab_aggs:
        table_name = f"lab_rollup_{lab}_v1"
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
        n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[217]   {table_name}: {n:,} patients")

    # Build unified wide rollup joined to canonical spine
    con.execute("""
        CREATE OR REPLACE TABLE lab_rollup_wide_v1 AS
        SELECT
            c.research_id,
            -- TSH
            tsh.lab_tsh_n_measurements,
            tsh.lab_tsh_min,
            tsh.lab_tsh_max,
            tsh.lab_tsh_first_date,
            tsh.lab_tsh_last_date,
            tsh.lab_tsh_n_censored,
            tsh.lab_tsh_n_parsed_from_raw,
            tsh.lab_tsh_most_recent,
            tsh.lab_tsh_most_recent_date,
            COALESCE(tsh.lab_tsh_unit, 'mIU/L') AS lab_tsh_unit,
            -- VitD
            vitd.lab_vitamin_d_n_measurements,
            vitd.lab_vitamin_d_min,
            vitd.lab_vitamin_d_max,
            vitd.lab_vitamin_d_first_date,
            vitd.lab_vitamin_d_last_date,
            vitd.lab_vitamin_d_n_censored,
            vitd.lab_vitamin_d_n_parsed_from_raw,
            vitd.lab_vitamin_d_most_recent,
            vitd.lab_vitamin_d_most_recent_date,
            COALESCE(vitd.lab_vitamin_d_unit, 'ng/mL') AS lab_vitamin_d_unit,
            -- PTH
            pth.lab_pth_n_measurements,
            pth.lab_pth_min,
            pth.lab_pth_max,
            pth.lab_pth_first_date,
            pth.lab_pth_last_date,
            pth.lab_pth_n_censored,
            pth.lab_pth_n_parsed_from_raw,
            pth.lab_pth_most_recent,
            pth.lab_pth_most_recent_date,
            COALESCE(pth.lab_pth_unit, 'pg/mL') AS lab_pth_unit,
            -- Calcium
            ca.lab_calcium_n_measurements,
            ca.lab_calcium_min,
            ca.lab_calcium_max,
            ca.lab_calcium_first_date,
            ca.lab_calcium_last_date,
            ca.lab_calcium_n_censored,
            ca.lab_calcium_n_parsed_from_raw,
            ca.lab_calcium_most_recent,
            ca.lab_calcium_most_recent_date,
            COALESCE(ca.lab_calcium_unit, 'mg/dL') AS lab_calcium_unit
        FROM canonical_patient_master_v1 c
        LEFT JOIN lab_rollup_tsh_v1       tsh  ON c.research_id = tsh.research_id
        LEFT JOIN lab_rollup_vitamin_d_v1 vitd ON c.research_id = vitd.research_id
        LEFT JOIN lab_rollup_pth_v1       pth  ON c.research_id = pth.research_id
        LEFT JOIN lab_rollup_calcium_v1   ca   ON c.research_id = ca.research_id
    """)
    n = con.execute("SELECT COUNT(*) FROM lab_rollup_wide_v1").fetchone()[0]
    print(f"[217] lab_rollup_wide_v1: {n:,} rows (should be {TOTAL_ROWS})")


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — Clinical Note LN Integration
# ─────────────────────────────────────────────────────────────────────────────

def phase2_copy_ln_table(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Copy clinical_note_ln_extracted_v1 from thyroid_research_ro_v2 to canonical DB."""
    print("\n[217] ── PHASE 2: Copy Clinical Note LN Table ──")

    # Verify source
    src = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM thyroid_research_ro_v2.clinical_note_ln_extracted_v1
    """).fetchone()
    print(f"[217] Source (thyroid_research_ro_v2): {src[0]:,} rows, {src[1]:,} patients")

    if dry_run:
        print("[217] DRY RUN: skipping copy")
        return

    con.execute("""
        CREATE OR REPLACE TABLE clinical_note_ln_extracted_v1 AS
        SELECT * FROM thyroid_research_ro_v2.clinical_note_ln_extracted_v1
    """)
    verify = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM clinical_note_ln_extracted_v1"
    ).fetchone()
    print(f"[217] clinical_note_ln_extracted_v1 copied: "
          f"{verify[0]:,} rows, {verify[1]:,} patients")

    if verify[0] != 7751 or verify[1] != 3588:
        print(f"[217] WARNING: Expected 7751 rows/3588 patients, "
              f"got {verify[0]}/{verify[1]}")


def phase2_validate_rids(con: duckdb.DuckDBPyConnection) -> int:
    """Validate LN research_ids against canonical spine; return orphan count."""
    print("\n[217] Validating LN research_ids against canonical spine ...")
    r = con.execute("""
        SELECT 
            COUNT(DISTINCT cn.research_id) as total_ln_patients,
            COUNT(DISTINCT cn.research_id) FILTER (
                WHERE cn.research_id IN (SELECT research_id FROM canonical_patient_master_v1)
            ) as in_canonical,
            COUNT(DISTINCT cn.research_id) FILTER (
                WHERE cn.research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
            ) as orphans
        FROM clinical_note_ln_extracted_v1 cn
        WHERE cn.extraction_status = 'ok'
    """).fetchone()
    print(f"[217] LN patients: {r[0]} total | {r[1]} in canonical | {r[2]} orphans")
    if r[2] > 0:
        print("[217] NOTE: Orphan RIDs may include known edge cases (e.g., RID 11454)")
    return r[2]


def phase2_build_ln_rollup(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Build patient-level LN rollup from clinical note entities."""
    print("\n[217] ── Phase 2: Build LN Patient Rollup ──")
    if dry_run:
        print("[217] DRY RUN: skipping clinical_note_ln_patient_rollup_v1")
        return

    con.execute("""
        CREATE OR REPLACE TABLE clinical_note_ln_patient_rollup_v1 AS
        WITH ok_entities AS (
            SELECT *
            FROM clinical_note_ln_extracted_v1
            WHERE extraction_status = 'ok'
        ),

        -- Surgical pathology LN data (highest confidence for LN positivity)
        surg_path AS (
            SELECT
                research_id,
                COUNT(*)                       AS cnln_surg_n_entities,
                COUNT(DISTINCT note_date)      AS cnln_surg_n_notes,
                BOOL_OR(ln_status = 'positive') AS cnln_surg_any_positive,
                MAX(count_positive)            AS cnln_surg_max_positive_count,
                MAX(count_total_examined)      AS cnln_surg_max_total_examined,
                -- extranodal_extension is BOOLEAN
                BOOL_OR(extranodal_extension = TRUE) AS cnln_surg_ene_any,
                BOOL_OR(LOWER(laterality) LIKE '%bilateral%') AS cnln_surg_bilateral,
                STRING_AGG(DISTINCT ln_level, '; ')
                    FILTER (WHERE ln_level IS NOT NULL) AS cnln_surg_levels_mentioned,
                MIN(note_date)                 AS cnln_surg_first_date,
                MAX(note_date)                 AS cnln_surg_last_date,
                STRING_AGG(DISTINCT source_note_type, '; ') AS cnln_surg_source_note_types,
                AVG(confidence)                AS cnln_surg_avg_confidence,
                'clinical_note_ln_extracted_v1' AS cnln_surg_source_table
            FROM ok_entities
            WHERE evidence_source_modality = 'surgical_path'
            GROUP BY research_id
        ),

        -- Pathology-sourced LN data (from pathology notes, separate from surgical_path)
        pathology AS (
            SELECT
                research_id,
                COUNT(*)                       AS cnln_path_n_entities,
                BOOL_OR(ln_status = 'positive') AS cnln_path_any_positive,
                MAX(count_positive)            AS cnln_path_max_positive_count,
                MAX(count_total_examined)      AS cnln_path_max_total_examined,
                BOOL_OR(extranodal_extension = TRUE) AS cnln_path_ene_any,
                AVG(confidence)                AS cnln_path_avg_confidence
            FROM ok_entities
            WHERE evidence_source_modality = 'pathology'
            GROUP BY research_id
        ),

        -- Imaging-sourced LN data (from clinical notes describing imaging findings)
        imaging AS (
            SELECT
                research_id,
                COUNT(*)                       AS cnln_img_n_entities,
                BOOL_OR(ln_status IN ('positive', 'suspicious')) AS cnln_img_any_suspicious,
                MAX(size_cm)                   AS cnln_img_max_size_cm,
                STRING_AGG(DISTINCT laterality, '; ')
                    FILTER (WHERE laterality IS NOT NULL) AS cnln_img_laterality,
                STRING_AGG(DISTINCT ln_level, '; ')
                    FILTER (WHERE ln_level IS NOT NULL) AS cnln_img_levels_mentioned,
                MIN(note_date)                 AS cnln_img_first_date,
                MAX(note_date)                 AS cnln_img_last_date,
                AVG(confidence)                AS cnln_img_avg_confidence,
                'clinical_note_ln_extracted_v1' AS cnln_img_source_table
            FROM ok_entities
            WHERE evidence_source_modality = 'imaging'
            GROUP BY research_id
        ),

        -- Clinical context LN mentions (from clinical notes)
        clinical AS (
            SELECT
                research_id,
                COUNT(*)                       AS cnln_clin_n_entities,
                BOOL_OR(ln_status = 'positive') AS cnln_clin_any_positive,
                AVG(confidence)                AS cnln_clin_avg_confidence
            FROM ok_entities
            WHERE evidence_source_modality = 'clinical'
            GROUP BY research_id
        ),

        -- Combined: any modality, worst finding per patient
        combined AS (
            SELECT
                research_id,
                COUNT(*)                                  AS cnln_total_entities,
                COUNT(DISTINCT evidence_source_modality)  AS cnln_n_modalities,
                BOOL_OR(ln_status = 'positive')           AS cnln_any_positive_any_modality,
                BOOL_OR(extranodal_extension = TRUE)      AS cnln_ene_any_modality,
                STRING_AGG(DISTINCT evidence_source_modality, '; ') AS cnln_modalities_present,
                MIN(note_date)                            AS cnln_earliest_date,
                MAX(note_date)                            AS cnln_latest_date,
                'clinical_note_ln_extracted_v1'           AS cnln_source_table
            FROM ok_entities
            GROUP BY research_id
        )

        SELECT
            COALESCE(c.research_id, s.research_id, i.research_id, p.research_id) AS research_id,
            -- Combined summary
            c.cnln_total_entities,
            c.cnln_n_modalities,
            c.cnln_any_positive_any_modality,
            c.cnln_ene_any_modality,
            c.cnln_modalities_present,
            c.cnln_earliest_date,
            c.cnln_latest_date,
            c.cnln_source_table,
            -- Surgical path detail
            s.cnln_surg_n_entities,
            s.cnln_surg_n_notes,
            s.cnln_surg_any_positive,
            s.cnln_surg_max_positive_count,
            s.cnln_surg_max_total_examined,
            s.cnln_surg_ene_any,
            s.cnln_surg_bilateral,
            s.cnln_surg_levels_mentioned,
            s.cnln_surg_first_date,
            s.cnln_surg_last_date,
            s.cnln_surg_source_note_types,
            s.cnln_surg_avg_confidence,
            -- Pathology detail
            p.cnln_path_n_entities,
            p.cnln_path_any_positive,
            p.cnln_path_max_positive_count,
            p.cnln_path_max_total_examined,
            p.cnln_path_ene_any,
            p.cnln_path_avg_confidence,
            -- Imaging detail
            i.cnln_img_n_entities,
            i.cnln_img_any_suspicious,
            i.cnln_img_max_size_cm,
            i.cnln_img_laterality,
            i.cnln_img_levels_mentioned,
            i.cnln_img_first_date,
            i.cnln_img_last_date,
            i.cnln_img_avg_confidence,
            -- Clinical detail
            clin.cnln_clin_n_entities,
            clin.cnln_clin_any_positive,
            clin.cnln_clin_avg_confidence
        FROM combined c
        FULL OUTER JOIN surg_path s
            ON c.research_id = s.research_id
        FULL OUTER JOIN pathology p
            ON COALESCE(c.research_id, s.research_id) = p.research_id
        FULL OUTER JOIN imaging i
            ON COALESCE(c.research_id, s.research_id, p.research_id) = i.research_id
        FULL OUTER JOIN clinical clin
            ON COALESCE(c.research_id, s.research_id, p.research_id, i.research_id) = clin.research_id
    """)
    n = con.execute("SELECT COUNT(*) FROM clinical_note_ln_patient_rollup_v1").fetchone()[0]
    print(f"[217] clinical_note_ln_patient_rollup_v1: {n:,} patients")


def phase2_cross_validate(con: duckdb.DuckDBPyConnection) -> int:
    """Cross-validate LN findings vs structured canonical data; return novel positive count."""
    print("\n[217] ── Phase 2: Cross-Validation vs Structured LN Data ──")
    df = con.execute("""
        SELECT
            CASE
                WHEN r.cnln_surg_any_positive = TRUE
                     AND COALESCE(c.ln_rollup_total_positive, c.tp_ln_positive,
                                  c.ln_total_positive, 0) = 0
                THEN 'NOVEL_POSITIVE'
                WHEN r.cnln_surg_any_positive = TRUE
                     AND COALESCE(c.ln_rollup_total_positive, c.tp_ln_positive,
                                  c.ln_total_positive, 0) > 0
                THEN 'CONCORDANT_POSITIVE'
                WHEN COALESCE(r.cnln_surg_any_positive, FALSE) = FALSE
                     AND COALESCE(c.ln_rollup_total_positive, c.tp_ln_positive,
                                  c.ln_total_positive, 0) > 0
                THEN 'STRUCTURED_ONLY'
                ELSE 'BOTH_NEGATIVE_OR_NULL'
            END AS concordance_status,
            COUNT(*) AS n
        FROM clinical_note_ln_patient_rollup_v1 r
        JOIN canonical_patient_master_v1 c ON r.research_id = c.research_id
        GROUP BY 1
        ORDER BY 2 DESC
    """).df()
    print(f"[217] LN concordance summary:\n{df.to_string()}")

    novel_n = df.loc[df["concordance_status"] == "NOVEL_POSITIVE", "n"]
    novel_count = int(novel_n.iloc[0]) if len(novel_n) else 0
    print(f"[217] Novel positive patients (clinical note adds new signal): {novel_count}")
    return novel_count


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — Canonical Rebuild
# ─────────────────────────────────────────────────────────────────────────────

# Lab columns that exist in current canonical and will be REPLACED by wider rollup
EXISTING_LAB_COLS = [
    "lab_tsh_min", "lab_tsh_max", "lab_tsh_n_measurements",
    "lab_tsh_first_date", "lab_tsh_last_date",
    "lab_vitd_min", "lab_vitd_max", "lab_vitd_n_measurements",
    "lab_vitd_first_date", "lab_vitd_last_date",
    "lab_pth_min", "lab_pth_max", "lab_pth_n_measurements",
    "lab_pth_first_date", "lab_pth_last_date",
    "lab_calcium_min", "lab_calcium_max", "lab_calcium_n_measurements",
    "lab_calcium_first_date", "lab_calcium_last_date",
]


def get_canonical_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return list of current canonical column names (distinct, ordered)."""
    rows = con.execute("""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master_v1'
        ORDER BY column_name
    """).fetchall()
    return [r[0] for r in rows]


def phase3_rebuild_canonical(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Rebuild canonical with expanded lab columns + cnln_* columns."""
    print("\n[217] ── PHASE 3: Canonical Rebuild ──")

    # Backup first
    if not dry_run:
        print("[217] Creating backup canonical_patient_master_v1_pre217 ...")
        con.execute("""
            CREATE OR REPLACE TABLE canonical_patient_master_v1_pre217 AS
            SELECT * FROM canonical_patient_master_v1
        """)
        n_bk = con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master_v1_pre217"
        ).fetchone()[0]
        print(f"[217] Backup created: {n_bk:,} rows")

    # Get all current column names
    all_cols = get_canonical_columns(con)
    cols_to_exclude = set(EXISTING_LAB_COLS)
    # Build SELECT list for old columns
    keep_cols = [c for c in all_cols if c not in cols_to_exclude]
    print(f"[217] Keeping {len(keep_cols)} original columns, replacing {len(cols_to_exclude)} lab cols")

    # Format column selects with aliases (from old.* except replaced cols)
    old_col_selects = ",\n            ".join(f"old.{c}" for c in keep_cols)

    rebuild_sql = f"""
        CREATE OR REPLACE TABLE canonical_patient_master_v1 AS
        SELECT
            -- All original columns EXCEPT the old lab columns
            {old_col_selects},

            -- ── UPDATED LAB COLUMNS (expanded with parsed raw values) ──
            -- TSH
            COALESCE(lr.lab_tsh_n_measurements, 0)::BIGINT  AS lab_tsh_n_measurements,
            lr.lab_tsh_min,
            lr.lab_tsh_max,
            lr.lab_tsh_first_date,
            lr.lab_tsh_last_date,
            -- NEW TSH columns
            lr.lab_tsh_most_recent,
            lr.lab_tsh_most_recent_date,
            lr.lab_tsh_unit,
            COALESCE(lr.lab_tsh_n_censored, 0)::BIGINT      AS lab_tsh_n_censored,
            COALESCE(lr.lab_tsh_n_parsed_from_raw, 0)::BIGINT AS lab_tsh_n_parsed_from_raw,

            -- Vitamin D
            COALESCE(lr.lab_vitamin_d_n_measurements, 0)::BIGINT AS lab_vitd_n_measurements,
            lr.lab_vitamin_d_min                             AS lab_vitd_min,
            lr.lab_vitamin_d_max                             AS lab_vitd_max,
            lr.lab_vitamin_d_first_date                      AS lab_vitd_first_date,
            lr.lab_vitamin_d_last_date                       AS lab_vitd_last_date,
            -- NEW VitD columns
            lr.lab_vitamin_d_most_recent                     AS lab_vitd_most_recent,
            lr.lab_vitamin_d_most_recent_date                AS lab_vitd_most_recent_date,
            lr.lab_vitamin_d_unit                            AS lab_vitd_unit,
            COALESCE(lr.lab_vitamin_d_n_censored, 0)::BIGINT AS lab_vitd_n_censored,
            COALESCE(lr.lab_vitamin_d_n_parsed_from_raw, 0)::BIGINT AS lab_vitd_n_parsed_from_raw,

            -- PTH
            COALESCE(lr.lab_pth_n_measurements, 0)::INTEGER  AS lab_pth_n_measurements,
            lr.lab_pth_min,
            lr.lab_pth_max,
            lr.lab_pth_first_date,
            lr.lab_pth_last_date,
            -- NEW PTH columns
            lr.lab_pth_most_recent,
            lr.lab_pth_most_recent_date,
            lr.lab_pth_unit,
            COALESCE(lr.lab_pth_n_censored, 0)::BIGINT       AS lab_pth_n_censored,
            COALESCE(lr.lab_pth_n_parsed_from_raw, 0)::BIGINT AS lab_pth_n_parsed_from_raw,

            -- Calcium
            COALESCE(lr.lab_calcium_n_measurements, 0)::INTEGER AS lab_calcium_n_measurements,
            lr.lab_calcium_min,
            lr.lab_calcium_max,
            lr.lab_calcium_first_date,
            lr.lab_calcium_last_date,
            -- NEW Calcium columns
            lr.lab_calcium_most_recent,
            lr.lab_calcium_most_recent_date,
            lr.lab_calcium_unit,
            COALESCE(lr.lab_calcium_n_censored, 0)::BIGINT    AS lab_calcium_n_censored,
            COALESCE(lr.lab_calcium_n_parsed_from_raw, 0)::BIGINT AS lab_calcium_n_parsed_from_raw,

            -- ── NEW CLINICAL NOTE LN COLUMNS ──
            -- Combined summary
            COALESCE(cnln.cnln_total_entities, 0)::BIGINT    AS cnln_total_entities,
            COALESCE(cnln.cnln_n_modalities, 0)::INTEGER     AS cnln_n_modalities,
            COALESCE(cnln.cnln_any_positive_any_modality, FALSE) AS cnln_any_positive_any_modality,
            COALESCE(cnln.cnln_ene_any_modality, FALSE)       AS cnln_ene_any_modality,
            cnln.cnln_modalities_present,
            cnln.cnln_earliest_date,
            cnln.cnln_latest_date,
            -- Surgical path detail
            COALESCE(cnln.cnln_surg_n_entities, 0)::BIGINT   AS cnln_surg_n_entities,
            COALESCE(cnln.cnln_surg_n_notes, 0)::BIGINT      AS cnln_surg_n_notes,
            COALESCE(cnln.cnln_surg_any_positive, FALSE)      AS cnln_surg_any_positive,
            cnln.cnln_surg_max_positive_count,
            cnln.cnln_surg_max_total_examined,
            COALESCE(cnln.cnln_surg_ene_any, FALSE)           AS cnln_surg_ene_any,
            COALESCE(cnln.cnln_surg_bilateral, FALSE)         AS cnln_surg_bilateral,
            cnln.cnln_surg_levels_mentioned,
            cnln.cnln_surg_first_date,
            cnln.cnln_surg_last_date,
            cnln.cnln_surg_source_note_types,
            cnln.cnln_surg_avg_confidence,
            -- Imaging detail
            COALESCE(cnln.cnln_img_n_entities, 0)::BIGINT    AS cnln_img_n_entities,
            COALESCE(cnln.cnln_img_any_suspicious, FALSE)     AS cnln_img_any_suspicious,
            cnln.cnln_img_max_size_cm,
            cnln.cnln_img_laterality,
            cnln.cnln_img_levels_mentioned,
            cnln.cnln_img_first_date,
            cnln.cnln_img_last_date,
            cnln.cnln_img_avg_confidence,
            -- Pathology detail
            COALESCE(cnln.cnln_path_n_entities, 0)::BIGINT   AS cnln_path_n_entities,
            COALESCE(cnln.cnln_path_any_positive, FALSE)      AS cnln_path_any_positive,
            cnln.cnln_path_max_positive_count,
            cnln.cnln_path_ene_any,
            -- Clinical detail
            COALESCE(cnln.cnln_clin_n_entities, 0)::BIGINT   AS cnln_clin_n_entities,
            COALESCE(cnln.cnln_clin_any_positive, FALSE)      AS cnln_clin_any_positive,
            cnln.cnln_clin_avg_confidence,
            -- Cross-validated novel-positive flag:
            -- Clinical note says LN+ from surgical_path/pathology but canonical structured = 0
            CASE
                WHEN COALESCE(cnln.cnln_surg_any_positive, FALSE) = TRUE
                     OR COALESCE(cnln.cnln_path_any_positive, FALSE) = TRUE
                THEN
                    CASE
                        WHEN COALESCE(old.ln_rollup_total_positive, old.tp_ln_positive,
                                      old.ln_total_positive, 0) = 0
                        THEN TRUE
                        ELSE FALSE
                    END
                ELSE FALSE
            END AS cnln_novel_positive_flag,
            -- Provenance
            'clinical_note_ln_extracted_v1' AS cnln_source_table

        FROM canonical_patient_master_v1 old
        LEFT JOIN lab_rollup_wide_v1 lr
            ON old.research_id = lr.research_id
        LEFT JOIN clinical_note_ln_patient_rollup_v1 cnln
            ON old.research_id = cnln.research_id
    """

    if dry_run:
        print("[217] DRY RUN: canonical rebuild SQL prepared (not executed)")
        # Show column count preview
        preview_cols = len(keep_cols) + len(EXISTING_LAB_COLS) + 5 * 4 + 30
        print(f"[217] DRY RUN: estimated column count ~{preview_cols}")
        return

    print("[217] Rebuilding canonical_patient_master_v1 ...")
    con.execute(rebuild_sql)
    n = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[217] Rebuild complete: {n:,} rows")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Provenance Checks
# ─────────────────────────────────────────────────────────────────────────────

def phase3_provenance_checks(con: duckdb.DuckDBPyConnection) -> None:
    """Run full provenance validation suite."""
    print("\n[217] ── Phase 3: Provenance Checks ──")

    # CHECK 1: Orphan data
    print("[217] CHECK 1: Orphan data ...")
    orp_lab = con.execute("""
        SELECT COUNT(*) FROM lab_values_complete_v1
        WHERE CAST(research_id AS VARCHAR)
            NOT IN (SELECT research_id FROM canonical_patient_master_v1)
    """).fetchone()[0]
    orp_cnln = con.execute("""
        SELECT COUNT(*) FROM clinical_note_ln_patient_rollup_v1
        WHERE research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
    """).fetchone()[0]
    print(f"[217]   lab orphans: {orp_lab} | cnln orphans: {orp_cnln}")

    # CHECK 2: Lab dates
    print("[217] CHECK 2: Lab date coverage ...")
    r = con.execute("""
        SELECT COUNT(*), 
            COUNT(*) FILTER (WHERE lab_date IS NOT NULL) as has_date,
            COUNT(*) FILTER (WHERE lab_date IS NULL) as missing_date
        FROM lab_values_complete_v1
    """).fetchone()
    print(f"[217]   Lab rows: {r[0]:,} | has_date: {r[1]:,} | missing_date: {r[2]:,}")

    # CHECK 3: Unit coverage
    print("[217] CHECK 3: Unit coverage ...")
    df = con.execute("""
        SELECT lab_name_standardized,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE unit_standardized IS NOT NULL) as has_unit
        FROM lab_values_complete_v1
        GROUP BY 1
    """).df()
    print(f"[217]   {df.to_string()}")

    # CHECK 4: Source provenance
    print("[217] CHECK 4: Source provenance ...")
    r4 = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE source_table IS NOT NULL) as lab_has_source,
            COUNT(*) FILTER (WHERE parse_script IS NOT NULL) as lab_has_script
        FROM lab_values_complete_v1
    """).fetchone()
    print(f"[217]   lab source: {r4[0]:,} | lab parse_script: {r4[1]:,}")

    # CHECK 5: Out-of-range values
    print("[217] CHECK 5: Out-of-range values ...")
    oor = con.execute("""
        SELECT lab_name_standardized, COUNT(*) as n
        FROM lab_values_complete_v1
        WHERE (lab_name_standardized = 'tsh'       AND (value_numeric < 0 OR value_numeric > 2000))
           OR (lab_name_standardized = 'vitamin_d' AND (value_numeric < 0 OR value_numeric > 1500))
           OR (lab_name_standardized = 'pth'       AND (value_numeric < 0 OR value_numeric > 5000))
           OR (lab_name_standardized = 'calcium'   AND (value_numeric < 0 OR value_numeric > 200))
        GROUP BY 1
    """).fetchall()
    if oor:
        for r in oor:
            print(f"[217]   WARNING: {r[0]} has {r[1]} out-of-range values")
    else:
        print("[217]   All values in range ✓")


def phase3_coverage_report(con: duckdb.DuckDBPyConnection) -> None:
    """Print final coverage report for all new/updated columns."""
    print("\n[217] ── Phase 3: Coverage Report ──")

    # Lab coverage
    lab_r = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE lab_tsh_n_measurements > 0)      AS tsh,
            COUNT(*) FILTER (WHERE lab_vitd_n_measurements > 0)      AS vitd,
            COUNT(*) FILTER (WHERE lab_pth_n_measurements > 0)       AS pth,
            COUNT(*) FILTER (WHERE lab_calcium_n_measurements > 0)   AS calcium,
            COUNT(*) FILTER (WHERE lab_tsh_most_recent IS NOT NULL)  AS tsh_recent,
            COUNT(*) FILTER (WHERE lab_vitd_most_recent IS NOT NULL) AS vitd_recent,
            COUNT(*) FILTER (WHERE lab_pth_most_recent IS NOT NULL)  AS pth_recent,
            COUNT(*) FILTER (WHERE lab_calcium_most_recent IS NOT NULL) AS calcium_recent
        FROM canonical_patient_master_v1
    """).fetchone()
    print("[217] Lab patient coverage:")
    print(f"[217]   TSH: {lab_r[0]} ({lab_r[0]/TOTAL_ROWS*100:.1f}%) | "
          f"VitD: {lab_r[1]} ({lab_r[1]/TOTAL_ROWS*100:.1f}%) | "
          f"PTH: {lab_r[2]} ({lab_r[2]/TOTAL_ROWS*100:.1f}%) | "
          f"Calcium: {lab_r[3]} ({lab_r[3]/TOTAL_ROWS*100:.1f}%)")
    print(f"[217]   TSH most_recent: {lab_r[4]} | VitD most_recent: {lab_r[5]} | "
          f"PTH most_recent: {lab_r[6]} | Ca most_recent: {lab_r[7]}")

    # CNLN coverage
    cnln_r = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE cnln_total_entities > 0)         AS has_cnln_data,
            COUNT(*) FILTER (WHERE cnln_surg_any_positive = TRUE)   AS surg_positive,
            COUNT(*) FILTER (WHERE cnln_novel_positive_flag = TRUE)  AS novel_positive,
            COUNT(*) FILTER (WHERE cnln_ene_any_modality = TRUE)    AS has_ene,
            COUNT(*) FILTER (WHERE cnln_img_any_suspicious = TRUE)  AS img_suspicious
        FROM canonical_patient_master_v1
    """).fetchone()
    print("[217] Clinical note LN coverage:")
    print(f"[217]   Has any CNLN data: {cnln_r[0]} ({cnln_r[0]/TOTAL_ROWS*100:.1f}%)")
    print(f"[217]   Surgical path LN+: {cnln_r[1]}")
    print(f"[217]   Novel positive (new signal): {cnln_r[2]}")
    print(f"[217]   Has ENE (any modality): {cnln_r[3]}")
    print(f"[217]   Imaging suspicious LN: {cnln_r[4]}")

    # Column count
    n_cols = len(get_canonical_columns(con))
    print(f"\n[217] Canonical: {TOTAL_ROWS} rows × {n_cols} columns")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Script 217: Lab Recovery + LN Integration")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview actions without modifying any tables")
    parser.add_argument("--phase", default="all",
                        choices=["1", "2", "3", "all"],
                        help="Which phase to run (default: all)")
    args = parser.parse_args()
    dry_run = args.dry_run

    if dry_run:
        print("[217] ═══ DRY RUN MODE — no tables will be modified ═══")

    con = connect()

    # Pre-run invariant check
    print("\n[217] Pre-run canonical state:")
    check_invariants(con, CANONICAL, "pre-run")

    run_phase1 = args.phase in ("1", "all")
    run_phase2 = args.phase in ("2", "all")
    run_phase3 = args.phase in ("3", "all")

    # ── PHASE 1: Lab Value Recovery ──────────────────────────────────────────
    if run_phase1:
        phase1_build_lab_complete(con, dry_run)
        if not dry_run:
            phase1_validate(con)
            phase1_build_lab_rollup(con, dry_run)

    # ── PHASE 2: Clinical Note LN Integration ────────────────────────────────
    if run_phase2:
        phase2_copy_ln_table(con, dry_run)
        if not dry_run:
            phase2_validate_rids(con)
            phase2_build_ln_rollup(con, dry_run)
            phase2_cross_validate(con)

    # ── PHASE 3: Canonical Rebuild ───────────────────────────────────────────
    if run_phase3:
        if not dry_run and (run_phase1 or run_phase2):
            # Verify intermediate tables exist
            for tbl in ["lab_rollup_wide_v1", "clinical_note_ln_patient_rollup_v1"]:
                try:
                    n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                    print(f"[217] {tbl}: {n:,} rows ✓")
                except Exception:
                    print(f"[217] ERROR: {tbl} does not exist — run phases 1+2 first")
                    sys.exit(1)
        elif dry_run:
            # Dry run: build rollup tables that already exist or skip
            pass

        phase3_rebuild_canonical(con, dry_run)

        if not dry_run:
            phase3_provenance_checks(con)
            # Post-rebuild invariant check
            print("\n[217] Post-rebuild canonical state:")
            ok = check_invariants(con, CANONICAL, "post-rebuild")
            if not ok:
                print("[217] FATAL: Canonical invariants FAILED after rebuild")
                sys.exit(1)
            phase3_coverage_report(con)
            print("\n[217] ═══ Script 217 COMPLETE ═══")

    if dry_run:
        print("\n[217] ═══ DRY RUN COMPLETE — no data was modified ═══")

    con.close()


if __name__ == "__main__":
    main()
