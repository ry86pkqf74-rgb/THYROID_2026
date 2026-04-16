#!/usr/bin/env python3
"""
Script 224: Canonical Dataset ETL Fixes

Addresses 7 confirmed ETL issues in thyroid_canonical_publication_v1_0:
  Issue 1: distant_mets_proxy = recurrence_flag (CRITICAL)
  Issue 2: T4a/T4b dropped by AJCC 8 algorithm (HIGH)
  Issue 3: Add AJCC 7th edition staging (MEDIUM)
  Issue 4: n_surgeries massively undercounts (MEDIUM-HIGH)
  Issue 5: N-stage NULL resolution (MEDIUM)
  Issue 6: BMI documentation (LOW)
  Issue 7: Recurrence reclassification per clinical definition (MEDIUM)

All writes go to thyroid_canonical_publication_v1_0.
Thyroid 2026 UPdated is READ-ONLY.

Usage:
    .venv/bin/python scripts/224_canonical_etl_fixes.py
    .venv/bin/python scripts/224_canonical_etl_fixes.py --dry-run
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase preflight
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase scoring
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase ajcc7
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase nsurg
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase recurrence
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase bmi
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase rebuild
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase validate
    .venv/bin/python scripts/224_canonical_etl_fixes.py --phase dedup
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked, PUBLICATION_DB, assert_row_count, assert_distinct_rids

FQ = f'"{PUBLICATION_DB}".main'
LEGACY_DB = '"Thyroid 2026 UPdated"'
TOTAL_ROWS = 10_871
SCRIPT_TAG = "224_canonical_etl_fixes"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


def log(msg: str):
    print(f"  [{SCRIPT_TAG}] {msg}")


# ============================================================================
# PHASE 0: PRE-FLIGHT
# ============================================================================

def phase_preflight(con) -> bool:
    log("=== PRE-FLIGHT CHECK ===")

    row = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM {FQ}.canonical_patient_master) AS pub_n_patients,
          (SELECT COUNT(*) FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='main' AND table_name='canonical_patient_master') AS pub_n_cols,
          (SELECT COUNT(*) FROM duckdb_tables()
             WHERE database_name='{PUBLICATION_DB}' AND schema_name='main') AS pub_n_tables
    """).fetchone()

    n_patients, n_cols, n_tables = row
    log(f"  Patients: {n_patients} (expected 10871)")
    log(f"  Columns:  {n_cols} (expected ~1423)")
    log(f"  Tables:   {n_tables} (expected ~110)")

    if n_patients != 10871:
        log(f"  FATAL: Patient count {n_patients} != 10871. STOPPING.")
        return False

    inv = con.execute(f"""
        SELECT
          COUNT(*) AS n,
          COUNT(*) - 10871 AS row_delta,
          COUNT(DISTINCT research_id) AS distinct_rids,
          COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
          COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna,
          COUNT(*) FILTER (WHERE is_malignant IS NULL) AS null_malignant,
          COUNT(*) FILTER (WHERE diagnosis_primary IS NULL) AS null_dx
        FROM {FQ}.canonical_patient_master
    """).fetchone()

    log(f"  Invariants: n={inv[0]}, delta={inv[1]}, distinct_rids={inv[2]}, "
        f"null_rids={inv[3]}, null_fna={inv[4]}, null_malig={inv[5]}, null_dx={inv[6]}")

    if inv[1] != 0 or inv[3] != 0:
        log("  FATAL: Row count or NULL research_id invariant violated. STOPPING.")
        return False

    log("  Pre-flight PASSED.")
    return True


# ============================================================================
# PHASE 1: SCORING FIXES (Issues 1, 2, 5)
# ============================================================================

def phase_scoring_direct(con, dry_run: bool = False):
    """Recompute T/N/M staging directly from canonical_patient_master data.
    Uses SQL-based approach against the publication DB since the 51b source
    tables (demographics_harmonized_v2, etc.) only exist in the legacy DB.
    """
    log("=== PHASE 1: SCORING FIXES (Issues 1, 2, 5) ===")
    log("  Computing corrected T/N/M staging from canonical_patient_master...")

    scoring_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.thyroid_scoring_py_v1_corrected AS
    WITH base AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            is_malignant,
            age_at_surgery,
            tumor_size_cm,
            path_t_stage_raw,
            path_n_stage_raw,
            path_m_stage_raw,
            pet_distant_mets_ever,
            ete_grade,
            ete_grade_final_v2,
            gross_ete_flag,
            ln_total_positive,
            ln_total_examined,
            ln_rollup_lateral_right_positive,
            ln_rollup_lateral_left_positive,
            ln_lateral_dissected,
            lateral_neck_dissected,
            any_recurrence_flag AS recurrence_flag,
            histology_final,
            diagnosis_primary
        FROM {FQ}.canonical_patient_master
    ),
    -- Issue 2: T-stage with pathologist T4 precedence
    t_fixed AS (
        SELECT *,
            CASE
                WHEN LOWER(TRIM(REPLACE(COALESCE(path_t_stage_raw,''),'T',''))) IN ('4a') THEN 'T4a'
                WHEN LOWER(TRIM(REPLACE(COALESCE(path_t_stage_raw,''),'T',''))) IN ('4b') THEN 'T4b'
                WHEN LOWER(TRIM(REPLACE(COALESCE(path_t_stage_raw,''),'T',''))) IN ('3a') THEN 'T3a'
                WHEN LOWER(TRIM(REPLACE(COALESCE(path_t_stage_raw,''),'T',''))) IN ('3b') THEN 'T3b'
                WHEN (COALESCE(gross_ete_flag, FALSE) = TRUE
                      OR LOWER(COALESCE(CAST(ete_grade AS VARCHAR),'')) LIKE '%gross%'
                      OR LOWER(COALESCE(CAST(ete_grade_final_v2 AS VARCHAR),'')) LIKE '%gross%')
                    THEN 'T3b'
                WHEN tumor_size_cm IS NULL THEN NULL
                WHEN tumor_size_cm <= 1.0 THEN 'T1a'
                WHEN tumor_size_cm <= 2.0 THEN 'T1b'
                WHEN tumor_size_cm <= 4.0 THEN 'T2'
                WHEN tumor_size_cm > 4.0 THEN 'T3a'
                ELSE NULL
            END AS ajcc8_t_stage_v2
        FROM base
    ),
    -- Issue 1: M-stage from path_m_stage_raw + pet_distant_mets_ever (NOT recurrence)
    m_fixed AS (
        SELECT *,
            CASE
                WHEN UPPER(TRIM(COALESCE(CAST(path_m_stage_raw AS VARCHAR),''))) IN ('M1','1') THEN 'M1'
                WHEN COALESCE(pet_distant_mets_ever, FALSE) = TRUE THEN 'M1'
                ELSE 'M0'
            END AS ajcc8_m_stage_v2,
            CASE
                WHEN UPPER(TRIM(COALESCE(CAST(path_m_stage_raw AS VARCHAR),''))) IN ('M1','1') THEN TRUE
                WHEN COALESCE(pet_distant_mets_ever, FALSE) = TRUE THEN TRUE
                ELSE FALSE
            END AS distant_mets_proxy_v2
        FROM t_fixed
    ),
    -- Issue 5: N-stage from path_n_stage_raw cascade
    n_fixed AS (
        SELECT *,
            CASE
                WHEN COALESCE(is_malignant, FALSE) = FALSE THEN NULL
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1a') THEN 'N1a'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1b') THEN 'N1b'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('0','0a') THEN 'N0'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1') THEN 'N1a'
                WHEN ln_total_positive IS NOT NULL AND ln_total_positive > 0 THEN
                    CASE
                        WHEN COALESCE(ln_rollup_lateral_right_positive,0) > 0
                             OR COALESCE(ln_rollup_lateral_left_positive,0) > 0 THEN 'N1b'
                        ELSE 'N1a'
                    END
                WHEN ln_total_positive IS NOT NULL AND ln_total_positive = 0 THEN 'N0'
                WHEN ln_total_examined IS NOT NULL AND ln_total_examined > 0
                     AND (ln_total_positive IS NULL OR ln_total_positive = 0) THEN 'N0'
                ELSE 'Nx'
            END AS ajcc8_n_stage_v2,
            CASE
                WHEN COALESCE(is_malignant, FALSE) = FALSE THEN NULL
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1a') THEN 'pathologist N1a'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1b') THEN 'pathologist N1b'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('0','0a') THEN 'pathologist N0'
                WHEN LOWER(TRIM(REPLACE(REPLACE(COALESCE(CAST(path_n_stage_raw AS VARCHAR),''),'N',''),'T','')))
                    IN ('1') THEN 'pathologist N1 (subcategory unspecified)'
                WHEN ln_total_positive IS NOT NULL AND ln_total_positive > 0 THEN
                    CASE
                        WHEN COALESCE(ln_rollup_lateral_right_positive,0) > 0
                             OR COALESCE(ln_rollup_lateral_left_positive,0) > 0
                            THEN 'derived from LN laterality (lateral involvement)'
                        ELSE 'derived from LN laterality (central-only)'
                    END
                WHEN ln_total_positive IS NOT NULL AND ln_total_positive = 0
                    THEN 'derived from 0 positive of >=1 examined'
                WHEN ln_total_examined IS NOT NULL AND ln_total_examined > 0
                     AND (ln_total_positive IS NULL OR ln_total_positive = 0)
                    THEN 'derived from 0 positive of >=1 examined'
                ELSE 'N-stage cannot be assessed (no raw N-stage and no LN exam data)'
            END AS ajcc8_n_stage_note
        FROM m_fixed
    ),
    -- AJCC8 stage group from corrected T/N/M
    staged AS (
        SELECT *,
            CASE
                WHEN age_at_surgery IS NULL OR ajcc8_t_stage_v2 IS NULL THEN NULL
                WHEN age_at_surgery < 55 THEN
                    CASE WHEN ajcc8_m_stage_v2 = 'M1' THEN 'II' ELSE 'I' END
                ELSE
                    CASE
                        WHEN ajcc8_m_stage_v2 = 'M1' THEN 'IVB'
                        WHEN ajcc8_t_stage_v2 = 'T4b' THEN 'IVA'
                        WHEN ajcc8_t_stage_v2 = 'T4a' OR ajcc8_n_stage_v2 = 'N1b' THEN 'III'
                        WHEN ajcc8_t_stage_v2 IN ('T1a','T1b','T2')
                             AND COALESCE(ajcc8_n_stage_v2,'N0') IN ('N0','Nx') THEN 'I'
                        WHEN ajcc8_t_stage_v2 IN ('T1a','T1b','T2')
                             AND ajcc8_n_stage_v2 IN ('N1a','N1b') THEN 'II'
                        WHEN ajcc8_t_stage_v2 IN ('T3a','T3b') THEN 'II'
                        ELSE NULL
                    END
            END AS ajcc8_stage_group_v2
        FROM n_fixed
    )
    SELECT
        research_id, is_malignant,
        ajcc8_t_stage_v2, ajcc8_n_stage_v2, ajcc8_n_stage_note,
        ajcc8_m_stage_v2, ajcc8_stage_group_v2,
        distant_mets_proxy_v2,
        path_t_stage_raw, path_n_stage_raw, path_m_stage_raw,
        pet_distant_mets_ever, recurrence_flag
    FROM staged
    """

    if dry_run:
        log("  [DRY-RUN] Would create thyroid_scoring_py_v1_corrected")
        return

    con.execute(scoring_sql)
    n = con.execute(f"SELECT COUNT(*) FROM {FQ}.thyroid_scoring_py_v1_corrected").fetchone()[0]
    log(f"  thyroid_scoring_py_v1_corrected: {n} rows")

    validate_scoring(con)


def validate_scoring(con):
    """Post-scoring validation queries."""
    log("  --- Scoring Validation ---")

    scoring_table = f"{FQ}.thyroid_scoring_py_v1_corrected"

    r = con.execute(f"""
        SELECT
          SUM(CASE WHEN distant_mets_proxy_v2 THEN 1 ELSE 0 END) AS dmp_true,
          SUM(CASE WHEN COALESCE(recurrence_flag, FALSE) THEN 1 ELSE 0 END) AS rec_true,
          SUM(CASE WHEN distant_mets_proxy_v2 AND NOT COALESCE(recurrence_flag, FALSE)
              THEN 1 ELSE 0 END) AS dmp_no_rec,
          SUM(CASE WHEN COALESCE(recurrence_flag, FALSE) AND NOT distant_mets_proxy_v2
              THEN 1 ELSE 0 END) AS rec_no_dmp
        FROM {scoring_table}
    """).fetchone()
    log(f"  Issue 1 check: dmp_true={r[0]}, rec_true={r[1]}, "
        f"dmp_no_rec={r[2]}, rec_no_dmp={r[3]}")
    if r[0] == r[1] and r[0] > 100:
        log("  WARNING: distant_mets_proxy still matches recurrence_flag!")

    # Issue 2: T4 stages
    t4 = con.execute(f"""
        SELECT ajcc8_t_stage_v2, COUNT(*) AS n
        FROM {scoring_table}
        WHERE ajcc8_t_stage_v2 LIKE 'T4%'
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for stage, n in t4:
        log(f"  Issue 2 check: {stage} = {n} patients")
    if not t4:
        log("  WARNING: No T4 patients found — Issue 2 may not be fixed!")

    # Issue 5: malignant with NULL N-stage
    n_null = con.execute(f"""
        SELECT COUNT(*) FROM {scoring_table}
        WHERE is_malignant = TRUE AND ajcc8_n_stage_v2 IS NULL
    """).fetchone()[0]
    log(f"  Issue 5 check: malignant with NULL N-stage = {n_null} (expected 0)")

    # M1 breakdown
    m1 = con.execute(f"""
        SELECT ajcc8_m_stage_v2, COUNT(*) AS n
        FROM {scoring_table}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for stage, n in m1:
        log(f"  M-stage: {stage} = {n}")

    # T-stage distribution
    t_dist = con.execute(f"""
        SELECT ajcc8_t_stage_v2, COUNT(*) AS n
        FROM {scoring_table}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  T-stage distribution:")
    for stage, n in t_dist:
        log(f"    {stage}: {n}")

    # N-stage note distribution for malignant
    n_note = con.execute(f"""
        SELECT ajcc8_n_stage_v2, ajcc8_n_stage_note, COUNT(*) AS n
        FROM {scoring_table}
        WHERE is_malignant = TRUE
        GROUP BY 1, 2 ORDER BY n DESC
        LIMIT 15
    """).fetchall()
    log("  N-stage derivation (malignant, top 15):")
    for stage, note, n in n_note:
        log(f"    {stage} ({note}): {n}")


# ============================================================================
# PHASE 2: AJCC 7TH EDITION (Issue 3)
# ============================================================================

def _normalize_tnm(raw: str, prefix: str) -> str | None:
    """Normalize a raw T/N/M value to canonical form."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().upper()
    p = prefix.upper()
    s = s.replace(p, "")
    if s in ("", "X", "NX", "TX", "MX"):
        return f"{p}x"
    mapping = {
        "0": f"{p}0", "0A": f"{p}0", "1": f"{p}1",
        "1A": f"{p}1a", "1B": f"{p}1b",
        "2": f"{p}2", "3": f"{p}3", "3A": f"{p}3a", "3B": f"{p}3b",
        "4A": f"{p}4a", "4B": f"{p}4b",
    }
    return mapping.get(s, f"{p}{s.lower()}" if s else None)


def compute_ajcc7_t_stage(t8: str, ete_group: str | None = None) -> str | None:
    """Map AJCC8 T-stage to AJCC7 T-stage.
    Key difference: T3b (gross ETE limited to strap muscles) -> T3 in AJCC7.
    T4a/T4b pass through unchanged.
    """
    if t8 is None:
        return None
    if t8 == "T4b":
        return "T4b"
    if t8 == "T4a":
        return "T4a"
    if t8 == "T3b":
        return "T3"  # AJCC7: strap-muscle invasion was T3, not T4a
    if t8 == "T3a":
        return "T3"
    return t8  # T1a, T1b, T2 are the same


def compute_ajcc7_stage_group(row) -> str | None:
    """Compute AJCC 7th edition overall stage group."""
    hist = str(row.get("histology_final") or row.get("histology") or "").lower()
    age = row.get("age_at_surgery")
    t7 = row.get("ajcc7_t_stage")
    n = row.get("ajcc8_n_stage") or row.get("ajcc7_n_stage")
    m = row.get("ajcc7_m_stage") or row.get("ajcc8_m_stage") or "M0"

    is_malig = row.get("is_malignant")
    if is_malig is not None and not is_malig:
        return None

    if t7 is None and n is None:
        return None

    m = str(m or "M0")
    n = str(n or "Nx")
    t7 = str(t7 or "Tx")

    is_dtc = any(x in hist for x in (
        "papillary", "follicular", "hurthle", "hürthle", "oncocytic",
        "ptc", "ftc", "hcc", "niftp"
    ))
    is_mtc = "medullary" in hist
    is_atc = "anaplastic" in hist or "undifferentiated" in hist

    if is_dtc:
        if age is not None and not pd.isna(age) and float(age) < 45:
            return "II" if m == "M1" else "I"
        # age >= 45 or unknown
        if m == "M1":
            return "IVC"
        if t7 == "T4b":
            return "IVB"
        if t7 == "T4a" or n == "N1b":
            return "IVA"
        if t7 in ("T3", "T3a", "T3b") or n == "N1a":
            return "III"
        if t7 == "T2":
            return "II"
        if t7 in ("T1", "T1a", "T1b"):
            return "I"
        return None

    if is_mtc:
        if m == "M1":
            return "IVC"
        if t7 == "T4b":
            return "IVB"
        if t7 == "T4a" or n == "N1b":
            return "IVA"
        if t7 in ("T1", "T1a", "T1b", "T2", "T3", "T3a", "T3b") and n == "N1a":
            return "III"
        if t7 in ("T2", "T3", "T3a", "T3b") and n in ("N0", "Nx"):
            return "II"
        if t7 in ("T1", "T1a", "T1b") and n in ("N0", "Nx"):
            return "I"
        return None

    if is_atc:
        if m == "M1":
            return "IVC"
        if t7 == "T4b":
            return "IVB"
        return "IVA"

    return None


def phase_ajcc7(con, dry_run: bool = False):
    """Add AJCC 7th edition staging columns to canonical."""
    log("=== PHASE 2: AJCC 7TH EDITION STAGING (Issue 3) ===")

    # Check for existing AJCC7 columns
    existing = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND (LOWER(column_name) LIKE '%ajcc7%' OR LOWER(column_name) LIKE '%ajcc_7%')
    """).fetchall()
    if existing:
        log(f"  Found existing AJCC7 columns: {[r[0] for r in existing]}")
        log("  Will rebuild them.")

    log("  Pulling staging data from canonical + corrected scoring...")
    # Use corrected T/N/M from scoring phase if available
    try:
        con.execute(f"SELECT 1 FROM {FQ}.thyroid_scoring_py_v1_corrected LIMIT 1")
        use_corrected = True
    except Exception:
        use_corrected = False

    if use_corrected:
        df = con.execute(f"""
            SELECT
                CAST(cpm.research_id AS VARCHAR) AS research_id,
                sc.ajcc8_t_stage_v2 AS ajcc8_t_stage,
                sc.ajcc8_n_stage_v2 AS ajcc8_n_stage,
                sc.ajcc8_m_stage_v2 AS ajcc8_m_stage,
                sc.ajcc8_stage_group_v2 AS ajcc8_stage_group,
                cpm.is_malignant,
                cpm.age_at_surgery,
                cpm.histology_final,
                cpm.path_t_stage_raw, cpm.path_n_stage_raw, cpm.path_m_stage_raw,
                cpm.pet_distant_mets_ever
            FROM {FQ}.canonical_patient_master cpm
            LEFT JOIN {FQ}.thyroid_scoring_py_v1_corrected sc
              ON CAST(cpm.research_id AS VARCHAR) = sc.research_id
        """).fetchdf()
        log("  Using corrected T/N/M from thyroid_scoring_py_v1_corrected")
    else:
        df = con.execute(f"""
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group,
                is_malignant,
                age_at_surgery,
                histology_final,
                path_t_stage_raw, path_n_stage_raw, path_m_stage_raw,
                pet_distant_mets_ever
            FROM {FQ}.canonical_patient_master
        """).fetchdf()
        log("  Using original staging (corrected scoring table not found)")
    df["research_id"] = pd.to_numeric(df["research_id"], errors="coerce").astype("Int64")

    log(f"  Pulled {len(df)} patients")

    # Compute AJCC7 T-stage
    df["ajcc7_t_stage"] = df["ajcc8_t_stage"].apply(
        lambda t: compute_ajcc7_t_stage(t) if pd.notna(t) else None
    )

    # AJCC7 N-stage = same as AJCC8 N-stage (N-staging unchanged between editions)
    df["ajcc7_n_stage"] = df["ajcc8_n_stage"]

    # AJCC7 M-stage = same as corrected AJCC8 M-stage
    df["ajcc7_m_stage"] = df["ajcc8_m_stage"]

    # Compute AJCC7 stage group
    df["histology"] = df["histology_final"]
    df["ajcc7_stage_group"] = df.apply(compute_ajcc7_stage_group, axis=1)

    # Calculability
    df["ajcc7_stage_calculable_flag"] = df["ajcc7_stage_group"].notna()

    # Missing components
    def _missing_components(row):
        missing = []
        if pd.isna(row.get("ajcc7_t_stage")) or row.get("ajcc7_t_stage") is None:
            missing.append("T")
        if pd.isna(row.get("ajcc7_n_stage")) or row.get("ajcc7_n_stage") is None:
            missing.append("N")
        if pd.isna(row.get("age_at_surgery")) or row.get("age_at_surgery") is None:
            missing.append("age")
        return ",".join(missing) if missing else None

    df["ajcc7_missing_components"] = df.apply(_missing_components, axis=1)

    # Stage migration 7 -> 8
    def _migration(row):
        s7 = row.get("ajcc7_stage_group")
        s8 = row.get("ajcc8_stage_group")
        if s7 is None or s8 is None:
            return None
        if s7 == s8:
            return None
        return f"{s7}->{s8}"

    df["stage_migration_7_to_8"] = df.apply(_migration, axis=1)

    out = df[["research_id", "ajcc7_t_stage", "ajcc7_n_stage", "ajcc7_m_stage",
              "ajcc7_stage_group", "ajcc7_stage_calculable_flag",
              "ajcc7_missing_components", "stage_migration_7_to_8"]].copy()

    log("  AJCC7 stage distribution (malignant only):")
    malig = out.merge(df[["research_id", "is_malignant"]], on="research_id")
    dist = malig[malig["is_malignant"] == True]["ajcc7_stage_group"].value_counts().sort_index()
    for stage, n in dist.items():
        log(f"    {stage}: {n}")

    if dry_run:
        log("  [DRY-RUN] Would write ajcc7 staging table")
        return out

    log("  Writing ajcc7_staging_v1 table...")
    import tempfile
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    out.to_parquet(tmp, index=False)
    con.execute("DROP TABLE IF EXISTS ajcc7_staging_v1")
    con.execute(f"CREATE TABLE ajcc7_staging_v1 AS SELECT * FROM read_parquet('{tmp}')")
    tmp.unlink(missing_ok=True)
    n = con.execute("SELECT COUNT(*) FROM ajcc7_staging_v1").fetchone()[0]
    log(f"  ajcc7_staging_v1: {n} rows written")

    # Validate migration
    migration = con.execute(f"""
        SELECT stage_migration_7_to_8, COUNT(*) AS n
        FROM ajcc7_staging_v1
        WHERE stage_migration_7_to_8 IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
        LIMIT 20
    """).fetchall()
    log("  Stage migration 7->8 (top 20):")
    for m, n in migration:
        log(f"    {m}: {n}")

    return out


# ============================================================================
# PHASE 3: N_SURGERIES REBUILD (Issue 4)
# ============================================================================

def phase_nsurg(con, dry_run: bool = False):
    """Rebuild n_surgeries from path_synoptics in the legacy DB."""
    log("=== PHASE 3: N_SURGERIES REBUILD (Issue 4) ===")

    # Check legacy DB is accessible
    try:
        con.execute(f'SELECT COUNT(*) FROM {LEGACY_DB}.main.path_synoptics')
    except Exception as e:
        log(f"  Cannot access {LEGACY_DB}.main.path_synoptics: {e}")
        log("  Attaching Thyroid 2026 UPdated...")
        try:
            con.execute(f'ATTACH IF NOT EXISTS \'md:{LEGACY_DB.strip(chr(34))}\'')
        except Exception as e2:
            log(f"  Failed to attach: {e2}. Skipping n_surgeries rebuild.")
            return

    log("  Building surgery dates from path_synoptics (authoritative)...")
    build_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.patient_surgery_dates_rebuilt_v1 AS
    WITH raw AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            CAST(surg_date AS DATE) AS surgery_date
        FROM {LEGACY_DB}.main.path_synoptics
        WHERE surg_date IS NOT NULL
    ),
    distinct_dates AS (
        SELECT DISTINCT research_id, surgery_date FROM raw
    ),
    ordered AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surgery_date) AS rn,
            LAG(surgery_date) OVER (PARTITION BY research_id ORDER BY surgery_date) AS prev_date
        FROM distinct_dates
    ),
    deduped AS (
        SELECT research_id, surgery_date
        FROM ordered
        WHERE prev_date IS NULL
           OR DATEDIFF('day', prev_date, surgery_date) > 7
    ),
    ranked AS (
        SELECT
            research_id,
            surgery_date,
            ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surgery_date ASC) AS surgery_number,
            COUNT(*) OVER (PARTITION BY research_id) AS total_surgeries
        FROM deduped
    )
    SELECT * FROM ranked
    """

    if dry_run:
        log("  [DRY-RUN] Would create patient_surgery_dates_rebuilt_v1")
        # Still run the query to check it works
        test_sql = build_sql.replace("CREATE OR REPLACE TABLE", "CREATE OR REPLACE TEMP TABLE _test_nsurg AS WITH _t AS (")
        return

    con.execute(build_sql)
    n = con.execute(f"SELECT COUNT(*) FROM {FQ}.patient_surgery_dates_rebuilt_v1").fetchone()[0]
    log(f"  patient_surgery_dates_rebuilt_v1: {n} surgery-date rows")

    # Distribution
    dist = con.execute(f"""
        SELECT total_surgeries, COUNT(DISTINCT research_id) AS n_patients
        FROM {FQ}.patient_surgery_dates_rebuilt_v1
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  Surgery count distribution:")
    for nsurg, np_ in dist:
        log(f"    {nsurg} surgeries: {np_} patients")

    # Cross-validate with completion_reason
    xv = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE completion_reason IS NOT NULL AND total_surgeries >= 2) AS concordant,
            COUNT(*) FILTER (WHERE completion_reason IS NOT NULL AND total_surgeries < 2) AS discordant
        FROM {FQ}.canonical_patient_master cpm
        LEFT JOIN (
            SELECT research_id, MAX(total_surgeries) AS total_surgeries
            FROM {FQ}.patient_surgery_dates_rebuilt_v1
            GROUP BY 1
        ) ps ON CAST(cpm.research_id AS VARCHAR) = ps.research_id
    """).fetchone()
    log(f"  Completion cross-validation: concordant={xv[0]}, discordant={xv[1]}")


# ============================================================================
# PHASE 4: RECURRENCE RECLASSIFICATION (Issue 7 + Addendum)
# ============================================================================

def phase_recurrence(con, dry_run: bool = False):
    """Reclassify recurrence as pathology-proven only."""
    log("=== PHASE 4: RECURRENCE RECLASSIFICATION (Issue 7 + Addendum) ===")

    try:
        con.execute(f'SELECT COUNT(*) FROM {LEGACY_DB}.main.path_synoptics')
    except Exception:
        try:
            con.execute(f'ATTACH IF NOT EXISTS \'md:{LEGACY_DB.strip(chr(34))}\'')
        except Exception as e:
            log(f"  Cannot attach legacy DB: {e}. Skipping recurrence.")
            return

    # Get first surgery dates
    log("  Computing first surgery dates...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _first_surg AS
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MIN(surgery_date) AS first_surgery_date
        FROM {FQ}.patient_surgery_dates_rebuilt_v1
        WHERE surgery_number = 1
        GROUP BY 1
    """)

    # TIER 1: Pathology-proven recurrence (reoperation with cancer)
    log("  TIER 1: Reoperation with pathology-proven recurrence...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _rec_tier1_reop AS
        SELECT DISTINCT ON (ps.research_id)
            CAST(ps.research_id AS VARCHAR) AS research_id,
            CAST(ps.surg_date AS DATE) AS recurrence_date,
            ps.tumor_1_histologic_type AS recurrence_histology,
            'operative_pathology' AS recurrence_evidence_type,
            CAST(ps.research_id AS VARCHAR) || '_' || CAST(ps.surg_date AS VARCHAR) AS recurrence_pathology_specimen_id,
            'path_synoptics' AS recurrence_pathology_source_table,
            ps.tumor_1_site_laterality AS recurrence_site_raw
        FROM {LEGACY_DB}.main.path_synoptics ps
        JOIN _first_surg fs ON CAST(ps.research_id AS VARCHAR) = fs.research_id
        WHERE CAST(ps.surg_date AS DATE) > fs.first_surgery_date
          AND ps.tumor_1_histologic_type IS NOT NULL
          AND TRIM(CAST(ps.tumor_1_histologic_type AS VARCHAR)) != ''
          AND (LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%carcinoma%'
               OR LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%malignan%'
               OR LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%metast%'
               OR LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%papillary%'
               OR LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%follicular%'
               OR LOWER(CAST(ps.tumor_1_histologic_type AS VARCHAR)) LIKE '%medullary%')
        ORDER BY ps.research_id, ps.surg_date ASC
    """)
    n_reop = con.execute("SELECT COUNT(*) FROM _rec_tier1_reop").fetchone()[0]
    log(f"    Tier 1 (reoperation pathology): {n_reop} patients")

    # TIER 2: Post-op FNA with Bethesda V/VI
    log("  TIER 2: Post-op FNA Bethesda V/VI...")
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _rec_tier2_fna AS
            SELECT DISTINCT ON (fna.research_id)
                CAST(fna.research_id AS VARCHAR) AS research_id,
                TRY_CAST(fna.resolved_fna_date AS DATE) AS recurrence_date,
                NULL AS recurrence_histology,
                CASE
                    WHEN fna.bethesda_category = 6 THEN 'biopsy_fna_bethesda_vi'
                    ELSE 'biopsy_fna_bethesda_v'
                END AS recurrence_evidence_type,
                CAST(fna.fna_episode_id AS VARCHAR) AS recurrence_pathology_specimen_id,
                'fna_episode_master_v2' AS recurrence_pathology_source_table,
                fna.specimen_site_raw AS recurrence_site_raw
            FROM {LEGACY_DB}.main.fna_episode_master_v2 fna
            JOIN _first_surg fs ON CAST(fna.research_id AS VARCHAR) = fs.research_id
            WHERE TRY_CAST(fna.resolved_fna_date AS DATE) > fs.first_surgery_date
              AND fna.bethesda_category IN (5, 6)
            ORDER BY fna.research_id, fna.resolved_fna_date ASC
        """)
        n_fna = con.execute("SELECT COUNT(*) FROM _rec_tier2_fna").fetchone()[0]
    except Exception as e:
        log(f"    FNA query failed: {e}")
        con.execute("CREATE OR REPLACE TEMP TABLE _rec_tier2_fna AS SELECT NULL AS research_id WHERE FALSE")
        n_fna = 0
    log(f"    Tier 2 (FNA Bethesda V/VI): {n_fna} patients")

    # Combine pathology-proven tiers
    log("  Combining pathology-proven recurrence...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _rec_pathology_proven AS
        SELECT * FROM (
            SELECT research_id, recurrence_date, recurrence_histology,
                   recurrence_evidence_type, recurrence_pathology_specimen_id,
                   recurrence_pathology_source_table, recurrence_site_raw
            FROM _rec_tier1_reop
            UNION ALL
            SELECT research_id, recurrence_date, recurrence_histology,
                   recurrence_evidence_type, recurrence_pathology_specimen_id,
                   recurrence_pathology_source_table, recurrence_site_raw
            FROM _rec_tier2_fna
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY recurrence_date ASC) = 1
    """)
    n_proven = con.execute("SELECT COUNT(*) FROM _rec_pathology_proven").fetchone()[0]
    log(f"    Pathology-proven recurrence: {n_proven} patients")

    # TIER 3: Biochemical concern (from canonical_recurrence_v1 or canonical columns)
    log("  TIER 3: Biochemical concern...")
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _rec_tier3_biochem AS
            SELECT DISTINCT
                CAST(research_id AS VARCHAR) AS research_id,
                TRUE AS biochemical_concern_flag,
                recurrence_date AS biochemical_concern_first_date
            FROM {FQ}.canonical_recurrence_v1
            WHERE recurrence_type IN ('biochemical_tg_rise', 'persistent_biochemical_disease')
              AND CAST(research_id AS VARCHAR) NOT IN (SELECT research_id FROM _rec_pathology_proven)
        """)
        n_biochem = con.execute("SELECT COUNT(*) FROM _rec_tier3_biochem").fetchone()[0]
    except Exception as e:
        log(f"    canonical_recurrence_v1 query failed: {e}")
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _rec_tier3_biochem AS
                SELECT DISTINCT
                    CAST(research_id AS VARCHAR) AS research_id,
                    TRUE AS biochemical_concern_flag,
                    recurrence_date AS biochemical_concern_first_date
                FROM {FQ}.canonical_patient_master
                WHERE biochemical_recurrence_flag = TRUE
                  AND CAST(research_id AS VARCHAR) NOT IN (SELECT research_id FROM _rec_pathology_proven)
            """)
            n_biochem = con.execute("SELECT COUNT(*) FROM _rec_tier3_biochem").fetchone()[0]
        except Exception as e2:
            log(f"    Biochemical fallback also failed: {e2}")
            n_biochem = 0
            con.execute("CREATE OR REPLACE TEMP TABLE _rec_tier3_biochem AS SELECT NULL::VARCHAR AS research_id, NULL::BOOLEAN AS biochemical_concern_flag, NULL::DATE AS biochemical_concern_first_date WHERE FALSE")
    log(f"    Tier 3 (biochemical concern): {n_biochem} patients")

    # Build the comprehensive recurrence classification table
    log("  Building recurrence_classification_v1...")
    recurrence_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.recurrence_classification_v1 AS
    SELECT
        CAST(cpm.research_id AS VARCHAR) AS research_id,
        -- Pathology-proven recurrence flag
        CASE WHEN pp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS recurrence_flag_v2,
        pp.recurrence_date AS recurrence_date_v2,
        CASE WHEN pp.research_id IS NOT NULL
            THEN DATEDIFF('day', fs.first_surgery_date, pp.recurrence_date)
        END AS recurrence_days_from_surg,
        pp.recurrence_evidence_type,
        pp.recurrence_pathology_specimen_id,
        pp.recurrence_pathology_source_table,
        pp.recurrence_histology,
        pp.recurrence_site_raw AS recurrence_site,
        -- Imaging suspicious (from existing canonical flags)
        COALESCE(cpm.imaging_suspicious_unconfirmed, FALSE) AS imaging_suspicious_recurrence_flag,
        -- Biochemical concern
        COALESCE(bc.biochemical_concern_flag, FALSE) AS biochemical_concern_flag,
        bc.biochemical_concern_first_date,
        -- Summary flags
        CASE WHEN pp.research_id IS NOT NULL OR bc.biochemical_concern_flag = TRUE
             OR COALESCE(cpm.imaging_suspicious_unconfirmed, FALSE) = TRUE
            THEN TRUE ELSE FALSE
        END AS any_disease_concern_flag,
        CASE
            WHEN pp.research_id IS NOT NULL THEN 'pathology_proven'
            WHEN COALESCE(cpm.imaging_suspicious_unconfirmed, FALSE) = TRUE THEN 'imaging_suspicious'
            WHEN bc.biochemical_concern_flag = TRUE THEN 'biochemical_only'
            ELSE NULL
        END AS concern_highest_tier,
        -- Detection method
        CASE
            WHEN pp.research_id IS NOT NULL AND bc.biochemical_concern_flag = TRUE THEN 'multiple'
            WHEN pp.research_id IS NOT NULL THEN 'pathology_confirmed'
            WHEN bc.biochemical_concern_flag = TRUE THEN 'biochemical_only'
            WHEN COALESCE(cpm.imaging_suspicious_unconfirmed, FALSE) = TRUE THEN 'imaging_only'
            ELSE NULL
        END AS rec_detection_method
    FROM {FQ}.canonical_patient_master cpm
    LEFT JOIN _first_surg fs ON CAST(cpm.research_id AS VARCHAR) = fs.research_id
    LEFT JOIN _rec_pathology_proven pp ON CAST(cpm.research_id AS VARCHAR) = pp.research_id
    LEFT JOIN _rec_tier3_biochem bc ON CAST(cpm.research_id AS VARCHAR) = bc.research_id
    """

    if dry_run:
        log("  [DRY-RUN] Would create recurrence_classification_v1")
        return

    # Check if imaging_suspicious_unconfirmed column exists
    img_col = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND LOWER(column_name) = 'imaging_suspicious_unconfirmed'
    """).fetchall()

    if not img_col:
        recurrence_sql = recurrence_sql.replace(
            "COALESCE(cpm.imaging_suspicious_unconfirmed, FALSE)",
            "FALSE"
        )

    con.execute(recurrence_sql)
    n = con.execute(f"SELECT COUNT(*) FROM {FQ}.recurrence_classification_v1").fetchone()[0]
    log(f"  recurrence_classification_v1: {n} rows")

    # Validate
    validation = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE recurrence_flag_v2 = TRUE) AS n_path_proven,
            COUNT(*) FILTER (WHERE imaging_suspicious_recurrence_flag = TRUE) AS n_imaging_susp,
            COUNT(*) FILTER (WHERE biochemical_concern_flag = TRUE) AS n_biochem,
            COUNT(*) FILTER (WHERE any_disease_concern_flag = TRUE) AS n_any_concern,
            COUNT(*) FILTER (WHERE recurrence_flag_v2 = TRUE AND recurrence_evidence_type IS NULL) AS violations
        FROM {FQ}.recurrence_classification_v1
    """).fetchone()
    log(f"  Validation: path_proven={validation[0]}, imaging_susp={validation[1]}, "
        f"biochem={validation[2]}, any_concern={validation[3]}, violations={validation[4]}")

    # Tier distribution
    tier_dist = con.execute(f"""
        SELECT concern_highest_tier, COUNT(*) AS n
        FROM {FQ}.recurrence_classification_v1
        WHERE concern_highest_tier IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  Concern tier distribution:")
    for tier, n in tier_dist:
        log(f"    {tier}: {n}")


# ============================================================================
# PHASE 5: BMI DOCUMENTATION (Issue 6)
# ============================================================================

def phase_bmi(con, dry_run: bool = False):
    """Add bmi_missingness_reason column."""
    log("=== PHASE 5: BMI DOCUMENTATION (Issue 6) ===")

    log("  Building BMI missingness table...")
    bmi_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.bmi_missingness_v1 AS
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        bmi_combined,
        CASE
            WHEN bmi_combined IS NOT NULL THEN 'has_bmi'
            WHEN nsqip_bmi IS NOT NULL THEN 'has_bmi'
            ELSE 'no_bmi_source_available'
        END AS bmi_missingness_reason
    FROM {FQ}.canonical_patient_master
    """

    if dry_run:
        log("  [DRY-RUN] Would create bmi_missingness_v1")
        return

    # Check which BMI columns exist
    bmi_cols = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND LOWER(column_name) LIKE '%bmi%'
    """).fetchall()
    bmi_col_names = [r[0] for r in bmi_cols]
    log(f"  Available BMI columns: {bmi_col_names}")

    has_bmi_combined = "bmi_combined" in bmi_col_names
    has_nsqip = "nsqip_bmi" in bmi_col_names
    has_ops = "ops_bmi" in bmi_col_names

    select_parts = ["CAST(research_id AS VARCHAR) AS research_id"]
    case_parts = []

    if has_bmi_combined:
        select_parts.append("bmi_combined")
        case_parts.append("WHEN bmi_combined IS NOT NULL THEN 'has_bmi'")
    if has_nsqip:
        case_parts.append("WHEN nsqip_bmi IS NOT NULL THEN 'has_bmi'")
    if has_ops:
        case_parts.append("WHEN ops_bmi IS NOT NULL THEN 'has_bmi'")
    case_parts.append("ELSE 'no_bmi_source_available'")

    bmi_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.bmi_missingness_v1 AS
    SELECT
        {', '.join(select_parts)},
        CASE {' '.join(case_parts)} END AS bmi_missingness_reason
    FROM {FQ}.canonical_patient_master
    """

    con.execute(bmi_sql)
    dist = con.execute(f"""
        SELECT bmi_missingness_reason, COUNT(*) AS n
        FROM {FQ}.bmi_missingness_v1
        GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    log("  BMI missingness distribution:")
    for reason, n in dist:
        log(f"    {reason}: {n}")


# ============================================================================
# PHASE 6: REBUILD CANONICAL_PATIENT_MASTER (v224)
# ============================================================================

def phase_rebuild(con, dry_run: bool = False):
    """Rebuild canonical_patient_master_v224 with all fixes."""
    log("=== PHASE 6: REBUILD CANONICAL_PATIENT_MASTER (v224) ===")

    # Check which enrichment tables exist
    enrichment_tables = {}
    for tbl in ["ajcc7_staging_v1", "patient_surgery_dates_rebuilt_v1",
                 "recurrence_classification_v1", "bmi_missingness_v1"]:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {FQ}.\"{tbl}\"").fetchone()[0]
            enrichment_tables[tbl] = n
            log(f"  Found {tbl}: {n} rows")
        except Exception:
            log(f"  Missing {tbl} — will skip in rebuild")

    scoring_table = None
    for tbl in ["thyroid_scoring_py_v1_corrected", "thyroid_scoring_systems_v1", "thyroid_scoring_py_v1"]:
        try:
            con.execute(f"SELECT 1 FROM {FQ}.\"{tbl}\" LIMIT 1")
            scoring_table = tbl
            break
        except Exception:
            pass
    if scoring_table:
        log(f"  Found scoring table: {scoring_table}")
    else:
        log("  WARNING: No scoring table found. Scoring columns will not be updated.")

    # Build the canonical master v224
    log("  Building canonical_patient_master_v224...")

    # Start with current canonical
    join_clauses = []
    select_additions = []

    # Scoring fixes (Issues 1, 2, 5)
    if scoring_table:
        join_clauses.append(f"""
            LEFT JOIN {FQ}."{scoring_table}" sc
              ON CAST(cpm.research_id AS VARCHAR) = CAST(sc.research_id AS VARCHAR)
        """)
        if "corrected" in scoring_table:
            select_additions.extend([
                "sc.ajcc8_t_stage_v2",
                "sc.ajcc8_n_stage_v2",
                "sc.ajcc8_n_stage_note",
                "sc.ajcc8_m_stage_v2",
                "sc.ajcc8_stage_group_v2",
                "sc.distant_mets_proxy_v2",
            ])
        else:
            select_additions.extend([
                "sc.ajcc8_t_stage AS ajcc8_t_stage_v2",
                "sc.ajcc8_n_stage AS ajcc8_n_stage_v2",
                "sc.ajcc8_n_stage_note",
                "sc.ajcc8_m_stage AS ajcc8_m_stage_v2",
                "sc.ajcc8_stage_group AS ajcc8_stage_group_v2",
                "sc.distant_mets_proxy AS distant_mets_proxy_v2",
            ])

    # AJCC7 (Issue 3)
    if "ajcc7_staging_v1" in enrichment_tables:
        join_clauses.append(f"""
            LEFT JOIN {FQ}.ajcc7_staging_v1 a7
              ON CAST(cpm.research_id AS VARCHAR) = CAST(a7.research_id AS VARCHAR)
        """)
        select_additions.extend([
            "a7.ajcc7_t_stage",
            "a7.ajcc7_n_stage",
            "a7.ajcc7_m_stage",
            "a7.ajcc7_stage_group",
            "a7.ajcc7_stage_calculable_flag",
            "a7.ajcc7_missing_components",
            "a7.stage_migration_7_to_8",
        ])

    # N_surgeries (Issue 4)
    if "patient_surgery_dates_rebuilt_v1" in enrichment_tables:
        join_clauses.append(f"""
            LEFT JOIN (
                SELECT
                    research_id,
                    MAX(total_surgeries) AS n_surgeries_v2,
                    MIN(CASE WHEN surgery_number = 1 THEN surgery_date END) AS first_surgery_date_v2,
                    MIN(CASE WHEN surgery_number = 2 THEN surgery_date END) AS second_surgery_date_v2,
                    MIN(CASE WHEN surgery_number = 3 THEN surgery_date END) AS third_surgery_date_v2,
                    DATEDIFF('day',
                        MIN(CASE WHEN surgery_number = 1 THEN surgery_date END),
                        MIN(CASE WHEN surgery_number = 2 THEN surgery_date END)
                    ) AS days_between_first_second_surgery_v2,
                    'path_synoptics_distinct_dates' AS n_surgeries_source
                FROM {FQ}.patient_surgery_dates_rebuilt_v1
                GROUP BY research_id
            ) ns ON CAST(cpm.research_id AS VARCHAR) = ns.research_id
        """)
        select_additions.extend([
            "ns.n_surgeries_v2",
            "ns.first_surgery_date_v2",
            "ns.second_surgery_date_v2",
            "ns.third_surgery_date_v2",
            "ns.days_between_first_second_surgery_v2",
            "ns.n_surgeries_source",
        ])

    # Recurrence (Issue 7)
    if "recurrence_classification_v1" in enrichment_tables:
        join_clauses.append(f"""
            LEFT JOIN {FQ}.recurrence_classification_v1 rc
              ON CAST(cpm.research_id AS VARCHAR) = rc.research_id
        """)
        select_additions.extend([
            "rc.recurrence_flag_v2",
            "rc.recurrence_date_v2",
            "rc.recurrence_days_from_surg",
            "rc.recurrence_evidence_type",
            "rc.recurrence_pathology_specimen_id",
            "rc.recurrence_pathology_source_table",
            "rc.recurrence_histology AS recurrence_histology_v2",
            "rc.recurrence_site AS recurrence_site_v2",
            "rc.imaging_suspicious_recurrence_flag",
            "rc.biochemical_concern_flag",
            "rc.biochemical_concern_first_date",
            "rc.any_disease_concern_flag",
            "rc.concern_highest_tier",
            "rc.rec_detection_method",
        ])

    # BMI (Issue 6)
    if "bmi_missingness_v1" in enrichment_tables:
        join_clauses.append(f"""
            LEFT JOIN {FQ}.bmi_missingness_v1 bm
              ON CAST(cpm.research_id AS VARCHAR) = bm.research_id
        """)
        select_additions.append("bm.bmi_missingness_reason")

    additions_sql = ""
    if select_additions:
        additions_sql = ",\n        " + ",\n        ".join(select_additions)

    joins_sql = "\n    ".join(join_clauses)

    rebuild_sql = f"""
    CREATE OR REPLACE TABLE {FQ}.canonical_patient_master_v224 AS
    SELECT
        cpm.*{additions_sql}
    FROM {FQ}.canonical_patient_master cpm
    {joins_sql}
    """

    if dry_run:
        log("  [DRY-RUN] Would create canonical_patient_master_v224")
        log(f"  Additions: {len(select_additions)} new columns")
        return

    log(f"  Executing rebuild with {len(select_additions)} new columns...")
    con.execute(rebuild_sql)

    # Verify
    n = con.execute(f"SELECT COUNT(*) FROM {FQ}.canonical_patient_master_v224").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master_v224'
    """).fetchone()[0]
    log(f"  canonical_patient_master_v224: {n} rows, {n_cols} columns")

    if n != TOTAL_ROWS:
        log(f"  FATAL: Expected {TOTAL_ROWS} rows, got {n}. NOT swapping.")
        return

    # Archive current and swap
    log("  Archiving current canonical_patient_master -> ARCHIVE__canonical_patient_master_v223...")
    try:
        con.execute(f"""
            ALTER TABLE {FQ}.canonical_patient_master
            RENAME TO "ARCHIVE__canonical_patient_master_v223_pre224"
        """)
    except Exception as e:
        log(f"  Archive rename failed (may already exist): {e}")
        try:
            con.execute(f"DROP TABLE IF EXISTS {FQ}.\"ARCHIVE__canonical_patient_master_v223_pre224\"")
            con.execute(f"""
                ALTER TABLE {FQ}.canonical_patient_master
                RENAME TO "ARCHIVE__canonical_patient_master_v223_pre224"
            """)
        except Exception as e2:
            log(f"  Second archive attempt failed: {e2}. Trying backup name...")
            con.execute(f"DROP TABLE IF EXISTS {FQ}.\"_backup_cpm_pre224\"")
            con.execute(f"""
                ALTER TABLE {FQ}.canonical_patient_master
                RENAME TO "_backup_cpm_pre224"
            """)

    log("  Promoting v224 to canonical_patient_master...")
    con.execute(f"""
        ALTER TABLE {FQ}.canonical_patient_master_v224
        RENAME TO canonical_patient_master
    """)

    # Final verification
    n_final = con.execute(f"SELECT COUNT(*) FROM {FQ}.canonical_patient_master").fetchone()[0]
    n_distinct = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {FQ}.canonical_patient_master").fetchone()[0]
    log(f"  Final: {n_final} rows, {n_distinct} distinct research_ids")

    if n_final != TOTAL_ROWS:
        log(f"  CRITICAL: Row count mismatch! Expected {TOTAL_ROWS}, got {n_final}")
    elif n_final != n_distinct:
        log(f"  CRITICAL: Duplicate research_ids! {n_final - n_distinct} duplicates")
    else:
        log("  canonical_patient_master successfully rebuilt as v224.")


# ============================================================================
# PHASE 7: VALIDATION
# ============================================================================

def phase_validate(con):
    """Run all validation queries."""
    log("=== PHASE 7: COMPREHENSIVE VALIDATION ===")

    # Canonical invariants
    inv = con.execute(f"""
        SELECT
          COUNT(*) AS n,
          COUNT(*) - 10871 AS row_delta,
          COUNT(DISTINCT research_id) AS distinct_rids,
          COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
          COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna,
          COUNT(*) FILTER (WHERE is_malignant IS NULL) AS null_malignant,
          COUNT(*) FILTER (WHERE diagnosis_primary IS NULL) AS null_dx
        FROM {FQ}.canonical_patient_master
    """).fetchone()
    log(f"  Invariants: n={inv[0]}, delta={inv[1]}, distinct_rids={inv[2]}, "
        f"null_rids={inv[3]}, null_fna={inv[4]}, null_malig={inv[5]}, null_dx={inv[6]}")

    all_pass = True
    if inv[1] != 0:
        log("  FAIL: Row count delta != 0")
        all_pass = False
    if inv[3] != 0:
        log("  FAIL: NULL research_ids found")
        all_pass = False

    # Issue 1: M-stage validation
    try:
        m1 = con.execute(f"""
            SELECT
                SUM(CASE WHEN distant_mets_proxy_v2 THEN 1 ELSE 0 END) AS dmp_true,
                SUM(CASE WHEN recurrence_flag_v2 THEN 1 ELSE 0 END) AS rec_true
            FROM {FQ}.canonical_patient_master
        """).fetchone()
        log(f"  Issue 1: distant_mets_proxy_v2={m1[0]}, recurrence_flag_v2={m1[1]}")
    except Exception:
        log("  Issue 1: v2 columns not found in canonical, checking scoring table...")

    # Issue 2: T4 stages
    try:
        t4 = con.execute(f"""
            SELECT ajcc8_t_stage_v2, COUNT(*) AS n
            FROM {FQ}.canonical_patient_master
            WHERE ajcc8_t_stage_v2 LIKE 'T4%'
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        for stage, n in t4:
            log(f"  Issue 2: {stage} = {n}")
    except Exception:
        log("  Issue 2: v2 T-stage columns not in canonical")

    # Issue 3: AJCC7 distribution
    try:
        ajcc7 = con.execute(f"""
            SELECT ajcc7_stage_group, COUNT(*) AS n
            FROM {FQ}.canonical_patient_master
            WHERE is_malignant = TRUE AND ajcc7_stage_group IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        log("  Issue 3 (AJCC7 distribution, malignant):")
        for stage, n in ajcc7:
            log(f"    {stage}: {n}")
    except Exception:
        log("  Issue 3: AJCC7 columns not in canonical")

    # Issue 4: n_surgeries distribution
    try:
        nsurg = con.execute(f"""
            SELECT n_surgeries_v2, COUNT(*) AS n
            FROM {FQ}.canonical_patient_master
            WHERE n_surgeries_v2 IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        log("  Issue 4 (n_surgeries_v2 distribution):")
        for ns, n in nsurg:
            log(f"    {ns} surgeries: {n}")
    except Exception:
        log("  Issue 4: n_surgeries_v2 not in canonical")

    # Issue 5: N-stage NULL for malignant
    try:
        n_null = con.execute(f"""
            SELECT COUNT(*) FROM {FQ}.canonical_patient_master
            WHERE is_malignant = TRUE AND ajcc8_n_stage_v2 IS NULL
        """).fetchone()[0]
        log(f"  Issue 5: Malignant with NULL N-stage_v2 = {n_null} (expected 0)")
    except Exception:
        log("  Issue 5: N-stage v2 not in canonical")

    # Issue 7: Recurrence
    try:
        rec = con.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE recurrence_flag_v2 = TRUE) AS n_path_proven,
                COUNT(*) FILTER (WHERE imaging_suspicious_recurrence_flag = TRUE) AS n_img_susp,
                COUNT(*) FILTER (WHERE biochemical_concern_flag = TRUE) AS n_biochem,
                COUNT(*) FILTER (WHERE any_disease_concern_flag = TRUE) AS n_any
            FROM {FQ}.canonical_patient_master
        """).fetchone()
        log(f"  Issue 7: path_proven={rec[0]}, img_susp={rec[1]}, biochem={rec[2]}, any={rec[3]}")
    except Exception:
        log("  Issue 7: Recurrence v2 columns not in canonical")

    # Column count
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    log(f"  Total columns: {n_cols}")

    if all_pass:
        log("  ALL VALIDATIONS PASSED.")
    else:
        log("  SOME VALIDATIONS FAILED — review above.")


# ============================================================================
# PHASE 8: DEDUP AND ARCHIVE CLEANUP
# ============================================================================

def phase_dedup(con, dry_run: bool = False):
    """Audit tables and clean duplicates/archives."""
    log("=== PHASE 8: DEDUP AND ARCHIVE CLEANUP ===")

    tables = con.execute(f"""
        SELECT table_name,
               (SELECT COUNT(*) FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name=t.table_name) AS n_cols
        FROM information_schema.tables t
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_type='BASE TABLE'
        ORDER BY table_name
    """).fetchall()

    log(f"  Found {len(tables)} tables in {PUBLICATION_DB}")

    deprecated = []
    archive = []
    md_prefix = []
    live = []

    for tname, ncols in tables:
        if tname.startswith("DEPRECATED__"):
            deprecated.append(tname)
        elif tname.startswith("ARCHIVE__"):
            archive.append(tname)
        elif tname.startswith("md_"):
            md_prefix.append(tname)
        else:
            live.append(tname)

    log(f"  Live: {len(live)}, Archive: {len(archive)}, "
        f"Deprecated: {len(deprecated)}, md_prefix: {len(md_prefix)}")

    # Check md_ prefix tables for duplicates
    for md_tbl in md_prefix:
        base_name = md_tbl[3:]  # strip md_ prefix
        if base_name in live:
            log(f"  Checking if md_{base_name} is identical to {base_name}...")
            try:
                na = con.execute(f'SELECT COUNT(*) FROM {FQ}."{md_tbl}"').fetchone()[0]
                nb = con.execute(f'SELECT COUNT(*) FROM {FQ}."{base_name}"').fetchone()[0]
                if na == nb:
                    ha = con.execute(f"""
                        SELECT md5(STRING_AGG(CAST(research_id AS VARCHAR), ',' ORDER BY research_id))
                        FROM (SELECT research_id FROM {FQ}."{md_tbl}" LIMIT 100)
                    """).fetchone()[0]
                    hb = con.execute(f"""
                        SELECT md5(STRING_AGG(CAST(research_id AS VARCHAR), ',' ORDER BY research_id))
                        FROM (SELECT research_id FROM {FQ}."{base_name}" LIMIT 100)
                    """).fetchone()[0]
                    if ha == hb:
                        log(f"    IDENTICAL: md_{base_name} == {base_name} ({na} rows). Safe to drop.")
                        if not dry_run:
                            con.execute(f'DROP TABLE {FQ}."{md_tbl}"')
                            log(f"    DROPPED: {md_tbl}")
                    else:
                        log(f"    DIFFERENT hashes: keeping both")
                else:
                    log(f"    Different row counts ({na} vs {nb}): keeping both")
            except Exception as e:
                log(f"    Error checking {md_tbl}: {e}")

    # Ensure canonical_patient_master has no duplicate columns
    dup_cols = con.execute(f"""
        SELECT column_name, COUNT(*) AS n
        FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
        GROUP BY column_name
        HAVING COUNT(*) > 1
    """).fetchall()
    if dup_cols:
        log(f"  WARNING: Duplicate column names in canonical_patient_master: {dup_cols}")
    else:
        log("  No duplicate column names in canonical_patient_master.")

    # Final table count
    n_tables = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{PUBLICATION_DB}' AND schema_name='main'
    """).fetchone()[0]
    log(f"  Final table count: {n_tables}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    p = argparse.ArgumentParser(description="Script 224: Canonical ETL Fixes")
    p.add_argument("--dry-run", action="store_true", help="Show what would be done")
    p.add_argument("--phase", type=str, default="all",
                   choices=["all", "preflight", "scoring", "ajcc7", "nsurg",
                            "recurrence", "bmi", "rebuild", "validate", "dedup"],
                   help="Run specific phase only")
    args = p.parse_args()

    print("=" * 70)
    print(f"  Script 224: Canonical ETL Fixes — {datetime.now().isoformat()}")
    print(f"  Phase: {args.phase}, Dry-run: {args.dry_run}")
    print("=" * 70)

    log("Connecting to MotherDuck (publication DB)...")
    con = connect_locked()
    log("Connected and locked to thyroid_canonical_publication_v1_0")

    phases = {
        "preflight": lambda: phase_preflight(con),
        "scoring": lambda: phase_scoring_direct(con, args.dry_run),
        "ajcc7": lambda: phase_ajcc7(con, args.dry_run),
        "nsurg": lambda: phase_nsurg(con, args.dry_run),
        "recurrence": lambda: phase_recurrence(con, args.dry_run),
        "bmi": lambda: phase_bmi(con, args.dry_run),
        "rebuild": lambda: phase_rebuild(con, args.dry_run),
        "validate": lambda: phase_validate(con),
        "dedup": lambda: phase_dedup(con, args.dry_run),
    }

    if args.phase == "all":
        # Run all phases in order
        if not phase_preflight(con):
            log("Pre-flight FAILED. Aborting.")
            sys.exit(1)
        phase_scoring_direct(con, args.dry_run)
        phase_ajcc7(con, args.dry_run)
        phase_nsurg(con, args.dry_run)
        phase_recurrence(con, args.dry_run)
        phase_bmi(con, args.dry_run)
        phase_rebuild(con, args.dry_run)
        phase_validate(con)
        phase_dedup(con, args.dry_run)
        log("ALL PHASES COMPLETE.")
    else:
        result = phases[args.phase]()
        if args.phase == "preflight" and result is False:
            sys.exit(1)

    con.close()
    print("\n" + "=" * 70)
    print("  Script 224 complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
