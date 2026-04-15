#!/usr/bin/env python3
"""
THYROID_2026 — Script 216: Complete Data Gap Resolution (Phase A)
Database: thyroid_ete_fix_20260413

Phase A quick wins — no LLM required:
  A1: Fix BIGINT/VARCHAR lab join bug (TSH, VitD, PTH, Calcium)
  A2: Ingest MRI extraction data (715 exams, 462 patients)
  A3: Ingest thyroid weight data (10,001 patients)
  A4: Build NSQIP crosswalk via EUH_MRN, ingest BMI/ASA/comorbidities
  A5: Ingest OP Sheet structured fields (9,368 patients)
  C1: Canonical rebuild with all new sources
  C3: Final validation + coverage report

Run:
  .venv/bin/python scripts/216_data_gap_resolution.py [--dry-run] [--phase A1|A2|A3|A4|A5|C1|all]
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10871

# PHI columns to never ingest
PHI_COLS = {
    "date of birth", "dob", "patient_last_nm", "patient_first_nm",
    "empi_nbr", "patient_id", "euh_mrn", "tec_mrn", "lmrn",
    "idn", "case number", "encounter number",
}


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[216] ERROR: No MotherDuck token found.")
        sys.exit(1)
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


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
    print(f"[216] {label}: {inv[0]} rows, {inv[1]} distinct RIDs, "
          f"{inv[2]} null RIDs, {inv[3]} null fna_path_outcome")
    errors = []
    if inv[0] != TOTAL_ROWS:
        errors.append(f"Row count {inv[0]} != {TOTAL_ROWS}")
    if inv[0] != inv[1]:
        errors.append(f"Duplicate research_ids: {inv[0] - inv[1]}")
    if inv[2] > 0:
        errors.append(f"NULL research_ids: {inv[2]}")
    for e in errors:
        print(f"[216] ERROR: {e}")
    return len(errors) == 0


def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


def get_canonical_rids(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        f"SELECT research_id FROM {CANONICAL}"
    ).fetchall()
    return {str(r[0]) for r in rows}


# ======================================================================
# A1: Fix TSH/VitD/PTH/Calcium lab bug
# ======================================================================

def task_a1_lab_fix(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, int]:
    """Fix BIGINT→VARCHAR join bug for TSH, VitD, PTH, Calcium labs."""
    print("\n" + "=" * 70)
    print("[216] TASK A1: Fix Lab Join Bug (BIGINT→VARCHAR)")
    print("=" * 70)

    analytes = {
        "tsh": "tsh",
        "vitamin_d": "vitd",
        "pth": "pth",
        "calcium": "calcium",
    }
    results = {}

    for lab_name, prefix in analytes.items():
        row = con.execute(f"""
            SELECT COUNT(*) AS n_rows,
                   COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS n_pts,
                   COUNT(*) FILTER (WHERE value_numeric IS NOT NULL) AS n_numeric
            FROM longitudinal_lab_canonical_v1
            WHERE LOWER(lab_name_standardized) = '{lab_name}'
        """).fetchone()
        print(f"  {lab_name}: {row[0]} rows, {row[1]} patients, {row[2]} with numeric values")
        results[lab_name] = row[1]

    if dry_run:
        print("  [DRY RUN] Would update lab columns in canonical")
        return results

    existing = get_existing_columns(con)

    lab_cols_to_add = {}
    for lab_name, prefix in analytes.items():
        for suffix in ["min", "max", "n_measurements", "first_date", "last_date"]:
            col = f"lab_{prefix}_{suffix}"
            if col not in existing:
                dtype = "DATE" if "date" in suffix else ("INTEGER" if suffix == "n_measurements" else "DOUBLE")
                lab_cols_to_add[col] = dtype

    if lab_cols_to_add:
        print(f"  Adding {len(lab_cols_to_add)} new lab columns via ALTER TABLE...")
        for col, dtype in lab_cols_to_add.items():
            try:
                con.execute(f'ALTER TABLE {CANONICAL} ADD COLUMN "{col}" {dtype}')
                print(f"    Added {col} ({dtype})")
            except Exception:
                pass

    for lab_name, prefix in analytes.items():
        print(f"  Updating {lab_name} columns with VARCHAR-safe join...")
        con.execute(f"""
            UPDATE {CANONICAL} AS c
            SET
                "lab_{prefix}_min" = sub.val_min,
                "lab_{prefix}_max" = sub.val_max,
                "lab_{prefix}_n_measurements" = sub.n_meas,
                "lab_{prefix}_first_date" = sub.first_dt,
                "lab_{prefix}_last_date" = sub.last_dt
            FROM (
                SELECT
                    CAST(research_id AS VARCHAR) AS rid,
                    MIN(value_numeric) AS val_min,
                    MAX(value_numeric) AS val_max,
                    COUNT(*) AS n_meas,
                    MIN(TRY_CAST(lab_date AS DATE)) AS first_dt,
                    MAX(TRY_CAST(lab_date AS DATE)) AS last_dt
                FROM longitudinal_lab_canonical_v1
                WHERE LOWER(lab_name_standardized) = '{lab_name}'
                  AND value_numeric IS NOT NULL
                GROUP BY 1
            ) sub
            WHERE c.research_id = sub.rid
        """)

    for lab_name, prefix in analytes.items():
        fill = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE "lab_{prefix}_n_measurements" IS NOT NULL
                                     AND "lab_{prefix}_n_measurements" > 0)
            FROM {CANONICAL}
        """).fetchone()[0]
        print(f"  ✓ lab_{prefix}: {fill} patients with data")

    return results


# ======================================================================
# A2: Ingest MRI extraction data
# ======================================================================

def task_a2_mri_ingest(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Ingest MRI extraction Excel into mri_imaging table."""
    print("\n" + "=" * 70)
    print("[216] TASK A2: Ingest MRI Extraction Data")
    print("=" * 70)

    excel_path = REPO / "raw" / "mri_extraction__FINAL_11_20_25.xlsx"
    if not excel_path.exists():
        print(f"  ERROR: {excel_path} not found")
        return 0

    df = pd.read_excel(excel_path, sheet_name="Sheet1")
    print(f"  Loaded: {len(df)} rows × {len(df.columns)} columns")

    df = df.rename(columns={"record_id": "research_id"})
    df["research_id"] = df["research_id"].astype(str)
    df["source_workbook"] = "mri_extraction__FINAL_11_20_25.xlsx"
    df["source_sheet"] = "Sheet1"
    df["ingest_script"] = "216_data_gap_resolution.py"
    df["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")

    valid_rids = get_canonical_rids(con)
    orphans = set(df["research_id"].unique()) - valid_rids
    n_pts = df["research_id"].nunique()
    print(f"  Patients: {n_pts}, orphan RIDs: {len(orphans)}")
    if orphans:
        print(f"  Orphan samples: {sorted(orphans)[:10]}")

    if dry_run:
        print("  [DRY RUN] Would upload to mri_imaging")
        return n_pts

    tmp = REPO / "scripts" / "output" / "_mri_staging_216.parquet"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(tmp), index=False)
    con.execute(f"CREATE OR REPLACE TABLE mri_imaging AS SELECT * FROM read_parquet('{tmp}')")
    try:
        tmp.unlink()
    except Exception:
        pass

    verify = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM mri_imaging").fetchone()
    print(f"  ✓ mri_imaging: {verify[0]} rows, {verify[1]} patients")
    return verify[1]


def _mri_rollup_to_canonical(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Roll up mri_imaging to patient-level and merge into canonical."""
    print("  Rolling up MRI to patient level...")

    mri_sql = """
    SELECT
        research_id,
        COUNT(*) AS mri_n_exams,
        TRUE AS mri_has_data,
        MIN(TRY_CAST(date_of_exam AS DATE)) AS mri_first_date,
        MAX(TRY_CAST(date_of_exam AS DATE)) AS mri_last_date
    FROM mri_imaging
    GROUP BY research_id
    """
    mri_agg = con.execute(mri_sql).fetchdf()

    mri_first = con.execute("""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY research_id
                    ORDER BY TRY_CAST(date_of_exam AS DATE) NULLS LAST) AS rn
            FROM mri_imaging
        )
        SELECT
            research_id,
            indication AS mri_indication_first,
            exam_type_detail AS mri_exam_type_first,
            CASE WHEN thyroid_nodule IS NOT NULL AND LOWER(CAST(thyroid_nodule AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_thyroid_nodule,
            CASE WHEN thyroid_enlarged IS NOT NULL AND LOWER(CAST(thyroid_enlarged AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_thyroid_enlarged,
            CASE WHEN thyroid_mass_effect IS NOT NULL AND LOWER(CAST(thyroid_mass_effect AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_mass_effect,
            CASE WHEN substernal_goiter IS NOT NULL AND LOWER(CAST(substernal_goiter AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_substernal,
            CASE WHEN substernal_extension IS NOT NULL AND LOWER(CAST(substernal_extension AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_substernal_extension,
            CASE WHEN pathologic_lymph_nodes IS NOT NULL AND LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) NOT IN ('false','0','','[]') THEN TRUE ELSE FALSE END AS mri_pathologic_ln,
            CASE WHEN lymph_nodes_mentioned IS NOT NULL AND LOWER(CAST(lymph_nodes_mentioned AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_ln_mentioned,
            CASE WHEN vocal_cords_described IS NOT NULL AND LOWER(CAST(vocal_cords_described AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_vocal_cords_described,
            CASE WHEN vocal_cords_normal IS NOT NULL AND LOWER(CAST(vocal_cords_normal AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END AS mri_vocal_cords_normal,
            CASE WHEN contrast IS NOT NULL AND LOWER(CAST(contrast AS VARCHAR)) LIKE '%with%' THEN TRUE ELSE FALSE END AS mri_contrast_used,
            CASE WHEN thyroid_dimensions_cm IS NOT NULL THEN TRUE ELSE FALSE END AS mri_has_dimensions,
            CASE WHEN dominant_nodule IS NOT NULL THEN TRUE ELSE FALSE END AS mri_has_dominant_nodule
        FROM ranked WHERE rn = 1
    """).fetchdf()

    mri_worst = con.execute("""
        SELECT
            research_id,
            MAX(CASE WHEN thyroid_nodule IS NOT NULL AND LOWER(CAST(thyroid_nodule AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END) AS mri_thyroid_nodule_any,
            MAX(CASE WHEN thyroid_enlarged IS NOT NULL AND LOWER(CAST(thyroid_enlarged AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END) AS mri_thyroid_enlarged_any,
            MAX(CASE WHEN thyroid_mass_effect IS NOT NULL AND LOWER(CAST(thyroid_mass_effect AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END) AS mri_mass_effect_any,
            MAX(CASE WHEN substernal_goiter IS NOT NULL AND LOWER(CAST(substernal_goiter AS VARCHAR)) NOT IN ('false','0','') THEN TRUE ELSE FALSE END) AS mri_substernal_any,
            MAX(CASE WHEN pathologic_lymph_nodes IS NOT NULL AND LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) NOT IN ('false','0','','[]') THEN TRUE ELSE FALSE END) AS mri_pathologic_ln_any,
            MAX(CASE WHEN contrast IS NOT NULL AND LOWER(CAST(contrast AS VARCHAR)) LIKE '%with%' THEN TRUE ELSE FALSE END) AS mri_contrast_used_any
        FROM mri_imaging
        GROUP BY research_id
    """).fetchdf()

    rollup = mri_agg.merge(mri_first, on="research_id", how="left")
    rollup = rollup.merge(mri_worst, on="research_id", how="left")
    print(f"  MRI rollup: {len(rollup)} patients, {len(rollup.columns)} columns")
    return rollup


# ======================================================================
# A3: Ingest thyroid weight data
# ======================================================================

def task_a3_weight_ingest(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Ingest thyroid weight data Excel."""
    print("\n" + "=" * 70)
    print("[216] TASK A3: Ingest Thyroid Weight Data")
    print("=" * 70)

    excel_path = REPO / "raw" / "Thyroid_Weight_Data_12_2_25.xlsx"
    if not excel_path.exists():
        print(f"  ERROR: {excel_path} not found")
        return 0

    df = pd.read_excel(excel_path, sheet_name="Sheet1")
    print(f"  Loaded: {len(df)} rows × {len(df.columns)} columns")

    df = df.rename(columns={"Research ID": "research_id"})
    df["research_id"] = df["research_id"].astype(str)

    valid_rids = get_canonical_rids(con)
    n_pts = df["research_id"].nunique()
    orphans = set(df["research_id"].unique()) - valid_rids
    print(f"  Patients: {n_pts}, orphan RIDs: {len(orphans)}")

    if dry_run:
        print("  [DRY RUN] Would upload to thyroid_weight_data")
        return n_pts

    phi_drop = [c for c in df.columns if c.lower() in PHI_COLS or c.lower() == "dob"]
    if phi_drop:
        print(f"  Dropping PHI columns: {phi_drop}")
        df = df.drop(columns=phi_drop)

    df["source_workbook"] = "Thyroid_Weight_Data_12_2_25.xlsx"
    df["ingest_script"] = "216_data_gap_resolution.py"
    df["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).replace({"nan": None, "NaT": None, "None": None})

    tmp = REPO / "scripts" / "output" / "_weight_staging_216.parquet"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(tmp), index=False)
    con.execute(f"CREATE OR REPLACE TABLE thyroid_weight_data AS SELECT * FROM read_parquet('{tmp}')")
    try:
        tmp.unlink()
    except Exception:
        pass

    verify = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM thyroid_weight_data").fetchone()
    print(f"  ✓ thyroid_weight_data: {verify[0]} rows, {verify[1]} patients")
    return verify[1]


def _weight_rollup(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Roll up weight data to patient level (first surgery values)."""
    print("  Rolling up weight data to patient level...")

    df_wt = con.execute("""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id
                    ORDER BY TRY_CAST("Date of Surgery" AS DATE) NULLS LAST
                ) AS rn
            FROM thyroid_weight_data
        )
        SELECT
            research_id,
            TRY_CAST("Specimen_Weight_Combined" AS DOUBLE) AS gland_weight_combined_g,
            TRY_CAST("Right Lobe (g)" AS DOUBLE) AS gland_weight_right_lobe_g,
            TRY_CAST("Left Lobe (g)" AS DOUBLE) AS gland_weight_left_lobe_g,
            TRY_CAST("Isthmus (g)" AS DOUBLE) AS gland_weight_isthmus_g,
            TRY_CAST("Total Weight (g)" AS DOUBLE) AS gland_weight_total_reported_g,
            'thyroid_weight_data' AS gland_weight_source
        FROM ranked
        WHERE rn = 1
    """).fetchdf()
    print(f"  Weight rollup: {len(df_wt)} patients")
    return df_wt


# ======================================================================
# A4: Build NSQIP crosswalk and ingest
# ======================================================================

def task_a4_nsqip(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build NSQIP crosswalk via EUH_MRN and ingest."""
    print("\n" + "=" * 70)
    print("[216] TASK A4: NSQIP Crosswalk + Ingest")
    print("=" * 70)

    nsqip_path = REPO / "raw" / "Thyroid NSQIP dataset 2010-2023.xlsx"
    op_path = REPO / "raw" / "Thyroid OP Sheet data.xlsx"

    if not nsqip_path.exists() or not op_path.exists():
        print(f"  ERROR: Required files not found")
        return 0

    print("  Loading OP Sheet for EUH_MRN → research_id crosswalk...")
    df_op = pd.read_excel(
        op_path, sheet_name="Physical OP sheet data",
        usecols=["Research ID number", "EUH_MRN"]
    )
    df_op = df_op.rename(columns={"Research ID number": "research_id", "EUH_MRN": "euh_mrn"})
    df_op["research_id"] = df_op["research_id"].astype(str)

    def norm_mrn(x):
        if pd.isna(x):
            return None
        try:
            return str(int(float(str(x))))
        except (ValueError, OverflowError):
            return None

    df_op["euh_mrn"] = df_op["euh_mrn"].apply(norm_mrn)
    df_op = df_op.dropna(subset=["euh_mrn"]).drop_duplicates(subset=["euh_mrn"], keep="first")
    print(f"  Crosswalk: {len(df_op)} MRN→research_id mappings")

    print("  Loading NSQIP dataset...")
    df_nsqip = pd.read_excel(nsqip_path, sheet_name="report_data")
    print(f"  NSQIP raw: {len(df_nsqip)} rows × {len(df_nsqip.columns)} columns")

    df_nsqip["IDN_str"] = df_nsqip["IDN"].apply(norm_mrn)

    df_linked = df_nsqip.merge(df_op, left_on="IDN_str", right_on="euh_mrn", how="inner")
    print(f"  Linked: {len(df_linked)} of {len(df_nsqip)} NSQIP rows ({len(df_linked)/len(df_nsqip)*100:.1f}%)")
    print(f"  Linked patients: {df_linked['research_id'].nunique()}")

    nsqip_extract_cols = {
        "research_id": "research_id",
        "Age at Time of Surgery": "nsqip_age_at_surgery",
        "Sex at Birth": "nsqip_sex",
    }

    bmi_col = [c for c in df_linked.columns if c.upper() == "BMI"]
    if bmi_col:
        nsqip_extract_cols[bmi_col[0]] = "nsqip_bmi"

    height_col = [c for c in df_linked.columns if "height" in c.lower()]
    if height_col:
        nsqip_extract_cols[height_col[0]] = "nsqip_height_in"

    weight_col = [c for c in df_linked.columns if c.lower() == "weight" or
                  (c.lower().startswith("weight") and "total" not in c.lower())]
    if weight_col:
        nsqip_extract_cols[weight_col[0]] = "nsqip_weight_lbs"

    asa_col = [c for c in df_linked.columns if "asa" in c.lower()]
    if asa_col:
        nsqip_extract_cols[asa_col[0]] = "nsqip_asa_class"

    dm_col = [c for c in df_linked.columns if "diabetes" in c.lower()]
    if dm_col:
        nsqip_extract_cols[dm_col[0]] = "nsqip_diabetes"

    htn_col = [c for c in df_linked.columns if "hypertension" in c.lower()]
    if htn_col:
        nsqip_extract_cols[htn_col[0]] = "nsqip_hypertension"

    smoke_col = [c for c in df_linked.columns if "smoke" in c.lower() or "tobacco" in c.lower()]
    if smoke_col:
        nsqip_extract_cols[smoke_col[0]] = "nsqip_smoker"

    func_col = [c for c in df_linked.columns if "functional" in c.lower() and "status" in c.lower()]
    if func_col:
        nsqip_extract_cols[func_col[0]] = "nsqip_functional_status"

    dyspnea_col = [c for c in df_linked.columns if "dyspnea" in c.lower()]
    if dyspnea_col:
        nsqip_extract_cols[dyspnea_col[0]] = "nsqip_dyspnea"

    wound_col = [c for c in df_linked.columns if "wound" in c.lower() and "class" in c.lower()]
    if wound_col:
        nsqip_extract_cols[wound_col[0]] = "nsqip_wound_class"

    optime_col = [c for c in df_linked.columns if "total operation" in c.lower() or
                  ("operative" in c.lower() and "time" in c.lower())]
    if optime_col:
        nsqip_extract_cols[optime_col[0]] = "nsqip_operative_time_min"

    los_col = [c for c in df_linked.columns if "length of" in c.lower() and "stay" in c.lower()]
    if los_col:
        nsqip_extract_cols[los_col[0]] = "nsqip_length_of_stay_days"

    available_src = [c for c in nsqip_extract_cols.keys() if c in df_linked.columns]
    print(f"  Extracting {len(available_src)} NSQIP columns")

    df_clean = df_linked[available_src].rename(columns=nsqip_extract_cols)
    df_clean["nsqip_source"] = "Thyroid NSQIP dataset 2010-2023.xlsx"

    valid_rids = get_canonical_rids(con)
    df_clean = df_clean[df_clean["research_id"].isin(valid_rids)]
    df_clean = df_clean.drop_duplicates(subset=["research_id"], keep="first")
    n_pts = len(df_clean)
    print(f"  Valid NSQIP patients (in canonical): {n_pts}")

    if dry_run:
        print("  [DRY RUN] Would upload to nsqip_data")
        return n_pts

    tmp = REPO / "scripts" / "output" / "_nsqip_staging_216.parquet"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(str(tmp), index=False)
    con.execute(f"CREATE OR REPLACE TABLE nsqip_data AS SELECT * FROM read_parquet('{tmp}')")
    try:
        tmp.unlink()
    except Exception:
        pass

    verify = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM nsqip_data").fetchone()
    print(f"  ✓ nsqip_data: {verify[0]} rows, {verify[1]} patients")

    if "nsqip_bmi" in df_clean.columns:
        bmi_fill = df_clean["nsqip_bmi"].notna().sum()
        print(f"  BMI coverage: {bmi_fill}/{n_pts} ({bmi_fill/n_pts*100:.1f}%)")

    return verify[1]


# ======================================================================
# A5: Ingest OP Sheet structured fields
# ======================================================================

def task_a5_opsheet(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Ingest OP Sheet structured fields (NO PHI)."""
    print("\n" + "=" * 70)
    print("[216] TASK A5: OP Sheet Structured Fields")
    print("=" * 70)

    excel_path = REPO / "raw" / "Thyroid OP Sheet data.xlsx"
    if not excel_path.exists():
        print(f"  ERROR: {excel_path} not found")
        return 0

    cols_to_load = [
        "Research ID number",
        "Surgeon",
        "Surg Date",
        "Preop Diagnosis operative sheet* (not true preop dx)",
        "Prior Neck Operation",
        "Prior Neck irradiation",
        "Family History of Thyroid Cancer",
        "Preop symptoms",
        "Preop imaging performed",
        "Cervical Lymph Node US performed",
        "Head/neck US findings",
        "Thyroid scintigraphy",
        "Preop Laryngoscopy",
        "Dominant nodule size on US",
        "Dominant nodule location",
        "Dominant nodule Bethesda2",
        "Number of Nodules Identified preop & Size",
        "Metastatic disease  (identified preop)",
        "palpable lesion on Physical exam",
        "Maximum diameter of largest tumor or goiter from operative sheet",
        "Side of largest tumor (or goiter)",
        "Parathyroid AG Notes",
        "Para AG performed",
        "Parathyroidectomy (resection)",
        "IO Tumor appearance",
        "Intraop thyroid/parathyroid appearance (General)",
        "Number of nodules identified intraop",
        "Perioperative Complications",
        "Final Nerve Stim",
        "Final Nerve Stim value Right",
        "Final Nerve Stim value Left",
        "Difficult Airway",
        "EBL",
        "Skin-SKin time (min)",
        "BMI",
        "anticoagulation meds",
        "RL AG", "RU AG", "LL AG", "LU AG", "Other AG",
        "RU resection", "RL Resection", "LU resection", "LL resection",
        "RU Parathyroid Visualized", "RL Parathyroid Visualized",
        "LU Parathyroid Visualized", "LL Parathyroid Visualized",
        "supranumary para gland",
    ]

    df = pd.read_excel(excel_path, sheet_name="Physical OP sheet data")
    available = [c for c in cols_to_load if c in df.columns]
    df = df[available].copy()
    print(f"  Loaded: {len(df)} rows × {len(df.columns)} columns")

    df = df.rename(columns={"Research ID number": "research_id"})
    df["research_id"] = df["research_id"].astype(str)

    rename_map = {
        "Surgeon": "ops_surgeon",
        "Surg Date": "ops_surg_date",
        "Preop Diagnosis operative sheet* (not true preop dx)": "ops_preop_diagnosis",
        "Prior Neck Operation": "ops_prior_neck_operation",
        "Prior Neck irradiation": "ops_prior_neck_irradiation",
        "Family History of Thyroid Cancer": "ops_family_hx_thyroid_ca",
        "Preop symptoms": "ops_preop_symptoms",
        "Preop imaging performed": "ops_preop_imaging_performed",
        "Cervical Lymph Node US performed": "ops_cervical_ln_us_performed",
        "Head/neck US findings": "ops_head_neck_us_findings",
        "Thyroid scintigraphy": "ops_thyroid_scintigraphy",
        "Preop Laryngoscopy": "ops_preop_laryngoscopy",
        "Dominant nodule size on US": "ops_dominant_nodule_size_us",
        "Dominant nodule location": "ops_dominant_nodule_location",
        "Dominant nodule Bethesda2": "ops_dominant_nodule_bethesda",
        "Number of Nodules Identified preop & Size": "ops_preop_nodules_count_size",
        "Metastatic disease  (identified preop)": "ops_preop_metastatic_disease",
        "palpable lesion on Physical exam": "ops_palpable_lesion",
        "Maximum diameter of largest tumor or goiter from operative sheet": "ops_max_diameter_cm",
        "Side of largest tumor (or goiter)": "ops_tumor_side",
        "Parathyroid AG Notes": "ops_parathyroid_ag_notes",
        "Para AG performed": "ops_para_ag_performed",
        "Parathyroidectomy (resection)": "ops_parathyroidectomy",
        "IO Tumor appearance": "ops_io_tumor_appearance",
        "Intraop thyroid/parathyroid appearance (General)": "ops_intraop_appearance",
        "Number of nodules identified intraop": "ops_intraop_nodule_count",
        "Perioperative Complications": "ops_periop_complications",
        "Final Nerve Stim": "ops_nerve_stim_final",
        "Final Nerve Stim value Right": "ops_nerve_stim_right",
        "Final Nerve Stim value Left": "ops_nerve_stim_left",
        "Difficult Airway": "ops_difficult_airway",
        "EBL": "ops_ebl_ml",
        "Skin-SKin time (min)": "ops_skin_to_skin_min",
        "BMI": "ops_bmi",
        "anticoagulation meds": "ops_anticoagulation_meds",
        "RL AG": "ops_rl_ag",
        "RU AG": "ops_ru_ag",
        "LL AG": "ops_ll_ag",
        "LU AG": "ops_lu_ag",
        "Other AG": "ops_other_ag",
        "RU resection": "ops_ru_resection",
        "RL Resection": "ops_rl_resection",
        "LU resection": "ops_lu_resection",
        "LL resection": "ops_ll_resection",
        "RU Parathyroid Visualized": "ops_ru_para_visualized",
        "RL Parathyroid Visualized": "ops_rl_para_visualized",
        "LU Parathyroid Visualized": "ops_lu_para_visualized",
        "LL Parathyroid Visualized": "ops_ll_para_visualized",
        "supranumary para gland": "ops_supranumerary_para",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["ops_source_workbook"] = "Thyroid OP Sheet data.xlsx"
    df["ops_ingest_script"] = "216_data_gap_resolution.py"

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).replace({"nan": None, "NaT": None, "None": None})

    valid_rids = get_canonical_rids(con)
    n_before = len(df)
    df = df[df["research_id"].isin(valid_rids)]
    df = df.drop_duplicates(subset=["research_id"], keep="first")
    n_pts = len(df)
    print(f"  Valid OP Sheet patients: {n_pts} (dropped {n_before - n_pts} invalid/dups)")

    if dry_run:
        print("  [DRY RUN] Would upload to op_sheet_data")
        return n_pts

    tmp = REPO / "scripts" / "output" / "_opsheet_staging_216.parquet"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(tmp), index=False)
    con.execute(f"CREATE OR REPLACE TABLE op_sheet_data AS SELECT * FROM read_parquet('{tmp}')")
    try:
        tmp.unlink()
    except Exception:
        pass

    verify = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM op_sheet_data").fetchone()
    print(f"  ✓ op_sheet_data: {verify[0]} rows, {verify[1]} patients")

    fill_checks = [
        ("ops_prior_neck_operation", "Prior neck op"),
        ("ops_family_hx_thyroid_ca", "Family hx thyroid CA"),
        ("ops_nerve_stim_final", "Nerve stim"),
        ("ops_ebl_ml", "EBL"),
        ("ops_bmi", "BMI"),
        ("ops_preop_laryngoscopy", "Preop laryngoscopy"),
    ]
    for col, label in fill_checks:
        if col in df.columns:
            fill = df[col].notna().sum()
            print(f"    {label}: {fill}/{n_pts} ({fill/n_pts*100:.1f}%)")

    return verify[1]


# ======================================================================
# C1: Canonical rebuild with all new sources
# ======================================================================

def task_c1_canonical_rebuild(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Rebuild canonical_patient_master_v1 with all new source columns."""
    print("\n" + "=" * 70)
    print("[216] TASK C1: Canonical Rebuild")
    print("=" * 70)

    existing = get_existing_columns(con)
    cur_col_count = len(existing)
    print(f"  Current canonical: {TOTAL_ROWS} rows × {cur_col_count} columns")

    tables_present = {}
    for tbl in ["mri_imaging", "thyroid_weight_data", "nsqip_data", "op_sheet_data"]:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            tables_present[tbl] = cnt
            print(f"  {tbl}: {cnt} rows")
        except Exception:
            tables_present[tbl] = 0
            print(f"  {tbl}: NOT FOUND — skipping")

    cte_parts = []
    select_parts = []
    join_parts = []

    # ── MRI rollup ──
    if tables_present.get("mri_imaging", 0) > 0:
        cte_parts.append("""
mri_agg AS (
    SELECT
        research_id,
        COUNT(*) AS mri_n_exams,
        TRUE AS mri_has_data,
        MIN(TRY_CAST(date_of_exam AS DATE)) AS mri_first_date,
        MAX(TRY_CAST(date_of_exam AS DATE)) AS mri_last_date
    FROM mri_imaging
    GROUP BY research_id
),
mri_first AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY research_id
                ORDER BY TRY_CAST(date_of_exam AS DATE) NULLS LAST) AS rn
        FROM mri_imaging
    ) WHERE rn = 1
),
mri_worst AS (
    SELECT
        research_id,
        MAX(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) NOT IN ('false','0','') AND thyroid_nodule IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_thyroid_nodule_any,
        MAX(CASE WHEN LOWER(CAST(thyroid_enlarged AS VARCHAR)) NOT IN ('false','0','') AND thyroid_enlarged IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_thyroid_enlarged_any,
        MAX(CASE WHEN LOWER(CAST(thyroid_mass_effect AS VARCHAR)) NOT IN ('false','0','') AND thyroid_mass_effect IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_mass_effect_any,
        MAX(CASE WHEN LOWER(CAST(substernal_goiter AS VARCHAR)) NOT IN ('false','0','') AND substernal_goiter IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_substernal_any,
        MAX(CASE WHEN LOWER(CAST(substernal_extension AS VARCHAR)) NOT IN ('false','0','') AND substernal_extension IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_substernal_extension_any,
        MAX(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) NOT IN ('false','0','','[]') AND pathologic_lymph_nodes IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_pathologic_ln_any,
        MAX(CASE WHEN LOWER(CAST(lymph_nodes_mentioned AS VARCHAR)) NOT IN ('false','0','') AND lymph_nodes_mentioned IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_ln_mentioned_any,
        MAX(CASE WHEN LOWER(CAST(vocal_cords_described AS VARCHAR)) NOT IN ('false','0','') AND vocal_cords_described IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_vocal_cords_described,
        MAX(CASE WHEN LOWER(CAST(vocal_cords_normal AS VARCHAR)) NOT IN ('false','0','') AND vocal_cords_normal IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_vocal_cords_normal,
        MAX(CASE WHEN contrast IS NOT NULL AND LOWER(CAST(contrast AS VARCHAR)) LIKE '%with%' THEN TRUE ELSE FALSE END) AS mri_contrast_used_any,
        MAX(CASE WHEN thyroid_dimensions_cm IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_has_dimensions,
        MAX(CASE WHEN dominant_nodule IS NOT NULL THEN TRUE ELSE FALSE END) AS mri_has_dominant_nodule
    FROM mri_imaging
    GROUP BY research_id
)
""")
        mri_cols = [
            "ma.mri_n_exams", "ma.mri_has_data", "ma.mri_first_date", "ma.mri_last_date",
            "mf.indication AS mri_indication_first",
            "mf.exam_type_detail AS mri_exam_type_first",
            "mw.mri_thyroid_nodule_any", "mw.mri_thyroid_enlarged_any",
            "mw.mri_mass_effect_any", "mw.mri_substernal_any",
            "mw.mri_substernal_extension_any", "mw.mri_pathologic_ln_any",
            "mw.mri_ln_mentioned_any", "mw.mri_vocal_cords_described",
            "mw.mri_vocal_cords_normal", "mw.mri_contrast_used_any",
            "mw.mri_has_dimensions", "mw.mri_has_dominant_nodule",
        ]
        select_parts.extend([c for c in mri_cols
                             if c.split(" AS ")[-1].strip() not in existing
                             and c.split(".")[-1].split(" AS ")[0].strip() not in existing])
        join_parts.append("LEFT JOIN mri_agg ma ON c.research_id = ma.research_id")
        join_parts.append("LEFT JOIN mri_first mf ON c.research_id = mf.research_id")
        join_parts.append("LEFT JOIN mri_worst mw ON c.research_id = mw.research_id")

    # ── Weight rollup ──
    if tables_present.get("thyroid_weight_data", 0) > 0:
        cte_parts.append("""
wt_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST("Date of Surgery" AS DATE) NULLS LAST
        ) AS rn
    FROM thyroid_weight_data
),
wt_cte AS (
    SELECT
        research_id,
        TRY_CAST("Specimen_Weight_Combined" AS DOUBLE) AS gland_weight_combined_g,
        TRY_CAST("Right Lobe (g)" AS DOUBLE) AS gland_weight_right_lobe_g,
        TRY_CAST("Left Lobe (g)" AS DOUBLE) AS gland_weight_left_lobe_g,
        TRY_CAST("Isthmus (g)" AS DOUBLE) AS gland_weight_isthmus_g,
        TRY_CAST("Total Weight (g)" AS DOUBLE) AS gland_weight_total_reported_g,
        'thyroid_weight_data' AS gland_weight_source
    FROM wt_ranked WHERE rn = 1
)
""")
        wt_cols = [
            "wt.gland_weight_combined_g",
            "wt.gland_weight_right_lobe_g",
            "wt.gland_weight_left_lobe_g",
            "wt.gland_weight_isthmus_g",
            "wt.gland_weight_total_reported_g",
            "wt.gland_weight_source",
            "COALESCE(c.syn_total_weight_g, wt.gland_weight_combined_g) AS gland_weight_final_g",
        ]
        select_parts.extend([col for col in wt_cols
                             if col.split(" AS ")[-1].strip() not in existing])
        join_parts.append("LEFT JOIN wt_cte wt ON c.research_id = wt.research_id")

    # ── NSQIP rollup ──
    if tables_present.get("nsqip_data", 0) > 0:
        nsqip_schema = con.execute("""
            SELECT DISTINCT column_name FROM information_schema.columns
            WHERE table_name = 'nsqip_data' AND table_schema = 'main'
              AND column_name LIKE 'nsqip_%'
        """).fetchall()
        nsqip_col_names = [r[0] for r in nsqip_schema]
        for nc in nsqip_col_names:
            if nc not in existing:
                select_parts.append(f'nsq."{nc}"')
        if any(nc not in existing for nc in nsqip_col_names):
            join_parts.append("LEFT JOIN nsqip_data nsq ON c.research_id = nsq.research_id")

    # ── OP Sheet rollup ──
    if tables_present.get("op_sheet_data", 0) > 0:
        ops_schema = con.execute("""
            SELECT DISTINCT column_name FROM information_schema.columns
            WHERE table_name = 'op_sheet_data' AND table_schema = 'main'
              AND column_name LIKE 'ops_%'
              AND column_name NOT IN ('ops_source_workbook', 'ops_ingest_script')
        """).fetchall()
        ops_col_names = [r[0] for r in ops_schema]
        for oc in ops_col_names:
            if oc not in existing:
                select_parts.append(f'ops."{oc}"')
        if any(oc not in existing for oc in ops_col_names):
            join_parts.append("LEFT JOIN op_sheet_data ops ON c.research_id = ops.research_id")

    # ── Lab fix (PTH, Calcium — TSH/VitD already done in A1 via ALTER+UPDATE) ──
    lab_new_analytes = {"pth": "pth", "calcium": "calcium"}
    for lab_name, prefix in lab_new_analytes.items():
        cols_needed = [f"lab_{prefix}_{s}" for s in ["min", "max", "n_measurements", "first_date", "last_date"]]
        new_lab_cols = [c for c in cols_needed if c not in existing]
        if new_lab_cols:
            alias = f"lab_{prefix}"
            cte_parts.append(f"""
{alias}_cte AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(value_numeric) AS lab_{prefix}_min,
        MAX(value_numeric) AS lab_{prefix}_max,
        COUNT(*) AS lab_{prefix}_n_measurements,
        MIN(TRY_CAST(lab_date AS DATE)) AS lab_{prefix}_first_date,
        MAX(TRY_CAST(lab_date AS DATE)) AS lab_{prefix}_last_date
    FROM longitudinal_lab_canonical_v1
    WHERE LOWER(lab_name_standardized) = '{lab_name}'
      AND value_numeric IS NOT NULL
    GROUP BY 1
)
""")
            for nc in new_lab_cols:
                select_parts.append(f'{alias}."{nc}"')
            join_parts.append(f"LEFT JOIN {alias}_cte {alias} ON c.research_id = {alias}.research_id")

    if not select_parts:
        print("  No new columns to add — canonical is already up to date")
        return cur_col_count

    n_new = len(select_parts)
    print(f"\n  Adding {n_new} new columns via rebuild...")

    if dry_run:
        print(f"  [DRY RUN] Would add {n_new} columns")
        for sp in select_parts:
            col_name = sp.split(" AS ")[-1].strip().strip('"')
            print(f"    + {col_name}")
        return cur_col_count + n_new

    cte_block = ",\n".join([p.strip().rstrip(",") for p in cte_parts])
    select_block = ",\n    ".join(select_parts)
    join_block = "\n".join(join_parts)

    if cte_block:
        rebuild_sql = f"""
WITH
{cte_block}

SELECT
    c.*,
    {select_block}
FROM {CANONICAL} c
{join_block}
"""
    else:
        rebuild_sql = f"""
SELECT
    c.*,
    {select_block}
FROM {CANONICAL} c
{join_block}
"""

    staging = f"{CANONICAL}_staging_216"
    t0 = time.time()
    print(f"  Creating staging table {staging}...")
    con.execute(f"DROP TABLE IF EXISTS {staging}")
    con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")

    if not check_invariants(con, staging, "Staging"):
        print("  ABORTING — invariant failure")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    print("  Invariants passed — swapping tables...")
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    elapsed = time.time() - t0
    new_col_count = len(get_existing_columns(con))
    print(f"  ✓ Canonical rebuilt: {TOTAL_ROWS} × {new_col_count} columns "
          f"(+{new_col_count - cur_col_count}) in {elapsed:.1f}s")
    return new_col_count


# ======================================================================
# C3: Final validation + coverage report
# ======================================================================

def task_c3_validate(con: duckdb.DuckDBPyConnection) -> None:
    """Final validation and coverage report."""
    print("\n" + "=" * 70)
    print("[216] TASK C3: Final Validation + Coverage Report")
    print("=" * 70)

    if not check_invariants(con, CANONICAL, "Final"):
        print("  INVARIANT FAILURE — manual review required")
        return

    print("\n  Coverage report for new columns:")
    new_col_prefixes = ["mri_", "gland_weight_", "nsqip_", "ops_", "lab_pth_", "lab_calcium_",
                        "lab_tsh_", "lab_vitd_"]
    cols = get_existing_columns(con)
    report_cols = sorted([c for c in cols if any(c.startswith(p) for p in new_col_prefixes)])

    for col in report_cols:
        try:
            row = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) AS non_null,
                    ROUND(COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) * 100.0 / {TOTAL_ROWS}, 1) AS pct
                FROM {CANONICAL}
            """).fetchone()
            print(f"    {col:50s}: {row[0]:>6,} ({row[1]:>5.1f}%)")
        except Exception as e:
            print(f"    {col:50s}: ERROR ({e})")

    total_cols = len(cols)
    print(f"\n  FINAL: canonical_patient_master_v1 = {TOTAL_ROWS} rows × {total_cols} columns")

    xval = [
        ("OP Sheet structured prior_neck_op", """
            SELECT COUNT(*) FROM canonical_patient_master_v1
            WHERE ops_prior_neck_operation IS NOT NULL
              AND op_nlp_parathyroid_managed IS NOT NULL
        """),
        ("Weight cross-check (syn vs weight_data)", """
            SELECT COUNT(*) FROM canonical_patient_master_v1
            WHERE syn_total_weight_g IS NOT NULL AND gland_weight_combined_g IS NOT NULL
        """),
    ]
    print("\n  Cross-validation:")
    for label, sql in xval:
        try:
            val = con.execute(sql).fetchone()[0]
            print(f"    {label}: {val} patients with both sources")
        except Exception as e:
            print(f"    {label}: ERROR ({e})")


# ======================================================================
# Main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Script 216: Data Gap Resolution")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying DB")
    parser.add_argument("--phase", default="all",
                        help="Run specific phase: A1|A2|A3|A4|A5|C1|C3|all")
    args = parser.parse_args()

    con = connect()
    print(f"[216] Connected to {DB}")
    print(f"[216] Dry run: {args.dry_run}")

    phases = args.phase.upper().split(",") if args.phase != "all" else [
        "A1", "A2", "A3", "A4", "A5", "C1", "C3"
    ]
    print(f"[216] Phases: {phases}")

    cur_cols = len(get_existing_columns(con))
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[216] Current canonical: {cur_rows} rows × {cur_cols} columns")

    if "A1" in phases:
        task_a1_lab_fix(con, args.dry_run)

    if "A2" in phases:
        task_a2_mri_ingest(con, args.dry_run)

    if "A3" in phases:
        task_a3_weight_ingest(con, args.dry_run)

    if "A4" in phases:
        task_a4_nsqip(con, args.dry_run)

    if "A5" in phases:
        task_a5_opsheet(con, args.dry_run)

    if "C1" in phases:
        task_c1_canonical_rebuild(con, args.dry_run)

    if "C3" in phases:
        task_c3_validate(con)

    print("\n[216] Done.")
    con.close()


if __name__ == "__main__":
    main()
