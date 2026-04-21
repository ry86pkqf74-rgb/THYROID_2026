#!/usr/bin/env python3
"""
THYROID_2026 — Script 214: Final Canonical Integration (Clean + Validate + Integrate)
Database: thyroid_ete_fix_20260413

Integrates ALL remaining structured data into canonical_patient_master_v1 with
rigorous cleaning, semantic duplicate detection, cross-validation, and provenance.

Phases:
  1. gold_master_patient_facts_v1   → gm_* columns (raw pathology, provenance)
  2. patient_refined_master_clinical_v12 → prm_* columns (refined clinical)
  3. path_synoptics                 → syn_* columns (cleaned synoptic pathology)
  4. longitudinal_lab_canonical_v1  → lab_* columns (TSH, Vitamin D)
  5. ultrasound_reports             → us_* columns (thyroid volumes)

Run:
  .venv/bin/python scripts/214_final_canonical_integration.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10871

# ======================================================================
# Cleaning helpers
# ======================================================================

def clean_x_marker_to_bool(val: Any) -> bool | None:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("x", "1", "yes", "y", "true", "present"):
        return True
    if v in ("0", "no", "n", "false", "absent", "none", ""):
        return False
    return None


def clean_frozen_section(val: Any) -> bool | None:
    if val is None:
        return None
    v = str(val).strip().lower().rstrip(";").strip()
    if v in ("yes", "y", "1", "true"):
        return True
    if v in ("no", "n", "0", "false", "n o"):
        return False
    if v in ("n/s", "n/a", ""):
        return None
    if len(v) > 20:
        return None
    return None


def clean_parathyroid_in_specimen(val: Any) -> bool | None:
    if val is None:
        return None
    v = str(val).strip().lower().rstrip(";").rstrip(".").strip()
    if v in ("yes", "y", "1", "true", "x"):
        return True
    if v in ("no", "n", "0", "false"):
        return False
    return None


def clean_capsular_invasion(val: Any) -> str | None:
    if val is None:
        return None
    v = str(val).strip().lower().rstrip(";").strip()
    if v in ("x", "present", "yes", "true", "1"):
        return "present"
    if v in ("minimally invasive", "minimal", "focal"):
        return "minimally_invasive"
    if v in ("widely invasive", "extensive"):
        return "widely_invasive"
    if v in ("encapsulated", "totally encapsulated", "completely encapsulated",
             "encapsulated/well-demarcated", "encapsualted"):
        return "encapsulated"
    if v in ("partially encapsulated", "partial"):
        return "partially_encapsulated"
    if v in ("no", "none", "absent", "false", "0"):
        return "absent"
    if v in ("indeterminate", "c/a", "n/s", "n/a", "indeeterminate",
             "indeterminent", "indetermiante"):
        return None
    return None


def clean_necrosis(val: Any) -> str | None:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("x", "present", "preesent", "prewsent"):
        return "present"
    if v in ("extensive", "extesive"):
        return "extensive"
    if v in ("focal",):
        return "focal"
    if v in ("absent", "no", "none", "false", "0"):
        return "absent"
    if v in ("indeterminate", "see comment", "c/a", "n/s"):
        return None
    if "present" in v:
        return "present"
    return None


def clean_lymphatic_invasion(val: Any) -> str | None:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("x", "present", "preesent"):
        return "present"
    if v in ("extensive", "extensivre", "extensiver", "extesive"):
        return "extensive"
    if v in ("focal",):
        return "focal"
    if v in ("absent", "no", "none", "false", "0"):
        return "absent"
    if v in ("indeterminate", "indeeterminate", "indeterminent",
             "indetermiante", "suspicious", "c/a", "n/s"):
        return "indeterminate"
    if len(v) > 30:
        return None
    return None


def clean_mitotic_rate(val: Any) -> tuple[float | None, str | None]:
    if val is None:
        return None, None
    v = str(val).strip().lower()
    if v in ("c/a", "n/s", "n/a", ""):
        return None, None
    if v == "increased":
        return None, "elevated"
    try:
        return float(v), None
    except ValueError:
        pass
    if v.startswith("<"):
        try:
            return float(v[1:].strip()), "less_than"
        except ValueError:
            return None, None
    if v.startswith(">"):
        try:
            return float(v[1:].strip()), "greater_than"
        except ValueError:
            return None, None
    num = re.search(r"(\d+\.?\d*)", v)
    if num:
        try:
            return float(num.group(1)), "extracted"
        except ValueError:
            pass
    return None, None


def clean_gland_weight(val: Any) -> float | None:
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("n/a", "n/s", "", "c/a"):
        return None
    v = v.replace(";", "").replace("g", "").strip()
    try:
        w = float(v)
        if 0 < w <= 500:
            return w
        return None
    except ValueError:
        return None


def clean_us_volume(val: Any) -> float | None:
    if val is None:
        return None
    v = str(val).strip()
    v = v.replace(" mL", "").replace(" ml", "").replace("mL", "").replace("ml", "").strip()
    try:
        vol = float(v)
        if 0 < vol <= 1000:
            return vol
        return None
    except ValueError:
        return None


def clean_architecture(val: Any) -> str | None:
    if val is None or (not isinstance(val, str) and pd.isna(val)):
        return None
    v = str(val).strip().lower().rstrip(";").strip()
    if not v or v in ("n/a", "n/s", "c/a", "nan", "nat", "none"):
        return None
    if len(v) > 60:
        return None
    return v


def clean_margin_distance(val: Any) -> str | None:
    if val is None or (not isinstance(val, str) and pd.isna(val)):
        return None
    v = str(val).strip()
    if not v or v.lower() in ("n/a", "n/s", "c/a", "nan", "nat", "none"):
        return None
    return v


# ======================================================================
# Connection
# ======================================================================

def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[214] ERROR: No MotherDuck token found.")
        sys.exit(1)
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ======================================================================
# Semantic duplicate check
# ======================================================================

def semantic_dup_check(
    con: duckdb.DuckDBPyConnection,
    new_expr: str,
    existing_col: str,
    label: str,
) -> dict[str, int]:
    """Compare a new expression against an existing canonical column."""
    sql = f"""
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE n_val IS NOT NULL AND e_val IS NOT NULL
                         AND CAST(n_val AS VARCHAR) = CAST(e_val AS VARCHAR)) AS identical,
        COUNT(*) FILTER (WHERE n_val IS NOT NULL AND e_val IS NOT NULL
                         AND CAST(n_val AS VARCHAR) != CAST(e_val AS VARCHAR)) AS different,
        COUNT(*) FILTER (WHERE n_val IS NULL AND e_val IS NOT NULL) AS new_null,
        COUNT(*) FILTER (WHERE n_val IS NOT NULL AND e_val IS NULL) AS new_fills_gap
    FROM (
        SELECT
            ({new_expr}) AS n_val,
            c."{existing_col}" AS e_val
        FROM {CANONICAL} c
    )
    """
    try:
        row = con.execute(sql).fetchone()
        result = {
            "total": row[0], "identical": row[1], "different": row[2],
            "new_null": row[3], "new_fills_gap": row[4],
        }
        decision = "ADD"
        if result["identical"] == result["total"]:
            decision = "SKIP (exact duplicate)"
        elif result["different"] > 0 and result["new_fills_gap"] == 0:
            decision = "SKIP (existing is superset)"
        elif result["new_fills_gap"] > 0 and result["different"] == 0:
            decision = "ADD (fills gaps only)"
        elif result["new_fills_gap"] > 0 and result["different"] > 0:
            decision = "ADD (fills gaps + has differences)"
        print(f"  SemDup [{label}]: identical={result['identical']}, diff={result['different']}, "
              f"fills_gap={result['new_fills_gap']} → {decision}")
        result["decision"] = decision
        return result
    except Exception as e:
        print(f"  SemDup [{label}]: SKIP (error: {e})")
        return {"decision": "SKIP (error)"}


# ======================================================================
# Phase builders
# ======================================================================

def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


# ----- PHASE 1: gold_master_patient_facts_v1 -----

PHASE1_SQL = """
gm_cte AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        tg_below_threshold_ever AS gm_tg_below_threshold_ever,
        lab_completeness_score AS gm_lab_completeness_score,
        provenance_confidence AS gm_provenance_confidence,
        CAST(path_ete_raw AS VARCHAR) AS gm_path_ete_raw,
        CAST(path_vascular_invasion_raw AS VARCHAR) AS gm_path_vascular_inv_raw,
        CAST(path_lvi_raw AS VARCHAR) AS gm_path_lvi_raw,
        CAST(path_pni_raw AS VARCHAR) AS gm_path_pni_raw,
        CAST(path_ene_raw AS VARCHAR) AS gm_path_ene_raw,
        CAST(path_stage_raw AS VARCHAR) AS gm_path_stage_raw,
        CAST(path_m_stage_raw AS VARCHAR) AS gm_path_m_stage_raw,
        CAST(recurrence_date_source AS VARCHAR) AS gm_recurrence_date_source,
        CAST(recurrence_type_primary AS VARCHAR) AS gm_recurrence_type_primary,
        CAST(recurrence_site_primary AS VARCHAR) AS gm_recurrence_site_primary,
        CAST(recurrence_source AS VARCHAR) AS gm_recurrence_source,
        CAST(rai_date_confidence AS VARCHAR) AS gm_rai_date_confidence,
        CAST(rai_date_source AS VARCHAR) AS gm_rai_date_source
    FROM gold_master_patient_facts_v1
)
"""

PHASE1_COLS = [
    "gm_tg_below_threshold_ever", "gm_lab_completeness_score",
    "gm_provenance_confidence", "gm_path_ete_raw", "gm_path_vascular_inv_raw",
    "gm_path_lvi_raw", "gm_path_pni_raw", "gm_path_ene_raw",
    "gm_path_stage_raw", "gm_path_m_stage_raw",
    "gm_recurrence_date_source", "gm_recurrence_type_primary",
    "gm_recurrence_site_primary", "gm_recurrence_source",
    "gm_rai_date_confidence", "gm_rai_date_source",
]


# ----- PHASE 2: patient_refined_master_clinical_v12 -----

PHASE2_SQL = """
prm_dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY research_id
        ORDER BY refined_at DESC NULLS LAST) AS rn
    FROM patient_refined_master_clinical_v12
),
prm_cte AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        CAST(ete_imaging_path_concordance AS VARCHAR) AS prm_ete_imaging_path_concordance,
        CAST(ete_path_confirmed AS BOOLEAN) AS prm_ete_path_confirmed,
        CAST(ete_rule_applied AS VARCHAR) AS prm_ete_rule_applied,
        CAST(size_concordance AS VARCHAR) AS prm_size_concordance,
        TRY_CAST(first_fna_date AS DATE) AS prm_first_fna_date,
        TRY_CAST(last_fna_date AS DATE) AS prm_last_fna_date,
        TRY_CAST(fna_n_sources AS INTEGER) AS prm_fna_n_sources,
        CAST(fna_source_tables AS VARCHAR) AS prm_fna_source_tables,
        CAST(high_risk_marker_any AS BOOLEAN) AS prm_high_risk_marker_any,
        CAST(molecular_risk_category AS VARCHAR) AS prm_molecular_risk_category,
        CAST(margin_with_gross_ete AS VARCHAR) AS prm_margin_with_gross_ete,
        CAST(margin_confidence AS VARCHAR) AS prm_margin_confidence,
        CAST(margin_source AS VARCHAR) AS prm_margin_source,
        CAST(structural_disease_flag AS BOOLEAN) AS prm_structural_disease_flag,
        CAST(imaging_data_completeness AS VARCHAR) AS prm_imaging_data_completeness,
        CAST(recurrence_detection_category AS VARCHAR) AS prm_recurrence_detection_category,
        TRY_CAST(n_recurrence_sources AS INTEGER) AS prm_n_recurrence_sources,
        CAST(rln_worst_grade AS VARCHAR) AS prm_rln_worst_grade,
        TRY_CAST(followup_clinical_events AS INTEGER) AS prm_followup_clinical_events,
        CAST(followup_has_complications AS BOOLEAN) AS prm_followup_has_complications,
        TRY_CAST(followup_tg_labs AS INTEGER) AS prm_followup_tg_labs,
        CAST(tg_adequate_followup AS BOOLEAN) AS prm_tg_adequate_followup,
        CAST(hypocalcemia_lab_flag AS BOOLEAN) AS prm_hypocalcemia_lab_flag,
        CAST(hypoparathyroidism_lab_flag AS BOOLEAN) AS prm_hypoparathyroidism_lab_flag
    FROM prm_dedup WHERE rn = 1
)
"""

PHASE2_COLS = [
    "prm_ete_imaging_path_concordance", "prm_ete_path_confirmed",
    "prm_ete_rule_applied", "prm_size_concordance",
    "prm_first_fna_date", "prm_last_fna_date",
    "prm_fna_n_sources", "prm_fna_source_tables",
    "prm_high_risk_marker_any", "prm_molecular_risk_category",
    "prm_margin_with_gross_ete", "prm_margin_confidence", "prm_margin_source",
    "prm_structural_disease_flag", "prm_imaging_data_completeness",
    "prm_recurrence_detection_category", "prm_n_recurrence_sources",
    "prm_rln_worst_grade",
    "prm_followup_clinical_events", "prm_followup_has_complications",
    "prm_followup_tg_labs", "prm_tg_adequate_followup",
    "prm_hypocalcemia_lab_flag", "prm_hypoparathyroidism_lab_flag",
]


# ----- PHASE 4: longitudinal_lab_canonical_v1 (TSH + VitD) -----

PHASE4_SQL = """
lab_tsh_cte AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        MIN(value_numeric) AS lab_tsh_min,
        MAX(value_numeric) AS lab_tsh_max,
        COUNT(*) AS lab_tsh_n_measurements,
        MIN(TRY_CAST(lab_date AS DATE)) AS lab_tsh_first_date,
        MAX(TRY_CAST(lab_date AS DATE)) AS lab_tsh_last_date
    FROM longitudinal_lab_canonical_v1
    WHERE LOWER(analyte_group) = 'tsh'
      AND value_numeric IS NOT NULL
    GROUP BY CAST(research_id AS BIGINT)
),
lab_vitd_cte AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        MIN(value_numeric) AS lab_vitd_min,
        MAX(value_numeric) AS lab_vitd_max,
        COUNT(*) AS lab_vitd_n_measurements
    FROM longitudinal_lab_canonical_v1
    WHERE LOWER(analyte_group) = 'vitamin_d'
      AND value_numeric IS NOT NULL
    GROUP BY CAST(research_id AS BIGINT)
)
"""

PHASE4_COLS = [
    "lab_tsh_min", "lab_tsh_max", "lab_tsh_n_measurements",
    "lab_tsh_first_date", "lab_tsh_last_date",
    "lab_vitd_min", "lab_vitd_max", "lab_vitd_n_measurements",
]


# ----- PHASE 5: ultrasound_reports (thyroid volumes) -----

PHASE5_SQL = """
us_ranked AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        total_thyroid_volume_ml,
        left_lobe_volume_ml,
        right_lobe_volume_ml,
        isthmus_thickness,
        TRY_CAST(ultrasound_date AS DATE) AS us_date,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS BIGINT)
            ORDER BY TRY_CAST(ultrasound_date AS DATE) DESC NULLS LAST) AS rn
    FROM raw.ultrasound_reports
),
us_latest AS (
    SELECT * FROM us_ranked WHERE rn = 1
),
us_counts AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        COUNT(*) AS us_n_reports,
        MAX(TRY_CAST(ultrasound_date AS DATE)) AS us_most_recent_date
    FROM raw.ultrasound_reports
    GROUP BY CAST(research_id AS BIGINT)
)
"""

PHASE5_COLS = [
    "us_left_lobe_volume_ml", "us_right_lobe_volume_ml",
    "us_total_volume_ml", "us_isthmus_thickness_mm",
    "us_n_reports", "us_most_recent_date",
]


# ======================================================================
# Phase 3: path_synoptics — cleaned in Python, then joined via parquet
# ======================================================================

def build_synoptic_rollup(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Pull path_synoptics, clean in Python, rollup to patient level."""
    print("[214] Phase 3: Loading path_synoptics...")

    df = con.execute("""
        SELECT
            CAST(research_id AS BIGINT) AS research_id,
            TRY_CAST(surg_date AS DATE) AS surg_date,
            frozen_section_obtained,
            fs_pathology_frozen_section,
            carcinoma_identified_on_fs_sent_intraop,
            tumor_1_capsular_invasion,
            tumor_1_necrosis,
            tumor_1_lymphatic_invasion,
            tumor_1_angioinvasion,
            tumor_1_mitotic_rate_per_2mm2,
            tumor_1_architecture,
            tumor_1_histologic_grade,
            tumor_1_margin_status,
            tumor_1_distance_to_closest_margin_mm,
            tumor_1_ki_67_labeling_index,
            hashimoto_thyroiditis,
            graves,
            multinodular_goiter,
            chronic_thyroiditis,
            chronic_lymphocytic_thyroiditis,
            follicular_adenoma,
            hurthle_cell_change,
            hurthle_cell_metaplasia,
            hurthle_cell_nodule,
            hurthle_cell_oncocytic_adenoma,
            hyperplastic_nodules,
            hyperplastic_change_follicular_hyperplasia,
            adenomatoid_nodules,
            c_cell_hyperplasia,
            colloid_nodule,
            colloid_cyst,
            ll_g,
            rl_g,
            isthmus_g,
            weight_total,
            ll_size_cm,
            rl_size_cm,
            isthmus_size_cm,
            parathyroid_gland_or_tissue_included_in_resected_specimen,
            parag_1_location,
            parag_2_location,
            parag_3_location,
            parag_4_location,
            parag_5_location,
            parag_6_location,
            central_compartment_dissection,
            bilateral_neck_dissection,
            io_rln_monitoring,
            tumor_2_histologic_type,
            tumor_2_size_greatest_dimension_cm,
            tumor_3_histologic_type,
            tumor_4_histologic_type,
            tumor_5_histologic_type,
            substernal_multinodular_goiter
        FROM path_synoptics
    """).fetchdf()

    print(f"  Loaded {len(df)} rows for {df['research_id'].nunique()} patients")

    # -- Clean each field --
    df["syn_frozen_section"] = df["frozen_section_obtained"].apply(clean_frozen_section)
    df["syn_frozen_section_result"] = df["fs_pathology_frozen_section"].astype(str).where(
        df["fs_pathology_frozen_section"].notna(), None
    )
    df["syn_carcinoma_on_frozen"] = df["carcinoma_identified_on_fs_sent_intraop"].apply(clean_x_marker_to_bool)

    df["syn_capsular_invasion_clean"] = df["tumor_1_capsular_invasion"].apply(clean_capsular_invasion)
    df["syn_necrosis_clean"] = df["tumor_1_necrosis"].apply(clean_necrosis)
    df["syn_lymphatic_invasion_clean"] = df["tumor_1_lymphatic_invasion"].apply(
        clean_lymphatic_invasion
    )

    mito = df["tumor_1_mitotic_rate_per_2mm2"].apply(clean_mitotic_rate)
    df["syn_mitotic_rate_numeric"] = mito.apply(lambda x: x[0] if x else None)
    df["syn_mitotic_rate_qualifier"] = mito.apply(lambda x: x[1] if x else None)

    df["syn_architecture"] = df["tumor_1_architecture"].apply(clean_architecture)
    df["syn_histologic_grade"] = pd.to_numeric(
        df["tumor_1_histologic_grade"], errors="coerce"
    )
    df["syn_ki67_index"] = df["tumor_1_ki_67_labeling_index"].astype(str).where(
        df["tumor_1_ki_67_labeling_index"].notna() & (df["tumor_1_ki_67_labeling_index"].astype(str).str.strip() != ""), None
    )
    df["syn_margin_status_synoptic"] = df["tumor_1_margin_status"].astype(str).where(
        df["tumor_1_margin_status"].notna(), None
    )
    df["syn_margin_distance_mm"] = df["tumor_1_distance_to_closest_margin_mm"].apply(
        clean_margin_distance
    )

    # Multi-tumor
    tumor_cols = ["tumor_2_histologic_type", "tumor_3_histologic_type",
                  "tumor_4_histologic_type", "tumor_5_histologic_type"]
    df["syn_has_second_tumor"] = df["tumor_2_histologic_type"].notna()
    df["syn_has_third_plus_tumor"] = df[tumor_cols[1:]].notna().any(axis=1)
    df["syn_n_tumors_in_synoptic"] = 1 + df[tumor_cols].notna().sum(axis=1)
    df["syn_tumor2_histologic_type"] = df["tumor_2_histologic_type"].astype(str).where(
        df["tumor_2_histologic_type"].notna(), None
    )
    df["syn_tumor2_size_cm"] = df["tumor_2_size_greatest_dimension_cm"].astype(str).where(
        df["tumor_2_size_greatest_dimension_cm"].notna(), None
    )

    # Benign concurrent pathology
    df["syn_hashimoto"] = df["hashimoto_thyroiditis"].apply(clean_x_marker_to_bool)
    df["syn_graves"] = df["graves"].apply(clean_x_marker_to_bool)
    df["syn_multinodular_goiter"] = df["multinodular_goiter"].apply(clean_x_marker_to_bool)
    mng_sub = df["substernal_multinodular_goiter"].apply(clean_x_marker_to_bool)
    df["syn_multinodular_goiter"] = df["syn_multinodular_goiter"].combine_first(mng_sub)
    df["syn_multinodular_goiter"] = df["syn_multinodular_goiter"].apply(
        lambda x: True if x is True else (False if x is False else None)
    )
    ct1 = df["chronic_thyroiditis"].apply(clean_x_marker_to_bool)
    ct2 = df["chronic_lymphocytic_thyroiditis"].apply(clean_x_marker_to_bool)
    df["syn_chronic_thyroiditis"] = ct1.combine_first(ct2)
    df["syn_chronic_thyroiditis"] = df["syn_chronic_thyroiditis"].apply(
        lambda x: True if x is True else (False if x is False else None)
    )
    df["syn_follicular_adenoma"] = df["follicular_adenoma"].apply(clean_x_marker_to_bool)
    hcc = pd.concat([
        df["hurthle_cell_change"].apply(clean_x_marker_to_bool),
        df["hurthle_cell_metaplasia"].apply(clean_x_marker_to_bool),
        df["hurthle_cell_nodule"].apply(clean_x_marker_to_bool),
        df["hurthle_cell_oncocytic_adenoma"].apply(clean_x_marker_to_bool),
    ], axis=1)
    df["syn_hurthle_cell_change"] = hcc.any(axis=1).where(hcc.notna().any(axis=1), None)
    hn1 = df["hyperplastic_nodules"].apply(clean_x_marker_to_bool)
    hn2 = df["hyperplastic_change_follicular_hyperplasia"].apply(clean_x_marker_to_bool)
    df["syn_hyperplastic_nodules"] = hn1.combine_first(hn2)
    df["syn_hyperplastic_nodules"] = df["syn_hyperplastic_nodules"].apply(
        lambda x: True if x is True else (False if x is False else None)
    )
    df["syn_adenomatoid_nodules"] = df["adenomatoid_nodules"].apply(clean_x_marker_to_bool)
    df["syn_c_cell_hyperplasia"] = df["c_cell_hyperplasia"].apply(clean_x_marker_to_bool)
    cn1 = df["colloid_nodule"].apply(clean_x_marker_to_bool)
    cn2 = df["colloid_cyst"].apply(clean_x_marker_to_bool)
    df["syn_colloid_nodule"] = cn1.combine_first(cn2)
    df["syn_colloid_nodule"] = df["syn_colloid_nodule"].apply(
        lambda x: True if x is True else (False if x is False else None)
    )

    # Gland weights
    df["syn_left_lobe_weight_g"] = df["ll_g"].apply(clean_gland_weight)
    df["syn_right_lobe_weight_g"] = df["rl_g"].apply(clean_gland_weight)
    df["syn_isthmus_weight_g"] = df["isthmus_g"].apply(clean_gland_weight)
    df["syn_total_weight_g"] = df["weight_total"].apply(clean_gland_weight)
    df["syn_left_lobe_size_cm"] = df["ll_size_cm"].astype(str).where(
        df["ll_size_cm"].notna(), None
    )
    df["syn_right_lobe_size_cm"] = df["rl_size_cm"].astype(str).where(
        df["rl_size_cm"].notna(), None
    )
    df["syn_isthmus_size_cm"] = df["isthmus_size_cm"].astype(str).where(
        df["isthmus_size_cm"].notna(), None
    )

    # Parathyroid
    para_cols = [f"parag_{i}_location" for i in range(1, 7)]
    df["syn_n_parathyroid_identified"] = df[para_cols].notna().sum(axis=1)
    df["syn_parathyroid_in_specimen"] = df["parathyroid_gland_or_tissue_included_in_resected_specimen"].apply(
        clean_parathyroid_in_specimen
    )

    # Surgical detail
    df["syn_central_dissection"] = df["central_compartment_dissection"].apply(
        clean_x_marker_to_bool
    )
    df["syn_bilateral_neck_dissection"] = df["bilateral_neck_dissection"].apply(
        clean_x_marker_to_bool
    )
    df["syn_io_rln_monitoring"] = df["io_rln_monitoring"].apply(clean_x_marker_to_bool)

    # ----- Rollup to patient level -----
    syn_cols = [c for c in df.columns if c.startswith("syn_")]
    agg_dict: dict[str, Any] = {}
    for col in syn_cols:
        dtype = df[col].dtype
        if dtype == "bool" or df[col].dropna().isin([True, False]).all():
            agg_dict[col] = "max"
        elif dtype in ("float64", "int64", "Int64", "Float64"):
            if "weight" in col or "grade" in col or "rate" in col or "n_" in col:
                agg_dict[col] = "max"
            else:
                agg_dict[col] = "first"
        else:
            agg_dict[col] = "first"

    # Sort by most recent surgery first for 'first' aggregation
    df = df.sort_values(["research_id", "surg_date"], ascending=[True, False])
    rollup = df.groupby("research_id", as_index=False).agg(agg_dict)

    print(f"  Rolled up to {len(rollup)} patients, {len(syn_cols)} syn_ columns")

    # ----- Cleaning audit -----
    print("\n  === Synoptic Cleaning Audit ===")
    cleaning_stats = {
        "frozen_section": {
            "input": df["frozen_section_obtained"].notna().sum(),
            "cleaned_true": (df["syn_frozen_section"] == True).sum(),
            "cleaned_false": (df["syn_frozen_section"] == False).sum(),
            "nulled": df["frozen_section_obtained"].notna().sum()
            - (df["syn_frozen_section"].notna()).sum(),
        },
        "capsular_invasion": {
            "input": df["tumor_1_capsular_invasion"].notna().sum(),
            "output": df["syn_capsular_invasion_clean"].notna().sum(),
            "nulled": df["tumor_1_capsular_invasion"].notna().sum()
            - df["syn_capsular_invasion_clean"].notna().sum(),
        },
        "necrosis": {
            "input": df["tumor_1_necrosis"].notna().sum(),
            "output": df["syn_necrosis_clean"].notna().sum(),
            "nulled": df["tumor_1_necrosis"].notna().sum()
            - df["syn_necrosis_clean"].notna().sum(),
        },
    }
    for field, stats in cleaning_stats.items():
        print(f"    {field}: {stats}")

    return rollup[["research_id"] + syn_cols]


SYN_COLS = [
    "syn_architecture",
    "syn_capsular_invasion_clean", "syn_necrosis_clean",
    "syn_mitotic_rate_numeric", "syn_mitotic_rate_qualifier",
    "syn_histologic_grade", "syn_ki67_index",
    "syn_lymphatic_invasion_clean", "syn_margin_status_synoptic",
    "syn_margin_distance_mm",
    "syn_n_tumors_in_synoptic", "syn_has_second_tumor",
    "syn_tumor2_histologic_type", "syn_tumor2_size_cm",
    "syn_has_third_plus_tumor",
    "syn_frozen_section", "syn_frozen_section_result", "syn_carcinoma_on_frozen",
    "syn_hashimoto", "syn_graves", "syn_multinodular_goiter",
    "syn_chronic_thyroiditis", "syn_follicular_adenoma",
    "syn_hurthle_cell_change", "syn_hyperplastic_nodules",
    "syn_adenomatoid_nodules", "syn_c_cell_hyperplasia", "syn_colloid_nodule",
    "syn_left_lobe_weight_g", "syn_right_lobe_weight_g",
    "syn_isthmus_weight_g", "syn_total_weight_g",
    "syn_left_lobe_size_cm", "syn_right_lobe_size_cm", "syn_isthmus_size_cm",
    "syn_n_parathyroid_identified", "syn_parathyroid_in_specimen",
    "syn_central_dissection", "syn_bilateral_neck_dissection",
    "syn_io_rln_monitoring",
]


# ======================================================================
# Main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Script 214: Final Canonical Integration")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = connect()
    print(f"[214] Connected to {DB}")

    existing_cols = get_existing_columns(con)
    cur_col_count = len(existing_cols)
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[214] Current canonical: {cur_rows} rows × {cur_col_count} columns")

    # ==================================================================
    # SEMANTIC DUPLICATE CHECKS
    # ==================================================================
    print("\n[214] === Semantic Duplicate Checks ===")

    sem_checks = [
        ("gm_path_tumor_size_cm",
         "(SELECT g.path_tumor_size_cm FROM gold_master_patient_facts_v1 g WHERE CAST(g.research_id AS BIGINT) = c.research_id LIMIT 1)",
         "tumor_size_cm"),
    ]
    skip_set: set[str] = set()
    for label, new_expr, existing_col in sem_checks:
        if existing_col not in existing_cols:
            print(f"  SemDup [{label}]: existing col '{existing_col}' not found, ADD")
            continue
        result = semantic_dup_check(con, new_expr, existing_col, label)
        if "SKIP" in result.get("decision", ""):
            skip_set.add(label)

    # ==================================================================
    # PHASE 3: Build synoptic rollup in Python
    # ==================================================================
    print("\n[214] === Phase 3: Synoptic Pathology Cleaning ===")
    syn_df = build_synoptic_rollup(con)

    # Register synoptic as a temp table
    tmp_parquet = REPO / "scripts" / "output" / "_syn_rollup_214.parquet"
    # NaN→None coercion guard (v1_1 cleanup, Script 248): ensures pandas
    # NaN/NaT/NA do not serialize as the literal string 'nan'/'NaT'
    # downstream when DuckDB casts the column to VARCHAR on read_parquet.
    syn_df = syn_df.where(pd.notna(syn_df), None)
    syn_df.to_parquet(str(tmp_parquet), index=False)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE syn_rollup AS
        SELECT * FROM read_parquet('{tmp_parquet}')
    """)
    print(f"  Registered syn_rollup temp table: {len(syn_df)} rows")

    if args.dry_run:
        print("\n[214] DRY RUN — skipping table rebuild")
        print(f"  Would add ~{len(PHASE1_COLS) + len(PHASE2_COLS) + len(SYN_COLS) + len(PHASE4_COLS) + len(PHASE5_COLS)} columns")
        try:
            tmp_parquet.unlink()
        except Exception:
            pass
        return

    # ==================================================================
    # BUILD MEGA-JOIN SQL
    # ==================================================================
    print("\n[214] === Building integration SQL ===")

    # Filter out columns that already exist
    def filter_new(cols: list[str]) -> list[str]:
        return [c for c in cols if c not in existing_cols]

    p1_new = filter_new(PHASE1_COLS)
    p2_new = filter_new(PHASE2_COLS)
    p4_new = filter_new(PHASE4_COLS)
    p5_new = filter_new(PHASE5_COLS)
    syn_new = filter_new([c for c in syn_df.columns if c.startswith("syn_")])

    print(f"  Phase 1 (gold_master): {len(p1_new)} new columns (skipped {len(PHASE1_COLS) - len(p1_new)})")
    print(f"  Phase 2 (PRM): {len(p2_new)} new columns (skipped {len(PHASE2_COLS) - len(p2_new)})")
    print(f"  Phase 3 (synoptics): {len(syn_new)} new columns")
    print(f"  Phase 4 (labs): {len(p4_new)} new columns")
    print(f"  Phase 5 (US volumes): {len(p5_new)} new columns")

    total_new = len(p1_new) + len(p2_new) + len(syn_new) + len(p4_new) + len(p5_new)
    print(f"  TOTAL new columns: {total_new}")

    # Build CTE portions
    cte_parts = []
    select_parts = []
    join_parts = []

    # Phase 1
    if p1_new:
        cte_parts.append(PHASE1_SQL)
        select_parts.extend([f"gm.{c}" for c in p1_new])
        join_parts.append("LEFT JOIN gm_cte gm ON c.research_id = gm.research_id")

    # Phase 2
    if p2_new:
        cte_parts.append(PHASE2_SQL)
        select_parts.extend([f"prm.{c}" for c in p2_new])
        join_parts.append("LEFT JOIN prm_cte prm ON c.research_id = prm.research_id")

    # Phase 4
    if p4_new:
        cte_parts.append(PHASE4_SQL)
        tsh_cols = [c for c in p4_new if c.startswith("lab_tsh")]
        vitd_cols = [c for c in p4_new if c.startswith("lab_vitd")]
        if tsh_cols:
            select_parts.extend([f"tsh.{c}" for c in tsh_cols])
            join_parts.append("LEFT JOIN lab_tsh_cte tsh ON c.research_id = tsh.research_id")
        if vitd_cols:
            select_parts.extend([f"vitd.{c}" for c in vitd_cols])
            join_parts.append("LEFT JOIN lab_vitd_cte vitd ON c.research_id = vitd.research_id")

    # Phase 5
    if p5_new:
        cte_parts.append(PHASE5_SQL)
        us_vol_select = []
        for col in p5_new:
            if col == "us_left_lobe_volume_ml":
                us_vol_select.append("usl.left_lobe_volume_ml AS us_left_lobe_volume_ml")
            elif col == "us_right_lobe_volume_ml":
                us_vol_select.append("usl.right_lobe_volume_ml AS us_right_lobe_volume_ml")
            elif col == "us_total_volume_ml":
                us_vol_select.append("usl.total_thyroid_volume_ml AS us_total_volume_ml")
            elif col == "us_isthmus_thickness_mm":
                us_vol_select.append("usl.isthmus_thickness AS us_isthmus_thickness_mm")
            elif col == "us_n_reports":
                us_vol_select.append("usc.us_n_reports")
            elif col == "us_most_recent_date":
                us_vol_select.append("usc.us_most_recent_date")
        select_parts.extend(us_vol_select)
        join_parts.append("LEFT JOIN us_latest usl ON c.research_id = usl.research_id")
        join_parts.append("LEFT JOIN us_counts usc ON c.research_id = usc.research_id")

    # Phase 3 (synoptic) — from temp table
    if syn_new:
        select_parts.extend([f"syn.{c}" for c in syn_new])
        join_parts.append("LEFT JOIN syn_rollup syn ON c.research_id = syn.research_id")

    cte_block = ",\n".join(cte_parts) if cte_parts else ""
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

    # ==================================================================
    # EXECUTE REBUILD (staging table approach)
    # ==================================================================
    t0 = time.time()
    staging = f"{CANONICAL}_staging_214"
    print(f"\n[214] Creating staging table {staging}...")

    con.execute(f"DROP TABLE IF EXISTS {staging}")
    con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")

    # ----- Invariant checks -----
    inv = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT research_id) AS distinct_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM {staging}
    """).fetchone()

    print(f"[214] Staging: {inv[0]} rows, {inv[1]} distinct RIDs, "
          f"{inv[2]} null RIDs, {inv[3]} null fna_path_outcome")

    errors = []
    if inv[0] != TOTAL_ROWS:
        errors.append(f"Row count {inv[0]} != {TOTAL_ROWS}")
    if inv[0] != inv[1]:
        errors.append(f"Duplicate research_ids: {inv[0] - inv[1]}")
    if inv[2] > 0:
        errors.append(f"NULL research_ids: {inv[2]}")

    if errors:
        for e in errors:
            print(f"[214] ERROR: {e}")
        print("[214] Aborting — dropping staging table")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        try:
            tmp_parquet.unlink()
        except Exception:
            pass
        sys.exit(1)

    # ----- Swap -----
    print("[214] Invariants passed — swapping tables...")
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    elapsed = time.time() - t0
    print(f"[214] Rebuild complete in {elapsed:.1f}s")

    # Clean up temp parquet
    try:
        tmp_parquet.unlink()
    except Exception:
        pass

    # ==================================================================
    # POST-INTEGRATION VALIDATION
    # ==================================================================
    print("\n[214] === Post-Integration Validation ===")

    new_col_count = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    added = new_col_count - cur_col_count
    print(f"[214] Columns: {cur_col_count} → {new_col_count} (+{added})")

    # V2 — Existing column integrity (spot check 5 columns)
    print("\n[214] V2: Existing column integrity spot checks...")
    spot_checks = [
        ("age_at_surgery", "gold_master_patient_facts_v1"),
        ("diagnosis_primary", "gold_master_patient_facts_v1"),
    ]
    for col, src in spot_checks:
        if col not in existing_cols:
            continue
        try:
            mismatch = con.execute(f"""
                SELECT COUNT(*) FROM {CANONICAL} c
                JOIN {src} g ON c.research_id = CAST(g.research_id AS BIGINT)
                WHERE c."{col}" IS DISTINCT FROM g."{col}"
            """).fetchone()[0]
            status = "✓ PASS" if mismatch == 0 else f"⚠ {mismatch} mismatches"
            print(f"  {col}: {status}")
        except Exception as e:
            print(f"  {col}: SKIP ({e})")

    # V3 — Cross-validation checks
    print("\n[214] V3: Cross-validation checks...")

    xval_checks = [
        ("frozen_section + surgery",
         f"SELECT COUNT(*) FROM {CANONICAL} WHERE syn_frozen_section IS TRUE AND first_surgery_date IS NULL",
         0),
        ("gland weight range",
         f"SELECT COUNT(*) FROM {CANONICAL} WHERE syn_total_weight_g IS NOT NULL AND (syn_total_weight_g <= 0 OR syn_total_weight_g > 500)",
         5),
    ]
    for label, sql, max_ok in xval_checks:
        try:
            val = con.execute(sql).fetchone()[0]
            status = "✓ PASS" if val <= max_ok else f"⚠ {val} violations"
            print(f"  {label}: {val} ({status})")
        except Exception as e:
            print(f"  {label}: SKIP ({e})")

    # V4 — Coverage report
    print("\n[214] V4: Coverage Report (all new columns)")
    all_new_cols = p1_new + p2_new + syn_new + p4_new + p5_new
    for col in all_new_cols:
        try:
            row = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) AS non_null,
                    ROUND(COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) * 100.0 / {TOTAL_ROWS}, 1) AS pct,
                    COUNT(DISTINCT "{col}") FILTER (WHERE "{col}" IS NOT NULL) AS n_distinct
                FROM {CANONICAL}
            """).fetchone()
            print(f"  {col:45s}: {row[0]:>6,} ({row[1]:>5.1f}%) — {row[2]} distinct")
        except Exception as e:
            print(f"  {col:45s}: ERROR ({e})")

    # Final invariants
    print("\n[214] Final invariant check...")
    final = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT research_id) AS distinct_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM {CANONICAL}
    """).fetchone()
    print(f"  {final[0]} rows, {final[1]} distinct RIDs, {final[2]} null RIDs, "
          f"{final[3]} null fna_path_outcome")

    if final[0] == TOTAL_ROWS and final[1] == TOTAL_ROWS and final[2] == 0:
        print("[214] ✓ All invariants PASS")
    else:
        print("[214] ✗ INVARIANT FAILURE")
        sys.exit(1)

    print(f"\n[214] Done. canonical_patient_master_v1: {TOTAL_ROWS} × {new_col_count} columns")


if __name__ == "__main__":
    main()
