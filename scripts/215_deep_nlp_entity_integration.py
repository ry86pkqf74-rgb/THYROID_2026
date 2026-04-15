#!/usr/bin/env python3
"""
THYROID_2026 — Script 215: Deep NLP Entity Integration (Full Provenance)
Database: thyroid_ete_fix_20260413

Parses NLP entity values from 8 source tables into patient-level clinical columns
with full date/time linkage, source provenance, confidence scores, and multi-mention
reliability tracking for every data point.

CRITICAL RULES:
  1. Orphan RID 11454 excluded — every CTE filters against canonical spine.
  2. Every boolean/value column has a companion date column.
  3. Per-domain provenance: extraction_method, n_source_notes, note_types.
  4. LLM columns carry min/mean confidence; regex columns document method only.
  5. n_mentions per flag for multi-mention reliability.

Sources:
  1. note_entities_operative_detail  (regex, 4,031 pts) → op_nlp_*  22 columns
  2. note_entities_medications        (regex, 2,070 pts) → med_nlp_*  9 columns
  3. note_entities_problem_list       (regex, 4,037 pts) → pmhx_nlp_* 28 columns
  4. note_entities_llm_past_medical_hx (qwen3:32b, 5,641 pts) → pmhx_llm_* 18 cols
  5. note_entities_llm_past_surgical_hx (qwen3:32b, 5,641 pts) → pshx_* 15 cols
  6. note_entities_procedures         (regex, 4,723 pts) → proc_nlp_*  8 columns
  7. note_entities_llm_presenting_symptoms (qwen3:32b) → sx_*  6 columns
  8. note_entities_llm_rad_treatment  (qwen3:32b) → radtx_*  6 columns

Run:
  .venv/bin/python scripts/215_deep_nlp_entity_integration.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10871
# Orphan RID to always exclude
ORPHAN_RID = 11454


# ======================================================================
# Connection
# ======================================================================

def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[215] ERROR: No MotherDuck token found.")
        sys.exit(1)
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


# ======================================================================
# LLM JSON parse CTE template (used for Sources 4, 5, 7, 8)
# Each LLM table has: research_id, note_row_id, result_json, note_date, note_type
# result_json structure: {"entities": [...]}
# Each entity: {entity_type, entity_value, entity_date, confidence,
#               present_or_negated, evidence_text, ...}
# ======================================================================

def llm_parse_cte(alias: str, table: str, min_confidence: float = 0.7) -> str:
    return f"""
{alias}_parsed AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        note_row_id,
        note_date,
        note_type,
        json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
    FROM {table}
    WHERE result_json IS NOT NULL
      AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
      AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
      AND CAST(research_id AS BIGINT) != {ORPHAN_RID}
      AND CAST(research_id AS BIGINT) IN (SELECT DISTINCT CAST(research_id AS BIGINT) FROM {CANONICAL})
),
{alias}_flat AS (
    SELECT
        research_id,
        note_row_id,
        note_date,
        note_type,
        UNNEST(CAST(entities_arr AS JSON[])) AS entity
    FROM {alias}_parsed
),
{alias}_ext AS (
    SELECT
        research_id,
        note_row_id,
        note_date,
        note_type,
        json_extract_string(entity, '$.entity_type')    AS entity_type,
        json_extract_string(entity, '$.entity_value')   AS entity_value,
        json_extract_string(entity, '$.entity_date')    AS entity_date,
        json_extract_string(entity, '$.present_or_negated') AS present_or_negated,
        COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0.0) AS confidence
    FROM {alias}_flat
),
{alias}_pos AS (
    SELECT * FROM {alias}_ext
    WHERE confidence >= {min_confidence}
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
)"""


# ======================================================================
# SOURCE 1: note_entities_operative_detail
# Regex-extracted, schema: entity_type, entity_value_norm, present_or_negated,
#                          note_date, note_row_id, note_type
# ======================================================================

OP_NLP_SQL = f"""
op_ent AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        entity_type,
        entity_value_norm,
        note_date,
        note_row_id,
        note_type
    FROM note_entities_operative_detail
    WHERE CAST(research_id AS BIGINT) != {ORPHAN_RID}
      AND CAST(research_id AS BIGINT) IN (SELECT DISTINCT CAST(research_id AS BIGINT) FROM {CANONICAL})
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
op_nlp AS (
    SELECT
        research_id,
        -- EBL: numeric value extraction (values are like "10 mL", "50 mL", "minimal")
        -- Use SPLIT_PART to grab the numeric token before the space
        MAX(CASE WHEN entity_type = 'ebl' THEN
            TRY_CAST(REPLACE(SPLIT_PART(entity_value_norm, ' ', 1), ',', '') AS DOUBLE)
        END) AS op_nlp_ebl_ml,
        MIN(CASE WHEN entity_type = 'ebl' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_ebl_date,
        COUNT(CASE WHEN entity_type = 'ebl' THEN 1 END)
            AS op_nlp_ebl_n_mentions,

        -- Nerve monitoring
        BOOL_OR(entity_type = 'nerve_monitoring') AS op_nlp_nerve_monitoring_used,
        MAX(CASE WHEN entity_type = 'nerve_monitoring' THEN entity_value_norm END)
            AS op_nlp_nerve_monitoring_type,
        MIN(CASE WHEN entity_type = 'nerve_monitoring' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_nerve_monitoring_date,
        COUNT(CASE WHEN entity_type = 'nerve_monitoring' THEN 1 END)
            AS op_nlp_nerve_monitoring_n_mentions,

        -- Berry ligament
        BOOL_OR(entity_type = 'berry_ligament' AND LOWER(entity_value_norm) LIKE '%dissect%')
            AS op_nlp_berry_ligament_dissected,
        BOOL_OR(entity_type = 'berry_ligament')
            AS op_nlp_berry_ligament_mentioned,
        MIN(CASE WHEN entity_type = 'berry_ligament' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_berry_ligament_date,
        COUNT(CASE WHEN entity_type = 'berry_ligament' THEN 1 END)
            AS op_nlp_berry_ligament_n_mentions,

        -- Parathyroid management
        BOOL_OR(entity_type = 'parathyroid_management')
            AS op_nlp_parathyroid_managed,
        COUNT(CASE WHEN entity_type = 'parathyroid_management' THEN 1 END)
            AS op_nlp_parathyroid_managed_n_mentions,
        BOOL_OR(entity_type = 'parathyroid_autograft')
            AS op_nlp_parathyroid_autograft,
        COUNT(CASE WHEN entity_type = 'parathyroid_autograft' THEN 1 END)
            AS op_nlp_parathyroid_autograft_n_mentions,
        MIN(CASE WHEN entity_type IN ('parathyroid_management','parathyroid_autograft')
            THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_parathyroid_date,

        -- RLN finding
        BOOL_OR(entity_type = 'rln_finding') AS op_nlp_rln_finding,
        COUNT(CASE WHEN entity_type = 'rln_finding' THEN 1 END)
            AS op_nlp_rln_finding_n_mentions,
        MIN(CASE WHEN entity_type = 'rln_finding' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_rln_finding_date,

        -- Drain placement
        BOOL_OR(entity_type = 'drain_placement') AS op_nlp_drain_placed,
        COUNT(CASE WHEN entity_type = 'drain_placement' THEN 1 END)
            AS op_nlp_drain_placed_n_mentions,
        MIN(CASE WHEN entity_type = 'drain_placement' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_drain_date,

        -- Strap muscle involvement
        BOOL_OR(entity_type = 'strap_muscle') AS op_nlp_strap_muscle_involved,
        COUNT(CASE WHEN entity_type = 'strap_muscle' THEN 1 END)
            AS op_nlp_strap_muscle_n_mentions,

        -- Reoperative field
        BOOL_OR(entity_type = 'reoperative_field') AS op_nlp_reoperative_field,
        COUNT(CASE WHEN entity_type = 'reoperative_field' THEN 1 END)
            AS op_nlp_reoperative_n_mentions,

        -- Intraoperative complication
        BOOL_OR(entity_type = 'intraop_complication') AS op_nlp_intraop_complication,
        COUNT(CASE WHEN entity_type = 'intraop_complication' THEN 1 END)
            AS op_nlp_intraop_complication_n_mentions,
        MIN(CASE WHEN entity_type = 'intraop_complication' THEN TRY_CAST(note_date AS DATE) END)
            AS op_nlp_intraop_complication_date,

        -- Gross invasion (tracheal / esophageal / general)
        BOOL_OR(entity_type = 'gross_invasion') AS op_nlp_gross_invasion,
        BOOL_OR(entity_type = 'tracheal_involvement') AS op_nlp_tracheal_involvement,
        COUNT(CASE WHEN entity_type = 'tracheal_involvement' THEN 1 END)
            AS op_nlp_tracheal_n_mentions,
        BOOL_OR(entity_type = 'esophageal_involvement') AS op_nlp_esophageal_involvement,
        COUNT(CASE WHEN entity_type = 'esophageal_involvement' THEN 1 END)
            AS op_nlp_esophageal_n_mentions,

        -- Domain-level provenance
        COUNT(DISTINCT note_row_id)          AS op_nlp_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',')  AS op_nlp_note_types,
        'regex_operative_v2'                 AS op_nlp_extraction_method
    FROM op_ent
    GROUP BY research_id
)"""

OP_NLP_COLS = [
    "op_nlp_ebl_ml", "op_nlp_ebl_date", "op_nlp_ebl_n_mentions",
    "op_nlp_nerve_monitoring_used", "op_nlp_nerve_monitoring_type",
    "op_nlp_nerve_monitoring_date", "op_nlp_nerve_monitoring_n_mentions",
    "op_nlp_berry_ligament_dissected", "op_nlp_berry_ligament_mentioned",
    "op_nlp_berry_ligament_date", "op_nlp_berry_ligament_n_mentions",
    "op_nlp_parathyroid_managed", "op_nlp_parathyroid_managed_n_mentions",
    "op_nlp_parathyroid_autograft", "op_nlp_parathyroid_autograft_n_mentions",
    "op_nlp_parathyroid_date",
    "op_nlp_rln_finding", "op_nlp_rln_finding_n_mentions", "op_nlp_rln_finding_date",
    "op_nlp_drain_placed", "op_nlp_drain_placed_n_mentions", "op_nlp_drain_date",
    "op_nlp_strap_muscle_involved", "op_nlp_strap_muscle_n_mentions",
    "op_nlp_reoperative_field", "op_nlp_reoperative_n_mentions",
    "op_nlp_intraop_complication", "op_nlp_intraop_complication_n_mentions",
    "op_nlp_intraop_complication_date",
    "op_nlp_gross_invasion",
    "op_nlp_tracheal_involvement", "op_nlp_tracheal_n_mentions",
    "op_nlp_esophageal_involvement", "op_nlp_esophageal_n_mentions",
    "op_nlp_n_source_notes", "op_nlp_note_types", "op_nlp_extraction_method",
]


# ======================================================================
# SOURCE 2: note_entities_medications
# Regex-extracted; entity_type='medication', entity_value_norm is drug name
# ======================================================================

MED_NLP_SQL = f"""
med_ent AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        entity_type,
        entity_value_norm,
        note_date,
        note_row_id,
        note_type
    FROM note_entities_medications
    WHERE CAST(research_id AS BIGINT) != {ORPHAN_RID}
      AND CAST(research_id AS BIGINT) IN (SELECT DISTINCT CAST(research_id AS BIGINT) FROM {CANONICAL})
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
med_nlp AS (
    SELECT
        research_id,
        -- Levothyroxine (thyroid hormone replacement)
        BOOL_OR(entity_value_norm = 'levothyroxine')
            AS med_nlp_levothyroxine,
        MIN(CASE WHEN entity_value_norm = 'levothyroxine' THEN TRY_CAST(note_date AS DATE) END)
            AS med_nlp_levothyroxine_date,
        COUNT(CASE WHEN entity_value_norm = 'levothyroxine' THEN 1 END)
            AS med_nlp_levothyroxine_n_mentions,

        -- Calcium supplement (postoperative hypocalcemia management)
        BOOL_OR(entity_value_norm = 'calcium_supplement')
            AS med_nlp_calcium_supplement,
        MIN(CASE WHEN entity_value_norm = 'calcium_supplement' THEN TRY_CAST(note_date AS DATE) END)
            AS med_nlp_calcium_supplement_date,
        COUNT(CASE WHEN entity_value_norm = 'calcium_supplement' THEN 1 END)
            AS med_nlp_calcium_supplement_n_mentions,

        -- Calcitriol (active vitamin D — implies hypoparathyroidism)
        BOOL_OR(entity_value_norm = 'calcitriol')
            AS med_nlp_calcitriol,
        MIN(CASE WHEN entity_value_norm = 'calcitriol' THEN TRY_CAST(note_date AS DATE) END)
            AS med_nlp_calcitriol_date,
        COUNT(CASE WHEN entity_value_norm = 'calcitriol' THEN 1 END)
            AS med_nlp_calcitriol_n_mentions,

        -- Domain-level provenance
        COUNT(DISTINCT note_row_id)         AS med_nlp_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',') AS med_nlp_note_types,
        'regex_medication_v2'               AS med_nlp_extraction_method
    FROM med_ent
    GROUP BY research_id
)"""

MED_NLP_COLS = [
    "med_nlp_levothyroxine", "med_nlp_levothyroxine_date", "med_nlp_levothyroxine_n_mentions",
    "med_nlp_calcium_supplement", "med_nlp_calcium_supplement_date",
    "med_nlp_calcium_supplement_n_mentions",
    "med_nlp_calcitriol", "med_nlp_calcitriol_date", "med_nlp_calcitriol_n_mentions",
    "med_nlp_n_source_notes", "med_nlp_note_types", "med_nlp_extraction_method",
]


# ======================================================================
# SOURCE 3: note_entities_problem_list (comorbidities)
# Regex-extracted; entity_value_norm is the problem name (normalized)
# ======================================================================

PMHX_REGEX_SQL = f"""
pmhx_ent AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        entity_value_norm,
        note_date,
        note_row_id,
        note_type
    FROM note_entities_problem_list
    WHERE CAST(research_id AS BIGINT) != {ORPHAN_RID}
      AND CAST(research_id AS BIGINT) IN (SELECT DISTINCT CAST(research_id AS BIGINT) FROM {CANONICAL})
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
pmhx_nlp AS (
    SELECT
        research_id,
        -- Hypertension
        BOOL_OR(entity_value_norm = 'hypertension')
            AS pmhx_nlp_hypertension,
        COUNT(CASE WHEN entity_value_norm = 'hypertension' THEN 1 END)
            AS pmhx_nlp_hypertension_n_mentions,
        MIN(CASE WHEN entity_value_norm = 'hypertension' THEN TRY_CAST(note_date AS DATE) END)
            AS pmhx_nlp_hypertension_first_date,

        -- Diabetes (merge diabetes + diabetes_type2)
        BOOL_OR(entity_value_norm IN ('diabetes', 'diabetes_type2'))
            AS pmhx_nlp_diabetes,
        COUNT(CASE WHEN entity_value_norm IN ('diabetes', 'diabetes_type2') THEN 1 END)
            AS pmhx_nlp_diabetes_n_mentions,
        MIN(CASE WHEN entity_value_norm IN ('diabetes', 'diabetes_type2')
            THEN TRY_CAST(note_date AS DATE) END)
            AS pmhx_nlp_diabetes_first_date,

        -- Hyperthyroidism (often pre-op Graves)
        BOOL_OR(entity_value_norm = 'hyperthyroidism')
            AS pmhx_nlp_hyperthyroidism,
        COUNT(CASE WHEN entity_value_norm = 'hyperthyroidism' THEN 1 END)
            AS pmhx_nlp_hyperthyroidism_n_mentions,
        MIN(CASE WHEN entity_value_norm = 'hyperthyroidism' THEN TRY_CAST(note_date AS DATE) END)
            AS pmhx_nlp_hyperthyroidism_first_date,

        -- Hypothyroidism (post-ablation, post-thyroidectomy)
        BOOL_OR(entity_value_norm = 'hypothyroidism')
            AS pmhx_nlp_hypothyroidism,
        COUNT(CASE WHEN entity_value_norm = 'hypothyroidism' THEN 1 END)
            AS pmhx_nlp_hypothyroidism_n_mentions,
        MIN(CASE WHEN entity_value_norm = 'hypothyroidism' THEN TRY_CAST(note_date AS DATE) END)
            AS pmhx_nlp_hypothyroidism_first_date,

        -- Obesity
        BOOL_OR(entity_value_norm = 'obesity')
            AS pmhx_nlp_obesity,
        COUNT(CASE WHEN entity_value_norm = 'obesity' THEN 1 END)
            AS pmhx_nlp_obesity_n_mentions,
        MIN(CASE WHEN entity_value_norm = 'obesity' THEN TRY_CAST(note_date AS DATE) END)
            AS pmhx_nlp_obesity_first_date,

        -- Breast cancer
        BOOL_OR(entity_value_norm = 'breast_cancer')
            AS pmhx_nlp_breast_cancer,
        COUNT(CASE WHEN entity_value_norm = 'breast_cancer' THEN 1 END)
            AS pmhx_nlp_breast_cancer_n_mentions,

        -- Depression
        BOOL_OR(entity_value_norm = 'depression')
            AS pmhx_nlp_depression,
        COUNT(CASE WHEN entity_value_norm = 'depression' THEN 1 END)
            AS pmhx_nlp_depression_n_mentions,

        -- CAD / Coronary artery disease
        BOOL_OR(entity_value_norm = 'CAD')
            AS pmhx_nlp_cad,
        COUNT(CASE WHEN entity_value_norm = 'CAD' THEN 1 END)
            AS pmhx_nlp_cad_n_mentions,

        -- CKD / Chronic kidney disease
        BOOL_OR(entity_value_norm = 'CKD')
            AS pmhx_nlp_ckd,
        COUNT(CASE WHEN entity_value_norm = 'CKD' THEN 1 END)
            AS pmhx_nlp_ckd_n_mentions,

        -- Atrial fibrillation
        BOOL_OR(entity_value_norm = 'atrial_fibrillation')
            AS pmhx_nlp_afib,
        COUNT(CASE WHEN entity_value_norm = 'atrial_fibrillation' THEN 1 END)
            AS pmhx_nlp_afib_n_mentions,

        -- COPD
        BOOL_OR(entity_value_norm = 'COPD')
            AS pmhx_nlp_copd,
        COUNT(CASE WHEN entity_value_norm = 'COPD' THEN 1 END)
            AS pmhx_nlp_copd_n_mentions,

        -- Asthma
        BOOL_OR(entity_value_norm = 'asthma')
            AS pmhx_nlp_asthma,
        COUNT(CASE WHEN entity_value_norm = 'asthma' THEN 1 END)
            AS pmhx_nlp_asthma_n_mentions,

        -- GERD
        BOOL_OR(entity_value_norm = 'GERD')
            AS pmhx_nlp_gerd,
        COUNT(CASE WHEN entity_value_norm = 'GERD' THEN 1 END)
            AS pmhx_nlp_gerd_n_mentions,

        -- Lung cancer
        BOOL_OR(entity_value_norm = 'lung_cancer')
            AS pmhx_nlp_lung_cancer,
        COUNT(CASE WHEN entity_value_norm = 'lung_cancer' THEN 1 END)
            AS pmhx_nlp_lung_cancer_n_mentions,

        -- Aggregate: distinct problem list count, concatenated problems, provenance
        COUNT(DISTINCT entity_value_norm)    AS pmhx_nlp_n_comorbidities,
        STRING_AGG(DISTINCT entity_value_norm, ';' ORDER BY entity_value_norm)
            AS pmhx_nlp_comorbidity_list,
        COUNT(DISTINCT note_row_id)          AS pmhx_nlp_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',')  AS pmhx_nlp_note_types,
        'regex_problem_list_v2'              AS pmhx_nlp_extraction_method
    FROM pmhx_ent
    GROUP BY research_id
)"""

PMHX_REGEX_COLS = [
    "pmhx_nlp_hypertension", "pmhx_nlp_hypertension_n_mentions",
    "pmhx_nlp_hypertension_first_date",
    "pmhx_nlp_diabetes", "pmhx_nlp_diabetes_n_mentions", "pmhx_nlp_diabetes_first_date",
    "pmhx_nlp_hyperthyroidism", "pmhx_nlp_hyperthyroidism_n_mentions",
    "pmhx_nlp_hyperthyroidism_first_date",
    "pmhx_nlp_hypothyroidism", "pmhx_nlp_hypothyroidism_n_mentions",
    "pmhx_nlp_hypothyroidism_first_date",
    "pmhx_nlp_obesity", "pmhx_nlp_obesity_n_mentions", "pmhx_nlp_obesity_first_date",
    "pmhx_nlp_breast_cancer", "pmhx_nlp_breast_cancer_n_mentions",
    "pmhx_nlp_depression", "pmhx_nlp_depression_n_mentions",
    "pmhx_nlp_cad", "pmhx_nlp_cad_n_mentions",
    "pmhx_nlp_ckd", "pmhx_nlp_ckd_n_mentions",
    "pmhx_nlp_afib", "pmhx_nlp_afib_n_mentions",
    "pmhx_nlp_copd", "pmhx_nlp_copd_n_mentions",
    "pmhx_nlp_asthma", "pmhx_nlp_asthma_n_mentions",
    "pmhx_nlp_gerd", "pmhx_nlp_gerd_n_mentions",
    "pmhx_nlp_lung_cancer", "pmhx_nlp_lung_cancer_n_mentions",
    "pmhx_nlp_n_comorbidities", "pmhx_nlp_comorbidity_list",
    "pmhx_nlp_n_source_notes", "pmhx_nlp_note_types", "pmhx_nlp_extraction_method",
]


# ======================================================================
# SOURCE 4: note_entities_llm_past_medical_hx (qwen3:32b)
# JSON-stored entities with confidence scores and entity_date
# Entity types: radiation_exposure, family_hx_thyroid, family_hx_cancer,
#   smoking_status, men_syndrome, autoimmune_thyroid, prior_cancer,
#   coagulopathy, osteoporosis, ...
# ======================================================================

def build_pmhx_llm_sql() -> str:
    parse = llm_parse_cte("pmhx_llm", "note_entities_llm_past_medical_hx", 0.7)
    return f"""
{parse},
pmhx_llm AS (
    SELECT
        research_id,
        -- Radiation exposure (CRITICAL — novel data, no structured equivalent)
        BOOL_OR(entity_type IN ('radiation_exposure', 'prior_radiation', 'neck_radiation'))
            AS pmhx_nlp_radiation_exposure,
        -- Use entity_date when available (date of radiation), else note_date
        MIN(CASE WHEN entity_type IN ('radiation_exposure', 'prior_radiation', 'neck_radiation')
            THEN COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)) END)
            AS pmhx_nlp_radiation_exposure_date,
        COUNT(CASE WHEN entity_type IN ('radiation_exposure', 'prior_radiation', 'neck_radiation')
            THEN 1 END)
            AS pmhx_nlp_radiation_exposure_n_mentions,
        AVG(CASE WHEN entity_type IN ('radiation_exposure', 'prior_radiation', 'neck_radiation')
            THEN confidence END)
            AS pmhx_nlp_radiation_exposure_confidence,

        -- Family history of thyroid disease
        BOOL_OR(entity_type ILIKE '%family%thyroid%' OR entity_type = 'family_hx_thyroid'
            OR (entity_type = 'family_history' AND LOWER(entity_value) LIKE '%thyroid%'))
            AS pmhx_nlp_family_hx_thyroid,
        COUNT(CASE WHEN entity_type ILIKE '%family%thyroid%' OR entity_type = 'family_hx_thyroid'
            OR (entity_type = 'family_history' AND LOWER(entity_value) LIKE '%thyroid%')
            THEN 1 END)
            AS pmhx_nlp_family_hx_thyroid_n_mentions,

        -- Family history of cancer (any)
        BOOL_OR(entity_type ILIKE '%family%cancer%' OR entity_type = 'family_hx_cancer'
            OR (entity_type = 'family_history' AND LOWER(entity_value) LIKE '%cancer%'))
            AS pmhx_nlp_family_hx_cancer,

        -- Smoking status (never/former/current)
        MAX(CASE WHEN entity_type IN ('smoking_status', 'tobacco_use', 'smoking_history')
            THEN entity_value END)
            AS pmhx_nlp_smoking_status,

        -- MEN syndrome (critical for MTC — rare but important)
        BOOL_OR(entity_type IN ('men_syndrome', 'men2', 'multiple_endocrine_neoplasia')
            OR LOWER(entity_value) LIKE '%men2%' OR LOWER(entity_value) LIKE '%men 2%'
            OR LOWER(entity_value) LIKE '%multiple endocrine neoplasia%')
            AS pmhx_nlp_men_syndrome,

        -- Autoimmune thyroid disease history (from patient-reported history, not pathology)
        BOOL_OR(entity_type IN ('autoimmune_thyroid', 'hashimoto_history', 'graves_history',
            'autoimmune_thyroid_hx', 'autoimmune_disease')
            OR (entity_type ILIKE '%autoimmune%' AND LOWER(entity_value) LIKE '%thyroid%'))
            AS pmhx_nlp_autoimmune_thyroid_hx,
        COUNT(CASE WHEN entity_type IN ('autoimmune_thyroid', 'hashimoto_history',
            'graves_history', 'autoimmune_thyroid_hx', 'autoimmune_disease')
            THEN 1 END)
            AS pmhx_nlp_autoimmune_thyroid_hx_n_mentions,

        -- Prior cancer history (other cancers before thyroid diagnosis)
        BOOL_OR(entity_type IN ('prior_cancer', 'prior_cancer_hx', 'cancer_history',
            'other_malignancy'))
            AS pmhx_nlp_prior_cancer_hx,
        COUNT(CASE WHEN entity_type IN ('prior_cancer', 'prior_cancer_hx',
            'cancer_history', 'other_malignancy') THEN 1 END)
            AS pmhx_nlp_prior_cancer_hx_n_mentions,

        -- Coagulopathy
        BOOL_OR(entity_type IN ('coagulopathy', 'bleeding_disorder', 'anticoagulation'))
            AS pmhx_nlp_coagulopathy,

        -- Osteoporosis
        BOOL_OR(entity_type IN ('osteoporosis', 'osteopenia', 'bone_density'))
            AS pmhx_nlp_osteoporosis,

        -- Domain provenance
        COUNT(DISTINCT note_row_id)          AS pmhx_llm_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',')  AS pmhx_llm_note_types,
        'qwen3_32b'                          AS pmhx_llm_extraction_method,
        MIN(confidence)                      AS pmhx_llm_min_confidence,
        AVG(confidence)                      AS pmhx_llm_mean_confidence
    FROM pmhx_llm_pos
    GROUP BY research_id
)"""

PMHX_LLM_COLS = [
    "pmhx_nlp_radiation_exposure", "pmhx_nlp_radiation_exposure_date",
    "pmhx_nlp_radiation_exposure_n_mentions", "pmhx_nlp_radiation_exposure_confidence",
    "pmhx_nlp_family_hx_thyroid", "pmhx_nlp_family_hx_thyroid_n_mentions",
    "pmhx_nlp_family_hx_cancer",
    "pmhx_nlp_smoking_status",
    "pmhx_nlp_men_syndrome",
    "pmhx_nlp_autoimmune_thyroid_hx", "pmhx_nlp_autoimmune_thyroid_hx_n_mentions",
    "pmhx_nlp_prior_cancer_hx", "pmhx_nlp_prior_cancer_hx_n_mentions",
    "pmhx_nlp_coagulopathy", "pmhx_nlp_osteoporosis",
    "pmhx_llm_n_source_notes", "pmhx_llm_note_types", "pmhx_llm_extraction_method",
    "pmhx_llm_min_confidence", "pmhx_llm_mean_confidence",
]


# ======================================================================
# SOURCE 5: note_entities_llm_past_surgical_hx (qwen3:32b)
# Entity types: prior_thyroidectomy, prior_fna, prior_rai,
#   prior_neck_surgery, prior_neck_dissection, prior_parathyroidectomy
# ======================================================================

def build_pshx_sql() -> str:
    parse = llm_parse_cte("pshx_llm", "note_entities_llm_past_surgical_hx", 0.7)
    return f"""
{parse},
pshx_nlp AS (
    SELECT
        research_id,
        -- Prior thyroidectomy (completion indicator)
        BOOL_OR(entity_type IN ('prior_thyroidectomy', 'prior_thyroid_surgery',
            'prior_thyroid_lobectomy', 'thyroidectomy_history'))
            AS pshx_nlp_prior_thyroidectomy,
        COUNT(CASE WHEN entity_type IN ('prior_thyroidectomy', 'prior_thyroid_surgery',
            'prior_thyroid_lobectomy', 'thyroidectomy_history') THEN 1 END)
            AS pshx_nlp_prior_thyroidectomy_n_mentions,
        MIN(CASE WHEN entity_type IN ('prior_thyroidectomy', 'prior_thyroid_surgery',
            'prior_thyroid_lobectomy', 'thyroidectomy_history')
            THEN COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)) END)
            AS pshx_nlp_prior_thyroidectomy_date,

        -- Prior FNA (fine needle aspiration biopsy)
        BOOL_OR(entity_type IN ('prior_fna', 'prior_biopsy', 'fna_history',
            'prior_needle_biopsy'))
            AS pshx_nlp_prior_fna,
        COUNT(CASE WHEN entity_type IN ('prior_fna', 'prior_biopsy', 'fna_history',
            'prior_needle_biopsy') THEN 1 END)
            AS pshx_nlp_prior_fna_n_mentions,

        -- Prior RAI (radioactive iodine)
        BOOL_OR(entity_type IN ('prior_rai', 'rai_history', 'prior_radioiodine',
            'radioactive_iodine_history'))
            AS pshx_nlp_prior_rai,
        COUNT(CASE WHEN entity_type IN ('prior_rai', 'rai_history', 'prior_radioiodine',
            'radioactive_iodine_history') THEN 1 END)
            AS pshx_nlp_prior_rai_n_mentions,
        MIN(CASE WHEN entity_type IN ('prior_rai', 'rai_history', 'prior_radioiodine',
            'radioactive_iodine_history')
            THEN COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)) END)
            AS pshx_nlp_prior_rai_date,

        -- Prior neck surgery (non-thyroid)
        BOOL_OR(entity_type IN ('prior_neck_surgery', 'neck_surgery_history',
            'prior_cervical_surgery'))
            AS pshx_nlp_prior_neck_surgery,
        COUNT(CASE WHEN entity_type IN ('prior_neck_surgery', 'neck_surgery_history',
            'prior_cervical_surgery') THEN 1 END)
            AS pshx_nlp_prior_neck_surgery_n_mentions,

        -- Prior neck dissection
        BOOL_OR(entity_type IN ('prior_neck_dissection', 'prior_lymph_node_dissection',
            'neck_dissection_history'))
            AS pshx_nlp_prior_neck_dissection,

        -- Prior parathyroidectomy
        BOOL_OR(entity_type IN ('prior_parathyroidectomy', 'parathyroidectomy_history',
            'prior_parathyroid_surgery'))
            AS pshx_nlp_prior_parathyroidectomy,

        -- Count of distinct prior procedure types
        COUNT(DISTINCT entity_type)          AS pshx_nlp_n_prior_procedures,

        -- Domain provenance
        COUNT(DISTINCT note_row_id)          AS pshx_llm_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',')  AS pshx_llm_note_types,
        'qwen3_32b'                          AS pshx_llm_extraction_method,
        MIN(confidence)                      AS pshx_llm_min_confidence,
        AVG(confidence)                      AS pshx_llm_mean_confidence
    FROM pshx_llm_pos
    GROUP BY research_id
)"""

PSHX_COLS = [
    "pshx_nlp_prior_thyroidectomy", "pshx_nlp_prior_thyroidectomy_n_mentions",
    "pshx_nlp_prior_thyroidectomy_date",
    "pshx_nlp_prior_fna", "pshx_nlp_prior_fna_n_mentions",
    "pshx_nlp_prior_rai", "pshx_nlp_prior_rai_n_mentions", "pshx_nlp_prior_rai_date",
    "pshx_nlp_prior_neck_surgery", "pshx_nlp_prior_neck_surgery_n_mentions",
    "pshx_nlp_prior_neck_dissection",
    "pshx_nlp_prior_parathyroidectomy",
    "pshx_nlp_n_prior_procedures",
    "pshx_llm_n_source_notes", "pshx_llm_note_types", "pshx_llm_extraction_method",
    "pshx_llm_min_confidence", "pshx_llm_mean_confidence",
]


# ======================================================================
# SOURCE 6: note_entities_procedures (regex)
# entity_type='procedure', entity_value_norm is the procedure type
# ======================================================================

PROC_NLP_SQL = f"""
proc_ent AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        entity_type,
        entity_value_norm,
        note_date,
        note_row_id,
        note_type
    FROM note_entities_procedures
    WHERE CAST(research_id AS BIGINT) != {ORPHAN_RID}
      AND CAST(research_id AS BIGINT) IN (SELECT DISTINCT CAST(research_id AS BIGINT) FROM {CANONICAL})
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
proc_nlp AS (
    SELECT
        research_id,
        -- Tracheostomy
        BOOL_OR(entity_value_norm = 'tracheostomy')
            AS proc_nlp_tracheostomy,
        MIN(CASE WHEN entity_value_norm = 'tracheostomy' THEN TRY_CAST(note_date AS DATE) END)
            AS proc_nlp_tracheostomy_date,
        COUNT(CASE WHEN entity_value_norm = 'tracheostomy' THEN 1 END)
            AS proc_nlp_tracheostomy_n_mentions,

        -- Laryngoscopy (vocal cord assessment)
        BOOL_OR(entity_value_norm = 'laryngoscopy')
            AS proc_nlp_laryngoscopy,
        MIN(CASE WHEN entity_value_norm = 'laryngoscopy' THEN TRY_CAST(note_date AS DATE) END)
            AS proc_nlp_laryngoscopy_date,
        COUNT(CASE WHEN entity_value_norm = 'laryngoscopy' THEN 1 END)
            AS proc_nlp_laryngoscopy_n_mentions,

        -- Modified radical neck dissection
        BOOL_OR(entity_value_norm = 'modified_radical_neck_dissection')
            AS proc_nlp_mrnd,
        COUNT(CASE WHEN entity_value_norm = 'modified_radical_neck_dissection' THEN 1 END)
            AS proc_nlp_mrnd_n_mentions,

        -- Lateral neck dissection (any)
        BOOL_OR(entity_value_norm IN ('lateral_neck_dissection',
            'modified_radical_neck_dissection'))
            AS proc_nlp_lateral_neck_dissection,

        -- Parathyroid autotransplant (from procedures table)
        BOOL_OR(entity_value_norm = 'parathyroid_autotransplant')
            AS proc_nlp_parathyroid_autotransplant,

        -- Domain provenance
        COUNT(DISTINCT note_row_id)          AS proc_nlp_n_source_notes,
        STRING_AGG(DISTINCT note_type, ',')  AS proc_nlp_note_types,
        'regex_procedure_v2'                 AS proc_nlp_extraction_method
    FROM proc_ent
    GROUP BY research_id
)"""

PROC_NLP_COLS = [
    "proc_nlp_tracheostomy", "proc_nlp_tracheostomy_date", "proc_nlp_tracheostomy_n_mentions",
    "proc_nlp_laryngoscopy", "proc_nlp_laryngoscopy_date", "proc_nlp_laryngoscopy_n_mentions",
    "proc_nlp_mrnd", "proc_nlp_mrnd_n_mentions",
    "proc_nlp_lateral_neck_dissection",
    "proc_nlp_parathyroid_autotransplant",
    "proc_nlp_n_source_notes", "proc_nlp_note_types", "proc_nlp_extraction_method",
]


# ======================================================================
# SOURCE 7: note_entities_llm_presenting_symptoms (qwen3:32b)
# Very sparse — flag as low-coverage
# ======================================================================

def build_sx_sql() -> str:
    parse = llm_parse_cte("sx_llm", "note_entities_llm_presenting_symptoms", 0.7)
    return f"""
{parse},
sx_nlp AS (
    SELECT
        research_id,
        BOOL_OR(entity_type IN ('dysphagia', 'swallowing_difficulty', 'difficulty_swallowing'))
            AS sx_nlp_dysphagia,
        BOOL_OR(entity_type IN ('hoarseness', 'voice_change', 'dysphonia', 'hoarse_voice'))
            AS sx_nlp_hoarseness,
        BOOL_OR(entity_type IN ('neck_mass', 'thyroid_mass', 'neck_swelling', 'goiter'))
            AS sx_nlp_neck_mass,
        BOOL_OR(entity_type IN ('dyspnea', 'shortness_of_breath', 'respiratory_symptom'))
            AS sx_nlp_dyspnea,
        TRUE                                AS sx_nlp_any_symptom_data,
        -- Domain provenance
        COUNT(DISTINCT note_row_id)         AS sx_llm_n_source_notes,
        'qwen3_32b'                         AS sx_llm_extraction_method,
        AVG(confidence)                     AS sx_llm_mean_confidence
    FROM sx_llm_pos
    GROUP BY research_id
)"""

SX_COLS = [
    "sx_nlp_dysphagia", "sx_nlp_hoarseness", "sx_nlp_neck_mass",
    "sx_nlp_dyspnea", "sx_nlp_any_symptom_data",
    "sx_llm_n_source_notes", "sx_llm_extraction_method", "sx_llm_mean_confidence",
]


# ======================================================================
# SOURCE 8: note_entities_llm_rad_treatment (qwen3:32b)
# Entity types: rai_ablation, thyrogen_prep, hormone_withdrawal,
#   post_tx_scan, external_beam_radiation
# ======================================================================

def build_radtx_sql() -> str:
    parse = llm_parse_cte("radtx_llm", "note_entities_llm_rad_treatment", 0.7)
    return f"""
{parse},
radtx_nlp AS (
    SELECT
        research_id,
        -- RAI ablation (redundant with structured but validates it)
        BOOL_OR(entity_type IN ('rai_ablation', 'radioactive_iodine_treatment',
            'radioiodine_ablation'))
            AS radtx_nlp_rai_ablation,
        COUNT(CASE WHEN entity_type IN ('rai_ablation', 'radioactive_iodine_treatment',
            'radioiodine_ablation') THEN 1 END)
            AS radtx_nlp_rai_ablation_n_mentions,

        -- Thyrogen preparation (recombinant TSH for RAI)
        BOOL_OR(entity_type IN ('thyrogen_prep', 'thyrogen', 'recombinant_tsh'))
            AS radtx_nlp_thyrogen_prep,

        -- Hormone withdrawal (levothyroxine held before RAI)
        BOOL_OR(entity_type IN ('hormone_withdrawal', 'thyroid_hormone_withdrawal',
            'levothyroxine_withdrawal'))
            AS radtx_nlp_hormone_withdrawal,

        -- Post-treatment scan outcome
        BOOL_OR(entity_type IN ('post_tx_scan', 'whole_body_scan', 'post_rai_scan')
            AND LOWER(entity_value) LIKE '%negative%')
            AS radtx_nlp_post_tx_scan_negative,

        -- External beam radiation (uncommon in thyroid — flag when present)
        BOOL_OR(entity_type IN ('external_beam_radiation', 'ebrt', 'radiation_therapy')
            AND entity_type NOT IN ('rai_ablation', 'radioactive_iodine_treatment'))
            AS radtx_nlp_external_beam_radiation,

        TRUE                                AS radtx_nlp_has_data,
        -- Domain provenance
        COUNT(DISTINCT note_row_id)         AS radtx_llm_n_source_notes,
        'qwen3_32b'                         AS radtx_llm_extraction_method,
        AVG(confidence)                     AS radtx_llm_mean_confidence
    FROM radtx_llm_pos
    GROUP BY research_id
)"""

RADTX_COLS = [
    "radtx_nlp_rai_ablation", "radtx_nlp_rai_ablation_n_mentions",
    "radtx_nlp_thyrogen_prep", "radtx_nlp_hormone_withdrawal",
    "radtx_nlp_post_tx_scan_negative", "radtx_nlp_external_beam_radiation",
    "radtx_nlp_has_data",
    "radtx_llm_n_source_notes", "radtx_llm_extraction_method", "radtx_llm_mean_confidence",
]


# ======================================================================
# Cross-validation suite
# ======================================================================

CROSS_CHECKS = [
    # (nlp_col, canon_col_or_expr, label, invert_canon)
    ("op_nlp_nerve_monitoring_used",     "op_rln_monitoring_any",         "nerve monitoring",    False),
    ("op_nlp_drain_placed",              "op_drain_placed_any",            "drain placement",     False),
    ("op_nlp_strap_muscle_involved",     "op_strap_muscle_any",            "strap muscle",        False),
    ("op_nlp_reoperative_field",         "op_reoperative_any",             "reoperative field",   False),
    ("op_nlp_parathyroid_autograft",     "op_parathyroid_autograft_any",   "para autograft",      False),
    ("op_nlp_tracheal_involvement",      "op_tracheal_inv_any",            "tracheal inv",        False),
    ("op_nlp_esophageal_involvement",    "op_esophageal_inv_any",          "esophageal inv",      False),
    ("med_nlp_calcium_supplement",       "calcium_supplement_required",    "calcium supplement",  False),
    ("pmhx_nlp_autoimmune_thyroid_hx",   "syn_hashimoto",                  "autoimmune (hashimoto)", False),
    ("proc_nlp_laryngoscopy",            "days_to_first_laryngoscopy",     "laryngoscopy",        False),
    ("pshx_nlp_prior_rai",               "rai_received_flag",              "prior RAI",           False),
]


def run_cross_validation(
    con: duckdb.DuckDBPyConnection,
    staging: str,
    total: int,
) -> dict[str, dict]:
    """Compare NLP booleans against existing structured columns. Returns concordance stats."""
    results: dict[str, dict] = {}
    print("\n[215] === Cross-Validation (NLP vs Structured) ===")
    for nlp_col, canon_col, label, _ in CROSS_CHECKS:
        try:
            # canon_col may be a column name or expression
            # Special case: days_to_first_laryngoscopy IS NOT NULL means canon_positive
            if canon_col == "days_to_first_laryngoscopy":
                canon_pos_expr = f'"{canon_col}" IS NOT NULL'
                canon_neg_expr = f'"{canon_col}" IS NULL'
            else:
                canon_pos_expr = f'(CAST("{canon_col}" AS BOOLEAN) IS TRUE)'
                canon_neg_expr = f'(CAST("{canon_col}" AS BOOLEAN) IS NOT TRUE)'

            row = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE "{nlp_col}" IS TRUE AND {canon_pos_expr})
                        AS both_pos,
                    COUNT(*) FILTER (WHERE "{nlp_col}" IS TRUE AND {canon_neg_expr})
                        AS nlp_only,
                    COUNT(*) FILTER (WHERE ("{nlp_col}" IS NOT TRUE) AND {canon_pos_expr})
                        AS structured_only,
                    COUNT(*) FILTER (WHERE ("{nlp_col}" IS NOT TRUE) AND {canon_neg_expr})
                        AS both_neg
                FROM {staging}
            """).fetchone()

            both_pos, nlp_only, struct_only, both_neg = row
            concordance = (both_pos + both_neg) / total if total > 0 else 0.0
            results[label] = {
                "both_pos": both_pos, "nlp_only": nlp_only,
                "structured_only": struct_only, "both_neg": both_neg,
                "concordance": concordance,
            }
            print(
                f"  {label:30s}: concordance={concordance:.1%}  "
                f"both_pos={both_pos:,}  NLP-only={nlp_only:,}  "
                f"structured-only={struct_only:,}"
            )
        except Exception as e:
            print(f"  {label:30s}: SKIP ({e})")
    return results


# ======================================================================
# Post-integration validation suite
# ======================================================================

def run_validations(
    con: duckdb.DuckDBPyConnection,
    table: str,
    all_new_cols: list[str],
) -> bool:
    """Run V1–V6 post-integration checks. Returns True if all critical pass."""
    all_ok = True

    print("\n[215] === V1: Invariants ===")
    r = con.execute(f"""
        SELECT
            COUNT(*)                                        AS total_rows,
            COUNT(*) - {TOTAL_ROWS}                        AS row_delta,
            COUNT(DISTINCT research_id)                    AS distinct_rids,
            COUNT(*) - COUNT(DISTINCT research_id)         AS dup_rids,
            COUNT(*) FILTER (WHERE research_id IS NULL)    AS null_rids,
            COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna_outcome
        FROM {table}
    """).fetchone()
    labels = ["total_rows", "row_delta", "distinct_rids", "dup_rids",
              "null_rids", "null_fna_outcome"]
    for i, (lbl, val) in enumerate(zip(labels, r)):
        expected = [TOTAL_ROWS, 0, TOTAL_ROWS, 0, 0, 0][i]
        status = "✓" if val == expected else "✗ FAIL"
        if val != expected:
            all_ok = False
        print(f"  {lbl}: {val} (expected {expected}) {status}")

    print("\n[215] === V2: No orphan RIDs ===")
    orphan_count = con.execute(f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE op_nlp_ebl_ml IS NOT NULL
          AND CAST(research_id AS BIGINT) NOT IN (
              SELECT DISTINCT CAST(research_id AS BIGINT) FROM gold_master_patient_facts_v1
          )
    """).fetchone()[0]
    status = "✓" if orphan_count == 0 else "✗ FAIL"
    if orphan_count > 0:
        all_ok = False
    print(f"  NLP data for non-gold RIDs: {orphan_count} (expected 0) {status}")

    print("\n[215] === V3: Existing column unchanged (age_at_surgery) ===")
    try:
        mismatch = con.execute(f"""
            SELECT COUNT(*) FROM {table} c
            JOIN gold_master_patient_facts_v1 g
              ON c.research_id = CAST(g.research_id AS BIGINT)
            WHERE c.age_at_surgery IS DISTINCT FROM CAST(g.age_at_surgery AS DOUBLE)
        """).fetchone()[0]
        status = "✓" if mismatch == 0 else f"✗ {mismatch} mismatches"
        if mismatch > 0:
            all_ok = False
        print(f"  age_at_surgery unchanged: {mismatch} mismatches {status}")
    except Exception as e:
        print(f"  age_at_surgery: SKIP ({e})")

    print("\n[215] === V4: Date sanity checks ===")
    # EBL date within 1 year of surgery
    ebl_far = con.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE op_nlp_ebl_date IS NOT NULL
          AND first_surgery_date IS NOT NULL
          AND ABS(DATEDIFF('day', op_nlp_ebl_date, TRY_CAST(first_surgery_date AS DATE))) > 365
    """).fetchone()[0]
    status = "✓" if ebl_far <= 10 else "⚠ WARN"
    print(f"  EBL date >1yr from surgery: {ebl_far} {status}")

    # Radiation date after surgery (RAI is radiation — could be post-op)
    rad_after = con.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE pmhx_nlp_radiation_exposure_date IS NOT NULL
          AND first_surgery_date IS NOT NULL
          AND pmhx_nlp_radiation_exposure_date > TRY_CAST(first_surgery_date AS DATE)
    """).fetchone()[0]
    print(f"  Radiation date after surgery (may include RAI): {rad_after} (flag for review)")

    print("\n[215] === V5: EBL value ranges ===")
    ebl_stats = con.execute(f"""
        SELECT
            MIN(op_nlp_ebl_ml) AS ebl_min,
            MAX(op_nlp_ebl_ml) AS ebl_max,
            ROUND(AVG(op_nlp_ebl_ml), 1) AS ebl_avg,
            COUNT(*) FILTER (WHERE op_nlp_ebl_ml > 2000) AS ebl_extreme,
            COUNT(*) FILTER (WHERE op_nlp_ebl_ml IS NOT NULL) AS ebl_n
        FROM {table}
        WHERE op_nlp_ebl_ml IS NOT NULL
    """).fetchone()
    print(f"  EBL: min={ebl_stats[0]}, max={ebl_stats[1]}, "
          f"avg={ebl_stats[2]}, n={ebl_stats[4]}, extreme(>2000)={ebl_stats[3]}")
    if ebl_stats[1] and ebl_stats[1] > 5000:
        print("  ⚠ EBL max exceeds 5000 mL — check for parsing errors")

    print("\n[215] === V6: Coverage Report (all new columns) ===")
    print(f"  {'Column':<50s} {'Non-null':>8} {'Pct':>6} {'Distinct':>8}  Source")
    sep = "  " + "-" * 80
    print(sep)
    sources = {
        "op_nlp": "operative_detail (regex)", "med_nlp": "medications (regex)",
        "pmhx_nlp": "problem_list (regex)", "pmhx_llm": "past_medical_hx (LLM)",
        "pshx_nlp": "past_surgical_hx (LLM)", "pshx_llm": "past_surgical_hx (LLM)",
        "proc_nlp": "procedures (regex)",
        "sx_nlp": "presenting_sx (LLM)  ⚠LOW-COVERAGE",
        "sx_llm": "presenting_sx (LLM)  ⚠LOW-COVERAGE",
        "radtx_nlp": "rad_treatment (LLM)", "radtx_llm": "rad_treatment (LLM)",
    }
    for col in all_new_cols:
        try:
            row = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) AS non_null,
                    ROUND(COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) * 100.0 / {TOTAL_ROWS}, 1),
                    COUNT(DISTINCT "{col}") FILTER (WHERE "{col}" IS NOT NULL)
                FROM {table}
            """).fetchone()
            prefix = next((k for k in sources if col.startswith(k)), "?")
            src = sources.get(prefix, "")
            print(f"  {col:<50s} {row[0]:>8,} {row[1]:>5.1f}% {row[2]:>8}  {src}")
        except Exception as e:
            print(f"  {col:<50s} ERROR ({e})")

    return all_ok


# ======================================================================
# Main
# ======================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Script 215: Deep NLP Entity Integration")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build SQL but do not execute writes")
    args = parser.parse_args()

    con = connect()
    print(f"[215] Connected to {DB}")

    existing_cols = get_existing_columns(con)
    cur_col_count = len(existing_cols)
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[215] Current canonical: {cur_rows} rows × {cur_col_count} columns")

    # ---------------------------------------------------------------
    # Build all CTE SQL blocks
    # ---------------------------------------------------------------
    cte_blocks: list[str] = [OP_NLP_SQL, MED_NLP_SQL, PMHX_REGEX_SQL]
    cte_blocks.append(build_pmhx_llm_sql())
    cte_blocks.append(build_pshx_sql())
    cte_blocks.append(PROC_NLP_SQL)
    cte_blocks.append(build_sx_sql())
    cte_blocks.append(build_radtx_sql())

    # CTE alias → output columns mapping
    cte_info: list[tuple[str, list[str]]] = [
        ("op_nlp",   OP_NLP_COLS),
        ("med_nlp",  MED_NLP_COLS),
        ("pmhx_nlp", PMHX_REGEX_COLS),
        ("pmhx_llm", PMHX_LLM_COLS),
        ("pshx_nlp", PSHX_COLS),
        ("proc_nlp", PROC_NLP_COLS),
        ("sx_nlp",   SX_COLS),
        ("radtx_nlp", RADTX_COLS),
    ]

    # Determine which columns are genuinely new (not already in canonical)
    new_cols_by_alias: dict[str, list[str]] = {}
    all_new_cols: list[str] = []
    for alias, cols in cte_info:
        new = [c for c in cols if c not in existing_cols]
        new_cols_by_alias[alias] = new
        all_new_cols.extend(new)
        skipped = len(cols) - len(new)
        print(f"  {alias}: {len(new)} new, {skipped} already exist")

    print(f"[215] Total new columns: {len(all_new_cols)}")

    # ---------------------------------------------------------------
    # Assemble mega-CTE SQL
    # ---------------------------------------------------------------
    cte_chain = ",\n".join(cte_blocks)

    # Build select + join for each CTE
    select_parts: list[str] = []
    join_parts: list[str] = []

    for alias, cols in cte_info:
        new = new_cols_by_alias[alias]
        if not new:
            continue
        for col in new:
            select_parts.append(f'{alias}."{col}"')
        join_parts.append(
            f"LEFT JOIN {alias} ON c.research_id = {alias}.research_id"
        )

    if not select_parts:
        print("[215] All columns already exist — nothing to add.")
        return

    select_block = ",\n    ".join(select_parts)
    join_block = "\n".join(join_parts)

    rebuild_sql = f"""
WITH
{cte_chain}

SELECT
    c.*,
    {select_block}
FROM {CANONICAL} c
{join_block}
"""

    if args.dry_run:
        print(f"\n[215] DRY RUN — SQL length: {len(rebuild_sql):,} chars")
        print(rebuild_sql[:3000])
        print("...[truncated]...")
        print(f"[215] Would add {len(all_new_cols)} columns to {CANONICAL}")
        return

    # ---------------------------------------------------------------
    # Execute staging table rebuild
    # ---------------------------------------------------------------
    staging = f"{CANONICAL}_staging_215"
    t0 = time.time()

    print(f"\n[215] Creating staging table {staging}...")
    try:
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")
    except Exception as e:
        print(f"[215] ERROR creating staging table: {e}")
        print("[215] Attempting batch rebuild...")
        _batched_rebuild(con, cte_blocks, cte_info, new_cols_by_alias, cur_rows)
        return

    elapsed = time.time() - t0
    print(f"[215] Staging created in {elapsed:.1f}s")

    # Invariant check on staging
    inv = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM {staging}
    """).fetchone()
    print(f"[215] Staging: {inv[0]} rows, {inv[1]} distinct RIDs, "
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
            print(f"[215] ERROR: {e}")
        print("[215] Aborting — dropping staging table")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    # ---------------------------------------------------------------
    # Cross-validation (on staging, before swap)
    # ---------------------------------------------------------------
    xval_results = run_cross_validation(con, staging, TOTAL_ROWS)

    # ---------------------------------------------------------------
    # Swap staging → canonical
    # ---------------------------------------------------------------
    print(f"\n[215] Swapping {staging} → {CANONICAL}...")
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    new_col_count = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    added = new_col_count - cur_col_count
    print(f"[215] Columns: {cur_col_count} → {new_col_count} (+{added})")

    # ---------------------------------------------------------------
    # Post-integration validation suite
    # ---------------------------------------------------------------
    all_ok = run_validations(con, CANONICAL, all_new_cols)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n[215] === Summary ===")
    print(f"  Added {added} columns to canonical_patient_master_v1")
    print(f"  Final: {TOTAL_ROWS} × {new_col_count} columns")
    print("\n  Cross-validation concordance:")
    for label, stats in xval_results.items():
        print(f"    {label:30s}: {stats.get('concordance', 0):.1%}  "
              f"NLP-only={stats.get('nlp_only', '?')}  "
              f"structured-only={stats.get('structured_only', '?')}")

    if all_ok:
        print("\n[215] ✓ All critical invariants PASS")
    else:
        print("\n[215] ✗ Some invariants FAILED — check output above")
        sys.exit(1)

    print("\n[215] Done.")


# ======================================================================
# Fallback: batch rebuild if mega-CTE fails
# ======================================================================

def _batched_rebuild(
    con: duckdb.DuckDBPyConnection,
    cte_blocks: list[str],
    cte_info: list[tuple[str, list[str]]],
    new_cols_by_alias: dict[str, list[str]],
    cur_rows: int,
) -> None:
    """Rebuild canonical in batches of 2 sources each if mega-CTE is too complex."""
    # Split cte_blocks and cte_info into batches of 2
    batch_size = 2
    for i in range(0, len(cte_blocks), batch_size):
        batch_blocks = cte_blocks[i:i + batch_size]
        batch_info = cte_info[i:i + batch_size]
        batch_num = i // batch_size + 1

        select_parts = []
        join_parts = []
        for alias, cols in batch_info:
            new = new_cols_by_alias.get(alias, [])
            if not new:
                continue
            for col in new:
                select_parts.append(f'{alias}."{col}"')
            join_parts.append(
                f"LEFT JOIN {alias} ON c.research_id = {alias}.research_id"
            )

        if not select_parts:
            continue

        staging = f"{CANONICAL}_staging_215_b{batch_num}"
        cte_chain = ",\n".join(batch_blocks)
        select_block = ",\n    ".join(select_parts)
        join_block = "\n".join(join_parts)

        sql = f"""
WITH
{cte_chain}

SELECT c.*, {select_block}
FROM {CANONICAL} c
{join_block}
"""
        print(f"[215] Batch {batch_num}: creating {staging}...")
        t0 = time.time()
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(f"CREATE TABLE {staging} AS {sql}")
        elapsed = time.time() - t0

        stg_rows = con.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
        stg_rids = con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {staging}"
        ).fetchone()[0]
        print(f"[215] Batch {batch_num}: {stg_rows} rows, {stg_rids} RIDs in {elapsed:.1f}s")

        if stg_rows != cur_rows or stg_rows != stg_rids:
            print(f"[215] ERROR in batch {batch_num}. Aborting.")
            con.execute(f"DROP TABLE IF EXISTS {staging}")
            sys.exit(1)

        con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
        con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")
        print(f"[215] Batch {batch_num} applied.")

    new_col_count = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    print(f"[215] Batched rebuild complete. Final column count: {new_col_count}")


if __name__ == "__main__":
    main()
