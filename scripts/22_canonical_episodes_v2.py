#!/usr/bin/env python3
"""
22_canonical_episodes_v2.py -- Canonical episode-level tables/views (v2)

Creates 9 canonical tables that serve as the manuscript-grade, audit-ready
data layer for downstream analytics, Streamlit dashboards, and MotherDuck
materialization.

Tables created:
  1. tumor_episode_master_v2        -- one row per tumor per surgery
  2. molecular_test_episode_v2      -- one row per molecular testing event
  3. rai_treatment_episode_v2       -- one row per RAI treatment event
  4. imaging_nodule_long_v2         -- one row per nodule per imaging exam
  5. imaging_exam_summary_v2        -- one row per imaging exam
  6. operative_episode_detail_v2    -- one row per surgery episode
  7. fna_episode_master_v2          -- one row per FNA episode
  8. event_date_audit_v2            -- one row per extracted fact (date audit)
  9. patient_cross_domain_timeline_v2 -- union timeline per patient

Run after scripts 15-20.
Supports --md flag for MotherDuck deployment.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"
SQL_OUT = ROOT / "scripts" / "22_canonical_episodes_v2_views.sql"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    tables = [
        "path_synoptics", "tumor_pathology", "operative_details",
        "molecular_testing", "fna_history", "fna_cytology",
        "ultrasound_reports", "us_nodules_tirads", "ct_imaging",
        "mri_imaging", "serial_imaging_us", "nuclear_med",
        "note_entities_staging", "note_entities_genetics",
        "note_entities_medications", "note_entities_procedures",
        "note_entities_complications", "note_entities_problem_list",
        "clinical_notes_long", "complications",
    ]
    for tbl in tables:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
        elif table_available(con, tbl):
            print(f"  Existing  {tbl:<45}")
        else:
            print(f"  SKIP      {tbl:<45}")


# ---------------------------------------------------------------------------
# SQL definitions for each canonical table
# ---------------------------------------------------------------------------

TUMOR_EPISODE_MASTER_V2_SQL = """
CREATE OR REPLACE TABLE tumor_episode_master_v2 AS
WITH ps AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(surg_date AS DATE)) AS surgery_episode_id,
        thyroid_procedure AS procedure_raw,
        -- tumor 1 fields from synoptics (column names reflect actual path_synoptics schema)
        tumor_1_histologic_type AS ps_histology_1,
        tumor_1_variant AS ps_variant_1,
        tumor_1_size_greatest_dimension_cm AS ps_size_cm_1,
        tumor_1_extrathyroidal_extension AS ps_ete_1,
        tumor_1_margin_status AS ps_margins_1,
        tumor_1_angioinvasion AS ps_vasc_inv_1,
        tumor_1_lymphatic_invasion AS ps_lymph_inv_1,
        tumor_1_perineural_invasion AS ps_perineural_1,
        tumor_1_capsular_invasion AS ps_capsular_inv_1,
        -- AJCC staging columns from path_synoptics
        tumor_1_pt AS ps_t_stage,
        tumor_1_pn AS ps_n_stage,
        tumor_1_pm AS ps_m_stage,
        NULL::VARCHAR AS ps_overall_stage,
        -- node counts
        tumor_1_ln_examined AS ps_nodes_total,
        tumor_1_ln_involved AS ps_nodes_positive,
        tumor_1_extranodal_extension AS ps_extranodal_ext,
        -- laterality
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%right%' THEN 'right'
            WHEN LOWER(thyroid_procedure) LIKE '%left%' THEN 'left'
            WHEN LOWER(thyroid_procedure) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(thyroid_procedure) LIKE '%total%' THEN 'bilateral'
            ELSE NULL
        END AS ps_laterality,
        -- multifocality
        tumor_1_multiple_tumor AS ps_tumor_count,
        CASE WHEN LOWER(COALESCE(tumor_1_multiple_tumor,'')) LIKE '%yes%'
                  OR LOWER(COALESCE(tumor_1_multiple_tumor,'')) LIKE '%multi%'
             THEN TRUE ELSE FALSE END AS ps_multifocal,
        -- consult / diagnosis summary
        path_diagnosis_summary AS ps_consult_diagnosis
    FROM path_synoptics
    WHERE research_id IS NOT NULL
),
tp AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        histology_1_type AS tp_histology,
        tumor_1_histology_variant AS tp_variant,
        histology_1_t_stage_ajcc8 AS tp_t_stage,
        histology_1_n_stage_ajcc8 AS tp_n_stage,
        histology_1_m_stage_ajcc8 AS tp_m_stage,
        NULL::VARCHAR AS tp_ete,
        NULL::VARCHAR AS tp_gross_ete,
        histology_1_largest_tumor_cm AS tp_size_cm,
        NULL::DATE AS tp_surgery_date
    FROM tumor_pathology
    WHERE research_id IS NOT NULL
)
SELECT
    ps.research_id,
    ps.surgery_episode_id,
    1 AS tumor_ordinal,
    ps.surgery_date,
    CASE
        WHEN ps.surgery_date IS NOT NULL THEN 'exact_source_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE
        WHEN ps.surgery_date IS NOT NULL THEN 100
        ELSE 0
    END AS date_confidence,
    -- histology with precedence: synoptic > tumor_pathology
    COALESCE(ps.ps_histology_1, tp.tp_histology) AS primary_histology,
    COALESCE(ps.ps_variant_1, tp.tp_variant) AS histology_variant,
    CASE
        WHEN ps.ps_histology_1 IS NOT NULL THEN 'path_synoptics'
        WHEN tp.tp_histology IS NOT NULL THEN 'tumor_pathology'
        ELSE NULL
    END AS histology_source,
    -- staging with precedence
    COALESCE(ps.ps_t_stage, tp.tp_t_stage) AS t_stage,
    COALESCE(ps.ps_n_stage, tp.tp_n_stage) AS n_stage,
    COALESCE(ps.ps_m_stage, tp.tp_m_stage) AS m_stage,
    ps.ps_overall_stage AS overall_stage,
    -- size
    COALESCE(TRY_CAST(ps.ps_size_cm_1 AS DOUBLE), TRY_CAST(tp.tp_size_cm AS DOUBLE)) AS tumor_size_cm,
    -- invasion/margin details
    COALESCE(ps.ps_ete_1, tp.tp_ete) AS extrathyroidal_extension,
    tp.tp_gross_ete AS gross_ete,
    ps.ps_vasc_inv_1 AS vascular_invasion,
    ps.ps_lymph_inv_1 AS lymphatic_invasion,
    ps.ps_perineural_1 AS perineural_invasion,
    ps.ps_capsular_inv_1 AS capsular_invasion,
    ps.ps_margins_1 AS margin_status,
    -- nodal disease
    TRY_CAST(ps.ps_nodes_positive AS INTEGER) AS nodal_disease_positive_count,
    TRY_CAST(ps.ps_nodes_total AS INTEGER) AS nodal_disease_total_count,
    ps.ps_extranodal_ext AS extranodal_extension,
    -- laterality / multifocality
    ps.ps_laterality AS laterality,
    TRY_CAST(ps.ps_tumor_count AS INTEGER) AS number_of_tumors,
    ps.ps_multifocal AS multifocality_flag,
    -- consult precedence
    ps.ps_consult_diagnosis AS consult_diagnosis,
    CASE WHEN ps.ps_consult_diagnosis IS NOT NULL THEN TRUE ELSE FALSE END AS consult_precedence_flag,
    -- discordance flags
    CASE WHEN ps.ps_histology_1 IS NOT NULL AND tp.tp_histology IS NOT NULL
         AND LOWER(TRIM(ps.ps_histology_1)) != LOWER(TRIM(tp.tp_histology))
         THEN TRUE ELSE FALSE END AS histology_discordance_flag,
    CASE WHEN ps.ps_t_stage IS NOT NULL AND tp.tp_t_stage IS NOT NULL
         AND UPPER(TRIM(ps.ps_t_stage)) != UPPER(TRIM(tp.tp_t_stage))
         THEN TRUE ELSE FALSE END AS t_stage_discordance_flag,
    -- confidence rank (1=synoptic, 2=tumor_path, 3=note-derived)
    CASE
        WHEN ps.ps_histology_1 IS NOT NULL THEN 1
        WHEN tp.tp_histology IS NOT NULL THEN 2
        ELSE 3
    END AS confidence_rank,
    -- provenance
    'path_synoptics+tumor_pathology' AS source_tables,
    ps.procedure_raw
FROM ps
LEFT JOIN tp
    ON ps.research_id = tp.research_id
    AND (ps.surgery_date = tp.tp_surgery_date
         OR tp.tp_surgery_date IS NULL
         OR ps.surgery_date IS NULL)
"""

MOLECULAR_TEST_EPISODE_V2_SQL = """
CREATE OR REPLACE TABLE molecular_test_episode_v2 AS
WITH mt AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY COALESCE(
                               TRY_CAST(date AS DATE),
                               DATE '2099-01-01')) AS molecular_episode_id,
        thyroseq_afirma AS platform_raw,
        CASE
            WHEN LOWER(thyroseq_afirma) LIKE '%thyroseq%' THEN 'ThyroSeq'
            WHEN LOWER(thyroseq_afirma) LIKE '%afirma%' THEN 'Afirma'
            ELSE 'Other'
        END AS platform,
        result,
        mutation,
        COALESCE(detailed_findings, '') AS detailed_findings_raw,
        TRY_CAST(date AS DATE) AS test_date_native,
        CASE
            WHEN TRY_CAST(date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(date AS DATE) IS NOT NULL THEN 100
            ELSE 0
        END AS date_confidence,
        -- result classification
        CASE
            WHEN LOWER(result) IN ('positive', 'detected', 'abnormal') THEN 'positive'
            WHEN LOWER(result) IN ('negative', 'not detected', 'normal', 'benign') THEN 'negative'
            WHEN LOWER(result) LIKE '%suspicious%' THEN 'suspicious'
            WHEN LOWER(result) LIKE '%indeterminate%' THEN 'indeterminate'
            WHEN LOWER(result) LIKE '%insufficient%' OR LOWER(result) LIKE '%inadequate%'
                 OR LOWER(result) LIKE '%non-diagnostic%' THEN 'non_diagnostic'
            WHEN LOWER(result) LIKE '%cancel%' THEN 'cancelled'
            ELSE 'other'
        END AS overall_result_class,
        -- mutation flags via regex on mutation+detailed_findings
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%braf%v600%' THEN TRUE
             WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%braf%' THEN TRUE
             ELSE FALSE END AS braf_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%v600e%' THEN 'V600E'
             WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%v600%' THEN 'V600'
             ELSE NULL END AS braf_variant,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(nras|hras|kras|\bras\b)' THEN TRUE ELSE FALSE END AS ras_flag,
        CASE
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%nras%' THEN 'NRAS'
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%hras%' THEN 'HRAS'
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%kras%' THEN 'KRAS'
            ELSE NULL
        END AS ras_subtype,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '\bret\b' THEN TRUE ELSE FALSE END AS ret_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ 'ret[/\\s-]*(ptc|fusion)' THEN TRUE ELSE FALSE END AS ret_fusion_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%tert%' THEN TRUE ELSE FALSE END AS tert_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%ntrk%' THEN TRUE ELSE FALSE END AS ntrk_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%eif1ax%' THEN TRUE ELSE FALSE END AS eif1ax_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%tp53%' THEN TRUE ELSE FALSE END AS tp53_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%pax8%pparg%' THEN TRUE ELSE FALSE END AS pax8_pparg_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(copy.?number|cna|amplif|delet)' THEN TRUE ELSE FALSE END AS cna_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%fusion%' THEN TRUE ELSE FALSE END AS fusion_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(loss.?of.?heterozygos|loh)' THEN TRUE ELSE FALSE END AS loh_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%alk%' THEN TRUE ELSE FALSE END AS alk_flag,
        -- high-risk marker composite
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(braf.*v600|tert|tp53|alk.*fusion|ret.*fusion|ntrk.*fusion)'
             THEN TRUE ELSE FALSE END AS high_risk_marker_flag,
        -- inadequate / cancelled
        CASE WHEN LOWER(COALESCE(result,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(insufficient|inadequate|low.?cellularity|non.?diagnostic|qns)'
             THEN TRUE ELSE FALSE END AS inadequate_flag,
        CASE WHEN LOWER(COALESCE(result,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%cancel%' THEN TRUE ELSE FALSE END AS cancelled_flag,
        'molecular_testing' AS source_table
    FROM molecular_testing
    WHERE research_id IS NOT NULL
)
SELECT
    mt.*,
    TRY_CAST(mt.test_date_native AS VARCHAR) AS resolved_test_date,
    NULL::VARCHAR AS linked_fna_episode_id,
    NULL::VARCHAR AS linked_nodule_id,
    NULL::VARCHAR AS linked_surgery_episode_id,
    NULL::VARCHAR AS specimen_site_raw,
    NULL::VARCHAR AS specimen_site_normalized,
    NULL::INTEGER AS bethesda_category,
    NULL::VARCHAR AS platform_version,
    NULL::VARCHAR AS risk_language_raw,
    NULL::DOUBLE  AS molecular_confidence,
    'pending' AS adjudication_status
FROM mt
"""

RAI_TREATMENT_EPISODE_V2_SQL = """
CREATE OR REPLACE TABLE rai_treatment_episode_v2 AS
WITH rai_notes AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        entity_value_raw,
        entity_value_norm,
        entity_date,
        note_date,
        present_or_negated,
        confidence,
        note_row_id,
        note_type
    FROM note_entities_medications
    WHERE LOWER(entity_value_norm) LIKE 'rai%'
       OR LOWER(entity_value_norm) LIKE 'i-131%'
       OR LOWER(entity_value_norm) LIKE 'i131%'
       OR LOWER(entity_value_norm) LIKE '%radioactive%iodine%'
       OR LOWER(entity_value_raw) LIKE '%radioactive%iodine%'
       OR LOWER(entity_value_raw) LIKE '%rai%'
       OR LOWER(entity_value_raw) LIKE '%i-131%'
       OR LOWER(entity_value_raw) LIKE '%131-i%'
       OR LOWER(entity_value_raw) LIKE '%131i%'
),
rai_parsed AS (
    SELECT
        research_id,
        ROW_NUMBER() OVER (PARTITION BY research_id
                           ORDER BY COALESCE(
                               TRY_CAST(entity_date AS DATE),
                               TRY_CAST(note_date AS DATE),
                               DATE '2099-01-01')) AS rai_episode_id,
        entity_value_raw AS rai_mention_raw,
        entity_value_norm AS rai_term_normalized,
        TRY_CAST(entity_date AS DATE) AS rai_date_native,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        COALESCE(TRY_CAST(entity_date AS DATE),
                 TRY_CAST(note_date AS DATE)) AS resolved_rai_date,
        CASE
            WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'inferred_day_level_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
            WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
            ELSE 0
        END AS date_confidence,
        -- dose extraction from raw text
        TRY_CAST(
            regexp_extract(entity_value_raw, '(\\d+\\.?\\d*)\\s*(?:mCi|GBq|millicuries)', 1)
            AS DOUBLE
        ) AS dose_mci,
        entity_value_raw AS dose_text_raw,
        -- assertion status
        CASE
            WHEN present_or_negated = 'negated' THEN 'negated'
            WHEN LOWER(entity_value_raw) ~ '(recommend|consider|plan|discuss|would benefit)'
                 THEN 'planned'
            WHEN LOWER(entity_value_raw) ~ '(received|treated|administered|given|underwent|completed)'
                 THEN 'definite_received'
            WHEN LOWER(entity_value_raw) ~ '(schedul|upcoming|will receive|to receive)'
                 THEN 'planned'
            WHEN LOWER(entity_value_raw) ~ '(history|previous|prior|past)'
                 THEN 'historical'
            WHEN present_or_negated = 'present' AND
                 TRY_CAST(regexp_extract(entity_value_raw, '(\\d+\\.?\\d*)\\s*(?:mCi|GBq)', 1) AS DOUBLE) IS NOT NULL
                 THEN 'likely_received'
            ELSE 'ambiguous'
        END AS rai_assertion_status,
        -- treatment intent
        CASE
            WHEN LOWER(entity_value_raw) ~ '(remnant.?ablat|ablat)' THEN 'remnant_ablation'
            WHEN LOWER(entity_value_raw) ~ '(adjuvant|additional)' THEN 'adjuvant'
            WHEN LOWER(entity_value_raw) ~ '(metasta|distant|persistent.?disease)' THEN 'metastatic_disease'
            WHEN LOWER(entity_value_raw) ~ '(recur)' THEN 'recurrence'
            ELSE 'unknown'
        END AS rai_intent,
        -- completion status
        CASE
            WHEN present_or_negated = 'negated' THEN 'not_received'
            WHEN LOWER(entity_value_raw) ~ '(received|treated|administered|given|underwent|completed)'
                 THEN 'completed'
            WHEN LOWER(entity_value_raw) ~ '(recommend|consider|plan|discuss|schedul)'
                 THEN 'recommended'
            ELSE 'uncertain'
        END AS completion_status,
        confidence AS rai_confidence,
        note_row_id AS source_note_id,
        note_type AS source_note_type,
        'note_entities_medications' AS source_table,
        'pending' AS adjudication_status
    FROM rai_notes
)
SELECT
    rp.*,
    NULL::VARCHAR AS linked_surgery_episode_id,
    NULL::VARCHAR AS linked_pathology_episode_id,
    NULL::VARCHAR AS linked_recurrence_episode_id,
    FALSE AS pre_scan_flag,
    FALSE AS post_therapy_scan_flag,
    NULL::VARCHAR AS scan_findings_raw,
    FALSE AS iodine_avidity_flag,
    NULL::DOUBLE AS stimulated_tg,
    NULL::DOUBLE AS stimulated_tsh
FROM rai_parsed rp
"""

IMAGING_NODULE_LONG_V2_SQL = """
CREATE OR REPLACE TABLE imaging_nodule_long_v2 AS
WITH us_unpivot AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        'US' AS modality,
        TRY_CAST(us_date AS DATE) AS exam_date_native,
        CASE
            WHEN TRY_CAST(us_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(us_date AS DATE) IS NOT NULL THEN 100
            ELSE 0
        END AS date_confidence,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(us_date AS DATE)) AS imaging_exam_id,
        1 AS nodule_index_within_exam,
        NULL::VARCHAR AS composition,
        NULL::VARCHAR AS echogenicity,
        NULL::VARCHAR AS shape,
        NULL::VARCHAR AS margins,
        NULL::VARCHAR AS calcifications,
        NULL::INTEGER AS tirads_score,
        NULL::VARCHAR AS tirads_category,
        TRY_CAST(dominant_nodule_size_on_us AS DOUBLE) AS size_cm_max,
        NULL::DOUBLE AS size_cm_x,
        NULL::DOUBLE AS size_cm_y,
        NULL::DOUBLE AS size_cm_z,
        dominant_nodule_location AS laterality,
        dominant_nodule_location AS location_detail,
        'serial_imaging_us' AS report_source_table,
        COALESCE(us_findings_impression, us_impression) AS exam_impression_raw,
        FALSE AS suspicious_node_flag,
        NULL::VARCHAR AS suspicious_node_details,
        FALSE AS growth_flag,
        TRUE AS dominant_nodule_flag
    FROM serial_imaging_us
    WHERE research_id IS NOT NULL
      AND us_date IS NOT NULL
),
all_nodules AS (
    SELECT * FROM us_unpivot
)
SELECT
    an.*,
    CAST(an.research_id AS VARCHAR) || '-' || an.modality || '-' ||
        CAST(an.imaging_exam_id AS VARCHAR) || '-' ||
        CAST(an.nodule_index_within_exam AS VARCHAR) AS nodule_id,
    an.exam_date_native AS resolved_exam_date,
    NULL::INTEGER AS nodule_count_in_exam,
    NULL::DOUBLE AS imaging_confidence,
    NULL::VARCHAR AS linked_fna_episode_id,
    NULL::VARCHAR AS linked_molecular_episode_id,
    NULL::VARCHAR AS linked_pathology_tumor_id
FROM all_nodules an
"""

IMAGING_EXAM_SUMMARY_V2_SQL = """
CREATE OR REPLACE TABLE imaging_exam_summary_v2 AS
SELECT
    research_id,
    modality,
    imaging_exam_id,
    exam_date_native,
    resolved_exam_date,
    date_status,
    date_confidence,
    report_source_table,
    COUNT(*) AS nodule_count,
    MAX(size_cm_max) AS max_nodule_size_cm,
    MAX(tirads_score) AS max_tirads_score,
    BOOL_OR(suspicious_node_flag) AS any_suspicious_node,
    BOOL_OR(growth_flag) AS any_growth_noted
FROM imaging_nodule_long_v2
GROUP BY research_id, modality, imaging_exam_id, exam_date_native,
         resolved_exam_date, date_status, date_confidence, report_source_table
"""

OPERATIVE_EPISODE_DETAIL_V2_SQL = """
CREATE OR REPLACE TABLE operative_episode_detail_v2 AS
WITH ops AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date_native,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(surg_date AS DATE)) AS surgery_episode_id,
        CASE
            WHEN TRY_CAST(surg_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        preop_diagnosis_operative_sheet_not_true_preop_dx AS procedure_raw,
        side_of_largest_tumor_or_goiter AS laterality_raw,
        CASE
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%right%' THEN 'right'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%left%' THEN 'left'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%isthmus%' THEN 'isthmus'
            ELSE NULL
        END AS laterality,
        TRY_CAST(ebl AS DOUBLE) AS ebl_ml,
        skin_skin_time_min AS skin_to_skin_time
    FROM operative_details
    WHERE research_id IS NOT NULL
),
ps_proc AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS ps_surgery_date,
        thyroid_procedure AS ps_procedure,
        -- procedure normalization
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%total thyroidectomy%' THEN 'total_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%near-total%' THEN 'total_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%completion%' THEN 'completion_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%lobectomy%' THEN 'hemithyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%hemithyroidectomy%' THEN 'hemithyroidectomy'
            ELSE 'other'
        END AS procedure_normalized,
        -- neck dissection flags from procedure text
        CASE WHEN LOWER(thyroid_procedure) ~ '(central.?neck|level.?vi|cnd)' THEN TRUE ELSE FALSE END AS cnd_from_ps,
        CASE WHEN LOWER(thyroid_procedure) ~ '(lateral.?neck|lnd|mrnd|modified.?radical)' THEN TRUE ELSE FALSE END AS lnd_from_ps
    FROM path_synoptics
    WHERE research_id IS NOT NULL
)
SELECT
    ops.research_id,
    ops.surgery_episode_id,
    ops.surgery_date_native,
    TRY_CAST(ops.surgery_date_native AS VARCHAR) AS resolved_surgery_date,
    ops.date_status,
    ops.procedure_raw,
    COALESCE(ps.procedure_normalized, 'unknown') AS procedure_normalized,
    ops.laterality,
    COALESCE(ps.cnd_from_ps, FALSE) AS central_neck_dissection_flag,
    COALESCE(ps.lnd_from_ps, FALSE) AS lateral_neck_dissection_flag,
    FALSE AS rln_monitoring_flag,
    NULL::VARCHAR AS rln_finding_raw,
    FALSE AS parathyroid_autograft_flag,
    NULL::INTEGER AS parathyroid_autograft_count,
    NULL::VARCHAR AS parathyroid_autograft_site,
    FALSE AS parathyroid_resection_flag,
    FALSE AS gross_ete_flag,
    FALSE AS local_invasion_flag,
    FALSE AS tracheal_involvement_flag,
    FALSE AS esophageal_involvement_flag,
    FALSE AS strap_muscle_involvement_flag,
    FALSE AS reoperative_field_flag,
    ops.ebl_ml,
    FALSE AS drain_flag,
    NULL::VARCHAR AS operative_findings_raw,
    'operative_details+path_synoptics' AS source_tables,
    NULL::DOUBLE AS op_confidence
FROM ops
LEFT JOIN ps_proc ps
    ON ops.research_id = ps.research_id
    AND (ops.surgery_date_native = ps.ps_surgery_date
         OR ps.ps_surgery_date IS NULL
         OR ops.surgery_date_native IS NULL)
"""

FNA_EPISODE_MASTER_V2_SQL = """
CREATE OR REPLACE TABLE fna_episode_master_v2 AS
SELECT
    CAST(research_id AS INTEGER) AS research_id,
    ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                       ORDER BY COALESCE(
                           TRY_CAST(fna_date_parsed AS DATE),
                           TRY_CAST(date AS DATE),
                           DATE '2099-01-01')) AS fna_episode_id,
    TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) AS fna_date_native,
    TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) AS resolved_fna_date,
    CASE
        WHEN TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) IS NOT NULL
             THEN 'exact_source_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE
        WHEN TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) IS NOT NULL THEN 100
        ELSE 0
    END AS date_confidence,
    bethesda AS bethesda_raw,
    TRY_CAST(bethesda AS INTEGER) AS bethesda_category,
    path AS pathology_diagnosis,
    path_extended AS pathology_extended,
    COALESCE(preop_specimen_received_fna_location, specimen) AS specimen_site_raw,
    CASE
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%right%' THEN 'right'
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%left%' THEN 'left'
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%isthmus%' THEN 'isthmus'
        ELSE NULL
    END AS laterality,
    NULL::VARCHAR AS linked_molecular_episode_id,
    NULL::VARCHAR AS linked_imaging_nodule_id,
    NULL::VARCHAR AS linked_surgery_episode_id,
    'fna_history' AS source_table,
    NULL::DOUBLE AS fna_confidence
FROM fna_history
WHERE research_id IS NOT NULL
"""

EVENT_DATE_AUDIT_V2_SQL = """
CREATE OR REPLACE TABLE event_date_audit_v2 AS
SELECT 'tumor' AS domain, research_id,
       CAST(surgery_date AS VARCHAR) AS native_date,
       CAST(surgery_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       histology_source AS anchor_source,
       source_tables AS source_table
FROM tumor_episode_master_v2

UNION ALL

SELECT 'molecular' AS domain, research_id,
       CAST(test_date_native AS VARCHAR) AS native_date,
       resolved_test_date AS resolved_date,
       date_status, date_confidence,
       platform AS anchor_source,
       source_table
FROM molecular_test_episode_v2

UNION ALL

SELECT 'rai' AS domain, research_id,
       CAST(rai_date_native AS VARCHAR) AS native_date,
       CAST(resolved_rai_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       source_note_type AS anchor_source,
       source_table
FROM rai_treatment_episode_v2

UNION ALL

SELECT 'imaging' AS domain, research_id,
       CAST(exam_date_native AS VARCHAR) AS native_date,
       CAST(resolved_exam_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       modality AS anchor_source,
       report_source_table AS source_table
FROM imaging_nodule_long_v2

UNION ALL

SELECT 'operative' AS domain, research_id,
       CAST(surgery_date_native AS VARCHAR) AS native_date,
       resolved_surgery_date AS resolved_date,
       date_status, CASE WHEN date_status = 'exact_source_date' THEN 100 ELSE 0 END AS date_confidence,
       procedure_normalized AS anchor_source,
       source_tables AS source_table
FROM operative_episode_detail_v2

UNION ALL

SELECT 'fna' AS domain, research_id,
       CAST(fna_date_native AS VARCHAR) AS native_date,
       CAST(resolved_fna_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       specimen_site_raw AS anchor_source,
       source_table
FROM fna_episode_master_v2
"""

PATIENT_CROSS_DOMAIN_TIMELINE_V2_SQL = """
CREATE OR REPLACE TABLE patient_cross_domain_timeline_v2 AS
SELECT research_id, 'surgery' AS event_type, 'tumor' AS domain,
       surgery_date AS event_date, surgery_episode_id AS episode_id,
       primary_histology AS event_detail
FROM tumor_episode_master_v2

UNION ALL

SELECT research_id, 'molecular_test' AS event_type, 'molecular' AS domain,
       test_date_native AS event_date, molecular_episode_id AS episode_id,
       platform || ': ' || overall_result_class AS event_detail
FROM molecular_test_episode_v2

UNION ALL

SELECT research_id, 'rai_treatment' AS event_type, 'rai' AS domain,
       resolved_rai_date AS event_date, rai_episode_id AS episode_id,
       rai_assertion_status || ' ' || COALESCE(CAST(dose_mci AS VARCHAR),'') || ' mCi' AS event_detail
FROM rai_treatment_episode_v2

UNION ALL

SELECT research_id, 'imaging' AS event_type, 'imaging' AS domain,
       exam_date_native AS event_date, imaging_exam_id AS episode_id,
       modality || ' nodule' AS event_detail
FROM imaging_nodule_long_v2

UNION ALL

SELECT research_id, 'surgery' AS event_type, 'operative' AS domain,
       surgery_date_native AS event_date, surgery_episode_id AS episode_id,
       procedure_normalized AS event_detail
FROM operative_episode_detail_v2

UNION ALL

SELECT research_id, 'fna' AS event_type, 'fna' AS domain,
       fna_date_native AS event_date, fna_episode_id AS episode_id,
       'Bethesda ' || COALESCE(CAST(bethesda_category AS VARCHAR), '?') AS event_detail
FROM fna_episode_master_v2

ORDER BY research_id, event_date NULLS LAST, event_type
"""


ALL_CANONICAL_SQL = [
    ("tumor_episode_master_v2", TUMOR_EPISODE_MASTER_V2_SQL),
    ("molecular_test_episode_v2", MOLECULAR_TEST_EPISODE_V2_SQL),
    ("rai_treatment_episode_v2", RAI_TREATMENT_EPISODE_V2_SQL),
    ("imaging_nodule_long_v2", IMAGING_NODULE_LONG_V2_SQL),
    ("imaging_exam_summary_v2", IMAGING_EXAM_SUMMARY_V2_SQL),
    ("operative_episode_detail_v2", OPERATIVE_EPISODE_DETAIL_V2_SQL),
    ("fna_episode_master_v2", FNA_EPISODE_MASTER_V2_SQL),
    ("event_date_audit_v2", EVENT_DATE_AUDIT_V2_SQL),
    ("patient_cross_domain_timeline_v2", PATIENT_CROSS_DOMAIN_TIMELINE_V2_SQL),
]


def build_all(con: duckdb.DuckDBPyConnection) -> None:
    """Execute all canonical table creation SQL."""
    for name, sql in ALL_CANONICAL_SQL:
        section(name)
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<45} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN: {name} skipped — {e}")


def enrich_from_v2_extractors(con: duckdb.DuckDBPyConnection) -> None:
    """Run V2 extractors on clinical notes and enrich canonical tables.

    Fills in fields that are NULL/hardcoded in the structured-source SQL
    (e.g. scan_findings_raw, stimulated_tg, rln_monitoring_flag) with
    values parsed from free-text clinical notes by the V2 extractors.
    """
    section("V2 Extractor Enrichment")

    if not table_available(con, "clinical_notes_long"):
        print("  SKIP: clinical_notes_long not available")
        return

    from notes_extraction.extract_rai_v2 import RAIDetailExtractor
    from notes_extraction.extract_operative_v2 import OperativeDetailExtractor

    notes_df = con.execute(
        "SELECT note_row_id, CAST(research_id AS INTEGER) AS research_id, "
        "note_type, note_text, note_date "
        "FROM clinical_notes_long "
        "WHERE note_text IS NOT NULL AND LENGTH(note_text) > 10"
    ).fetchdf()
    print(f"  Loaded {len(notes_df):,} clinical notes for V2 extraction")

    rai_ext = RAIDetailExtractor()
    op_ext = OperativeDetailExtractor()
    rai_results: list[dict] = []
    op_results: list[dict] = []

    for _, row in notes_df.iterrows():
        rid = row["research_id"]
        nrid = str(row["note_row_id"])
        ntype = str(row["note_type"] or "")
        ntext = str(row["note_text"] or "")
        ndate = str(row["note_date"]) if pd.notna(row["note_date"]) else None

        for em in rai_ext.extract(nrid, rid, ntype, ntext, ndate):
            rai_results.append(em.to_dict())
        for em in op_ext.extract(nrid, rid, ntype, ntext, ndate):
            op_results.append(em.to_dict())

    print(f"  RAI extractor: {len(rai_results):,} entities extracted")
    print(f"  Operative extractor: {len(op_results):,} entities extracted")

    # ── RAI enrichment ──────────────────────────────────────────────
    if rai_results:
        rai_df = pd.DataFrame(rai_results)
        con.register("_rai_v2_raw", rai_df)
        con.execute("""
            CREATE OR REPLACE TABLE _v2_rai_enrichment AS
            SELECT
                CAST(research_id AS INTEGER) AS research_id,
                note_date,
                STRING_AGG(DISTINCT CASE WHEN entity_type = 'rai_scan_finding'
                    AND present_or_negated = 'present'
                    THEN entity_value_norm END, '; ') AS scan_findings_raw,
                BOOL_OR(entity_type = 'rai_avidity'
                    AND present_or_negated = 'present'
                    AND entity_value_norm = 'avid') AS iodine_avidity_flag,
                MAX(CASE WHEN entity_type = 'rai_stimulated_tg'
                    AND present_or_negated = 'present'
                    THEN TRY_CAST(
                        regexp_extract(entity_value_norm, '([0-9.]+)', 1)
                        AS DOUBLE) END) AS stimulated_tg,
                MAX(CASE WHEN entity_type = 'rai_stimulated_tsh'
                    AND present_or_negated = 'present'
                    THEN TRY_CAST(
                        regexp_extract(entity_value_norm, '([0-9.]+)', 1)
                        AS DOUBLE) END) AS stimulated_tsh,
                BOOL_OR(entity_type = 'rai_pre_scan'
                    AND present_or_negated = 'present') AS pre_scan_flag,
                BOOL_OR(entity_type = 'rai_post_scan'
                    AND present_or_negated = 'present') AS post_scan_flag
            FROM _rai_v2_raw
            GROUP BY CAST(research_id AS INTEGER), note_date
        """)
        con.unregister("_rai_v2_raw")

        con.execute("""
            UPDATE rai_treatment_episode_v2 r
            SET scan_findings_raw = e.scan_findings_raw,
                iodine_avidity_flag = COALESCE(e.iodine_avidity_flag, r.iodine_avidity_flag),
                stimulated_tg = COALESCE(e.stimulated_tg, r.stimulated_tg),
                stimulated_tsh = COALESCE(e.stimulated_tsh, r.stimulated_tsh),
                pre_scan_flag = COALESCE(e.pre_scan_flag, r.pre_scan_flag),
                post_therapy_scan_flag = COALESCE(e.post_scan_flag, r.post_therapy_scan_flag)
            FROM (
                SELECT DISTINCT ON (r2.research_id, r2.rai_episode_id)
                    r2.research_id, r2.rai_episode_id, e2.*
                FROM rai_treatment_episode_v2 r2
                CROSS JOIN _v2_rai_enrichment e2
                WHERE r2.research_id = e2.research_id
                  AND (e2.scan_findings_raw IS NOT NULL
                       OR e2.iodine_avidity_flag
                       OR e2.stimulated_tg IS NOT NULL
                       OR e2.stimulated_tsh IS NOT NULL)
                ORDER BY r2.research_id, r2.rai_episode_id,
                         ABS(DATEDIFF('day',
                             COALESCE(r2.resolved_rai_date, DATE '2099-01-01'),
                             COALESCE(TRY_CAST(e2.note_date AS DATE), DATE '2099-01-01')))
            ) e
            WHERE r.research_id = e.research_id
              AND r.rai_episode_id = e.rai_episode_id
        """)
        rai_enriched = con.execute(
            "SELECT COUNT(*) FROM rai_treatment_episode_v2 "
            "WHERE scan_findings_raw IS NOT NULL "
            "   OR iodine_avidity_flag "
            "   OR stimulated_tg IS NOT NULL"
        ).fetchone()[0]
        print(f"  RAI enrichment applied: {rai_enriched:,} episodes now have extractor data")

    # ── Operative enrichment ────────────────────────────────────────
    if op_results:
        op_df = pd.DataFrame(op_results)
        con.register("_op_v2_raw", op_df)
        con.execute("""
            CREATE OR REPLACE TABLE _v2_operative_enrichment AS
            SELECT
                CAST(research_id AS INTEGER) AS research_id,
                note_date,
                BOOL_OR(entity_type = 'nerve_monitoring'
                    AND present_or_negated = 'present') AS rln_monitoring_flag,
                MAX(CASE WHEN entity_type = 'rln_finding'
                    AND present_or_negated = 'present'
                    THEN entity_value_norm END) AS rln_finding_raw,
                BOOL_OR(entity_type = 'parathyroid_autograft'
                    AND present_or_negated = 'present') AS parathyroid_autograft_flag,
                BOOL_OR(entity_type = 'gross_invasion'
                    AND present_or_negated = 'present'
                    AND entity_value_norm IN ('gross_ete', 'ete_present')
                ) AS gross_ete_flag,
                BOOL_OR(entity_type = 'gross_invasion'
                    AND present_or_negated = 'present') AS local_invasion_flag,
                BOOL_OR(entity_type = 'tracheal_involvement'
                    AND present_or_negated = 'present'
                    AND entity_value_norm != 'trachea_intact'
                ) AS tracheal_involvement_flag,
                BOOL_OR(entity_type = 'esophageal_involvement'
                    AND present_or_negated = 'present'
                    AND entity_value_norm != 'esophagus_intact'
                ) AS esophageal_involvement_flag,
                BOOL_OR(entity_type = 'strap_muscle'
                    AND present_or_negated = 'present'
                    AND entity_value_norm IN ('strap_resected', 'strap_invaded')
                ) AS strap_muscle_involvement_flag,
                BOOL_OR(entity_type = 'reoperative_field'
                    AND present_or_negated = 'present') AS reoperative_field_flag,
                BOOL_OR(entity_type = 'drain_placement'
                    AND present_or_negated = 'present'
                    AND entity_value_norm != 'no_drain') AS drain_flag,
                STRING_AGG(DISTINCT CASE WHEN entity_type IN (
                        'gross_invasion', 'rln_finding', 'tracheal_involvement',
                        'esophageal_involvement', 'strap_muscle', 'intraop_complication')
                    AND present_or_negated = 'present'
                    THEN entity_value_norm END, '; ') AS operative_findings_raw
            FROM _op_v2_raw
            GROUP BY CAST(research_id AS INTEGER), note_date
        """)
        con.unregister("_op_v2_raw")

        if not table_available(con, "operative_episode_detail_v2"):
            print("  SKIP: operative_episode_detail_v2 not built — skipping operative enrichment")
            for tbl in ["_v2_rai_enrichment", "_v2_operative_enrichment"]:
                try:
                    con.execute(f"DROP TABLE IF EXISTS {tbl}")
                except Exception:
                    pass
            return
        con.execute("""
            UPDATE operative_episode_detail_v2 o
            SET rln_monitoring_flag = COALESCE(e.rln_monitoring_flag, o.rln_monitoring_flag),
                rln_finding_raw = COALESCE(e.rln_finding_raw, o.rln_finding_raw),
                parathyroid_autograft_flag = COALESCE(e.parathyroid_autograft_flag, o.parathyroid_autograft_flag),
                gross_ete_flag = COALESCE(e.gross_ete_flag, o.gross_ete_flag),
                local_invasion_flag = COALESCE(e.local_invasion_flag, o.local_invasion_flag),
                tracheal_involvement_flag = COALESCE(e.tracheal_involvement_flag, o.tracheal_involvement_flag),
                esophageal_involvement_flag = COALESCE(e.esophageal_involvement_flag, o.esophageal_involvement_flag),
                strap_muscle_involvement_flag = COALESCE(e.strap_muscle_involvement_flag, o.strap_muscle_involvement_flag),
                reoperative_field_flag = COALESCE(e.reoperative_field_flag, o.reoperative_field_flag),
                drain_flag = COALESCE(e.drain_flag, o.drain_flag),
                operative_findings_raw = COALESCE(e.operative_findings_raw, o.operative_findings_raw)
            FROM (
                SELECT DISTINCT ON (o2.research_id, o2.surgery_episode_id)
                    o2.research_id, o2.surgery_episode_id, e2.*
                FROM operative_episode_detail_v2 o2
                CROSS JOIN _v2_operative_enrichment e2
                WHERE o2.research_id = e2.research_id
                  AND (e2.rln_monitoring_flag
                       OR e2.rln_finding_raw IS NOT NULL
                       OR e2.operative_findings_raw IS NOT NULL)
                ORDER BY o2.research_id, o2.surgery_episode_id,
                         ABS(DATEDIFF('day',
                             COALESCE(o2.surgery_date_native, DATE '2099-01-01'),
                             COALESCE(TRY_CAST(e2.note_date AS DATE), DATE '2099-01-01')))
            ) e
            WHERE o.research_id = e.research_id
              AND o.surgery_episode_id = e.surgery_episode_id
        """)
        op_enriched = con.execute(
            "SELECT COUNT(*) FROM operative_episode_detail_v2 "
            "WHERE rln_monitoring_flag "
            "   OR rln_finding_raw IS NOT NULL "
            "   OR operative_findings_raw IS NOT NULL"
        ).fetchone()[0]
        print(f"  Operative enrichment applied: {op_enriched:,} episodes now have extractor data")

    for tbl in ["_v2_rai_enrichment", "_v2_operative_enrichment"]:
        try:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        except Exception:
            pass


def write_sql_file() -> None:
    """Write combined SQL to disk for MotherDuck deployment."""
    parts: list[str] = []
    for name, sql in ALL_CANONICAL_SQL:
        parts.append(f"-- {name}\n{sql.strip()};")
    SQL_OUT.write_text("\n\n".join(parts), encoding="utf-8")
    print(f"\n  SQL written to {SQL_OUT}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    section("22 — Canonical Episode Tables v2")

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            print("  Falling back to local DuckDB")
            con = duckdb.connect(str(DB_PATH))
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Using local DuckDB: {DB_PATH}")

    register_parquets(con)
    build_all(con)
    enrich_from_v2_extractors(con)
    write_sql_file()

    section("Summary — Row counts")
    for name, _ in ALL_CANONICAL_SQL:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            patients = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM {name}"
            ).fetchone()[0]
            print(f"  {name:<45} {cnt:>8,} rows  {patients:>6,} patients")
        except Exception:
            print(f"  {name:<45} (not available)")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
