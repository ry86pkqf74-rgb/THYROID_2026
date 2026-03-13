#!/usr/bin/env python3
"""
48_build_analysis_resolved_layer.py -- Unified analysis-grade resolved cohort

Creates the single authoritative, versioned analysis-grade cohort for
manuscript use.  Merges all upstream extraction, refinement, linkage,
scoring, complication, and lab tables into three clean resolved layers:

  patient_analysis_resolved_v1  -- one row per patient (~12,886 rows)
  episode_analysis_resolved_v1  -- one row per surgery episode
  lesion_analysis_resolved_v1   -- one row per tumor/lesion

Every domain follows a strict column naming convention:
  {domain}_{var}_raw     -- extracted/raw value
  {domain}_{var}_final   -- adjudicated / resolved value
  {domain}_{var}_source  -- source table that provided the final value
  {domain}_{var}_confidence -- numeric 0-100 confidence

Explicit eligibility flags per analysis domain:
  analysis_eligible_flag    -- histology + surgery_date + follow_up > 0
  molecular_eligible_flag   -- >=1 molecular test with valid result
  rai_eligible_flag         -- RAI assertion = definite or likely
  survival_eligible_flag    -- valid time_days > 0 + event defined
  scoring_ajcc8_flag        -- AJCC8 calculable
  scoring_ata_flag          -- ATA risk calculable
  scoring_macis_flag        -- MACIS calculable

Audit columns: resolved_layer_version, resolved_at, source_script

Run after scripts 49-53 (enhanced linkage, multinodule, scoring,
complications, labs). Requires scripts 18-27 upstream for canonical tables.
Supports --md, --local, --dry-run flags.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

sys.path.insert(0, str(ROOT))

TODAY = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def connect_md() -> duckdb.DuckDBPyConnection:
    token = _get_token()
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def col_available(con: duckdb.DuckDBPyConnection, tbl: str, col: str) -> bool:
    """Check if a specific column exists in a table."""
    try:
        con.execute(
            f"SELECT {col} FROM {tbl} LIMIT 1"
        )
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# patient_analysis_resolved_v1
# ─────────────────────────────────────────────────────────────────────────────
PATIENT_RESOLVED_SQL = """
CREATE OR REPLACE TABLE patient_analysis_resolved_v1 AS
WITH

-- ── Patient spine (all patients in master clinical table) ─────────────────
patient_spine AS (
    SELECT DISTINCT research_id
    FROM patient_refined_master_clinical_v12
),

-- ── Demographics ──────────────────────────────────────────────────────────
demo AS (
    SELECT
        research_id,
        age_at_surgery,
        LOWER(COALESCE(sex,''))         AS sex,
        race,
        CASE
            WHEN age_at_surgery IS NOT NULL AND sex IS NOT NULL AND race IS NOT NULL
                 THEN 'demographics_harmonized_v3'
            ELSE 'demographics_harmonized_v3_partial'
        END AS demo_source,
        CASE
            WHEN age_at_surgery IS NOT NULL AND sex IS NOT NULL THEN 90
            WHEN age_at_surgery IS NOT NULL THEN 70
            ELSE 30
        END AS demo_confidence
    FROM demographics_harmonized_v3
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY age_at_surgery DESC NULLS LAST) = 1
),

-- ── Primary pathology (tumor 1 / largest tumor) ───────────────────────────
primary_path AS (
    SELECT
        research_id,
        -- Raw fields from canonical v2 table
        primary_histology                       AS path_histology_raw,
        histology_variant                       AS path_histology_variant_raw,
        t_stage                                 AS path_t_stage_raw,
        n_stage                                 AS path_n_stage_raw,
        m_stage                                 AS path_m_stage_raw,
        overall_stage                           AS path_stage_raw,
        tumor_size_cm                           AS path_tumor_size_cm,
        extrathyroidal_extension                AS path_ete_raw,
        gross_ete                               AS path_gross_ete_flag,
        vascular_invasion                       AS path_vascular_invasion_raw,
        lymphatic_invasion                      AS path_lvi_raw,
        perineural_invasion                     AS path_pni_raw,
        capsular_invasion                       AS path_capsular_invasion_raw,
        margin_status                           AS path_margin_raw,
        nodal_disease_positive_count            AS path_ln_positive_raw,
        nodal_disease_total_count               AS path_ln_examined_raw,
        extranodal_extension                    AS path_ene_raw,
        laterality                              AS path_laterality,
        multifocality_flag                      AS path_multifocal_flag,
        number_of_tumors                        AS path_n_tumors,
        surgery_date                            AS path_surgery_date,
        surgery_episode_id                      AS path_surgery_episode_id,
        histology_source                        AS path_histology_source,
        date_confidence                         AS path_date_confidence
    FROM tumor_episode_master_v2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
            CASE WHEN tumor_ordinal = 1 THEN 0 ELSE 1 END,
            tumor_size_cm DESC NULLS LAST,
            surgery_date ASC NULLS LAST
    ) = 1
),

-- ── Refined staging (Phase 4-10 refinements) ──────────────────────────────
staging_refined AS (
    SELECT
        research_id,
        ete_path_confirmed                      AS path_ete_confirmed,
        COALESCE(ete_grade_v9, ete_grade_v5, ete_grade_v3) AS ete_grade_refined,
        margin_r_classification                 AS margin_r_class,
        closest_margin_mm,
        vasc_grade_final_v13                    AS vascular_who_grade,
        vasc_vessel_count_v13                   AS vascular_vessel_count,
        total_ln_positive_v10                   AS ln_positive_refined,
        lateral_neck_dissected_v10              AS lateral_neck_dissected,
        braf_positive_final                     AS braf_positive_refined,
        ras_positive_final                      AS ras_positive_refined,
        tert_positive_v9                        AS tert_positive_refined,
        ete_grade_v9                            AS ete_grade_v9
    FROM patient_refined_master_clinical_v12
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY research_id) = 1
),

-- ── Molecular testing ─────────────────────────────────────────────────────
molecular AS (
    SELECT
        research_id,
        -- Best platform (ThyroSeq > Afirma > Other)
        FIRST(platform ORDER BY
            CASE platform WHEN 'ThyroSeq' THEN 1 WHEN 'Afirma' THEN 2 ELSE 3 END,
            test_date_native ASC NULLS LAST)
                                                AS mol_platform,
        FIRST(test_date_native ORDER BY
            CASE platform WHEN 'ThyroSeq' THEN 1 WHEN 'Afirma' THEN 2 ELSE 3 END,
            test_date_native ASC NULLS LAST)
                                                AS mol_test_date,
        -- BRAF (from molecular_test_episode_v2)
        BOOL_OR(LOWER(CAST(braf_flag AS VARCHAR)) = 'true') AS mol_braf_flag,
        -- RAS
        BOOL_OR(LOWER(CAST(ras_flag AS VARCHAR)) = 'true'
                OR ras_subtype IS NOT NULL)     AS mol_ras_flag,
        -- TERT
        BOOL_OR(LOWER(CAST(tert_flag AS VARCHAR)) = 'true') AS mol_tert_flag,
        COUNT(DISTINCT molecular_episode_id)    AS mol_n_tests,
        BOOL_OR(braf_variant IS NOT NULL)       AS mol_braf_variant_available,
        FIRST(braf_variant ORDER BY test_date_native ASC NULLS LAST)
                                                AS mol_braf_variant,
        FIRST(ras_subtype ORDER BY test_date_native ASC NULLS LAST)
                                                AS mol_ras_subtype
    FROM molecular_test_episode_v2
    WHERE platform != 'x' OR platform IS NULL  -- exclude stubs
    GROUP BY research_id
),

-- ── FNA / Bethesda ────────────────────────────────────────────────────────
fna AS (
    SELECT
        research_id,
        bethesda_final                          AS fna_bethesda_final,
        source_tables                           AS fna_bethesda_source,
        confidence                              AS fna_bethesda_confidence
    FROM extracted_fna_bethesda_v1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY confidence DESC NULLS LAST,
                 CASE CAST(bethesda_final AS VARCHAR)
                     WHEN 'VI' THEN 1 WHEN '6' THEN 1
                     WHEN 'V' THEN 2  WHEN '5' THEN 2
                     WHEN 'IV' THEN 3 WHEN '4' THEN 3
                     WHEN 'III' THEN 4 WHEN '3' THEN 4
                     WHEN 'II' THEN 5  WHEN '2' THEN 5
                     ELSE 6 END
    ) = 1
),

-- ── Imaging / TIRADS ──────────────────────────────────────────────────────
imaging AS (
    SELECT
        research_id,
        tirads_best_score                       AS imaging_tirads_best,
        tirads_worst_score                      AS imaging_tirads_worst,
        tirads_best_category                    AS imaging_tirads_category,
        tirads_source                           AS imaging_tirads_source,
        tirads_reliability                      AS imaging_tirads_reliability,
        n_nodule_records                        AS imaging_n_nodule_records,
        nodule_size_max_mm / 10.0               AS imaging_nodule_size_cm
    FROM extracted_tirads_validated_v1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY tirads_worst_score DESC NULLS LAST
    ) = 1
),

-- ── Surgery (first/primary) ───────────────────────────────────────────────
surgery AS (
    SELECT
        research_id,
        FIRST(procedure_normalized ORDER BY surgery_date_native ASC NULLS LAST)
                                                AS surg_procedure_type,
        MIN(surgery_date_native)                AS surg_first_date,
        COUNT(DISTINCT surgery_episode_id)      AS surg_n_procedures,
        BOOL_OR(procedure_normalized = 'total_thyroidectomy')
                                                AS surg_total_thyroidectomy,
        BOOL_OR(procedure_normalized = 'hemithyroidectomy')
                                                AS surg_hemithyroidectomy
    FROM operative_episode_detail_v2
    GROUP BY research_id
),

-- ── RAI treatment ─────────────────────────────────────────────────────────
rai AS (
    SELECT
        research_id,
        TRUE                                    AS rai_received_flag,
        MIN(TRY_CAST(resolved_rai_date AS DATE)) AS rai_first_date,
        MAX(dose_mci)                           AS rai_max_dose_mci,
        STRING_AGG(DISTINCT rai_assertion_status, ',')
                                                AS rai_assertion_statuses,
        'rai_treatment_episode_v2'              AS rai_source
    FROM rai_treatment_episode_v2
    WHERE rai_assertion_status IN ('definite_received','likely_received')
    GROUP BY research_id
),

-- ── Scoring (AJCC8, ATA, MACIS, AGES, AMES) ──────────────────────────────
scoring AS (
    SELECT
        research_id,
        ajcc8_t_stage,
        ajcc8_n_stage,
        ajcc8_m_stage,
        ajcc8_stage_group,
        ajcc8_calculable_flag,
        ajcc8_missing_components,
        ata_risk_category,
        ata_calculable_flag,
        ata_response_category,
        ata_response_calculable_flag,
        macis_score,
        macis_risk_group,
        macis_calculable_flag,
        ages_score,
        ages_calculable_flag,
        ames_risk_group,
        ames_calculable_flag,
        ln_ratio,
        ln_burden_band,
        molecular_risk_tier
    FROM thyroid_scoring_systems_v1
),

-- ── Complications ─────────────────────────────────────────────────────────
complications AS (
    SELECT
        research_id,
        hypocalcemia_status,
        hypoparathyroidism_status,
        rln_status,
        hematoma_status,
        seroma_status,
        chyle_leak_status,
        wound_infection_status,
        any_confirmed_complication_flag,
        n_confirmed_complications,
        calcium_supplement_required,
        rln_permanent_flag,
        rln_transient_flag
    FROM complication_patient_summary_v1
),

-- ── Labs (Tg/anti-Tg/TSH summary) ────────────────────────────────────────
labs AS (
    SELECT
        research_id,
        tg_nadir,
        tg_last_value,
        tg_peak,
        tg_n_measurements,
        tg_rising_flag,
        tg_doubling_time_days,
        tg_below_threshold_ever,
        anti_tg_nadir,
        anti_tg_rising_flag,
        tsh_suppressed_ever,
        pth_nadir,
        calcium_nadir,
        postop_low_pth_flag,
        postop_low_calcium_flag,
        lab_completeness_score
    FROM longitudinal_lab_patient_summary_v1
),

-- ── Recurrence ────────────────────────────────────────────────────────────
recurrence AS (
    SELECT
        research_id,
        MAX(recurrence_date)                AS recurrence_date,
        BOOL_OR(structural_recurrence_flag) AS structural_recurrence_flag,
        BOOL_OR(biochemical_recurrence_flag) AS biochemical_recurrence_flag,
        BOOL_OR(structural_recurrence_flag OR biochemical_recurrence_flag)
                                            AS any_recurrence_flag,
        FIRST(recurrence_type ORDER BY source_priority DESC NULLS LAST)
                                            AS recurrence_type_primary,
        FIRST(recurrence_site ORDER BY source_priority DESC NULLS LAST)
                                            AS recurrence_site_primary,
        FIRST(source_table ORDER BY source_priority DESC NULLS LAST)
                                            AS recurrence_source
    FROM recurrence_event_clean_v1
    GROUP BY research_id
),

-- ── Provenance traceability ───────────────────────────────────────────────
provenance AS (
    SELECT
        research_id,
        date_traceability_status,
        CASE date_traceability_status
            WHEN 'entity_date_traced'  THEN 100
            WHEN 'inferred_date_traced' THEN 70
            WHEN 'note_date_only'       THEN 50
            WHEN 'surgery_anchor_only'  THEN 35
            ELSE 0
        END AS provenance_confidence
    FROM lineage_audit_v1
)

-- ── Final patient-level assembly ─────────────────────────────────────────
SELECT
    ps.research_id,

    -- ── Demographics ─────────────────────────────────────────────────────
    d.age_at_surgery,
    d.sex,
    d.race,
    d.demo_source,
    d.demo_confidence,

    -- ── Pathology (raw + final) ───────────────────────────────────────────
    pp.path_histology_raw,
    -- Final histology: prefer Phase 4 normalization, fall back to v2
    COALESCE(
        LOWER(CAST(mcv.histology_normalized AS VARCHAR)),
        pp.path_histology_raw
    )                                           AS histology_final,
    'patient_refined_master_clinical_v12'       AS histology_source,
    pp.path_histology_variant_raw,
    pp.path_t_stage_raw,
    pp.path_n_stage_raw,
    pp.path_m_stage_raw,
    pp.path_stage_raw,
    pp.path_tumor_size_cm,
    pp.path_ete_raw,
    -- Final ETE grade (most refined available)
    COALESCE(sr.ete_grade_v9, sr.ete_grade_refined, pp.path_ete_raw)
                                                AS ete_grade_final,
    CASE
        WHEN sr.ete_grade_v9 IS NOT NULL THEN 'extraction_audit_engine_v7'
        WHEN sr.ete_grade_refined IS NOT NULL THEN 'patient_refined_staging_flags_v3'
        ELSE 'tumor_episode_master_v2'
    END                                         AS ete_grade_source,
    pp.path_gross_ete_flag,
    pp.path_vascular_invasion_raw,
    COALESCE(sr.vascular_who_grade, pp.path_vascular_invasion_raw)
                                                AS vascular_invasion_final,
    sr.vascular_vessel_count,
    pp.path_lvi_raw,
    pp.path_pni_raw,
    pp.path_margin_raw,
    COALESCE(sr.margin_r_class, pp.path_margin_raw)
                                                AS margin_status_final,
    sr.closest_margin_mm,
    pp.path_ln_positive_raw,
    pp.path_ln_examined_raw,
    COALESCE(sr.ln_positive_refined, pp.path_ln_positive_raw)
                                                AS ln_positive_final,
    pp.path_ene_raw,
    pp.path_laterality,
    pp.path_multifocal_flag,
    pp.path_n_tumors,
    pp.path_surgery_date                        AS first_surgery_date,
    COALESCE(sr.lateral_neck_dissected, FALSE)  AS lateral_neck_dissected,

    -- ── Molecular (raw + final + source) ─────────────────────────────────
    m.mol_platform,
    m.mol_test_date,
    m.mol_n_tests,
    -- BRAF final (Phase 11 recovered > Phase 9 structured > mol_test_episode)
    COALESCE(
        CASE WHEN LOWER(CAST(sr.braf_positive_refined AS VARCHAR))='true' THEN TRUE END,
        m.mol_braf_flag, FALSE)                 AS braf_positive_final,
    CASE
        WHEN LOWER(CAST(sr.braf_positive_refined AS VARCHAR))='true'
             THEN 'patient_refined_master_clinical_v12'
        WHEN m.mol_braf_flag THEN 'molecular_test_episode_v2'
        ELSE NULL
    END                                         AS braf_source,
    m.mol_braf_variant                          AS braf_variant_raw,
    -- RAS final
    COALESCE(
        CASE WHEN LOWER(CAST(sr.ras_positive_refined AS VARCHAR))='true' THEN TRUE END,
        m.mol_ras_flag, FALSE)                  AS ras_positive_final,
    m.mol_ras_subtype                           AS ras_subtype_raw,
    -- TERT final
    COALESCE(
        CASE WHEN LOWER(CAST(sr.tert_positive_refined AS VARCHAR))='true' THEN TRUE END,
        m.mol_tert_flag, FALSE)                 AS tert_positive_final,

    -- ── FNA / Bethesda ────────────────────────────────────────────────────
    f.fna_bethesda_final,
    f.fna_bethesda_source,
    f.fna_bethesda_confidence,

    -- ── Imaging / TIRADS ──────────────────────────────────────────────────
    i.imaging_tirads_best,
    i.imaging_tirads_worst,
    i.imaging_tirads_category,
    i.imaging_tirads_source,
    i.imaging_nodule_size_cm,
    i.imaging_n_nodule_records,

    -- ── Surgery ───────────────────────────────────────────────────────────
    s.surg_procedure_type,
    s.surg_first_date,
    s.surg_n_procedures,
    s.surg_total_thyroidectomy,
    s.surg_hemithyroidectomy,

    -- ── RAI ───────────────────────────────────────────────────────────────
    COALESCE(r.rai_received_flag, FALSE)        AS rai_received_flag,
    r.rai_first_date,
    r.rai_max_dose_mci,
    r.rai_assertion_statuses,

    -- ── Scoring ───────────────────────────────────────────────────────────
    sc.ajcc8_t_stage,
    sc.ajcc8_n_stage,
    sc.ajcc8_m_stage,
    sc.ajcc8_stage_group,
    sc.ajcc8_calculable_flag,
    sc.ajcc8_missing_components,
    sc.ata_risk_category,
    sc.ata_calculable_flag,
    sc.ata_response_category,
    sc.ata_response_calculable_flag,
    sc.macis_score,
    sc.macis_risk_group,
    sc.macis_calculable_flag,
    sc.ages_score,
    sc.ames_risk_group,
    sc.ln_ratio,
    sc.ln_burden_band,
    sc.molecular_risk_tier,

    -- ── Complications ─────────────────────────────────────────────────────
    COALESCE(c.hypocalcemia_status, 'unknown')  AS hypocalcemia_status,
    COALESCE(c.hypoparathyroidism_status, 'unknown') AS hypoparathyroidism_status,
    COALESCE(c.rln_status, 'unknown')           AS rln_status,
    COALESCE(c.hematoma_status, 'absent')       AS hematoma_status,
    COALESCE(c.seroma_status, 'absent')         AS seroma_status,
    COALESCE(c.chyle_leak_status, 'absent')     AS chyle_leak_status,
    COALESCE(c.wound_infection_status, 'absent') AS wound_infection_status,
    COALESCE(c.any_confirmed_complication_flag, FALSE) AS any_confirmed_complication,
    COALESCE(c.n_confirmed_complications, 0)    AS n_confirmed_complications,
    COALESCE(c.calcium_supplement_required, FALSE) AS calcium_supplement_required,
    COALESCE(c.rln_permanent_flag, FALSE)       AS rln_permanent_flag,
    COALESCE(c.rln_transient_flag, FALSE)       AS rln_transient_flag,

    -- ── Labs ──────────────────────────────────────────────────────────────
    l.tg_nadir,
    l.tg_last_value,
    l.tg_peak,
    l.tg_n_measurements,
    l.tg_rising_flag,
    l.tg_below_threshold_ever,
    l.anti_tg_nadir,
    l.anti_tg_rising_flag,
    l.tsh_suppressed_ever,
    l.pth_nadir,
    l.calcium_nadir,
    l.postop_low_pth_flag,
    l.postop_low_calcium_flag,
    COALESCE(l.lab_completeness_score, 0)       AS lab_completeness_score,

    -- ── Recurrence ────────────────────────────────────────────────────────
    COALESCE(rec.any_recurrence_flag, FALSE)    AS any_recurrence_flag,
    rec.recurrence_date,
    rec.structural_recurrence_flag,
    rec.biochemical_recurrence_flag,
    rec.recurrence_type_primary,
    rec.recurrence_site_primary,
    rec.recurrence_source,

    -- ── Provenance ────────────────────────────────────────────────────────
    prov.date_traceability_status,
    COALESCE(prov.provenance_confidence, 0)     AS provenance_confidence,

    -- ── Analysis Eligibility Flags ────────────────────────────────────────
    -- Primary eligibility: must have pathology + surgery date
    (pp.path_histology_raw IS NOT NULL
     AND pp.path_surgery_date IS NOT NULL)      AS analysis_eligible_flag,
    -- Molecular eligibility: tested with non-stub result
    (m.mol_n_tests > 0)                         AS molecular_eligible_flag,
    -- RAI eligibility: definite or likely RAI
    (COALESCE(r.rai_received_flag, FALSE))       AS rai_eligible_flag,
    -- Survival eligibility: need surgery date and follow-up
    (pp.path_surgery_date IS NOT NULL)           AS survival_eligible_flag,
    -- Scoring eligibility flags
    COALESCE(sc.ajcc8_calculable_flag, FALSE)   AS scoring_ajcc8_flag,
    COALESCE(sc.ata_calculable_flag, FALSE)     AS scoring_ata_flag,
    COALESCE(sc.macis_calculable_flag, FALSE)   AS scoring_macis_flag,

    -- ── Audit metadata ────────────────────────────────────────────────────
    'v1'                AS resolved_layer_version,
    '48'                AS source_script,
    CURRENT_TIMESTAMP   AS resolved_at

FROM patient_spine ps
LEFT JOIN demo d USING (research_id)
LEFT JOIN primary_path pp USING (research_id)
LEFT JOIN staging_refined sr USING (research_id)
LEFT JOIN (
    SELECT * FROM patient_refined_master_clinical_v12
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY research_id) = 1
) mcv USING (research_id)
LEFT JOIN molecular m USING (research_id)
LEFT JOIN fna f USING (research_id)
LEFT JOIN imaging i USING (research_id)
LEFT JOIN surgery s USING (research_id)
LEFT JOIN rai r USING (research_id)
LEFT JOIN scoring sc USING (research_id)
LEFT JOIN complications c USING (research_id)
LEFT JOIN labs l USING (research_id)
LEFT JOIN recurrence rec USING (research_id)
LEFT JOIN provenance prov USING (research_id)
"""


# ─────────────────────────────────────────────────────────────────────────────
# episode_analysis_resolved_v1 (one row per surgery episode)
# ─────────────────────────────────────────────────────────────────────────────
EPISODE_RESOLVED_SQL = """
CREATE OR REPLACE TABLE episode_analysis_resolved_v1 AS
SELECT
    o.research_id,
    o.surgery_episode_id,
    o.surgery_date_native                   AS surgery_date,
    o.procedure_normalized                  AS procedure_type,
    o.laterality                            AS surg_laterality,
    o.rln_monitoring_flag,
    o.rln_finding_raw,
    o.gross_ete_flag                        AS intraop_gross_ete,
    o.parathyroid_resection_flag,
    o.drain_flag,
    -- Linked pathology (via v2 surgery_pathology_linkage)
    sp.tumor_ordinal                        AS linked_path_tumor_ordinal,
    sp.linkage_confidence                   AS path_link_confidence_v2,
    -- Enhanced linkage v3 score if available
    sp3.linkage_score                       AS path_link_score_v3,
    sp3.linkage_confidence_tier             AS path_link_tier_v3,
    sp3.linkage_reason_summary              AS path_link_reason,
    sp3.analysis_eligible_link_flag         AS path_link_eligible,
    -- Pathology fields from primary tumor
    t.primary_histology                     AS histology,
    t.t_stage,
    t.n_stage,
    t.tumor_size_cm,
    t.extrathyroidal_extension,
    t.gross_ete,
    t.vascular_invasion,
    t.margin_status,
    t.nodal_disease_positive_count          AS ln_positive,
    t.nodal_disease_total_count             AS ln_examined,
    -- Linked preop FNA (via v2 preop_surgery_linkage)
    ps.preop_episode_id                     AS linked_fna_episode_id,
    ps.preop_type                           AS linked_fna_type,
    ps.linkage_confidence                   AS fna_link_confidence_v2,
    ps3.linkage_score                       AS fna_link_score_v3,
    -- Linked RAI (via v2 pathology_rai_linkage)
    pr.rai_episode_id                       AS linked_rai_episode_id,
    pr.linkage_confidence                   AS rai_link_confidence_v2,
    pr3.linkage_score                       AS rai_link_score_v3,
    r.rai_assertion_status,
    r.dose_mci                              AS rai_dose_mci,
    -- Episode eligibility
    (o.surgery_date_native IS NOT NULL
     AND t.primary_histology IS NOT NULL)   AS episode_analysis_eligible_flag,
    -- Audit
    'v1'                                    AS resolved_layer_version,
    CURRENT_TIMESTAMP                       AS resolved_at
FROM operative_episode_detail_v2 o
-- Link to pathology
LEFT JOIN surgery_pathology_linkage_v2 sp
    ON sp.surgery_episode_id = o.surgery_episode_id
    AND sp.research_id = o.research_id
LEFT JOIN surgery_pathology_linkage_v3 sp3
    ON sp3.surgery_episode_id = o.surgery_episode_id
    AND sp3.research_id = o.research_id
    AND sp3.score_rank = 1
LEFT JOIN tumor_episode_master_v2 t
    ON t.surgery_episode_id = COALESCE(sp3.path_surgery_id, sp.tumor_episode_id, o.surgery_episode_id)
    AND t.research_id = o.research_id
    AND t.tumor_ordinal = 1
-- Link to preop
LEFT JOIN preop_surgery_linkage_v2 ps
    ON ps.surgery_episode_id = o.surgery_episode_id
    AND ps.research_id = o.research_id
LEFT JOIN preop_surgery_linkage_v3 ps3
    ON ps3.surgery_episode_id = o.surgery_episode_id
    AND ps3.research_id = o.research_id
    AND ps3.score_rank = 1
-- Link to RAI (prefer v3 path_surgery_id, fall back to v2 tumor_episode_id)
LEFT JOIN pathology_rai_linkage_v2 pr
    ON pr.surgery_episode_id = COALESCE(sp3.path_surgery_id, sp.tumor_episode_id, o.surgery_episode_id)
    AND pr.research_id = o.research_id
LEFT JOIN pathology_rai_linkage_v3 pr3
    ON pr3.surgery_episode_id = COALESCE(sp3.path_surgery_id, sp.tumor_episode_id, o.surgery_episode_id)
    AND pr3.research_id = o.research_id
    AND pr3.score_rank = 1
LEFT JOIN rai_treatment_episode_v2 r
    ON r.rai_episode_id = pr.rai_episode_id
"""


# ─────────────────────────────────────────────────────────────────────────────
# lesion_analysis_resolved_v1 (one row per tumor/lesion)
# ─────────────────────────────────────────────────────────────────────────────
LESION_RESOLVED_SQL = """
CREATE OR REPLACE TABLE lesion_analysis_resolved_v1 AS
SELECT
    t.research_id,
    t.surgery_episode_id,
    t.tumor_ordinal,
    t.surgery_date,
    t.primary_histology                     AS histology,
    t.histology_variant,
    t.laterality,
    t.tumor_size_cm,
    t.t_stage,
    t.extrathyroidal_extension              AS ete_grade,
    t.gross_ete,
    t.vascular_invasion,
    t.lymphatic_invasion,
    t.perineural_invasion,
    t.capsular_invasion,
    t.margin_status,
    t.nodal_disease_positive_count          AS ln_positive,
    t.nodal_disease_total_count             AS ln_examined,
    t.extranodal_extension,
    t.multifocality_flag,
    -- Linked FNA via best v3 linkage
    fi3.fna_episode_id                      AS linked_fna_episode_id,
    fi3.day_gap                             AS fna_day_gap,
    fi3.linkage_score                       AS fna_link_score,
    fi3.linkage_confidence_tier             AS fna_link_tier,
    fi3.analysis_eligible_link_flag         AS fna_link_eligible,
    f.bethesda_category                     AS fna_bethesda,
    -- Linked molecular via FNA -> molecular chain
    fm3.molecular_episode_id                AS linked_mol_episode_id,
    fm3.linkage_score                       AS mol_link_score,
    m.platform                              AS mol_platform,
    m.braf_flag                             AS mol_braf_flag,
    m.ras_flag                              AS mol_ras_flag,
    m.tert_flag                             AS mol_tert_flag,
    -- Linked imaging nodule via v3 imaging-FNA chain
    -- (nodule-level linkage from imaging_fna_linkage_v3)
    img.nodule_id                           AS linked_nodule_id,
    img.img_size_cm                         AS imaging_nodule_size_cm,
    img.linkage_score                       AS imaging_link_score,
    img.linkage_confidence_tier             AS imaging_link_tier,
    -- Lesion-level eligibility
    (t.primary_histology IS NOT NULL)       AS lesion_analysis_eligible_flag,
    -- Audit
    'v1'                                    AS resolved_layer_version,
    CURRENT_TIMESTAMP                       AS resolved_at
FROM tumor_episode_master_v2 t
-- Best FNA for this surgery (via v3 linkage — avoids cartesian product)
LEFT JOIN (
    SELECT research_id, surgery_episode_id,
           preop_episode_id AS fna_episode_id,
           linkage_score AS fna_preop_link_score
    FROM preop_surgery_linkage_v3
    WHERE preop_type = 'fna' AND score_rank = 1
) ps3 ON ps3.research_id = t.research_id
     AND ps3.surgery_episode_id = t.surgery_episode_id
LEFT JOIN fna_episode_master_v2 f ON f.fna_episode_id = ps3.fna_episode_id
-- Best imaging nodule for this patient (v3 scoring)
LEFT JOIN (
    SELECT research_id, nodule_id, fna_episode_id,
           day_gap, linkage_score, img_size_cm,
           linkage_confidence_tier, analysis_eligible_link_flag
    FROM imaging_fna_linkage_v3
    WHERE score_rank = 1
) img ON img.research_id = t.research_id
-- FNA -> imaging linkage (uses linked fna_episode_id from ps3)
LEFT JOIN (
    SELECT fna_episode_id, day_gap, linkage_score,
           linkage_confidence_tier, analysis_eligible_link_flag
    FROM imaging_fna_linkage_v3
    WHERE score_rank = 1
) fi3 ON fi3.fna_episode_id = ps3.fna_episode_id
-- FNA -> molecular chain (v3 scored)
LEFT JOIN (
    SELECT fna_episode_id, molecular_episode_id, linkage_score
    FROM fna_molecular_linkage_v3
    WHERE score_rank = 1
) fm3 ON fm3.fna_episode_id = ps3.fna_episode_id
LEFT JOIN molecular_test_episode_v2 m
    ON m.molecular_episode_id = fm3.molecular_episode_id
"""


def _resolve_md_prefix(con: duckdb.DuckDBPyConnection, tbl: str) -> None:
    """If *tbl* doesn't exist but md_*tbl* does, alias it via temp table."""
    if table_available(con, tbl):
        return
    md_name = f"md_{tbl}"
    if table_available(con, md_name):
        print(f"  [INFO] {tbl} not found; aliasing from {md_name}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {tbl} AS SELECT * FROM {md_name}")


def _ensure_optional_stubs(con: duckdb.DuckDBPyConnection) -> None:
    """Create empty stubs for tables that may not be deployed yet."""
    # Resolve md_* prefixed tables first (MotherDuck materialized copies)
    md_resolve = [
        "surgery_pathology_linkage_v2",
        "pathology_rai_linkage_v2",
        "preop_surgery_linkage_v2",
    ]
    for tbl in md_resolve:
        _resolve_md_prefix(con, tbl)

    # Special fallback: if thyroid_scoring_systems_v1 is locked but thyroid_scoring_py_v1 exists,
    # create a temp view with the correct column names so script 48 proceeds
    if not table_available(con, "thyroid_scoring_systems_v1") and table_available(con, "thyroid_scoring_py_v1"):
        print("  [INFO] thyroid_scoring_systems_v1 not found; aliasing from thyroid_scoring_py_v1")
        try:
            con.execute("""
CREATE OR REPLACE TEMP TABLE thyroid_scoring_systems_v1 AS
SELECT research_id,
    ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group,
    ajcc8_stage_calculable_flag AS ajcc8_calculable_flag,
    macis_missing_components AS ajcc8_missing_components,
    ata_initial_risk AS ata_risk_category,
    ata_risk_calculable_flag AS ata_calculable_flag,
    ata_response_provisional AS ata_response_category,
    ata_response_is_provisional AS ata_response_calculable_flag,
    macis_score, macis_risk_group, macis_calculable_flag,
    ages_score, ages_calculable_flag,
    ames_risk AS ames_risk_group, ames_calculable_flag,
    ln_ratio, ln_burden_band,
    molecular_risk_tier
FROM thyroid_scoring_py_v1
""")
        except Exception as e2:
            print(f"  [WARN] Could not alias thyroid_scoring_py_v1: {e2}")

    stubs = {
        "demographics_harmonized_v3": """
SELECT NULL::INTEGER AS research_id, NULL::DOUBLE AS age_at_surgery,
       NULL::VARCHAR AS sex, NULL::VARCHAR AS race WHERE 1=0""",
        "extracted_fna_bethesda_v1": """
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS bethesda_final,
       NULL::VARCHAR AS source_tables, NULL::INTEGER AS confidence WHERE 1=0""",
        "extracted_tirads_validated_v1": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS tirads_best_score,
       NULL::INTEGER AS tirads_worst_score, NULL::VARCHAR AS tirads_best_category,
       NULL::VARCHAR AS tirads_source, NULL::VARCHAR AS tirads_reliability,
       NULL::INTEGER AS n_nodule_records, NULL::DOUBLE AS nodule_size_max_mm WHERE 1=0""",
        "thyroid_scoring_systems_v1": """
SELECT NULL::INTEGER AS research_id,
       NULL::VARCHAR AS ajcc8_t_stage, NULL::VARCHAR AS ajcc8_n_stage,
       NULL::VARCHAR AS ajcc8_m_stage, NULL::VARCHAR AS ajcc8_stage_group,
       NULL::BOOLEAN AS ajcc8_calculable_flag, NULL::VARCHAR AS ajcc8_missing_components,
       NULL::VARCHAR AS ata_risk_category, NULL::BOOLEAN AS ata_calculable_flag,
       NULL::VARCHAR AS ata_response_category, NULL::BOOLEAN AS ata_response_calculable_flag,
       NULL::DOUBLE AS macis_score, NULL::VARCHAR AS macis_risk_group,
       NULL::BOOLEAN AS macis_calculable_flag,
       NULL::DOUBLE AS ages_score, NULL::BOOLEAN AS ages_calculable_flag,
       NULL::VARCHAR AS ames_risk_group, NULL::BOOLEAN AS ames_calculable_flag,
       NULL::DOUBLE AS ln_ratio, NULL::VARCHAR AS ln_burden_band,
       NULL::VARCHAR AS molecular_risk_tier WHERE 1=0""",
        "complication_patient_summary_v1": """
SELECT NULL::INTEGER AS research_id,
       NULL::VARCHAR AS hypocalcemia_status, NULL::VARCHAR AS hypoparathyroidism_status,
       NULL::VARCHAR AS rln_status, NULL::VARCHAR AS hematoma_status,
       NULL::VARCHAR AS seroma_status, NULL::VARCHAR AS chyle_leak_status,
       NULL::VARCHAR AS wound_infection_status,
       NULL::BOOLEAN AS any_confirmed_complication_flag,
       NULL::INTEGER AS n_confirmed_complications,
       NULL::BOOLEAN AS calcium_supplement_required,
       NULL::BOOLEAN AS rln_permanent_flag, NULL::BOOLEAN AS rln_transient_flag WHERE 1=0""",
        "longitudinal_lab_patient_summary_v1": """
SELECT NULL::INTEGER AS research_id,
       NULL::DOUBLE AS tg_nadir, NULL::DOUBLE AS tg_last_value, NULL::DOUBLE AS tg_peak,
       NULL::INTEGER AS tg_n_measurements, NULL::BOOLEAN AS tg_rising_flag,
       NULL::DOUBLE AS tg_doubling_time_days, NULL::BOOLEAN AS tg_below_threshold_ever,
       NULL::DOUBLE AS anti_tg_nadir, NULL::BOOLEAN AS anti_tg_rising_flag,
       NULL::BOOLEAN AS tsh_suppressed_ever,
       NULL::DOUBLE AS pth_nadir, NULL::DOUBLE AS calcium_nadir,
       NULL::BOOLEAN AS postop_low_pth_flag, NULL::BOOLEAN AS postop_low_calcium_flag,
       NULL::INTEGER AS lab_completeness_score WHERE 1=0""",
        "recurrence_event_clean_v1": """
SELECT NULL::INTEGER AS research_id,
       NULL::DATE AS recurrence_date, NULL::BOOLEAN AS structural_recurrence_flag,
       NULL::BOOLEAN AS biochemical_recurrence_flag,
       NULL::VARCHAR AS recurrence_type_primary, NULL::VARCHAR AS recurrence_site_primary,
       NULL::DOUBLE AS source_priority, NULL::VARCHAR AS source_table WHERE 1=0""",
        "lineage_audit_v1": """
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS date_traceability_status WHERE 1=0""",
        "rai_treatment_episode_v2": """
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS rai_episode_id,
       NULL::DATE AS resolved_rai_date, NULL::VARCHAR AS rai_assertion_status,
       NULL::DOUBLE AS dose_mci WHERE 1=0""",
        "imaging_fna_linkage_v3": """
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS nodule_id,
       NULL::VARCHAR AS fna_episode_id, NULL::INTEGER AS day_gap,
       NULL::DOUBLE AS linkage_score, NULL::VARCHAR AS linkage_confidence_tier,
       NULL::DOUBLE AS img_size_cm, NULL::BOOLEAN AS analysis_eligible_link_flag,
       NULL::INTEGER AS score_rank WHERE 1=0""",
        "fna_molecular_linkage_v3": """
SELECT NULL::VARCHAR AS fna_episode_id, NULL::VARCHAR AS molecular_episode_id,
       NULL::DOUBLE AS linkage_score, NULL::INTEGER AS score_rank WHERE 1=0""",
        "surgery_pathology_linkage_v2": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS surgery_episode_id,
       NULL::INTEGER AS tumor_episode_id, NULL::INTEGER AS tumor_ordinal,
       NULL::DATE AS surgery_date_native, NULL::DATE AS tumor_surgery_date,
       NULL::VARCHAR AS linkage_confidence, NULL::BOOLEAN AS laterality_match WHERE 1=0""",
        "preop_surgery_linkage_v2": """
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS preop_type,
       NULL::VARCHAR AS preop_episode_id, NULL::INTEGER AS surgery_episode_id,
       NULL::VARCHAR AS linkage_confidence, NULL::INTEGER AS day_gap WHERE 1=0""",
        "pathology_rai_linkage_v2": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS surgery_episode_id,
       NULL::VARCHAR AS rai_episode_id, NULL::VARCHAR AS linkage_confidence,
       NULL::INTEGER AS days_surg_to_rai WHERE 1=0""",
        "surgery_pathology_linkage_v3": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS surgery_episode_id,
       NULL::INTEGER AS path_surgery_id, NULL::INTEGER AS tumor_ordinal,
       NULL::DOUBLE AS linkage_score,
       NULL::VARCHAR AS linkage_confidence_tier, NULL::VARCHAR AS linkage_reason_summary,
       NULL::BOOLEAN AS analysis_eligible_link_flag, NULL::INTEGER AS score_rank WHERE 1=0""",
        "preop_surgery_linkage_v3": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS surgery_episode_id,
       NULL::DOUBLE AS linkage_score, NULL::INTEGER AS score_rank WHERE 1=0""",
        "pathology_rai_linkage_v3": """
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS surgery_episode_id,
       NULL::VARCHAR AS rai_episode_id, NULL::DOUBLE AS linkage_score,
       NULL::INTEGER AS score_rank WHERE 1=0""",
    }

    for tbl, stub_select in stubs.items():
        if not table_available(con, tbl):
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {tbl} AS {stub_select}"
            )


def _ensure_mcv12(con: duckdb.DuckDBPyConnection) -> bool:
    """Ensure patient_refined_master_clinical_v12 is available, return True if ok."""
    if table_available(con, "patient_refined_master_clinical_v12"):
        return True
    # Try older versions
    for ver in range(11, 4, -1):
        tbl = f"patient_refined_master_clinical_v{ver}"
        if table_available(con, tbl):
            print(f"  [WARN] v12 not found; using {tbl} as alias")
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE patient_refined_master_clinical_v12 "
                f"AS SELECT * FROM {tbl}"
            )
            return True
    print("  [ERROR] No patient_refined_master_clinical_v* found")
    return False


def _patch_histology_sql(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    """Replace histology_normalized reference if column doesn't exist in mcv12."""
    if col_available(con, "patient_refined_master_clinical_v12",
                     "histology_normalized"):
        return sql
    print("  [INFO] histology_normalized not in mcv12 — using path_histology_raw")
    return sql.replace(
        "LOWER(CAST(mcv.histology_normalized AS VARCHAR))",
        "NULL",
    )


def build_resolved_tables(con: duckdb.DuckDBPyConnection,
                          dry_run: bool = False) -> None:
    section("Building analysis-grade resolved layer")

    # Check required tables
    required = ["tumor_episode_master_v2", "operative_episode_detail_v2"]
    for tbl in required:
        if not table_available(con, tbl):
            print(f"  [SKIP] Required table {tbl} not found -- run script 22 first")
            return

    if not _ensure_mcv12(con):
        return

    _ensure_optional_stubs(con)

    if dry_run:
        print("  [DRY-RUN] Would create: patient_analysis_resolved_v1, "
              "episode_analysis_resolved_v1, lesion_analysis_resolved_v1")
        return

    # ── Patient resolved ──────────────────────────────────────────────────
    print("  Building patient_analysis_resolved_v1...")
    try:
        patient_sql = _patch_histology_sql(con, PATIENT_RESOLVED_SQL)
        con.execute(patient_sql)
        r = con.execute(
            "SELECT COUNT(*) AS n_patients, "
            "SUM(CASE WHEN analysis_eligible_flag THEN 1 ELSE 0 END) AS n_eligible "
            "FROM patient_analysis_resolved_v1"
        ).fetchone()
        print(f"    patient_analysis_resolved_v1: {r[0]:,} rows, "
              f"{r[1]:,} analysis-eligible")
    except Exception as exc:
        print(f"    [ERROR] patient_analysis_resolved_v1: {exc}")

    # ── Episode resolved ──────────────────────────────────────────────────
    print("  Building episode_analysis_resolved_v1...")
    try:
        con.execute(EPISODE_RESOLVED_SQL)
        r = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT research_id) "
            "FROM episode_analysis_resolved_v1"
        ).fetchone()
        print(f"    episode_analysis_resolved_v1: {r[0]:,} episodes, "
              f"{r[1]:,} patients")
    except Exception as exc:
        print(f"    [ERROR] episode_analysis_resolved_v1: {exc}")

    # ── Lesion resolved ───────────────────────────────────────────────────
    print("  Building lesion_analysis_resolved_v1...")
    try:
        con.execute(LESION_RESOLVED_SQL)
        r = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT research_id) "
            "FROM lesion_analysis_resolved_v1"
        ).fetchone()
        print(f"    lesion_analysis_resolved_v1: {r[0]:,} lesions, "
              f"{r[1]:,} patients")
    except Exception as exc:
        print(f"    [ERROR] lesion_analysis_resolved_v1: {exc}")

    # ── Quick validation ──────────────────────────────────────────────────
    print("\n  Quick validation...")
    try:
        dupes = con.execute(
            "SELECT COUNT(*) FROM (SELECT research_id FROM patient_analysis_resolved_v1 "
            "GROUP BY research_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        print(f"    Duplicate research_id check: {dupes} duplicates "
              f"({'PASS' if dupes == 0 else 'FAIL'})")
    except Exception:
        pass

    print("\n  [DONE] Analysis-grade resolved layer created")


def main() -> None:
    p = argparse.ArgumentParser(
        description="48_build_analysis_resolved_layer.py"
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--dry-run", action="store_true", help="Audit only, no writes")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        build_resolved_tables(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 48_build_analysis_resolved_layer.py finished")


if __name__ == "__main__":
    main()
