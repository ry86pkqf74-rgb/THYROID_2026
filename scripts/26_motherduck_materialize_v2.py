#!/usr/bin/env python3
"""
26_motherduck_materialize_v2.py -- MotherDuck materialization of v2 tables

Materializes canonical episode tables, granular linkage tables,
reconciliation review views, QA outputs, Streamlit-critical views,
upstream heavy views, adjudication views, manual review queues,
and date rescue KPIs into MotherDuck for downstream Streamlit
consumption.  Skips any source view that does not yet exist.

Run after scripts 15-25, 27.
Supports --md flag (required for actual MotherDuck deployment).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

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


MATERIALIZATION_MAP: list[tuple[str, str]] = [
    # ── V2 canonical episode tables (script 22) ─────────────────────
    ("md_tumor_episode_master_v2", "tumor_episode_master_v2"),
    ("md_molecular_test_episode_v2", "molecular_test_episode_v2"),
    ("md_rai_treatment_episode_v2", "rai_treatment_episode_v2"),
    ("md_imaging_nodule_long_v2", "imaging_nodule_long_v2"),
    ("md_imaging_exam_summary_v2", "imaging_exam_summary_v2"),
    ("md_oper_episode_detail_v2", "operative_episode_detail_v2"),
    ("md_fna_episode_master_v2", "fna_episode_master_v2"),
    ("md_event_date_audit_v2", "event_date_audit_v2"),
    ("md_patient_cross_domain_timeline_v2", "patient_cross_domain_timeline_v2"),
    # ── V2 granular linkage tables (script 23) ──────────────────────
    ("md_imaging_fna_linkage_v2", "imaging_fna_linkage_v2"),
    ("md_fna_molecular_linkage_v2", "fna_molecular_linkage_v2"),
    ("md_preop_surgery_linkage_v2", "preop_surgery_linkage_v2"),
    ("md_surgery_pathology_linkage_v2", "surgery_pathology_linkage_v2"),
    ("md_pathology_rai_linkage_v2", "pathology_rai_linkage_v2"),
    ("md_linkage_summary_v2", "linkage_summary_v2"),
    # ── V2 QA tables (script 25) ───────────────────────────────────
    ("md_date_quality_summary_v2", "qa_date_completeness_v2"),
    ("md_qa_issues_v2", "qa_issues_v2"),
    ("md_qa_summary_v2", "qa_summary_by_domain_v2"),
    ("md_qa_high_priority_v2", "qa_high_priority_review_v2"),
    # ── V2 reconciliation review views (script 24) ─────────────────
    ("md_pathology_recon_review_v2", "pathology_reconciliation_review_v2"),
    ("md_molecular_linkage_review_v2", "molecular_linkage_review_v2"),
    ("md_rai_adjudication_review_v2", "rai_adjudication_review_v2"),
    ("md_imaging_path_concordance_v2", "imaging_pathology_concordance_review_v2"),
    ("md_op_path_recon_review_v2", "operative_pathology_reconciliation_review_v2"),
    # ── P0: Streamlit-critical views (script 18) ───────────────────
    ("md_streamlit_cohort_qc_summary", "streamlit_cohort_qc_summary_v"),
    ("md_patient_reconciliation_summary", "patient_reconciliation_summary_v"),
    ("md_streamlit_patient_header", "streamlit_patient_header_v"),
    ("md_streamlit_patient_timeline", "streamlit_patient_timeline_v"),
    ("md_streamlit_patient_conflicts", "streamlit_patient_conflicts_v"),
    ("md_streamlit_patient_manual_review", "streamlit_patient_manual_review_v"),
    # ── P1: Upstream heavy views (scripts 16-18) ───────────────────
    ("md_histology_reconciliation_v2", "histology_reconciliation_v2"),
    ("md_validation_failures_v3", "validation_failures_v3"),
    ("md_patient_validation_rollup_v2", "patient_validation_rollup_v2_mv"),
    ("md_molecular_episode_v2", "molecular_episode_v2"),
    ("md_rai_episode_v2", "rai_episode_v2"),
    ("md_patient_master_timeline_v2", "patient_master_timeline_v2"),
    ("md_patient_manual_review_summary", "patient_manual_review_summary_v"),
    # ── P1: Adjudication / v3 views (scripts 18-19) ────────────────
    ("md_histology_analysis_cohort", "histology_analysis_cohort_v"),
    ("md_histology_discordance_summary", "histology_discordance_summary_v"),
    ("md_molecular_episode_v3", "molecular_episode_v3"),
    ("md_molecular_analysis_cohort", "molecular_analysis_cohort_v"),
    ("md_molecular_linkage_failure_summary", "molecular_linkage_failure_summary_v"),
    ("md_rai_episode_v3", "rai_episode_v3"),
    ("md_rai_analysis_cohort", "rai_analysis_cohort_v"),
    ("md_rai_linkage_failure_summary", "rai_linkage_failure_summary_v"),
    ("md_adjudication_progress_summary", "adjudication_progress_summary_v"),
    ("md_adjudication_domain_counts", "adjudication_domain_counts_v"),
    ("md_top_priority_review_batches", "top_priority_review_batches_v"),
    ("md_reviewer_resolved_patient_summary", "reviewer_resolved_patient_summary_v"),
    # ── P1: Post-review overlay views (script 19) ──────────────────
    ("md_histology_post_review", "histology_post_review_v"),
    ("md_molecular_post_review", "molecular_post_review_v"),
    ("md_rai_post_review", "rai_post_review_v"),
    # ── P1: Manual review queues (script 18) ───────────────────────
    ("md_histology_manual_review_queue", "histology_manual_review_queue_v"),
    ("md_molecular_manual_review_queue", "molecular_manual_review_queue_v"),
    ("md_rai_manual_review_queue", "rai_manual_review_queue_v"),
    ("md_timeline_manual_review_queue", "timeline_manual_review_queue_v"),
    ("md_unresolved_high_value_cases", "unresolved_high_value_cases_v"),
    # ── Manuscript cohort views (script 20) ────────────────────────
    ("md_manuscript_histology_cohort", "manuscript_histology_cohort_v"),
    ("md_manuscript_molecular_cohort", "manuscript_molecular_cohort_v"),
    ("md_manuscript_rai_cohort", "manuscript_rai_cohort_v"),
    ("md_manuscript_patient_summary", "manuscript_patient_summary_v"),
    # ── Date rescue KPI (script 27) ────────────────────────────────
    ("md_date_rescue_rate_summary", "date_rescue_rate_summary"),
    # ── RLN injury refined (rln_refined_pipeline) ────────────────
    ("md_extracted_rln_injury_refined", "extracted_rln_injury_refined_v2"),
    ("md_extracted_rln_injury_refined_summary", "extracted_rln_injury_refined_summary_v2"),
    ("md_extracted_rln_exclusion_audit", "extracted_rln_exclusion_audit_v2"),
    # ── Phase 2 QA: Complication refinement (complications_refined_pipeline) ──
    ("md_extracted_chyle_leak_refined_v2", "extracted_chyle_leak_refined_v2"),
    ("md_extracted_hypocalcemia_refined_v2", "extracted_hypocalcemia_refined_v2"),
    ("md_extracted_seroma_refined_v2", "extracted_seroma_refined_v2"),
    ("md_extracted_hematoma_refined_v2", "extracted_hematoma_refined_v2"),
    ("md_extracted_hypoparathyroidism_refined_v2", "extracted_hypoparathyroidism_refined_v2"),
    ("md_extracted_wound_infection_refined_v2", "extracted_wound_infection_refined_v2"),
    ("md_extracted_complications_refined_v5", "extracted_complications_refined_v5"),
    ("md_extracted_complications_exclusion_audit_v2", "extracted_complications_exclusion_audit_v2"),
    ("md_patient_refined_complication_flags_v2", "patient_refined_complication_flags_v2"),
    # ── Phase 4 Source-Specific Refinement ─────────────────────────
    ("md_extracted_ete_refined_v1", "extracted_ete_refined_v1"),
    ("md_extracted_variables_refined_v6", "extracted_variables_refined_v6"),
    ("md_patient_refined_staging_flags_v3", "patient_refined_staging_flags_v3"),
    # ── Phase 5 Top-5 Variable Refinement ────────────────────────
    ("md_extracted_ete_subgraded_v1", "extracted_ete_subgraded_v1"),
    ("md_extracted_molecular_refined_v1", "extracted_molecular_refined_v1"),
    ("md_extracted_postop_labs_v1", "extracted_postop_labs_v1"),
    ("md_extracted_rai_validated_v1", "extracted_rai_validated_v1"),
    ("md_extracted_ene_refined_v1", "extracted_ene_refined_v1"),
    ("md_patient_refined_master_clinical_v4", "patient_refined_master_clinical_v4"),
    # ── Phase 6 Source-Linked Staging Refinement ─────────────────
    ("md_extracted_margins_refined_v1", "extracted_margins_refined_v1"),
    ("md_extracted_invasion_profile_v1", "extracted_invasion_profile_v1"),
    ("md_extracted_ln_yield_v1", "extracted_ln_yield_v1"),
    ("md_extracted_ene_refined_v2", "extracted_ene_refined_v2"),
    ("md_extracted_staging_details_refined_v1", "extracted_staging_details_refined_v1"),
    ("md_patient_refined_master_clinical_v5", "patient_refined_master_clinical_v5"),
    ("md_vw_margins_by_source", "vw_margins_by_source"),
    ("md_vw_invasion_profile", "vw_invasion_profile"),
    ("md_vw_ln_yield_summary", "vw_ln_yield_summary"),
    # ── Phase 9 Targeted Refinement (extraction_audit_engine_v7) ────
    ("md_extracted_postop_labs_expanded_v1", "extracted_postop_labs_expanded_v1"),
    ("md_vw_postop_lab_expanded", "vw_postop_lab_expanded"),
    ("md_extracted_rai_dose_refined_v1", "extracted_rai_dose_refined_v1"),
    ("md_vw_rai_dose_by_source", "vw_rai_dose_by_source"),
    ("md_extracted_ete_ene_tert_refined_v1", "extracted_ete_ene_tert_refined_v1"),
    ("md_vw_ete_microscopic_rule", "vw_ete_microscopic_rule"),
    ("md_extracted_ene_multisource_v1", "extracted_ene_multisource_v1"),
    ("md_vw_ene_concordance", "vw_ene_concordance"),
    ("md_vw_ene_source_summary", "vw_ene_source_summary"),
    ("md_patient_refined_master_clinical_v8", "patient_refined_master_clinical_v8"),
    # ── Phase 10 Source-Linked Recovery (extraction_audit_engine_v8) ────
    ("md_extracted_margin_r0_recovery_v1", "extracted_margin_r0_recovery_v1"),
    ("md_vw_margin_r0_recovery", "vw_margin_r0_recovery"),
    ("md_extracted_invasion_grading_recovery_v1", "extracted_invasion_grading_recovery_v1"),
    ("md_extracted_lateral_neck_v1", "extracted_lateral_neck_v1"),
    ("md_vw_lateral_neck", "vw_lateral_neck"),
    ("md_extracted_multi_tumor_aggregate_v1", "extracted_multi_tumor_aggregate_v1"),
    ("md_extracted_staging_recovery_v1", "extracted_staging_recovery_v1"),
    ("md_extracted_mice_summary_v1", "extracted_mice_summary_v1"),
    ("md_patient_refined_master_clinical_v9", "patient_refined_master_clinical_v9"),
    # ── Phase 11 Final Sweep: Imaging, RAS, BRAF, Pre-op Excel (extraction_audit_engine_v9) ──
    ("md_extracted_us_tirads_v1", "extracted_us_tirads_v1"),
    ("md_extracted_nodule_sizes_v1", "extracted_nodule_sizes_v1"),
    ("md_extracted_ras_subtypes_v1", "extracted_ras_subtypes_v1"),
    ("md_extracted_ras_patient_summary_v1", "extracted_ras_patient_summary_v1"),
    ("md_extracted_braf_recovery_v1", "extracted_braf_recovery_v1"),
    ("md_vw_braf_audit", "vw_braf_audit"),
    ("md_extracted_preop_sweep_v1", "extracted_preop_sweep_v1"),
    ("md_vw_us_tirads", "vw_us_tirads"),
    ("md_vw_molecular_subtypes", "vw_molecular_subtypes"),
    ("md_extracted_imaging_molecular_final_v1", "extracted_imaging_molecular_final_v1"),
    ("md_patient_refined_master_clinical_v10", "patient_refined_master_clinical_v10"),
    # ── Phase 11 Provenance Traceability (extraction_audit + date accuracy) ──
    ("md_provenance_enriched_events_v1", "provenance_enriched_events_v1"),
    ("md_lineage_audit_v1", "lineage_audit_v1"),
    ("md_val_provenance_traceability", "val_provenance_traceability"),
]

SURVIVAL_COHORT_ENRICHED_SQL = """
CREATE OR REPLACE TABLE survival_cohort_enriched AS
SELECT
    s.research_id,
    GREATEST(s.time_to_event_days, 1)                            AS time_days,
    LEAST(GREATEST(s.time_to_event_days, 1), 365 * 15)          AS time_days_capped,
    CASE WHEN s.event_occurred::VARCHAR IN ('1','true','True')
         THEN TRUE ELSE FALSE END                                AS event,
    s.age_at_surgery                                             AS age_at_diagnosis,
    s.sex,
    s.histology_1_type                                           AS histology,
    s.overall_stage_ajcc8                                        AS ajcc_stage_8,
    CASE
        WHEN LOWER(CAST(af.tumor_1_extrathyroidal_ext AS VARCHAR))
             LIKE '%extensive%'
          OR LOWER(CAST(af.tumor_1_extrathyroidal_ext AS VARCHAR))
             LIKE '%gross%'                                  THEN 'gross'
        WHEN af.tumor_1_extrathyroidal_ext IS NOT NULL
         AND LOWER(CAST(af.tumor_1_extrathyroidal_ext AS VARCHAR))
             NOT IN ('','no','none','absent','not identified',
                     'not present','negative')               THEN 'microscopic'
        ELSE 'none'
    END                                                          AS ete_type,
    COALESCE(
        LOWER(CAST(r.braf_positive AS VARCHAR)) = 'true',
        LOWER(CAST(af.braf_mutation_mentioned AS VARCHAR)) = 'true',
        FALSE)                                                   AS braf_status,
    COALESCE(
        LOWER(CAST(r.tert_positive AS VARCHAR)) = 'true',
        LOWER(CAST(af.tert_mutation_mentioned AS VARCHAR)) = 'true',
        FALSE)                                                   AS tert_status,
    COALESCE(
        LOWER(CAST(r.ras_positive AS VARCHAR)) = 'true',
        LOWER(CAST(af.ras_mutation_mentioned AS VARCHAR)) = 'true',
        FALSE)                                                   AS ras_status,
    COALESCE(
        LOWER(CAST(r.ret_positive AS VARCHAR)) = 'true',
        LOWER(CAST(af.ret_mutation_mentioned AS VARCHAR)) = 'true',
        FALSE)                                                   AS ret_status,
    r.recurrence_risk_band,
    EXTRACT(YEAR FROM TRY_CAST(s.surgery_date AS DATE))          AS diagnosis_year,
    TRY_CAST(r.tumor_size_cm AS DOUBLE)                          AS tumor_size_cm,
    COALESCE(TRY_CAST(r.ln_positive AS INT), 0)                  AS ln_positive,
    COALESCE(TRY_CAST(r.ln_examined AS INT), 0)                  AS ln_examined,
    TRY_CAST(r.tg_annual_log_slope AS DOUBLE)                    AS tg_annual_log_slope,
    CASE WHEN s.event_occurred::VARCHAR IN ('1','true','True')
         THEN 1 ELSE 0 END                                      AS event_type
FROM survival_cohort_ready_mv s
LEFT JOIN advanced_features_sorted af
    ON CAST(s.research_id AS VARCHAR) = CAST(af.research_id AS VARCHAR)
LEFT JOIN recurrence_risk_features_mv r
    ON CAST(s.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
WHERE s.age_at_surgery BETWEEN 18 AND 90
  AND s.time_to_event_days > 0
"""

SURVIVAL_KPIS_SQL = """
CREATE OR REPLACE TABLE survival_kpis AS
SELECT
    COUNT(*)                         AS n,
    SUM(event::INT)                  AS events,
    ROUND(AVG(time_days) / 365.25, 2) AS mean_followup_years,
    ROUND(MEDIAN(time_days) / 365.25, 2) AS median_followup_years,
    ROUND(100.0 * SUM(event::INT) / NULLIF(COUNT(*), 0), 1) AS event_rate_pct
FROM survival_cohort_enriched
"""

CURE_COHORT_SQL = """
-- MIXTURE CURE LAYER — added for high-cure-fraction modeling
CREATE OR REPLACE TABLE cure_cohort AS
SELECT
    research_id,
    COALESCE(time_days, 365 * 15)                                AS time_days,   -- 15-year administrative censor
    event::BOOLEAN                                               AS event,
    age_at_diagnosis,
    sex,
    ajcc_stage_8,
    ete_type,
    braf_status,
    tert_status,
    ras_status,
    ret_status,
    recurrence_risk_band,
    diagnosis_year,
    histology,
    ln_positive,
    ln_examined,
    tumor_size_cm
FROM survival_cohort_enriched
WHERE age_at_diagnosis BETWEEN 18 AND 90
  AND diagnosis_year >= 2010
  AND (event IS NOT NULL OR time_days IS NOT NULL)
"""

CURE_KPIS_SQL = """
CREATE OR REPLACE TABLE cure_kpis AS
SELECT
    COUNT(*)                           AS n_total,
    AVG(event::INT)                    AS observed_event_rate,
    1 - AVG(event::INT)                AS crude_cure_rate
FROM cure_cohort
"""

# ── PROMOTION TIME CURE LAYER (added 2026-03-10) ──────────────────────────
# Reads from survival_cohort_enriched (built immediately above) so that
# braf_status, tert_status, ete_type, etc. are already normalised.
# ps_weight defaults to 1.0 (unweighted); override post-PSM if desired.
# ntrk_status uses ras_status as the closest available proxy.
PROMOTION_CURE_COHORT_SQL = """
CREATE OR REPLACE TABLE promotion_cure_cohort AS
SELECT
    research_id,
    COALESCE(time_days, 365 * 15)            AS time_days,
    event::BOOLEAN                            AS event,
    age_at_diagnosis,
    sex,
    ajcc_stage_8,
    ete_type,
    braf_status,
    tert_status,
    ras_status                                AS ntrk_status,
    recurrence_risk_band,
    NULL::VARCHAR                             AS rai_avidity_category,
    diagnosis_year,
    histology,
    1.0::DOUBLE                               AS ps_weight
FROM survival_cohort_enriched
WHERE age_at_diagnosis BETWEEN 18 AND 90
  AND diagnosis_year >= 2010
"""

PROMOTION_CURE_KPIS_SQL = """
CREATE OR REPLACE TABLE promotion_cure_kpis AS
SELECT
    COUNT(*)                                                                  AS n_total,
    AVG(event::INT)                                                           AS event_rate,
    COUNT(CASE WHEN event = FALSE AND time_days > 365 * 10 THEN 1 END)::FLOAT
        / NULLIF(COUNT(*), 0)                                                 AS plateau_10y_rate
FROM promotion_cure_cohort
"""

# ── MIXTURE CURE LAYER — population split model (added 2026-03-11) ─────
# Same base cohort as PTCM for direct head-to-head comparison.
MIXTURE_CURE_COHORT_SQL = """
CREATE OR REPLACE TABLE mixture_cure_cohort AS
SELECT * FROM promotion_cure_cohort
"""

MIXTURE_CURE_KPIS_SQL = """
CREATE OR REPLACE TABLE mixture_cure_kpis AS
SELECT
    COUNT(*)                           AS n_total,
    AVG(event::INT)                    AS event_rate,
    1 - AVG(event::INT)               AS crude_cure_rate
FROM mixture_cure_cohort
"""

MANUAL_REVIEW_QUEUE_SUMMARY_SQL = """
CREATE OR REPLACE TABLE md_manual_review_queue_summary_v2 AS
SELECT 'pathology' AS domain,
       COUNT(*) AS total_issues,
       COUNT(*) FILTER (WHERE review_severity = 'error') AS error_count,
       COUNT(*) FILTER (WHERE review_severity = 'warning') AS warning_count,
       COUNT(DISTINCT research_id) AS patients_affected
FROM pathology_reconciliation_review_v2
UNION ALL
SELECT 'molecular',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM molecular_linkage_review_v2
UNION ALL
SELECT 'rai',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM rai_adjudication_review_v2
UNION ALL
SELECT 'imaging_pathology',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM imaging_pathology_concordance_review_v2
UNION ALL
SELECT 'operative_pathology',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM operative_pathology_reconciliation_review_v2
"""


def materialize_all(
    source_con: duckdb.DuckDBPyConnection,
    target_con: duckdb.DuckDBPyConnection,
    same_connection: bool = False,
) -> None:
    """Materialize all v2 tables from source to target."""

    section("Materializing v2 tables")

    if same_connection:
        for md_name, src_name in MATERIALIZATION_MAP:
            if not table_available(source_con, src_name):
                print(f"  SKIP {src_name:<50} (not found in source)")
                continue
            try:
                source_con.execute(
                    f"CREATE OR REPLACE TABLE {md_name} AS "
                    f"SELECT * FROM {src_name}"
                )
                cnt = source_con.execute(
                    f"SELECT COUNT(*) FROM {md_name}"
                ).fetchone()[0]
                print(f"  OK   {md_name:<50} {cnt:>8,} rows")
            except Exception as e:
                print(f"  WARN {md_name:<50} {e}")

        # Manual review queue summary
        try:
            source_con.execute(MANUAL_REVIEW_QUEUE_SUMMARY_SQL)
            cnt = source_con.execute(
                "SELECT COUNT(*) FROM md_manual_review_queue_summary_v2"
            ).fetchone()[0]
            print(f"  OK   {'md_manual_review_queue_summary_v2':<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN md_manual_review_queue_summary_v2: {e}")

        # Survival cohort enriched + KPIs + PTCM cohort
        for sql, tbl in [
            (SURVIVAL_COHORT_ENRICHED_SQL, "survival_cohort_enriched"),
            (SURVIVAL_KPIS_SQL, "survival_kpis"),
            (CURE_COHORT_SQL, "cure_cohort"),
            (CURE_KPIS_SQL, "cure_kpis"),
            (PROMOTION_CURE_COHORT_SQL, "promotion_cure_cohort"),
            (PROMOTION_CURE_KPIS_SQL, "promotion_cure_kpis"),
            (MIXTURE_CURE_COHORT_SQL, "mixture_cure_cohort"),
            (MIXTURE_CURE_KPIS_SQL, "mixture_cure_kpis"),
        ]:
            try:
                source_con.execute(sql)
                cnt = source_con.execute(
                    f"SELECT COUNT(*) FROM {tbl}"
                ).fetchone()[0]
                print(f"  OK   {tbl:<50} {cnt:>8,} rows")
            except Exception as e:
                print(f"  WARN {tbl:<50} {e}")

    else:
        for md_name, src_name in MATERIALIZATION_MAP:
            if not table_available(source_con, src_name):
                print(f"  SKIP {src_name:<50} (not found in source)")
                continue
            try:
                df = source_con.execute(f"SELECT * FROM {src_name}").fetchdf()
                target_con.execute(f"DROP TABLE IF EXISTS {md_name}")
                target_con.register("_tmp_df", df)
                target_con.execute(
                    f"CREATE TABLE {md_name} AS SELECT * FROM _tmp_df"
                )
                target_con.unregister("_tmp_df")
                print(f"  OK   {md_name:<50} {len(df):>8,} rows")
            except Exception as e:
                print(f"  WARN {md_name:<50} {e}")

        # Manual review queue summary (build from materialized tables)
        try:
            target_con.execute(
                MANUAL_REVIEW_QUEUE_SUMMARY_SQL.replace(
                    "pathology_reconciliation_review_v2",
                    "md_pathology_recon_review_v2",
                ).replace(
                    "molecular_linkage_review_v2",
                    "md_molecular_linkage_review_v2",
                ).replace(
                    "rai_adjudication_review_v2",
                    "md_rai_adjudication_review_v2",
                ).replace(
                    "imaging_pathology_concordance_review_v2",
                    "md_imaging_path_concordance_v2",
                ).replace(
                    "operative_pathology_reconciliation_review_v2",
                    "md_op_path_recon_review_v2",
                )
            )
            cnt = target_con.execute(
                "SELECT COUNT(*) FROM md_manual_review_queue_summary_v2"
            ).fetchone()[0]
            print(f"  OK   {'md_manual_review_queue_summary_v2':<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN md_manual_review_queue_summary_v2: {e}")

        # Survival cohort enriched + KPIs + PTCM cohort (cross-DB: use md_ source tables)
        _surv_sql = SURVIVAL_COHORT_ENRICHED_SQL.replace(
            "survival_cohort_ready_mv", "md_survival_cohort_ready_mv"
        ).replace(
            "advanced_features_sorted", "md_advanced_features_sorted"
        ).replace(
            "recurrence_risk_features_mv", "md_recurrence_risk_features_mv"
        ) if not table_available(target_con, "survival_cohort_ready_mv") else SURVIVAL_COHORT_ENRICHED_SQL
        # PTCM cohort always reads from survival_cohort_enriched (built above)
        for sql, tbl in [
            (_surv_sql, "survival_cohort_enriched"),
            (SURVIVAL_KPIS_SQL, "survival_kpis"),
            (CURE_COHORT_SQL, "cure_cohort"),
            (CURE_KPIS_SQL, "cure_kpis"),
            (PROMOTION_CURE_COHORT_SQL, "promotion_cure_cohort"),
            (PROMOTION_CURE_KPIS_SQL, "promotion_cure_kpis"),
            (MIXTURE_CURE_COHORT_SQL, "mixture_cure_cohort"),
            (MIXTURE_CURE_KPIS_SQL, "mixture_cure_kpis"),
        ]:
            try:
                target_con.execute(sql)
                cnt = target_con.execute(
                    f"SELECT COUNT(*) FROM {tbl}"
                ).fetchone()[0]
                print(f"  OK   {tbl:<50} {cnt:>8,} rows")
            except Exception as e:
                print(f"  WARN {tbl:<50} {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck (required for production)")
    args = parser.parse_args()

    section("26 -- MotherDuck Materialization v2")

    # Defer local DB connection — only open if actually needed
    try:
        local_con = duckdb.connect(str(DB_PATH))
        print(f"  Source: {DB_PATH}")
    except Exception as local_err:
        local_con = None
        print(f"  Local DB unavailable ({local_err}); will attempt MD-only mode.")
        if not args.md:
            print("  ERROR: Local DB locked and --md not specified. Exiting.")
            sys.exit(1)

    required = [
        "tumor_episode_master_v2", "molecular_test_episode_v2",
        "rai_treatment_episode_v2", "imaging_nodule_long_v2",
        "operative_episode_detail_v2",
    ]

    if args.md:
        # MotherDuck-first path: check MD for required tables, skip local if all present
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            md_con = client.connect_rw()
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            local_con.close()
            sys.exit(1)

        missing_md = [t for t in required if not table_available(md_con, t)]
        missing_local = (
            [t for t in required if not table_available(local_con, t)]
            if local_con is not None else required
        )

        if not missing_md:
            print("  Source: MotherDuck (all required tables present)")
            print("  Target: MotherDuck (RW, md-only mode)")
            materialize_all(md_con, md_con, same_connection=True)
            section("Materialization Summary")
            for md_name, _ in MATERIALIZATION_MAP:
                try:
                    cnt = md_con.execute(f"SELECT COUNT(*) FROM {md_name}").fetchone()[0]
                    print(f"  {md_name:<50} {cnt:>8,} rows")
                except Exception:
                    pass
            md_con.close()
            if local_con is not None:
                local_con.close()
            print("\n  Done.\n")
            return
        elif not missing_local:
            print("  Source: local DuckDB")
            print("  Target: MotherDuck (RW)")
            materialize_all(local_con, md_con, same_connection=False)
        else:
            print(f"\n  ERROR: Tables missing from both local and MotherDuck: {missing_md}")
            print("  Run scripts 22-25 first or use _fix_missing_v2_tables.py.")
            md_con.close()
            if local_con is not None:
                local_con.close()
            sys.exit(1)

        section("Materialization Summary")
        for md_name, _ in MATERIALIZATION_MAP:
            try:
                cnt = md_con.execute(f"SELECT COUNT(*) FROM {md_name}").fetchone()[0]
                print(f"  {md_name:<50} {cnt:>8,} rows")
            except Exception:
                pass
        md_con.close()
        if local_con is not None:
            local_con.close()
        print("\n  Done.\n")
        return

    missing_local = (
        [t for t in required if not table_available(local_con, t)]
        if local_con is not None else required
    )
    if missing_local:
        print(f"\n  ERROR: Missing required tables: {missing_local}")
        print("  Run scripts 22-25 first.")
        if local_con is not None:
            local_con.close()
        sys.exit(1)

    print("  Target: local DuckDB (use --md for MotherDuck)")
    materialize_all(local_con, local_con, same_connection=True)

    section("Materialization Summary")
    for md_name, _ in MATERIALIZATION_MAP:
        try:
            cnt = local_con.execute(
                f"SELECT COUNT(*) FROM {md_name}"
            ).fetchone()[0]
            print(f"  {md_name:<50} {cnt:>8,} rows")
        except Exception:
            pass

    if local_con is not None:
        local_con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
