-- mig_273 — main.cohort_m038_massive_goiter_v1 (Snowflake COHORT_M038_MASSIVE_GOITER mirror)
--
-- APPLY: .venv/bin/python scripts/mig_273_m038_cohort_mirror.py --apply
--
-- Mirrors snowflake_trial/scripts/31_m038_massive_goiter_table1.py cohort projection
-- (weight buckets + staging/demographics passthrough). Thin SSOT for M038 tooling;
-- manuscript_workspace.cohort_m038_massive_goiter_v1 remains the extended Lane-M view.
--
-- Database: thyroid_canonical_publication_v1_0
-- signoff_migration.mig_id = 'mig_273'

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW main.cohort_m038_massive_goiter_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  race,
  histology_final,
  is_malignant,
  first_surgery_date,
  ajcc8_t_stage,
  ajcc8_n_stage,
  ajcc8_m_stage,
  ajcc8_stage_group,
  tumor_size_cm_max,
  ete_grade,
  gland_weight_final_g,
  gland_weight_source,
  multifocal_flag_path,
  syn_multinodular_goiter,
  ct_goiter_present_any,
  surg_procedure_type,
  rai_received_flag,
  any_recurrence_flag,
  overall_survival_years,
  followup_years,
  cpm_op_time_min,
  cpm_ebl_ml,
  cpm_los_days,
  cpm_op_time_min_source,
  cpm_ebl_ml_source,
  cpm_los_days_source,
  CASE
    WHEN gland_weight_final_g IS NULL THEN 'unknown'
    WHEN gland_weight_final_g >= 200 THEN 'massive_200g_plus'
    WHEN gland_weight_final_g >= 50 THEN 'moderate_50_to_199g'
    ELSE 'small_under_50g'
  END AS weight_bucket,
  (gland_weight_final_g >= 200) AS is_massive_goiter
FROM main.canonical_patient_master;
