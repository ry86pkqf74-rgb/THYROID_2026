-- =============================================================================
-- mig_087_bqml_boosted_models.sql
-- Prompt 5 — Tier 3.B: BOOSTED_TREE_CLASSIFIER vs logistic baseline
-- DFL: DFL-20260506-087
-- Purpose: Train recurrence_5y_boosted_v1 with same cohort/features as
--   recurrence_5y_baseline_v1 for direct AUC comparison.
-- Applied: 2026-05-06
-- =============================================================================

-- STEP 1: Train the boosted tree model
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.recurrence_5y_boosted_v1`
OPTIONS(
  model_type            = 'BOOSTED_TREE_CLASSIFIER',
  input_label_cols      = ['recurrence_5y'],
  num_parallel_tree     = 8,
  max_iterations        = 50,
  early_stop            = TRUE,
  enable_global_explain = TRUE,
  data_split_method     = 'RANDOM',
  data_split_eval_fraction = 0.2,
  auto_class_weights    = TRUE
) AS
SELECT
  cpm.age_at_surgery,
  cpm.sex,
  cpm.histology_final,
  cpm.ata_risk_category,
  cpm.ajcc8_stage_group,
  CAST(cpm.braf_positive AS INT64)         AS braf_positive,
  cpm.ete_grade_final_v2                   AS ete_grade_final,
  cpm.ln_positive_final,
  cpm.tumor_size_cm_dominant               AS tumor_size_cm_dominant,
  CAST(cpm.multifocal_flag_path AS INT64)  AS multifocal_flag,
  CASE
    WHEN cpm.braf_positive IS NOT NULL
      OR cpm.tert_positive IS NOT NULL THEN 1
    ELSE 0
  END                                      AS molecular_tested,
  CAST(cpm.any_recurrence_flag AS INT64)   AS recurrence_5y
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` cpm
WHERE cpm.is_malignant = TRUE
  AND (
    CAST(cpm.any_recurrence_flag AS BOOL) = TRUE
    OR (cpm.followup_years >= 5 AND cpm.followup_years IS NOT NULL)
  )
;

-- STEP 2: Evaluation (run after training completes)
-- SELECT * FROM ML.EVALUATE(MODEL `thyroid-canonical-pub-2026.pub_workspace.recurrence_5y_boosted_v1`);

-- STEP 3: ROC Curve
-- SELECT * FROM ML.ROC_CURVE(MODEL `thyroid-canonical-pub-2026.pub_workspace.recurrence_5y_boosted_v1`);

-- STEP 4: Global feature importance
-- SELECT * FROM ML.GLOBAL_EXPLAIN(
--   MODEL `thyroid-canonical-pub-2026.pub_workspace.recurrence_5y_boosted_v1`,
--   STRUCT(TRUE AS class_level_explain)
-- );

-- STEP 5: QC assertion — boosted AUC must be >= logistic baseline AUC
-- This is run in qc_runner.py after model training; assertion SQL:
-- SELECT
--   CASE WHEN boosted_auc >= baseline_auc THEN 'PASS' ELSE 'WARN' END AS assertion_result,
--   boosted_auc, baseline_auc
-- FROM (
--   SELECT
--     (SELECT auc FROM `pub_workspace.bqml_eval_log_v1` WHERE model_id='recurrence_5y_boosted_v1' ORDER BY trained_at DESC LIMIT 1) AS boosted_auc,
--     (SELECT auc FROM `pub_workspace.bqml_eval_log_v1` WHERE model_id='recurrence_5y_baseline_v1' ORDER BY trained_at DESC LIMIT 1) AS baseline_auc
-- )
;
