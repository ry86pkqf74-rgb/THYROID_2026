-- =====================================================================
-- M011 — Sensitivity analyses: NIFTP three-way + clinically-significant malignancy
-- Re-runs the sequential model sequence (A/C/D/E) with alternate outcomes.
-- Run after m011_models.sql.
-- =====================================================================

-- NIFTP-inclusive modeling dataset (the primary m011_model_data EXCLUDES NIFTP/borderline;
-- the NIFTP three-way analysis must include them, so build a separate table).
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_model_data_niftp` AS
SELECT
  research_id, final_path_class,
  any_malignancy_niftp_malig, any_malignancy_niftp_benign,
  CAST(bethesda_highest AS STRING) AS beth_cat,
  CONCAT('TR', CAST(acr_imputed_max AS STRING)) AS acr_cat, acr_imputed_max AS acr_ord,
  age_at_surgery, sex,
  COALESCE(max_nodule_size_cm, path_tumor_size_cm) AS nodule_size_cm, surgery_year,
  CAST(feat_taller_than_wide AS INT64) AS f_taller, CAST(feat_marked_hypoechoic AS INT64) AS f_marked_hypo,
  CAST(feat_microcalcifications AS INT64) AS f_microcalc, CAST(feat_suspicious_ln AS INT64) AS f_susp_ln,
  CAST(feat_irregular_margin AS INT64) AS f_irreg_margin, CAST(feat_solid_composition AS INT64) AS f_solid,
  CAST(feat_ete_on_us AS INT64) AS f_ete,
  (bethesda_highest IS NOT NULL AND acr_imputed_max IS NOT NULL AND age_at_surgery IS NOT NULL AND sex IS NOT NULL) AS cc_main
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`
WHERE in_primary_cohort;  -- includes benign, malignant, NIFTP, borderline

-- Clinically-significant-malignancy models (cs_*) train on m011_model_data WHERE cc_main AND clin_sig_malignancy IS NOT NULL.
-- NIFTP-as-malignant models (nm_*) and NIFTP-as-benign models (nb_*) train on m011_model_data_niftp WHERE cc_main.
-- For each outcome, train A (beth_cat), C (beth_cat+acr_cat),
--   D (beth_cat+acr_cat+age+sex+nodule_size+surgery_year), E (beth_cat + 7 US-feature flags + nodule_size).
-- Pattern identical to m011_models.sql step 6b; example:
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_cs_C_beth_tirads`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT clin_sig_malignancy AS label, beth_cat, acr_cat
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND clin_sig_malignancy IS NOT NULL;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_nm_C_beth_tirads`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT any_malignancy_niftp_malig AS label, beth_cat, acr_cat
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data_niftp` WHERE cc_main;
-- (... train cs_A/D/E, nm_A/D/E, nb_A/C/D/E analogously ...)

-- Predictions + rank-based AUC -> m011_sensitivity_predictions, m011_sensitivity_metrics
-- (same ML.PREDICT + average-rank Mann-Whitney AUC pattern as m011_models.sql steps 6d-6e,
--  partitioned by (outcome, model); see repo for the full UNION).
-- Paired DeLong tests for C/D/E vs A within each outcome: scripts/m011_sensitivity_delong.py
-- Output: tables/m011_sensitivity_analyses.csv, tables/m011_sensitivity_delong.csv
