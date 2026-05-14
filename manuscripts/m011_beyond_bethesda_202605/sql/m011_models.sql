-- =====================================================================
-- M011 STEP 6 — Modeling dataset, BigQuery ML models, predictions, metrics
-- Outcome: any malignancy on final surgical pathology (NIFTP & FTUMP excluded
-- from the primary outcome; NIFTP 3-way columns retained for sensitivity).
-- All models LOGISTIC_REG, data_split_method='NO_SPLIT' (apparent performance;
-- bootstrap optimism correction is performed in scripts/m011_advanced_stats.R).
-- =====================================================================

-- 6a. Modeling dataset (primary cohort, outcome non-null)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` AS
SELECT
  research_id,
  any_malignancy AS label,
  any_malignancy_niftp_malig, any_malignancy_niftp_benign, clin_sig_malignancy,
  CAST(bethesda_highest AS STRING) AS beth_cat, bethesda_highest AS beth_ord, bethesda_iii_iv_flag,
  CONCAT('TR', CAST(acr_imputed_max AS STRING)) AS acr_cat, acr_imputed_max AS acr_ord, acr_pts_imputed_max AS acr_pts,
  eu_max, ata_max, ktirads_max, ctirads_max,
  age_at_surgery, sex,
  COALESCE(max_nodule_size_cm, path_tumor_size_cm) AS nodule_size_cm, surgery_year,
  CAST(feat_taller_than_wide AS INT64) AS f_taller,
  CAST(feat_marked_hypoechoic AS INT64) AS f_marked_hypo,
  CAST(feat_microcalcifications AS INT64) AS f_microcalc,
  CAST(feat_suspicious_ln AS INT64) AS f_susp_ln,
  CAST(feat_irregular_margin AS INT64) AS f_irreg_margin,
  CAST(feat_solid_composition AS INT64) AS f_solid,
  CAST(feat_ete_on_us AS INT64) AS f_ete,
  molecular_tested, CAST(molecular_positive AS INT64) AS mol_positive, molecular_result_3level,
  (bethesda_highest IS NOT NULL AND acr_imputed_max IS NOT NULL AND age_at_surgery IS NOT NULL
   AND sex IS NOT NULL AND any_malignancy IS NOT NULL) AS cc_main,
  (bethesda_highest IS NOT NULL AND any_malignancy IS NOT NULL AND molecular_tested) AS cc_molec
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`
WHERE in_primary_cohort AND any_malignancy IS NOT NULL;

-- 6b. Sequential models (A-G) on complete-case main set; F/G on molecular subset
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_A_beth`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_B_tirads`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, acr_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_C_beth_tirads`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, acr_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_D_beth_tirads_clin`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, acr_cat, age_at_surgery, sex, nodule_size_cm, surgery_year
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_E_beth_feats`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, f_taller, f_marked_hypo, f_microcalc, f_susp_ln, f_irreg_margin, f_solid, f_ete, nodule_size_cm
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_F_beth_tirads_mol`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, acr_cat, mol_positive, age_at_surgery, sex, nodule_size_cm
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND molecular_tested;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_G_beth_feats_mol`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, f_taller, f_marked_hypo, f_microcalc, f_susp_ln, f_irreg_margin, f_solid, f_ete, mol_positive
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND molecular_tested;
-- molecular-cohort reference models (for clean delta-AUC chain F0 -> F1 -> F)
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_F0_beth_molcohort`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND molecular_tested;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_mdl_F1_beth_tirads_molcohort`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat, acr_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND molecular_tested;

-- 6c. Bethesda III/IV subgroup models
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_beth_ref`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, beth_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND bethesda_iii_iv_flag=1;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_tirads`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, acr_cat FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND bethesda_iii_iv_flag=1;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_feats`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, f_taller, f_marked_hypo, f_microcalc, f_susp_ln, f_irreg_margin, f_solid, f_ete, nodule_size_cm
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND bethesda_iii_iv_flag=1;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_mol`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, mol_positive FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`
  WHERE cc_main AND bethesda_iii_iv_flag=1 AND molecular_tested;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_tirads_mol`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, acr_cat, mol_positive FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data`
  WHERE cc_main AND bethesda_iii_iv_flag=1 AND molecular_tested;
CREATE OR REPLACE MODEL `thyroid-canonical-pub-2026.pub_workspace.m011_sub_combined`
  OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT') AS
  SELECT label, acr_cat, f_susp_ln, f_microcalc, f_irreg_margin, mol_positive
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_data` WHERE cc_main AND bethesda_iii_iv_flag=1 AND molecular_tested;

-- 6d. Predictions (probability of label=1) for every model -> m011_predictions
--     (See repo: long table with columns model, cohort, research_id, label, prob.
--      Built with ML.PREDICT over the same filtered rows used to train each model;
--      reference models F0/F1/SUB_beth appended via INSERT.)

-- 6e. Discrimination metrics: rank-based AUC (Mann-Whitney, average-rank tie handling)
--     + Hanley-McNeil analytic 95% CI + Brier score + calibration-in-the-large.
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_model_metrics` AS
WITH ranked AS (
  SELECT model, cohort, label, prob,
    RANK() OVER (PARTITION BY model ORDER BY prob) AS r_min,
    COUNT(*) OVER (PARTITION BY model, CAST(prob AS STRING)) AS tie_cnt
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions`
),
withavg AS (SELECT model, cohort, label, (r_min + (tie_cnt-1)/2.0) AS avg_rank FROM ranked),
auc AS (
  SELECT model, ANY_VALUE(cohort) cohort, COUNT(*) n, SUM(label) n_pos, COUNTIF(label=0) n_neg,
    SAFE_DIVIDE(SUM(IF(label=1,avg_rank,0)) - SUM(label)*(SUM(label)+1)/2.0, SUM(label)*COUNTIF(label=0)) AS auc
  FROM withavg GROUP BY model
),
brier AS (SELECT model, AVG(POW(prob-label,2)) brier, AVG(prob) mean_pred, AVG(label) obs_rate
          FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions` GROUP BY model),
se AS (
  SELECT a.*, b.brier, b.mean_pred, b.obs_rate,
    SQRT( (a.auc*(1-a.auc)
           + (a.n_pos-1)*(a.auc/(2-a.auc)-a.auc*a.auc)
           + (a.n_neg-1)*(2*a.auc*a.auc/(1+a.auc)-a.auc*a.auc)) / (a.n_pos*a.n_neg) ) AS se_auc
  FROM auc a JOIN brier b USING(model)
)
SELECT model, cohort, n, n_pos, n_neg,
  ROUND(auc,4) auc, ROUND(se_auc,4) se_auc,
  ROUND(GREATEST(auc-1.96*se_auc,0),4) auc_ci_lo, ROUND(LEAST(auc+1.96*se_auc,1),4) auc_ci_hi,
  ROUND(brier,4) brier, ROUND(mean_pred,4) mean_pred, ROUND(obs_rate,4) obs_rate
FROM se ORDER BY cohort, model;

-- 6f. Threshold metrics (sens/spec/PPV/NPV/FNR/FPR + decision-curve net benefit), 0-1 grid step 0.01
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_threshold_metrics` AS
WITH p AS (SELECT model, cohort, label, prob FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions`),
g AS (SELECT thr FROM UNNEST(GENERATE_ARRAY(0,100,1)) AS t, UNNEST([t/100.0]) AS thr),
mt AS (
  SELECT p.model, p.cohort, g.thr,
    COUNTIF(prob>=g.thr AND label=1) tp, COUNTIF(prob>=g.thr AND label=0) fp,
    COUNTIF(prob< g.thr AND label=0) tn, COUNTIF(prob< g.thr AND label=1) fn,
    COUNT(*) n, SUM(label) n_pos
  FROM p CROSS JOIN g GROUP BY 1,2,3
)
SELECT model, cohort, thr, tp, fp, tn, fn, n, n_pos,
  ROUND(SAFE_DIVIDE(tp,tp+fn),4) sensitivity, ROUND(SAFE_DIVIDE(tn,tn+fp),4) specificity,
  ROUND(SAFE_DIVIDE(tp,tp+fp),4) ppv, ROUND(SAFE_DIVIDE(tn,tn+fn),4) npv,
  ROUND(SAFE_DIVIDE(fn,tp+fn),4) false_negative_rate, ROUND(SAFE_DIVIDE(fp,fp+tn),4) false_positive_rate,
  ROUND(SAFE_DIVIDE(tp,n) - SAFE_DIVIDE(fp,n)*SAFE_DIVIDE(thr,NULLIF(1-thr,0)),5) net_benefit
FROM mt ORDER BY model, thr;

-- 6g. Calibration deciles
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_calibration_bins` AS
WITH b AS (SELECT model, cohort, label, prob, NTILE(10) OVER (PARTITION BY model ORDER BY prob) decile
           FROM `thyroid-canonical-pub-2026.pub_workspace.m011_predictions`)
SELECT model, cohort, decile, COUNT(*) n, ROUND(AVG(prob),4) mean_predicted,
  ROUND(AVG(label),4) observed_rate, ROUND(AVG(label)-AVG(prob),4) calib_gap
FROM b GROUP BY 1,2,3 ORDER BY model, decile;

-- 6h. Delta AUC vs reference (Hanley-McNeil approximate SE for the difference; DeLong in R script)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_delta_auc` AS
WITH m AS (SELECT model, cohort, n, auc, SAFE_DIVIDE(auc_ci_hi-auc_ci_lo,2*1.96) se_auc
           FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_metrics`)
SELECT x.model, x.cohort, x.n, x.auc, r.model ref_model, r.auc ref_auc,
  ROUND(x.auc-r.auc,4) delta_auc,
  ROUND(SQRT(x.se_auc*x.se_auc + r.se_auc*r.se_auc),4) se_delta_approx,
  ROUND((x.auc-r.auc)/NULLIF(SQRT(x.se_auc*x.se_auc+r.se_auc*r.se_auc),0),2) z_approx
FROM m x JOIN m r ON (
  (x.cohort='main' AND r.model='A_Bethesda_only')
  OR (x.cohort='molecular' AND x.model IN ('F_Bethesda_TIRADS_molecular','F1_Bethesda_TIRADS_molcohort') AND r.model='F0_Bethesda_only_molcohort')
  OR (x.cohort='beth_III_IV' AND x.model IN ('SUB_TIRADS_only','SUB_USfeatures') AND r.model='SUB_Bethesda_ref'))
WHERE x.model <> r.model
ORDER BY x.cohort, x.auc DESC;
