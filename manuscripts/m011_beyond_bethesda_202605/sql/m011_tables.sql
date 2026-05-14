-- =====================================================================
-- M011 STEP 7 — Descriptive and risk tables (Tables 1, 2, 3, 6, 7)
-- Tables 4 (sequential model performance) and 5 (Bethesda III/IV subgroup)
-- are assembled from m011_model_metrics + m011_delta_auc + m011_threshold_metrics.
-- =====================================================================

-- Table 1. Cohort characteristics by Bethesda category (primary cohort)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tbl1_characteristics` AS
SELECT CAST(bethesda_highest AS STRING) bethesda, COUNT(*) n_patients,
  ROUND(AVG(age_at_surgery),1) mean_age, ROUND(STDDEV(age_at_surgery),1) sd_age,
  ROUND(100*SAFE_DIVIDE(COUNTIF(sex='female'),COUNT(*)),1) pct_female,
  ROUND(AVG(max_nodule_size_cm),2) mean_nodule_size_cm,
  ROUND(APPROX_QUANTILES(max_nodule_size_cm,2)[OFFSET(1)],2) median_nodule_size_cm,
  ROUND(AVG(acr_imputed_max),2) mean_acr_tirads, COUNTIF(acr_imputed_max>=4) n_acr_tr4_tr5,
  ROUND(100*SAFE_DIVIDE(COUNTIF(acr_imputed_max>=4),COUNTIF(acr_imputed_max IS NOT NULL)),1) pct_acr_tr4_tr5,
  COUNTIF(molecular_tested) n_molecular_tested,
  ROUND(100*SAFE_DIVIDE(COUNTIF(molecular_tested),COUNT(*)),1) pct_molecular_tested,
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1) malignancy_pct,
  ROUND(AVG(surgery_year),0) mean_surgery_year, COUNTIF(surg_total_thyroidectomy) n_total_thyroidectomy
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort
GROUP BY bethesda
UNION ALL
SELECT 'ALL', COUNT(*), ROUND(AVG(age_at_surgery),1), ROUND(STDDEV(age_at_surgery),1),
  ROUND(100*SAFE_DIVIDE(COUNTIF(sex='female'),COUNT(*)),1), ROUND(AVG(max_nodule_size_cm),2),
  ROUND(APPROX_QUANTILES(max_nodule_size_cm,2)[OFFSET(1)],2), ROUND(AVG(acr_imputed_max),2),
  COUNTIF(acr_imputed_max>=4), ROUND(100*SAFE_DIVIDE(COUNTIF(acr_imputed_max>=4),COUNTIF(acr_imputed_max IS NOT NULL)),1),
  COUNTIF(molecular_tested), ROUND(100*SAFE_DIVIDE(COUNTIF(molecular_tested),COUNT(*)),1),
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1),
  ROUND(AVG(surgery_year),0), COUNTIF(surg_total_thyroidectomy)
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort;

-- Table 2. Pathology outcomes by Bethesda category
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tbl2_path_by_bethesda` AS
SELECT bethesda_highest bethesda, COUNT(*) n_patients,
  COUNTIF(final_path_class='benign') n_benign, COUNTIF(final_path_class='malignant') n_malignant,
  COUNTIF(final_path_class='NIFTP') n_niftp, COUNTIF(final_path_class='borderline') n_borderline,
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1) malignancy_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(clin_sig_malignancy=1),COUNTIF(clin_sig_malignancy IS NOT NULL)),1) clinsig_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(final_path_class='NIFTP'),COUNT(*)),1) niftp_pct,
  COUNTIF(incidental_ptmc_flag=1) n_incidental_ptmc, COUNTIF(histology_group='PTC') n_ptc,
  COUNTIF(histology_group='FTC') n_ftc, COUNTIF(histology_group='MTC') n_mtc,
  COUNTIF(histology_group IN ('ATC','PDTC')) n_atc_pdtc, COUNTIF(aggressive_feature_flag) n_aggressive
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort
GROUP BY bethesda ORDER BY bethesda;

-- Table 3. Bethesda x ACR TI-RADS malignancy-risk heat table
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tbl3_beth_tirads_heat` AS
SELECT bethesda_highest bethesda, acr_imputed_max acr_tirads, COUNT(*) n,
  COUNTIF(any_malignancy=1) n_malignant,
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1) malignancy_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(clin_sig_malignancy=1),COUNTIF(clin_sig_malignancy IS NOT NULL)),1) clinsig_pct
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`
WHERE in_primary_cohort AND acr_imputed_max IS NOT NULL
GROUP BY bethesda, acr_tirads ORDER BY bethesda, acr_tirads;

-- Table 6. Combined risk groups (Bethesda III/IV x ACR low/high x molecular)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tbl6_combined_risk` AS
SELECT
  CASE WHEN bethesda_highest=3 THEN 'Beth III' WHEN bethesda_highest=4 THEN 'Beth IV' END bethesda_group,
  CASE WHEN acr_imputed_max>=4 THEN 'high TR4-5' WHEN acr_imputed_max BETWEEN 1 AND 3 THEN 'low TR1-3' ELSE 'TIRADS missing' END tirads_group,
  CASE WHEN NOT molecular_tested THEN 'not tested' WHEN molecular_positive=1 THEN 'molec positive' ELSE 'molec negative' END molecular_group,
  COUNT(*) n, COUNTIF(any_malignancy=1) n_malignant,
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1) malignancy_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(clin_sig_malignancy=1),COUNTIF(clin_sig_malignancy IS NOT NULL)),1) clinsig_pct,
  COUNTIF(final_path_class='NIFTP') n_niftp
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`
WHERE in_primary_cohort AND bethesda_highest IN (3,4)
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Table 7. Molecular tested vs not tested (selection-bias comparison)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tbl7_molecular_selection` AS
SELECT CASE WHEN bethesda_highest IN (3,4) THEN 'Beth III/IV' ELSE 'Beth I/II/V/VI' END bethesda_group,
  molecular_tested, COUNT(*) n, ROUND(AVG(age_at_surgery),1) mean_age,
  ROUND(100*SAFE_DIVIDE(COUNTIF(sex='female'),COUNT(*)),1) pct_female,
  ROUND(AVG(max_nodule_size_cm),2) mean_size,
  ROUND(100*SAFE_DIVIDE(COUNTIF(acr_imputed_max>=4),COUNTIF(acr_imputed_max IS NOT NULL)),1) pct_acr_high,
  ROUND(100*SAFE_DIVIDE(COUNTIF(any_malignancy=1),COUNTIF(any_malignancy IS NOT NULL)),1) malignancy_pct,
  ROUND(100*SAFE_DIVIDE(COUNTIF(clin_sig_malignancy=1),COUNTIF(clin_sig_malignancy IS NOT NULL)),1) clinsig_pct
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort
GROUP BY 1,2 ORDER BY 1,2;
