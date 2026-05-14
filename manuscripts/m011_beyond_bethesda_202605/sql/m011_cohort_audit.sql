-- =====================================================================
-- M011 STEP 5 — Cohort audit, missingness, linkage-confidence tiers
-- =====================================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_cohort_audit` AS
WITH fb AS (SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b`),
pc AS (SELECT * FROM fb WHERE in_primary_cohort)
SELECT * FROM UNNEST([
  STRUCT('01_registry_total_patients' AS metric, (SELECT COUNT(*) FROM fb) AS n, CAST(NULL AS STRING) AS detail),
  ('02_has_preop_fna_bethesda', (SELECT COUNTIF(has_preop_bethesda) FROM fb), 'preoperative FNA with mappable Bethesda'),
  ('03_has_preop_us', (SELECT COUNTIF(has_preop_us) FROM fb), 'preoperative ultrasound nodule before surgery'),
  ('04_has_surgery_date', (SELECT COUNTIF(surgery_date IS NOT NULL) FROM fb), NULL),
  ('05_PRIMARY_COHORT', (SELECT COUNT(*) FROM pc), 'preop US + preop Bethesda + surgical pathology'),
  ('06_primary_with_acr_tirads', (SELECT COUNTIF(has_acr_tirads) FROM pc), 'ACR TI-RADS imputed available'),
  ('07_primary_bethesda_III_IV', (SELECT COUNTIF(bethesda_iii_iv_flag=1) FROM pc), NULL),
  ('08_primary_bethesda_III', (SELECT COUNTIF(bethesda_highest=3) FROM pc), NULL),
  ('09_primary_bethesda_IV', (SELECT COUNTIF(bethesda_highest=4) FROM pc), NULL),
  ('10_primary_molecular_tested', (SELECT COUNTIF(molecular_tested) FROM pc), 'Afirma or ThyroSeq, preoperative'),
  ('11_primary_molecular_tested_III_IV', (SELECT COUNTIF(molecular_tested AND bethesda_iii_iv_flag=1) FROM pc), NULL),
  ('12_primary_any_malignancy', (SELECT COUNTIF(any_malignancy=1) FROM pc), NULL),
  ('13_primary_benign', (SELECT COUNTIF(any_malignancy=0) FROM pc), NULL),
  ('14_primary_NIFTP', (SELECT COUNTIF(final_path_class='NIFTP') FROM pc), 'handled separately'),
  ('15_primary_borderline_FTUMP', (SELECT COUNTIF(final_path_class='borderline') FROM pc), 'handled separately'),
  ('16_primary_clin_sig_malignancy', (SELECT COUNTIF(clin_sig_malignancy=1) FROM pc), NULL),
  ('17_primary_incidental_PTMC', (SELECT COUNTIF(incidental_ptmc_flag=1) FROM pc), NULL),
  ('20_frameA_link_rows', (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary`), 'one row per linked US-nodule/FNA pair'),
  ('21_frameA_cohort_rows', (SELECT COUNTIF(in_frame_a_cohort) FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary`), 'preop US+FNA, Bethesda present'),
  ('22_frameA_hiconf_rows', (SELECT COUNTIF(in_frame_a_hiconf) FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary`), 'linkage tier exact/high/plausible'),
  ('23_frameA_hiconf_patients', (SELECT COUNT(DISTINCT IF(in_frame_a_hiconf,research_id,NULL)) FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary`), NULL),
  ('30_missing_age', (SELECT COUNTIF(age_at_surgery IS NULL) FROM pc), 'missingness in primary cohort'),
  ('31_missing_sex', (SELECT COUNTIF(sex IS NULL OR sex='') FROM pc), NULL),
  ('32_missing_acr_tirads', (SELECT COUNTIF(acr_imputed_max IS NULL) FROM pc), NULL),
  ('33_missing_acr_strict', (SELECT COUNTIF(acr_strict_max IS NULL) FROM pc), NULL),
  ('34_missing_eu_tirads', (SELECT COUNTIF(eu_max IS NULL) FROM pc), NULL),
  ('35_missing_ata', (SELECT COUNTIF(ata_max IS NULL) FROM pc), NULL),
  ('36_missing_ktirads', (SELECT COUNTIF(ktirads_max IS NULL) FROM pc), NULL),
  ('37_missing_ctirads', (SELECT COUNTIF(ctirads_max IS NULL) FROM pc), NULL),
  ('38_missing_nodule_size', (SELECT COUNTIF(max_nodule_size_cm IS NULL) FROM pc), NULL),
  ('39_missing_path_tumor_size', (SELECT COUNTIF(path_tumor_size_cm IS NULL AND any_malignancy=1) FROM pc), 'among malignant'),
  ('40_not_molecular_tested', (SELECT COUNTIF(NOT molecular_tested) FROM pc), NULL),
  ('50_linktier_exact', (SELECT COUNTIF(linkage_confidence_tier='exact_match') FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` WHERE in_frame_a_cohort), 'Frame A linkage tiers (cohort rows)'),
  ('51_linktier_high', (SELECT COUNTIF(linkage_confidence_tier='high_confidence') FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` WHERE in_frame_a_cohort), NULL),
  ('52_linktier_plausible', (SELECT COUNTIF(linkage_confidence_tier='plausible') FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` WHERE in_frame_a_cohort), NULL),
  ('53_linktier_weak', (SELECT COUNTIF(linkage_confidence_tier='weak') FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` WHERE in_frame_a_cohort), NULL)
])
ORDER BY metric;
