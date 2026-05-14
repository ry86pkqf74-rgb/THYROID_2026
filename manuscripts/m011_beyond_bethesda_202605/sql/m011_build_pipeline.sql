-- =====================================================================
-- M011 "Beyond Bethesda?" — BigQuery build pipeline
-- Project: thyroid-canonical-pub-2026   Workspace dataset: pub_workspace
-- Incremental value of TI-RADS, US morphology, and molecular testing
-- AFTER Bethesda cytology in surgically managed thyroid nodules.
-- Run top-to-bottom. All objects are prefixed m011_.
-- =====================================================================

-- ---------------------------------------------------------------------
-- STEP 1. Patient base + outcome definitions (Frame B spine)
-- Source: pub_canonical.manuscript_cohort_v1 (10,871 surgical patients)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base` AS
WITH mc AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    age_at_surgery,
    LOWER(TRIM(COALESCE(demo_sex_final, sex))) AS sex,
    COALESCE(demo_race_final, race) AS race,
    DATE(COALESCE(surgery_date, first_surgery_date, surg_first_date)) AS surgery_date,
    surg_procedure_type, surg_total_thyroidectomy, surg_hemithyroidectomy,
    path_tumor_size_cm, path_ete_final, vascular_invasion_final, ln_positive_final,
    margin_status_final, path_multifocal_final, histology_final,
    LOWER(TRIM(REGEXP_REPLACE(COALESCE(histology_final,''), r'[\n*]+', ' '))) AS hist_norm
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
),
classed AS (
  SELECT *,
    CASE
      WHEN hist_norm = '' THEN 'benign'
      WHEN hist_norm LIKE '%niftp%' THEN 'NIFTP'
      WHEN hist_norm LIKE '%ftump%' OR hist_norm LIKE '%tumor of uncertain malignant potential%'
           OR hist_norm LIKE '%atypical follicular adenoma%' OR hist_norm LIKE '%atypical hurthle cell neoplasm%' THEN 'borderline'
      WHEN hist_norm LIKE '%follicular adenoma%' THEN 'benign'
      ELSE 'malignant'
    END AS final_path_class,
    CASE
      WHEN hist_norm LIKE '%anaplastic%' THEN 'ATC'
      WHEN hist_norm LIKE '%poorly differentiated%' OR hist_norm LIKE '%high grade%' OR hist_norm LIKE '%high-grade%' THEN 'PDTC'
      WHEN hist_norm LIKE '%medullary%' OR hist_norm LIKE '%mtc%' THEN 'MTC'
      WHEN hist_norm LIKE '%hurthle%' AND hist_norm LIKE '%carcinoma%' THEN 'Hurthle'
      WHEN hist_norm LIKE '%follicular carcinoma%' THEN 'FTC'
      WHEN hist_norm LIKE '%ptc%' OR hist_norm LIKE '%papillary%' OR hist_norm = 'differentiated thyroid carcinoma' THEN 'PTC'
      WHEN hist_norm = '' OR hist_norm LIKE '%adenoma%' THEN 'benign'
      WHEN hist_norm LIKE '%niftp%' THEN 'NIFTP'
      WHEN hist_norm LIKE '%ftump%' OR hist_norm LIKE '%uncertain malignant%' THEN 'borderline'
      ELSE 'Other malignant'
    END AS histology_group
  FROM mc
)
SELECT c.*, EXTRACT(YEAR FROM surgery_date) AS surgery_year,
  ( COALESCE(LOWER(path_ete_final) LIKE '%gross%' OR LOWER(path_ete_final) LIKE '%extensive%' OR LOWER(path_ete_final) LIKE '%t4%', FALSE)
    OR COALESCE(LOWER(vascular_invasion_final) LIKE '%present%' OR LOWER(vascular_invasion_final) LIKE '%angio%' OR LOWER(vascular_invasion_final) LIKE '%extensive%', FALSE)
    OR COALESCE(ln_positive_final > 0, FALSE)
    OR COALESCE(LOWER(margin_status_final) LIKE '%positive%', FALSE)
    OR histology_group IN ('ATC','PDTC','MTC','Hurthle','Other malignant') ) AS aggressive_feature_flag,
  CASE WHEN final_path_class='malignant' THEN 1 WHEN final_path_class='benign' THEN 0 ELSE NULL END AS any_malignancy,
  CASE WHEN final_path_class='malignant' THEN 1 WHEN final_path_class='benign' THEN 0
       WHEN final_path_class='NIFTP' THEN 1 WHEN final_path_class='borderline' THEN 0 END AS any_malignancy_niftp_malig,
  CASE WHEN final_path_class='malignant' THEN 1 WHEN final_path_class IN ('benign','NIFTP','borderline') THEN 0 END AS any_malignancy_niftp_benign,
  CASE WHEN final_path_class='malignant' AND (
            COALESCE(path_tumor_size_cm>1.0,FALSE)
            OR COALESCE(LOWER(path_ete_final) LIKE '%gross%' OR LOWER(path_ete_final) LIKE '%extensive%' OR LOWER(path_ete_final) LIKE '%t4%',FALSE)
            OR COALESCE(LOWER(vascular_invasion_final) LIKE '%present%' OR LOWER(vascular_invasion_final) LIKE '%angio%' OR LOWER(vascular_invasion_final) LIKE '%extensive%',FALSE)
            OR COALESCE(ln_positive_final>0,FALSE)
            OR COALESCE(LOWER(margin_status_final) LIKE '%positive%',FALSE)
            OR histology_group IN ('ATC','PDTC','MTC','Hurthle') ) THEN 1
       WHEN final_path_class='malignant' THEN 0 WHEN final_path_class='benign' THEN 0 ELSE NULL END AS clin_sig_malignancy,
  CASE WHEN histology_group='PTC' AND COALESCE(path_tumor_size_cm<=1.0,FALSE)
            AND NOT (COALESCE(LOWER(path_ete_final) LIKE '%gross%',FALSE)
                     OR COALESCE(ln_positive_final>0,FALSE)
                     OR COALESCE(LOWER(margin_status_final) LIKE '%positive%',FALSE))
       THEN 1 ELSE 0 END AS incidental_ptmc_flag
FROM classed c;

-- ---------------------------------------------------------------------
-- STEP 2. Patient-level preoperative predictor rollups
-- ---------------------------------------------------------------------
-- 2a. Bethesda (first / last / highest among preoperative FNAs)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_beth_rollup` AS
WITH pb AS (SELECT research_id, surgery_date FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`),
fna AS (
  SELECT f.research_id, f.fna_event_id, f.fna_date_resolved, f.days_to_surgery, f.bethesda_final_num
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_fna_events_v1` f
  JOIN pb USING(research_id)
  WHERE f.bethesda_final_num BETWEEN 1 AND 6
    AND ( (f.fna_date_resolved IS NOT NULL AND pb.surgery_date IS NOT NULL AND f.fna_date_resolved <= pb.surgery_date)
          OR (f.fna_date_resolved IS NULL AND f.days_to_surgery > 0)
          OR (pb.surgery_date IS NULL AND f.fna_date_resolved IS NOT NULL) )
),
ord AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved ASC NULLS LAST, fna_event_id ASC) rn_first,
    ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved DESC NULLS LAST, fna_event_id DESC) rn_last
  FROM fna
)
SELECT research_id, COUNT(*) n_preop_fna,
  MAX(bethesda_final_num) bethesda_highest,
  MAX(IF(rn_first=1,bethesda_final_num,NULL)) bethesda_first,
  MAX(IF(rn_last=1,bethesda_final_num,NULL)) bethesda_last,
  MIN(fna_date_resolved) first_fna_date, MAX(fna_date_resolved) last_fna_date
FROM ord GROUP BY research_id;

-- 2b. TI-RADS multisystem rollup (max-risk per patient over preoperative US exams)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_tirads_rollup` AS
WITH pb AS (SELECT research_id, surgery_date FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`),
t AS (
  SELECT m.research_id, m.us_exam_id, m.exam_date, m.size_cm_max,
    SAFE_CAST(REGEXP_EXTRACT(m.acr2017_category_imputed,r'(\d)') AS INT64) acr_imp_ord,
    SAFE_CAST(REGEXP_EXTRACT(m.acr2017_category_strict, r'(\d)') AS INT64) acr_strict_ord,
    m.acr2017_total_pts_imputed acr_pts_imp, m.acr2017_total_pts_strict acr_pts_strict,
    SAFE_CAST(REGEXP_EXTRACT(m.eutirads_category,r'(\d)') AS INT64) eu_ord,
    CASE m.ata_pattern WHEN 'benign' THEN 0 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
         WHEN 'intermediate' THEN 3 WHEN 'high' THEN 4 END ata_ord,
    SAFE_CAST(m.ktirads_category AS INT64) k_ord,
    CASE m.ctirads_category WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4A' THEN 4 WHEN '4B' THEN 5 WHEN '4C' THEN 6 END c_ord
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
  JOIN pb USING(research_id)
  WHERE (pb.surgery_date IS NULL OR m.exam_date <= pb.surgery_date)
)
SELECT research_id, COUNT(*) n_preop_us_nodules, COUNT(DISTINCT us_exam_id) n_preop_us_exams,
  MIN(exam_date) first_us_date, MAX(exam_date) last_us_date, MAX(size_cm_max) max_nodule_size_cm,
  MAX(acr_imp_ord) acr_imputed_max, MAX(acr_strict_ord) acr_strict_max,
  MAX(acr_pts_imp) acr_pts_imputed_max, MAX(acr_pts_strict) acr_pts_strict_max,
  MAX(eu_ord) eu_max, MAX(ata_ord) ata_max, MAX(k_ord) ktirads_max, MAX(c_ord) ctirads_max,
  COUNTIF(acr_imp_ord IS NOT NULL) n_nodules_acr_imp_scored
FROM t GROUP BY research_id;

-- 2c. Molecular rollup (Afirma/ThyroSeq only, preoperative)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_molec_rollup` AS
WITH pb AS (SELECT research_id, surgery_date FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`),
m AS (
  SELECT e.research_id, e.platform, e.overall_result_class,
    e.braf_flag, e.ras_flag, e.ret_flag, e.ret_fusion_flag, e.tert_flag, e.ntrk_flag,
    e.alk_flag, e.high_risk_marker_flag, e.inadequate_flag, DATE(e.test_date_native) test_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.molecular_test_episode_v2` e
  JOIN pb USING(research_id)
  WHERE e.platform IN ('Afirma','ThyroSeq')
    AND (pb.surgery_date IS NULL OR DATE(e.test_date_native) IS NULL OR DATE(e.test_date_native) <= pb.surgery_date)
)
SELECT research_id, TRUE molecular_tested, COUNT(*) n_molecular_tests,
  STRING_AGG(DISTINCT platform ORDER BY platform) molecular_platforms,
  LOGICAL_OR(COALESCE(braf_flag,FALSE)) braf_pos,
  LOGICAL_OR(COALESCE(ras_flag,FALSE)) ras_pos,
  LOGICAL_OR(COALESCE(tert_flag,FALSE)) tert_pos,
  LOGICAL_OR(COALESCE(ret_flag,FALSE) OR COALESCE(ret_fusion_flag,FALSE) OR COALESCE(ntrk_flag,FALSE) OR COALESCE(alk_flag,FALSE)) fusion_pos,
  LOGICAL_OR(COALESCE(high_risk_marker_flag,FALSE)) high_risk_marker,
  LOGICAL_OR(COALESCE(inadequate_flag,FALSE)) any_inadequate,
  MIN(test_date) first_molecular_date,
  CASE
    WHEN LOGICAL_OR(COALESCE(braf_flag,FALSE) OR COALESCE(tert_flag,FALSE) OR COALESCE(ret_flag,FALSE)
        OR COALESCE(ret_fusion_flag,FALSE) OR COALESCE(ntrk_flag,FALSE) OR COALESCE(alk_flag,FALSE)
        OR COALESCE(high_risk_marker_flag,FALSE) OR overall_result_class IN ('positive','suspicious')) THEN 'positive_suspicious'
    WHEN LOGICAL_OR(COALESCE(ras_flag,FALSE)) THEN 'ras_only'
    WHEN LOGICAL_OR(overall_result_class='negative') THEN 'negative_benign'
    WHEN LOGICAL_OR(COALESCE(inadequate_flag,FALSE) OR overall_result_class='non_diagnostic') THEN 'inadequate'
    ELSE 'unclassified' END molecular_result_3level,
  CASE WHEN LOGICAL_OR(COALESCE(braf_flag,FALSE) OR COALESCE(ras_flag,FALSE) OR COALESCE(tert_flag,FALSE)
        OR COALESCE(ret_flag,FALSE) OR COALESCE(ret_fusion_flag,FALSE) OR COALESCE(ntrk_flag,FALSE)
        OR COALESCE(alk_flag,FALSE) OR COALESCE(high_risk_marker_flag,FALSE)
        OR overall_result_class IN ('positive','suspicious')) THEN 1 ELSE 0 END molecular_positive
FROM m GROUP BY research_id;

-- 2d. US individual-feature rollup (patient-level max-risk; clean park-feature booleans)
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_usfeat_rollup` AS
WITH pb AS (SELECT research_id, surgery_date FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`),
n AS (
  SELECT m.research_id,
    m.park_x1_taller, m.park_x4_microlobulation, m.park_x5_infiltrative_margin,
    m.park_x6_marked_hypo, m.park_x7_hypo, m.park_x10_solid, m.park_x11_microcalc, m.park_x12_abnormal_ln,
    v.ete_on_us_presence_simple
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
  JOIN pb USING(research_id)
  LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` v ON m.nodule_id = v.nodule_id
  WHERE (pb.surgery_date IS NULL OR m.exam_date <= pb.surgery_date)
)
SELECT research_id,
  LOGICAL_OR(COALESCE(park_x1_taller,FALSE)) feat_taller_than_wide,
  LOGICAL_OR(COALESCE(park_x6_marked_hypo,FALSE)) feat_marked_hypoechoic,
  LOGICAL_OR(COALESCE(park_x7_hypo,FALSE) OR COALESCE(park_x6_marked_hypo,FALSE)) feat_hypoechoic_any,
  LOGICAL_OR(COALESCE(park_x11_microcalc,FALSE)) feat_microcalcifications,
  LOGICAL_OR(COALESCE(park_x12_abnormal_ln,FALSE)) feat_suspicious_ln,
  LOGICAL_OR(COALESCE(park_x4_microlobulation,FALSE) OR COALESCE(park_x5_infiltrative_margin,FALSE)) feat_irregular_margin,
  LOGICAL_OR(COALESCE(park_x10_solid,FALSE)) feat_solid_composition,
  LOGICAL_OR(COALESCE(LOWER(ete_on_us_presence_simple) LIKE '%present%',FALSE)) feat_ete_on_us,
  COUNT(*) n_nodules_feat
FROM n GROUP BY research_id;

-- ---------------------------------------------------------------------
-- STEP 3. FRAME B — patient-level fallback analytic frame
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` AS
SELECT pb.* EXCEPT(hist_norm),
  b.n_preop_fna, b.bethesda_highest, b.bethesda_first, b.bethesda_last, b.first_fna_date, b.last_fna_date,
  CASE WHEN b.bethesda_highest IN (3,4) THEN 1 ELSE 0 END bethesda_iii_iv_flag,
  t.n_preop_us_nodules, t.n_preop_us_exams, t.first_us_date, t.last_us_date, t.max_nodule_size_cm,
  t.acr_imputed_max, t.acr_strict_max, t.acr_pts_imputed_max, t.acr_pts_strict_max,
  t.eu_max, t.ata_max, t.ktirads_max, t.ctirads_max, t.n_nodules_acr_imp_scored,
  COALESCE(mr.molecular_tested,FALSE) molecular_tested, mr.n_molecular_tests, mr.molecular_platforms,
  COALESCE(mr.braf_pos,FALSE) braf_pos, COALESCE(mr.ras_pos,FALSE) ras_pos,
  COALESCE(mr.tert_pos,FALSE) tert_pos, COALESCE(mr.fusion_pos,FALSE) fusion_pos,
  mr.molecular_result_3level, mr.molecular_positive, mr.first_molecular_date,
  uf.feat_taller_than_wide, uf.feat_marked_hypoechoic, uf.feat_hypoechoic_any, uf.feat_microcalcifications,
  uf.feat_suspicious_ln, uf.feat_irregular_margin, uf.feat_solid_composition, uf.feat_ete_on_us,
  (b.bethesda_highest IS NOT NULL AND t.n_preop_us_nodules > 0 AND pb.surgery_date IS NOT NULL) in_primary_cohort,
  (b.bethesda_highest IS NOT NULL) has_preop_bethesda,
  (t.n_preop_us_nodules > 0) has_preop_us,
  (t.acr_imputed_max IS NOT NULL) has_acr_tirads
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base` pb
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.m011_beth_rollup` b USING(research_id)
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.m011_tirads_rollup` t USING(research_id)
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.m011_molec_rollup` mr USING(research_id)
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.m011_usfeat_rollup` uf USING(research_id);

-- ---------------------------------------------------------------------
-- STEP 4. FRAME A — high-confidence nodule-linked analytic frame
-- US nodule -> FNA/Bethesda (via imaging_fna_linkage_v3) -> molecular (patient-level) -> patient pathology
-- Bridge: imaging_fna_linkage_v3.nodule_id -> imaging_nodule_long_v2 -> (research_id, exam_date,
--         nodule_index_within_exam) -> canonical_us_nodule_tirads_multisystem_v1 / canonical_us_nodule_v2
-- NOTE: molecular is joined at PATIENT level because molecular_test_episode_v2.linked_fna_episode_id
--       is a within-patient ordinal (values 1-6), not a global FNA episode key.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a` AS
WITH pb AS (
  SELECT research_id, surgery_date, surgery_year, age_at_surgery, sex,
         final_path_class, histology_group, any_malignancy, any_malignancy_niftp_malig,
         any_malignancy_niftp_benign, clin_sig_malignancy, incidental_ptmc_flag, path_tumor_size_cm
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_patient_base`
),
lnk AS (
  SELECT research_id, nodule_id, fna_episode_id, img_date, fna_date, day_gap,
         linkage_confidence_tier, analysis_eligible_link_flag
  FROM `thyroid-canonical-pub-2026.pub_canonical.imaging_fna_linkage_v3`
  WHERE linkage_confidence_tier <> 'unlinked'
),
inl AS (
  SELECT nodule_id, research_id, resolved_exam_date, nodule_index_within_exam
  FROM `thyroid-canonical-pub-2026.pub_canonical.imaging_nodule_long_v2`
),
fem AS (
  SELECT research_id, fna_episode_id, bethesda_category
  FROM `thyroid-canonical-pub-2026.pub_canonical.fna_episode_master_v2`
  WHERE bethesda_category BETWEEN 1 AND 6
)
SELECT
  lnk.research_id, lnk.nodule_id, lnk.fna_episode_id,
  lnk.img_date, lnk.fna_date, lnk.day_gap, lnk.linkage_confidence_tier, lnk.analysis_eligible_link_flag,
  pb.surgery_date, pb.surgery_year, pb.age_at_surgery, pb.sex,
  (lnk.img_date IS NOT NULL AND (pb.surgery_date IS NULL OR DATE(lnk.img_date) <= pb.surgery_date)) us_preop,
  (lnk.fna_date IS NOT NULL AND (pb.surgery_date IS NULL OR lnk.fna_date <= pb.surgery_date)) fna_preop,
  fem.bethesda_category bethesda,
  CASE WHEN fem.bethesda_category IN (3,4) THEN 1 ELSE 0 END bethesda_iii_iv_flag,
  m.size_cm_max nodule_size_cm,
  SAFE_CAST(REGEXP_EXTRACT(m.acr2017_category_imputed,r'(\d)') AS INT64) acr_imputed,
  SAFE_CAST(REGEXP_EXTRACT(m.acr2017_category_strict, r'(\d)') AS INT64) acr_strict,
  m.acr2017_total_pts_imputed acr_pts_imputed,
  SAFE_CAST(REGEXP_EXTRACT(m.eutirads_category,r'(\d)') AS INT64) eu_cat,
  CASE m.ata_pattern WHEN 'benign' THEN 0 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
       WHEN 'intermediate' THEN 3 WHEN 'high' THEN 4 END ata_cat,
  SAFE_CAST(m.ktirads_category AS INT64) k_cat,
  CASE m.ctirads_category WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4A' THEN 4 WHEN '4B' THEN 5 WHEN '4C' THEN 6 END c_cat,
  COALESCE(m.park_x1_taller,FALSE) feat_taller_than_wide,
  COALESCE(m.park_x6_marked_hypo,FALSE) feat_marked_hypoechoic,
  COALESCE(m.park_x11_microcalc,FALSE) feat_microcalcifications,
  COALESCE(m.park_x12_abnormal_ln,FALSE) feat_suspicious_ln,
  COALESCE(m.park_x4_microlobulation,FALSE) OR COALESCE(m.park_x5_infiltrative_margin,FALSE) feat_irregular_margin,
  COALESCE(m.park_x10_solid,FALSE) feat_solid_composition,
  COALESCE(LOWER(v.ete_on_us_presence_simple) LIKE '%present%',FALSE) feat_ete_on_us,
  pb.final_path_class, pb.histology_group, pb.any_malignancy,
  pb.any_malignancy_niftp_malig, pb.any_malignancy_niftp_benign,
  pb.clin_sig_malignancy, pb.incidental_ptmc_flag, pb.path_tumor_size_cm
FROM lnk
JOIN pb USING(research_id)
JOIN inl ON lnk.nodule_id = inl.nodule_id
LEFT JOIN fem ON lnk.research_id = fem.research_id AND lnk.fna_episode_id = fem.fna_episode_id
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
  ON inl.research_id = m.research_id AND inl.resolved_exam_date = m.exam_date AND inl.nodule_index_within_exam = m.nodule_index_within_exam
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` v
  ON inl.research_id = v.research_id AND inl.resolved_exam_date = v.exam_date AND inl.nodule_index_within_exam = v.nodule_index_within_exam;

-- 4b. Collapse Frame A to one row per (patient, nodule, fna) and attach patient-level molecular
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a_primary` AS
WITH ranked AS (
  SELECT *,
    CASE linkage_confidence_tier WHEN 'exact_match' THEN 1 WHEN 'high_confidence' THEN 2
         WHEN 'plausible' THEN 3 WHEN 'weak' THEN 4 ELSE 5 END tier_rank,
    ROW_NUMBER() OVER (PARTITION BY research_id, nodule_id, fna_episode_id
      ORDER BY CASE linkage_confidence_tier WHEN 'exact_match' THEN 1 WHEN 'high_confidence' THEN 2
                    WHEN 'plausible' THEN 3 WHEN 'weak' THEN 4 ELSE 5 END, acr_imputed DESC NULLS LAST) rn
  FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_a`
)
SELECT r.* EXCEPT(rn),
  COALESCE(mr.molecular_tested,FALSE) molecular_tested,
  CASE WHEN mr.molecular_tested THEN mr.molecular_positive ELSE NULL END molecular_positive,
  mr.molecular_result_3level,
  (us_preop AND fna_preop AND bethesda IS NOT NULL) in_frame_a_cohort,
  (us_preop AND fna_preop AND bethesda IS NOT NULL AND linkage_confidence_tier IN ('exact_match','high_confidence','plausible')) in_frame_a_hiconf
FROM ranked r
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.m011_molec_rollup` mr USING(research_id)
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- STEP 5. Cohort audit (STARD denominators, missingness, linkage tiers)
--   See sql/m011_cohort_audit.sql
-- STEP 6. Modeling dataset, BigQuery ML models, predictions, metrics
--   See sql/m011_models.sql
-- STEP 7. Descriptive + risk tables
--   See sql/m011_tables.sql
-- =====================================================================
