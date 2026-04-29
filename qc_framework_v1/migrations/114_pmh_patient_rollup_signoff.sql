-- =============================================================================
-- Migration 114 -- canonical_pmh_patient_rollup_v1 REBUILD + SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cursor lane 7)
-- Author: Logan Glosser <logan.glosser@gmail.com> (drafted with Copilot)
-- Plan:   Close the PMH family by rebuilding the stale patient rollup and
--         verifying it against the current, already-verified PMH events table.
--
-- Why rebuild: canonical_pmh_patient_rollup_v1 build_ts was 2026-04-22, while
--   canonical_pmh_events_v1 was signed off in mig_107 with 12,696 rows after
--   +252 synthetic rows were added on 2026-04-28 (+246 mig_98*, +6 mig_103).
--   Therefore verify-only would sign off a stale rollup.
--
-- Methodology: mig_101 rebuild-then-verify pattern, using the canonical Script
--   365 _build_rollup_sql_for_domain('pmh') derivation. A pre-rebuild archive
--   snapshot is preserved in archive_pub_v1_0 before CREATE OR REPLACE TABLE.
--
-- Post-execution validation (run with /tmp/pmh_lane7_execute.py): rebuilt
--   table has 10,871 rows / 10,871 patients; event source has 12,696 rows /
--   4,158 patients; 77/77 derivable columns have 0 drift vs a fresh Script 365
--   re-derivation; registry closes as 77 verified + 2 na = 79/79.
-- =============================================================================

-- 114a: defensive snapshot of pre-rebuild stale rollup into archive_pub_v1_0.
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pmh_patient_rollup_v1_pre_mig114_20260429 AS
SELECT * FROM main.canonical_pmh_patient_rollup_v1;

-- 114b: rebuild rollup using Script 365 PMH rollup logic against current events.
CREATE OR REPLACE TABLE main.canonical_pmh_patient_rollup_v1 AS
WITH ev AS (
    SELECT
        research_id, finding_status, finding_text, finding_value_norm,
        finding_date, evidence_strength, anchor_source
    FROM "thyroid_canonical_publication_v1_0"."main"."canonical_pmh_events_v1"
),
agg AS (
    SELECT
        research_id,
        ANY_VALUE(anchor_source) AS anchor_source,
        SUM(CASE WHEN finding_status IN
             ('present','suspected','indeterminate','absent')
             THEN 1 ELSE 0 END)                          AS n_findings_any,
        SUM(CASE WHEN finding_status = 'present' THEN 1 ELSE 0 END)
                                                         AS n_findings_present,
        SUM(CASE WHEN finding_status = 'present'
                  AND evidence_strength = 'definitive' THEN 1 ELSE 0 END)
                                                         AS n_findings_definitive,
        SUM(CASE WHEN finding_status = 'present'
                  AND evidence_strength IN ('definitive','probable')
                  THEN 1 ELSE 0 END)
                                                         AS n_findings_probable_or_better,
        MIN(CASE WHEN finding_status = 'present' THEN finding_date END)
                                                         AS first_finding_date,
        MAX(CASE WHEN finding_status = 'present' THEN finding_date END)
                                                         AS last_finding_date,
        COUNT(DISTINCT CASE WHEN finding_status = 'present'
                              THEN finding_value_norm END)
                                                         AS n_distinct_findings_norm,
    COALESCE(BOOL_OR((finding_value_norm IN ('diabetes_mellitus', 'diabetes_type_1', 'diabetes_type_2', 'diabetes') OR LOWER(finding_text) LIKE '%diabet%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_diabetes_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('diabetes_mellitus', 'diabetes_type_1', 'diabetes_type_2', 'diabetes') OR LOWER(finding_text) LIKE '%diabet%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_diabetes_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('diabetes_mellitus', 'diabetes_type_1', 'diabetes_type_2', 'diabetes') OR LOWER(finding_text) LIKE '%diabet%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_diabetes_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypertension') OR LOWER(finding_text) LIKE '%hypertens%' OR LOWER(finding_text) LIKE '%htn%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_hypertension_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypertension') OR LOWER(finding_text) LIKE '%hypertens%' OR LOWER(finding_text) LIKE '%htn%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_hypertension_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypertension') OR LOWER(finding_text) LIKE '%hypertens%' OR LOWER(finding_text) LIKE '%htn%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_hypertension_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('coronary_artery_disease', 'cardiovascular') OR LOWER(finding_text) LIKE '%coronary%artery%disease%' OR LOWER(finding_text) LIKE '% cad %' OR LOWER(finding_text) LIKE '%cad,%' OR LOWER(finding_text) LIKE '%cad.%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_cad_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('coronary_artery_disease', 'cardiovascular') OR LOWER(finding_text) LIKE '%coronary%artery%disease%' OR LOWER(finding_text) LIKE '% cad %' OR LOWER(finding_text) LIKE '%cad,%' OR LOWER(finding_text) LIKE '%cad.%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_cad_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('coronary_artery_disease', 'cardiovascular') OR LOWER(finding_text) LIKE '%coronary%artery%disease%' OR LOWER(finding_text) LIKE '% cad %' OR LOWER(finding_text) LIKE '%cad,%' OR LOWER(finding_text) LIKE '%cad.%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_cad_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('chronic_kidney_disease') OR LOWER(finding_text) LIKE '%chronic%kidney%' OR LOWER(finding_text) LIKE '% ckd %' OR LOWER(finding_text) LIKE '%ckd,%' OR LOWER(finding_text) LIKE '%ckd.%' OR LOWER(finding_text) LIKE '%renal_disease%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_ckd_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('chronic_kidney_disease') OR LOWER(finding_text) LIKE '%chronic%kidney%' OR LOWER(finding_text) LIKE '% ckd %' OR LOWER(finding_text) LIKE '%ckd,%' OR LOWER(finding_text) LIKE '%ckd.%' OR LOWER(finding_text) LIKE '%renal_disease%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_ckd_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('chronic_kidney_disease') OR LOWER(finding_text) LIKE '%chronic%kidney%' OR LOWER(finding_text) LIKE '% ckd %' OR LOWER(finding_text) LIKE '%ckd,%' OR LOWER(finding_text) LIKE '%ckd.%' OR LOWER(finding_text) LIKE '%renal_disease%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_ckd_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('copd') OR LOWER(finding_text) LIKE '%copd%' OR LOWER(finding_text) LIKE '%chronic%obstructive%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_copd_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('copd') OR LOWER(finding_text) LIKE '%copd%' OR LOWER(finding_text) LIKE '%chronic%obstructive%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_copd_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('copd') OR LOWER(finding_text) LIKE '%copd%' OR LOWER(finding_text) LIKE '%chronic%obstructive%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_copd_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('depression') OR LOWER(finding_text) LIKE '%depression%' OR LOWER(finding_text) LIKE '%depressive%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_depression_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('depression') OR LOWER(finding_text) LIKE '%depression%' OR LOWER(finding_text) LIKE '%depressive%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_depression_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('depression') OR LOWER(finding_text) LIKE '%depression%' OR LOWER(finding_text) LIKE '%depressive%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_depression_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('atrial_fibrillation') OR LOWER(finding_text) LIKE '%atrial%fibrillation%' OR LOWER(finding_text) LIKE '%afib%' OR LOWER(finding_text) LIKE '%a-fib%' OR LOWER(finding_text) LIKE '%a fib%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_afib_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('atrial_fibrillation') OR LOWER(finding_text) LIKE '%atrial%fibrillation%' OR LOWER(finding_text) LIKE '%afib%' OR LOWER(finding_text) LIKE '%a-fib%' OR LOWER(finding_text) LIKE '%a fib%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_afib_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('atrial_fibrillation') OR LOWER(finding_text) LIKE '%atrial%fibrillation%' OR LOWER(finding_text) LIKE '%afib%' OR LOWER(finding_text) LIKE '%a-fib%' OR LOWER(finding_text) LIKE '%a fib%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_afib_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('asthma') OR LOWER(finding_text) LIKE '%asthma%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_asthma_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('asthma') OR LOWER(finding_text) LIKE '%asthma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_asthma_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('asthma') OR LOWER(finding_text) LIKE '%asthma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_asthma_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('gerd') OR LOWER(finding_text) LIKE '%gerd%' OR LOWER(finding_text) LIKE '%reflux%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_gerd_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('gerd') OR LOWER(finding_text) LIKE '%gerd%' OR LOWER(finding_text) LIKE '%reflux%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_gerd_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('gerd') OR LOWER(finding_text) LIKE '%gerd%' OR LOWER(finding_text) LIKE '%reflux%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_gerd_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('obesity') OR LOWER(finding_text) LIKE '%obesity%' OR LOWER(finding_text) LIKE '%obese%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_obesity_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('obesity') OR LOWER(finding_text) LIKE '%obesity%' OR LOWER(finding_text) LIKE '%obese%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_obesity_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('obesity') OR LOWER(finding_text) LIKE '%obesity%' OR LOWER(finding_text) LIKE '%obese%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_obesity_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('osteoporosis') OR LOWER(finding_text) LIKE '%osteoporos%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_osteoporosis_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('osteoporosis') OR LOWER(finding_text) LIKE '%osteoporos%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_osteoporosis_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('osteoporosis') OR LOWER(finding_text) LIKE '%osteoporos%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_osteoporosis_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('hyperthyroidism') OR LOWER(finding_text) LIKE '%hyperthyroid%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_hyperthyroidism_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('hyperthyroidism') OR LOWER(finding_text) LIKE '%hyperthyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_hyperthyroidism_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('hyperthyroidism') OR LOWER(finding_text) LIKE '%hyperthyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_hyperthyroidism_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypothyroidism') OR LOWER(finding_text) LIKE '%hypothyroid%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_hypothyroidism_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypothyroidism') OR LOWER(finding_text) LIKE '%hypothyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_hypothyroidism_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('hypothyroidism') OR LOWER(finding_text) LIKE '%hypothyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_hypothyroidism_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('graves_disease', 'hashimoto_thyroiditis', 'autoimmune_thyroid') OR LOWER(finding_text) LIKE '%graves%' OR LOWER(finding_text) LIKE '%hashimoto%' OR LOWER(finding_text) LIKE '%autoimmune%thyroid%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_autoimmune_thyroid_hx_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('graves_disease', 'hashimoto_thyroiditis', 'autoimmune_thyroid') OR LOWER(finding_text) LIKE '%graves%' OR LOWER(finding_text) LIKE '%hashimoto%' OR LOWER(finding_text) LIKE '%autoimmune%thyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_autoimmune_thyroid_hx_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('graves_disease', 'hashimoto_thyroiditis', 'autoimmune_thyroid') OR LOWER(finding_text) LIKE '%graves%' OR LOWER(finding_text) LIKE '%hashimoto%' OR LOWER(finding_text) LIKE '%autoimmune%thyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_autoimmune_thyroid_hx_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('breast_cancer') OR LOWER(finding_text) LIKE '%breast%cancer%' OR LOWER(finding_text) LIKE '%breast%carcinoma%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_breast_cancer_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('breast_cancer') OR LOWER(finding_text) LIKE '%breast%cancer%' OR LOWER(finding_text) LIKE '%breast%carcinoma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_breast_cancer_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('breast_cancer') OR LOWER(finding_text) LIKE '%breast%cancer%' OR LOWER(finding_text) LIKE '%breast%carcinoma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_breast_cancer_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('lung_cancer') OR LOWER(finding_text) LIKE '%lung%cancer%' OR LOWER(finding_text) LIKE '%lung%carcinoma%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_lung_cancer_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('lung_cancer') OR LOWER(finding_text) LIKE '%lung%cancer%' OR LOWER(finding_text) LIKE '%lung%carcinoma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_lung_cancer_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('lung_cancer') OR LOWER(finding_text) LIKE '%lung%cancer%' OR LOWER(finding_text) LIKE '%lung%carcinoma%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_lung_cancer_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('radiation_exposure') OR LOWER(finding_text) LIKE '%radiation%exposure%' OR LOWER(finding_text) LIKE '%radiation%treatment%' OR LOWER(finding_text) LIKE '%childhood%radiation%' OR LOWER(finding_text) LIKE '%head%neck%radiation%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_radiation_exposure_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('radiation_exposure') OR LOWER(finding_text) LIKE '%radiation%exposure%' OR LOWER(finding_text) LIKE '%radiation%treatment%' OR LOWER(finding_text) LIKE '%childhood%radiation%' OR LOWER(finding_text) LIKE '%head%neck%radiation%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_radiation_exposure_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('radiation_exposure') OR LOWER(finding_text) LIKE '%radiation%exposure%' OR LOWER(finding_text) LIKE '%radiation%treatment%' OR LOWER(finding_text) LIKE '%childhood%radiation%' OR LOWER(finding_text) LIKE '%head%neck%radiation%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_radiation_exposure_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('prior_cancer', 'breast_cancer', 'lung_cancer') OR LOWER(finding_text) LIKE '%prior%cancer%' OR LOWER(finding_text) LIKE '%history%of%cancer%' OR LOWER(finding_text) LIKE '%cancer%history%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_prior_cancer_hx_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('prior_cancer', 'breast_cancer', 'lung_cancer') OR LOWER(finding_text) LIKE '%prior%cancer%' OR LOWER(finding_text) LIKE '%history%of%cancer%' OR LOWER(finding_text) LIKE '%cancer%history%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_prior_cancer_hx_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('prior_cancer', 'breast_cancer', 'lung_cancer') OR LOWER(finding_text) LIKE '%prior%cancer%' OR LOWER(finding_text) LIKE '%history%of%cancer%' OR LOWER(finding_text) LIKE '%cancer%history%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_prior_cancer_hx_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('coagulopathy') OR LOWER(finding_text) LIKE '%coagulopath%' OR LOWER(finding_text) LIKE '%bleeding%disorder%' OR LOWER(finding_text) LIKE '%clotting%disorder%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_coagulopathy_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('coagulopathy') OR LOWER(finding_text) LIKE '%coagulopath%' OR LOWER(finding_text) LIKE '%bleeding%disorder%' OR LOWER(finding_text) LIKE '%clotting%disorder%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_coagulopathy_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('coagulopathy') OR LOWER(finding_text) LIKE '%coagulopath%' OR LOWER(finding_text) LIKE '%bleeding%disorder%' OR LOWER(finding_text) LIKE '%clotting%disorder%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_coagulopathy_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_cancer') OR LOWER(finding_text) LIKE '%family%history%of%cancer%' OR LOWER(finding_text) LIKE '%family%hx%cancer%' OR LOWER(finding_text) LIKE '%fhx%cancer%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_family_hx_cancer_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_cancer') OR LOWER(finding_text) LIKE '%family%history%of%cancer%' OR LOWER(finding_text) LIKE '%family%hx%cancer%' OR LOWER(finding_text) LIKE '%fhx%cancer%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_family_hx_cancer_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_cancer') OR LOWER(finding_text) LIKE '%family%history%of%cancer%' OR LOWER(finding_text) LIKE '%family%hx%cancer%' OR LOWER(finding_text) LIKE '%fhx%cancer%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_family_hx_cancer_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_thyroid') OR LOWER(finding_text) LIKE '%family%history%of%thyroid%' OR LOWER(finding_text) LIKE '%family%hx%thyroid%' OR LOWER(finding_text) LIKE '%fhx%thyroid%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_family_hx_thyroid_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_thyroid') OR LOWER(finding_text) LIKE '%family%history%of%thyroid%' OR LOWER(finding_text) LIKE '%family%hx%thyroid%' OR LOWER(finding_text) LIKE '%fhx%thyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_family_hx_thyroid_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('family_hx_thyroid') OR LOWER(finding_text) LIKE '%family%history%of%thyroid%' OR LOWER(finding_text) LIKE '%family%hx%thyroid%' OR LOWER(finding_text) LIKE '%fhx%thyroid%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_family_hx_thyroid_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('men_syndrome') OR LOWER(finding_text) LIKE '%men%syndrome%' OR LOWER(finding_text) LIKE '%multiple%endocrine%neoplasia%' OR LOWER(finding_text) LIKE '%men 1%' OR LOWER(finding_text) LIKE '%men 2%' OR LOWER(finding_text) LIKE '%men2a%' OR LOWER(finding_text) LIKE '%men2b%') AND finding_status='present' AND evidence_strength = 'definitive'), FALSE) AS pmh_men_syndrome_definitive,
    COALESCE(BOOL_OR((finding_value_norm IN ('men_syndrome') OR LOWER(finding_text) LIKE '%men%syndrome%' OR LOWER(finding_text) LIKE '%multiple%endocrine%neoplasia%' OR LOWER(finding_text) LIKE '%men 1%' OR LOWER(finding_text) LIKE '%men 2%' OR LOWER(finding_text) LIKE '%men2a%' OR LOWER(finding_text) LIKE '%men2b%') AND finding_status='present' AND evidence_strength IN ('definitive','probable')), FALSE) AS pmh_men_syndrome_probable_or_better,
    COALESCE(BOOL_OR((finding_value_norm IN ('men_syndrome') OR LOWER(finding_text) LIKE '%men%syndrome%' OR LOWER(finding_text) LIKE '%multiple%endocrine%neoplasia%' OR LOWER(finding_text) LIKE '%men 1%' OR LOWER(finding_text) LIKE '%men 2%' OR LOWER(finding_text) LIKE '%men2a%' OR LOWER(finding_text) LIKE '%men2b%') AND finding_status='present' AND evidence_strength IN ('definitive','probable','possible')), FALSE) AS pmh_men_syndrome_any_evidence,
    COALESCE(BOOL_OR((finding_value_norm IN ('smoking_current') OR LOWER(finding_text) LIKE '%current%smoker%' OR LOWER(finding_text) LIKE '%active%smoker%' OR LOWER(finding_text) LIKE '% smoker%' OR LOWER(finding_text) LIKE '%tobacco%use%' OR LOWER(finding_text) LIKE '%cigarette%' OR LOWER(finding_text) LIKE '%pack%year%' OR LOWER(finding_text) LIKE '%pack-year%') AND finding_status='present'), FALSE) AS pmh_smoking_status_current,
    COALESCE(BOOL_OR((finding_value_norm IN ('smoking_former') OR LOWER(finding_text) LIKE '%former%smoker%' OR LOWER(finding_text) LIKE '%ex-smoker%' OR LOWER(finding_text) LIKE '%ex smoker%' OR LOWER(finding_text) LIKE '%quit%smoking%' OR LOWER(finding_text) LIKE '%previously%smoked%' OR LOWER(finding_text) LIKE '%history%of%smoking%') AND finding_status='present'), FALSE) AS pmh_smoking_status_former,
    COALESCE(BOOL_OR((finding_value_norm IN ('smoking_never') OR LOWER(finding_text) LIKE '%never%smoker%' OR LOWER(finding_text) LIKE '%non-smoker%' OR LOWER(finding_text) LIKE '%nonsmoker%' OR LOWER(finding_text) LIKE '%no%smoking%history%' OR LOWER(finding_text) LIKE '%denies%smoking%') AND finding_status='present'), FALSE) AS pmh_smoking_status_never
    FROM ev
    GROUP BY research_id
),
-- Hybrid anchor source per patient (NULL when no surgery info at all).
hybrid_anchor_per_pt AS (
    SELECT
        CAST(cpm.research_id AS VARCHAR) AS research_id,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM main.canonical_operative_events_v1 o
                WHERE CAST(o.research_id AS VARCHAR)
                      = CAST(cpm.research_id AS VARCHAR)
                  AND o.procedure_normalized ILIKE '%thyroidect%'
                  AND o.surgery_date_native IS NOT NULL
            ) THEN 'strict'
            WHEN cpm.first_surgery_date IS NOT NULL
                THEN 'first_surgery_fallback'
            ELSE NULL
        END AS anchor_source
    FROM main.canonical_patient_master cpm
)
SELECT
    cpm.research_id,
    -- anchor_source from per-patient lookup (not from agg, so it's
    -- populated for patients with NO findings too).
    ha.anchor_source                                     AS anchor_source,
    COALESCE(agg.n_findings_any, 0)                      AS n_findings_any,
    COALESCE(agg.n_findings_present, 0)                  AS n_findings_present,
    COALESCE(agg.n_findings_definitive, 0)               AS n_findings_definitive,
    COALESCE(agg.n_findings_probable_or_better, 0)       AS n_findings_probable_or_better,
    agg.first_finding_date                               AS first_finding_date,
    agg.last_finding_date                                AS last_finding_date,
    COALESCE(agg.n_distinct_findings_norm, 0)            AS n_distinct_findings_norm,
    COALESCE(agg.pmh_diabetes_definitive, FALSE) AS pmh_diabetes_definitive,
    COALESCE(agg.pmh_diabetes_probable_or_better, FALSE) AS pmh_diabetes_probable_or_better,
    COALESCE(agg.pmh_diabetes_any_evidence, FALSE) AS pmh_diabetes_any_evidence,
    COALESCE(agg.pmh_hypertension_definitive, FALSE) AS pmh_hypertension_definitive,
    COALESCE(agg.pmh_hypertension_probable_or_better, FALSE) AS pmh_hypertension_probable_or_better,
    COALESCE(agg.pmh_hypertension_any_evidence, FALSE) AS pmh_hypertension_any_evidence,
    COALESCE(agg.pmh_cad_definitive, FALSE) AS pmh_cad_definitive,
    COALESCE(agg.pmh_cad_probable_or_better, FALSE) AS pmh_cad_probable_or_better,
    COALESCE(agg.pmh_cad_any_evidence, FALSE) AS pmh_cad_any_evidence,
    COALESCE(agg.pmh_ckd_definitive, FALSE) AS pmh_ckd_definitive,
    COALESCE(agg.pmh_ckd_probable_or_better, FALSE) AS pmh_ckd_probable_or_better,
    COALESCE(agg.pmh_ckd_any_evidence, FALSE) AS pmh_ckd_any_evidence,
    COALESCE(agg.pmh_copd_definitive, FALSE) AS pmh_copd_definitive,
    COALESCE(agg.pmh_copd_probable_or_better, FALSE) AS pmh_copd_probable_or_better,
    COALESCE(agg.pmh_copd_any_evidence, FALSE) AS pmh_copd_any_evidence,
    COALESCE(agg.pmh_depression_definitive, FALSE) AS pmh_depression_definitive,
    COALESCE(agg.pmh_depression_probable_or_better, FALSE) AS pmh_depression_probable_or_better,
    COALESCE(agg.pmh_depression_any_evidence, FALSE) AS pmh_depression_any_evidence,
    COALESCE(agg.pmh_afib_definitive, FALSE) AS pmh_afib_definitive,
    COALESCE(agg.pmh_afib_probable_or_better, FALSE) AS pmh_afib_probable_or_better,
    COALESCE(agg.pmh_afib_any_evidence, FALSE) AS pmh_afib_any_evidence,
    COALESCE(agg.pmh_asthma_definitive, FALSE) AS pmh_asthma_definitive,
    COALESCE(agg.pmh_asthma_probable_or_better, FALSE) AS pmh_asthma_probable_or_better,
    COALESCE(agg.pmh_asthma_any_evidence, FALSE) AS pmh_asthma_any_evidence,
    COALESCE(agg.pmh_gerd_definitive, FALSE) AS pmh_gerd_definitive,
    COALESCE(agg.pmh_gerd_probable_or_better, FALSE) AS pmh_gerd_probable_or_better,
    COALESCE(agg.pmh_gerd_any_evidence, FALSE) AS pmh_gerd_any_evidence,
    COALESCE(agg.pmh_obesity_definitive, FALSE) AS pmh_obesity_definitive,
    COALESCE(agg.pmh_obesity_probable_or_better, FALSE) AS pmh_obesity_probable_or_better,
    COALESCE(agg.pmh_obesity_any_evidence, FALSE) AS pmh_obesity_any_evidence,
    COALESCE(agg.pmh_osteoporosis_definitive, FALSE) AS pmh_osteoporosis_definitive,
    COALESCE(agg.pmh_osteoporosis_probable_or_better, FALSE) AS pmh_osteoporosis_probable_or_better,
    COALESCE(agg.pmh_osteoporosis_any_evidence, FALSE) AS pmh_osteoporosis_any_evidence,
    COALESCE(agg.pmh_hyperthyroidism_definitive, FALSE) AS pmh_hyperthyroidism_definitive,
    COALESCE(agg.pmh_hyperthyroidism_probable_or_better, FALSE) AS pmh_hyperthyroidism_probable_or_better,
    COALESCE(agg.pmh_hyperthyroidism_any_evidence, FALSE) AS pmh_hyperthyroidism_any_evidence,
    COALESCE(agg.pmh_hypothyroidism_definitive, FALSE) AS pmh_hypothyroidism_definitive,
    COALESCE(agg.pmh_hypothyroidism_probable_or_better, FALSE) AS pmh_hypothyroidism_probable_or_better,
    COALESCE(agg.pmh_hypothyroidism_any_evidence, FALSE) AS pmh_hypothyroidism_any_evidence,
    COALESCE(agg.pmh_autoimmune_thyroid_hx_definitive, FALSE) AS pmh_autoimmune_thyroid_hx_definitive,
    COALESCE(agg.pmh_autoimmune_thyroid_hx_probable_or_better, FALSE) AS pmh_autoimmune_thyroid_hx_probable_or_better,
    COALESCE(agg.pmh_autoimmune_thyroid_hx_any_evidence, FALSE) AS pmh_autoimmune_thyroid_hx_any_evidence,
    COALESCE(agg.pmh_breast_cancer_definitive, FALSE) AS pmh_breast_cancer_definitive,
    COALESCE(agg.pmh_breast_cancer_probable_or_better, FALSE) AS pmh_breast_cancer_probable_or_better,
    COALESCE(agg.pmh_breast_cancer_any_evidence, FALSE) AS pmh_breast_cancer_any_evidence,
    COALESCE(agg.pmh_lung_cancer_definitive, FALSE) AS pmh_lung_cancer_definitive,
    COALESCE(agg.pmh_lung_cancer_probable_or_better, FALSE) AS pmh_lung_cancer_probable_or_better,
    COALESCE(agg.pmh_lung_cancer_any_evidence, FALSE) AS pmh_lung_cancer_any_evidence,
    COALESCE(agg.pmh_radiation_exposure_definitive, FALSE) AS pmh_radiation_exposure_definitive,
    COALESCE(agg.pmh_radiation_exposure_probable_or_better, FALSE) AS pmh_radiation_exposure_probable_or_better,
    COALESCE(agg.pmh_radiation_exposure_any_evidence, FALSE) AS pmh_radiation_exposure_any_evidence,
    COALESCE(agg.pmh_prior_cancer_hx_definitive, FALSE) AS pmh_prior_cancer_hx_definitive,
    COALESCE(agg.pmh_prior_cancer_hx_probable_or_better, FALSE) AS pmh_prior_cancer_hx_probable_or_better,
    COALESCE(agg.pmh_prior_cancer_hx_any_evidence, FALSE) AS pmh_prior_cancer_hx_any_evidence,
    COALESCE(agg.pmh_coagulopathy_definitive, FALSE) AS pmh_coagulopathy_definitive,
    COALESCE(agg.pmh_coagulopathy_probable_or_better, FALSE) AS pmh_coagulopathy_probable_or_better,
    COALESCE(agg.pmh_coagulopathy_any_evidence, FALSE) AS pmh_coagulopathy_any_evidence,
    COALESCE(agg.pmh_family_hx_cancer_definitive, FALSE) AS pmh_family_hx_cancer_definitive,
    COALESCE(agg.pmh_family_hx_cancer_probable_or_better, FALSE) AS pmh_family_hx_cancer_probable_or_better,
    COALESCE(agg.pmh_family_hx_cancer_any_evidence, FALSE) AS pmh_family_hx_cancer_any_evidence,
    COALESCE(agg.pmh_family_hx_thyroid_definitive, FALSE) AS pmh_family_hx_thyroid_definitive,
    COALESCE(agg.pmh_family_hx_thyroid_probable_or_better, FALSE) AS pmh_family_hx_thyroid_probable_or_better,
    COALESCE(agg.pmh_family_hx_thyroid_any_evidence, FALSE) AS pmh_family_hx_thyroid_any_evidence,
    COALESCE(agg.pmh_men_syndrome_definitive, FALSE) AS pmh_men_syndrome_definitive,
    COALESCE(agg.pmh_men_syndrome_probable_or_better, FALSE) AS pmh_men_syndrome_probable_or_better,
    COALESCE(agg.pmh_men_syndrome_any_evidence, FALSE) AS pmh_men_syndrome_any_evidence,
    COALESCE(agg.pmh_smoking_status_current, FALSE) AS pmh_smoking_status_current,
    COALESCE(agg.pmh_smoking_status_former, FALSE) AS pmh_smoking_status_former,
    COALESCE(agg.pmh_smoking_status_never, FALSE) AS pmh_smoking_status_never,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                 AS build_ts
FROM main.canonical_patient_master cpm
LEFT JOIN agg ON CAST(cpm.research_id AS VARCHAR) = agg.research_id
LEFT JOIN hybrid_anchor_per_pt ha ON CAST(cpm.research_id AS VARCHAR)
                                       = ha.research_id;

-- 114c: flip 77 derivable not_started columns to verified.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_post_events_repair',
    batch_id            = 'mig_114_pmh_rollup_rebuild_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_114: rebuilt stale PMH rollup using Script 365 '
                          || 'PMH derivation logic after mig_107 PMH events signoff. '
                          || 'Events now 12,696 rows / 4,158 patients including +252 '
                          || 'synthetic rows (+246 mig_98*, +6 mig_103) added after '
                          || 'the previous 2026-04-22 rollup build. Post-rebuild fresh '
                          || 're-derivation showed 0 drift across all 77 derivable cols; '
                          || 'cohort parity 10,871 = CPM.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_pmh_patient_rollup_v1'
  AND verification_status = 'not_started';

-- 114d: recompute table_signoff_registry counts and sign off.
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/114_pmh_patient_rollup_signoff.sql',
    notes             = 'Rebuilt stale PMH patient rollup with Script 365 PMH logic '
                        || 'after mig_107 verified canonical_pmh_events_v1. '
                        || 'Pre-rebuild rollup build_ts was 2026-04-22; events now '
                        || '12,696 rows / 4,158 patients after +252 synthetic rows. '
                        || 'Post-rebuild validation: 10,871 rows matching CPM and '
                        || '0 drift across 77 derivable columns vs fresh re-derivation. '
                        || 'PMH family closed: events (mig_107) + rollup (mig_114).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_pmh_patient_rollup_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 114 -- canonical_pmh_patient_rollup_v1 closed under Protocol v2.
-- =============================================================================
