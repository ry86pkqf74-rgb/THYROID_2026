-- M038 — Massive Goiter Composite-Definition Descriptive Cohort
-- Single SQL package reproducing every numeric cell in the manuscript.
-- Run against MotherDuck `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`).
-- Most-recent applied migration: mig_255_cohort_m038_complication_temporality_columns_20260502.

-- Standing reference for is_massive composite + era binning + hypopara split:
-- - Composite (Methods §2.3): weight ≥100g OR substernal (CT or MRI) OR airway (CT)
-- - Era binning: upper-bound rule sweeps pre-1999 dates into 1999–2004 bucket
-- - Hypopara split: per memory/feedback_complications_transient_vs_permanent.md

-- ============================================================
-- Q01 — Cohort assembly + composite-flag composition (§3.1)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive,
    COALESCE(gland_weight_final_g >= 100, FALSE) AS w,
    (COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)) AS s,
    (COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS a
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  COUNT(*)                                              AS n_total,                 -- 10,871
  COUNT(*) FILTER (WHERE is_massive)                    AS n_massive,               -- 2,501 (23.0%)
  COUNT(*) FILTER (WHERE w)                             AS n_weight,                -- 1,429
  COUNT(*) FILTER (WHERE s)                             AS n_substernal,            -- 1,047
  COUNT(*) FILTER (WHERE a)                             AS n_airway,                -- 1,440
  COUNT(*) FILTER (WHERE w AND s)                       AS n_w_and_s,               -- 404
  COUNT(*) FILTER (WHERE w AND a)                       AS n_w_and_a,               -- 513
  COUNT(*) FILTER (WHERE s AND a)                       AS n_s_and_a,               -- 884
  COUNT(*) FILTER (WHERE w AND s AND a)                 AS n_all_three,             -- 386
  COUNT(*) FILTER (WHERE w AND NOT s AND NOT a)         AS n_weight_only,           -- 898
  COUNT(*) FILTER (WHERE s AND NOT w AND NOT a)         AS n_substernal_only,       -- 145
  COUNT(*) FILTER (WHERE a AND NOT w AND NOT s)         AS n_airway_only            -- 429
FROM base;
-- Inclusion-exclusion check: 1429+1047+1440 - 404-513-884 + 386 = 2,501 ✓

-- ============================================================
-- Q02 — Demographics + Table 1 base (§3.2)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  is_massive,
  COUNT(*) AS n,
  AVG(age_at_surgery) AS age_mean,
  MEDIAN(age_at_surgery) AS age_median,
  QUANTILE_CONT(age_at_surgery, 0.25) AS age_q25,
  QUANTILE_CONT(age_at_surgery, 0.75) AS age_q75,
  COUNT(*) FILTER (WHERE LOWER(sex) = 'female') AS n_female,
  COUNT(*) FILTER (WHERE LOWER(sex) = 'male')   AS n_male,
  COUNT(*) FILTER (WHERE is_malignant) AS n_malignant,
  COUNT(*) FILTER (WHERE bilateral_disease_flag) AS n_bilateral,
  AVG(followup_years) AS fu_mean_all,
  COUNT(*) FILTER (WHERE followup_years > 0) AS n_fu_pos,
  AVG(followup_years) FILTER (WHERE followup_years > 0) AS fu_mean_pos,
  COUNT(bmi_combined) AS n_bmi,
  AVG(bmi_combined) AS bmi_mean,
  MEDIAN(bmi_combined) AS bmi_median,
  -- NLP comorbidities
  COUNT(*) FILTER (WHERE pmhx_nlp_hypertension) AS n_htn,
  COUNT(*) FILTER (WHERE pmhx_nlp_diabetes) AS n_dm,
  COUNT(*) FILTER (WHERE pmhx_nlp_cad) AS n_cad,
  COUNT(*) FILTER (WHERE pmhx_nlp_ckd) AS n_ckd,
  COUNT(*) FILTER (WHERE pmhx_nlp_copd) AS n_copd,
  AVG(pmhx_nlp_n_comorbidities) AS comorb_mean,
  -- Thyroid history
  COUNT(*) FILTER (WHERE syn_graves) AS n_graves,
  COUNT(*) FILTER (WHERE syn_hashimoto) AS n_hashimoto,
  COUNT(*) FILTER (WHERE pshx_nlp_prior_thyroidectomy) AS n_prior_thy,
  COUNT(*) FILTER (WHERE pshx_nlp_prior_neck_surgery) AS n_prior_neck
FROM base
GROUP BY is_massive
ORDER BY is_massive DESC;

-- Race breakdown (full 9 buckets)
SELECT
  (COALESCE(gland_weight_final_g >= 100, FALSE)
   OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
   OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive,
  race,
  COUNT(*) AS n
FROM manuscript_workspace.cohort_m038_massive_goiter_v1
GROUP BY is_massive, race
ORDER BY is_massive DESC, n DESC;

-- ASA (NSQIP-linked subset only)
SELECT
  (COALESCE(gland_weight_final_g >= 100, FALSE)
   OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
   OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive,
  nsqip_asa_class,
  COUNT(*) AS n
FROM manuscript_workspace.cohort_m038_massive_goiter_v1
WHERE nsqip_asa_class IS NOT NULL
GROUP BY is_massive, nsqip_asa_class
ORDER BY is_massive DESC, nsqip_asa_class;

-- ============================================================
-- Q03 — Histology in malignant subset (§3.3 / Table 2)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT histology_final, COUNT(*) AS n
FROM base
WHERE is_massive AND is_malignant
GROUP BY histology_final
ORDER BY n DESC;

-- ============================================================
-- Q04 — Procedure type + operative context (§3.4 / Table 3)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  is_massive,
  surg_procedure_type,
  COUNT(*) AS n
FROM base
GROUP BY is_massive, surg_procedure_type
ORDER BY is_massive DESC, n DESC;

-- Operative context (LOS uses nsqip_length_of_stay_days, n=246/1,164)
SELECT
  is_massive,
  COUNT(*) FILTER (WHERE LOWER(nsqip_central_neck_dissection) IN ('yes','y','true','1')) AS n_cnd,
  COUNT(*) FILTER (WHERE LOWER(nsqip_lateral_neck_dissection) IN ('yes','y','true','1')) AS n_lnd,
  AVG(nsqip_operative_duration_min) AS opdur_mean,
  MEDIAN(nsqip_operative_duration_min) AS opdur_median,
  AVG(nsqip_length_of_stay_days) AS los_mean,
  MEDIAN(nsqip_length_of_stay_days) AS los_median,
  COUNT(*) FILTER (WHERE nsqip_transfusion >= 1) AS n_transfusion,
  COUNT(*) FILTER (WHERE nsqip_unplanned_intubation >= 1) AS n_unplanned_intub,
  COUNT(*) FILTER (WHERE nsqip_readmission_30d_flag = 1) AS n_readmit_30d,
  COUNT(*) FILTER (WHERE proc_nlp_tracheostomy) AS n_tracheo
FROM (SELECT *, (COALESCE(gland_weight_final_g >= 100, FALSE)
                 OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
                 OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
      FROM manuscript_workspace.cohort_m038_massive_goiter_v1) b
GROUP BY is_massive
ORDER BY is_massive DESC;

-- ============================================================
-- Q05 — Strict-definition complications (§3.5 / Table 4) — postop confirmed
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  is_massive,
  COUNT(*) AS n,
  COUNT(*) FILTER (WHERE any_confirmed_complication_flag) AS n_any,
  COUNT(*) FILTER (WHERE comp_hematoma_confirmed) AS n_hematoma,
  COUNT(*) FILTER (WHERE comp_seroma_confirmed) AS n_seroma,
  COUNT(*) FILTER (WHERE comp_chyle_leak_confirmed) AS n_chyle,
  COUNT(*) FILTER (WHERE comp_rln_injury_confirmed) AS n_rln,
  COUNT(*) FILTER (WHERE comp_vc_paresis_confirmed) AS n_vc_paresis,
  COUNT(*) FILTER (WHERE comp_vc_paralysis_confirmed) AS n_vc_paralysis,
  COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed) AS n_hypocalcemia,
  COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed) AS n_hypopara_total,
  COUNT(*) FILTER (WHERE death_occurred) AS n_death
FROM base
GROUP BY is_massive
ORDER BY is_massive DESC;

-- ============================================================
-- Q05b — Hypoparathyroidism transient/permanent split + hypocalcemia preop (standing rule)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  is_massive,
  COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_transient) AS hpt_transient,
  COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_permanent) AS hpt_permanent,
  COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed AND NOT comp_hypoparathyroidism_transient AND NOT comp_hypoparathyroidism_permanent) AS hpt_unclassified,
  COUNT(*) FILTER (WHERE comp_hypocalcemia_timing_window = 'pre_surgery'
                   OR comp_hypocalcemia_clinical_preexisting) AS hca_preop,
  COUNT(*) FILTER (WHERE comp_hypoparathyroidism_preexisting) AS hpt_preexisting_fyi
FROM base
GROUP BY is_massive
ORDER BY is_massive DESC;

-- ============================================================
-- Q06 — Era stratification (§3.6 / Table 5)
-- ============================================================
WITH base AS (
  SELECT *,
    (COALESCE(gland_weight_final_g >= 100, FALSE)
     OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
     OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE)) AS is_massive,
    CASE WHEN surg_first_date IS NULL THEN 'unknown'
         WHEN surg_first_date <= '2004-12-31' THEN '1999-2004'
         WHEN surg_first_date <= '2009-12-31' THEN '2005-2009'
         WHEN surg_first_date <= '2014-12-31' THEN '2010-2014'
         WHEN surg_first_date <= '2019-12-31' THEN '2015-2019'
         ELSE '2020-2025' END AS era
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  era,
  COUNT(*) AS n_total,
  COUNT(*) FILTER (WHERE is_massive) AS n_massive,
  COUNT(*) FILTER (WHERE NOT is_massive) AS n_nonmassive,
  ROUND(100.0 * COUNT(*) FILTER (WHERE is_massive) / NULLIF(COUNT(*),0), 1) AS pct_massive
FROM base
GROUP BY era
ORDER BY era;

-- ============================================================
-- Q07 — Component coverage by era × arm (Supp S2 / Figure 4)
-- ============================================================
-- See build_m038_figures.py for the per-cell coverage extract used in Supp S2 and Fig 4.

-- ============================================================
-- Q08 — Sensitivity: weight-only ≥200g focal cohort (Supp S6)
-- ============================================================
WITH base AS (
  SELECT *, COALESCE(gland_weight_final_g >= 200, FALSE) AS w200
  FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  COUNT(*) FILTER (WHERE w200) AS n_w200,
  COUNT(*) FILTER (WHERE w200 AND any_confirmed_complication_flag) AS w200_anycomp,
  COUNT(*) FILTER (WHERE w200 AND comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_transient) AS w200_hpt_trans,
  COUNT(*) FILTER (WHERE w200 AND comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_permanent) AS w200_hpt_perm,
  COUNT(*) FILTER (WHERE w200 AND comp_rln_injury_confirmed) AS w200_rln,
  COUNT(*) FILTER (WHERE w200 AND comp_vc_paralysis_confirmed) AS w200_vcl
FROM base;

-- ============================================================
-- §5 limitation footnote: surgical-date coverage
-- ============================================================
SELECT
  COUNT(*) AS n_total,
  COUNT(surg_first_date) AS n_known,
  ROUND(100.0 * COUNT(surg_first_date) / COUNT(*), 1) AS pct_known_cohort_wide,
  COUNT(*) FILTER (WHERE (COALESCE(gland_weight_final_g >= 100, FALSE)
                          OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
                          OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE))
                     AND surg_first_date IS NOT NULL) AS n_known_in_massive,
  ROUND(100.0 * COUNT(*) FILTER (WHERE (COALESCE(gland_weight_final_g >= 100, FALSE)
                                        OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
                                        OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE))
                                   AND surg_first_date IS NOT NULL) /
        NULLIF(COUNT(*) FILTER (WHERE (COALESCE(gland_weight_final_g >= 100, FALSE)
                                       OR COALESCE(ct_substernal_extension_any, FALSE) OR COALESCE(mri_substernal_any, FALSE)
                                       OR COALESCE(ct_tracheal_deviation_any, FALSE) OR COALESCE(ct_tracheal_narrowing_any, FALSE) OR COALESCE(ct_airway_compromise_any, FALSE))), 0), 1) AS pct_known_in_massive
FROM manuscript_workspace.cohort_m038_massive_goiter_v1;
-- Cohort-wide: 80.3%; massive arm: 69.6%

-- ============================================================
-- End of M038 descriptive analysis SQL package.
-- ============================================================
