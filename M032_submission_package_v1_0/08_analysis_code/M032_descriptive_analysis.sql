-- M032 — Twenty-Five Year Single-Institution Descriptive Cohort of Thyroid Surgery
-- Single SQL package reproducing every numeric cell in the manuscript.
-- Run against MotherDuck `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`).
-- Post-release NLP augment: mig_281 (smoking/family-hx NLP promotion) + mig_285 (cohort_m032 view NLP augment)
-- Cohort lock commit: 590acb5 (2026-05-04, post-mig_281/285)
--
-- Era binning: 5-year periods anchored to surg_first_date calendar year
--   A_1999_2004: YEAR(surg_first_date) BETWEEN 1999 AND 2004
--   B_2005_2009: YEAR(surg_first_date) BETWEEN 2005 AND 2009
--   C_2010_2014: YEAR(surg_first_date) BETWEEN 2010 AND 2014
--   D_2015_2019: YEAR(surg_first_date) BETWEEN 2015 AND 2019
--   E_2020_2025: YEAR(surg_first_date) BETWEEN 2020 AND 2025
--
-- All queries use DISTINCT research_id dedup via the cohort view's built-in deduplicated grain.
-- Expected counts annotated as comments; DIFF = live_value – expected_value.

USE thyroid_canonical_publication_v1_0;

-- ============================================================
-- ERA HELPER CTE (reuse in all queries)
-- ============================================================
-- DEFINE era_base AS:
WITH era_base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
)
SELECT surgery_era, COUNT(*) AS n
FROM era_base
GROUP BY surgery_era
ORDER BY surgery_era;
-- EXPECTED: A=905, B=1194, C=1889, D=2948, E=3935, total=10871

-- ============================================================
-- Q01 — Cohort assembly (§3.1 / Figure 1 flow)
-- ============================================================
SELECT
  COUNT(*)                                              AS n_total,          -- 10871
  COUNT(*) FILTER (WHERE is_malignant = TRUE)           AS n_malignant,      -- 4018
  COUNT(*) FILTER (WHERE is_malignant = FALSE OR is_malignant IS NULL)
                                                        AS n_benign_or_null, -- 6853
  ROUND(COUNT(*) FILTER (WHERE is_malignant = TRUE) * 100.0 / COUNT(*), 1)
                                                        AS pct_malignant     -- 37.0
FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1;

-- ============================================================
-- Q02 — Table 1: Cohort demographics + tumor characteristics
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
)
SELECT
  -- Demographics
  COUNT(*)                                              AS n_total,
  ROUND(AVG(age_at_surgery::DOUBLE), 1)                 AS age_mean,
  MEDIAN(age_at_surgery)                                AS age_median,
  ROUND(QUANTILE_CONT(age_at_surgery::DOUBLE, 0.25), 1) AS age_q25,
  ROUND(QUANTILE_CONT(age_at_surgery::DOUBLE, 0.75), 1) AS age_q75,
  ROUND(STDDEV(age_at_surgery::DOUBLE), 1)              AS age_sd,
  COUNT(*) FILTER (WHERE LOWER(sex) = 'female')         AS n_female,
  COUNT(*) FILTER (WHERE LOWER(sex) = 'male')           AS n_male,
  -- Race
  COUNT(*) FILTER (WHERE LOWER(race) LIKE '%white%' OR LOWER(race) LIKE '%caucasian%') AS n_white,
  COUNT(*) FILTER (WHERE LOWER(race) LIKE '%black%' OR LOWER(race) LIKE '%african%')   AS n_black,
  COUNT(*) FILTER (WHERE LOWER(race) LIKE '%asian%')    AS n_asian,
  COUNT(*) FILTER (WHERE LOWER(race) LIKE '%hispanic%') AS n_hispanic,
  -- Malignancy
  COUNT(*) FILTER (WHERE is_malignant = TRUE)           AS n_malignant,
  -- Surgery
  COUNT(*) FILTER (WHERE surg_total_thyroidectomy = TRUE) AS n_total_thyroidectomy,
  COUNT(*) FILTER (WHERE surg_hemithyroidectomy = TRUE)   AS n_hemithyroidectomy,
  -- Tumor
  ROUND(MEDIAN(tumor_size_cm), 2)                       AS tumor_size_median_cm,
  ROUND(QUANTILE_CONT(tumor_size_cm, 0.25), 2)          AS tumor_size_q25,
  ROUND(QUANTILE_CONT(tumor_size_cm, 0.75), 2)          AS tumor_size_q75,
  COUNT(*) FILTER (WHERE tumor_size_cm < 1.0)           AS n_lt1cm,
  COUNT(*) FILTER (WHERE tumor_size_cm BETWEEN 1.0 AND 2.0) AS n_1to2cm,
  COUNT(*) FILTER (WHERE tumor_size_cm BETWEEN 2.0 AND 4.0) AS n_2to4cm,
  COUNT(*) FILTER (WHERE tumor_size_cm > 4.0)           AS n_gt4cm,
  COUNT(*) FILTER (WHERE multifocal_flag = TRUE)        AS n_multifocal,
  -- Staging
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'I')       AS n_stage_I,
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'II')      AS n_stage_II,
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'III')     AS n_stage_III,
  COUNT(*) FILTER (WHERE ajcc8_stage_group LIKE '%IV%') AS n_stage_IV_any,
  -- Follow-up
  ROUND(MEDIAN(followup_years), 1)                      AS fu_median_yrs,
  ROUND(QUANTILE_CONT(followup_years, 0.25), 1)         AS fu_q25,
  ROUND(QUANTILE_CONT(followup_years, 0.75), 1)         AS fu_q75
FROM base;

-- ============================================================
-- Q03 — Table 2: Histology distribution + malignancy rate by era
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
  WHERE is_malignant = TRUE
)
SELECT
  surgery_era,
  COUNT(*)                                              AS n_malignant,
  -- Histology
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%ptc%' OR LOWER(histology_final) LIKE '%papillary%') AS n_ptc,
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%ftc%' OR LOWER(histology_final) LIKE '%follicular%') AS n_ftc,
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%mtc%' OR LOWER(histology_final) LIKE '%medullary%') AS n_mtc,
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%atc%' OR LOWER(histology_final) LIKE '%anaplastic%') AS n_atc,
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%pdtc%') AS n_pdtc,
  COUNT(*) FILTER (WHERE LOWER(histology_final) LIKE '%hcc%' OR LOWER(histology_final) LIKE '%oncocytic%' OR LOWER(histology_final) LIKE '%hurthle%') AS n_hcc,
  -- ETE
  COUNT(*) FILTER (WHERE LOWER(ete_grade_final) = 'gross')        AS n_ete_gross,
  COUNT(*) FILTER (WHERE LOWER(ete_grade_final) = 'microscopic')  AS n_ete_micro,
  -- LN
  COUNT(*) FILTER (WHERE ln_positive_flag > 0)  AS n_ln_positive,
  ROUND(AVG(ln_total_examined::DOUBLE), 1)       AS ln_examined_mean,
  ROUND(AVG(ln_total_positive::DOUBLE), 1)       AS ln_positive_mean,
  -- RAI
  COUNT(*) FILTER (WHERE rai_received_flag = TRUE) AS n_rai
FROM base
GROUP BY surgery_era
ORDER BY surgery_era;
-- EXPECTED ERA TOTALS: A=264, B=397, C=654, D=1106, E=1597 (total=4018)

-- ============================================================
-- Q04 — Table 3: TNM stage migration over 25 years
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
  WHERE is_malignant = TRUE
)
SELECT
  surgery_era,
  COUNT(*) AS n,
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'I')       AS n_I,
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'II')      AS n_II,
  COUNT(*) FILTER (WHERE ajcc8_stage_group = 'III')     AS n_III,
  COUNT(*) FILTER (WHERE ajcc8_stage_group IN ('IVA','IVB','IVC') OR ajcc8_stage_group LIKE 'IV%') AS n_IV,
  COUNT(*) FILTER (WHERE ajcc8_stage_group IS NULL OR ajcc8_stage_group = 'unknown') AS n_unknown_stage,
  -- ATA risk
  COUNT(*) FILTER (WHERE LOWER(ata_risk_category) = 'low')          AS n_ata_low,
  COUNT(*) FILTER (WHERE LOWER(ata_risk_category) = 'intermediate')  AS n_ata_intermediate,
  COUNT(*) FILTER (WHERE LOWER(ata_risk_category) = 'high')          AS n_ata_high,
  -- RAI rates
  ROUND(COUNT(*) FILTER (WHERE rai_received_flag = TRUE) * 100.0 / COUNT(*), 1) AS pct_rai
FROM base
GROUP BY surgery_era
ORDER BY surgery_era;

-- ============================================================
-- Q05 — Table 4: Treatment patterns (surgery extent + RAI) by era
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
)
SELECT
  surgery_era,
  COUNT(*) AS n_total,
  COUNT(*) FILTER (WHERE surg_total_thyroidectomy = TRUE) AS n_total_thyroidectomy,
  ROUND(COUNT(*) FILTER (WHERE surg_total_thyroidectomy = TRUE) * 100.0 / COUNT(*), 1) AS pct_total_thy,
  COUNT(*) FILTER (WHERE surg_hemithyroidectomy = TRUE)   AS n_hemithyroidectomy,
  ROUND(COUNT(*) FILTER (WHERE surg_hemithyroidectomy = TRUE) * 100.0 / COUNT(*), 1)   AS pct_hemi,
  COUNT(*) FILTER (WHERE n_surgeries > 1)                 AS n_multisurgery,
  COUNT(*) FILTER (WHERE rai_received_flag = TRUE)         AS n_rai,
  ROUND(COUNT(*) FILTER (WHERE rai_received_flag = TRUE) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE is_malignant=TRUE),0), 1) AS pct_rai_among_malignant,
  -- Recurrence / complications
  COUNT(*) FILTER (WHERE any_recurrence_flag = TRUE)       AS n_recurrence,
  COUNT(*) FILTER (WHERE comp_rln_injury_confirmed = TRUE) AS n_rln,
  COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE) AS n_hypocalcemia,
  COUNT(*) FILTER (WHERE comp_hematoma_confirmed = TRUE)   AS n_hematoma,
  COUNT(*) FILTER (WHERE death_occurred = TRUE)            AS n_death,
  ROUND(MEDIAN(followup_years), 1)                         AS fu_median_yrs
FROM base
GROUP BY surgery_era
ORDER BY surgery_era;

-- ============================================================
-- Q06 — Table 5: Smoking + family hx by era (post-mig_281 NLP)
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
)
SELECT
  surgery_era,
  COUNT(*) AS n,
  -- Smoking (combined: NLP or structured)
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'current') AS n_current_smoker,
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'former')  AS n_former_smoker,
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'never')   AS n_never_smoker,
  COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL)         AS n_smoking_known,
  ROUND(COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'current') * 100.0
    / NULLIF(COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL), 0), 1) AS pct_current_of_known,
  -- NLP-specific
  COUNT(*) FILTER (WHERE pmhx_nlp_smoking_status IS NOT NULL)         AS n_smoking_nlp,
  -- Family hx
  COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid = TRUE)           AS n_fhx_thyroid,
  COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_cancer = TRUE)            AS n_fhx_any_cancer,
  COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL)      AS n_fhx_thyroid_known
FROM base
GROUP BY surgery_era
ORDER BY surgery_era;
-- EXPECTED from locked report:
-- Total: current=212, former=502, never=2298, known=3022 (27.8%)
-- Fhx present=366, known=3018

-- ============================================================
-- Q07 — Supp S1: Detailed sub-histology by era (malignant only)
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
  WHERE is_malignant = TRUE
)
SELECT
  histology_final,
  COUNT(*) AS n_total,
  COUNT(*) FILTER (WHERE surgery_era = 'A_1999_2004') AS n_era_A,
  COUNT(*) FILTER (WHERE surgery_era = 'B_2005_2009') AS n_era_B,
  COUNT(*) FILTER (WHERE surgery_era = 'C_2010_2014') AS n_era_C,
  COUNT(*) FILTER (WHERE surgery_era = 'D_2015_2019') AS n_era_D,
  COUNT(*) FILTER (WHERE surgery_era = 'E_2020_2025') AS n_era_E
FROM base
GROUP BY histology_final
ORDER BY n_total DESC;

-- ============================================================
-- Q08 — Supp S2: Race/ethnicity trends by era (full cohort)
-- ============================================================
WITH base AS (
  SELECT *,
    CASE
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
      WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
      ELSE 'F_pre1999_or_unknown'
    END AS surgery_era,
    CASE
      WHEN LOWER(race) LIKE '%white%' OR LOWER(race) LIKE '%caucasian%' THEN 'White'
      WHEN LOWER(race) LIKE '%black%' OR LOWER(race) LIKE '%african%'   THEN 'Black'
      WHEN LOWER(race) LIKE '%asian%'    THEN 'Asian'
      WHEN LOWER(race) LIKE '%hispanic%' THEN 'Hispanic'
      ELSE 'Other/Unknown'
    END AS race_group
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
)
SELECT surgery_era, race_group, COUNT(*) AS n
FROM base
GROUP BY surgery_era, race_group
ORDER BY surgery_era, n DESC;

-- ============================================================
-- Q09 — Cohort-wide smoking (Locked-numbers validation)
-- ============================================================
SELECT
  COUNT(*)                                                 AS n_total,
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'current') AS n_current,  -- expect 212
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'former')  AS n_former,   -- expect 502
  COUNT(*) FILTER (WHERE LOWER(smoking_status_combined) = 'never')   AS n_never,    -- expect 2298
  COUNT(*) FILTER (WHERE smoking_status_combined IS NOT NULL)         AS n_known,    -- expect 3022
  COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid = TRUE)          AS n_fhx_thy,  -- expect 366
  COUNT(*) FILTER (WHERE pmhx_nlp_family_hx_thyroid IS NOT NULL)     AS n_fhx_known -- expect 3018
FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1;

-- ============================================================
-- Q10 — signoff_migration entry (run LAST after validation)
-- ============================================================
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_290', CURRENT_TIMESTAMP, 'cursor_composer_mig290',
 'mig_290: M032 25-yr Descriptive submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp S1-S2 + 4 figures. SQL reproducibility package + 3 build scripts + validation report. Closes M032 ready-for-writing gate (CF-M032-READY-FOR-WRITING).');
