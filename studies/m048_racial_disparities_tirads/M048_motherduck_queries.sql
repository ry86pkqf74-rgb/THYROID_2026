-- ============================================================================
-- M048 — Racial Disparities in ACR TI-RADS Performance
--   25-year operative cohort, n=3,375 patients (45.5% Black, 40.9% White, 6.0% Asian)
--
-- Pre-specified DuckDB SQL on MotherDuck thyroid_canonical_publication_v1_0
-- (release tag pub_v1_1, 2026-05-04). Mirrors the M025 v2 patient/nodule grain
-- conventions. Does NOT introduce new outcome definitions; reuses
--   manuscript_workspace.m025_analytic_master_patient_v1 (mig_307b)
--   manuscript_workspace.m025_analytic_master_nodule_v1  (mig_307b)
--
-- Author: Logan D. Glosser. Drafted 2026-05-05 (Cowork session).
-- ============================================================================

USE thyroid_canonical_publication_v1_0;
SET schema = 'manuscript_workspace';

-- ----------------------------------------------------------------------------
-- 0. Sanity checks: race coverage and pre-specified strata
-- ----------------------------------------------------------------------------
-- 0a. Race distribution in the M025 patient cohort (should match Table_7 in
--     M025_tables_and_summary.xlsx: Black 1,535 (45.48%), White 1,382 (40.95%),
--     Asian 204 (6.04%), Unknown/Not Reported 165 (4.89%), Other 66 (1.96%),
--     Native HI/PI 11, AI/AN 10, NULL 2).
SELECT race,
       COUNT(*)                           AS n_total,
       SUM(is_malignant::INT)             AS n_malignant,
       ROUND(100.0 * SUM(is_malignant::INT) / COUNT(*), 2) AS rom_pct
FROM   m025_analytic_master_patient_v1
GROUP  BY race
ORDER  BY n_total DESC;

-- 0b. Pre-specified primary strata for M048 (powered for individual analysis):
--      'Black or African American', 'White', 'Asian'.
--      Smaller groups collapsed into 'Other / Unknown' for presentation only.
CREATE OR REPLACE VIEW m048_patient_master_v1 AS
SELECT
    p.*,
    CASE
        WHEN race = 'Black or African American'                  THEN 'Black'
        WHEN race = 'White'                                      THEN 'White'
        WHEN race = 'Asian'                                      THEN 'Asian'
        WHEN race IN ('Native Hawaiian or Other Pacific Islander',
                      'American Indian or Alaska Native',
                      'Other')                                    THEN 'Other'
        ELSE                                                          'Unknown'
    END AS race_strat
FROM m025_analytic_master_patient_v1 p;

-- Same for nodule master (race lives on the patient row; join through research_id)
CREATE OR REPLACE VIEW m048_nodule_master_v1 AS
SELECT
    n.*,
    p.race,
    CASE
        WHEN p.race = 'Black or African American'                THEN 'Black'
        WHEN p.race = 'White'                                    THEN 'White'
        WHEN p.race = 'Asian'                                    THEN 'Asian'
        WHEN p.race IN ('Native Hawaiian or Other Pacific Islander',
                        'American Indian or Alaska Native',
                        'Other')                                  THEN 'Other'
        ELSE                                                          'Unknown'
    END AS race_strat
FROM m025_analytic_master_nodule_v1 n
LEFT JOIN m025_analytic_master_patient_v1 p USING (research_id);

-- ----------------------------------------------------------------------------
-- 1. Per-race patient-level ROM by max TI-RADS category (mirrors M025 Table 3)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_rom_by_race_patient_v1 AS
SELECT
    race_strat,
    max_tirads_category_ever                 AS tr_category,
    COUNT(*)                                 AS n_total,
    SUM(is_malignant::INT)                   AS n_malignant,
    ROUND(100.0 * SUM(is_malignant::INT) / COUNT(*), 2) AS rom_pct
FROM   m048_patient_master_v1
WHERE  max_tirads_category_ever IS NOT NULL
GROUP  BY race_strat, max_tirads_category_ever
ORDER  BY race_strat, max_tirads_category_ever;

-- ----------------------------------------------------------------------------
-- 2. Per-race NODULE-level ROM by per-nodule TR (strict-eligible only)
--    Reuses analytic_eligible_strict_acr_pernodule from M025 mig_306.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_rom_by_race_nodule_v1 AS
SELECT
    race_strat,
    acr2017_tirads_category                  AS tr_category,
    COUNT(*)                                 AS n_total,
    SUM(nodule_path_proven_malignant::INT)   AS n_malignant,
    ROUND(100.0 * SUM(nodule_path_proven_malignant::INT) / COUNT(*), 2) AS rom_pct
FROM   m048_nodule_master_v1
WHERE  analytic_eligible_strict_acr_pernodule = TRUE
   AND acr2017_tirads_category IS NOT NULL
GROUP  BY race_strat, acr2017_tirads_category
ORDER  BY race_strat, acr2017_tirads_category;

-- ----------------------------------------------------------------------------
-- 3. Per-race threshold metrics (sens / spec / PPV / NPV at TR>=3, >=4, >=5)
--    Mirror M025 m025_threshold_metrics_v1 schema.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_threshold_metrics_v1 AS
WITH base AS (
    SELECT 'patient' AS grain, race_strat,
           TRY_CAST(regexp_extract(CAST(max_tirads_category_ever AS VARCHAR), '[0-9]+') AS INTEGER) AS tr,
           is_malignant::INT        AS y,
           1                        AS one
    FROM   m048_patient_master_v1
    WHERE  max_tirads_category_ever IS NOT NULL
    UNION ALL
    SELECT 'nodule', race_strat,
           TRY_CAST(regexp_extract(CAST(acr2017_tirads_category AS VARCHAR), '[0-9]+') AS INTEGER),
           nodule_path_proven_malignant::INT, 1
    FROM   m048_nodule_master_v1
    WHERE  analytic_eligible_strict_acr_pernodule = TRUE
       AND acr2017_tirads_category IS NOT NULL
),
thresholds(thr_label, thr_int) AS (
    VALUES ('TR>=TR3', 3), ('TR>=TR4', 4), ('TR>=TR5', 5)
)
SELECT
    grain,
    race_strat,
    thr_label                                                      AS threshold,
    SUM(CASE WHEN tr >= thr_int AND y = 1 THEN 1 ELSE 0 END)       AS tp,
    SUM(CASE WHEN tr >= thr_int AND y = 0 THEN 1 ELSE 0 END)       AS fp,
    SUM(CASE WHEN tr <  thr_int AND y = 1 THEN 1 ELSE 0 END)       AS fn,
    SUM(CASE WHEN tr <  thr_int AND y = 0 THEN 1 ELSE 0 END)       AS tn,
    -- Sens / Spec / PPV / NPV (raw, %; CIs computed in Python with Wilson)
    ROUND(100.0 * SUM(CASE WHEN tr >= thr_int AND y = 1 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN y = 1 THEN 1 ELSE 0 END), 0), 2)  AS sens_pct,
    ROUND(100.0 * SUM(CASE WHEN tr <  thr_int AND y = 0 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN y = 0 THEN 1 ELSE 0 END), 0), 2)  AS spec_pct,
    ROUND(100.0 * SUM(CASE WHEN tr >= thr_int AND y = 1 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN tr >= thr_int THEN 1 ELSE 0 END), 0), 2) AS ppv_pct,
    ROUND(100.0 * SUM(CASE WHEN tr <  thr_int AND y = 0 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN tr <  thr_int THEN 1 ELSE 0 END), 0), 2) AS npv_pct
FROM   base, thresholds
GROUP  BY grain, race_strat, thr_label, thr_int
ORDER  BY grain, race_strat, thr_label;

-- ----------------------------------------------------------------------------
-- 4. Per-race AUC (closed-form rank Mann–Whitney) at patient and nodule grain
--    Returns one AUC per (grain, race_strat); compare to overall M025 cohort
--    AUC (patient 0.6478; nodule 0.6399).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_auc_v1 AS
WITH long AS (
    SELECT 'patient' AS grain, race_strat,
           TRY_CAST(regexp_extract(CAST(max_tirads_category_ever AS VARCHAR), '[0-9]+') AS DOUBLE) AS pred,
           is_malignant::INT                AS y
    FROM   m048_patient_master_v1
    WHERE  max_tirads_category_ever IS NOT NULL
    UNION ALL
    SELECT 'nodule', race_strat,
           TRY_CAST(regexp_extract(CAST(acr2017_tirads_category AS VARCHAR), '[0-9]+') AS DOUBLE),
           nodule_path_proven_malignant::INT
    FROM   m048_nodule_master_v1
    WHERE  analytic_eligible_strict_acr_pernodule = TRUE
       AND acr2017_tirads_category IS NOT NULL
),
ranked AS (
    SELECT grain, race_strat, pred, y,
           AVG(CAST(rn AS DOUBLE)) OVER (PARTITION BY grain, race_strat, pred) AS avg_rank
    FROM (
        SELECT grain, race_strat, pred, y,
               ROW_NUMBER() OVER (PARTITION BY grain, race_strat ORDER BY pred) AS rn
        FROM long
    )
)
SELECT
    grain,
    race_strat,
    SUM(CASE WHEN y = 1 THEN 1 ELSE 0 END)                                 AS n_pos,
    SUM(CASE WHEN y = 0 THEN 1 ELSE 0 END)                                 AS n_neg,
    ROUND(
      ( SUM(CASE WHEN y = 1 THEN avg_rank ELSE 0 END)
        - SUM(CASE WHEN y = 1 THEN 1 ELSE 0 END)
          * (SUM(CASE WHEN y = 1 THEN 1 ELSE 0 END) + 1) / 2.0 )
      / NULLIF(SUM(CASE WHEN y = 1 THEN 1 ELSE 0 END)
               * SUM(CASE WHEN y = 0 THEN 1 ELSE 0 END), 0),
      4
    ) AS auc
FROM   ranked
GROUP  BY grain, race_strat
ORDER  BY grain, race_strat;

-- ----------------------------------------------------------------------------
-- 5. Per-race feature-component score distribution (test: do feature scores
--    differ by race? if yes, the calibration shift is mechanically explainable
--    at the feature level rather than at the gestalt-TR level)
--    Uses the 5 ACR 2017 components from imaging_nodule_master_v1.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_feature_distribution_v1 AS
SELECT
    race_strat,
    'composition'   AS feature, composition_pts   AS score, COUNT(*) AS n
FROM   m048_nodule_master_v1
WHERE  analytic_eligible_strict_acr_pernodule = TRUE
GROUP  BY race_strat, composition_pts
UNION ALL
SELECT race_strat, 'echogenicity', echogenicity_pts, COUNT(*)
FROM   m048_nodule_master_v1 WHERE analytic_eligible_strict_acr_pernodule = TRUE
GROUP  BY race_strat, echogenicity_pts
UNION ALL
SELECT race_strat, 'shape', shape_pts, COUNT(*)
FROM   m048_nodule_master_v1 WHERE analytic_eligible_strict_acr_pernodule = TRUE
GROUP  BY race_strat, shape_pts
UNION ALL
SELECT race_strat, 'margin', margin_pts, COUNT(*)
FROM   m048_nodule_master_v1 WHERE analytic_eligible_strict_acr_pernodule = TRUE
GROUP  BY race_strat, margin_pts
UNION ALL
SELECT race_strat, 'foci', foci_pts, COUNT(*)
FROM   m048_nodule_master_v1 WHERE analytic_eligible_strict_acr_pernodule = TRUE
GROUP  BY race_strat, foci_pts;

-- ----------------------------------------------------------------------------
-- 6. Per-race FNA-eligibility audit (mirror M025 1,553 unnecessary / 472 missed)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_fna_compliance_v1 AS
SELECT
    race_strat,
    SUM(CASE WHEN predicted_pos_TR4 = TRUE THEN 1 ELSE 0 END)         AS n_above_thr,
    SUM(CASE WHEN predicted_pos_TR4 = FALSE THEN 1 ELSE 0 END)        AS n_below_thr,
    SUM(CASE WHEN predicted_pos_TR4 = TRUE  AND is_malignant = TRUE  THEN 1 ELSE 0 END) AS tp_thr4,
    SUM(CASE WHEN predicted_pos_TR4 = TRUE  AND is_malignant = FALSE THEN 1 ELSE 0 END) AS fp_thr4,
    SUM(CASE WHEN predicted_pos_TR4 = FALSE AND is_malignant = TRUE  THEN 1 ELSE 0 END) AS fn_thr4,
    SUM(CASE WHEN predicted_pos_TR4 = FALSE AND is_malignant = FALSE THEN 1 ELSE 0 END) AS tn_thr4
FROM   m048_patient_master_v1
GROUP  BY race_strat
ORDER  BY race_strat;

-- ----------------------------------------------------------------------------
-- 7. Pre-specified Bethesda × race × TR contingency (descriptive)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_bethesda_x_race_x_tr_v1 AS
SELECT race_strat, bethesda_bucket, max_tirads_category_ever AS tr_category,
       COUNT(*) AS n,
       SUM(is_malignant::INT) AS n_malignant
FROM   m048_patient_master_v1
WHERE  max_tirads_category_ever IS NOT NULL
GROUP  BY race_strat, bethesda_bucket, max_tirads_category_ever
ORDER  BY race_strat, bethesda_bucket, max_tirads_category_ever;

-- ----------------------------------------------------------------------------
-- 8. QA gates (race coverage, strict-eligible nodule counts per race)
-- ----------------------------------------------------------------------------
SELECT 'patient_total'           AS gate,
       (SELECT COUNT(*) FROM m048_patient_master_v1) AS n
UNION ALL
SELECT 'patient_with_known_race',
       (SELECT COUNT(*) FROM m048_patient_master_v1 WHERE race_strat <> 'Unknown')
UNION ALL
SELECT 'nodule_strict_total',
       (SELECT COUNT(*) FROM m048_nodule_master_v1
        WHERE analytic_eligible_strict_acr_pernodule = TRUE)
UNION ALL
SELECT 'nodule_strict_black',
       (SELECT COUNT(*) FROM m048_nodule_master_v1
        WHERE analytic_eligible_strict_acr_pernodule = TRUE AND race_strat='Black')
UNION ALL
SELECT 'nodule_strict_white',
       (SELECT COUNT(*) FROM m048_nodule_master_v1
        WHERE analytic_eligible_strict_acr_pernodule = TRUE AND race_strat='White')
UNION ALL
SELECT 'nodule_strict_asian',
       (SELECT COUNT(*) FROM m048_nodule_master_v1
        WHERE analytic_eligible_strict_acr_pernodule = TRUE AND race_strat='Asian');


-- ============================================================================
-- M048 v2 COVARIATE EXPANSION (added 2026-05-05 after senior-author pushback
-- on v1: raw racial disparities need adjustment for genetics access, imaging
-- utilization, multinodular burden, and benign-diagnosis distribution before
-- being reported as a calibration finding).
--
-- Pre-specified hypothesis: the per-race per-TR ROM disparities observed in
-- the v1 analysis (TR5 nodule ROM Black 14.5% / White 30.7% / Asian 54.0%)
-- will substantially attenuate after adjustment for:
--   (a) per-patient nodule count (multinodular burden)
--   (b) preoperative molecular genetics testing access
--   (c) nuclear-medicine imaging utilization (functional-vs-structural workup)
--   (d) background path / benign-diagnosis distribution (CLT, Graves,
--       hyperplasia, follicular adenoma, NIFTP, FT-UMP)
--   (e) surgical procedure type (lobectomy vs total thyroidectomy)
--   (f) age, sex, surgery era
-- Residual race effect after adjustment is the publishable finding.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 9. Per-patient nodule count (multinodular burden as a covariate)
--    From canonical_us_nodule_v2: distinct nodules per research_id.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_nodule_count_per_patient_v1 AS
SELECT research_id,
       COUNT(DISTINCT nodule_master_id)                  AS n_nodules_total,
       SUM(CASE WHEN analytic_eligible_strict_acr_pernodule = TRUE
                THEN 1 ELSE 0 END)                       AS n_nodules_strict,
       MAX(TRY_CAST(regexp_extract(CAST(acr2017_tirads_category AS VARCHAR),
                                   '[0-9]+') AS INTEGER)) AS max_tr_observed
FROM   m048_nodule_master_v1
GROUP  BY research_id;

CREATE OR REPLACE TABLE m048_nodule_count_by_race_v1 AS
SELECT
    p.race_strat,
    COUNT(*)                                              AS n_patients,
    ROUND(AVG(n.n_nodules_total), 2)                      AS mean_nodules,
    MEDIAN(n.n_nodules_total)                             AS median_nodules,
    QUANTILE_CONT(n.n_nodules_total, 0.75)                AS q75_nodules,
    MAX(n.n_nodules_total)                                AS max_nodules,
    SUM(CASE WHEN n.n_nodules_total = 1 THEN 1 ELSE 0 END) AS n_solitary,
    SUM(CASE WHEN n.n_nodules_total BETWEEN 2 AND 4 THEN 1 ELSE 0 END) AS n_2to4,
    SUM(CASE WHEN n.n_nodules_total >= 5 THEN 1 ELSE 0 END) AS n_5plus
FROM   m048_patient_master_v1 p
LEFT JOIN m048_nodule_count_per_patient_v1 n USING (research_id)
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 10. Preoperative molecular genetics testing access by race
--     canonical_molecular_genetics_v2 (1,151 patients with any test).
--     Stratifies: any test / Afirma / ThyroSeq / mutation panel; pre-op only.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_genetics_access_by_race_v1 AS
WITH genetics AS (
    SELECT research_id,
           MAX(CASE WHEN assay_type ILIKE '%afirma%'   THEN 1 ELSE 0 END) AS had_afirma,
           MAX(CASE WHEN assay_type ILIKE '%thyroseq%' THEN 1 ELSE 0 END) AS had_thyroseq,
           MAX(CASE WHEN assay_type ILIKE '%mutation%'
                     OR assay_type ILIKE '%panel%'      THEN 1 ELSE 0 END) AS had_mutation_panel,
           MAX(1)                                                          AS had_any_genetics,
           MAX(CASE WHEN result_category ILIKE '%suspicious%'
                     OR result_category ILIKE '%positive%' THEN 1 ELSE 0 END) AS had_suspicious_result
    FROM   canonical_molecular_genetics_v2
    GROUP  BY research_id
)
SELECT
    p.race_strat,
    COUNT(*)                                              AS n_patients,
    SUM(COALESCE(g.had_any_genetics, 0))                  AS n_with_any_genetics,
    ROUND(100.0 * SUM(COALESCE(g.had_any_genetics, 0)) / COUNT(*), 2) AS pct_with_genetics,
    SUM(COALESCE(g.had_afirma, 0))                        AS n_afirma,
    SUM(COALESCE(g.had_thyroseq, 0))                      AS n_thyroseq,
    SUM(COALESCE(g.had_mutation_panel, 0))                AS n_mutation_panel,
    SUM(COALESCE(g.had_suspicious_result, 0))             AS n_suspicious_result,
    -- Cross with malignancy
    ROUND(100.0 * SUM(CASE WHEN g.had_any_genetics = 1 AND p.is_malignant THEN 1 ELSE 0 END)
                  / NULLIF(SUM(COALESCE(g.had_any_genetics, 0)), 0), 2) AS rom_pct_in_genetics_tested
FROM   m048_patient_master_v1 p
LEFT JOIN genetics g USING (research_id)
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 11. Nuclear-medicine and other imaging utilization by race
--     nuclear_med (1,148 pts / 2,220 rows) — radioiodine uptake / scintigraphy
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_imaging_utilization_by_race_v1 AS
WITH nm AS (
    SELECT research_id,
           1                                                AS had_any_nm,
           MAX(CASE WHEN scan_type ILIKE '%RAI%'
                     OR scan_type ILIKE '%I-123%'
                     OR scan_type ILIKE '%I-131%'           THEN 1 ELSE 0 END) AS had_radioiodine,
           MAX(CASE WHEN scan_type ILIKE '%uptake%'         THEN 1 ELSE 0 END) AS had_uptake,
           MAX(CASE WHEN scan_type ILIKE '%scinti%'         THEN 1 ELSE 0 END) AS had_scintigraphy
    FROM   nuclear_med
    GROUP  BY research_id
)
SELECT
    p.race_strat,
    COUNT(*)                                                AS n_patients,
    SUM(COALESCE(nm.had_any_nm, 0))                         AS n_any_nuclear_med,
    ROUND(100.0 * SUM(COALESCE(nm.had_any_nm, 0)) / COUNT(*), 2) AS pct_any_nm,
    SUM(COALESCE(nm.had_radioiodine, 0))                    AS n_radioiodine,
    SUM(COALESCE(nm.had_uptake, 0))                         AS n_uptake_scan,
    -- Total US exam burden (already in patient master)
    ROUND(AVG(p.n_us_exams), 2)                             AS mean_us_exams,
    MEDIAN(p.n_us_exams)                                    AS median_us_exams
FROM   m048_patient_master_v1 p
LEFT JOIN nm USING (research_id)
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 12. Background pathology / benign diagnosis distribution by race
--     Pulls from canonical_path_* — Hashimoto's/CLT, Graves, MNG, hyperplasia,
--     follicular adenoma, NIFTP, FT-UMP. These are background drivers of
--     operative-cohort selection and may explain TR-stratified ROM divergence.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_background_path_by_race_v1 AS
SELECT
    p.race_strat,
    COUNT(*) AS n_patients,
    -- Autoimmune / inflammatory
    SUM(CASE WHEN p.histology_final ILIKE '%hashimoto%'
              OR p.histology_final ILIKE '%lymphocytic thyroiditis%'
              OR p.histology_final ILIKE '%CLT%'              THEN 1 ELSE 0 END) AS n_clt_hashimoto,
    SUM(CASE WHEN p.histology_final ILIKE '%graves%'          THEN 1 ELSE 0 END) AS n_graves,
    -- Benign nodular disease
    SUM(CASE WHEN p.histology_final ILIKE '%multinodular%'
              OR p.histology_final ILIKE '%MNG%'
              OR p.histology_final ILIKE '%nodular goiter%'   THEN 1 ELSE 0 END) AS n_mng,
    SUM(CASE WHEN p.histology_final ILIKE '%follicular adenoma%' THEN 1 ELSE 0 END) AS n_follicular_adenoma,
    SUM(CASE WHEN p.histology_final ILIKE '%hyperplasia%'     THEN 1 ELSE 0 END) AS n_hyperplasia,
    -- Borderline (potential reclassification effect)
    SUM(CASE WHEN p.histology_final ILIKE '%NIFTP%'           THEN 1 ELSE 0 END) AS n_niftp,
    SUM(CASE WHEN p.histology_final ILIKE '%FTUMP%'
              OR p.histology_final ILIKE '%FT-UMP%'           THEN 1 ELSE 0 END) AS n_ftump,
    -- Percentages
    ROUND(100.0 * SUM(CASE WHEN p.histology_final ILIKE '%hashimoto%'
                            OR p.histology_final ILIKE '%lymphocytic%' THEN 1 ELSE 0 END)
                  / COUNT(*), 2) AS pct_clt_hashimoto,
    ROUND(100.0 * SUM(CASE WHEN p.histology_final ILIKE '%multinodular%'
                            OR p.histology_final ILIKE '%MNG%' THEN 1 ELSE 0 END)
                  / COUNT(*), 2) AS pct_mng
FROM   m048_patient_master_v1 p
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 13. Surgical procedure type by race (lobectomy vs total thyroidectomy)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_procedure_by_race_v1 AS
SELECT
    p.race_strat,
    COUNT(*) AS n_patients,
    SUM(CASE WHEN p.surg_procedure_type ILIKE '%lobectomy%'        THEN 1 ELSE 0 END) AS n_lobectomy,
    SUM(CASE WHEN p.surg_procedure_type ILIKE '%total%'
              OR p.surg_procedure_type ILIKE '%TT%'                THEN 1 ELSE 0 END) AS n_total_thyroidectomy,
    SUM(CASE WHEN p.surg_procedure_type ILIKE '%completion%'       THEN 1 ELSE 0 END) AS n_completion,
    ROUND(100.0 * SUM(CASE WHEN p.surg_procedure_type ILIKE '%lobectomy%' THEN 1 ELSE 0 END)
                  / COUNT(*), 2) AS pct_lobectomy
FROM   m048_patient_master_v1 p
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 14. Build the EXTENDED ANALYTIC MASTER (one row per patient with all
--     covariates joined). This is what the multivariable logistic regression
--     in Python will consume. The Cursor analysis pipeline runs:
--       statsmodels.Logit(is_malignant ~ race_strat + n_nodules_total
--                                       + had_any_genetics + had_any_nm
--                                       + has_clt + has_mng + has_graves
--                                       + age_at_surgery + sex
--                                       + surg_year + surg_procedure_type)
--     and reports ORs (95% CI) for each race level vs White reference.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_extended_patient_master_v1 AS
SELECT
    p.research_id,
    p.race_strat,
    p.race AS race_raw,
    p.age_at_surgery,
    p.sex,
    p.surg_year,
    p.surg_procedure_type,
    p.max_tirads_category_ever,
    TRY_CAST(regexp_extract(CAST(p.max_tirads_category_ever AS VARCHAR), '[0-9]+') AS INTEGER) AS max_tr_int,
    p.n_us_exams,
    p.bethesda_final,
    p.bethesda_bucket,
    p.histology_final,
    p.histology_category,
    p.is_malignant,
    p.predicted_pos_TR3,
    p.predicted_pos_TR4,
    p.predicted_pos_TR5,
    -- Multinodular burden
    nc.n_nodules_total,
    nc.n_nodules_strict,
    CASE WHEN nc.n_nodules_total = 1 THEN 'solitary'
         WHEN nc.n_nodules_total BETWEEN 2 AND 4 THEN '2-4'
         WHEN nc.n_nodules_total >= 5 THEN '5+'
         ELSE 'unknown' END AS nodule_burden_cat,
    -- Genetics access
    g.had_any_genetics,
    g.had_afirma,
    g.had_thyroseq,
    g.had_mutation_panel,
    g.had_suspicious_result,
    -- Nuclear medicine utilization
    nm.had_any_nm,
    nm.had_radioiodine,
    -- Background path
    CASE WHEN p.histology_final ILIKE '%hashimoto%' OR p.histology_final ILIKE '%lymphocytic%' THEN 1 ELSE 0 END AS has_clt,
    CASE WHEN p.histology_final ILIKE '%graves%'      THEN 1 ELSE 0 END AS has_graves,
    CASE WHEN p.histology_final ILIKE '%multinodular%'
          OR p.histology_final ILIKE '%MNG%'          THEN 1 ELSE 0 END AS has_mng,
    CASE WHEN p.histology_final ILIKE '%adenoma%'    THEN 1 ELSE 0 END AS has_follicular_adenoma,
    CASE WHEN p.histology_final ILIKE '%NIFTP%'      THEN 1 ELSE 0 END AS has_niftp,
    CASE WHEN p.histology_final ILIKE '%FTUMP%'
          OR p.histology_final ILIKE '%FT-UMP%'      THEN 1 ELSE 0 END AS has_ftump
FROM   m048_patient_master_v1 p
LEFT   JOIN m048_nodule_count_per_patient_v1 nc USING (research_id)
LEFT   JOIN (
    SELECT research_id,
           MAX(CASE WHEN assay_type ILIKE '%afirma%'   THEN 1 ELSE 0 END) AS had_afirma,
           MAX(CASE WHEN assay_type ILIKE '%thyroseq%' THEN 1 ELSE 0 END) AS had_thyroseq,
           MAX(CASE WHEN assay_type ILIKE '%mutation%'
                     OR assay_type ILIKE '%panel%'      THEN 1 ELSE 0 END) AS had_mutation_panel,
           MAX(1)                                                          AS had_any_genetics,
           MAX(CASE WHEN result_category ILIKE '%suspicious%'
                     OR result_category ILIKE '%positive%' THEN 1 ELSE 0 END) AS had_suspicious_result
    FROM   canonical_molecular_genetics_v2
    GROUP  BY research_id
) g USING (research_id)
LEFT   JOIN (
    SELECT research_id,
           1 AS had_any_nm,
           MAX(CASE WHEN scan_type ILIKE '%RAI%' OR scan_type ILIKE '%I-123%'
                     OR scan_type ILIKE '%I-131%' THEN 1 ELSE 0 END) AS had_radioiodine
    FROM   nuclear_med
    GROUP  BY research_id
) nm USING (research_id);

-- ----------------------------------------------------------------------------
-- 15. Pre-specified extended-master nodule view for nodule-grain mediation
--     analysis (per-nodule outcome with patient-level covariates joined).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_extended_nodule_master_v1 AS
SELECT
    n.*,
    em.race_strat,
    em.age_at_surgery,
    em.sex,
    em.surg_year,
    em.n_nodules_total,
    em.nodule_burden_cat,
    em.had_any_genetics,
    em.had_any_nm,
    em.has_clt,
    em.has_graves,
    em.has_mng,
    em.has_follicular_adenoma,
    em.has_niftp,
    em.has_ftump,
    em.surg_procedure_type
FROM   m025_analytic_master_nodule_v1 n
LEFT   JOIN m048_extended_patient_master_v1 em USING (research_id);

-- ----------------------------------------------------------------------------
-- 16. Extended QA gates (covariate-coverage sanity)
-- ----------------------------------------------------------------------------
SELECT 'extended_master_total' AS gate, COUNT(*) AS n FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_genetics_any',     SUM(COALESCE(had_any_genetics, 0))  FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_nuclear_med_any',  SUM(COALESCE(had_any_nm, 0))        FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_nodule_count',     SUM(CASE WHEN n_nodules_total IS NOT NULL THEN 1 ELSE 0 END)
                                FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_clt',              SUM(has_clt)                        FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_graves',           SUM(has_graves)                     FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_mng',              SUM(has_mng)                        FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'with_niftp',            SUM(has_niftp)                      FROM m048_extended_patient_master_v1
UNION ALL
SELECT 'reconciles_with_m025_patient_n',
       CASE WHEN (SELECT COUNT(*) FROM m048_extended_patient_master_v1) = 3375
            THEN 1 ELSE 0 END;
