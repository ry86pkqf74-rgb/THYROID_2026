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
-- 7b. Bethesda x race x TR cell-level ROM (for v3 supplementary heatmap)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_bethesda_x_race_x_tr_rom_v1 AS
SELECT race_strat,
       bethesda_bucket,
       max_tirads_category_ever AS tr_category,
       COUNT(*)                 AS n,
       SUM(is_malignant::INT)   AS n_malignant,
       CASE WHEN COUNT(*) > 0
            THEN ROUND(100.0 * SUM(is_malignant::INT) / COUNT(*), 2)
            ELSE NULL END       AS rom_pct
FROM   m048_patient_master_v1
WHERE  max_tirads_category_ever IS NOT NULL
   AND bethesda_bucket IS NOT NULL
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
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%afirma%' THEN 1 ELSE 0 END) AS had_afirma,
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%thyroseq%' THEN 1 ELSE 0 END) AS had_thyroseq,
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%mutation%'
                     OR COALESCE(platform, platform_raw) ILIKE '%panel%' THEN 1 ELSE 0 END) AS had_mutation_panel,
           MAX(1)                                                          AS had_any_genetics,
           MAX(CASE WHEN overall_result_class ILIKE '%suspicious%'
                     OR overall_result_class ILIKE '%positive%'
                     OR overall_result_class ILIKE '%malignant%'
                     OR high_risk_marker_flag IS TRUE THEN 1 ELSE 0 END) AS had_suspicious_result
    FROM   main.canonical_molecular_genetics_v2
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
           MAX(CASE WHEN scantype ILIKE '%RAI%'
                     OR scantype ILIKE '%I-123%'
                     OR scantype ILIKE '%I-131%'
                     OR radiotracer ILIKE '%I-131%'
                     OR radiotracer ILIKE '%I-123%'       THEN 1 ELSE 0 END) AS had_radioiodine,
           MAX(CASE WHEN scantype ILIKE '%uptake%'
                     OR indication_text ILIKE '%uptake%'  THEN 1 ELSE 0 END) AS had_uptake,
           MAX(CASE WHEN scantype ILIKE '%scinti%'
                     OR findings_text ILIKE '%scintigraph%' THEN 1 ELSE 0 END) AS had_scintigraphy
    FROM   main.nuclear_med
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
    p.surg_first_date,
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
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%afirma%' THEN 1 ELSE 0 END) AS had_afirma,
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%thyroseq%' THEN 1 ELSE 0 END) AS had_thyroseq,
           MAX(CASE WHEN COALESCE(platform, platform_raw) ILIKE '%mutation%'
                     OR COALESCE(platform, platform_raw) ILIKE '%panel%' THEN 1 ELSE 0 END) AS had_mutation_panel,
           MAX(1)                                                          AS had_any_genetics,
           MAX(CASE WHEN overall_result_class ILIKE '%suspicious%'
                     OR overall_result_class ILIKE '%positive%'
                     OR overall_result_class ILIKE '%malignant%'
                     OR high_risk_marker_flag IS TRUE THEN 1 ELSE 0 END) AS had_suspicious_result
    FROM   main.canonical_molecular_genetics_v2
    GROUP  BY research_id
) g USING (research_id)
LEFT   JOIN (
    SELECT research_id,
           1 AS had_any_nm,
           MAX(CASE WHEN scantype ILIKE '%RAI%' OR scantype ILIKE '%I-123%'
                     OR scantype ILIKE '%I-131%' OR radiotracer ILIKE '%I-131%'
                     OR radiotracer ILIKE '%I-123%' THEN 1 ELSE 0 END) AS had_radioiodine
    FROM   main.nuclear_med
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


-- ============================================================================
-- M048 v3 EXPANSION (added 2026-05-05): FNA pattern, tumor biology
-- descriptors, presentation context.
--
-- IMPORTANT CAUSAL NOTE:
--   * Sections 17-19 are PRE-SURGERY variables (FNA pattern, Bethesda,
--     concordance, US-to-surgery interval) — eligible to be adjusters in
--     primary regression models alongside the v2 covariates.
--   * Sections 20-22 are POST-SURGERY tumor-biology DESCRIPTORS (size,
--     multifocality, histology subtype, ETE, LN, frozen section findings).
--     These are CONSEQUENCES of the surgery, not predictors of it. They
--     MUST NOT be added as adjusters to the malignancy-outcome regression
--     (that would be adjusting for the outcome). Instead they are reported
--     as a per-race × per-TR interpretive table that distinguishes
--     "over-referral of indolent disease" (Black-TR5 → smaller, lower-stage
--     tumors) from "under-referral until aggressive presentation"
--     (Asian-TR5 → larger, higher-stage tumors).
--   * Sections 23-25 are mixed pre-/post-surgery and are descriptive only.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 17. FNA pattern by race (eligible adjuster)
--     Counts FNAs per patient, categorizes Bethesda distribution, flags
--     repeat FNA workups (a marker of indeterminate-cytology pathway).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_fna_pattern_by_race_v1 AS
WITH fna AS (
    SELECT research_id,
           COUNT(*)                                          AS n_fnas_total,
           COUNT(DISTINCT fna_date_resolved)                 AS n_distinct_fna_dates,
           MAX(CASE WHEN bethesda_final_num = 1             THEN 1 ELSE 0 END) AS ever_b1,
           MAX(CASE WHEN bethesda_final_num = 3             THEN 1 ELSE 0 END) AS ever_b3,
           MAX(CASE WHEN bethesda_final_num = 4             THEN 1 ELSE 0 END) AS ever_b4,
           MAX(CASE WHEN bethesda_final_num = 5             THEN 1 ELSE 0 END) AS ever_b5,
           MAX(CASE WHEN bethesda_final_num = 6             THEN 1 ELSE 0 END) AS ever_b6,
           CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END         AS had_repeat_fna
    FROM   main.canonical_fna_events_v1
    GROUP  BY research_id
)
SELECT
    p.race_strat,
    COUNT(*)                                                 AS n_patients,
    SUM(CASE WHEN f.research_id IS NOT NULL THEN 1 ELSE 0 END) AS n_with_fna,
    ROUND(100.0 * SUM(CASE WHEN f.research_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_fna,
    ROUND(AVG(COALESCE(f.n_fnas_total, 0)), 2)               AS mean_fnas_per_patient,
    SUM(COALESCE(f.had_repeat_fna, 0))                       AS n_repeat_fna,
    ROUND(100.0 * SUM(COALESCE(f.had_repeat_fna, 0))
                  / NULLIF(SUM(CASE WHEN f.research_id IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS pct_repeat_fna_among_biopsied,
    SUM(COALESCE(f.ever_b1, 0))                              AS n_ever_b1,
    SUM(COALESCE(f.ever_b3, 0))                              AS n_ever_b3,
    SUM(COALESCE(f.ever_b4, 0))                              AS n_ever_b4,
    SUM(COALESCE(f.ever_b5, 0))                              AS n_ever_b5,
    SUM(COALESCE(f.ever_b6, 0))                              AS n_ever_b6
FROM   m048_patient_master_v1 p
LEFT   JOIN fna f USING (research_id)
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 18. FNA-to-surgery interval by race (workup-pathway speed proxy)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_fna_to_surgery_interval_by_race_v1 AS
WITH fna_first AS (
    SELECT research_id, MIN(TRY_CAST(fna_date_resolved AS DATE)) AS first_fna_date
    FROM   main.canonical_fna_events_v1
    WHERE  fna_date_resolved IS NOT NULL
    GROUP  BY research_id
)
SELECT
    p.race_strat,
    COUNT(CASE WHEN ff.first_fna_date IS NOT NULL THEN 1 END) AS n_with_dated_fna,
    ROUND(AVG(DATE_DIFF('day', ff.first_fna_date, p.surg_first_date)), 1)        AS mean_days_fna_to_surg,
    MEDIAN(DATE_DIFF('day', ff.first_fna_date, p.surg_first_date))               AS median_days_fna_to_surg,
    QUANTILE_CONT(DATE_DIFF('day', ff.first_fna_date, p.surg_first_date), 0.75)  AS q75_days,
    QUANTILE_CONT(DATE_DIFF('day', ff.first_fna_date, p.surg_first_date), 0.95)  AS q95_days,
    SUM(CASE WHEN DATE_DIFF('day', ff.first_fna_date, p.surg_first_date) <= 30  THEN 1 ELSE 0 END) AS n_30d_or_less,
    SUM(CASE WHEN DATE_DIFF('day', ff.first_fna_date, p.surg_first_date) BETWEEN 31 AND 90  THEN 1 ELSE 0 END) AS n_31_to_90d,
    SUM(CASE WHEN DATE_DIFF('day', ff.first_fna_date, p.surg_first_date) BETWEEN 91 AND 365 THEN 1 ELSE 0 END) AS n_91_to_365d,
    SUM(CASE WHEN DATE_DIFF('day', ff.first_fna_date, p.surg_first_date) > 365 THEN 1 ELSE 0 END)  AS n_over_365d
FROM   m048_patient_master_v1 p
LEFT   JOIN fna_first ff USING (research_id)
WHERE  p.surg_first_date IS NOT NULL
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 19. FNA-path concordance by race (pre-surgery cytology agreement with
--     final operative pathology — relevant interpretive variable)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_fna_path_concordance_by_race_v1 AS
SELECT
    race_strat,
    fna_path_concordance_category,
    COUNT(*)                                                 AS n,
    SUM(is_malignant::INT)                                   AS n_malignant,
    ROUND(100.0 * SUM(is_malignant::INT) / COUNT(*), 2)      AS rom_pct
FROM   m048_patient_master_v1
WHERE  fna_path_concordance_category IS NOT NULL
GROUP  BY race_strat, fna_path_concordance_category
ORDER  BY race_strat, fna_path_concordance_category;

-- ----------------------------------------------------------------------------
-- 20. Tumor biology DESCRIPTORS by race (POST-SURGERY; for interpretive
--     'over- vs under-referral' table — NOT for adjustment regressions)
--     Restricted to malignant patients (is_malignant = TRUE).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_tumor_biology_descriptors_by_race_v1 AS
-- Issue 2 fix: canonical_path_malignant_events_v1.multifocality_flag is NULL
-- for the entire 6469-row table and number_of_tumors is also NULL. The
-- M048-specific cohort_m048_tnm_multifocal_v1 in manuscript_workspace has
-- the correct patient-grain multifocal_flag_path (39.6% True among malignant
-- M048 patients) plus n_tumors and tumor_size_cm. Source tumour-size and
-- multifocality from there; keep size_greatest_dimension_cm as a fallback.
WITH multifoc AS (
    SELECT research_id,
           CASE WHEN multifocal_flag_path IS TRUE THEN 1 ELSE 0 END AS multifocal_flag,
           n_tumors                                                AS n_malignant_tumors,
           tumor_size_cm                                            AS m048_tumor_size_cm
    FROM   manuscript_workspace.cohort_m048_tnm_multifocal_v1
),
path_size_fallback AS (
    SELECT research_id,
           MAX(COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery)) AS max_tumor_size_cm,
           MIN(COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery)) AS min_tumor_size_cm
    FROM   main.canonical_path_malignant_events_v1
    GROUP  BY research_id
),
path_per_patient AS (
    SELECT  m.research_id,
            COALESCE(m.n_malignant_tumors, 0)                       AS n_malignant_tumors,
            COALESCE(m.multifocal_flag, 0)                          AS multifocal_flag,
            COALESCE(m.m048_tumor_size_cm, p.max_tumor_size_cm)     AS max_tumor_size_cm,
            COALESCE(p.min_tumor_size_cm, m.m048_tumor_size_cm)     AS min_tumor_size_cm
    FROM    multifoc m
    FULL OUTER JOIN path_size_fallback p USING (research_id)
)
SELECT
    p.race_strat,
    p.max_tirads_category_ever                               AS tr_category,
    COUNT(*)                                                 AS n_malignant_in_cell,
    ROUND(AVG(pp.max_tumor_size_cm), 2)                      AS mean_tumor_size_cm,
    MEDIAN(pp.max_tumor_size_cm)                             AS median_tumor_size_cm,
    SUM(CASE WHEN pp.max_tumor_size_cm < 1.0  THEN 1 ELSE 0 END) AS n_micro_lt_1cm,
    SUM(CASE WHEN pp.max_tumor_size_cm BETWEEN 1.0 AND 4.0 THEN 1 ELSE 0 END) AS n_1_to_4cm,
    SUM(CASE WHEN pp.max_tumor_size_cm > 4.0 THEN 1 ELSE 0 END) AS n_gt_4cm,
    SUM(pp.multifocal_flag)                                  AS n_multifocal,
    ROUND(AVG(pp.n_malignant_tumors), 2)                     AS mean_malignant_tumors_per_patient
FROM   m048_patient_master_v1 p
JOIN   path_per_patient pp USING (research_id)
WHERE  p.is_malignant = TRUE
GROUP  BY p.race_strat, p.max_tirads_category_ever
ORDER  BY p.race_strat, p.max_tirads_category_ever;

-- ----------------------------------------------------------------------------
-- 21. Histologic subtype distribution by race (DESCRIPTIVE)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_histology_subtype_by_race_v1 AS
SELECT
    race_strat,
    histology_category,
    COUNT(*)                                                 AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY race_strat), 2) AS pct_within_race
FROM   m048_patient_master_v1
WHERE  is_malignant = TRUE
   AND histology_category IS NOT NULL
GROUP  BY race_strat, histology_category
ORDER  BY race_strat, n_patients DESC;

-- ----------------------------------------------------------------------------
-- 22. ETE + LN involvement by race (DESCRIPTIVE; M044 ETE canonical)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_aggressive_features_by_race_v1 AS
SELECT
    p.race_strat,
    COUNT(*)                                                 AS n_malignant,
    SUM(CASE WHEN ete.ete_grade IN ('microscopic', 'gross') THEN 1 ELSE 0 END) AS n_any_ete,
    SUM(CASE WHEN ete.ete_grade = 'microscopic' THEN 1 ELSE 0 END) AS n_micro_ete,
    SUM(CASE WHEN ete.ete_grade = 'gross'       THEN 1 ELSE 0 END) AS n_gross_ete,
    SUM(CASE WHEN ln.ln_any_positive IS TRUE    THEN 1 ELSE 0 END) AS n_ln_positive,
    ROUND(100.0 * SUM(CASE WHEN ete.ete_grade IN ('microscopic', 'gross') THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_any_ete,
    ROUND(100.0 * SUM(CASE WHEN ln.ln_any_positive IS TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_ln_positive
FROM   m048_patient_master_v1 p
LEFT   JOIN main.canonical_ete_event_resolved_v1 ete USING (research_id)
LEFT   JOIN ln_master_rollup_v1                   ln  USING (research_id)
WHERE  p.is_malignant = TRUE
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 23. Frozen section utilization by race (presentation context; surgical
--     decision-making proxy)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_frozen_section_by_race_v1 AS
SELECT
    p.race_strat,
    COUNT(*)                                                 AS n_patients,
    SUM(CASE WHEN fs.research_id IS NOT NULL THEN 1 ELSE 0 END) AS n_with_frozen,
    ROUND(100.0 * SUM(CASE WHEN fs.research_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_frozen
FROM   m048_patient_master_v1 p
LEFT   JOIN (
    SELECT DISTINCT research_id FROM main.canonical_frozen_section_events_v1
) fs USING (research_id)
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 24. Time from index US to surgery by race (workup-pathway proxy;
--     short = direct-to-OR, long = surveillance pathway)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_us_to_surgery_interval_by_race_v1 AS
WITH us_first AS (
    SELECT research_id, MIN(TRY_CAST(exam_date AS DATE)) AS first_us_date
    FROM   main.canonical_us_nodule_v2
    WHERE  exam_date IS NOT NULL
    GROUP  BY research_id
)
SELECT
    p.race_strat,
    COUNT(CASE WHEN uf.first_us_date IS NOT NULL THEN 1 END) AS n_with_dated_us,
    ROUND(AVG(DATE_DIFF('day', uf.first_us_date, p.surg_first_date)), 1)        AS mean_days_us_to_surg,
    MEDIAN(DATE_DIFF('day', uf.first_us_date, p.surg_first_date))               AS median_days_us_to_surg,
    QUANTILE_CONT(DATE_DIFF('day', uf.first_us_date, p.surg_first_date), 0.75)  AS q75_days,
    SUM(CASE WHEN DATE_DIFF('day', uf.first_us_date, p.surg_first_date) <= 90  THEN 1 ELSE 0 END) AS n_le_90d,
    SUM(CASE WHEN DATE_DIFF('day', uf.first_us_date, p.surg_first_date) BETWEEN 91 AND 365  THEN 1 ELSE 0 END) AS n_91_to_365d,
    SUM(CASE WHEN DATE_DIFF('day', uf.first_us_date, p.surg_first_date) > 365 THEN 1 ELSE 0 END)  AS n_gt_365d
FROM   m048_patient_master_v1 p
LEFT   JOIN us_first uf USING (research_id)
WHERE  p.surg_first_date IS NOT NULL
GROUP  BY p.race_strat
ORDER  BY p.race_strat;

-- ----------------------------------------------------------------------------
-- 25. v3 EXTENDED-EXTENDED patient master (joins FNA pattern, intervals,
--     and tumor-biology descriptors onto v2 master)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_v3_patient_master_v1 AS
WITH fna AS (
    SELECT research_id,
           COUNT(*) AS n_fnas_total,
           CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END AS had_repeat_fna,
           MIN(TRY_CAST(fna_date_resolved AS DATE)) AS first_fna_date
    FROM   main.canonical_fna_events_v1
    GROUP  BY research_id
),
us_first AS (
    SELECT research_id, MIN(TRY_CAST(exam_date AS DATE)) AS first_us_date
    FROM   main.canonical_us_nodule_v2
    WHERE  exam_date IS NOT NULL
    GROUP  BY research_id
),
-- Issue 2 fix (mirror): pull multifocality + n_tumors from
-- manuscript_workspace.cohort_m048_tnm_multifocal_v1 (the canonical event
-- table has all-NULL multifocality_flag). Keep canonical size as a fallback.
multifoc_join AS (
    SELECT research_id,
           CASE WHEN multifocal_flag_path IS TRUE THEN 1 ELSE 0 END AS multifocal_flag,
           n_tumors                                                AS n_malignant_tumors,
           tumor_size_cm                                            AS m048_tumor_size_cm
    FROM   manuscript_workspace.cohort_m048_tnm_multifocal_v1
),
size_fallback AS (
    SELECT research_id,
           MAX(COALESCE(size_greatest_dimension_cm, tumor_size_cm_per_surgery)) AS max_tumor_size_cm
    FROM   main.canonical_path_malignant_events_v1
    GROUP  BY research_id
),
path_per_patient AS (
    SELECT  m.research_id,
            COALESCE(m.n_malignant_tumors, 0)                       AS n_malignant_tumors,
            COALESCE(m.multifocal_flag, 0)                          AS multifocal_flag,
            COALESCE(m.m048_tumor_size_cm, s.max_tumor_size_cm)     AS max_tumor_size_cm
    FROM    multifoc_join m
    FULL OUTER JOIN size_fallback s USING (research_id)
)
SELECT
    em.*,                                          -- all v2 covariates + surg_first_date
    -- FNA pattern (PRE-SURGERY; eligible regression adjuster)
    COALESCE(f.n_fnas_total, 0)                                AS n_fnas_total,
    COALESCE(f.had_repeat_fna, 0)                              AS had_repeat_fna,
    CASE WHEN f.research_id IS NOT NULL THEN 1 ELSE 0 END      AS had_any_fna,
    DATE_DIFF('day', f.first_fna_date, em.surg_first_date)     AS days_fna_to_surg_approx,
    -- US-to-surgery interval (PRE-SURGERY; pathway proxy)
    DATE_DIFF('day', uf.first_us_date, em.surg_first_date)    AS days_us_to_surg_approx,
    -- Tumor biology DESCRIPTORS (POST-SURGERY; DO NOT use as adjusters)
    pp.n_malignant_tumors,
    pp.multifocal_flag,
    pp.max_tumor_size_cm,
    CASE WHEN pp.max_tumor_size_cm < 1.0 THEN 'micro'
         WHEN pp.max_tumor_size_cm BETWEEN 1.0 AND 4.0 THEN '1-4cm'
         WHEN pp.max_tumor_size_cm > 4.0 THEN '>4cm'
         ELSE 'unknown' END                                    AS tumor_size_band
FROM   m048_extended_patient_master_v1 em
LEFT   JOIN fna             f  USING (research_id)
LEFT   JOIN us_first        uf USING (research_id)
LEFT   JOIN path_per_patient pp USING (research_id);

-- ----------------------------------------------------------------------------
-- 25b. Nodule grain + v3 patient-level covariates (Model F-Nodule)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_v3_nodule_master_v1 AS
SELECT
    n.*,
    v3.race_strat,
    v3.age_at_surgery,
    v3.sex,
    v3.surg_year,
    v3.surg_first_date,
    v3.surg_procedure_type,
    v3.max_tr_int,
    v3.nodule_burden_cat,
    v3.had_any_genetics,
    v3.had_any_nm,
    v3.has_clt,
    v3.has_graves,
    v3.has_mng,
    v3.has_niftp,
    v3.has_ftump,
    v3.n_fnas_total,
    v3.had_repeat_fna,
    v3.had_any_fna,
    v3.days_fna_to_surg_approx,
    v3.days_us_to_surg_approx,
    v3.bethesda_bucket AS patient_bethesda_bucket
FROM   m025_analytic_master_nodule_v1 n
LEFT   JOIN m048_v3_patient_master_v1 v3 USING (research_id);

-- ----------------------------------------------------------------------------
-- 26. v3 QA gates (additional coverage checks; materialized for CSV audit)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE m048_v3_sql_qa_counts_v1 AS
SELECT 'v3_master_n'              AS gate, CAST(COUNT(*) AS BIGINT)            AS n FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_any_fna',             CAST(SUM(had_any_fna) AS BIGINT)             FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_repeat_fna',          CAST(SUM(had_repeat_fna) AS BIGINT)          FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_n_fnas_ge_3',         CAST(SUM(CASE WHEN n_fnas_total >= 3 THEN 1 ELSE 0 END) AS BIGINT) FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_multifocal',          CAST(SUM(CASE WHEN multifocal_flag IS TRUE THEN 1 ELSE 0 END) AS BIGINT) FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_tumor_size_known',    CAST(SUM(CASE WHEN max_tumor_size_cm IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) FROM m048_v3_patient_master_v1
UNION ALL
SELECT 'with_ete_canonical',       CAST((SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1
                                     WHERE CAST(research_id AS VARCHAR) IN (SELECT research_id FROM m048_v3_patient_master_v1)) AS BIGINT)
UNION ALL
SELECT 'with_frozen_section',      CAST((SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM main.canonical_frozen_section_events_v1
                                     WHERE CAST(research_id AS VARCHAR) IN (SELECT research_id FROM m048_v3_patient_master_v1)) AS BIGINT)
UNION ALL
SELECT 'reconciles_v3_to_v2',
       CAST(CASE WHEN (SELECT COUNT(*) FROM m048_v3_patient_master_v1)
                 = (SELECT COUNT(*) FROM m048_extended_patient_master_v1)
            THEN 1 ELSE 0 END AS BIGINT);
