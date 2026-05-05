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
JOIN m025_analytic_master_patient_v1 p USING (research_id);

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
           max_tirads_category_ever AS tr,
           is_malignant::INT        AS y,
           1                        AS one
    FROM   m048_patient_master_v1
    WHERE  max_tirads_category_ever IS NOT NULL
    UNION ALL
    SELECT 'nodule', race_strat,
           acr2017_tirads_category, nodule_path_proven_malignant::INT, 1
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
           max_tirads_category_ever::DOUBLE AS pred,
           is_malignant::INT                AS y
    FROM   m048_patient_master_v1
    WHERE  max_tirads_category_ever IS NOT NULL
    UNION ALL
    SELECT 'nodule', race_strat,
           acr2017_tirads_category::DOUBLE,
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
