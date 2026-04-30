-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY
-- mig_196 Template 4 — Complication prevalence by surgery / neck-dissection bucket
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- SSOT events: main.canonical_complications_events_v1 (finding_status = 'present').
-- Complication taxonomy aligns with scripts/364_complications_consolidation.py enum (12-type spine).
--
-- WINDOWS (document in manuscript)
-- * Acute (0–30 d post-op): hypocalcemia_clinical, chyle_leak, hematoma, vocal cord palsy
--   (vocal_cord_paralysis ∪ rln_injury), using COALESCE(timing_days, -1) BETWEEN 0 AND 30.
-- * Any-time follow-up: hypoparathyroidism, mortality (timing_days ignored).
-- * Rows carry analysis_window so Logan can filter or re-stratify.
--
-- 95% CI: asymptotic (Wald) on binomial p = x/n; for sparse cells prefer exact CI in R/Python.
--
-- CAVEATS
-- * timing_days NULL → excluded from acute-window numerators (still in denominators).
-- * Surgery bucket mixes operative flags + LN imaging/synoptic signals (same spirit as mig_184 ND detector).

USE thyroid_canonical_publication_v1_0;

WITH
malignant_pts AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_malignant_events_v1
),
indeterminate_only_rid AS (
  SELECT DISTINCT CAST(i.research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_indeterminate_events_v1 AS i
  WHERE NOT EXISTS (
    SELECT 1 FROM main.canonical_path_malignant_events_v1 AS m
    WHERE CAST(m.research_id AS VARCHAR) = CAST(i.research_id AS VARCHAR)
  )
),
step1_pool AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master
  WHERE is_malignant IS TRUE
),
step2_pool AS (
  SELECT p.research_id
  FROM step1_pool p
  WHERE NOT EXISTS (
    SELECT 1 FROM indeterminate_only_rid i WHERE i.research_id = p.research_id
  )
),
step3_pool AS (
  SELECT p.research_id
  FROM step2_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE NOT (c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL)
),
step4_pool AS (
  SELECT p.research_id
  FROM step3_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.last_contact_date IS NOT NULL
),
analytic_rid AS (
  SELECT DISTINCT p.research_id
  FROM step4_pool p
  INNER JOIN malignant_pts m ON m.research_id = p.research_id
),
patient_surgery AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    CASE
      WHEN COALESCE(c.surg_total_thyroidectomy, FALSE) IS TRUE THEN 'total_thyroidectomy'
      WHEN COALESCE(c.surg_hemithyroidectomy, FALSE) IS TRUE THEN 'hemithyroidectomy'
      ELSE 'other_or_unknown_primary_procedure'
    END || ' × ' ||
    CASE
      WHEN
        (
          COALESCE(c.lateral_neck_dissected_structured_or_nlp, FALSE) IS TRUE
          OR COALESCE(c.lateral_neck_dissected, FALSE) IS TRUE
          OR COALESCE(c.cnln_img_lateral_neck_present, FALSE) IS TRUE
        )
        AND (
          COALESCE(c.cnln_img_central_present, FALSE) IS TRUE
          OR COALESCE(c.ln_rollup_central_examined, 0) > 0
        )
        THEN 'ND_both'
      WHEN COALESCE(c.lateral_neck_dissected_structured_or_nlp, FALSE) IS TRUE
        OR COALESCE(c.lateral_neck_dissected, FALSE) IS TRUE
        OR COALESCE(c.cnln_img_lateral_neck_present, FALSE) IS TRUE
        THEN 'ND_lateral'
      WHEN COALESCE(c.cnln_img_central_present, FALSE) IS TRUE
        OR COALESCE(c.ln_rollup_central_examined, 0) > 0
        THEN 'ND_central'
      ELSE 'ND_none_signal'
    END AS surgery_type_label
  FROM main.canonical_patient_master c
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(c.research_id AS VARCHAR)
),
comp_long AS (
  SELECT
    CAST(ce.research_id AS VARCHAR) AS research_id,
    ce.complication_type,
    ce.timing_days,
    ce.finding_status,
    CASE ce.complication_type
      WHEN 'hypocalcemia_clinical' THEN 'hypocalcemia'
      WHEN 'hypoparathyroidism' THEN 'hypoparathyroidism'
      WHEN 'vocal_cord_paralysis' THEN 'vocal_cord_palsy'
      WHEN 'rln_injury' THEN 'vocal_cord_palsy'
      WHEN 'chyle_leak' THEN 'chyle_leak'
      WHEN 'hematoma' THEN 'hematoma'
      WHEN 'mortality' THEN 'mortality'
      ELSE NULL
    END AS complication_category,
    CASE
      WHEN ce.complication_type IN ('hypoparathyroidism', 'mortality') THEN 'any_time_followup'
      WHEN COALESCE(ce.timing_days, -1) BETWEEN 0 AND 30 THEN 'acute_0_30d'
      ELSE 'outside_acute_window'
    END AS analysis_window
  FROM main.canonical_complications_events_v1 ce
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(ce.research_id AS VARCHAR)
  WHERE LOWER(TRIM(COALESCE(ce.finding_status, ''))) = 'present'
),
comp_flag AS (
  SELECT DISTINCT
    research_id,
    complication_category,
    analysis_window
  FROM comp_long
  WHERE complication_category IS NOT NULL
    AND (
      analysis_window = 'any_time_followup'
      OR analysis_window = 'acute_0_30d'
    )
),
cats AS (
  SELECT * FROM (
    VALUES
      ('hypocalcemia'),
      ('hypoparathyroidism'),
      ('vocal_cord_palsy'),
      ('chyle_leak'),
      ('hematoma'),
      ('mortality')
  ) AS v(complication_category)
),
sx AS (
  SELECT DISTINCT surgery_type_label FROM patient_surgery
),
grid AS (
  SELECT sx.surgery_type_label, cats.complication_category
  FROM sx CROSS JOIN cats
),
denom AS (
  SELECT surgery_type_label, COUNT(*)::BIGINT AS denom_n
  FROM patient_surgery
  GROUP BY 1
),
numer AS (
  SELECT
    ps.surgery_type_label,
    cf.complication_category,
    COUNT(DISTINCT ps.research_id)::BIGINT AS numer_n
  FROM patient_surgery ps
  INNER JOIN comp_flag cf ON cf.research_id = ps.research_id
  WHERE (cf.complication_category IN ('hypoparathyroidism', 'mortality') AND cf.analysis_window = 'any_time_followup')
     OR (cf.complication_category NOT IN ('hypoparathyroidism', 'mortality') AND cf.analysis_window = 'acute_0_30d')
  GROUP BY 1, 2
)

SELECT
  g.surgery_type_label,
  g.complication_category,
  CASE
    WHEN g.complication_category IN ('hypoparathyroidism', 'mortality') THEN 'any_time_followup'
    ELSE 'acute_0_30d'
  END AS analysis_window,
  COALESCE(n.numer_n, 0)::BIGINT AS n_with_complication,
  d.denom_n AS n_patients_in_surgery_bucket,
  ROUND(COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0), 5) AS proportion,
  ROUND(GREATEST(
    COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)
    - 1.96 * SQRT(
      (COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      * (1.0 - COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      / NULLIF(d.denom_n, 0)
    ), 0.0
  ), 5) AS ci95_lower_wald,
  ROUND(LEAST(
    COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)
    + 1.96 * SQRT(
      (COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      * (1.0 - COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      / NULLIF(d.denom_n, 0)
    ), 1.0
  ), 5) AS ci95_upper_wald
FROM grid g
INNER JOIN denom d ON d.surgery_type_label = g.surgery_type_label
LEFT JOIN numer n
  ON n.surgery_type_label = g.surgery_type_label
 AND n.complication_category = g.complication_category
ORDER BY g.surgery_type_label, g.complication_category;
