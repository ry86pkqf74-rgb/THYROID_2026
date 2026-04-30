-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY
-- mig_196 Template 1 — Overall survival (person-level)
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- ASSUMPTIONS
-- * Index date = DATE_trunc calendar date of first thyroid-related surgery (`canonical_survival_followup_v1.first_surgery_date`
--   preferred; falls back to `canonical_patient_master.first_surgery_date`).
-- * Administrative censor = last known alive / last contact when death not asserted.
-- * Death event = non-null `death_date` OR `vital_status_current` indicating deceased (string guard below — tune to live vocabulary).
-- * Analytic cohort matches mig_195 cohort-flow step 4 ∩ ≥1 `canonical_path_malignant_events_v1` row (post–mig_186b pool).
--
-- CAVEATS
-- * Normalize TIMESTAMP vs DATE at joins (CAST … AS DATE or DATE_TRUNC('day', …)) before DATE_DIFF — CF-100 family.
-- * Recurrence / competing risks ignored here (use Template 2 separately); interpret OS accordingly.
-- * `COUNT(DISTINCT research_id)` whenever aggregating patient metrics (source-distinct dup hygiene).

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
cohort AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    COALESCE(
      CAST(s.first_surgery_date AS DATE),
      CAST(c.first_surgery_date AS DATE)
    ) AS index_date,
    COALESCE(
      CAST(s.death_date AS DATE),
      CAST(c.death_date AS DATE)
    ) AS death_day,
    COALESCE(
      CAST(s.last_known_alive_date AS DATE),
      CAST(c.last_contact_date AS DATE)
    ) AS censor_day,
    COALESCE(s.vital_status_current, CAST(c.vital_status AS VARCHAR)) AS vital_lab,
    COALESCE(c.ajcc8_stage_group_resolved, 'T0_or_unstaged') AS stage_group_resolved,
    CASE
      WHEN c.age_at_surgery IS NULL THEN 'unknown_age'
      WHEN c.age_at_surgery < 55 THEN 'lt_55'
      WHEN c.age_at_surgery < 70 THEN '55_to_69'
      ELSE 'ge_70'
    END AS age_tertile_band,
    CASE
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%papillary%' OR LOWER(c.histology_final) LIKE '%ptc%'
        OR LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%ptc%'
      ) THEN 'PTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%follicular%'
        AND LOWER(c.histology_final) NOT LIKE '%papillary%'
      ) THEN 'FTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%medullary%' OR LOWER(c.histology_final) LIKE '%mtc%'
      ) THEN 'MTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%anaplastic%' OR LOWER(c.histology_final) LIKE '%atc%'
      ) THEN 'ATC'
      WHEN c.histology_final IS NOT NULL AND TRIM(CAST(c.histology_final AS VARCHAR)) <> ''
        THEN 'other_or_mixed'
      ELSE 'unknown_histology'
    END AS histology_bucket
  FROM main.canonical_patient_master c
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(c.research_id AS VARCHAR)
  LEFT JOIN main.canonical_survival_followup_v1 s
    ON CAST(c.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
),
typed AS (
  SELECT
    research_id,
    index_date,
    CASE
      WHEN death_day IS NOT NULL THEN death_day
      WHEN vital_lab IS NOT NULL AND (
        LOWER(TRIM(vital_lab)) LIKE '%dead%'
        OR LOWER(TRIM(vital_lab)) LIKE '%dec%'
        OR LOWER(TRIM(vital_lab)) LIKE '%expired%'
      )
        THEN COALESCE(death_day, censor_day)
      ELSE censor_day
    END AS end_date_calendar,
    CASE
      WHEN death_day IS NOT NULL THEN 1
      WHEN vital_lab IS NOT NULL AND (
        LOWER(TRIM(vital_lab)) LIKE '%dead%'
        OR LOWER(TRIM(vital_lab)) LIKE '%dec%'
        OR LOWER(TRIM(vital_lab)) LIKE '%expired%'
      )
        THEN 1
      ELSE 0
    END::INTEGER AS event_indicator,
    stage_group_resolved AS strata_stage,
    histology_bucket AS strata_histology,
    age_tertile_band AS strata_age
  FROM cohort
  WHERE index_date IS NOT NULL
)

/* ---------------------------------------------------------------------------
   Final row-per-patient output — duplicate SELECT blocks swapping strata_var.
--------------------------------------------------------------------------- */
SELECT
  research_id,
  DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25 AS time_to_event_years,
  event_indicator,
  strata_stage AS strata_var,
  'ajcc8_stage_group_resolved' AS strata_role
FROM typed

UNION ALL

SELECT
  research_id,
  DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
  event_indicator,
  strata_histology,
  'histology_bucket'
FROM typed

UNION ALL

SELECT
  research_id,
  DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
  event_indicator,
  strata_age,
  'age_tertile_band'
FROM typed

ORDER BY strata_role, research_id;
