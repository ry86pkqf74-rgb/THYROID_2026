-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY
-- mig_196 Template 2 — Recurrence-free survival (RFS)
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- ASSUMPTIONS
-- * SSOT recurrence spine = `canonical_recurrence_v1` (`recurrence_confirmed`, `recurrence_date`).
-- * Secondary fallback flag on PM: `COALESCE(cr.recurrence_confirmed, c.any_recurrence_flag)` — gated by @RECURRENCE_FALLBACK_ANY_FLAG below.
-- * Index = first surgery date (survival SSOT then CPM), aligned with Template 1.
-- * Event date for recurrence = CAST(recurrence_date AS DATE) when confirmed; censored at last contact / last known alive otherwise.
--
-- CAVEATS
-- * Many recurrence rows lack calendar `recurrence_date`; sensitivity analyses may restrict to datable events only.
-- * TIMESTAMP recurrence_date → CAST AS DATE for RFS (CF-mig123 recurrence date policy).
-- * Death without recurrence may warrant competing-risks framing (not modeled here).

USE thyroid_canonical_publication_v1_0;

WITH params AS (
  SELECT TRUE AS recurrence_fallback_any_flag   -- set FALSE to require canonical_recurrence_v1 spine only
),
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
cr_first AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    BOOL_OR(COALESCE(recurrence_confirmed, FALSE)) AS recurrence_confirmed_any,
    MIN(CAST(recurrence_date AS DATE)) FILTER (
      WHERE COALESCE(recurrence_confirmed, FALSE) AND recurrence_date IS NOT NULL
    ) AS recurrence_first_date
  FROM main.canonical_recurrence_v1
  GROUP BY 1
),
cohort AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    COALESCE(
      CAST(s.first_surgery_date AS DATE),
      CAST(c.first_surgery_date AS DATE)
    ) AS index_date,
    COALESCE(
      CAST(s.last_known_alive_date AS DATE),
      CAST(c.last_contact_date AS DATE)
    ) AS admin_censor_day,
    CASE
      WHEN cr.recurrence_confirmed_any IS TRUE THEN cr.recurrence_first_date
      ELSE NULL
    END AS cr_recurrence_day,
    CASE
      WHEN cr.recurrence_confirmed_any IS TRUE THEN TRUE
      WHEN (SELECT recurrence_fallback_any_flag FROM params) IS TRUE AND COALESCE(c.any_recurrence_flag, FALSE) IS TRUE
        THEN TRUE
      ELSE FALSE
    END AS recurrence_event_any,
    COALESCE(c.ajcc8_stage_group_resolved, 'T0_or_unstaged') AS strata_stage,
    CASE
      WHEN c.age_at_surgery IS NULL THEN 'unknown_age'
      WHEN c.age_at_surgery < 55 THEN 'lt_55'
      WHEN c.age_at_surgery < 70 THEN '55_to_69'
      ELSE 'ge_70'
    END AS strata_age,
    COALESCE(c.ajcc8_t_stage_resolved, 'unknown_T') AS strata_T,
    COALESCE(c.ajcc8_n_stage_resolved, 'unknown_N') AS strata_N,
    COALESCE(c.r_class_true, 'unknown_margin') AS strata_margin
  FROM main.canonical_patient_master c
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(c.research_id AS VARCHAR)
  LEFT JOIN main.canonical_survival_followup_v1 s
    ON CAST(c.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
  LEFT JOIN cr_first cr ON cr.research_id = CAST(c.research_id AS VARCHAR)
),
typed AS (
  SELECT
    research_id,
    index_date,
    CASE
      WHEN recurrence_event_any IS NOT TRUE THEN admin_censor_day
      WHEN cr_recurrence_day IS NOT NULL THEN cr_recurrence_day
      ELSE admin_censor_day
    END AS end_date_calendar,
    CASE
      WHEN recurrence_event_any IS TRUE AND cr_recurrence_day IS NOT NULL THEN 1
      WHEN recurrence_event_any IS TRUE AND cr_recurrence_day IS NULL THEN 0 /* flagged recurrence without calendar date */
      ELSE 0
    END::INTEGER AS event_indicator,
    strata_stage,
    strata_age,
    strata_T,
    strata_N,
    strata_margin
  FROM cohort
  WHERE index_date IS NOT NULL
)

SELECT
  research_id,
  DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25 AS time_to_event_years,
  event_indicator,
  strata_stage AS strata_var,
  'stage_group_resolved' AS strata_role
FROM typed

UNION ALL
SELECT research_id,
       DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
       event_indicator,
       strata_age,
       'age_tertile_band'
FROM typed

UNION ALL
SELECT research_id,
       DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
       event_indicator,
       strata_T,
       'ajcc8_t_stage_resolved'
FROM typed

UNION ALL
SELECT research_id,
       DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
       event_indicator,
       strata_N,
       'ajcc8_n_stage_resolved'
FROM typed

UNION ALL
SELECT research_id,
       DATE_DIFF('day', index_date, end_date_calendar)::DOUBLE / 365.25,
       event_indicator,
       strata_margin,
       'r_class_true_margin'
FROM typed

ORDER BY strata_role, research_id;
