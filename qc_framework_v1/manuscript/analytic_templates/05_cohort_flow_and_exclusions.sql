-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY
-- mig_196 Template 5 — Cohort flow counts + per-step research_id lists
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- USAGE
-- * QUERY A reproduces mig_195 `cohort_flow_diagram.sql` row-for-row (CONSORT counts + analytic cross-check).
-- * QUERY B emits long-format IDs for each exclusion arm + final analytic intersection — sensitivity analyses.
--
-- PARAMETERIZATION
-- * To mirror Table 1 denominator only (no last-contact exclusion), duplicate QUERY B and drop the
--   `excluded_no_last_contact_date` arm; intersect pools accordingly — documented in README.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- QUERY A — cohort flow counts (duplicate of qc_framework_v1/manuscript/cohort_flow_diagram.sql)
-- =============================================================================

WITH
malignant_rid AS (
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
step0 AS (
  SELECT COUNT(*)::BIGINT AS n FROM main.canonical_patient_master
),
step1_pool AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master
  WHERE is_malignant IS TRUE
),
step1 AS (
  SELECT COUNT(*)::BIGINT AS n FROM step1_pool
),
excl2 AS (
  SELECT COUNT(*)::BIGINT AS n
  FROM step1_pool p
  INNER JOIN indeterminate_only_rid i ON p.research_id = i.research_id
),
step2_pool AS (
  SELECT p.research_id
  FROM step1_pool p
  WHERE NOT EXISTS (
    SELECT 1 FROM indeterminate_only_rid i WHERE i.research_id = p.research_id
  )
),
step2 AS (
  SELECT COUNT(*)::BIGINT AS n FROM step2_pool
),
excl3 AS (
  SELECT COUNT(*)::BIGINT AS n
  FROM step2_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL
),
step3_pool AS (
  SELECT p.research_id
  FROM step2_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE NOT (c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL)
),
step3 AS (
  SELECT COUNT(*)::BIGINT AS n FROM step3_pool
),
excl4 AS (
  SELECT COUNT(*)::BIGINT AS n
  FROM step3_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.last_contact_date IS NULL
),
step4_pool AS (
  SELECT p.research_id
  FROM step3_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.last_contact_date IS NOT NULL
),
step4 AS (
  SELECT COUNT(*)::BIGINT AS n FROM step4_pool
),
analytic_from_events AS (
  SELECT COUNT(DISTINCT mp.research_id)::BIGINT AS n
  FROM malignant_rid mp
  INNER JOIN main.canonical_patient_master c ON mp.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.is_malignant IS TRUE
),
excl0_non_malig AS (
  SELECT (s0.n - s1.n)::BIGINT AS n FROM step0 s0 CROSS JOIN step1 s1
)

SELECT * FROM (
  SELECT
    1 AS step,
    'Total distinct patients in canonical_patient_master' AS description,
    0::BIGINT AS n_excluded,
    s0.n AS n_remaining
  FROM step0 s0

  UNION ALL
  SELECT
    2,
    'Excluded: not malignant (is_malignant=FALSE or NULL)',
    e.n,
    s1.n
  FROM excl0_non_malig e CROSS JOIN step1 s1

  UNION ALL
  SELECT
    3,
    'Excluded: NIFTP/UMP-only (indeterminate events, no remaining malignant event) [mig_186b]',
    ex.n,
    s2.n
  FROM excl2 ex CROSS JOIN step2 s2

  UNION ALL
  SELECT
    4,
    'Excluded: no histology + no AJCC8 T-stage resolved (both NULL)',
    ex.n,
    s3.n
  FROM excl3 ex CROSS JOIN step3 s3

  UNION ALL
  SELECT
    5,
    'Excluded: no last_contact_date',
    ex.n,
    s4.n
  FROM excl4 ex CROSS JOIN step4 s4

  UNION ALL
  SELECT
    6,
    'Analytic cohort — intersection: malignant CPM + ≥1 canonical_path_malignant_events_v1 row (cross-check)',
    NULL::BIGINT,
    ae.n
  FROM analytic_from_events ae
) q
ORDER BY step;

-- =============================================================================
-- QUERY B — research_id lists (second statement; run after QUERY A if client splits batches)
-- =============================================================================

WITH
malignant_rid AS (
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
)

SELECT 'excluded_not_malignant_cpm' AS step_key, 'excluded' AS pool_kind, CAST(pm.research_id AS VARCHAR) AS research_id
FROM main.canonical_patient_master pm
WHERE CAST(pm.research_id AS VARCHAR) NOT IN (SELECT research_id FROM step1_pool)

UNION ALL
SELECT 'excluded_indeterminate_only_mig186b', 'excluded', p.research_id
FROM step1_pool p
WHERE EXISTS (SELECT 1 FROM indeterminate_only_rid i WHERE i.research_id = p.research_id)

UNION ALL
SELECT 'excluded_no_histology_and_no_T_resolved', 'excluded', p.research_id
FROM step2_pool p
INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
WHERE c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL

UNION ALL
SELECT 'excluded_no_last_contact_date', 'excluded', p.research_id
FROM step3_pool p
INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
WHERE c.last_contact_date IS NULL

UNION ALL
SELECT 'included_analytic_flow_after_step5_cap_malignant_events', 'included', p.research_id
FROM step4_pool p
INNER JOIN malignant_rid m ON m.research_id = p.research_id

ORDER BY pool_kind DESC, step_key, research_id;
