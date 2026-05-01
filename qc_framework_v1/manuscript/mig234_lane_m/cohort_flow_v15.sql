-- Lane M mig_234 — CONSORT-style cohort flow (extended vs mig_195; semantic spine where noted)

USE thyroid_canonical_publication_v1_0;

WITH
semantic_malignant_rid AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1
),
raw_malignant_rid AS (
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
borderline_flag_rid AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_malignant_events_v1
  WHERE COALESCE(is_borderline_or_benign_with_staging, FALSE) IS TRUE
),
recurrence_quarantine_rid AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_recurrence_resolved_v1
  WHERE COALESCE(is_implausible_date_quarantine, FALSE) IS TRUE
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
analytic_from_semantic AS (
  SELECT COUNT(*)::BIGINT AS n
  FROM semantic_malignant_rid sm
  INNER JOIN main.canonical_patient_master c ON sm.research_id = CAST(c.research_id AS VARCHAR)
  WHERE COALESCE(c.is_malignant, FALSE) IS TRUE
),
excl0_non_malig AS (
  SELECT (s0.n - s1.n)::BIGINT AS n FROM step0 s0 CROSS JOIN step1 s1
),
borderline_pts AS (
  SELECT COUNT(DISTINCT research_id)::BIGINT AS n FROM borderline_flag_rid
),
recurrence_quar_pts AS (
  SELECT COUNT(DISTINCT research_id)::BIGINT AS n FROM recurrence_quarantine_rid
),
raw_minus_semantic AS (
  SELECT COUNT(*)::BIGINT AS n
  FROM raw_malignant_rid r
  WHERE NOT EXISTS (SELECT 1 FROM semantic_malignant_rid s WHERE s.research_id = r.research_id)
)

SELECT * FROM (
  SELECT
    1 AS step,
    'Total distinct patients in canonical_patient_master' AS description,
    0::BIGINT AS n_excluded,
    s0.n AS n_remaining
  FROM step0 s0

  UNION ALL
  SELECT 2,
    'Excluded: not malignant (is_malignant=FALSE or NULL)',
    e.n,
    s1.n
  FROM excl0_non_malig e CROSS JOIN step1 s1

  UNION ALL
  SELECT 3,
    'Excluded: NIFTP/UMP-only (indeterminate events, no remaining malignant event) [mig_186b]',
    ex.n,
    s2.n
  FROM excl2 ex CROSS JOIN step2 s2

  UNION ALL
  SELECT 4,
    'Excluded: no histology + no AJCC8 T-stage resolved (both NULL)',
    ex.n,
    s3.n
  FROM excl3 ex CROSS JOIN step3 s3

  UNION ALL
  SELECT 5,
    'Excluded: no last_contact_date',
    ex.n,
    s4.n
  FROM excl4 ex CROSS JOIN step4 s4

  UNION ALL
  SELECT 6,
    'Informational: distinct patients with ≥1 borderline/benign-with-staging malignant row [mig_229]',
    NULL::BIGINT,
    bp.n
  FROM borderline_pts bp

  UNION ALL
  SELECT 7,
    'Informational: distinct patients with recurrence implausible-date quarantine (excluded from vw_recurrence_safe)',
    NULL::BIGINT,
    rq.n
  FROM recurrence_quar_pts rq

  UNION ALL
  SELECT 8,
    'Informational: raw malignant-event patients absent from semantic vw_path_malignant after dedup/quarantine',
    rm.n,
    NULL::BIGINT
  FROM raw_minus_semantic rm

  UNION ALL
  SELECT 9,
    'Analytic malignant cohort — patients with ≥1 vw_path_malignant_tumor_safe_VIEW_v1 row ∩ malignant CPM',
    NULL::BIGINT,
    ae.n
  FROM analytic_from_semantic ae
) q
ORDER BY step;
