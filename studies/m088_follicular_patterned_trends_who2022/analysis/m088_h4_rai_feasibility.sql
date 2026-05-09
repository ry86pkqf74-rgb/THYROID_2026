-- M088 H4 RAI feasibility query
-- Run: 2026-05-09 (Cowork session 1)
-- Purpose: Test whether rai_treatment_episode_v2.rai_assertion_status carries usable
--          signal for the M088 H4 RAI-ordered/received endpoint.
-- Result:  <2% likely_received across 10 of 11 strata. RAI dropped from H4 endpoints.
--          Notable Finding NF-2026-05-09-rai-extraction-sparse-follicular-cohort filed.
--          Linear: THY-55.
-- Note:    rai_treatment_episode_v2.research_id is INT64; canonical_diagnosis_unified_v1.research_id
--          is STRING. Cast required at the join.

WITH cohort AS (
  SELECT DISTINCT research_id, diagnosis_primary, diagnosis_variant
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1`
  WHERE diagnosis_primary IN (
    'follicular_adenoma','hurthle_cell_adenoma','FTUMP',
    'atypical_follicular_adenoma','NIFTP','FTC','HCC','DHGTC'
  )
),
rai AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    MAX(CASE WHEN rai_assertion_status = 'likely_received' THEN 1 ELSE 0 END) AS any_likely_received,
    MAX(CASE WHEN rai_assertion_status IN ('likely_received','ambiguous') THEN 1 ELSE 0 END) AS any_lr_or_ambiguous,
    MAX(CASE WHEN rai_assertion_status = 'negated' THEN 1 ELSE 0 END) AS any_negated
  FROM `thyroid-canonical-pub-2026.pub_canonical.rai_treatment_episode_v2`
  GROUP BY research_id
),
joined AS (
  SELECT c.*,
         COALESCE(r.any_likely_received, 0) AS lr,
         COALESCE(r.any_lr_or_ambiguous,  0) AS lr_amb,
         COALESCE(r.any_negated,          0) AS neg
  FROM cohort c
  LEFT JOIN rai   r USING (research_id)
)
SELECT
  diagnosis_primary,
  CASE WHEN diagnosis_primary = 'FTC' THEN COALESCE(diagnosis_variant, '(null variant)') END AS ftc_variant,
  COUNT(*)                                       AS n_total,
  SUM(lr)                                        AS n_likely_received,
  SUM(lr_amb)                                    AS n_lr_or_ambiguous,
  SUM(neg)                                       AS n_negated,
  ROUND(100 * SUM(lr)     / COUNT(*), 1)         AS pct_lr,
  ROUND(100 * SUM(lr_amb) / COUNT(*), 1)         AS pct_lr_amb
FROM joined
GROUP BY diagnosis_primary, ftc_variant
ORDER BY diagnosis_primary, ftc_variant;
