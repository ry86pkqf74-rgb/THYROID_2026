-- Lane M mig_234 — Table 3 LN publication-safe summaries (patient grain + margins)

USE thyroid_canonical_publication_v1_0;

WITH src AS (
  SELECT * FROM manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1
),
rollup_status AS (
  SELECT
    COALESCE(ln_crossval_status, 'NULL') AS ln_crossval_status,
    COUNT(*)::BIGINT AS n_patients,
    ROUND(AVG(ln_total_examined_safe), 4) AS mean_ln_total_examined_safe,
    ROUND(AVG(ln_total_positive_safe), 4) AS mean_ln_total_positive_safe,
    COUNT(*) FILTER (WHERE COALESCE(ln_denominator_source_conflict_any, FALSE) IS TRUE)::BIGINT AS n_denominator_conflict,
    COUNT(*) FILTER (WHERE COALESCE(ln_attribution_ambiguous_any, FALSE) IS TRUE)::BIGINT AS n_attribution_ambiguous
  FROM src
  GROUP BY 1
),
rollup_positive AS (
  SELECT
    CASE
      WHEN COALESCE(ln_total_positive_safe, 0) <= 0 THEN 'N0_or_unknown_negative'
      WHEN ln_total_positive_safe BETWEEN 1 AND 3 THEN 'N1_1_to_3'
      WHEN ln_total_positive_safe BETWEEN 4 AND 9 THEN 'N1_4_to_9'
      ELSE 'N1_10_plus'
    END AS positive_bucket,
    COUNT(*)::BIGINT AS n_patients
  FROM src
  GROUP BY 1
)
SELECT *
FROM (
SELECT 'ln_crossval_status' AS summary_slice,
       ln_crossval_status AS bucket,
       n_patients,
       mean_ln_total_examined_safe,
       mean_ln_total_positive_safe,
       n_denominator_conflict,
       n_attribution_ambiguous,
       NULL::BIGINT AS rollup_sort
FROM rollup_status

UNION ALL
SELECT 'ln_positive_bucket',
       positive_bucket,
       n_patients,
       NULL, NULL, NULL, NULL,
       CASE positive_bucket
         WHEN 'N0_or_unknown_negative' THEN 1
         WHEN 'N1_1_to_3' THEN 2
         WHEN 'N1_4_to_9' THEN 3
         ELSE 4
       END
FROM rollup_positive
) AS u
ORDER BY summary_slice, COALESCE(rollup_sort, 999), bucket;
