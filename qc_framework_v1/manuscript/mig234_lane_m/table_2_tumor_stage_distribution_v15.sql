-- Lane M mig_234 — Table 2 tumor / AJCC8 stage distribution (tumor-row grain)
-- Source: semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1

USE thyroid_canonical_publication_v1_0;

SELECT
  COALESCE(stage_group_ajcc8, 'unknown') AS stage_group_ajcc8,
  COALESCE(overall_stage_ajcc8, 'unknown') AS overall_stage_ajcc8,
  COUNT(*)::BIGINT AS n_tumor_rows,
  COUNT(DISTINCT CAST(research_id AS VARCHAR))::BIGINT AS n_distinct_patients
FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1
GROUP BY 1, 2
ORDER BY 3 DESC;
