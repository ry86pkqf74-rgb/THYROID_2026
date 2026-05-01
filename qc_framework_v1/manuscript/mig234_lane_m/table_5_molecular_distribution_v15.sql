-- Lane M mig_234 — Table 5 molecular testing distribution (episode grain rollups)

USE thyroid_canonical_publication_v1_0;

SELECT
  COALESCE(platform, 'unknown') AS platform,
  COALESCE(is_patient_level_only_evidence, FALSE) AS is_patient_level_only_evidence,
  COUNT(*)::BIGINT AS n_molecular_rows,
  COUNT(DISTINCT CAST(research_id AS VARCHAR))::BIGINT AS n_distinct_patients,
  COUNT(*) FILTER (WHERE COALESCE(braf_flag, FALSE) IS TRUE)::BIGINT AS n_rows_braf_pos,
  COUNT(*) FILTER (WHERE COALESCE(ras_flag, FALSE) IS TRUE)::BIGINT AS n_rows_ras_pos,
  COUNT(*) FILTER (WHERE COALESCE(tert_flag, FALSE) IS TRUE)::BIGINT AS n_rows_tert_pos
FROM semantic_publication.vw_molecular_safe_VIEW_v1
GROUP BY 1, 2
ORDER BY platform, is_patient_level_only_evidence;
