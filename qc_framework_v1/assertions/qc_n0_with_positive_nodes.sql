-- qc_n0_with_positive_nodes — N0 stage but LN positives documented elsewhere
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_n0_with_positive_nodes';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
SELECT
  'qc_n0_with_positive_nodes' AS assertion_id,
  t.research_id,
  t.surgery_date AS event_date,
  CONCAT(
    'tumor_1_n_stage_ajcc8=', COALESCE(t.histology_1_n_stage_ajcc8, '(null)'),
    ' ln_total_positive_from_locations=', CAST(COALESCE(t.ln_total_positive_from_locations, 0) AS STRING),
    ' primary_ln_ln_total_positive=', CAST(COALESCE(t.primary_ln_ln_total_positive, 0) AS STRING)
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology` t
WHERE t.histology_1_n_stage_ajcc8 IN ('N0', 'N0a', 'N0b')
  AND (COALESCE(t.ln_total_positive_from_locations, 0) > 0
       OR COALESCE(t.primary_ln_ln_total_positive, 0) > 0);
