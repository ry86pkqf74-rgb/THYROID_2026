-- qc_focality_contradictions — tumor_focality = unifocal but num_tumors_identified > 1
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_focality_contradictions';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
SELECT
  'qc_focality_contradictions' AS assertion_id,
  research_id,
  surgery_date AS event_date,
  CONCAT(
    'tumor_focality_overall=', COALESCE(tumor_focality_overall, '(null)'),
    ' num_tumors_identified=', CAST(COALESCE(num_tumors_identified, 0) AS STRING)
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology`
WHERE LOWER(COALESCE(tumor_focality_overall, '')) LIKE '%unifocal%'
  AND COALESCE(num_tumors_identified, 0) > 1;
