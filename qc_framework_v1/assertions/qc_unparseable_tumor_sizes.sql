-- qc_unparseable_tumor_sizes — tumor size strings that aren't numeric (49 known)
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_unparseable_tumor_sizes';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
SELECT
  'qc_unparseable_tumor_sizes' AS assertion_id,
  research_id,
  surg_date AS event_date,
  CONCAT(
    'tumor_1_size_greatest_dimension_cm raw=', COALESCE(tumor_1_size_greatest_dimension_cm, '(null)')
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.path_synoptics`
WHERE tumor_1_size_greatest_dimension_cm IS NOT NULL
  AND SAFE_CAST(tumor_1_size_greatest_dimension_cm AS FLOAT64) IS NULL;
