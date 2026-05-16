-- qc_recurrence_before_surgery — recurrence_date precedes index surgery date (impossible)
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_recurrence_before_surgery';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
SELECT
  'qc_recurrence_before_surgery' AS assertion_id,
  r.research_id,
  DATE(r.recurrence_date) AS event_date,
  CONCAT(
    'recurrence_date=', CAST(DATE(r.recurrence_date) AS STRING),
    ' first_surgery_date=', CAST(SAFE_CAST(t.surgery_date AS DATE) AS STRING),
    ' delta_days=', CAST(DATE_DIFF(DATE(r.recurrence_date), SAFE_CAST(t.surgery_date AS DATE), DAY) AS STRING)
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.recurrence_event_clean_v1` r
JOIN (
  SELECT research_id, MIN(SAFE_CAST(surgery_date AS DATE)) AS surgery_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology`
  WHERE SAFE_CAST(surgery_date AS DATE) IS NOT NULL
  GROUP BY research_id
) t USING (research_id)
WHERE r.recurrence_date IS NOT NULL
  AND DATE(r.recurrence_date) < SAFE_CAST(t.surgery_date AS DATE);
