-- qc_date_monotonicity — FNA should precede surgery; surgery should precede recurrence
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_date_monotonicity';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
WITH events AS (
  SELECT research_id,
         MIN(SAFE_CAST(fna_date AS DATE)) AS first_fna_date,
         (SELECT MIN(SAFE_CAST(surgery_date AS DATE)) FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology` p
          WHERE p.research_id = f.research_id) AS first_surgery_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.fna_cytology` f
  GROUP BY research_id
)
SELECT
  'qc_date_monotonicity' AS assertion_id,
  research_id,
  first_surgery_date AS event_date,
  CONCAT(
    'first_fna_date=', CAST(first_fna_date AS STRING),
    ' first_surgery_date=', CAST(first_surgery_date AS STRING),
    ' delta_days=', CAST(DATE_DIFF(first_surgery_date, first_fna_date, DAY) AS STRING)
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM events
WHERE first_fna_date IS NOT NULL
  AND first_surgery_date IS NOT NULL
  AND first_fna_date > first_surgery_date;
