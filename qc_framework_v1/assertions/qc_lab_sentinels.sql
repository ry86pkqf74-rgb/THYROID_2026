-- qc_lab_sentinels — physiologically impossible lab values (likely typos / unit errors / sentinels)
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_lab_sentinels';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
WITH tg_outliers AS (
  SELECT CAST(research_id AS STRING) AS research_id,
         DATE(lab_datetime) AS event_date,
         CONCAT('tg=', CAST(value_numeric AS STRING), ' ', COALESCE(unit_standardized, '')) AS detail
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
  WHERE value_numeric IS NOT NULL
    AND (value_numeric > 100000 OR value_numeric < 0)
),
ca_outliers AS (
  SELECT research_id, DATE(lab_datetime) AS event_date,
         CONCAT('ca=', CAST(value_numeric AS STRING), ' ', COALESCE(unit_standardized, '')) AS detail
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_calcium_v1`
  WHERE value_numeric IS NOT NULL
    AND (value_numeric < 4 OR value_numeric > 20)
),
pth_outliers AS (
  SELECT research_id, DATE(lab_datetime) AS event_date,
         CONCAT('pth=', CAST(value_numeric AS STRING), ' ', COALESCE(unit_standardized, '')) AS detail
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_pth_v1`
  WHERE value_numeric IS NOT NULL
    AND (value_numeric < 0 OR value_numeric > 5000)
)
SELECT
  'qc_lab_sentinels' AS assertion_id,
  research_id,
  event_date,
  detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM (
  SELECT research_id, event_date, detail FROM tg_outliers
  UNION ALL SELECT research_id, event_date, detail FROM ca_outliers
  UNION ALL SELECT research_id, event_date, detail FROM pth_outliers
);
