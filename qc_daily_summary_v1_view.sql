-- ============================================================================
-- pub_signoff.qc_daily_summary_v1 + qc_weekly_trend_v1
-- ============================================================================
-- Joins qc_violations_v1 to qc_assertions_v1 for severity (violations table
-- does not store severity). Severities follow mig_007: error | warning | info.
-- ============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_signoff.qc_daily_summary_v1` AS
WITH latest_run AS (
  SELECT run_id
  FROM `thyroid-canonical-pub-2026.pub_signoff.qc_violations_v1`
  WHERE DATE(ran_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY run_id
  QUALIFY ROW_NUMBER() OVER (ORDER BY MAX(ran_at) DESC) = 1
),
today_rows AS (
  SELECT
    v.*,
    a.severity AS assertion_severity
  FROM `thyroid-canonical-pub-2026.pub_signoff.qc_violations_v1` v
  JOIN latest_run lr USING (run_id)
  LEFT JOIN `thyroid-canonical-pub-2026.pub_signoff.qc_assertions_v1` a
    USING (assertion_id)
)
SELECT
  (SELECT run_id FROM latest_run)        AS run_id,
  (SELECT MAX(ran_at) FROM today_rows)   AS run_timestamp,
  COUNT(*)                               AS rules_run,

  COUNTIF(violation_count > 0)         AS rules_with_violations,
  COUNTIF(error_message IS NOT NULL)     AS rules_errored,

  COUNTIF(assertion_severity = 'error' AND violation_count > 0)
                                       AS error_severity_failures,
  COUNTIF(assertion_severity IN ('warning', 'warn') AND violation_count > 0)
                                       AS warn_severity_failures,

  SUM(IFNULL(violation_count, 0))      AS total_violation_rows,

  ARRAY_AGG(
    STRUCT(assertion_id, assertion_severity, violation_count, error_message)
    ORDER BY
      CASE
        WHEN error_message IS NOT NULL THEN 0
        WHEN violation_count > 0 THEN 1
        ELSE 2
      END,
      violation_count DESC NULLS LAST
    LIMIT 10
  )                                      AS top_offenders,

  (COUNTIF(violation_count > 0) = 0 AND COUNTIF(error_message IS NOT NULL) = 0)
                                       AS all_clear
FROM today_rows;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_signoff.qc_weekly_trend_v1` AS
SELECT
  DATE(ran_at) AS run_date,
  COUNT(DISTINCT run_id) AS runs,
  COUNTIF(violation_count > 0) AS rules_with_violations,
  SUM(IFNULL(violation_count, 0)) AS total_violation_rows,
  COUNTIF(error_message IS NOT NULL) AS rules_errored
FROM `thyroid-canonical-pub-2026.pub_signoff.qc_violations_v1`
WHERE DATE(ran_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
GROUP BY run_date
ORDER BY run_date DESC;
