-- ============================================================================
-- QC Scheduled Query — BigQuery-native daily QC runner (stored procedure)
-- ============================================================================
-- Created: 2026-05-06 (Cowork) for Task 4; aligned 2026-05-06 to live BQ schema
-- defined in mig_007_qc_framework.sql (MotherDuck → GC migration).
--
-- WHAT THIS DOES:
--   Iterates pub_signoff.qc_assertions_v1, executes each active check_sql
--   (returns violating ROWS — 0 rows = pass — same semantics as _scripts/qc_runner.py),
--   and writes into pub_signoff.qc_violations_v1.
--
-- VERIFIED SCHEMAS (bq show thyroid-canonical-pub-2026:pub_signoff.*):
--
-- qc_assertions_v1:
--   assertion_id, category, severity, affected_object, description, check_sql,
--   expected_result, active, added_at, added_by, notes
--
-- qc_violations_v1:
--   run_id, assertion_id, ran_at, passed, violation_count, bytes_scanned,
--   duration_ms, sample_rows, error_message
--
-- Scheduled query body: CALL `thyroid-canonical-pub-2026.pub_signoff.run_qc_assertions`();
-- ============================================================================

CREATE OR REPLACE PROCEDURE `thyroid-canonical-pub-2026.pub_signoff.run_qc_assertions`()
BEGIN
  DECLARE batch_run_id STRING DEFAULT GENERATE_UUID();
  DECLARE batch_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  FOR rule_record IN (
    SELECT assertion_id, check_sql
    FROM `thyroid-canonical-pub-2026.pub_signoff.qc_assertions_v1`
    WHERE active = TRUE
    ORDER BY assertion_id
  ) DO
    BEGIN
      -- Count violating rows returned by check_sql (mirrors qc_runner.py).
      EXECUTE IMMEDIATE CONCAT(
        'INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.qc_violations_v1` ',
        '(run_id, assertion_id, ran_at, passed, violation_count, bytes_scanned, duration_ms, sample_rows, error_message) ',
        'SELECT ',
        FORMAT('%T', batch_run_id), ', ',
        FORMAT('%T', rule_record.assertion_id), ', ',
        FORMAT('%T', batch_started_at), ', ',
        '(cnt = 0), cnt, CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS STRING), CAST(NULL AS STRING) ',
        'FROM (SELECT COUNT(*) AS cnt FROM (',
        rule_record.check_sql,
        ') AS _qc_inner)'
      );
    EXCEPTION WHEN ERROR THEN
      INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.qc_violations_v1`
        (run_id, assertion_id, ran_at, passed, violation_count, bytes_scanned, duration_ms, sample_rows, error_message)
      VALUES (
        batch_run_id,
        rule_record.assertion_id,
        batch_started_at,
        FALSE,
        CAST(NULL AS INT64),
        CAST(NULL AS INT64),
        CAST(NULL AS INT64),
        CAST(NULL AS STRING),
        @@error.message
      );
    END;
  END FOR;
END;
