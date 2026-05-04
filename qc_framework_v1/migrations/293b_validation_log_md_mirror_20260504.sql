-- mig_293b — Mirror Snowflake VALIDATION_RUN_LOG_V1 on MotherDuck (mig_293 retry)
-- Generated: 2026-05-04 | Cursor Composer (mig_293b dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Purpose:
--   Cross-platform audit trail: SF-side VALIDATE_ALL_COHORTS() checks (baseline v2,
--   17 checks per run) visible alongside MD-native main.signoff_migration.
--
-- Runtime data refresh (full replace):
--   SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/35_pull_sf_validation_log.py --md
--
-- SF source: THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1
-- MD target: main.cowork_sf_validation_log_v1
--
-- Closes: CF-mig293-VALIDATION-LOG-MIRROR

USE thyroid_canonical_publication_v1_0;

CREATE TABLE IF NOT EXISTS main.cowork_sf_validation_log_v1 (
  sf_run_id BIGINT,
  sf_run_ts TIMESTAMP,
  check_name VARCHAR,
  expected VARCHAR,
  observed VARCHAR,
  status VARCHAR,
  notes VARCHAR,
  pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE main.cowork_sf_validation_log_v1 IS
  'Mirror of Snowflake THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1; refresh via snowflake_trial/scripts/35_pull_sf_validation_log.py --md (mig_293b).';

-- ── Signoff ────────────────────────────────────────────────────────────────
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_293b',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig293b_retry_of_293',
  'mig_293b: Created main.cowork_sf_validation_log_v1 mirror + 35_pull_sf_validation_log.py (mig_293 retry). SF SP now baseline v2 (17 checks). Cross-platform audit trail enabled. Closes CF-mig293-VALIDATION-LOG-MIRROR.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_293b');

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 160) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_293b';
