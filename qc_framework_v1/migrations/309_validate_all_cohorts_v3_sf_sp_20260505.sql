-- mig_309 — MotherDuck signoff for Snowflake VALIDATE_ALL_COHORTS_V3
-- Date: 2026-05-05
-- DB: thyroid_canonical_publication_v1_0
--
-- Run this file on MotherDuck only after Snowflake reports 24/24 PASS from
--   CALL VALIDATE_ALL_COHORTS_V3();
--
-- Snowflake artifact (deploy first):
--   snowflake_trial/sql_drops/mig_309_sp_v3.sql
--
-- Post-deploy on SF:
--   CALL VALIDATE_ALL_COHORTS_V3();  -- expect 24 rows, all PASS
--
-- Mirror validation log (unchanged schema; now includes mig_309-tagged rows):
--   SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/35_pull_sf_validation_log.py --md
--
-- Closes: CF-mig_305-SP-V3-HANG (once SF SP is deployed + 24/24 PASS verified)

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_309',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig309',
  'mig_309: VALIDATE_ALL_COHORTS_V3 deployed on Snowflake (Option A). CALL returned 24/24 PASS. Log mirrored to main.cowork_sf_validation_log_v1. Closes CF-mig_305-SP-V3-HANG.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_309');

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_305',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig309_retro',
  'mig_305: SF SP v3 attempt hung on INFORMATION_SCHEMA iteration inside SP body. Superseded by mig_309 (Option A pre-materialized meta CTE). v2 SP (17 checks) remained the floor between 2026-05-04 and mig_309 deploy.'
WHERE NOT EXISTS (
  SELECT 1 FROM main.signoff_migration
  WHERE mig_id = 'mig_305' AND by_actor = 'cursor_composer_mig309_retro'
);

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 120) AS summary_head
FROM main.signoff_migration
WHERE mig_id IN ('mig_309', 'mig_305')
ORDER BY signed_off_at DESC;
