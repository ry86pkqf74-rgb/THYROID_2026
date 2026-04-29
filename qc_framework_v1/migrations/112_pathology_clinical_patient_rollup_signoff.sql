-- =============================================================================
-- Migration 112 -- canonical_pathology_clinical_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Close the pathology_clinical family. Events table verified mig_110
--         (e2441e4). Rollup built 2026-04-22 by Script 369; events not modified
--         since (only registry status changed). Verify and sign off.
--
-- Methodology: Derivation re-derivation against verified events
--   (mig_106 pattern — events not modified, no rebuild needed).
--
-- Pre-signoff probe (run 2026-04-29):
--   - 3,382 rollup rows = 3,382 distinct events patients (events-scope, NOT
--     cohort-wide — Script 369 emits one row per RID with any entity)
--   - 10/10 derived cols: 0 drift on 3,382 patients
--
-- Sign-off scope:
--   10 not_started cols flipped via derivation_re_derivation_against_verified_events
--   2 already-na cols: research_id, build_ts
--
-- Final: 10 verified + 2 na = 12/12 closed.
-- pathology_clinical family complete: events (mig_110) + rollup (mig_112).
-- 26th canonical table verified.
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_112_pathology_clinical_rollup_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_112: per-pt aggregate re-derivation '
                          || 'against verified canonical_pathology_clinical_events_v1 '
                          || '(mig_110, e2441e4). 3,382/3,382 patients match. '
                          || 'Events not modified since rollup build (Script 369 '
                          || '2026-04-22) — rollup remains consistent.'
WHERE schema_name='main'
  AND table_name='canonical_pathology_clinical_patient_rollup_v1'
  AND verification_status='not_started';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified, n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed,0), n_na = subq.n_na,
    table_status = CASE WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified' WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/112_pathology_clinical_patient_rollup_signoff.sql',
    notes = 'Derivation re-derivation against verified canonical_pathology_clinical_events_v1 (mig_110, e2441e4). 10/10 cols 0 drift on 3,382 patients. Events-scope rollup. pathology_clinical family complete (events mig_110 + rollup mig_112).'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_pathology_clinical_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
