-- =============================================================================
-- Migration 113 -- canonical_cervical_ln_clinical_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Close the cervical_ln_clinical family. Events table verified mig_111.
--         Rollup built 2026-04-22 by Script 382; events not modified since.
--
-- Methodology: Derivation re-derivation against verified events.
--
-- Pre-signoff probe (run 2026-04-29):
--   - 1,643 rollup rows = 1,643 distinct events patients (events-scope)
--   - 7/7 derived cols: 0 drift on 1,643 patients
--
-- Sign-off scope:
--   7 not_started cols flipped via derivation_re_derivation_against_verified_events
--   2 already-na cols: research_id, build_ts
--
-- Final: 7 verified + 2 na = 9/9 closed.
-- cervical_ln_clinical family complete: events (mig_111) + rollup (mig_113).
-- 27th canonical table verified.
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_113_cervical_ln_clinical_rollup_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_113: per-pt aggregate re-derivation '
                          || 'against verified canonical_cervical_ln_clinical_events_v1 '
                          || '(mig_111). 1,643/1,643 patients match.'
WHERE schema_name='main'
  AND table_name='canonical_cervical_ln_clinical_patient_rollup_v1'
  AND verification_status='not_started';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified, n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed,0), n_na = subq.n_na,
    table_status = CASE WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified' WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/113_cervical_ln_clinical_patient_rollup_signoff.sql',
    notes = 'Derivation re-derivation against verified canonical_cervical_ln_clinical_events_v1 (mig_111). 7/7 cols 0 drift on 1,643 patients. Events-scope rollup. cervical_ln_clinical family complete (events mig_111 + rollup mig_113).'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_cervical_ln_clinical_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
