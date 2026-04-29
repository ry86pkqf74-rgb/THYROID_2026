-- =============================================================================
-- Migration 108 -- canonical_complications_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cursor lane 5)
-- Author: Logan Glosser (drafted with GitHub Copilot)
-- Plan:   Close the complications family by verifying the already-rebuilt
--         patient rollup against verified canonical_complications_events_v1.
--         Events were signed off in mig_99 (commit cbccd4a); the rollup build_ts
--         is fresh at 2026-04-28 19:36:23.768812.
--
-- Methodology: Derivation re-derivation against verified events, following the
--   mig_105 medications rollup signoff pattern. The rollup was not rebuilt here:
--   every derivable column was independently re-derived from
--   canonical_complications_events_v1 plus canonical_patient_master and
--   canonical_operative_events_v1 for first-surgery temporal flags.
--
-- Pre-signoff probe (run 2026-04-29 via local MotherDuck connection):
--   - Cohort parity: 10,871 rollup rows / 10,871 distinct patients =
--     canonical_patient_master 10,871 rows / 10,871 distinct patients.
--   - Events source: 5,050 rows / 2,481 distinct patients.
--   - Rollup build_ts: min=max=2026-04-28 19:36:23.768812.
--   - 10,871 patients x 49 derivable metrics: 0 patients with drift and
--     0 total cell drifts.
--   - 10,416 patients with no present complication evidence: 0 contamination
--     for n_complication_findings_total, n_complication_types_present, and
--     first/last_complication_date.
--
-- Sign-off scope:
--   49 not_started cols flipped to verified via
--   derivation_re_derivation_against_verified_events:
--     36 ever_*_<tier> phenotype flags (12 complication types x 3 tiers),
--     n_complication_types_present, n_complication_findings_total,
--     first_complication_date, last_complication_date, and 8 hypopara /
--     hypocalcemia temporal flags.
--   2 already-na cols carry over: research_id (auto_identifier_skip),
--     build_ts (auto_provenance_skip).
--
-- Final expected state:
--   Rows     : 10,871 (one row per canonical_patient_master patient)
--   Patients : 10,871
--   Cols     : 51
--   Verified : 49 / 51 + 2 na = 51 / 51 closed
--
-- complications family complete: events (mig_99 commit cbccd4a) + rollup
-- (this migration).
-- =============================================================================

-- 108a: flip 49 not_started derivable columns
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_108_complications_rollup_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_108: verified already-rebuilt rollup '
                          || '(build_ts 2026-04-28 19:36:23.768812) against '
                          || 'canonical_complications_events_v1 signed off in '
                          || 'mig_99 (commit cbccd4a). Per-patient aggregate '
                          || 're-derivation across all 49 derivable columns: '
                          || '10,871 patients compared, 0 patients with drift, '
                          || '0 total cell drifts. No-present-evidence cohort '
                          || '10,416 patients: 0 count/date contamination. '
                          || 'Cohort parity 10,871 = canonical_patient_master '
                          || '10,871 (CHANGE J satisfied).'
WHERE schema_name='main'
  AND table_name='canonical_complications_patient_rollup_v1'
  AND verification_status='not_started';

-- 108b: recompute table_signoff_registry counts and sign off
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/108_complications_patient_rollup_signoff.sql',
    notes             = 'Derivation re-derivation against verified '
                        || 'canonical_complications_events_v1 (mig_99 commit '
                        || 'cbccd4a). Rollup build_ts 2026-04-28 '
                        || '19:36:23.768812. 10,871 patients compared across '
                        || '49 derivable metrics: 0 drift. 10,416 patients '
                        || 'with no present complication evidence: 0 count/date '
                        || 'contamination. Cohort parity 10,871 = CPM. '
                        || 'complications family closed: events (mig_99) + '
                        || 'rollup (mig_108).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_complications_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 108 -- canonical_complications_patient_rollup_v1 closed
-- =============================================================================