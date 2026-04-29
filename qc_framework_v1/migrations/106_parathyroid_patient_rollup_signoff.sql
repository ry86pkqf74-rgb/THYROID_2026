-- =============================================================================
-- Migration 106 -- canonical_parathyroid_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Close the parathyroid family. Events table closed mig_102 (85e224d).
--         Rollup was built mig_58 (Script 59 SQL, 2026-04-25); events haven't
--         been modified since (only registry status changed in mig_102), so the
--         rollup is still consistent with events. Verify and sign off.
--
-- Methodology: Derivation re-derivation against verified events
--   (mig_95b / mig_101 / mig_104 pattern). Re-derive each rollup col fresh from
--   canonical_parathyroid_events_v1 using Script 59 builder logic and compare
--   per-patient.
--
-- Pre-signoff probe (run 2026-04-29 via Cowork query_rw):
--   - 4,443 rollup rows = 4,443 distinct events patients (1:1, no full-cohort
--     padding — different from medications rollup; parathyroid rollup is
--     events-scope only)
--   - 11 of 13 derived cols: 0 drift on 4,443 patients
--   - 2 cols (parathyroid_pathologies, autotransplant_locations): drift due to
--     STRING_AGG(DISTINCT, ';') ordering non-determinism. Set-equality probe
--     confirms 131 / 131 drifts on parathyroid_pathologies and 1 / 1 drift on
--     autotransplant_locations are PURE ORDERING (set values identical, just
--     reordered between rollup-build-time and re-derivation). Zero value-set
--     drift.
--   - This was already flagged in mig_58 close-out:
--     "(The split `normal;not_assessed` vs `not_assessed;normal` is a
--      STRING_AGG ordering artifact; harmless for analytics, normalize
--      downstream if needed.)"
--
-- Sign-off scope:
--   13 not_started cols flipped to verified:
--     11 via derivation_re_derivation_against_verified_events
--      2 via derivation_re_derivation_with_string_agg_ordering_artifact
--        (parathyroid_pathologies, autotransplant_locations)
--   3 already-na cols: research_id, build_script, build_ts
--
-- Final state of canonical_parathyroid_patient_rollup_v1 (post-mig_106):
--   Rows     : 4,443 (one per parathyroid-events patient — events-scope only)
--   Patients : 4,443
--   Cols     : 16
--   Verified : 13 / 16 + 3 na = 16 / 16 closed
--
-- Carry-forwards:
--   CF-mig58-STRING-AGG-ORDER (open, inherited): STRING_AGG(DISTINCT, ';')
--     without explicit ORDER BY produces non-deterministic ordering. Affects
--     parathyroid_pathologies (~131 patients) and autotransplant_locations
--     (~1 patient). Set values are correct. Downstream analytics that string-
--     compare these cols should normalize via list_sort(string_split(...,';'))
--     before compare. Future Script 59 rebuild should add ORDER BY in the
--     STRING_AGG calls.
--
-- parathyroid family complete: events (mig_102 / 85e224d) + rollup (mig_106)
-- 22nd canonical table closed under Protocol v2.
-- Executed via Cowork query_rw 2026-04-29.
-- =============================================================================

-- 106a: flip 11 cleanly-matching cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_106_parathyroid_rollup_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_106: per-pt aggregate re-derivation '
                          || 'against verified canonical_parathyroid_events_v1 '
                          || '(mig_102, 85e224d). 4,443/4,443 patients match. '
                          || 'Events have not been modified since the rollup '
                          || 'was built mig_58 (2026-04-25); rollup remains '
                          || 'consistent.'
WHERE schema_name='main'
  AND table_name='canonical_parathyroid_patient_rollup_v1'
  AND verification_status='not_started'
  AND column_name NOT IN ('parathyroid_pathologies','autotransplant_locations');

-- 106b: flip 2 STRING_AGG-ordering-affected cols with note
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_with_string_agg_ordering_artifact',
    batch_id            = 'mig_106_parathyroid_rollup_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_106: STRING_AGG(DISTINCT, '';'') without '
                          || 'ORDER BY produces non-deterministic ordering. '
                          || 'Set-equality probe vs fresh re-derivation: 0 '
                          || 'value-set drift, only ordering. Downstream '
                          || 'consumers should list_sort(string_split(...,'';''))'
                          || ' before string-compare. CF-mig58-STRING-AGG-ORDER '
                          || 'open for future Script 59 rebuild.'
WHERE schema_name='main'
  AND table_name='canonical_parathyroid_patient_rollup_v1'
  AND column_name IN ('parathyroid_pathologies','autotransplant_locations')
  AND verification_status='not_started';

-- 106c: recompute table_signoff_registry counts and sign off
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
    signed_off_ts     = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql',
    notes             = 'Derivation re-derivation against verified '
                        || 'canonical_parathyroid_events_v1 (mig_102 / '
                        || '85e224d). 11/13 cols 0 drift; 2 cols (path '
                        || 'pathologies + auto-transplant locations) have '
                        || 'STRING_AGG ordering non-determinism (set-equal, '
                        || 'value-equal, only ordering differs). 4,443/4,443 '
                        || 'patients verified. parathyroid family closed: '
                        || 'events (mig_102) + rollup (mig_106). 22nd canonical '
                        || 'under Protocol v2. CF-mig58-STRING-AGG-ORDER open '
                        || '(harmless cosmetic, inherited from build).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_parathyroid_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 106 -- canonical_parathyroid_patient_rollup_v1 closed
-- 22nd canonical table verified; parathyroid family complete.
-- =============================================================================
