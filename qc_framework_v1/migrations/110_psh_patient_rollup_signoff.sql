-- =============================================================================
-- Migration 110 -- canonical_psh_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork lane 6)
-- Author: Logan Glosser <logan.glosser@gmail.com> (drafted with Copilot)
-- Plan:   Close the PSH family. Events table closed mig_104 (d971cdc) using
--         extraction-faithfulness against Script 365 deterministic transforms.
--         Rollup build_ts is 2026-04-22, but events verification did not modify
--         events data, so a verify-only sign-off is appropriate.
--
-- Methodology: Derivation re-derivation against verified events
--   (mig_106 parathyroid rollup pattern, adapted to Script 365 PSH full-cohort
--   rollup). Re-derived each patient-level aggregate from
--   main.canonical_psh_events_v1 using scripts/365_psh_pmh_meds_consolidation.py
--   _build_rollup_sql_for_domain('psh') logic, then compared against the live
--   main.canonical_psh_patient_rollup_v1 row-by-row.
--
-- Pre-signoff probe (run 2026-04-29 via local connect_locked/MotherDuck):
--   - Cohort parity: 10,871 rollup rows / 10,871 patients = canonical_patient_master.
--   - Events: 3,919 rows / 1,878 patients (canonical_psh_events_v1, verified mig_104).
--   - Rollup patients with findings: 1,878 (= event patients).
--   - 26/26 derivable columns: 0 drift vs fresh re-derivation.
--   - Anti-join: 0 live-not-fresh, 0 fresh-not-live.
--   - No STRING_AGG columns in PSH rollup, so no ordering-artifact exception.
--
-- Sign-off scope:
--   26 not_started cols flipped to verified via
--   derivation_re_derivation_against_verified_events:
--     anchor_source, n_findings_any, n_findings_present,
--     n_findings_definitive, n_findings_probable_or_better,
--     first_finding_date, last_finding_date, n_distinct_findings_norm,
--     psh_prior_thyroidectomy_{definitive,probable_or_better,any_evidence},
--     psh_prior_neck_surgery_{definitive,probable_or_better,any_evidence},
--     psh_prior_parathyroidectomy_{definitive,probable_or_better,any_evidence},
--     psh_prior_rai_{definitive,probable_or_better,any_evidence},
--     psh_prior_fna_{definitive,probable_or_better,any_evidence},
--     psh_prior_neck_dissection_{definitive,probable_or_better,any_evidence}
--   2 already-na cols carry over: research_id (auto_identifier_skip),
--   build_ts (auto_provenance_skip).
--
-- Final expected state of canonical_psh_patient_rollup_v1 (post-mig_110):
--   Rows     : 10,871 (one per canonical_patient_master patient)
--   Patients : 10,871
--   Cols     : 28
--   Verified : 26 / 28 + 2 na = 28 / 28 closed
--
-- PSH family complete: events (mig_104) + rollup (mig_110).
-- =============================================================================

-- 110a: flip all 26 derivable not_started cols.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_110_psh_rollup_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_110: per-patient aggregate re-derivation '
                          || 'against verified canonical_psh_events_v1 '
                          || '(mig_104, d971cdc) using Script 365 PSH rollup '
                          || 'logic. Cohort parity 10,871 = CPM; events '
                          || '3,919 rows / 1,878 patients; rollup patients '
                          || 'with findings 1,878; 0 drift across all 26 '
                          || 'derivable columns; 0 anti-join rows both '
                          || 'directions. Rollup build_ts 2026-04-22 remains '
                          || 'consistent because mig_104 verified extraction '
                          || 'faithfulness without modifying events data.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_psh_patient_rollup_v1'
  AND verification_status = 'not_started';

-- 110b: recompute table_signoff_registry counts and sign off.
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/110_psh_patient_rollup_signoff.sql',
    notes             = 'Derivation re-derivation against verified canonical_psh_events_v1 '
                        || '(mig_104 / d971cdc). Full-cohort rollup has 10,871 '
                        || 'rows matching CPM and 1,878 patients with findings '
                        || 'matching events patients. Fresh Script 365 PSH '
                        || 'rollup derivation showed 0 drift across all 26 '
                        || 'derivable columns and 0 anti-join rows. 26 columns '
                        || 'verified, 2 auto identifier/provenance columns remain '
                        || 'NA. PSH family closed: events (mig_104) + rollup '
                        || '(mig_110).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_psh_patient_rollup_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 110 -- canonical_psh_patient_rollup_v1 closed under Protocol v2.
-- =============================================================================
