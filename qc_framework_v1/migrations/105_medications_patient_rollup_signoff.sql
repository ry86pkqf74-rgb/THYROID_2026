-- =============================================================================
-- Migration 105 -- canonical_medications_patient_rollup_v1 SIGN-OFF
-- (renumbered from 104 because Cursor lane 3 took 104 for psh_events_table_signoff
--  in parallel; both mig_105_psh_events and mig_105_medications_rollup landed
--  on MotherDuck within the same hour. Registry pointers updated to match new
--  filename. batch_id renamed mig_105_medications_rollup_* -> mig_105_*.)
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Close the medications family. Cursor's mig_103 (commit 3612128) signed
--         off canonical_medications_events_v1 + rebuilt the rollup (Script 365
--         step 2, fresh build_ts 2026-04-28 20:42). This migration verifies the
--         already-rebuilt rollup against the just-verified events and signs off
--         the 26 not_started cols.
--
-- Methodology: Derivation re-derivation against verified events (mig_95b /
--   mig_101 pattern). The rollup is already fresh (rebuilt by mig_103 apply
--   script post-events-cleanup), so no rebuild is needed — just verify and flip.
--
-- Pre-signoff probe (run 2026-04-29 via Cowork query_rw):
--   - Cohort parity: 10,871 rollup rows = 10,871 canonical_patient_master rows
--     (CHANGE J satisfied per Script 365 spec)
--   - Patients with meds in events: 1,820 (= n_findings_any > 0 in rollup)
--   - Patients without meds in events: 9,051 (rollup row exists with zeros/false)
--   - 1,820 patients × 15 derived metrics: 0 drift vs fresh re-derivation
--     (n_findings_any/present/definitive/probable_or_better, first/last_finding_date,
--      n_distinct_findings_norm, all 6 meds_*_definitive/probable_or_better/any_evidence)
--   - 9,051 patients × 4 sample no-meds checks: 0 contamination
--     (n_findings_any=0, first_finding_date=NULL, all phenotype BOOLs=FALSE)
--
-- Sign-off scope:
--   26 not_started cols flipped to verified via derivation_re_derivation_against_verified_events:
--     anchor_source, n_findings_any, n_findings_present, n_findings_definitive,
--     n_findings_probable_or_better, first_finding_date, last_finding_date,
--     n_distinct_findings_norm, meds_levothyroxine_{definitive, probable_or_better, any_evidence},
--     meds_calcium_supplement_*, meds_calcitriol_*, meds_rai_dose_*,
--     meds_methimazole_or_ptu_*, meds_liothyronine_*
--   2 already-na cols carry over: research_id (auto_identifier_skip), build_ts (auto_provenance_skip)
--
-- Final state of canonical_medications_patient_rollup_v1 (post-mig_105):
--   Rows     : 10,871 (one per patient_master patient — full cohort)
--   Patients : 10,871
--   Cols     : 28
--   Verified : 26 / 28 + 2 na = 28 / 28 closed
--
-- medications family complete: events (mig_103 commit 3612128) + rollup (this mig)
-- 21st canonical table closed under Protocol v2.
-- Executed via Cowork query_rw 2026-04-29.
-- =============================================================================

-- 104a: flip 26 not_started cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id            = 'mig_105_medications_rollup_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_105: rollup was rebuilt by mig_103 apply '
                          || '(Script 365 step 2, build_ts 2026-04-28 20:42) '
                          || 'against the just-verified canonical_medications_'
                          || 'events_v1. Per-pt aggregate re-derivation: 0 drift '
                          || 'across all 15 derived metrics on 1,820 with-meds '
                          || 'pts; 0 contamination on 9,051 no-meds pts. Cohort '
                          || 'parity 10,871 = canonical_patient_master 10,871 '
                          || '(CHANGE J satisfied).'
WHERE schema_name='main'
  AND table_name='canonical_medications_patient_rollup_v1'
  AND verification_status='not_started';

-- 104b: recompute table_signoff_registry counts and sign off
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
    signoff_migration = 'qc_framework_v1/migrations/105_medications_patient_rollup_signoff.sql',
    notes             = 'Derivation re-derivation against verified '
                        || 'canonical_medications_events_v1 (mig_103 commit '
                        || '3612128). Rollup was rebuilt by mig_103 apply '
                        || 'script (Script 365 step 2). 1,820 with-meds pts: '
                        || '0 drift across 15 derived metrics. 9,051 no-meds '
                        || 'pts: 0 contamination. Cohort parity 10,871 = CPM. '
                        || 'medications family closed: events (mig_103) + '
                        || 'rollup (mig_105). 21st canonical under Protocol v2.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_medications_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 104 -- canonical_medications_patient_rollup_v1 closed
-- 21st canonical table verified; medications family complete.
-- =============================================================================
