-- =============================================================================
-- Migration 168b — PM empty-VARCHAR reclass + BOOLEAN sneaker CF (Cowork-authored)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Cowork direct cleanup (per v6 handoff §9 + §7 pre-authorization)
--
-- Lane:    mig_168b — Cowork direct cleanup
-- batch_id: mig_168b_pm_empty_varchar_reclass_20260429
--
-- EFFECT: Registry-only writes. No PM data mutation.
--   * 2 BOOLEAN cols stay `verified`, get CF-mig167-RLN-FLAG-DEGENERATE note (rln_permanent_flag, rln_transient_flag)
--   * 2 BOOLEAN cols reclass `verified` → `na` (nsqip_hypoparathyroidism_recovered_flag, biochemical_concern_flag)
--   * 5 VARCHAR cols reclass `verified` → `na` (empty_verified_varchar pattern)
--   * 1 PM signoff registry resync at end
--
-- Pre-flight (Cowork live 2026-04-29 verified):
--   | col                                     | dtype   | status   | T  / F     / N    |
--   |-----------------------------------------|---------|----------|---|------|-------|
--   | rln_permanent_flag                      | BOOLEAN | verified | 0 / 10871  / 0    |  contradicts comp_rln_injury_confirmed=39
--   | rln_transient_flag                      | BOOLEAN | verified | 0 / 10871  / 0    |  same lineage
--   | nsqip_hypoparathyroidism_recovered_flag | BOOLEAN | verified | 0 / 10871  / 0    |  vs hypocalcemia mate=80 TRUE
--   | biochemical_concern_flag                | BOOLEAN | verified | 0 / 10871  / 0    |  Script 224 deferred
--   | gm_recurrence_site_primary              | VARCHAR | verified | 0 non-null/10871 NULL
--   | tsh_suppressed_ever_source              | VARCHAR | verified | 0 non-null/10871 NULL
--   | op_esophageal_inv_first_evidence_text   | VARCHAR | verified | 0 non-null/10871 NULL
--   | nucmed_tgab_max_source                  | VARCHAR | verified | 0 non-null/10871 NULL
--   | biochemical_concern_first_date_source   | VARCHAR | verified | 0 non-null/10871 NULL
--
-- Apply order: Step 4 of apply queue (between mig_159 and mig_160 per v6 §9.4) so
--   the PM resync in §C doesn't conflict with mig_152's later resync.
--
-- Rollback: archive_pub_v1_0.canonical_column_verification_registry_pre_mig168b_20260429
--   captures the 9 affected rows pre-mutation. Restore via UPDATE FROM if needed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshot (registry slice for the 9 cols only)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig168b_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig168b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN (
    'rln_permanent_flag','rln_transient_flag',
    'nsqip_hypoparathyroidism_recovered_flag','biochemical_concern_flag',
    'gm_recurrence_site_primary','tsh_suppressed_ever_source',
    'op_esophageal_inv_first_evidence_text','nucmed_tgab_max_source',
    'biochemical_concern_first_date_source'
  );

-- -----------------------------------------------------------------------------
-- Section B1 — rln_*_flag cols stay verified, open CF
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED — '
            || 'rln_permanent_flag and rln_transient_flag both 0 TRUE / 10871 FALSE / 0 NULL while '
            || 'comp_rln_injury_confirmed=39 patients (mig_135 cluster). The extracted_rln_injury_refined_v2 '
            || 'spine appears unpopulated; PM displays the v2 flags but no data has flowed through. '
            || 'Defer to refined_v2 pipeline restoration; keep verified informational.'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN ('rln_permanent_flag','rln_transient_flag');

-- -----------------------------------------------------------------------------
-- Section B2 — nsqip_hypoparathyroidism_recovered_flag verified → na
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_nsqip_hypopara_recovered_pending_real_extraction',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    batch_id='mig_168b_pm_empty_varchar_reclass_20260429',
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE — '
            || '0 TRUE / 10871 FALSE / 0 NULL; mate nsqip_hypocalcemia_recovered_flag has 80 TRUE '
            || 'in identical NSQIP study scope. Reclassified verified→na (placeholder pending real population). '
            || 'Pre-snapshot canonical_column_verification_registry_pre_mig168b_20260429.'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name='nsqip_hypoparathyroidism_recovered_flag';

-- -----------------------------------------------------------------------------
-- Section B3 — biochemical_concern_flag verified → na
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_biochemical_concern_pending_script_224_landing',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    batch_id='mig_168b_pm_empty_varchar_reclass_20260429',
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER — '
            || '0 TRUE / 10871 FALSE / 0 NULL; mig_134 marked Script 224 helper "deferred" but col was verified. '
            || 'Reclassified verified→na (placeholder pending Script 224 build). '
            || 'Inconsistent with biochemical_recurrence_flag=128 TRUE (different lineage; recurrence_v1 spine).'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name='biochemical_concern_flag';

-- -----------------------------------------------------------------------------
-- Section B4 — 5 empty_verified_varchar cols reclass verified → na
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='na',
    verification_method='helper_empty_varchar_pending_real_extraction',
    verified_by='cowork',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    batch_id='mig_168b_pm_empty_varchar_reclass_20260429',
    notes = COALESCE(notes,'')
            || ' | mig_168b: CF-mig168b-EMPTY-VERIFIED-VARCHAR-RECLASS-NA — '
            || 'col has 0 non-null / 10871 NULL across cohort; reclassified verified→na (placeholder). '
            || 'See mig_168 audit (qc_framework_v1/reports/mig_168_pm_controlled_vocab_audit_20260429.md).'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN (
    'gm_recurrence_site_primary',
    'tsh_suppressed_ever_source',
    'op_esophageal_inv_first_evidence_text',
    'nucmed_tgab_max_source',
    'biochemical_concern_first_date_source'
  );

-- -----------------------------------------------------------------------------
-- Section C — Resync canonical_table_signoff_registry_v1 for canonical_patient_master
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
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
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes           = COALESCE(ts.notes, '')
                      || ' | mig_168b: 7 cols verified→na (Cowork session-1 sneakers + mig_168 empty-VARCHAR finds). '
                      || '2 BOOLEAN cols (rln_*) stay verified with CF-mig167-RLN-FLAG-DEGENERATE note.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- -----------------------------------------------------------------------------
-- Section D — Post-state verify (commented; Cowork runs after apply)
-- -----------------------------------------------------------------------------
-- D1: Per-col status check (5 reclassed varchar + 2 reclassed bool should be na;
--     2 rln cols should still be verified)
-- SELECT column_name, verification_status, batch_id
-- FROM main.canonical_column_verification_registry_v1
-- WHERE schema_name='main' AND table_name='canonical_patient_master'
--   AND column_name IN (
--     'rln_permanent_flag','rln_transient_flag',
--     'nsqip_hypoparathyroidism_recovered_flag','biochemical_concern_flag',
--     'gm_recurrence_site_primary','tsh_suppressed_ever_source',
--     'op_esophageal_inv_first_evidence_text','nucmed_tgab_max_source',
--     'biochemical_concern_first_date_source'
--   )
-- ORDER BY column_name;
-- Expect: rln_*_flag both verified; other 7 all na with batch_id=mig_168b_*.

-- D2: PM signoff registry post-state
-- SELECT n_verified, n_na, n_not_started, n_failed, n_columns_total, table_status
-- FROM main.canonical_table_signoff_registry_v1 WHERE table_name='canonical_patient_master';
-- Apply order: if mig_168b runs after mig_159 (Step 3), expect:
--   1441 + 27 - 7 = 1461 verified / 13 + 7 = 20 na / 117 not_started / 0 failed / 1598 total / in_progress
-- (Pre-159: 1441/13/144/0/1598 ; post-159: 1468/13/117/0/1598 ; post-168b: 1461/20/117/0/1598)

-- =============================================================================
-- end migration 168b — PM empty-VARCHAR reclass + BOOLEAN sneaker CF (9 cols)
-- =============================================================================
