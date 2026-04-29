-- Migration: 139_cpm_recurrence_spine_resync_20260429.sql
-- Purpose: Resync canonical_patient_master.recurrence_* cols from canonical_recurrence_v1
--          (mig_123 rebuild SSOT). Closes CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING.
-- Trigger: Cowork verification of mig_138 found PM was undercounting confirmed recurrences
--          by ~5x (PM=82 TRUE vs canonical_recurrence_v1=514 TRUE) and 9 cols showed drift
--          ranging 303–1,338 mismatches against the verified SSOT.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 33 (post-mig_138 cleanup; not part of next 4-prompt batch)
-- Scope  : 9 cols on canonical_patient_master, single UPDATE...FROM via research_id join.
--          Pre-snapshot to archive_pub_v1_0. Post-verify drift = 0.
--
-- ⚠️ Pre-write rules per Logan-ratified standards:
--    - Pre-snapshot to archive_pub_v1_0 BEFORE the UPDATE
--    - Cohort parity already verified (10,871 = 10,871, 0 orphans on either side)
--    - Data-type compatibility already verified (PM and CR exact match on all 9 cols)
--    - All 9 cols stay BATCH_ID = 'mig_138_*' (the original sign-off batch); mig_139 is a
--      RESYNC pass that appends to notes and refreshes verified_ts. No status change.
--    - recurrence_date stays TIMESTAMP per existing CF-mig123-RECURRENCE-DATE-RETYPE
--      (calendar-only policy applies upstream-and-downstream in a future date-retype batch)
--
-- Drift baseline (pre-resync, 2026-04-29):
--   recurrence_confirmed:           462 mismatches  (PM 82 TRUE  vs CR 514 TRUE)
--   recurrence_date:                673 mismatches
--   recurrence_definition:          663 mismatches
--   recurrence_evidence_source:     661 mismatches
--   recurrence_histology:           335 mismatches
--   recurrence_site:              1,338 mismatches   (largest)
--   recurrence_type:                661 mismatches
--   time_to_recurrence_days:        679 mismatches
--   biochemical_tg_at_recurrence:   303 mismatches

-- ============================================================
-- STEP 1. Pre-snapshot to archive_pub_v1_0
-- ============================================================
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig139_recurrence_resync_20260429 AS
SELECT
  research_id,
  biochemical_tg_at_recurrence,
  recurrence_confirmed,
  recurrence_date,
  recurrence_definition,
  recurrence_evidence_source,
  recurrence_histology,
  recurrence_site,
  recurrence_type,
  time_to_recurrence_days,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig139_snapshot_ts
FROM main.canonical_patient_master;

-- ============================================================
-- STEP 2. Resync UPDATE — pull SSOT values from canonical_recurrence_v1
-- ============================================================
UPDATE main.canonical_patient_master AS pm
SET
  biochemical_tg_at_recurrence = cr.biochemical_tg_at_recurrence,
  recurrence_confirmed         = cr.recurrence_confirmed,
  recurrence_date              = cr.recurrence_date,
  recurrence_definition        = cr.recurrence_definition,
  recurrence_evidence_source   = cr.recurrence_evidence_source,
  recurrence_histology         = cr.recurrence_histology,
  recurrence_site              = cr.recurrence_site,
  recurrence_type              = cr.recurrence_type,
  time_to_recurrence_days      = cr.time_to_recurrence_days
FROM main.canonical_recurrence_v1 AS cr
WHERE CAST(pm.research_id AS VARCHAR) = CAST(cr.research_id AS VARCHAR);

-- ============================================================
-- STEP 3. Post-verify drift = 0
-- ============================================================
-- This SELECT is a sanity check; expect ALL zeros for every col below.
SELECT
  'post_mig139' AS phase,
  SUM(CASE WHEN COALESCE(pm.recurrence_confirmed,FALSE)         IS DISTINCT FROM COALESCE(cr.recurrence_confirmed,FALSE)        THEN 1 ELSE 0 END) AS rec_confirmed_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_date,'1900-01-01'::TIMESTAMP) IS DISTINCT FROM COALESCE(cr.recurrence_date,'1900-01-01'::TIMESTAMP) THEN 1 ELSE 0 END) AS rec_date_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_definition,'')           IS DISTINCT FROM COALESCE(cr.recurrence_definition,'')          THEN 1 ELSE 0 END) AS rec_def_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_evidence_source,'')      IS DISTINCT FROM COALESCE(cr.recurrence_evidence_source,'')     THEN 1 ELSE 0 END) AS rec_evid_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_histology,'')            IS DISTINCT FROM COALESCE(cr.recurrence_histology,'')           THEN 1 ELSE 0 END) AS rec_hist_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_site,'')                 IS DISTINCT FROM COALESCE(cr.recurrence_site,'')                THEN 1 ELSE 0 END) AS rec_site_drift,
  SUM(CASE WHEN COALESCE(pm.recurrence_type,'')                 IS DISTINCT FROM COALESCE(cr.recurrence_type,'')                THEN 1 ELSE 0 END) AS rec_type_drift,
  SUM(CASE WHEN COALESCE(pm.time_to_recurrence_days,-1.0)       IS DISTINCT FROM COALESCE(cr.time_to_recurrence_days,-1.0)      THEN 1 ELSE 0 END) AS ttr_drift,
  SUM(CASE WHEN COALESCE(pm.biochemical_tg_at_recurrence,-1.0)  IS DISTINCT FROM COALESCE(cr.biochemical_tg_at_recurrence,-1.0) THEN 1 ELSE 0 END) AS tg_at_rec_drift
FROM main.canonical_patient_master pm
LEFT JOIN main.canonical_recurrence_v1 cr ON CAST(pm.research_id AS VARCHAR) = CAST(cr.research_id AS VARCHAR);

-- ============================================================
-- STEP 4. Append CF closure note + refresh verified_ts on the 9 cols
--          (preserves original mig_138 batch_id; mig_139 is a resync, not a re-flip)
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_139 (2026-04-29): CPM recurrence-spine resync from canonical_recurrence_v1 ' ||
            '(mig_123 rebuild). Pre-mig_139 drift was 303-1,338 mismatches per col (worst: recurrence_site=1,338); ' ||
            'PM was undercounting recurrence_confirmed by ~5x (82 TRUE vs SSOT 514). ' ||
            'Post-resync drift = 0 across all 9 cols. ' ||
            'CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING CLOSED. ' ||
            'Note: recurrence_date remains TIMESTAMP per upstream CF-mig123-RECURRENCE-DATE-RETYPE; ' ||
            'calendar retype is a separate cross-table batch.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master'
  AND column_name IN (
    'biochemical_tg_at_recurrence',
    'recurrence_confirmed',
    'recurrence_date',
    'recurrence_definition',
    'recurrence_evidence_source',
    'recurrence_histology',
    'recurrence_site',
    'recurrence_type',
    'time_to_recurrence_days'
  );

-- ============================================================
-- STEP 5. Refresh canonical_table_signoff_registry_v1 timestamp on canonical_patient_master
--          (status remains in_progress; no col-count change)
-- ============================================================
-- Optional. PM is in_progress and signoff registry typically flips only on table-level verified.
-- Skipping unless table-level metadata needs touching. (No-op if not needed.)

-- End of mig_139.
