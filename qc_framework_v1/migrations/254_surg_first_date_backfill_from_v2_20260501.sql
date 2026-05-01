-- =============================================================================
-- Migration 254 — CPM surg_first_date backfill from first_surgery_date_v2
-- =============================================================================
-- Date:   2026-05-01
-- Lane:   mig_254 / CF-SURG-FIRST-DATE-NULL-BACKFILL
-- Scope:  main.canonical_patient_master (rows where surg_first_date IS NULL)
--
-- BACKGROUND (investigation 2026-05-01):
--   first_surgery_date / first_surgery_date_v2 are 100% populated (10,871/10,871).
--   surg_first_date was NULL for 2,140 rows (operative spine gap) while
--   first_surgery_date_v2 matched path_synoptics-derived recovery for all of them.
--   M038 / cohort views using surg_first_date therefore overstated "surgical date unknown".
--
-- RULE:
--   UPDATE only WHERE surg_first_date IS NULL AND first_surgery_date_v2 IS NOT NULL.
--   Do not touch the ~105 rows where both columns are non-NULL but differ (multi-source spine).
--
-- GOVERNANCE: Logan-approved apply 2026-05-01 (Cursor).
-- Database: thyroid_canonical_publication_v1_0
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- Point-in-time archive of rows to be mutated (subset only)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig254_surg_first_backfill_20260501 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig254_snapshot_ts
FROM main.canonical_patient_master
WHERE surg_first_date IS NULL
  AND first_surgery_date_v2 IS NOT NULL;

BEGIN TRANSACTION;

-- Pre-gates
SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master WHERE surg_first_date IS NULL
) = 0
  THEN error('mig_254 abort: no NULL surg_first_date rows (already applied or unexpected state)')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master
  WHERE surg_first_date IS NULL AND first_surgery_date_v2 IS NULL
) > 0
  THEN error('mig_254 abort: surg_first NULL but first_surgery_date_v2 NULL — invariant violation')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master
) <> 10871
  THEN error('mig_254 abort: CPM row count must be 10,871')
  ELSE 0 END;

UPDATE main.canonical_patient_master AS pm
SET
  surg_first_date = pm.first_surgery_date_v2,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE pm.surg_first_date IS NULL
  AND pm.first_surgery_date_v2 IS NOT NULL;

-- Post-gates
SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master WHERE surg_first_date IS NULL
) > 0
  THEN error('mig_254 abort: post-update surg_first_date must be non-NULL for all CPM rows')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master
) <> 10871
  THEN error('mig_254 abort: CPM row count changed after UPDATE')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master
) <> 10871
  THEN error('mig_254 abort: distinct research_id must remain 10,871')
  ELSE 0 END;

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id, started_at, ended_at, phases_applied,
  critical_findings_cleared, high_findings_cleared, med_findings_cleared,
  held_for_adjudication
)
SELECT
  'mig_254_surg_first_date_backfill_from_v2_20260501',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'backfill_cpm_surg_first_date_from_first_surgery_date_v2_where_surg_first_was_null_only',
  '0',
  '0',
  CAST(COUNT(*) AS VARCHAR),
  '0'
FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig254_surg_first_backfill_20260501;

COMMIT;

-- =============================================================================
-- Post-apply spot checks (informational)
-- =============================================================================
-- SELECT COUNT(*) AS n_surg_first_null FROM main.canonical_patient_master WHERE surg_first_date IS NULL;
-- -- expected: 0
-- SELECT COUNT(*) AS archive_rows FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig254_surg_first_backfill_20260501;
-- -- expected: 2,140 at investigation time
-- =============================================================================
-- End mig_254
-- =============================================================================
