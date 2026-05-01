-- mig_231 — v14 round registry cleanup
-- Date: 2026-05-01
-- Author: Cowork (direct apply, post-v14 Path-C verification)
-- Purpose: Fix two metadata artifacts surfaced during v14 verification.
--
-- (1) Duplicate signoff registry row for `manuscript_workspace.dim_histology_standardized_VIEW_v1`
--     - Cause: Lane LN's mig_224 apply path inserted twice (Cursor Composer at 2026-04-30 23:41:03
--       + Cline Sonnet 4.6's ISSUE_REGISTRY pass at 2026-05-01 00:16:03 re-ran the INSERT)
--     - Fix: DELETE the later row (preserve the original signed_off_ts)
--
-- (2) Lane G mig_223 signoff_migration path mismatch
--     - Cause: Cline GPT-5.5 used a Python apply script
--       (qc_framework_v1/scripts/apply_mig223_semantic_publication_layer.py) instead of the
--       conventional .sql migration file. Registry references the SQL path that doesn't exist.
--     - Fix: UPDATE the 9 Lane G signoff rows to reference the actual .py script
--
-- Pre-snapshot:
--     "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig231_20260501

-- ============================================================================
-- (0) PRE-SNAPSHOT
-- ============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig231_20260501 AS
SELECT * FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

-- ============================================================================
-- (1) DELETE duplicate dim_histology_standardized_VIEW_v1 row (keep original)
-- ============================================================================

DELETE FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name  = 'dim_histology_standardized_VIEW_v1'
  AND signed_off_ts > TIMESTAMP '2026-04-30 23:50:00';

-- Expected: 1 row deleted (the 2026-05-01 00:16:03 dup)

-- ============================================================================
-- (2) UPDATE Lane G signoff_migration paths to reference actual .py apply script
-- ============================================================================

UPDATE thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
SET signoff_migration = 'qc_framework_v1/scripts/apply_mig223_semantic_publication_layer.py'
WHERE schema_name      = 'semantic_publication'
  AND signoff_migration = 'qc_framework_v1/migrations/223_semantic_publication_layer_20260430.sql';

-- Expected: 9 rows updated (1 release_manifest + 8 safe views)

-- ============================================================================
-- (3) PROVENANCE
-- ============================================================================

INSERT INTO thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_231_v14_registry_cleanup_v15',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'mig_231_v14_registry_cleanup',
  '0',
  '0',
  '2',  -- 1 dup row deleted + 9 path UPDATE = 2 medium findings cleared (treated as one each)
  '0'
);

-- ============================================================================
-- (4) VERIFICATION (run as SELECT post-apply to confirm)
-- ============================================================================

-- Expected: gate1_total = gate1_distinct = 208 post-cleanup
-- SELECT
--   (SELECT COUNT(*) FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1_total,
--   (SELECT COUNT(DISTINCT (schema_name, table_name)) FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1_distinct;
