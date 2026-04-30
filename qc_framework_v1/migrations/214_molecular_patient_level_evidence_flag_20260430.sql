-- mig_214 — Patient-level-only molecular evidence flag (NULL molecular_episode_id cohort)
-- Target DB: thyroid_canonical_publication_v1_0
-- Investigation: qc_framework_v1/reports/mig_214_investigation_molecular_null_episode_20260430.md
-- Decision: 525 rows are script_269_backfill with no recoverable test/date anchors; flag for analysts.
--
USE thyroid_canonical_publication_v1_0;

-- §0 Pre-flight
SELECT 'cmg_v2_row_count' AS check_name, COUNT(*) AS n FROM main.canonical_molecular_genetics_v2;
SELECT 'null_episode_rows' AS check_name, COUNT(*) AS n
FROM main.canonical_molecular_genetics_v2 WHERE molecular_episode_id IS NULL;

-- §A Pre-snapshot (full table, 1,384 rows)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_molecular_genetics_v2_pre_mig214_patient_level_flag_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig214_snapshot_ts
FROM main.canonical_molecular_genetics_v2;

-- §B Schema — additive column + comment
ALTER TABLE main.canonical_molecular_genetics_v2
  ADD COLUMN IF NOT EXISTS is_patient_level_only_evidence BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN main.canonical_molecular_genetics_v2.is_patient_level_only_evidence IS
'TRUE when structured molecular genetics row has no molecular_episode_id (patient-level / script_269_backfill-only anchor). Per-test or test-dated analyses MUST exclude these rows (WHERE is_patient_level_only_evidence = FALSE). Added mig_214 2026-04-30 after NULL-episode investigation confirmed no recoverable episode key in canonical or legacy thyroseq enrichment.';

-- §C Populate flag
UPDATE main.canonical_molecular_genetics_v2
SET is_patient_level_only_evidence = TRUE
WHERE molecular_episode_id IS NULL;

UPDATE main.canonical_molecular_genetics_v2
SET is_patient_level_only_evidence = FALSE
WHERE molecular_episode_id IS NOT NULL;

-- §D Column verification registry (single new column; ordinal 75)
INSERT INTO main.canonical_column_verification_registry_v1 (
  schema_name, table_name, column_name, data_type, ordinal_position,
  category, upstream_source, verification_status, verified_by, verified_ts,
  verification_method, batch_id, notes, registered_ts
)
SELECT
  'main',
  'canonical_molecular_genetics_v2',
  'is_patient_level_only_evidence',
  'BOOLEAN',
  75,
  'derived',
  NULL,
  'verified',
  'cursor_agent',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'derivation_from_molecular_episode_id_null_indicator_post_investigation_confirmed_no_recoverable_anchor',
  'mig_214_molecular_patient_level_evidence_flag_20260430',
  'TRUE for 525 rows where molecular_episode_id IS NULL (520 patients). All script_269_backfill — '
    || 'no test_date_native or resolved_test_date or FNA/surgery links — see mig_214 investigation report.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name = 'main'
    AND r.table_name = 'canonical_molecular_genetics_v2'
    AND r.column_name = 'is_patient_level_only_evidence'
);

-- §E Table signoff registry — recompute from column registry (mig_124 pattern)
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
    signoff_migration = 'qc_framework_v1/migrations/214_molecular_patient_level_evidence_flag_20260430.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_214: is_patient_level_only_evidence BOOLEAN derived flag '
                        || '(525 TRUE = NULL molecular_episode_id, script_269_backfill cohort). '
                        || 'CF: per-test analyses exclude WHERE is_patient_level_only_evidence=FALSE.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_molecular_genetics_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- §F Provenance ledger
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_214_molecular_patient_level_evidence_flag_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'canonical_molecular_genetics_v2_add_is_patient_level_only_evidence_update_525_rows_registry_signoff',
   'NULL_MOLECULAR_EPISODE_ID_SCRIPT_269_BACKFILL_COHORT_FLAGGED',
   'pre_snapshot_archive_pub_v1_0_canonical_molecular_genetics_v2_pre_mig214_patient_level_flag_20260430',
   'column_verification_registry_insert_ordinal_75_table_signoff_recomputed',
   'none');

-- §G Post-verify
SELECT 'flag_true_count' AS check_name, COUNT(*) AS n
FROM main.canonical_molecular_genetics_v2 WHERE is_patient_level_only_evidence IS TRUE;
SELECT 'flag_false_nonnull_episode' AS check_name, COUNT(*) AS n
FROM main.canonical_molecular_genetics_v2
WHERE is_patient_level_only_evidence IS NOT TRUE AND molecular_episode_id IS NOT NULL;
SELECT 'equiv_null_episode_vs_flag' AS check_name,
       SUM(CASE WHEN (molecular_episode_id IS NULL) <> (is_patient_level_only_evidence IS TRUE) THEN 1 ELSE 0 END) AS mismatch_rows
FROM main.canonical_molecular_genetics_v2;
