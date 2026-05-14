-- mig_100 — canonical_recurrence_v1: reload recurrence_histology + recurrence_evidence_source
-- from MotherDuck mig_332 export (archive canonical_recurrence_v1_pre_mig284_20260503),
-- NOT from canonical_patient_master (removes circular feeder←CPM lineage).
--
-- Project: thyroid-canonical-pub-2026
-- Supersedes: mig_098 §1b (CPM MERGE) — do not re-run §1b after this migration.
--
-- Prerequisites:
--   1. MotherDuck: scripts/mig_332_recurrence_histology_evidence_apply.py --apply
--   2. Export: scripts/mig_332_recurrence_export_reconcile.py --export-parquet <local.parquet>
--   3. Upload parquet to GCS (replace URI below), then load staging table.
--
-- Load example (adjust URI + dataset):
--   bq load --project_id=thyroid-canonical-pub-2026 \
--     --source_format=PARQUET \
--     --replace \
--     thyroid-canonical-pub-2026:pub_workspace.stg_canonical_recurrence_v1_mig332 \
--     gs://YOUR_BUCKET/path/stg_canonical_recurrence_v1_mig332.parquet
--
-- Post-checks:
--   SELECT COUNT(*), COUNTIF(recurrence_histology IS NOT NULL), COUNTIF(recurrence_evidence_source IS NOT NULL)
--   FROM `thyroid-canonical-pub-2026.pub_workspace.stg_canonical_recurrence_v1_mig332`;
--   -- Expect 10871 rows; non-null counts should match MotherDuck mig_332 validate output.
--
-- ⚠ Run `bq load ... --replace` BEFORE the MERGE below — empty staging would NULL-out targets.
-- =============================================================================

MERGE `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` AS T
USING `thyroid-canonical-pub-2026.pub_workspace.stg_canonical_recurrence_v1_mig332` AS S
ON CAST(T.research_id AS STRING) = CAST(S.research_id AS STRING)
WHEN MATCHED THEN UPDATE SET
  T.recurrence_histology = S.recurrence_histology,
  T.recurrence_evidence_source = S.recurrence_evidence_source;

-- Reconciliation vs interim CPM-backfill (informational):
-- SELECT
--   COUNT(*) AS n,
--   COUNTIF(
--     T.recurrence_histology IS NOT DISTINCT FROM CAST(P.recurrence_histology AS STRING)
--     AND T.recurrence_evidence_source IS NOT DISTINCT FROM CAST(P.recurrence_evidence_source AS STRING)
--   ) AS rows_match_cpm,
--   COUNTIF(NOT (
--     T.recurrence_histology IS NOT DISTINCT FROM CAST(P.recurrence_histology AS STRING)
--     AND T.recurrence_evidence_source IS NOT DISTINCT FROM CAST(P.recurrence_evidence_source AS STRING)
--   )) AS rows_mismatch_cpm
-- FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` T
-- JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` P
--   ON CAST(T.research_id AS STRING) = CAST(P.research_id AS STRING);
