-- mig_334 — Clear mig_333 BQ-placeholder caveat after BigQuery mig_100 applied + staging loaded.
--
-- Run manually after:
--   bq query < bq_migrations/mig_100_canonical_recurrence_v1_archive_feeder_mig332_20260514.sql

USE thyroid_canonical_publication_v1_0;

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
  || ' | mig_334 2026-05-14: BigQuery recurrence_histology/recurrence_evidence_source reloaded '
  || 'from MotherDuck mig_332 parquet → pub_workspace.stg_canonical_recurrence_v1_mig332 → mig_100 MERGE; '
  || 'feeder aligned with archive_pre_mig284 provenance (non-circular).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND column_name IN ('recurrence_histology', 'recurrence_evidence_source');
