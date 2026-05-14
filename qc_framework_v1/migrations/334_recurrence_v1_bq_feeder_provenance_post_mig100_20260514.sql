-- mig_334 — Clear mig_333 BQ-placeholder caveat after BigQuery mig_100 applied + staging loaded.
--
-- Run manually after:
--   bq query < bq_migrations/mig_101_canonical_recurrence_v1_bq_native_histology_evidence_20260514.sql

USE thyroid_canonical_publication_v1_0;

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
  || ' | mig_334 2026-05-14: BigQuery recurrence_histology/recurrence_evidence_source reloaded '
  || 'via mig_101 BQ-native tier logic (Scripts 203/203b); no MotherDuck archive / no parquet feeder; '
  || 'non-circular lineage.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND column_name IN ('recurrence_histology', 'recurrence_evidence_source');
