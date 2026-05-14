-- mig_333 — Provenance note: BQ feeder recurrence_histology / recurrence_evidence_source
-- until mig_100 parquet reload (supersedes mig_098 §1b CPM circular backfill).
--
-- MotherDuck: after mig_332, both columns on main.canonical_recurrence_v1 are sourced from
-- archive_pub_v1_0.canonical_recurrence_v1_pre_mig284_20260503 (source-traced).
-- BigQuery may lag until stg_canonical_recurrence_v1_mig332 is loaded + mig_100 MERGE.
--
-- Apply on MotherDuck publication DB (same host as mig_332).

USE thyroid_canonical_publication_v1_0;

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
  || ' | mig_333 2026-05-14: BigQuery pub_canonical canonical_recurrence_v1 recurrence_histology/'
  || 'recurrence_evidence_source — if still populated only via superseded mig_098 §1b CPM MERGE, '
  || 'treat as CPM-backfill placeholder (not feeder-validated input) until '
  || 'bq_migrations/mig_100_canonical_recurrence_v1_archive_feeder_mig332_20260514.sql '
  || '+ scripts/mig_332_recurrence_export_reconcile.py parquet export/load. '
  || 'MD main.canonical_recurrence_v1 post-mig_332: archive_pre_mig284 join (source-traced).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND column_name IN ('recurrence_histology', 'recurrence_evidence_source');
