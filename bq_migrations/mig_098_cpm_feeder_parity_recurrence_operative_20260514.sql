-- mig_098 — CPM feeder parity closeout (recurrence columns + operative facade refresh)
-- Project: thyroid-canonical-pub-2026
-- Date: 2026-05-14
--
-- (1) pub_canonical.canonical_recurrence_v1: restore recurrence_histology and
--     recurrence_evidence_source from legacy snapshot (12-column parity vs
--     pub_legacy_source_20260416; Script 203 contract).
-- (2) pub_canonical.operative_episode_detail_v2: CREATE OR REPLACE VIEW to
--     force BigQuery to re-resolve SELECT * against legacy (fixes stale 39-col
--     cached schema after source grew to 48 cols including rln_signal_status_nlp,
--     op_time_nlp_present, los_nlp_present, ligasure_used_nlp, harmonic_used_nlp,
--     energy_device_other_used_nlp, suture_ligation_only_nlp,
--     trach_concurrent_evidence, trach_nonperioperative_evidence).
--
-- Preconditions:
--   - canonical_recurrence_v1 in pub_canonical is a BASE TABLE with 10,871 rows
--   - Legacy snapshot may have the two columns typed INT64 and 100% NULL in BQ.
--
-- ⚠ §1b DEPRECATED (circular provenance: feeder ← CPM output).
--     Legacy MERGE from pub_legacy_source is parity-only; prefer BQ-native rebuild:
--     bq_migrations/mig_101_canonical_recurrence_v1_bq_native_histology_evidence_20260514.sql
--     (supersedes mig_100 parquet-from-archive path).
--
-- Post-checks (run in BigQuery):
--   SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`;
--   SELECT column_name FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
--     WHERE table_name = 'canonical_recurrence_v1' ORDER BY ordinal_position;
--   SELECT column_name FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
--     WHERE table_name = 'operative_episode_detail_v2' ORDER BY ordinal_position;
-- =============================================================================

-- §1 Recurrence clinical columns backfill from legacy ---------------------------------
-- Run once per dataset. If columns already exist, skip the ALTERs or expect a benign error.
ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`
ADD COLUMN recurrence_histology STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`
ADD COLUMN recurrence_evidence_source STRING;

MERGE `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` AS T
USING `thyroid-canonical-pub-2026.pub_legacy_source_20260416.canonical_recurrence_v1` AS S
ON CAST(T.research_id AS STRING) = CAST(S.research_id AS STRING)
WHEN MATCHED THEN UPDATE SET
  T.recurrence_histology = CAST(S.recurrence_histology AS STRING),
  T.recurrence_evidence_source = CAST(S.recurrence_evidence_source AS STRING);

-- §1b REMOVED — was CPM MERGE (circular: canonical_recurrence_v1 is an INPUT to CPM assembly).
-- Feed BigQuery-native rebuild via mig_101 (not parquet / not CPM).

-- §2 Operative episode facade — re-resolve SELECT * ---------------------------------
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.operative_episode_detail_v2`
AS SELECT * FROM `thyroid-canonical-pub-2026.pub_legacy_source_20260416.operative_episode_detail_v2`;
