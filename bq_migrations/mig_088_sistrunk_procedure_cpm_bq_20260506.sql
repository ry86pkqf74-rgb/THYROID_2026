-- migration_id: mig_088_sistrunk_procedure_cpm_bq_20260506
-- DFL: DFL-20260506-SISTRUNKPARSE-BQ
-- Linear: THY-4
--
-- BigQuery canonical additions for operative-note Sistrunk keyword arm (VC-TGDC-009):
--   * pub_canonical.canonical_patient_master — BOOLEAN + VARCHAR provenance (paraphrase only)
--   * pub_workspace.extracted_sistrunk_procedure_opnote_v1 — one row per note-level parse hit
--
-- Data population: scripts/mig_322_sistrunk_procedure_bq.py (--apply).
-- Operational notes filter: clinical_notes_long uses note_type ``OPNOTE`` in BQ exports
--   (MotherDuck used ``op_note``); the Python loader normalizes both.
--
-- BigQuery tip: ``ALTER TABLE`` bursts on ``canonical_patient_master`` can hit rate limits;
-- rerun failed statements after a cooldown; confirm columns with INFORMATION_SCHEMA before data apply.

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure BOOL;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure_evidence_summary STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure_match_kind STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure_match_offset INT64;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure_parser_rule_id STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS sistrunk_procedure_evidence_note_row_id STRING;

CREATE TABLE IF NOT EXISTS `thyroid-canonical-pub-2026.pub_workspace.extracted_sistrunk_procedure_opnote_v1` (
  research_id STRING NOT NULL OPTIONS(description="research_id (string-keyed cohort spine)"),
  note_row_id STRING OPTIONS(description="SHA-256 surrogate (no raw PHI)"),
  parser_rule_id STRING NOT NULL,
  match_kind STRING NOT NULL,
  match_offset INT64 NOT NULL,
  evidence_summary STRING NOT NULL OPTIONS(description="Paraphrase-only evidence line"),
  built_at TIMESTAMP NOT NULL OPTIONS(description="Population timestamp (UTC)")
);

-- Governance (idempotent)
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1` (
  migration_id,
  applied_at,
  applied_by,
  description,
  affected_dataset,
  affected_table,
  pre_snapshot_table,
  rows_before,
  rows_after,
  rollback_sql,
  notes
)
SELECT
  'mig_088_sistrunk_procedure_cpm_bq_schema_20260506',
  CURRENT_TIMESTAMP(),
  'cursor_agent_sistrunk_bq',
  'THY-4: ADD COLUMN sistrunk_procedure + cohort extract table schema on canonical BigQuery.',
  'pub_canonical',
  'canonical_patient_master',
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`),
  'Re-load population via scripts/mig_322_sistrunk_procedure_bq.py --apply.',
  FORMAT(
    'DFL=DFL-20260506-SISTRUNKPARSE-BQ; extract=%s;',
    '`thyroid-canonical-pub-2026.pub_workspace.extracted_sistrunk_procedure_opnote_v1`'
  )
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1
  FROM `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = 'mig_088_sistrunk_procedure_cpm_bq_schema_20260506'
);
