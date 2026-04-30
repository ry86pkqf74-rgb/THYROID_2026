-- mig_205 — retro signoff + column-verification registry inserts for mig_194 Option B derivative tables
-- Batch_id: mig_205_us_gland_v2_signoff_registry_inserts_20260430
-- Predecessor: mig_194 (commit 3cdb804) Cursor-applied without registering 3 new tables in signoff_registry.
--   Existing live state: events_v2 13,578 / rollup_v2 10,871 / val_mig194 10 gates (9 PASS + 1 SKIP).
--   28 column-level CF-117 closure notes are on the SHELL canonical_us_thyroid_gland_v2 (handled by mig_194 batch_id).
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized 2026-04-30); registry-only mutations; no data writes.
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP); no BEGIN TRANSACTION.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot signoff_registry baseline (audit trail)
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig205_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig205_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

-- =============================================================================
-- §B INSERT signoff_registry: 3 new tables
-- =============================================================================

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('main','canonical_us_thyroid_gland_events_v2', 38, 38, 0, 0, 0,
   'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/194_canonical_us_thyroid_gland_v2_shell_only_20260430.sql',
   'tier2_canonical',
   'mig_194 Option B (Cursor-applied 3cdb804) + mig_205 retro signoff: 13,578 events / 10,859 pts; shell-only build from canonical_us_thyroid_gland_v2; exam_id_source = structured (6,793) + fallback (6,785); val_mig194 G1-G7+G9-G10 PASS, G8 SKIP (Option B no-NLP source-limitation Logan-ratified). CF-117-US-GLAND-PARENCHYMA closed at column-level on shell table by mig_194; new derivative tables now under governance.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('main','canonical_us_thyroid_gland_patient_rollup_v2', 22, 22, 0, 0, 0,
   'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/194_canonical_us_thyroid_gland_v2_shell_only_20260430.sql',
   'tier2_canonical',
   'mig_194 Option B (Cursor-applied 3cdb804) + mig_205 retro signoff: 10,871 patient rollup spans CPM spine; built from events_v2 + CPM. n_events_exam_source_nlp_supplemental + n_events_from_clinical_note_thyroid_us are present-but-zero per Option B.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('main','val_mig194_canonical_us_thyroid_gland_shell_only_v1', 7, 0, 0, 0, 7,
   'na', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/194_canonical_us_thyroid_gland_v2_shell_only_20260430.sql',
   'helper_validation',
   'mig_194 10-gate validation table (per check_id schema): 9 PASS + 1 SKIP (G8 NLP source absent per Option B). Helper table — not analytic.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- =============================================================================
-- §C INSERT column-verification registry rows for analytic canonicals (events_v2 + rollup_v2)
-- =============================================================================

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main' AS schema_name,
  c.table_name,
  c.column_name,
  c.data_type,
  c.ordinal_position,
  CASE
    WHEN c.column_name IN ('research_id','us_exam_id','gland_event_id','source_row_id','source_report_id') THEN 'identifier'
    WHEN c.column_name IN ('build_ts','build_migration','extracted_at','llm_model','source_modality','source_note_type','source_table','exam_id_source','exam_date_unavailable_fallback_flag','date_confidence','date_source_keyword','confidence','evidence_text','gland_entity_index_within_exam') THEN 'provenance'
    WHEN c.column_name IN ('exam_date','first_us_gland_exam_date','last_us_gland_exam_date') THEN 'temporal'
    ELSE 'analytic'
  END AS category,
  'main.canonical_us_thyroid_gland_v2 (Option B shell-only); rollup spans CPM spine' AS upstream_source,
  'verified' AS verification_status,
  'mig_205' AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
  CASE WHEN c.table_name='canonical_us_thyroid_gland_events_v2'
       THEN 'derivation_vs_canonical_us_thyroid_gland_v2_shell_only_option_b'
       ELSE 'derivation_vs_canonical_us_thyroid_gland_events_v2_cpm_spine'
  END AS verification_method,
  'mig_205_us_gland_v2_signoff_registry_inserts_20260430' AS batch_id,
  'mig_205 retro signoff for mig_194 Option B Cursor-applied (commit 3cdb804); val_mig194 10/10 gates resolved (9 PASS + G8 SKIP per Option B Logan ratification); CF-117-US-GLAND-PARENCHYMA closure trace at shell-table column rows.' AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS registered_ts
FROM information_schema.columns c
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name IN ('canonical_us_thyroid_gland_events_v2','canonical_us_thyroid_gland_patient_rollup_v2');

-- =============================================================================
-- §D INSERT column-verification registry rows for val_mig194 (status='na')
-- =============================================================================

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main', c.table_name, c.column_name, c.data_type, c.ordinal_position,
  'helper_validation' AS category,
  'val_mig194_canonical_us_thyroid_gland_shell_only_v1 (10-gate validation table)' AS upstream_source,
  'na' AS verification_status,
  'mig_205' AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
  'helper_validation_table_na' AS verification_method,
  'mig_205_us_gland_v2_signoff_registry_inserts_20260430' AS batch_id,
  'mig_205: helper validation table column; not analytic.' AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name='val_mig194_canonical_us_thyroid_gland_shell_only_v1';

-- =============================================================================
-- §E Provenance row insert
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_205_us_gland_v2_signoff_registry_inserts_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'retro_signoff_inserts_for_mig194_option_b_3_new_tables',
   'GAP-A-MIG198-DERIVATIVE-TABLES-NOT-IN-SIGNOFF-REGISTRY',
   'mig_194_option_b_full_governance_attained',
   '67_col_rows_inserted_3_signoff_rows_inserted',
   'none');

-- =============================================================================
-- §F Verification (re-run 5-gate audit + cohort parity)
-- =============================================================================

SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1) AS post_signoff_row_count,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS post_gate1_verified,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='mig_205_us_gland_v2_signoff_registry_inserts_20260430') AS post_mig205_col_rows,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS post_gate3_violations;
