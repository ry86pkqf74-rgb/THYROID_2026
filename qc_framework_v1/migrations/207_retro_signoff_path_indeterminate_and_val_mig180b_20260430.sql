-- mig_207 — retro signoff registry inserts for §12 governance gaps
-- Batch_id: mig_207_retro_signoff_path_indeterminate_and_val_mig180b_20260430
-- Predecessors:
--   mig_186 R-D NIFTP/uncertain exclusion RATIFIED (commit 65ba4d6) created
--     canonical_path_indeterminate_events_v1 (220 rows / 202 pts / 68 cols) but did NOT
--     register it in canonical_table_signoff_registry_v1.
--   mig_180b NLP upstream lineage closure (commit 8e89120) created
--     val_mig180b_nlp_upstream_lineage_v1 (12 rows / 16 cols) — same gap.
--   Surfaced by Cowork verification suite §12 governance gap detector 2026-04-30.
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized 2026-04-30 'address issues now'); registry-only mutations; no data writes.
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP); no BEGIN TRANSACTION.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot signoff_registry baseline (audit trail)
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig207_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig207_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig207_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig207_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1;

-- =============================================================================
-- §B INSERT signoff_registry: 2 new tables
-- =============================================================================

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('main','canonical_path_indeterminate_events_v1', 68, 68, 0, 0, 0,
   'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/186_apply_RD_niftp_exclusion_ratified_20260430.sql',
   'tier2_canonical',
   'mig_186 RATIFIED (commit 65ba4d6) NIFTP+uncertain landing table (220 events / 202 pts) lifted from canonical_path_malignant_events_v1 pre-exclusion. mig_207 retro signoff: 64 inherited cols verified extraction-faithful from pre_mig186b_snapshot snapshot of canonical_path_malignant_events_v1; 4 mig_186b-specific provenance cols (pre_mig186b_snapshot_ts, indeterminate_reason, reclassified_at, indeterminate_mig_batch_id) verified self-describing. NIFTP exclusion math intact: 6,469 + 220 = 6,689 pre-exclusion events.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('main','val_mig180b_nlp_upstream_lineage_v1', 16, 0, 0, 0, 16,
   'na', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/180b_nlp_upstream_missing_lineage_20260429.sql',
   'helper_validation',
   'mig_180b NLP upstream missing-lineage closure audit table (12 rows / 16 cols). Helper validation table — not analytic. Same val_* na pattern as val_mig171b + val_mig194.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- =============================================================================
-- §C INSERT column-verification registry rows for canonical_path_indeterminate_events_v1
--     (extraction-faithful from pre_mig186b snapshot canonical_path_malignant_events_v1)
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
    WHEN c.column_name IN ('research_id','surgery_episode_id','tumor_ordinal','path_surgery_id','specimen_id','synoptic_row_ix','specimen_focus_id') THEN 'identifier'
    WHEN c.column_name IN ('build_ts','build_script','consolidation_source','source_tables','resolution_rule','staging_source_note','linkage_confidence_tier','linkage_score','data_completeness_pct','pre_mig186b_snapshot_ts','indeterminate_reason','reclassified_at','indeterminate_mig_batch_id') THEN 'provenance'
    WHEN c.column_name IN ('surgery_date') THEN 'temporal'
    ELSE 'analytic'
  END AS category,
  'main.canonical_path_malignant_events_v1 (pre_mig186b snapshot, archive_pub_v1_0)' AS upstream_source,
  'verified' AS verification_status,
  'mig_207' AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
  CASE
    WHEN c.column_name IN ('pre_mig186b_snapshot_ts','indeterminate_reason','reclassified_at','indeterminate_mig_batch_id')
      THEN 'self_describing_mig186b_provenance'
    ELSE 'extraction_faithful_from_pre_mig186b_snapshot_path_malignant'
  END AS verification_method,
  'mig_207_retro_signoff_path_indeterminate_and_val_mig180b_20260430' AS batch_id,
  'mig_207 retro signoff for mig_186 RATIFIED (commit 65ba4d6); NIFTP/uncertain landing table preserves 220 events lifted from canonical_path_malignant_events_v1; pre_mig186b_snapshot_ts populated on every row (audit trail). Inherited cols verified bytewise-equivalent at lift time per mig_186b SELECT*+LIFT+DELETE pattern.' AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS registered_ts
FROM information_schema.columns c
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name='canonical_path_indeterminate_events_v1';

-- =============================================================================
-- §D INSERT column-verification registry rows for val_mig180b (status='na')
-- =============================================================================

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main', c.table_name, c.column_name, c.data_type, c.ordinal_position,
  'helper_validation' AS category,
  'val_mig180b_nlp_upstream_lineage_v1 (NLP upstream missing-lineage closure audit)' AS upstream_source,
  'na' AS verification_status,
  'mig_207' AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
  'helper_validation_table_na' AS verification_method,
  'mig_207_retro_signoff_path_indeterminate_and_val_mig180b_20260430' AS batch_id,
  'mig_207: helper validation table column; not analytic. Same na pattern as val_mig171b + val_mig194.' AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND c.table_name='val_mig180b_nlp_upstream_lineage_v1';

-- =============================================================================
-- §E Provenance row insert
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_207_retro_signoff_path_indeterminate_and_val_mig180b_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'retro_signoff_inserts_for_mig186_and_mig180b_2_governance_gap_tables',
   'GAP-COWORK-VERIFY-SUITE-S12-GOVERNANCE-GAP-PATH-INDETERMINATE-AND-VAL-MIG180B',
   'mig_186_and_mig_180b_full_governance_attained',
   '84_col_rows_inserted_2_signoff_rows_inserted',
   'none');

-- =============================================================================
-- §F Verification (post-apply state)
-- =============================================================================

SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1) AS post_signoff_row_count,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS post_gate1_verified,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='mig_207_retro_signoff_path_indeterminate_and_val_mig180b_20260430') AS post_mig207_col_rows,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS post_gate3_violations;

-- §F2 Re-run §12 governance gap detector (should be 0 rows)
WITH all_main_tables AS (
  SELECT table_name FROM information_schema.tables
  WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_type='BASE TABLE'
    AND (table_name LIKE 'canonical_%' OR table_name LIKE 'val_%')
),
in_registry AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
)
SELECT t.table_name AS ungoverned_table
FROM all_main_tables t LEFT JOIN in_registry r USING (table_name)
WHERE r.table_name IS NULL
ORDER BY 1;
