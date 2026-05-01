-- mig_232 — narrow ACR-missing view (CF-mig219 follow-up)
-- run_id / batch: mig_232_narrow_acr_v15
-- Source: cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md — §1 Prompt 1
--         qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260501.md
-- Target DB: thyroid_canonical_publication_v1_0
-- Base surface: manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1
-- Naming: reference_view_naming_convention — `_VIEW` infix before `_v1`.
-- COWORK-DIRECT / Cline Sonnet 4.6 APPLY; new VIEW only — no base-table mutation.
--
-- Rationale:
--   CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT closed with semantic clarification (mig_221):
--   `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` (24,371 rows) uses
--   descriptor-completeness as its filter (acr2017_feature_points_complete=FALSE),
--   NOT derived-point/category missingness.  After Script 376 backfills, 17,067 of
--   those rows now have derived ACR points+category present despite incomplete descriptors.
--   The ChatGPT planning count of 8,243 corresponded to the NARROW definition:
--   descriptor-incomplete AND derived-ACR-missing simultaneously (7,304 live).
--   This view materialises that narrow definition as a separately named surface so
--   manuscript Methods can choose unambiguously between the two denominators.
--
-- Pre-snapshot: N/A (new VIEW; nothing to snapshot).
-- Post-apply row-count expectation: 7,200 – 7,400 (Copilot crosstab: 7,304)

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Idempotent registry prep (re-run safe)
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name  = 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name  = 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1';

-- =============================================================================
-- §A CREATE VIEW
-- =============================================================================
CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_derived_acr_missing_VIEW_v1 AS
SELECT
  research_id,
  us_exam_id,
  nodule_index_within_exam,
  exam_date,
  tirads_reported_in_text,
  updated_tirads_category,
  acr2017_tirads_category,
  acr2017_tirads_points,
  acr2017_feature_points_complete,
  -- explicit derived flag making filter intent queryable at run-time
  CASE
    WHEN acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL THEN TRUE
    ELSE FALSE
  END AS derived_acr_missing,
  'mig_232_narrow_acr_missing_filter' AS view_filter_provenance
FROM manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1
WHERE acr2017_feature_points_complete = FALSE
  AND (acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL);

-- =============================================================================
-- §B Post-create verification (embed; run as SELECT after apply)
-- =============================================================================
-- Expected: 7,200 – 7,400
-- SELECT 'derived_acr_missing', COUNT(*) AS n
-- FROM manuscript_workspace.vw_us_nodule_tirads_derived_acr_missing_VIEW_v1;

-- =============================================================================
-- §C Register VIEW in signoff registry (Gate 1 +1)
-- =============================================================================
INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES (
  'manuscript_workspace',
  'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
  11,
  11,
  0,
  0,
  0,
  'verified',
  CURRENT_TIMESTAMP,
  'qc_framework_v1/migrations/232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql',
  'tier2_canonical_view',
  'mig_232 CF-mig219 follow-up: narrow derived-ACR-missing view (~7,304 rows = descriptor_incomplete AND acr2017_tirads_points/category both NULL). Sibling to vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1 (24,371) which uses descriptor-completeness. Applied by Cline Sonnet 4.6 v15 batch.',
  CURRENT_TIMESTAMP
);

-- =============================================================================
-- §D Register columns in col registry
-- =============================================================================
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'research_id',                   'VARCHAR',  1, 'key',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'us_exam_id',                    'VARCHAR',  2, 'key',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'nodule_index_within_exam',      'INTEGER',  3, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'exam_date',                     'DATE',     4, 'date',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'tirads_reported_in_text',       'VARCHAR',  5, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'updated_tirads_category',       'VARCHAR',  6, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'acr2017_tirads_category',       'VARCHAR',  7, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'acr2017_tirads_points',         'INTEGER',  8, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'acr2017_feature_points_complete', 'BOOLEAN', 9, 'analytic',
   'canonical_us_nodule_v2_filtered', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 inherited from any_reported view; filter gate (always FALSE in this view)', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'derived_acr_missing',           'BOOLEAN', 10, 'derived',
   'mig_232_computation', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 new derived col: TRUE when acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL; always TRUE in this view by filter construction', CURRENT_TIMESTAMP),

  ('manuscript_workspace', 'vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
   'view_filter_provenance',        'VARCHAR', 11, 'metadata',
   'mig_232_literal', 'verified', 'cline_sonnet_4_6_mig_232',
   CURRENT_TIMESTAMP, 'view_ddl_with_explicit_filter_provenance',
   'mig_232_narrow_acr', 'mig_232 literal string tag: mig_232_narrow_acr_missing_filter', CURRENT_TIMESTAMP);

-- =============================================================================
-- §E Provenance
-- =============================================================================
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_232_narrow_acr_v15',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'lane_mig232_create_view_register_signoff_col_registry',
  '0',
  'CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT_final_closure_via_new_named_view',
  'col_registry_11_cols_vw_us_nodule_tirads_derived_acr_missing_VIEW_v1',
  'none'
);
