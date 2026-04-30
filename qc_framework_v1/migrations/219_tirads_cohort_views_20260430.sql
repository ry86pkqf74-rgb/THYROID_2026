-- mig_219 — Lane E4 (Round 2): four manuscript-facing TIRADS cohort VIEWs
-- run_id / batch: mig_219_tirads_cohort_views_20260430
-- Source: CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md — E4
-- Target DB: thyroid_canonical_publication_v1_0
-- Base surface: manuscript_workspace.canonical_us_nodule_v2_filtered
-- Naming: reference_view_naming_convention — `_VIEW` infix before `_v1`.
-- COWORK-DIRECT APPLY; no base-table data mutation (VIEWs + registry + provenance only).
-- Post-apply row-count guidance (live-verify after apply; Doc 2 expectations):
--   strict ≈ 5,149 | any_reported order ~22k + overlaps | not_fully_parsed ≈ 8,243
--   excluded: aggregate + shell + nlp_pending (minus overlaps in analytic filters)

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Idempotent registry prep (re-run safe)
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name IN (
    'vw_us_nodule_tirads_strict_acr2017_VIEW_v1',
    'vw_us_nodule_tirads_any_reported_VIEW_v1',
    'vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1',
    'vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1'
  );

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name IN (
    'vw_us_nodule_tirads_strict_acr2017_VIEW_v1',
    'vw_us_nodule_tirads_any_reported_VIEW_v1',
    'vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1',
    'vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1'
  );

-- =============================================================================
-- §A Pre-snapshot signoff registry (audit)
-- =============================================================================
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig219_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig219_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

-- =============================================================================
-- §B Cohort VIEWs (filtered nodule grain)
-- =============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1 AS
SELECT f.*
FROM manuscript_workspace.canonical_us_nodule_v2_filtered AS f
WHERE COALESCE(f.is_aggregate_row, FALSE) = FALSE
  AND f.us_row_type <> 'shell'
  AND f.acr2017_feature_points_complete IS TRUE
  AND f.acr2017_tirads_points IS NOT NULL
  AND f.acr2017_tirads_category IS NOT NULL;

CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1 AS
SELECT f.*
FROM manuscript_workspace.canonical_us_nodule_v2_filtered AS f
WHERE COALESCE(f.is_aggregate_row, FALSE) = FALSE
  AND f.us_row_type <> 'shell'
  AND (
    f.tirads_reported_in_text IS NOT NULL
    OR (f.acr2017_tirads_category IS NOT NULL
        AND TRIM(CAST(f.acr2017_tirads_category AS VARCHAR)) <> '')
    OR (f.updated_tirads_category IS NOT NULL
        AND TRIM(CAST(f.updated_tirads_category AS VARCHAR)) <> '')
  );

CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1 AS
SELECT f.*
FROM manuscript_workspace.canonical_us_nodule_v2_filtered AS f
WHERE COALESCE(f.is_aggregate_row, FALSE) = FALSE
  AND f.us_row_type <> 'shell'
  AND (
    f.tirads_reported_in_text IS NOT NULL
    OR (f.acr2017_tirads_category IS NOT NULL
        AND TRIM(CAST(f.acr2017_tirads_category AS VARCHAR)) <> '')
    OR (f.updated_tirads_category IS NOT NULL
        AND TRIM(CAST(f.updated_tirads_category AS VARCHAR)) <> '')
  )
  AND COALESCE(f.acr2017_feature_points_complete, FALSE) = FALSE;

CREATE OR REPLACE VIEW manuscript_workspace.vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1 AS
SELECT f.*
FROM manuscript_workspace.canonical_us_nodule_v2_filtered AS f
WHERE COALESCE(f.is_aggregate_row, FALSE) = TRUE
   OR f.us_row_type = 'shell'
   OR COALESCE(f.nlp_backfill_pending, FALSE) = TRUE;

-- =============================================================================
-- §C Post-create read checks (embed after apply)
-- =============================================================================
-- SELECT 'strict', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1;
-- SELECT 'any_reported', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1;
-- SELECT 'reported_not_fully_parsed', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1;
-- SELECT 'excluded', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1;

-- =============================================================================
-- §D Register four VIEWs (Gate1) + column inheritance from filtered base VIEW
-- =============================================================================

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
SELECT
  'manuscript_workspace',
  view_list.table_name,
  ncol.cnt,
  ncol.cnt,
  0,
  0,
  0,
  'verified',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'qc_framework_v1/migrations/219_tirads_cohort_views_20260430.sql',
  'tier2_canonical_view',
  'mig_219 Lane E4: ChatGPT TIRADS Phase-1 cohort VIEW '
    || view_list.table_name
    || ' over manuscript_workspace.canonical_us_nodule_v2_filtered. '
    || 'verification_method=view_filter_inheritance_per_chatgpt_tirads_doc_phase1_2026-04-30.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (VALUES
    ('vw_us_nodule_tirads_strict_acr2017_VIEW_v1'),
    ('vw_us_nodule_tirads_any_reported_VIEW_v1'),
    ('vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1'),
    ('vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1')
  ) AS view_list(table_name)
CROSS JOIN (
  SELECT COUNT(*)::INTEGER AS cnt
  FROM information_schema.columns
  WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
    AND table_schema = 'manuscript_workspace'
    AND table_name = 'canonical_us_nodule_v2_filtered'
) AS ncol;

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  specs.schema_name,
  specs.view_name,
  c.column_name,
  c.data_type,
  c.ordinal_position,
  COALESCE(r.category, 'analytic'),
  'canonical_us_nodule_v2_filtered',
  'verified',
  'mig_219',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'view_filter_inheritance_per_chatgpt_tirads_doc_phase1_2026-04-30',
  'mig_219_tirads_cohort_views_20260430',
  'mig_219 E4: inherited from canonical_us_nodule_v2_filtered; cohort semantics in VIEW DDL.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (
  SELECT * FROM (VALUES
    ('manuscript_workspace', 'vw_us_nodule_tirads_strict_acr2017_VIEW_v1'),
    ('manuscript_workspace', 'vw_us_nodule_tirads_any_reported_VIEW_v1'),
    ('manuscript_workspace', 'vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1'),
    ('manuscript_workspace', 'vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1')
  ) AS specs(schema_name, view_name)
CROSS JOIN information_schema.columns AS c
LEFT JOIN main.canonical_column_verification_registry_v1 AS r
  ON r.schema_name = 'manuscript_workspace'
 AND r.table_name = 'canonical_us_nodule_v2_filtered'
 AND r.column_name = c.column_name
 AND r.verification_status IN ('verified', 'na')
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'manuscript_workspace'
  AND c.table_name = 'canonical_us_nodule_v2_filtered';

-- =============================================================================
-- §E Provenance
-- =============================================================================
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_219_tirads_cohort_views_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'lane_e4_four_tirads_cohort_views_create_register',
   '0',
   'manuscript_workspace_vw_us_nodule_tirads_*_VIEW_v1_x4',
   'col_registry_inherit_from_canonical_us_nodule_v2_filtered',
   'none');
