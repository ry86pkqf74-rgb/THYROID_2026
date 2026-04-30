-- mig_212 — canonical_path_malignant_events_dedup_VIEW_v1 (single CREATE VIEW + register)
-- Batch_id: mig_212_canonical_path_malignant_dedup_view_20260430
-- Lane: B (Cline Sonnet 4.6) from CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md
-- Logan-locked decision: VIEW name = canonical_path_malignant_events_dedup_VIEW_v1
-- Filter rule per mig_185b: WHERE is_source_distinct_duplicate_grain=FALSE OR IS NULL
--   → 5,944 rows / 4,022 patients / 0 remaining duplicates by (research_id, path_surgery_id, tumor_ordinal)
-- Base table: main.canonical_path_malignant_events_v1 (6,469 rows, 65 cols, 525 dup-grain rows)
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized via locked prompt 2026-04-30); no data writes to base table.
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP); no BEGIN TRANSACTION.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Pre-snapshot signoff_registry baseline (audit trail)
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig212_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig212_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1;

-- =============================================================================
-- §B CREATE OR REPLACE VIEW — manuscript-safe deduplicated PM surface
-- =============================================================================

CREATE OR REPLACE VIEW main.canonical_path_malignant_events_dedup_VIEW_v1 AS
SELECT *
FROM main.canonical_path_malignant_events_v1
WHERE is_source_distinct_duplicate_grain = FALSE
   OR is_source_distinct_duplicate_grain IS NULL;

-- =============================================================================
-- §C Verify post-create (read-only checks embedded as comments after apply)
-- Expected: row_count=5944, distinct_pts=4022, uniqueness on (research_id, path_surgery_id, tumor_ordinal)
-- =============================================================================
-- SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_path_malignant_events_dedup_VIEW_v1;
-- SELECT COUNT(*) FROM (
--   SELECT research_id, path_surgery_id, tumor_ordinal, COUNT(*) AS n
--   FROM main.canonical_path_malignant_events_dedup_VIEW_v1
--   GROUP BY 1,2,3 HAVING n > 1
-- );

-- =============================================================================
-- §D INSERT 1 signoff_registry row for the new VIEW
-- =============================================================================

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('main', 'canonical_path_malignant_events_dedup_VIEW_v1', 65, 65, 0, 0, 0,
   'verified',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/212_canonical_path_malignant_dedup_view_20260430.sql',
   'tier2_canonical',
   'mig_212 Lane B: manuscript-safe dedup VIEW over canonical_path_malignant_events_v1. Filter: is_source_distinct_duplicate_grain=FALSE OR IS NULL (mig_185b rule). Post-apply: 5,944 rows / 4,022 patients / 0 duplicate (research_id, path_surgery_id, tumor_ordinal) keys. All 65 cols inherited from base table; verification_method=view_filter_inheritance_from_canonical_path_malignant_events_v1_dedup_rule_mig185b. Analytic SQL must use this VIEW (not base table) for per-tumor counts to avoid double-counting source-distinct duplicates.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- =============================================================================
-- §E INSERT col_registry rows for all 65 cols (inherited from base table)
-- =============================================================================

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main'                                                          AS schema_name,
  'canonical_path_malignant_events_dedup_VIEW_v1'                AS table_name,
  c.column_name,
  c.data_type,
  c.ordinal_position,
  COALESCE(r.category, 'analytic')                               AS category,
  'canonical_path_malignant_events_v1'                           AS upstream_source,
  'verified'                                                      AS verification_status,
  'mig_212'                                                       AS verified_by,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                           AS verified_ts,
  'view_filter_inheritance_from_canonical_path_malignant_events_v1_dedup_rule_mig185b' AS verification_method,
  'mig_212_canonical_path_malignant_dedup_view_20260430'         AS batch_id,
  'mig_212 Lane B: col inherited from canonical_path_malignant_events_v1 via dedup VIEW filter (is_source_distinct_duplicate_grain=FALSE OR IS NULL). All verification from base table applies.'
                                                                  AS notes,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                           AS registered_ts
FROM information_schema.columns c
LEFT JOIN (
  SELECT column_name, category
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name  = 'canonical_path_malignant_events_v1'
    AND verification_status IN ('verified', 'na')
) r ON r.column_name = c.column_name
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema  = 'main'
  AND c.table_name    = 'canonical_path_malignant_events_v1'
ORDER BY c.ordinal_position;

-- =============================================================================
-- §F INSERT provenance row
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
VALUES (
  'canonical_cleanup_mig212_dedup_view_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'lane_b_dedup_view_create_and_register',
  '0', '0', '0',
  '0 | mig_212 Lane B: CREATE OR REPLACE VIEW canonical_path_malignant_events_dedup_VIEW_v1 (5944 rows / 4022 pts / 0 dup keys). gate1 185->186. Col registry +65. Filter: is_source_distinct_duplicate_grain=FALSE OR IS NULL per mig_185b. Logan-authorized via locked prompt CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md.'
);
