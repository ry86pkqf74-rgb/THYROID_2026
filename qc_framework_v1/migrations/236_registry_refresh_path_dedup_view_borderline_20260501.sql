-- mig_236 — registry refresh: canonical_path_malignant_events_dedup_VIEW_v1
--           append `is_borderline_or_benign_with_staging` (added to base table by mig_229)
--           and update signoff registry n_columns_total 65 -> 66.
-- run_id / batch: mig_236_path_dedup_view_borderline_registry_refresh_20260501
-- Source: ChatGPT cleanup audit 2026-05-01 (verified live by Cowork);
--         see qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md §A claim 1.
-- Target DB: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT (Cowork orchestrator); registry-only mutation, no archive snapshot.
--
-- Rationale:
--   The dedup VIEW physically exposes 66 cols (DESCRIBE), but
--   canonical_column_verification_registry_v1 records only 65 rows for it.
--   The missing col is `is_borderline_or_benign_with_staging` (BOOLEAN, ord 66),
--   inherited from base canonical_path_malignant_events_v1 via mig_229 (Lane LN).
--   This is a metadata drift, not a data-quality failure — gate3 stays 0
--   because n_verified+n_na = n_columns_total within the registry's own bookkeeping
--   (65/0/65). gate1 stays 210 (this is an UPDATE, not a new view).
--
-- Pre-snapshot: N/A (metadata-only; reversible by row delete + count revert).
-- Post-apply: dedup VIEW registry row count 65 -> 66; signoff registry shows 66/66.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Idempotent prep — re-run safe
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_dedup_VIEW_v1'
  AND column_name = 'is_borderline_or_benign_with_staging';

-- =============================================================================
-- §A Append the missing column row (inherits from base mig_229 verification)
-- =============================================================================
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
VALUES (
  'main',
  'canonical_path_malignant_events_dedup_VIEW_v1',
  'is_borderline_or_benign_with_staging',
  'BOOLEAN',
  66,
  'analytic',
  'canonical_path_malignant_events_v1',
  'verified',
  'mig_236',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'view_filter_inheritance_from_canonical_path_malignant_events_v1_dedup_rule_mig185b',
  'mig_236_path_dedup_view_borderline_registry_refresh_20260501',
  'mig_236 registry refresh: col was added to base table by mig_229 (Lane LN borderline_quarantine_rule); inherited into dedup VIEW via mig_185b filter (is_source_distinct_duplicate_grain=FALSE OR IS NULL). All verification from base table applies. 27 TRUE rows on base; FTUMP / follicular adenoma with N1*/M1 staging — manuscript quarantine flag.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
);

-- =============================================================================
-- §B Update signoff registry: n_columns_total 65 -> 66, n_verified 65 -> 66
-- =============================================================================
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total  = 66,
    n_verified       = 66,
    signed_off_ts    = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/236_registry_refresh_path_dedup_view_borderline_20260501.sql',
    notes = 'mig_236 (2026-05-01) refresh: added is_borderline_or_benign_with_staging row inherited from base mig_229. mig_212 Lane B base notes preserved below. ORIGINAL: mig_212 Lane B: manuscript-safe dedup VIEW over canonical_path_malignant_events_v1. Filter: is_source_distinct_duplicate_grain=FALSE OR IS NULL (mig_185b rule). Post-apply: 5,944 rows / 4,022 patients / 0 duplicate (research_id, path_surgery_id, tumor_ordinal) keys. All cols inherited from base table; verification_method=view_filter_inheritance_from_canonical_path_malignant_events_v1_dedup_rule_mig185b. Analytic SQL must use this VIEW (not base table) for per-tumor counts to avoid double-counting source-distinct duplicates.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_dedup_VIEW_v1';

-- =============================================================================
-- §C Acceptance assertions (run after apply)
-- =============================================================================
-- ASSERT: dedup VIEW registry rows = 66 (was 65)
SELECT CASE WHEN COUNT(*) = 66 THEN 'PASS' ELSE 'FAIL: got ' || COUNT(*)::VARCHAR END AS assert_dedup_view_66_rows
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_path_malignant_events_dedup_VIEW_v1';

-- ASSERT: signoff registry shows 66/66 for dedup VIEW
SELECT CASE WHEN n_columns_total = 66 AND n_verified = 66 THEN 'PASS'
            ELSE 'FAIL: n_total=' || n_columns_total::VARCHAR || ' n_verified=' || n_verified::VARCHAR END AS assert_signoff_66
FROM main.canonical_table_signoff_registry_v1
WHERE table_name = 'canonical_path_malignant_events_dedup_VIEW_v1';

-- ASSERT: 5-gate audit unchanged at gate1=210, gates 2-5=0, parity TRUE
SELECT CASE WHEN gate1_verified_tables = 210
             AND gate2_missing_signoff = 0
             AND gate3_count_mismatch = 0
             AND gate4_verified_cols_missing_metadata = 0
             AND gate5_clinical_date_violations = 0
             AND cohort_parity_ok = TRUE
            THEN 'PASS'
            ELSE 'FAIL: gate1=' || gate1_verified_tables::VARCHAR
                || ' g2=' || gate2_missing_signoff::VARCHAR
                || ' g3=' || gate3_count_mismatch::VARCHAR
                || ' g4=' || gate4_verified_cols_missing_metadata::VARCHAR
                || ' g5=' || gate5_clinical_date_violations::VARCHAR
                || ' parity=' || cohort_parity_ok::VARCHAR
            END AS assert_dashboard_clean
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
