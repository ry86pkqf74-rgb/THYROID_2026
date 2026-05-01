-- mig_225_vw_ln_surgery_publication_safe — Lane LN (v14)
-- Per-(research_id, surgery_key) LN safe counts over canonical_path_malignant_events_dedup_VIEW_v1
-- MAX() aggregation within surgery per assessment §6 mig_225 (not SUM).
-- Quarantine filter for borderline/benign-with-staging applied in mig_229 (replaces this VIEW).

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 AS
WITH d AS (
  SELECT
    *,
    COALESCE(CAST(path_surgery_id AS VARCHAR), CAST(surgery_episode_id AS VARCHAR), 'NULL_SURG') AS surgery_key
  FROM main.canonical_path_malignant_events_dedup_VIEW_v1
),
agg AS (
  SELECT
    research_id,
    surgery_key,
    COUNT(DISTINCT primary_histology) AS n_histologies_surgery,
    MAX(TRY_CAST(ln_examined AS DOUBLE)) AS ln_examined_double,
    MAX(CAST(nodal_disease_total_count AS BIGINT)) AS nodal_disease_total_count_int,
    MAX(
      COALESCE(
        CAST(nodal_disease_positive_count AS BIGINT),
        TRY_CAST(ln_involved AS BIGINT)
      )
    ) AS ln_positive_agg
  FROM d
  GROUP BY research_id, surgery_key
),
calc AS (
  SELECT
    research_id,
    surgery_key,
    n_histologies_surgery,
    ln_examined_double,
    nodal_disease_total_count_int,
    CAST(ln_positive_agg AS BIGINT) AS ln_positive_safe,
    COALESCE(
      NULLIF(ln_examined_double, CAST(0 AS DOUBLE)),
      CAST(nodal_disease_total_count_int AS DOUBLE)
    ) AS ln_examined_safe,
    ((ln_examined_double IS NULL OR ln_examined_double = CAST(0 AS DOUBLE))
      AND nodal_disease_total_count_int IS NOT NULL
      AND nodal_disease_total_count_int > 0) AS ln_denominator_source_conflict_flag,
    (n_histologies_surgery > 1 AND COALESCE(ln_positive_agg, 0) > 0) AS ln_attribution_ambiguous_flag
  FROM agg
)
SELECT
  research_id,
  surgery_key,
  n_histologies_surgery,
  ln_examined_double,
  nodal_disease_total_count_int,
  ln_examined_safe,
  ln_positive_safe,
  ln_denominator_source_conflict_flag,
  ln_attribution_ambiguous_flag,
  (CAST(ln_positive_safe AS DOUBLE) > COALESCE(ln_examined_safe, CAST(-1 AS DOUBLE))) AS ln_impossible_count_flag
FROM calc;

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1',
   10, 10, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/225_vw_ln_surgery_publication_safe_20260430.sql',
   'tier2_analytic',
   'mig_225 Lane LN: surgery-grain LN safe view; denominator COALESCE(NULLIF(ln_examined_double,0), nodal_disease_total_count); conflict + impossible flags.',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'research_id', 'INTEGER', 1, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'surgery_key', 'VARCHAR', 2, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'n_histologies_surgery', 'BIGINT', 3, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_examined_double', 'DOUBLE', 4, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'nodal_disease_total_count_int', 'BIGINT', 5, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_examined_safe', 'DOUBLE', 6, 'analytic', 'derived_ln_denominator_rule_v14', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_positive_safe', 'BIGINT', 7, 'analytic', 'canonical_path_malignant_events_dedup_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_denominator_source_conflict_flag', 'BOOLEAN', 8, 'analytic', 'derived_ln_denominator_rule_v14', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_attribution_ambiguous_flag', 'BOOLEAN', 9, 'analytic', 'derived_multi_hist_rule_v14', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_surgery_publication_safe_VIEW_v1', 'ln_impossible_count_flag', 'BOOLEAN', 10, 'analytic', 'derived_ln_consistency_check_v14', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_225_lane_ln_v14', 'mig_225', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig225_vw_ln_surgery_publication_safe_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_225_vw_ln_surgery_publication_safe_VIEW_v1',
  '0', '0', '0',
  '0 | mig_225 Lane LN: vw_ln_surgery_publication_safe_VIEW_v1'
);
