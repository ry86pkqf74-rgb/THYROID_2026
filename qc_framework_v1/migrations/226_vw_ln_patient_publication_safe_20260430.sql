-- mig_226_vw_ln_patient_publication_safe — Lane LN (v14)
-- Patient-grain LN SSOT: aggregates vw_ln_surgery_publication_safe_VIEW_v1 + crossval vs CPM ln_rollup_* 

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1 AS
WITH surg AS (
  SELECT * FROM manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1
),
pt AS (
  SELECT
    research_id,
    SUM(CAST(ln_examined_safe AS DOUBLE)) AS ln_total_examined_safe,
    SUM(CAST(ln_positive_safe AS BIGINT)) AS ln_total_positive_safe,
    SUM(CASE WHEN ln_impossible_count_flag THEN 1 ELSE 0 END) AS n_impossible_surgery_ln_rows,
    BOOL_OR(ln_attribution_ambiguous_flag) AS ln_attribution_ambiguous_any,
    BOOL_OR(ln_denominator_source_conflict_flag) AS ln_denominator_source_conflict_any
  FROM surg
  GROUP BY research_id
)
SELECT
  pt.research_id,
  CAST(pt.ln_total_examined_safe AS DOUBLE) AS ln_total_examined_safe,
  pt.ln_total_positive_safe,
  pt.n_impossible_surgery_ln_rows,
  pt.ln_attribution_ambiguous_any,
  pt.ln_denominator_source_conflict_any,
  cpm.ln_rollup_total_examined AS cpm_ln_rollup_total_examined,
  cpm.ln_rollup_total_positive AS cpm_ln_rollup_total_positive,
  CASE
    WHEN cpm.ln_rollup_total_examined IS NOT DISTINCT FROM CAST(ROUND(pt.ln_total_examined_safe) AS BIGINT)
     AND cpm.ln_rollup_total_positive IS NOT DISTINCT FROM pt.ln_total_positive_safe
      THEN 'concordant'
    WHEN cpm.ln_rollup_total_examined IS NULL AND cpm.ln_rollup_total_positive IS NULL
         AND NOT (pt.ln_total_examined_safe IS NULL AND pt.ln_total_positive_safe IS NULL)
      THEN 'cpm_only_null'
    WHEN pt.ln_total_examined_safe IS NULL AND pt.ln_total_positive_safe IS NULL
         AND NOT (cpm.ln_rollup_total_examined IS NULL AND cpm.ln_rollup_total_positive IS NULL)
      THEN 'safe_only_null'
    ELSE 'discordant_with_cpm'
  END AS ln_crossval_status
FROM pt
LEFT JOIN main.canonical_patient_master cpm
  ON CAST(pt.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR);

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1',
   9, 9, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/226_vw_ln_patient_publication_safe_20260430.sql',
   'tier2_analytic',
   'mig_226 Lane LN: patient LN SSOT + CPM crossval (IS NOT DISTINCT FROM on examined/positive integers).',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'research_id', 'INTEGER', 1, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'ln_total_examined_safe', 'DOUBLE', 2, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'ln_total_positive_safe', 'BIGINT', 3, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'n_impossible_surgery_ln_rows', 'BIGINT', 4, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'ln_attribution_ambiguous_any', 'BOOLEAN', 5, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'ln_denominator_source_conflict_any', 'BOOLEAN', 6, 'analytic', 'vw_ln_surgery_publication_safe_VIEW_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'cpm_ln_rollup_total_examined', 'BIGINT', 7, 'analytic', 'canonical_patient_master', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'cpm_ln_rollup_total_positive', 'BIGINT', 8, 'analytic', 'canonical_patient_master', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'vw_ln_patient_publication_safe_VIEW_v1', 'ln_crossval_status', 'VARCHAR', 9, 'analytic', 'derived_crossval_rule_v14', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_226_lane_ln_v14', 'mig_226', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig226_vw_ln_patient_publication_safe_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_226_vw_ln_patient_publication_safe_VIEW_v1',
  '0', '0', '0',
  '0 | mig_226 Lane LN: vw_ln_patient_publication_safe_VIEW_v1'
);
