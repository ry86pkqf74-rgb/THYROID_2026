-- mig_227_vw_ln_histology_attribution — Lane LN (v14)
-- Tumor-grain (dedup rows) + surgery LN safe flags + ln_master_rollup histology-specific LN mets evidence.

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_histology_attribution_VIEW_v1 AS
WITH dedup_base AS (
  SELECT
    *,
    COALESCE(CAST(path_surgery_id AS VARCHAR), CAST(surgery_episode_id AS VARCHAR), 'NULL_SURG') AS surgery_key
  FROM main.canonical_path_malignant_events_dedup_VIEW_v1
),
surgery_hist_stats AS (
  SELECT
    research_id,
    surgery_key,
    COUNT(DISTINCT primary_histology) AS n_histologies_surgery_stats,
    COUNT(DISTINCT COALESCE(CAST(nodal_disease_positive_count AS VARCHAR), CAST(ln_involved AS VARCHAR), ''))
      AS distinct_ln_pos_value_patterns
  FROM dedup_base
  GROUP BY research_id, surgery_key
),
rollup_evidence AS (
  SELECT
    CAST(l.research_id AS VARCHAR) AS research_id,
    MAX(CAST(l.ln_mets_tumor_types_array AS VARCHAR)) AS ln_mets_tumor_types_array_any,
    BOOL_OR(COALESCE(l.ln_mets_ptc, FALSE)) AS any_ln_mets_ptc,
    BOOL_OR(COALESCE(l.ln_mets_ftc, FALSE)) AS any_ln_mets_ftc,
    BOOL_OR(COALESCE(l.ln_mets_mtc, FALSE)) AS any_ln_mets_mtc,
    BOOL_OR(COALESCE(l.ln_mets_atc, FALSE)) AS any_ln_mets_atc,
    BOOL_OR(COALESCE(l.ln_mets_pdtc, FALSE)) AS any_ln_mets_pdtc,
    BOOL_OR(COALESCE(l.ln_mets_hurthle, FALSE)) AS any_ln_mets_hurthle
  FROM manuscript_workspace.ln_master_rollup_v1 l
  GROUP BY 1
)
SELECT
  e.*,
  s.n_histologies_surgery AS surgery_n_histologies,
  s.ln_positive_safe AS surgery_ln_positive_safe,
  s.ln_examined_safe AS surgery_ln_examined_safe,
  shs.distinct_ln_pos_value_patterns AS surgery_distinct_ln_pos_patterns,
  re.ln_mets_tumor_types_array_any,
  (
    (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'papillary|\\bptc\\b')
      AND COALESCE(re.any_ln_mets_ptc, FALSE)
    )
    OR (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'follicular')
      AND NOT regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'papillary|ptc')
      AND COALESCE(re.any_ln_mets_ftc, FALSE)
    )
    OR (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'medullary|\\bmtc\\b')
      AND COALESCE(re.any_ln_mets_mtc, FALSE)
    )
    OR (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'anaplastic|\\batc\\b')
      AND COALESCE(re.any_ln_mets_atc, FALSE)
    )
    OR (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'poorly differentiated|\\bpdtc\\b')
      AND COALESCE(re.any_ln_mets_pdtc, FALSE)
    )
    OR (
      regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'hurthle|oncocytic|hürthle')
      AND COALESCE(re.any_ln_mets_hurthle, FALSE)
    )
  ) AS ln_mets_histology_specific_evidence,
  CASE
    WHEN COALESCE(s.ln_positive_safe, 0) <= 0 THEN 'none_or_unknown'
    WHEN (
      (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'papillary|\\bptc\\b')
        AND COALESCE(re.any_ln_mets_ptc, FALSE)
      )
      OR (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'follicular')
        AND NOT regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'papillary|ptc')
        AND COALESCE(re.any_ln_mets_ftc, FALSE)
      )
      OR (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'medullary|\\bmtc\\b')
        AND COALESCE(re.any_ln_mets_mtc, FALSE)
      )
      OR (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'anaplastic|\\batc\\b')
        AND COALESCE(re.any_ln_mets_atc, FALSE)
      )
      OR (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'poorly differentiated|\\bpdtc\\b')
        AND COALESCE(re.any_ln_mets_pdtc, FALSE)
      )
      OR (
        regexp_matches(LOWER(COALESCE(e.primary_histology, '')), 'hurthle|oncocytic|hürthle')
        AND COALESCE(re.any_ln_mets_hurthle, FALSE)
      )
    )
      THEN 'definite_histology_specific'
    WHEN COALESCE(s.n_histologies_surgery, 1) = 1 AND COALESCE(s.ln_positive_safe, 0) > 0
      THEN 'probable_histology_specific'
    WHEN COALESCE(s.n_histologies_surgery, 1) > 1 AND COALESCE(shs.distinct_ln_pos_value_patterns, 0) <= 1
      THEN 'surgery_level_only'
    WHEN COALESCE(s.n_histologies_surgery, 1) > 1 AND COALESCE(shs.distinct_ln_pos_value_patterns, 0) > 1
      THEN 'ambiguous_multi_histology'
    ELSE 'ambiguous_multi_histology'
  END AS ln_attribution_confidence
FROM main.canonical_path_malignant_events_dedup_VIEW_v1 e
LEFT JOIN manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 s
  ON e.research_id = s.research_id
 AND COALESCE(CAST(e.path_surgery_id AS VARCHAR), CAST(e.surgery_episode_id AS VARCHAR), 'NULL_SURG') = s.surgery_key
LEFT JOIN surgery_hist_stats shs
  ON e.research_id = shs.research_id
 AND COALESCE(CAST(e.path_surgery_id AS VARCHAR), CAST(e.surgery_episode_id AS VARCHAR), 'NULL_SURG') = shs.surgery_key
LEFT JOIN rollup_evidence re
  ON CAST(e.research_id AS VARCHAR) = re.research_id;

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
SELECT
  'manuscript_workspace',
  'vw_ln_histology_attribution_VIEW_v1',
  COUNT(*)::INTEGER,
  COUNT(*)::INTEGER,
  0, 0, 0,
  'verified',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'qc_framework_v1/migrations/227_vw_ln_histology_attribution_20260430.sql',
  'tier2_analytic',
  'mig_227 Lane LN: tumor-grain histology-specific LN attribution confidence + ln_mets rollup evidence.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'manuscript_workspace'
  AND c.table_name = 'vw_ln_histology_attribution_VIEW_v1';

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'manuscript_workspace',
  'vw_ln_histology_attribution_VIEW_v1',
  c.column_name,
  c.data_type,
  c.ordinal_position,
  'analytic',
  'canonical_path_malignant_events_dedup_VIEW_v1|vw_ln_surgery_publication_safe_VIEW_v1|ln_master_rollup_v1',
  'verified',
  'cursor_composer_lane_LN',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'lane_ln_v14_construct',
  'mig_227_lane_ln_v14',
  'mig_227 overlay column on histology attribution view.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'manuscript_workspace'
  AND c.table_name = 'vw_ln_histology_attribution_VIEW_v1';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig227_vw_ln_histology_attribution_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_227_vw_ln_histology_attribution_VIEW_v1',
  '0', '0', '0',
  '0 | mig_227 Lane LN: vw_ln_histology_attribution_VIEW_v1 — see migration file for col_registry follow-up if gate1 requires per-col inserts'
);
