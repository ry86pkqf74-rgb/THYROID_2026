-- mig_229_borderline_quarantine_flag — Lane LN (v14)
-- Adds is_borderline_or_benign_with_staging to canonical_path_malignant_events_v1;
-- rebuilds dedup VIEW; rebuilds LN publication views excluding quarantined tumor rows.

USE thyroid_canonical_publication_v1_0;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig229_quarantine_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig229_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_path_malignant_events_v1;

ALTER TABLE main.canonical_path_malignant_events_v1
  ADD COLUMN is_borderline_or_benign_with_staging BOOLEAN;

UPDATE main.canonical_path_malignant_events_v1
SET is_borderline_or_benign_with_staging = TRUE
WHERE (
    primary_histology IN ('FTUMP', 'follicular adenoma', 'Follicular adenoma')
    AND (
      CAST(n_stage_ajcc8 AS VARCHAR) LIKE 'N1%'
      OR CAST(m_stage_ajcc8 AS VARCHAR) = 'M1'
    )
  );

UPDATE main.canonical_path_malignant_events_v1
SET is_borderline_or_benign_with_staging = FALSE
WHERE is_borderline_or_benign_with_staging IS NULL;

CREATE OR REPLACE VIEW main.canonical_path_malignant_events_dedup_VIEW_v1 AS
SELECT *
FROM main.canonical_path_malignant_events_v1
WHERE is_source_distinct_duplicate_grain = FALSE
   OR is_source_distinct_duplicate_grain IS NULL;

CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 AS
WITH d AS (
  SELECT
    *,
    COALESCE(CAST(path_surgery_id AS VARCHAR), CAST(surgery_episode_id AS VARCHAR), 'NULL_SURG') AS surgery_key
  FROM main.canonical_path_malignant_events_dedup_VIEW_v1
  WHERE NOT COALESCE(is_borderline_or_benign_with_staging, FALSE)
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

CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_histology_attribution_VIEW_v1 AS
WITH dedup_base AS (
  SELECT
    *,
    COALESCE(CAST(path_surgery_id AS VARCHAR), CAST(surgery_episode_id AS VARCHAR), 'NULL_SURG') AS surgery_key
  FROM main.canonical_path_malignant_events_dedup_VIEW_v1
  WHERE NOT COALESCE(is_borderline_or_benign_with_staging, FALSE)
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
FROM dedup_base e
LEFT JOIN manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 s
  ON e.research_id = s.research_id
 AND e.surgery_key = s.surgery_key
LEFT JOIN surgery_hist_stats shs
  ON e.research_id = shs.research_id
 AND e.surgery_key = shs.surgery_key
LEFT JOIN rollup_evidence re
  ON CAST(e.research_id AS VARCHAR) = re.research_id;

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'vw_ln_histology_attribution_VIEW_v1';

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
  'mig_229_quarantine_exclusion_recompile',
  'mig_229_lane_ln_v14',
  'mig_229 rebuild histology attribution view excluding quarantine rows.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'manuscript_workspace'
  AND c.table_name = 'vw_ln_histology_attribution_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'vw_ln_histology_attribution_VIEW_v1';

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
  'qc_framework_v1/migrations/229_borderline_quarantine_flag_20260430.sql',
  'tier2_analytic',
  'mig_229 recompiled vw_ln_histology_attribution_VIEW_v1 post-quarantine exclusion.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'manuscript_workspace'
  AND c.table_name = 'vw_ln_histology_attribution_VIEW_v1';

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
SELECT
  'main',
  'canonical_path_malignant_events_v1',
  c.column_name,
  c.data_type,
  c.ordinal_position,
  'analytic',
  'mig_229_borderline_quarantine_rule',
  'verified',
  'cursor_composer_lane_LN',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'mig_229_borderline_quarantine_rule',
  'mig_229_lane_ln_v14',
  'FTUMP / follicular adenoma with N1* or M1 staging — manuscript quarantine flag.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'main'
  AND c.table_name = 'canonical_path_malignant_events_v1'
  AND c.column_name = 'is_borderline_or_benign_with_staging'
  AND NOT EXISTS (
    SELECT 1 FROM main.canonical_column_verification_registry_v1 x
    WHERE x.schema_name = 'main'
      AND x.table_name = 'canonical_path_malignant_events_v1'
      AND x.column_name = 'is_borderline_or_benign_with_staging'
  );

UPDATE main.canonical_table_signoff_registry_v1
SET
  n_columns_total = (
    SELECT COUNT(*)::INTEGER
    FROM main.canonical_column_verification_registry_v1
    WHERE schema_name = 'main'
      AND table_name = 'canonical_path_malignant_events_v1'
  ),
  n_verified = (
    SELECT COUNT(*) FILTER (WHERE verification_status = 'verified')::INTEGER
    FROM main.canonical_column_verification_registry_v1
    WHERE schema_name = 'main'
      AND table_name = 'canonical_path_malignant_events_v1'
  ),
  signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  signoff_migration = 'qc_framework_v1/migrations/229_borderline_quarantine_flag_20260430.sql',
  notes = COALESCE(notes, '') || ' | mig_229: recounted cols after is_borderline_or_benign_with_staging'
WHERE schema_name = 'main'
  AND table_name = 'canonical_path_malignant_events_v1';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig229_borderline_quarantine_flag_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_229_quarantine_flag_dedup_recompile_ln_views',
  '0', '0', '0',
  '0 | mig_229: is_borderline_or_benign_with_staging + dedup VIEW + LN safe views exclude quarantine rows'
);
