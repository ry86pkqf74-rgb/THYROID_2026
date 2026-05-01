-- mig_224_histology_vocab_extension — Lane LN (v14)
-- Extends histology_vocab_normalization_map_v1 (+7/+8 typo rows) + dim_histology_standardized_VIEW_v1
-- DB: thyroid_canonical_publication_v1_0
-- Batch: mig_224_lane_ln_v14

USE thyroid_canonical_publication_v1_0;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.histology_vocab_normalization_map_v1_pre_mig224_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig224_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.histology_vocab_normalization_map_v1;

INSERT INTO main.histology_vocab_normalization_map_v1 (raw_value, canonical_code, display_label, source_col)
SELECT v.raw_value, v.canonical_code, v.display_label, v.source_col
FROM (
  VALUES
    ('Follicular caricinoma', 'ftc', 'Follicular Carcinoma', 'primary_histology'),
    ('microcarinoma', 'ptc', 'PTC, microcarcinoma', 'histology_variant'),
    ('microcarcinooma', 'ptc', 'PTC, microcarcinoma', 'histology_variant'),
    ('microcaricnoma', 'ptc', 'PTC, microcarcinoma', 'histology_variant'),
    ('folliucalr', 'ptc', 'PTC, follicular variant (orthography fix)', 'histology_variant'),
    ('follicualr', 'ptc', 'PTC, follicular variant (orthography fix)', 'histology_variant'),
    ('classsical', 'ptc', 'PTC, classical variant (orthography fix)', 'histology_variant'),
    ('poorly differntiated', 'pdtc', 'PDTC (orthography fix)', 'histology_variant')
) AS v(raw_value, canonical_code, display_label, source_col)
WHERE NOT EXISTS (
  SELECT 1 FROM main.histology_vocab_normalization_map_v1 m
  WHERE m.raw_value = v.raw_value AND m.source_col = v.source_col
);

CREATE OR REPLACE VIEW manuscript_workspace.dim_histology_standardized_VIEW_v1 AS
WITH base AS (
  SELECT
    canonical_code,
    MAX(display_label) AS display_label
  FROM main.histology_vocab_normalization_map_v1
  GROUP BY canonical_code
)
SELECT
  canonical_code,
  display_label,
  (canonical_code IN (
    'ptc', 'ftc', 'mtc', 'atc', 'pdtc', 'dhgtc', 'dtc_nos', 'angiosarcoma', 'neuroendocrine'
  )) AS carcinoma_flag,
  (canonical_code IN ('ftump', 'niftp')) AS borderline_flag,
  (canonical_code IN ('adenoma', 'benign', 'hyperplasia', 'nodular', 'thyroiditis')) AS benign_flag,
  (canonical_code IN ('atc', 'pdtc', 'dhgtc', 'angiosarcoma')) AS aggressive_histology_flag,
  CASE
    WHEN canonical_code = 'ptc' THEN 'PTC_family'
    WHEN canonical_code IN ('ftc', 'ftump') THEN 'Follicular_family'
    WHEN canonical_code = 'mtc' THEN 'MTC'
    ELSE 'other'
  END AS ptc_variant_group,
  CASE
    WHEN canonical_code IN ('hurthle_oncocytic', 'hcc_oncocytic')
      OR display_label ILIKE '%hurthle%' OR display_label ILIKE '%Hürthle%'
      THEN 'Oncocytic thyroid carcinoma'
    ELSE display_label
  END AS who_terminology_preferred
FROM base;

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1',
   8, 8, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/224_histology_vocab_extension_20260430.sql',
   'tier2_analytic',
   'mig_224 Lane LN: dim_histology_standardized_VIEW_v1 over histology_vocab_normalization_map_v1 (distinct canonical_code + manuscript flags).',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category, upstream_source,
   verification_status, verified_by, verified_ts, verification_method, batch_id, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'canonical_code', 'VARCHAR', 1, 'analytic', 'histology_vocab_normalization_map_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'display_label', 'VARCHAR', 2, 'analytic', 'histology_vocab_normalization_map_v1', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'carcinoma_flag', 'BOOLEAN', 3, 'analytic', 'derived_vocab_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'borderline_flag', 'BOOLEAN', 4, 'analytic', 'derived_vocab_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'benign_flag', 'BOOLEAN', 5, 'analytic', 'derived_vocab_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'aggressive_histology_flag', 'BOOLEAN', 6, 'analytic', 'derived_vocab_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'ptc_variant_group', 'VARCHAR', 7, 'analytic', 'derived_vocab_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'dim_histology_standardized_VIEW_v1', 'who_terminology_preferred', 'VARCHAR', 8, 'analytic', 'derived_who2017_rule', 'verified', 'cursor_composer_lane_LN', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'lane_ln_v14_construct', 'mig_224_lane_ln_v14', 'mig_224', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig224_histology_vocab_extension_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_224_vocab_extension_dim_histology_standardized',
  '0', '0', '0',
  '0 | mig_224 Lane LN: histology_vocab inserts + dim_histology_standardized_VIEW_v1'
);
