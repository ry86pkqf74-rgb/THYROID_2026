-- mig_228_qc_ln_histology_tables — Lane LN (v14)
-- Five governed QC tables (materialized) per assessment §6 mig_228.

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE TABLE manuscript_workspace.qc_ln_impossible_counts_v1 AS
SELECT *
FROM (
  SELECT
    CAST(v.research_id AS VARCHAR) AS research_id,
    'vw_ln_surgery_publication_safe_VIEW_v1'::VARCHAR AS source_table,
    'safe_ln_positive_gt_ln_examined_safe'::VARCHAR AS issue_class,
    v.ln_examined_double,
    v.nodal_disease_total_count_int,
    CAST(v.ln_positive_safe AS BIGINT) AS nodal_disease_positive_count_int,
    CAST(NULL AS BIGINT) AS ln_total_examined_rollup,
    CAST(NULL AS BIGINT) AS ln_total_positive_rollup,
    v.surgery_key AS context_key
  FROM manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 v
  WHERE v.ln_impossible_count_flag

  UNION ALL

  SELECT
    CAST(l.research_id AS VARCHAR),
    'ln_master_rollup_v1',
    'rollup_positive_gt_examined',
    CAST(NULL AS DOUBLE),
    CAST(NULL AS BIGINT),
    CAST(NULL AS BIGINT),
    CAST(l.ln_total_examined AS BIGINT),
    CAST(l.ln_total_positive AS BIGINT),
    CAST(NULL AS VARCHAR)
  FROM manuscript_workspace.ln_master_rollup_v1 l
  WHERE COALESCE(CAST(l.ln_total_positive AS BIGINT), 0)
      > COALESCE(CAST(l.ln_total_examined AS BIGINT), 0)

  UNION ALL

  SELECT
    CAST(c.research_id AS VARCHAR),
    'canonical_patient_master',
    'cpm_positive_gt_examined',
    CAST(NULL AS DOUBLE),
    CAST(NULL AS BIGINT),
    CAST(NULL AS BIGINT),
    CAST(c.ln_rollup_total_examined AS BIGINT),
    CAST(c.ln_rollup_total_positive AS BIGINT),
    CAST(NULL AS VARCHAR)
  FROM main.canonical_patient_master c
  WHERE COALESCE(CAST(c.ln_rollup_total_positive AS BIGINT), 0)
      > COALESCE(CAST(c.ln_rollup_total_examined AS BIGINT), 0)
) x;

CREATE OR REPLACE TABLE manuscript_workspace.qc_ln_duplicate_rollup_patients_v1 AS
SELECT research_id, COUNT(*) AS n_rollup_rows
FROM manuscript_workspace.ln_master_rollup_v1
GROUP BY research_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE manuscript_workspace.qc_ln_multihistology_attribution_queue_v1 AS
WITH d AS (
  SELECT
    *,
    COALESCE(CAST(path_surgery_id AS VARCHAR), CAST(surgery_episode_id AS VARCHAR), 'NULL_SURG') AS surgery_key,
    COALESCE(CAST(nodal_disease_positive_count AS BIGINT), TRY_CAST(ln_involved AS BIGINT)) AS ln_pos_eff
  FROM main.canonical_path_malignant_events_dedup_VIEW_v1
),
surg AS (
  SELECT
    research_id,
    surgery_key,
    COUNT(DISTINCT primary_histology) AS n_histologies,
    COUNT(DISTINCT CAST(ln_pos_eff AS VARCHAR)) AS distinct_ln_pos_patterns,
    MAX(COALESCE(ln_pos_eff, 0)) AS max_ln_pos
  FROM d
  GROUP BY research_id, surgery_key
)
SELECT
  research_id,
  surgery_key,
  n_histologies,
  distinct_ln_pos_patterns,
  max_ln_pos,
  CASE WHEN distinct_ln_pos_patterns <= 1 THEN 'identical_ln_values_across_rows'
       ELSE 'row_level_ln_positive_variation'
  END AS multihist_ln_pattern
FROM surg
WHERE n_histologies > 1 AND max_ln_pos > 0;

CREATE OR REPLACE TABLE manuscript_workspace.qc_histology_borderline_in_malignant_table_v1 AS
SELECT
  research_id,
  surgery_episode_id,
  path_surgery_id,
  tumor_ordinal,
  primary_histology,
  n_stage_ajcc8,
  m_stage_ajcc8,
  overall_stage_ajcc8
FROM main.canonical_path_malignant_events_dedup_VIEW_v1
WHERE (
    primary_histology IN ('FTUMP', 'follicular adenoma', 'Follicular adenoma')
    AND (
      CAST(n_stage_ajcc8 AS VARCHAR) LIKE 'N1%'
      OR CAST(m_stage_ajcc8 AS VARCHAR) = 'M1'
    )
  );

CREATE OR REPLACE TABLE manuscript_workspace.qc_histology_vocab_typos_v1 AS
SELECT DISTINCT primary_histology AS raw_primary_histology_present
FROM main.canonical_path_malignant_events_dedup_VIEW_v1
WHERE primary_histology IN (
  'Follicular caricinoma',
  'microcarinoma',
  'microcarcinooma',
  'microcaricnoma',
  'folliucalr',
  'follicualr',
  'classsical',
  'poorly differntiated'
);

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'qc_ln_impossible_counts_v1', 9, 9, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql', 'tier3_qc', 'mig_228 Lane LN QC impossible LN enumeration', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'qc_ln_duplicate_rollup_patients_v1', 2, 2, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql', 'tier3_qc', 'mig_228 duplicate ln_master_rollup patients', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'qc_ln_multihistology_attribution_queue_v1', 6, 6, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql', 'tier3_qc', 'mig_228 multi-hist LN attribution queue', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'qc_histology_borderline_in_malignant_table_v1', 8, 8, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql', 'tier3_qc', 'mig_228 FTUMP/FA with N1/M1 staging', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('manuscript_workspace', 'qc_histology_vocab_typos_v1', 1, 1, 0, 0, 0, 'verified', CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql', 'tier3_qc', 'mig_228 residual typo strings still present in dedup primary_histology', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'lane_ln_mig228_qc_ln_histology_tables_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'mig_228_qc_tables_x5',
  '0', '0', '0',
  '0 | mig_228 Lane LN: 5 QC tables materialized in manuscript_workspace'
);
