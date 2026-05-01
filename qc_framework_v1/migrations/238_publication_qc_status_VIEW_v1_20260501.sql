-- mig_238 — semantic_publication.vw_publication_qc_status_VIEW_v1
--           one-row "is the publication still clean?" superset of
--           manuscript_workspace.qc_audit_dashboard_VIEW_v1 (mig_233).
--           Adds: release manifest id, 8 semantic-view row counts, 7 quarantine
--           counts, latest col-registry batch_id, governance-gap count
--           (verified canonical_* main objects missing table comment).
-- run_id / batch: mig_238_publication_qc_status_v15plus
-- Source: ChatGPT cleanup audit 2026-05-01 (verified live by Cowork);
--         see qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-05-01_v16.md §A claim 5 view-1.
-- Target DB: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT (Cowork orchestrator); new VIEW only — no base-table mutation.
--
-- Rationale:
--   manuscript_workspace.qc_audit_dashboard_VIEW_v1 (mig_233) is the 5-gate health
--   wrapper. ChatGPT recommended a publication-tier extension that adds release
--   manifest, semantic-view row counts, quarantine counts, latest batch IDs, and a
--   governance-gap metric. This view sits in semantic_publication so any analyst
--   reading from semantic_publication.* can probe DB health with one SELECT.
--
-- Pre-snapshot: N/A (new VIEW; nothing to snapshot).
-- Post-apply expectation: 1 row; gate1 = 211 (210 baseline + this view's self-reg);
-- verified_main_objects_missing_comment = 0 (after mig_237 lands).

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Idempotent registry prep (re-run safe)
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_publication_qc_status_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_publication_qc_status_VIEW_v1';

-- =============================================================================
-- §A CREATE VIEW — superset of qc_audit_dashboard_VIEW_v1
-- =============================================================================
CREATE OR REPLACE VIEW semantic_publication.vw_publication_qc_status_VIEW_v1 AS
SELECT
  -- 5-gate (from mig_233 dashboard)
  d.gate1_verified_tables,
  d.gate1_distinct_objects,
  d.gate2_missing_signoff,
  d.gate3_count_mismatch,
  d.gate4_verified_cols_missing_metadata,
  d.gate5_clinical_date_violations,

  -- Cohort parity
  d.cpm_pts,
  d.us_gland_v2_pts,
  d.us_ln_v2_pts,
  d.cohort_parity_ok,

  -- Release manifest
  (SELECT release_id FROM semantic_publication.release_manifest_v1 LIMIT 1) AS release_id,

  -- Semantic safe-view row counts (8 views)
  (SELECT COUNT(*) FROM semantic_publication.vw_patient_master_safe_VIEW_v1)        AS sem_patient_master_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_cohort_membership_safe_VIEW_v1)     AS sem_cohort_membership_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1)  AS sem_path_malignant_tumor_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_recurrence_safe_VIEW_v1)            AS sem_recurrence_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_fna_safe_VIEW_v1)                   AS sem_fna_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_us_nodule_safe_VIEW_v1)             AS sem_us_nodule_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_molecular_safe_VIEW_v1)             AS sem_molecular_rows,
  (SELECT COUNT(*) FROM semantic_publication.vw_labs_long_safe_VIEW_v1)             AS sem_labs_long_rows,

  -- Quarantine / limitation counts (verified-live by Cowork 2026-05-01)
  (SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 WHERE is_borderline_or_benign_with_staging = TRUE) AS path_borderline_count,
  (SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1   WHERE is_implausible_date_quarantine = TRUE)        AS recurrence_implausible_date_count,
  (SELECT COUNT(*) FROM main.canonical_us_nodule_v2             WHERE is_size_outlier_quarantine = TRUE)            AS us_nodule_size_outlier_count,
  (SELECT COUNT(*) FROM main.canonical_us_nodule_v2             WHERE multi_nodule_attribution_unresolved = TRUE)    AS us_nodule_multi_attr_unresolved_count,
  (SELECT COUNT(*) FROM main.canonical_us_nodule_v2             WHERE nlp_backfill_pending = TRUE)                   AS us_nodule_nlp_backfill_pending_count,
  (SELECT COUNT(*) FROM main.canonical_us_lymph_node_v2         WHERE nlp_backfill_pending = TRUE)                   AS us_ln_nlp_backfill_pending_count,
  (SELECT COUNT(*) FROM main.canonical_us_thyroid_gland_v2      WHERE nlp_backfill_pending = TRUE)                   AS us_gland_nlp_backfill_pending_count,

  -- Latest registry batch_id (most recent verified col-registry batch)
  (SELECT batch_id
     FROM main.canonical_column_verification_registry_v1
     WHERE batch_id IS NOT NULL
     ORDER BY verified_ts DESC NULLS LAST
     LIMIT 1) AS latest_col_registry_batch_id,

  -- Governance-gap metric: verified canonical_* main objects with NULL comment
  -- (after mig_237 lands, this should be 0)
  (SELECT COUNT(*)
     FROM duckdb_tables() t
     WHERE t.schema_name = 'main'
       AND t.table_name LIKE 'canonical_%'
       AND t.comment IS NULL
       AND t.table_name IN (
         SELECT DISTINCT table_name FROM main.canonical_column_verification_registry_v1
       )
  ) AS verified_main_objects_missing_comment,

  -- Carry-forward metadata from mig_233 dashboard
  d.most_recent_signoff_ts,
  d.most_recent_signoff_migration,

  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS publication_qc_status_built_at
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1 d;

-- =============================================================================
-- §B Post-create spot-check (run as SELECT after apply)
-- =============================================================================
-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- Expected: 1 row, 31 cols; gate1 = 211; verified_main_objects_missing_comment = 0
-- (provided mig_237 has been applied); release_id = 'pub_v1_0_20260430'.

-- =============================================================================
-- §C Register VIEW in signoff registry (Gate 1 +1 -> 211)
-- =============================================================================
INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES (
  'semantic_publication',
  'vw_publication_qc_status_VIEW_v1',
  31,
  31,
  0,
  0,
  0,
  'verified',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'qc_framework_v1/migrations/238_publication_qc_status_VIEW_v1_20260501.sql',
  'tier2_canonical_view',
  'mig_238 publication-tier QC status superset of mig_233 dashboard. Adds release_id, 8 semantic-view row counts, 7 quarantine counts, latest col-registry batch_id, governance-gap metric. Single-row refreshable view; analysts probe DB health with SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1. Applied Cowork-direct 2026-05-01.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
);

-- =============================================================================
-- §D Register columns in col registry (31 cols)
-- =============================================================================
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
VALUES
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate1_verified_tables',                 'INTEGER',    1, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate1 of v2 audit; carried from dashboard view',                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate1_distinct_objects',                'INTEGER',    2, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate1 distinct (catches dup signoff rows); carried',                                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate2_missing_signoff',                 'INTEGER',    3, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate2 of v2 audit; carried',                                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate3_count_mismatch',                  'INTEGER',    4, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate3 of v2 audit; carried',                                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate4_verified_cols_missing_metadata',  'INTEGER',    5, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate4 of v2 audit; carried',                                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'gate5_clinical_date_violations',        'INTEGER',    6, 'metric',   'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'gate5 of v2 audit; carried',                                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'cpm_pts',                               'INTEGER',    7, 'metric',   'canonical_patient_master',                       'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'cohort parity: distinct research_id in CPM',                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_gland_v2_pts',                       'INTEGER',    8, 'metric',   'canonical_us_thyroid_gland_patient_rollup_v2',   'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'cohort parity: distinct research_id in US gland rollup',                                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_ln_v2_pts',                          'INTEGER',    9, 'metric',   'canonical_us_lymph_node_patient_rollup_v2',      'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'cohort parity: distinct research_id in US LN rollup',                                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'cohort_parity_ok',                      'BOOLEAN',   10, 'derived',  'mig_233_computation',                            'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'TRUE when cpm_pts=us_gland_v2_pts=us_ln_v2_pts=10871; carried',                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'release_id',                            'VARCHAR',   11, 'metadata', 'semantic_publication.release_manifest_v1',       'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_release_manifest_v1',           'mig_238_publication_qc_status_v15plus', 'pub_v1_0_20260430 currently; freezes per release',                                          CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_patient_master_rows',               'BIGINT',    12, 'metric',   'semantic_publication.vw_patient_master_safe_VIEW_v1',       'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 10,871',                                                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_cohort_membership_rows',            'BIGINT',    13, 'metric',   'semantic_publication.vw_cohort_membership_safe_VIEW_v1',    'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 10,871',                                                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_path_malignant_tumor_rows',         'BIGINT',    14, 'metric',   'semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 5,944 (dedup view passthrough)',                                                   CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_recurrence_rows',                   'BIGINT',    15, 'metric',   'semantic_publication.vw_recurrence_safe_VIEW_v1',           'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 10,739 (10,871 - 132 quarantined)',                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_fna_rows',                          'BIGINT',    16, 'metric',   'semantic_publication.vw_fna_safe_VIEW_v1',                  'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 8,050',                                                                             CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_us_nodule_rows',                    'BIGINT',    17, 'metric',   'semantic_publication.vw_us_nodule_safe_VIEW_v1',            'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 29,504',                                                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_molecular_rows',                    'BIGINT',    18, 'metric',   'semantic_publication.vw_molecular_safe_VIEW_v1',            'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 1,384',                                                                             CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'sem_labs_long_rows',                    'BIGINT',    19, 'metric',   'semantic_publication.vw_labs_long_safe_VIEW_v1',            'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_star',                    'mig_238_publication_qc_status_v15plus', 'expected 44,124',                                                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'path_borderline_count',                 'BIGINT',    20, 'metric',   'main.canonical_path_malignant_events_v1',                   'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_is_borderline_or_benign_with_staging', 'mig_238_publication_qc_status_v15plus', 'expected 27 (FTUMP/follicular adenoma w/ N1*/M1 staging quarantine)',         CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'recurrence_implausible_date_count',     'BIGINT',    21, 'metric',   'main.canonical_recurrence_resolved_v1',                     'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_is_implausible_date_quarantine',       'mig_238_publication_qc_status_v15plus', 'expected 132',                                                                CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_nodule_size_outlier_count',          'BIGINT',    22, 'metric',   'main.canonical_us_nodule_v2',                               'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_is_size_outlier_quarantine',           'mig_238_publication_qc_status_v15plus', 'expected 15',                                                                 CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_nodule_multi_attr_unresolved_count', 'BIGINT',    23, 'metric',   'main.canonical_us_nodule_v2',                               'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_multi_nodule_attribution_unresolved',  'mig_238_publication_qc_status_v15plus', 'expected 10,570 (large; documented limitation)',                              CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_nodule_nlp_backfill_pending_count',  'BIGINT',    24, 'metric',   'main.canonical_us_nodule_v2',                               'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_nlp_backfill_pending',                 'mig_238_publication_qc_status_v15plus', 'expected 2,061',                                                              CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_ln_nlp_backfill_pending_count',      'BIGINT',    25, 'metric',   'main.canonical_us_lymph_node_v2',                           'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_nlp_backfill_pending',                 'mig_238_publication_qc_status_v15plus', 'expected 6,793',                                                              CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'us_gland_nlp_backfill_pending_count',   'BIGINT',    26, 'metric',   'main.canonical_us_thyroid_gland_v2',                        'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_filter_nlp_backfill_pending',                 'mig_238_publication_qc_status_v15plus', 'expected 13,578',                                                             CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'latest_col_registry_batch_id',          'VARCHAR',   27, 'metadata', 'main.canonical_column_verification_registry_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_max_verified_ts_batch_id',     'mig_238_publication_qc_status_v15plus', 'most recent batch_id by verified_ts',                                                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'verified_main_objects_missing_comment', 'BIGINT',    28, 'metric',   'duckdb_tables() + col registry',                  'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'subselect_count_null_comment_join_registry', 'mig_238_publication_qc_status_v15plus', 'expected 0 after mig_237 lands; governance gap detector',                                  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'most_recent_signoff_ts',                'TIMESTAMP', 29, 'metadata', 'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'freshness indicator',                                                                       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'most_recent_signoff_migration',         'VARCHAR',   30, 'metadata', 'manuscript_workspace.qc_audit_dashboard_VIEW_v1', 'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_pass_through_from_mig233_dashboard', 'mig_238_publication_qc_status_v15plus', 'which mig last touched signoff registry',                                                   CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('semantic_publication', 'vw_publication_qc_status_VIEW_v1', 'publication_qc_status_built_at',        'TIMESTAMP', 31, 'metadata', 'mig_238_literal',                                  'verified', 'mig_238', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'cast_current_timestamp',                  'mig_238_publication_qc_status_v15plus', 'wall-clock of SELECT execution',                                                            CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- =============================================================================
-- §E Acceptance assertions (run after apply)
-- =============================================================================
-- ASSERT: view returns exactly 1 row
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL: expected 1 row, got ' || COUNT(*)::VARCHAR END AS assert_single_row
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT: gate1 = 211 (210 baseline + this view's self-reg)
SELECT CASE WHEN gate1_verified_tables = 211 THEN 'PASS'
            ELSE 'FAIL: gate1=' || gate1_verified_tables::VARCHAR || ' expected 211' END AS assert_gate1_211
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT: gates 2-5 = 0
SELECT CASE WHEN gate2_missing_signoff = 0
             AND gate3_count_mismatch = 0
             AND gate4_verified_cols_missing_metadata = 0
             AND gate5_clinical_date_violations = 0
            THEN 'PASS'
            ELSE 'FAIL: g2=' || gate2_missing_signoff::VARCHAR
                || ' g3=' || gate3_count_mismatch::VARCHAR
                || ' g4=' || gate4_verified_cols_missing_metadata::VARCHAR
                || ' g5=' || gate5_clinical_date_violations::VARCHAR
            END AS assert_gates_clean
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT: cohort parity TRUE
SELECT CASE WHEN cohort_parity_ok = TRUE THEN 'PASS' ELSE 'FAIL' END AS assert_parity
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT: governance gap = 0 (assumes mig_237 lands first)
SELECT CASE WHEN verified_main_objects_missing_comment = 0 THEN 'PASS'
            ELSE 'FAIL: ' || verified_main_objects_missing_comment::VARCHAR || ' canonical_* main missing comment' END AS assert_governance_gap_zero
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- ASSERT: release_id matches frozen pub
SELECT CASE WHEN release_id = 'pub_v1_0_20260430' THEN 'PASS'
            ELSE 'FAIL: release_id=' || COALESCE(release_id,'NULL') END AS assert_release_id
FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
