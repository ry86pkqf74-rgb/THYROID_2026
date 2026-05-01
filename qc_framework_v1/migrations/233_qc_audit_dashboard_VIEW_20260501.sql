-- mig_233 — qc_audit_dashboard snapshot view
-- run_id / batch: mig_233_audit_dashboard_v15
-- Source: cursor_prompts/PARALLEL_AGENT_BATCH_20260501_v15.md — §4 Prompt 4
--         qc_framework_v1/queries/cowork_verification_suite_20260430.md §1 (5-gate verbatim)
-- Target DB: thyroid_canonical_publication_v1_0
-- Naming: reference_view_naming_convention — `_VIEW` infix before `_v1`.
-- COWORK-DIRECT / Cline Sonnet 4.6 APPLY; new VIEW only — no base-table mutation.
--
-- Rationale:
--   The 5-gate v2 audit (`cowork_verification_suite_20260430.md §1`) is the SSOT for
--   publication lakehouse cleanliness. Currently any agent must copy-paste the 15+
--   line CTE query manually. This view wraps gates 1-5, cohort-parity, and most-recent
--   signoff metadata into a single refreshable one-row surface that any agent can probe
--   in one call:
--       SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
--
-- Gated on mig_232 commit landing (mig_232 narrow-ACR-missing view must be applied first
-- so gate1 count reflects the new row).
--
-- Pre-snapshot: N/A (new VIEW; nothing to snapshot).
-- Post-apply row-count expectation: 1 row (single-row dashboard).
-- Expected gate1 at build time: ≥ 211 (209 baseline + mig_232 +1 + this view's own self-reg +1).

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Idempotent registry prep (re-run safe)
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name  = 'qc_audit_dashboard_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name  = 'qc_audit_dashboard_VIEW_v1';

-- =============================================================================
-- §A CREATE VIEW — wraps 5-gate + cohort parity + most-recent-signoff metadata
-- =============================================================================
CREATE OR REPLACE VIEW manuscript_workspace.qc_audit_dashboard_VIEW_v1 AS
WITH audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
    ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
    ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime'),
    ('cpm_built_at'),('rollup_built_at'),('resolved_at'),('reclassified_at'),
    ('pre_mig186b_snapshot_ts')
  ) v(col_name)
),
verified_tables AS (
  SELECT schema_name, table_name
  FROM main.canonical_table_signoff_registry_v1
  WHERE table_status = 'verified'
    AND table_name LIKE 'canonical_%'
),
gates AS (
  SELECT
    -- gate1: total verified rows in signoff registry
    (SELECT COUNT(*)
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_status = 'verified'
    )::INTEGER AS gate1_verified_tables,

    -- gate1_distinct: distinct (schema_name, table_name) pairs — catches dup rows
    (SELECT COUNT(*)
     FROM (
       SELECT DISTINCT schema_name, table_name
       FROM main.canonical_table_signoff_registry_v1
       WHERE table_status = 'verified'
     ) d
    )::INTEGER AS gate1_distinct_objects,

    -- gate2: verified tables with NULL signoff_migration
    (SELECT COUNT(*)
     FROM main.canonical_table_signoff_registry_v1
     WHERE table_status = 'verified'
       AND signoff_migration IS NULL
    )::INTEGER AS gate2_missing_signoff,

    -- gate3: verified tables with column-count math mismatch
    (SELECT COUNT(*)
     FROM main.canonical_table_signoff_registry_v1 t
     WHERE t.table_status = 'verified'
       AND (
         t.n_verified + t.n_na <> t.n_columns_total
         OR t.n_not_started <> 0
         OR COALESCE(t.n_failed, 0) <> 0
       )
    )::INTEGER AS gate3_count_mismatch,

    -- gate4: verified cols missing one of: verified_by / batch_id / verification_method
    (SELECT COUNT(*)
     FROM main.canonical_column_verification_registry_v1 r
     JOIN main.canonical_table_signoff_registry_v1 t
          USING (schema_name, table_name)
     WHERE t.table_status = 'verified'
       AND r.verification_status = 'verified'
       AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)
    )::INTEGER AS gate4_verified_cols_missing_metadata,

    -- gate5: verified canonical tables with TIMESTAMP / date-like VARCHAR cols
    --        not in allowlist and not registered (§14 v2 scoped to canonical_*)
    (SELECT COUNT(*)
     FROM information_schema.columns c
     JOIN verified_tables v ON c.table_name = v.table_name
     LEFT JOIN main.canonical_column_verification_registry_v1 r
          ON r.schema_name = 'main'
         AND r.table_name  = c.table_name
         AND r.column_name = c.column_name
     WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
       AND c.table_schema  = 'main'
       AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
       AND NOT regexp_matches(c.column_name, '_built_at$')
       AND NOT regexp_matches(c.column_name, '_derived_at$')
       AND NOT regexp_matches(c.column_name, '_resolved_at$')
       AND NOT regexp_matches(c.column_name, '_confidence$')
       AND c.column_name NOT LIKE '%_status'
       AND c.column_name NOT LIKE '%_source'
       AND c.column_name NOT LIKE '%_keyword'
       AND c.column_name NOT LIKE '%_raw'
       AND COALESCE(r.verification_status, 'unknown') != 'na'
       AND (
         c.data_type IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
         OR (
           c.data_type = 'VARCHAR'
           AND (
             regexp_matches(c.column_name, '(^|_)dates?(_|$)')
             OR regexp_matches(c.column_name, '(^|_)dt(_|$)')
           )
         )
       )
    )::INTEGER AS gate5_clinical_date_violations
),
cohort AS (
  SELECT
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master)::INTEGER              AS cpm_pts,
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_thyroid_gland_patient_rollup_v2)::INTEGER AS us_gland_v2_pts,
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_lymph_node_patient_rollup_v2)::INTEGER    AS us_ln_v2_pts
),
recent_signoff AS (
  SELECT
    CAST(signed_off_ts AS TIMESTAMP) AS most_recent_signoff_ts,
    signoff_migration                AS most_recent_signoff_migration
  FROM main.canonical_table_signoff_registry_v1
  WHERE table_status = 'verified'
    AND signed_off_ts IS NOT NULL
  ORDER BY signed_off_ts DESC
  LIMIT 1
)
SELECT
  g.gate1_verified_tables,
  g.gate1_distinct_objects,
  g.gate2_missing_signoff,
  g.gate3_count_mismatch,
  g.gate4_verified_cols_missing_metadata,
  g.gate5_clinical_date_violations,
  c.cpm_pts,
  c.us_gland_v2_pts,
  c.us_ln_v2_pts,
  (c.cpm_pts = 10871 AND c.us_gland_v2_pts = 10871 AND c.us_ln_v2_pts = 10871) AS cohort_parity_ok,
  r.most_recent_signoff_ts,
  r.most_recent_signoff_migration,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dashboard_built_at
FROM gates g
CROSS JOIN cohort c
CROSS JOIN recent_signoff r;

-- =============================================================================
-- §B Post-create spot-check (embed; run as SELECT after apply)
-- =============================================================================
-- SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
-- Expected: 1 row; gate1 >= 211; gate2-5 = 0; cohort_parity_ok = TRUE.

-- =============================================================================
-- §C Register VIEW in signoff registry (Gate 1 +1)
-- =============================================================================
INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES (
  'manuscript_workspace',
  'qc_audit_dashboard_VIEW_v1',
  13,
  13,
  0,
  0,
  0,
  'verified',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'qc_framework_v1/migrations/233_qc_audit_dashboard_VIEW_20260501.sql',
  'tier2_canonical_view',
  'mig_233 single-row refresh view: wraps 5-gate v2 audit (cowork_verification_suite_20260430.md §1) + cohort parity + most-recent-signoff metadata. Any agent probes lakehouse health via SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1. Applied by Cline Sonnet 4.6 v15 batch.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
);

-- =============================================================================
-- §D Register columns in col registry
-- =============================================================================
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position, category,
   upstream_source, verification_status, verified_by, verified_ts,
   verification_method, batch_id, notes, registered_ts)
VALUES
  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate1_verified_tables',            'INTEGER',   1, 'metric',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT(*) WHERE table_status=verified; §1 gate1 from verification suite', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate1_distinct_objects',           'INTEGER',   2, 'metric',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT DISTINCT (schema_name,table_name) WHERE verified; detects dup registry rows', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate2_missing_signoff',            'INTEGER',   3, 'metric',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT verified tables with signoff_migration IS NULL; §1 gate2', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate3_count_mismatch',             'INTEGER',   4, 'metric',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT verified tables where n_verified+n_na<>n_columns_total or n_not_started<>0 or n_failed>0; §1 gate3', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate4_verified_cols_missing_metadata', 'INTEGER', 5, 'metric',
   'canonical_column_verification_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT verified cols missing verified_by/batch_id/verification_method; §1 gate4', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'gate5_clinical_date_violations',   'INTEGER',   6, 'metric',
   'information_schema.columns + canonical_column_verification_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'TIMESTAMP/date-like VARCHAR cols in verified canonicals not in allowlist; §1 gate5 v2 (scoped canonical_* + extended allowlist)', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'cpm_pts',                          'INTEGER',   7, 'metric',
   'canonical_patient_master', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT DISTINCT research_id FROM canonical_patient_master; §2 cohort parity', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'us_gland_v2_pts',                  'INTEGER',   8, 'metric',
   'canonical_us_thyroid_gland_patient_rollup_v2', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT DISTINCT research_id FROM canonical_us_thyroid_gland_patient_rollup_v2; §2 cohort parity', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'us_ln_v2_pts',                     'INTEGER',   9, 'metric',
   'canonical_us_lymph_node_patient_rollup_v2', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'COUNT DISTINCT research_id FROM canonical_us_lymph_node_patient_rollup_v2; §2 cohort parity', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'cohort_parity_ok',                 'BOOLEAN',  10, 'derived',
   'mig_233_computation', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'TRUE when cpm_pts=us_gland_v2_pts=us_ln_v2_pts=10871; hard invariant gate', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'most_recent_signoff_ts',           'TIMESTAMP', 11, 'metadata',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'MAX signed_off_ts WHERE table_status=verified; freshness indicator', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'most_recent_signoff_migration',    'VARCHAR',  12, 'metadata',
   'canonical_table_signoff_registry_v1', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'signoff_migration of row with max signed_off_ts; which mig last touched the signoff registry', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),

  ('manuscript_workspace', 'qc_audit_dashboard_VIEW_v1',
   'dashboard_built_at',               'TIMESTAMP', 13, 'metadata',
   'mig_233_literal', 'verified', 'cline_sonnet_4_6_mig_233',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'view_ddl_5gate_cte_cowork_verification_suite_20260430',
   'mig_233_audit_dashboard', 'CAST(CURRENT_TIMESTAMP AS TIMESTAMP) — wall-clock of the SELECT execution; always fresh', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));

-- =============================================================================
-- §E Provenance
-- =============================================================================
DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1
WHERE run_id = 'mig_233_audit_dashboard_v15';

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_233_audit_dashboard_v15',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'lane_mig233_create_view_register_signoff_col_registry',
  '0',
  '0',
  'col_registry_13_cols_qc_audit_dashboard_VIEW_v1',
  'none'
);

-- =============================================================================
-- §F Acceptance assertions (run after apply)
-- =============================================================================
-- ASSERT: view returns exactly 1 row
SELECT CASE WHEN COUNT(*)=1 THEN 'PASS' ELSE 'FAIL: expected 1 row, got ' || COUNT(*)::VARCHAR END AS assert_single_row
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;

-- ASSERT: gates 2-5 = 0 (cleanliness)
SELECT CASE
  WHEN gate2_missing_signoff=0
   AND gate3_count_mismatch=0
   AND gate4_verified_cols_missing_metadata=0
   AND gate5_clinical_date_violations=0
  THEN 'PASS'
  ELSE 'FAIL: gate2=' || gate2_missing_signoff::VARCHAR
        || ' gate3=' || gate3_count_mismatch::VARCHAR
        || ' gate4=' || gate4_verified_cols_missing_metadata::VARCHAR
        || ' gate5=' || gate5_clinical_date_violations::VARCHAR
  END AS assert_gates_2_5_zero
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;

-- ASSERT: cohort parity
SELECT CASE WHEN cohort_parity_ok=TRUE THEN 'PASS' ELSE 'FAIL: cohort parity broken' END AS assert_cohort_parity
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;

-- ASSERT: gate1 >= 211 (209 baseline + mig_232 + mig_233 self-reg)
SELECT CASE WHEN gate1_verified_tables >= 211 THEN 'PASS'
            ELSE 'FAIL: gate1=' || gate1_verified_tables::VARCHAR || ' expected >=211' END AS assert_gate1_floor
FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
