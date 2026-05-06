-- =============================================================================
-- 320 — emr_demographics_v1 (pub_workspace) + signoff registry (THY-1)
-- =============================================================================
-- Database:   thyroid_canonical_publication_v1_0 (MotherDuck)
-- Date:       2026-05-06
-- DFL:        DFL-20260506-EMRDEMO (Data Feedback Log, base appJYOnUb7KrHKwpV)
-- Issue:      THY-1
--
-- Purpose:
--   * CREATE SCHEMA pub_workspace (if absent).
--   * Document DDL for pub_workspace.emr_demographics_v1 (typically materialized
--     via scripts/emr_demographics_v1_pipeline.py from PHI-scrubbed parquet).
--   * Register the table in main.canonical_table_signoff_registry_v1.
--
-- Row counts and parquet bytes are operator-specific; re-run pipeline before
-- applying if the table does not yet exist.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE SCHEMA IF NOT EXISTS pub_workspace;

-- Idempotent table shell (pipeline uses CREATE OR REPLACE TABLE AS SELECT from parquet).
CREATE TABLE IF NOT EXISTS pub_workspace.emr_demographics_v1 (
  research_id VARCHAR PRIMARY KEY,
  race        VARCHAR,
  ethnicity   VARCHAR,
  dob_year    INTEGER,
  sex         VARCHAR,
  source_table VARCHAR,
  extracted_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_emr_demographics_v1_research_id
  ON pub_workspace.emr_demographics_v1 (research_id);

-- Append-only registry row (skip if already registered).
INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
SELECT *
FROM (
  SELECT
    'pub_workspace'::VARCHAR AS schema_name,
    'emr_demographics_v1'::VARCHAR AS table_name,
    7 AS n_columns_total,
    0 AS n_verified,
    7 AS n_not_started,
    0 AS n_failed,
    0 AS n_na,
    'live'::VARCHAR AS table_status,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS signed_off_ts,
    'qc_framework_v1/migrations/320_emr_demographics_v1_20260506.sql'::VARCHAR AS signoff_migration,
    'tier2_reference'::VARCHAR AS priority_tier,
    'THY-1: PHI-safe EMR demographics keyed by research_id (dob_year only; no full DOB). Loaded from studies/tgdc_reconciliation/sources/emr_demographics_v1.parquet via scripts/emr_demographics_v1_pipeline.py. Replace rows from restricted EMR export when available.'::VARCHAR AS notes,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS registered_ts
) AS payload
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_table_signoff_registry_v1 z
  WHERE z.schema_name = 'pub_workspace' AND z.table_name = 'emr_demographics_v1'
);
