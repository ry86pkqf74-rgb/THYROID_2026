-- =============================================================================
-- 321 — tgdc_manual_addons_v1 + cohort build notes (THY-2)
-- =============================================================================
-- Database:   thyroid_canonical_publication_v1_0 (MotherDuck)
-- Date:       2026-05-06
-- DFL:        DFL-20260506-TGDCADDONS (Data Feedback Log, THYROID_MANUSCRIPT)
-- Issue:      THY-2
--
-- Purpose:
--   * Document DDL for pub_workspace.tgdc_manual_addons_v1 (13 PHI-safe rows).
--   * Table is normally loaded from repo CSV via
--     studies/tgdc_reconciliation/build_cohort.py, which uses an absolute path
--     to studies/tgdc_reconciliation/sources/tgdc_manual_addons_v1.csv.
--   * Optional one-shot load (run MotherDuck client from repo root so the
--     relative path resolves):
--       CREATE OR REPLACE TABLE pub_workspace.tgdc_manual_addons_v1 AS
--       SELECT
--         TRIM(CAST(research_id AS VARCHAR)) AS research_id,
--         TRIM(CAST(evidence_source AS VARCHAR)) AS evidence_source,
--         TRIM(CAST(evidence_summary AS VARCHAR)) AS evidence_summary,
--         CAST(
--           concat(added_at::DATE, ' 00:00:00') AS TIMESTAMP
--         ) AS added_at
--       FROM read_csv_auto(
--         'studies/tgdc_reconciliation/sources/tgdc_manual_addons_v1.csv',
--         header := true,
--         all_varchar := true
--       );
--
-- Related:
--   * TGDC_FINAL_RECONCILIATION_REPORT.md (2026-03-07) — 14-patient augmentation
--     narrative; current canonical rebuild yields 13 manual addons vs path-text
--     primary because one former manual patient now matches clinical preop text.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE SCHEMA IF NOT EXISTS pub_workspace;

CREATE TABLE IF NOT EXISTS pub_workspace.tgdc_manual_addons_v1 (
  research_id VARCHAR PRIMARY KEY,
  evidence_source VARCHAR NOT NULL,
  evidence_summary VARCHAR NOT NULL,
  added_at TIMESTAMP NOT NULL,
  loaded_from VARCHAR,
  loaded_at TIMESTAMP
);

COMMENT ON TABLE pub_workspace.tgdc_manual_addons_v1 IS
  'THY-2: PHI-safe manual TGDC cohort adds (research_id only). Source: studies/tgdc_reconciliation/sources/tgdc_manual_addons_v1.csv; loader: studies/tgdc_reconciliation/build_cohort.py.';
