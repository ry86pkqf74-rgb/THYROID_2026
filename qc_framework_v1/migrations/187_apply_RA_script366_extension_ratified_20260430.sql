-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY
-- mig_187 R-A apply — Logan-ratified Script 366 canonical_us_exam_master_VIEW_v2 extension
-- Batch: mig_187_apply_RA_script366_extension_ratified_20260430
-- Target DB (Path-C executor): thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Prerequisites:
--   1) Unified diff applied: scripts/366_canonical_us_exam_master_v2_patch_mig187_RA.diff
--   2) Redeploy VIEW: `.venv/bin/python scripts/366_canonical_us_exam_master_v2.py --commit`
--      (MotherDuck token via motherduck.local.toml — see docs / motherduck-credentials skill.)
--   3) Replay mig_171b Sections B,C,D after §C (exam-master must already include LN-NLP extension).
--
-- Path-C TRANSACTION BOUNDARIES:
--   1) Execute §A+§B (snapshots) only first.
--   2) Run Script 366 `--commit`; then rerun mig_171b Sections **B,C,D** from 171b file (verbatim DDL).
--   3) **Uncomment** the POST-MIG replay block §G§F§E below — do NOT run §G§F§E before Step 2 finishes.
--
-- Default check-in ships §G§F§E COMMENTED OUT to prevent accidental premature provenance/registry writes.

USE thyroid_canonical_publication_v1_0;

-- -----------------------------------------------------------------------------
-- §0 Pre-flight INVARIANT probes (Cowork verifies before mutating DDL)
-- -----------------------------------------------------------------------------
-- CPM invariant.
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids
-- FROM main.canonical_patient_master;
-- Expect: 10871 / 10871
--
-- Current exam-master VIEW row count (baseline for mig_187 scoping probes).
-- SELECT COUNT(*) AS exam_master_rows FROM main.canonical_us_exam_master_VIEW_v2;
-- Expect: ~11759 (live MotherDuck; confirm before apply)
--
-- Expected post-script-366-deploy row count ~11759 + 121 distinct LN-NLP-only pairs =~ 11880,
-- modulo any overlap if live state drifted vs scoping markdown.
--
-- Fallback events before replay (sanity anchor).
-- SELECT COUNT(*) FILTER (WHERE exam_id_source = 'fallback_ln_only_exam_id')
-- FROM main.canonical_us_lymph_node_events_v2;
-- Expect: ~159 events over ~121 pairs (see mig_187 scoping)

-- -----------------------------------------------------------------------------
-- §A Pre-snapshot — canonical_us_exam_master_VIEW_v2 (pre exam-master extension deploy)
-- -----------------------------------------------------------------------------
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_exam_master_VIEW_v2_pre_mig187_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig187_snapshot_ts
FROM main.canonical_us_exam_master_VIEW_v2;

-- -----------------------------------------------------------------------------
-- §B Pre-snapshot — canonical_us_lymph_node_events_v2 (pre mig_171b replay)
-- -----------------------------------------------------------------------------
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_lymph_node_events_v2_pre_mig187_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig187_snapshot_ts
FROM main.canonical_us_lymph_node_events_v2;

-- -----------------------------------------------------------------------------
-- §C Placeholder — Script 366 redeploy (executor runs OUTSIDE DuckDB SQL)
-- -----------------------------------------------------------------------------
--   cd <repo-root> && .venv/bin/python scripts/366_canonical_us_exam_master_v2.py --commit
--
-- VERIFY post-deploy VIEW selects new column + expected row uplift:
--   SELECT exam_id_source, COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1 ORDER BY 1 NULLS LAST;
--   Expect ~121 rows with exam_id_source = 'ln_nlp_only'; NULL for structured-shell spine rows.

-- -----------------------------------------------------------------------------
-- §D mig_171b Sections B, C, D replay (executor runs from 171b file — single SSOT)
-- -----------------------------------------------------------------------------
-- SOURCE OF TRUTH (verbatim DDL): qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql
-- Replay IN ORDER **after §C succeeds**:
--
--   * Section **B**  (~L95-L320): CREATE OR REPLACE TABLE main.canonical_us_lymph_node_events_v2 AS …
--   * Section **C**  (~L326-L453): CREATE OR REPLACE TABLE main.canonical_us_lymph_node_patient_rollup_v2 …
--   * Section **D**  (~L459-L571): CREATE OR REPLACE TABLE main.val_mig171b_canonical_us_ln_build_v1 …
--
-- `exam_master_by_rid_date` in Section B joins canonical_us_exam_master_VIEW_v2; once Script 366
-- emits ln_nlp_only rows matching mig_171b deterministic md5, fallbacks reconcile to exam_master_reused.

/*
-- -----------------------------------------------------------------------------
-- §G manuscript_workspace.cpm_reconciliation_provenance_v1 insert (audit trail)
-- -----------------------------------------------------------------------------
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared,
   held_for_adjudication)
SELECT
  'mig187_apply_RA_script366_extension_ratified_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'script366_exam_master_view_extension_mig171b_events_rollup_validation_replay_registry_closure',
  'none',
  'none',
  'none',
  'CF-mig171b-EXAM-MASTER-REBUILD (closed via mig_187 R-A)'
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1
  WHERE run_id = 'mig187_apply_RA_script366_extension_ratified_20260430'
);

-- -----------------------------------------------------------------------------
-- §F G9 validation probe — expects PASS after §D succeeds (fallback count 0 ideal)
-- -----------------------------------------------------------------------------
SELECT status,
       observed_value
FROM main.val_mig171b_canonical_us_ln_build_v1
WHERE check_id = 'G9_fallback_exam_ids_pending_rebuild';

-- -----------------------------------------------------------------------------
-- §E Registry appendix — CLOSE CF-mig171b-EXAM-MASTER-REBUILD (77-column cohort; run AFTER G9 PASS)
-- -----------------------------------------------------------------------------
-- VERIFY: SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
-- WHERE COALESCE(notes,'') LIKE '%CF-mig171b-EXAM-MASTER-REBUILD%'
--   AND COALESCE(notes,'') NOT LIKE '%CF-mig171b-EXAM-MASTER-REBUILD CLOSED (mig_187 R-A)%';  expect 77
UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
    WHEN COALESCE(notes, '') LIKE '%CF-mig171b-EXAM-MASTER-REBUILD CLOSED (mig_187 R-A)%'
      THEN notes
    ELSE COALESCE(notes, '')
      || ' | mig_187 R-A: Script 366 universe extended; LN-NLP exam dates seeded into VIEW with deterministic md5; G9 -> PASS; CF-mig171b-EXAM-MASTER-REBUILD CLOSED (mig_187 R-A)'
END
WHERE COALESCE(notes, '') LIKE '%CF-mig171b-EXAM-MASTER-REBUILD%';

*/

-- -----------------------------------------------------------------------------
-- §H Post-state probes (Cowork records evidence CSV / closeout appendix)
-- -----------------------------------------------------------------------------
-- H1 VIEW row counts
-- SELECT COUNT(*) AS n_exam_master_rows FROM main.canonical_us_exam_master_VIEW_v2;
--
-- H2 exam_id_source distribution on exam-master
-- SELECT exam_id_source, COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1 ORDER BY 1 NULLS LAST;
--
-- H3 Zero fallbacks remaining on LN events table
-- SELECT COUNT(*) FILTER (WHERE exam_id_source = 'fallback_ln_only_exam_id') AS n_fallback
-- FROM main.canonical_us_lymph_node_events_v2;
--
-- H4 All mig_171b gates
-- SELECT check_id, status, observed_value
-- FROM main.val_mig171b_canonical_us_ln_build_v1
-- ORDER BY check_id;
