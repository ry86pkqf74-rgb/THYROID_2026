-- mig_187 canonical_us_exam_master / exam-master VIEW rebuild (SKELETON — NOT APPLY)
-- Batch_id (proposed): mig_187_canonical_us_exam_master_rebuild_20260430
-- Database: thyroid_canonical_publication_v1_0
-- Carry-forward: CF-mig171b-EXAM-MASTER-REBUILD (159 LN events still use fallback us_exam_id)
--
-- AUTHOR: Logan Glosser <logan.glosser@gmail.com>
-- POSTURE: PLACEHOLDER / RATIFICATION SKELETON ONLY.
--
-- Prerequisites (closed): mig_171b at 9301b58 — G9 WARN observed_value=159 fallback rows.
--
-- LOGAN MUST RATIFY RULE (R-A / R-B / R-C) BEFORE ANY STATEMENT BELOW IS UNCOMMENTED AND RUN.
--
-- Source-of-truth for VIEW logic: scripts/366_canonical_us_exam_master_v2.py
--   Universe = UNION(nodule_agg, gland_agg, ln_agg) over:
--     canonical_us_nodule_v2, canonical_us_thyroid_gland_v2, canonical_us_lymph_node_v2 (legacy shell).
--   LN-only NLP dates from clinical_note_ln_extracted_v1 appear in mig_171b events but are NOT in
--   that UNION unless the legacy shell carries the same (research_id, exam_date).


USE thyroid_canonical_publication_v1_0;


-- =============================================================================
-- §A — Pre-snapshot (archive naming pattern; execute only after ratification)
-- =============================================================================

-- §A1 — Snapshot current exam-master VIEW dependents / consumers if needed by governanace:
-- CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_exam_master_VIEW_v2_pre_mig187_YYYYMMDD AS
-- SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig187_snapshot_ts
-- FROM thyroid_canonical_publication_v1_0.main.canonical_us_exam_master_VIEW_v2;

-- §A2 — Snapshot LN events v2 prior to mig_171b §B re-run after exam-master fix:
-- CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_lymph_node_events_v2_pre_mig187_post_exam_fix_YYYYMMDD AS
-- SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_rerun_snapshot_ts
-- FROM thyroid_canonical_publication_v1_0.main.canonical_us_lymph_node_events_v2;


-- =============================================================================
-- §B — Rebuild variants (PICK ONE RULE — ALL BLOCKED UNTIL RATIFIED)
-- =============================================================================

--------------------------------------------------------------------------------
-- R-A — Extend base universe so exam_master exposes every (rid, exam_date)
--       that mig_171b needs, then redeploy VIEW via Script 366.
--
-- Typical implementation paths (choose ONE; LOGAN MUST RATIFY RULE BEFORE EXECUTION):
--   (R-A.i) Extend Script 366 `ln_agg` / `exams` UNION to incorporate DISTINCT
--           (research_id, exam_date) from canonical_us_lymph_node_events_v2 WHERE
--           those pairs are absent from today's UNION AND carry us_exam_id =
--           md5('US_EXAM_V2|' || research_id || '|' || CAST(exam_date AS VARCHAR))
--           so 171b deterministic fallback matches rebuilt exam_master row exactly.
--
--   (R-A.ii) Upsert canonical_us_lymph_node_v2 synthetic rows OR a new staging
--            table wired into 366 instead of patching VIEW text ad hoc — only if
--            clinical governance prefers materialized rows over VIEW-only UNION.
--------------------------------------------------------------------------------
-- -- LOGAN MUST RATIFY RULE R-A BEFORE EXECUTION
-- -- (Example pattern only — do not run until recipe is approved)
-- -- .venv/bin/python scripts/366_canonical_us_exam_master_v2.py --commit


--------------------------------------------------------------------------------
-- R-B — Supplemental patch TABLE + UNION in VIEW (exam dates not in structured US children)
--------------------------------------------------------------------------------
-- -- LOGAN MUST RATIFY RULE R-B BEFORE EXECUTION
--
-- Steps (illustrative):
--   CREATE TABLE IF NOT EXISTS main.canonical_us_exam_ln_only_supplement_v1 ( ... );
--   INSERT supplements for the 121 (research_id, exam_date) fallback pairs ...
--   Patch Script 366 to UNION supplemental grain into `exams` / `joined` logic.
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- R-C — Accept fallback IDs (no DDL change): document manuscript + relax G9
--------------------------------------------------------------------------------
-- -- LOGAN MUST RATIFY RULE R-C BEFORE EXECUTION
--
-- Requires:
--   * Update mig_171b validation G9 semantics to PASS with documented fallback count, OR retire G9 WARN.
--   * Register CF disposition in manuscript appendix (IDs are deterministic; join to VIEW still fails).
--------------------------------------------------------------------------------


-- =============================================================================
-- §C — Re-run mig_171b §B (events rebuild) AFTER §B rule is executed
-- =============================================================================

-- Execute the ratified Sections B+D from:
--   qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql
-- OR isolate Section B+D only via governed apply script once ratified archive snapshots exist.


-- =============================================================================
-- §D — Re-validate mig_171b val table — G9 expectation
-- =============================================================================

-- After R-A/R-B succeeds and §C completes, expect:
--   SELECT status, observed_value FROM main.val_mig171b_canonical_us_ln_build_v1
--   WHERE check_id = 'G9_fallback_exam_ids_pending_rebuild';
-- Desired: PASS, observed_value = '0'

-- Sanity probe — all reused exam_ids resolve:
-- SELECT COUNT(*) AS bad_rows
-- FROM main.canonical_us_lymph_node_events_v2 e
-- WHERE e.exam_id_source = 'exam_master_reused'
--   AND NOT EXISTS (
--     SELECT 1 FROM main.canonical_us_exam_master_VIEW_v2 em
--      WHERE CAST(em.research_id AS VARCHAR) = e.research_id
--        AND em.exam_date = e.exam_date
--        AND em.us_exam_id = e.us_exam_id
--   );


-- =============================================================================
-- End mig_187 skeleton
-- =============================================================================
