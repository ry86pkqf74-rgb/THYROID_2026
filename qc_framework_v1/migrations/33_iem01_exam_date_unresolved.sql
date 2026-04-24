-- ============================================================================
-- Migration 33 — IEM01: imaging_exam_master_v1.exam_date NULL
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      IEM01 — 2,050 of 13,347 rows NULL exam_date (15.4%)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.imaging_exam_master_v1 has 3 upstream sources:
--   raw_us_tirads_excel_v1   : 6,025 rows / 0 null-date     (clean)
--   raw_us_tirads_scored_v1  : 2,506 rows / 0 null-date     (clean)
--   raw_imaging_12_slots_v1  : 4,816 rows / 2,050 null-date (DIRTY — archived)
-- All null-date rows come from the third source.
--
-- The source table `main.raw_imaging_12_slots_v1` has been ARCHIVED to
-- `"Thyroid 2026 UPdated".archive_legacy.main__raw_imaging_12_slots_v1_20260417T073708Z`
-- (column `exam_date_norm` carries the original date). Per Logan's "no
-- cross-DB canonical sourcing" rule, we do NOT reach into that archive from
-- the live publication DB. Recovery path is documented but not executed here.
--
-- Fix (within-pub-DB scope):
--   1. View `imaging_exam_master_v1_datecheck` surfaces
--      `exam_date_unresolved_flag` (TRUE iff exam_date IS NULL).
--   2. Queue all 2,050 rows under `IEM01` with exam_id/source context.
--   3. Downstream time-anchored views must filter
--      `exam_date_unresolved_flag=FALSE`.
--
-- Output:
--   manuscript_workspace.imaging_exam_master_v1_datecheck (VIEW)
--     + exam_date_unresolved_flag
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.imaging_exam_master_v1_datecheck AS
SELECT
  m.*,
  (m.exam_date IS NULL) AS exam_date_unresolved_flag
FROM main.imaging_exam_master_v1 m;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='IEM01';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'IEM01',
  research_id,
  'main.imaging_exam_master_v1',
  exam_id,
  TO_JSON(struct_pack(
    exam_id := exam_id,
    source := source,
    n_nodules := n_nodules,
    max_tirads := max_tirads,
    largest_nodule_cm := largest_nodule_cm
  )),
  'IEM01 exam_date NULL — source raw_imaging_12_slots_v1 archived; date recoverable via archive_legacy.exam_date_norm if needed',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.imaging_exam_master_v1_datecheck
WHERE exam_date_unresolved_flag;

COMMENT ON TABLE main.imaging_exam_master_v1 IS
'Imaging exam master (13,347 rows, 3 sources). 2,050 rows NULL exam_date — all from archived source raw_imaging_12_slots_v1. Use manuscript_workspace.imaging_exam_master_v1_datecheck and filter exam_date_unresolved_flag=FALSE for time-anchored analyses. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_32';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.imaging_exam_master_v1','table',
   'manuscript_workspace.imaging_exam_master_v1_datecheck',
   'IEM01','prompt_32','column_only',DATE '2026-04-23',
   '2,050 rows (15.4%) have NULL exam_date, all sourced from the archived raw_imaging_12_slots_v1. Live backfill not possible within pub DB per no-cross-DB rule. Recovery path: archive_legacy.main__raw_imaging_12_slots_v1_20260417T073708Z column exam_date_norm.',
   NULL,
   'exam_date_unresolved_flag surfaces the 2,050 rows. All queued under IEM01. Downstream time-anchored views must filter exam_date_unresolved_flag=FALSE.');
