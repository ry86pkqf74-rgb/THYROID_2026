-- ============================================================================
-- Migration 44 — FNA04: strict duplicate-signature FNA rows (dedup view)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      FNA04 — rows sharing
--                (research_id, fna_date_resolved, specimen_location,
--                 laterality, bethesda_final_num)
--                with identical bethesda_original_text (not re-reads with a
--                different interpretation).
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   12 signature groups / 28 rows → 16 excess (registry listed 2 — stale).
--   All excess rows verified as exact text duplicates of their group keeper
--   (bethesda_original_text identical within group).
--
-- Tie-break rule (deterministic, stable across reruns):
--   1. Prefer non-NULL fna_date_resolved (already the group key, so all N/A)
--   2. Prefer highest bethesda_final_num within group (malignancy > benign)
--   3. Prefer lowest fna_event_id (lexicographic) — deterministic anchor
--
-- Output:
--   manuscript_workspace.canonical_fna_events_v1_dedup (VIEW)
--     (same columns as source + fna_row_rank + fna04_duplicate_flag)
--   fna04_duplicate_flag=TRUE for all non-rank-1 rows in a duplicate group.
--   Dropped rows emitted to queue under FNA04.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_fna_events_v1_dedup AS
SELECT
  e.*,
  ROW_NUMBER() OVER (
    PARTITION BY research_id, fna_date_resolved, specimen_location, laterality, bethesda_final_num
    ORDER BY bethesda_final_num DESC NULLS LAST, fna_event_id ASC
  ) AS fna_row_rank,
  (COUNT(*) OVER (
    PARTITION BY research_id, fna_date_resolved, specimen_location, laterality, bethesda_final_num
  ) > 1
   AND fna_date_resolved IS NOT NULL
   AND ROW_NUMBER() OVER (
     PARTITION BY research_id, fna_date_resolved, specimen_location, laterality, bethesda_final_num
     ORDER BY bethesda_final_num DESC NULLS LAST, fna_event_id ASC
   ) > 1
  ) AS fna04_duplicate_flag
FROM main.canonical_fna_events_v1 e;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA04';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'FNA04',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_fna_events_v1',
  CAST(fna_event_id AS VARCHAR),
  TO_JSON(struct_pack(
    fna_date_resolved := fna_date_resolved,
    specimen_location := specimen_location,
    laterality := laterality,
    bethesda_final_num := bethesda_final_num,
    fna_row_rank := fna_row_rank
  )),
  'FNA04 strict-signature duplicate — keeper = rank-1 by (bethesda_final_num DESC, fna_event_id ASC)',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_fna_events_v1_dedup
WHERE fna04_duplicate_flag;

COMMENT ON TABLE main.canonical_fna_events_v1 IS
'FNA event table (8,119 rows). Clean views: _date_clean (mig 42), _dts_clean (mig 43), _dedup (mig 44). Dedup view surfaces fna_row_rank + fna04_duplicate_flag over (rid, date, specimen_location, laterality, bethesda_final_num); tie-break = bethesda_final_num DESC, fna_event_id ASC. 16 dup-rows queued under FNA04. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_43';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_fna_events_v1','table',
   'manuscript_workspace.canonical_fna_events_v1_dedup',
   'FNA04','prompt_43','pointer_only',DATE '2026-04-24',
   'FNA04: 12 duplicate-signature groups (28 rows / 16 excess) where bethesda_original_text is identical. Dedup view keeps rank-1 per group under deterministic tie-break; excess rows queued.',
   NULL,
   'Tie-break: bethesda_final_num DESC NULLS LAST → fna_event_id ASC. Downstream FNA event-level queries should use _dedup WHERE fna_row_rank=1. Rollup rebuild (FNA05, mig 45) will read from _dedup.');
