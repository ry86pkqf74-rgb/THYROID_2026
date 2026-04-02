-- 110_operative_notes_merge_clinical_notes_long.sql
-- Merge raw.operative_notes_full_history_v2 into clinical_notes_long without losing rows.
--
-- Prereqs:
--   1. Run: .venv/bin/python scripts/110_operative_notes_full_history_scan.py --md --publish-md ...
--   2. Rebuild local parquet: scripts/build_clinical_notes_long.py (uses extended op_note date scan)
--   3. scripts/09b_local DuckDB_upload_notes_entities.py --confirm
--
-- Root issue: note_date was often NULL for op_note because extract_note_date() scanned only
-- 500 chars; Lakehouse diagnostics using TRY_CAST(note_date AS DATE) < 2019 undercounted.
-- v2 table adds synoptic_surg_date fallback + 50k-char header scan.
--
-- Idempotent pattern: anti-join on note_row_id, then optional UPDATE of existing op_note rows
-- when v2 has a newer resolved_layer_version (not shown — INSERT-only is safest).

-- ---------------------------------------------------------------------------
-- A) Schema + validation (run manually on local DuckDB)
-- ---------------------------------------------------------------------------
-- CREATE SCHEMA IF NOT EXISTS raw;
-- SELECT COUNT(*) AS v2_rows FROM raw.operative_notes_full_history_v2;
-- SELECT note_date_source, COUNT(*) FROM raw.operative_notes_full_history_v2 GROUP BY 1 ORDER BY 2 DESC;

-- ---------------------------------------------------------------------------
-- B) INSERT new operative rows not already in clinical_notes_long
--    Map v2 columns → clinical_notes_long schema (drop v2-only audit cols).
-- ---------------------------------------------------------------------------

INSERT INTO clinical_notes_long (
    note_row_id,
    research_id,
    note_type,
    note_index,
    note_date,
    note_text,
    source_sheet,
    source_column,
    char_count
)
SELECT
    v.note_row_id,
    v.research_id,
    v.note_type,
    v.note_index,
    v.note_date,
    v.note_text,
    v.source_sheet,
    v.source_column,
    v.char_count
FROM raw.operative_notes_full_history_v2 v
WHERE NOT EXISTS (
    SELECT 1
    FROM clinical_notes_long c
    WHERE c.note_row_id = v.note_row_id
);

-- ---------------------------------------------------------------------------
-- C) Refresh rows that already exist but have NULL note_date in canonical table
--    (Optional — review impact first; backup table recommended.)
-- ---------------------------------------------------------------------------
/*
CREATE TABLE clinical_notes_long_backup_20260327 AS SELECT * FROM clinical_notes_long;

UPDATE clinical_notes_long AS c
SET
    note_date = v.note_date,
    note_text = COALESCE(NULLIF(TRIM(c.note_text), ''), v.note_text)
FROM raw.operative_notes_full_history_v2 v
WHERE c.note_row_id = v.note_row_id
  AND c.note_type = 'op_note'
  AND (c.note_date IS NULL OR TRIM(CAST(c.note_date AS VARCHAR)) = '')
  AND v.note_date IS NOT NULL;
*/

-- ---------------------------------------------------------------------------
-- D) Downstream (run existing pipelines — do not hand-edit analytic MVs)
-- ---------------------------------------------------------------------------
-- 1. llm_extraction/run_extraction.py --target procedures (or full domain set)
-- 2. scripts/46_provenance_audit.py --md
-- 3. scripts/48_* / 51b / manuscript rebuild chain per docs/analysis_resolved_layer.md
-- 4. scripts/90_manuscript_freeze_rebuild.py --md  (if manuscript tables must move)
