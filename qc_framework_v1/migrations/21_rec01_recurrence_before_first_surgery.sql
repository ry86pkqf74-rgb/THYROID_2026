-- ============================================================================
-- Migration 21 — REC01: flag recurrence dated before first surgery
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      REC01
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Source tables:
--   main.recurrence_event_clean_v1          (1,946 rows; 182 w/ recurrence_date)
--   main.canonical_path_malignant_events_v1 (6,689 rows; per-tumor-per-surgery)
--
-- Note: main.canonical_recurrence_v1 is a shell table (10,871 rows but all
-- date columns NULL) — not used. recurrence_event_clean_v1 holds the actual
-- dated events produced by Script 2xx notes-LLM pipeline.
--
-- Invariant: recurrence_date >= first_surgery_date (same patient).
-- Violations (before-first-surgery) at 7-day buffer = 19 events / 19 patients.
-- Without buffer = 20 events. Queue emits unbuffered count (20) for completeness;
-- the 7-day-buffered subset is available via the _buf_flag column.
--
-- Output:
--   manuscript_workspace.recurrence_event_clean_v1_first_surg_flag
--     — all recurrence_event_clean_v1 rows + first_surgery_date join +
--       recurrence_before_first_surgery_flag +
--       recurrence_before_first_surgery_7d_buf_flag
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.recurrence_event_clean_v1_first_surg_flag AS
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(surgery_date) AS first_surgery_date
    FROM main.canonical_path_malignant_events_v1
    GROUP BY research_id
)
SELECT
    r.*,
    f.first_surgery_date,
    (r.recurrence_date IS NOT NULL
       AND f.first_surgery_date IS NOT NULL
       AND r.recurrence_date < f.first_surgery_date)
        AS recurrence_before_first_surgery_flag,
    (r.recurrence_date IS NOT NULL
       AND f.first_surgery_date IS NOT NULL
       AND r.recurrence_date < f.first_surgery_date - INTERVAL 7 DAY)
        AS recurrence_before_first_surgery_7d_buf_flag
FROM main.recurrence_event_clean_v1 r
LEFT JOIN first_surg f ON r.research_id = f.research_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='REC01';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'REC01',
    -- r.research_id is VARCHAR on this table; coerce to INTEGER where parseable
    TRY_CAST(r.research_id AS INTEGER),
    'main.recurrence_event_clean_v1',
    CONCAT_WS('|',
        r.research_id,
        CAST(r.recurrence_date AS VARCHAR),
        COALESCE(r.recurrence_type, ''),
        CAST(r.event_rank AS VARCHAR)
    ),
    TO_JSON(struct_pack(
        recurrence_date := r.recurrence_date,
        first_surgery_date := f.first_surgery_date,
        days_before_first_surgery := DATE_DIFF('day', r.recurrence_date, f.first_surgery_date),
        recurrence_type := r.recurrence_type,
        structural := r.structural_recurrence_flag,
        biochemical := r.biochemical_recurrence_flag,
        source_table := r.source_table
    )),
    CONCAT('Recurrence dated ', CAST(DATE_DIFF('day', r.recurrence_date, f.first_surgery_date) AS VARCHAR),
           ' days before first surgery'),
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.recurrence_event_clean_v1 r
JOIN (SELECT CAST(research_id AS VARCHAR) AS research_id, MIN(surgery_date) AS first_surgery_date
      FROM main.canonical_path_malignant_events_v1 GROUP BY research_id) f
  ON r.research_id = f.research_id
WHERE r.recurrence_date IS NOT NULL
  AND r.recurrence_date < f.first_surgery_date;

COMMENT ON COLUMN main.recurrence_event_clean_v1.recurrence_date IS
'Structured recurrence timestamp. 20 events dated before patient''s first thyroid surgery (REC01). See manuscript_workspace.recurrence_event_clean_v1_first_surg_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_20';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.recurrence_event_clean_v1.recurrence_date','column',
   'manuscript_workspace.recurrence_event_clean_v1_first_surg_flag',
   'REC01','prompt_20','column_only',DATE '2026-04-23',
   '20 recurrence events dated before patient''s first thyroid surgery; 19 remain after 7-day date-mismatch buffer.',
   NULL,
   'Boolean flag + buffered-flag variant. 20 queue rows under REC01 require chart review to distinguish outside-institution recurrences vs data errors.');
