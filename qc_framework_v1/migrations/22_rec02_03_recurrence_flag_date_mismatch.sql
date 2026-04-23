-- ============================================================================
-- Migration 22 — REC02/REC03: recurrence flag/date mismatch
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     REC02 (flag TRUE, date NULL) — 1,764 pts
--                REC03 (date present, flag not TRUE) — 0 pts (guard only)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.manuscript_cohort_v1 cross-tab:
--   any_recurrence_flag=TRUE  & recurrence_date IS NOT NULL → 182 (both)
--   any_recurrence_flag=TRUE  & recurrence_date IS NULL     → 1,764 (REC02)
--   any_recurrence_flag=FALSE & recurrence_date IS NOT NULL → 0 (REC03)
--   any_recurrence_flag=FALSE & recurrence_date IS NULL     → 8,925 (neither)
--
-- Output:
--   manuscript_workspace.manuscript_cohort_v1_recurrence_clean
--     + any_recurrence_final (TRUE / FALSE / NULL triple-state)
--     + recurrence_unknown_date_flag (REC02)
--     + recurrence_orphan_date_flag  (REC03 — guard against future regressions)
--
-- Time-to-event analyses filter recurrence_unknown_date_flag=FALSE.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.manuscript_cohort_v1_recurrence_clean AS
SELECT
  c.*,
  CASE
    WHEN c.any_recurrence_flag AND c.recurrence_date IS NOT NULL THEN TRUE
    WHEN (c.any_recurrence_flag IS FALSE OR c.any_recurrence_flag IS NULL) AND c.recurrence_date IS NULL THEN FALSE
    ELSE NULL
  END AS any_recurrence_final,
  (c.any_recurrence_flag AND c.recurrence_date IS NULL) AS recurrence_unknown_date_flag,
  ((c.any_recurrence_flag IS FALSE OR c.any_recurrence_flag IS NULL)
     AND c.recurrence_date IS NOT NULL) AS recurrence_orphan_date_flag
FROM main.manuscript_cohort_v1 c;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('REC02','REC03');

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'REC02',
  TRY_CAST(research_id AS INTEGER),
  'main.manuscript_cohort_v1',
  CAST(research_id AS VARCHAR),
  TO_JSON(struct_pack(
    any_recurrence_flag := any_recurrence_flag,
    recurrence_date := recurrence_date,
    structural := structural_recurrence_flag,
    biochemical := biochemical_recurrence_flag,
    recurrence_type_primary := recurrence_type_primary,
    recurrence_source := recurrence_source
  )),
  'Any-recurrence flag TRUE but recurrence_date NULL — time-to-event drops; need chart review for date imputation',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.manuscript_cohort_v1
WHERE any_recurrence_flag AND recurrence_date IS NULL;

COMMENT ON COLUMN main.manuscript_cohort_v1.any_recurrence_flag IS
'Patient-level recurrence-ever flag. 1,764 patients TRUE without a recurrence_date (REC02). See manuscript_workspace.manuscript_cohort_v1_recurrence_clean.any_recurrence_final and recurrence_unknown_date_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_21';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.manuscript_cohort_v1.any_recurrence_flag','column',
   'manuscript_workspace.manuscript_cohort_v1_recurrence_clean',
   'REC02,REC03','prompt_21','column_only',DATE '2026-04-23',
   '1,764 patients flag=TRUE with NULL date (REC02); 0 patients date-without-flag (REC03). 182 patients have both.',
   NULL,
   'any_recurrence_final triple-state + recurrence_unknown_date_flag + recurrence_orphan_date_flag. 1,764 REC02 rows queued.');
