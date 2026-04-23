-- ============================================================================
-- Migration 24 — FNA01: FNA dated after first surgery
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      FNA01 — 349 events / 286 patients (154 PTC)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- SoT for first_surgery_date: canonical_operative_events_v1 (resolved_surgery_date
-- cast DATE, MIN per patient). Source column is VARCHAR in the canonical table —
-- TRY_CAST handles NULL-or-unparseable cleanly.
--
-- canonical_fna_events_v1: 8,119 events / 5,266 patients
--   no_fna_date:      1,582 (fna_date_resolved IS NULL)
--   pre_op:           6,188
--   post_op:            349 (286 distinct patients) ← FNA01
--   no_surgery_date:      0
--
-- Output:
--   manuscript_workspace.first_surgery_date_v1        — per-patient MIN
--   manuscript_workspace.canonical_fna_events_v1_temporal
--     + first_surgery_date, post_surgery_fna_flag, fna_pre_surgery_flag,
--       fna_temporal_status ∈ {pre_op, post_op, no_surgery_date, no_fna_date}
--
-- Index-FNA selection downstream filters fna_pre_surgery_flag=TRUE.
-- Post-op FNAs are NOT deleted — they are legitimate surveillance events.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.first_surgery_date_v1 AS
SELECT
  research_id,
  MIN(TRY_CAST(resolved_surgery_date AS DATE)) AS first_surgery_date
FROM main.canonical_operative_events_v1
WHERE TRY_CAST(resolved_surgery_date AS DATE) IS NOT NULL
GROUP BY research_id;

CREATE OR REPLACE VIEW manuscript_workspace.canonical_fna_events_v1_temporal AS
SELECT
  fna.*,
  f.first_surgery_date,
  (fna.fna_date_resolved IS NOT NULL AND f.first_surgery_date IS NOT NULL
     AND fna.fna_date_resolved > f.first_surgery_date) AS post_surgery_fna_flag,
  (fna.fna_date_resolved IS NOT NULL AND f.first_surgery_date IS NOT NULL
     AND fna.fna_date_resolved <= f.first_surgery_date) AS fna_pre_surgery_flag,
  CASE
    WHEN fna.fna_date_resolved IS NULL         THEN 'no_fna_date'
    WHEN f.first_surgery_date IS NULL          THEN 'no_surgery_date'
    WHEN fna.fna_date_resolved > f.first_surgery_date THEN 'post_op'
    ELSE 'pre_op'
  END AS fna_temporal_status
FROM main.canonical_fna_events_v1 fna
LEFT JOIN manuscript_workspace.first_surgery_date_v1 f USING (research_id);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA01';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'FNA01',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_fna_events_v1',
  CAST(fna_event_id AS VARCHAR),
  TO_JSON(struct_pack(
    fna_event_id := fna_event_id,
    fna_date_resolved := fna_date_resolved,
    first_surgery_date := first_surgery_date,
    days_to_surgery := days_to_surgery,
    bethesda_final_num := bethesda_final_num,
    fna_temporal_status := fna_temporal_status
  )),
  CONCAT('FNA dated after first surgery by ',
         CAST(DATE_DIFF('day', first_surgery_date, fna_date_resolved) AS VARCHAR),
         ' days'),
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_fna_events_v1_temporal
WHERE post_surgery_fna_flag;

COMMENT ON COLUMN main.canonical_fna_events_v1.fna_date_resolved IS
'Structured FNA date. 349 events / 286 patients (154 PTC) dated after first surgery (FNA01). See manuscript_workspace.canonical_fna_events_v1_temporal.fna_temporal_status for pre_op/post_op classification.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_23';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_fna_events_v1.fna_date_resolved','column',
   'manuscript_workspace.canonical_fna_events_v1_temporal',
   'FNA01','prompt_23','column_only',DATE '2026-04-23',
   '349 events / 286 patients have fna_date_resolved > first_surgery_date per canonical_operative_events_v1 SoT. Post-op FNAs are legitimate surveillance events but must not be selected as index_fna.',
   NULL,
   'fna_temporal_status ∈ {pre_op, post_op, no_surgery_date, no_fna_date} + post_surgery_fna_flag + fna_pre_surgery_flag. Index-FNA selection requires fna_pre_surgery_flag=TRUE. 349 rows queued under FNA01.');
