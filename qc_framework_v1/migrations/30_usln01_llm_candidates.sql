-- ============================================================================
-- Migration 30 — USLN01: US lymph node table 100% shell
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      USLN01 — 6,801 rows / 4,077 patients; 6,793 shell (99.9%)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.canonical_us_lymph_node_v2:
--   total_rows:         6,801
--   n_patients:         4,077
--   shell_rows:         6,793 (size_cm_max, short_axis_mm, long_axis_mm,
--                              neck_level, suspicious_flag, laterality all NULL)
--   non-shell rows:         8
--
-- The LN parse layer essentially never populated structured fields. Blocks
-- prompt 10's multi-source LN architecture from having any US component.
--
-- Candidate signal: parent gland row (canonical_us_thyroid_gland_v2) impression
-- or clinical text mentions LN terms. 855 exams qualify.
--
-- Terms searched (case-insensitive): lymph node, lymphadenopath, abnormal node,
-- cervical node, lad (word-bounded).
--
-- Output:
--   manuscript_workspace.qc_usln01_llm_candidates_v1 (TABLE — static snapshot)
--     research_id, us_exam_id, exam_date,
--     impression_mentions_ln (flag), clinical_mentions_ln (flag),
--     impression_excerpt (first 2KB), clinical_excerpt (first 2KB),
--     candidate_built_at
--
-- LLM extract skeleton: qc_framework_v1/usln01_llm_extract.py
--   (NOT executed in this prompt; awaits authorization.)
-- ============================================================================

CREATE OR REPLACE TABLE manuscript_workspace.qc_usln01_llm_candidates_v1 AS
WITH lnexam AS (
  SELECT DISTINCT research_id, us_exam_id, exam_date
  FROM main.canonical_us_lymph_node_v2
),
candidates AS (
  SELECT
    ln.research_id,
    ln.us_exam_id,
    ln.exam_date,
    g.source_us_impression_text,
    g.clinical_impression_text,
    (g.source_us_impression_text IS NOT NULL AND (
       LOWER(g.source_us_impression_text) LIKE '%lymph node%'
       OR LOWER(g.source_us_impression_text) LIKE '%lymphadenopath%'
       OR LOWER(g.source_us_impression_text) LIKE '%abnormal node%'
       OR LOWER(g.source_us_impression_text) LIKE '%cervical node%'
       OR LOWER(g.source_us_impression_text) LIKE '%lad %'
       OR LOWER(g.source_us_impression_text) LIKE '% lad%')) AS impression_mentions_ln,
    (g.clinical_impression_text IS NOT NULL AND (
       LOWER(g.clinical_impression_text) LIKE '%lymph node%'
       OR LOWER(g.clinical_impression_text) LIKE '%lymphadenopath%'
       OR LOWER(g.clinical_impression_text) LIKE '%abnormal node%'
       OR LOWER(g.clinical_impression_text) LIKE '%cervical node%')) AS clinical_mentions_ln
  FROM lnexam ln
  LEFT JOIN main.canonical_us_thyroid_gland_v2 g USING (us_exam_id)
)
SELECT
  research_id,
  us_exam_id,
  exam_date,
  impression_mentions_ln,
  clinical_mentions_ln,
  SUBSTRING(source_us_impression_text, 1, 2048) AS impression_excerpt,
  SUBSTRING(clinical_impression_text, 1, 2048) AS clinical_excerpt,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS candidate_built_at
FROM candidates
WHERE impression_mentions_ln OR clinical_mentions_ln;

COMMENT ON TABLE main.canonical_us_lymph_node_v2 IS
'US LN table — 6,801 rows / 4,077 patients. 6,793 (99.9%) are shell; structured fields never populated by parser (USLN01). 855 exams flagged as LLM re-parse candidates via parent-gland impression text — see manuscript_workspace.qc_usln01_llm_candidates_v1 and qc_framework_v1/usln01_llm_extract.py.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_29';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_lymph_node_v2','table',
   'manuscript_workspace.qc_usln01_llm_candidates_v1',
   'USLN01','prompt_29','pointer_only',DATE '2026-04-23',
   '6,793 of 6,801 LN rows are shells (99.9% shell rate). 855 exams have parent-gland impression text mentioning LN concerns — LLM re-parse candidate list produced but NOT executed.',
   NULL,
   'Candidate snapshot qc_usln01_llm_candidates_v1; LLM skeleton scaffolded at qc_framework_v1/usln01_llm_extract.py. LLM patch target will be canonical_us_lymph_node_v2_usln01_patch_v1 when authorized.');
