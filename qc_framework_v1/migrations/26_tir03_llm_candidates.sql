-- ============================================================================
-- Migration 26 — TIR03: multi-nodule under-explosion candidate list
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      TIR03 — multi-nodule under-explosion
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Detection per registry: exams where ≥5 nodules reported, ≥3 distinct reported
-- TIRADS values in text, but ≤2 ACR categories computed — strong signal the
-- parser collapsed multiple nodules into a single row pattern.
--
-- Registry estimate: 60 exams / 56 patients (circa batch-2 snapshot).
-- CURRENT detection yields 448 exams / 319 patients. This is a higher count
-- than previously observed, reflecting growth in canonical_us_nodule_v2 and
-- possibly noisier inm_v1_only rows. Logan to review before authorizing LLM run.
--
-- Output (TABLE, not view — static snapshot):
--   manuscript_workspace.qc_tir03_llm_candidates_v1
--     us_exam_id, research_id, exam_date, n_current_nodules,
--     n_reported_tirads, n_acr_cats, resolution_rules,
--     reported_tirads_list, acr_cats_list, candidate_built_at
--
-- LLM re-parse script skeleton at: qc_framework_v1/tir03_llm_reparse.py
--   (NOT executed in this prompt — awaits Logan authorization after review.)
-- ============================================================================

CREATE OR REPLACE TABLE manuscript_workspace.qc_tir03_llm_candidates_v1 AS
WITH exam_metrics AS (
  SELECT
    us_exam_id,
    ANY_VALUE(research_id) AS research_id,
    ANY_VALUE(exam_date)   AS exam_date,
    COUNT(*) AS n_current_nodules,
    COUNT(DISTINCT tirads_reported_in_text) AS n_reported_tirads,
    COUNT(DISTINCT acr2017_tirads_category) AS n_acr_cats,
    STRING_AGG(DISTINCT resolution_rule, '|') AS resolution_rules,
    STRING_AGG(DISTINCT CAST(tirads_reported_in_text AS VARCHAR), ',') AS reported_tirads_list,
    STRING_AGG(DISTINCT acr2017_tirads_category, ',') AS acr_cats_list
  FROM main.canonical_us_nodule_v2
  WHERE NOT is_aggregate_row
  GROUP BY us_exam_id
  HAVING COUNT(*) >= 5
     AND COUNT(DISTINCT tirads_reported_in_text) >= 3
     AND COUNT(DISTINCT acr2017_tirads_category) <= 2
)
SELECT
  em.us_exam_id,
  em.research_id,
  em.exam_date,
  em.n_current_nodules,
  em.n_reported_tirads,
  em.n_acr_cats,
  em.resolution_rules,
  em.reported_tirads_list,
  em.acr_cats_list,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS candidate_built_at
FROM exam_metrics em;

COMMENT ON TABLE main.canonical_us_nodule_v2 IS
'Per-nodule US nodule table. 448 exams flagged for TIR03 multi-nodule under-explosion (≥5 rows, ≥3 distinct reported TIRADS, ≤2 ACR categories) — see manuscript_workspace.qc_tir03_llm_candidates_v1 and qc_framework_v1/tir03_llm_reparse.py.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_25';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_nodule_v2','table',
   'manuscript_workspace.qc_tir03_llm_candidates_v1',
   'TIR03','prompt_25','pointer_only',DATE '2026-04-23',
   '448 exams / 319 patients flagged for multi-nodule under-explosion (vs registry estimate 60/56; current detection rule yields higher count). LLM re-parse script scaffolded at qc_framework_v1/tir03_llm_reparse.py — NOT YET EXECUTED; awaiting Logan review of candidate list.',
   NULL,
   'Candidate list ready; LLM patch table will populate canonical_us_nodule_v2_tir03_patch_v1 when run.');
