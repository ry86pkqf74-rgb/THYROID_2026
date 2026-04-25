-- ============================================================================
-- Migration 54 — Fresh ETE subgrade LLM layer on 167 PTC unspec_remaining
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace  (main.note_entities_llm_ete_subgrade_v1
--                populated by scripts/412_ete_subgrade_load_to_md.py before
--                this migration runs)
-- Issue ID:      MANUSCRIPT_ETE_SUBGRADE_FRESH
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Predecessor:   Migration 53 layered `main.note_entities_llm_pathology` ETE
--                entities onto 196 PTC present_unspecified patients; 29 got
--                reclassified, 167 remain as `ete_grade_final='unspec_remaining'`.
--
-- This migration: layers a fresh, narrow 4-way LLM pass (gross/microscopic/
-- absent/unable_to_determine) extracted from OPNOTE/HP/DC_SUM/ED_NOTE snippets
-- for those 167 patients. Output landed in main.note_entities_llm_ete_subgrade_v1
-- via 411/412 on 2026-04-24 using qwen3:14b on fleet server S8.
--
-- Scope of migration:
--   1. Build VIEW ete_llm_fresh_subgrade_patient_v1 (patient-level rollup of
--      fresh LLM output; gross > micro > absent > unable_to_determine).
--   2. Build VIEW ete_manuscript_analytic_v3 (layered on v2 — uses fresh LLM
--      output to resolve the 167 unspec_remaining when possible).
--   3. Register deprecation pointer v2 → v3.
-- ============================================================================

-- 1. Patient-level fresh-LLM rollup --------------------------------------------
CREATE OR REPLACE VIEW manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 AS
WITH graded AS (
  SELECT research_id, ete_grade_llm, confidence, evidence_quote, ajcc8_implication
  FROM main.note_entities_llm_ete_subgrade_v1
  WHERE ete_grade_llm IN ('gross','microscopic','absent','unable_to_determine')
)
SELECT
  research_id,
  -- Collapsed single-field (severity hierarchy): gross > microscopic > absent > unable
  CASE
    WHEN MAX(CASE WHEN ete_grade_llm='gross' THEN 1 ELSE 0 END)=1           THEN 'gross'
    WHEN MAX(CASE WHEN ete_grade_llm='microscopic' THEN 1 ELSE 0 END)=1     THEN 'microscopic'
    WHEN MAX(CASE WHEN ete_grade_llm='absent' THEN 1 ELSE 0 END)=1          THEN 'absent'
    ELSE 'unable_to_determine'
  END AS ete_grade_fresh_llm,
  MAX(CASE WHEN ete_grade_llm='gross' THEN 1 ELSE 0 END)::BOOLEAN AS has_gross_fresh,
  MAX(CASE WHEN ete_grade_llm='microscopic' THEN 1 ELSE 0 END)::BOOLEAN AS has_micro_fresh,
  MAX(CASE WHEN ete_grade_llm='absent' THEN 1 ELSE 0 END)::BOOLEAN AS has_absent_fresh,
  -- Best-available provenance
  STRING_AGG(
    CASE WHEN ete_grade_llm IN ('gross','microscopic','absent')
         THEN NULLIF(evidence_quote,'')
         ELSE NULL END,
    ' | '
  ) AS fresh_evidence_quotes,
  MAX(confidence) AS fresh_best_confidence,
  STRING_AGG(DISTINCT ajcc8_implication, ',') AS fresh_ajcc8_implications,
  COUNT(*) AS n_fresh_mentions
FROM graded
GROUP BY research_id;

-- 2. Layered analytic view -----------------------------------------------------
CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v3 AS
SELECT
  v2.*,
  f.ete_grade_fresh_llm,
  f.has_gross_fresh,
  f.has_micro_fresh,
  f.has_absent_fresh,
  f.fresh_evidence_quotes,
  f.fresh_best_confidence,
  f.fresh_ajcc8_implications,
  f.n_fresh_mentions AS llm_fresh_mention_count,
  -- Final resolved grade: v2 is authoritative except on 'unspec_remaining',
  -- where we apply the fresh LLM output.
  CASE
    WHEN v2.ete_grade_final <> 'unspec_remaining'                THEN v2.ete_grade_final
    WHEN f.has_gross_fresh                                       THEN 'gross'
    WHEN f.has_micro_fresh                                       THEN 'microscopic'
    WHEN f.has_absent_fresh                                      THEN 'none'
    ELSE 'unspec_remaining'
  END AS ete_grade_final_v3,
  CASE
    WHEN v2.ete_grade_final <> 'unspec_remaining'                THEN v2.ete_grade_source
    WHEN f.has_gross_fresh OR f.has_micro_fresh                  THEN 'llm_fresh_subgrade'
    WHEN f.has_absent_fresh                                      THEN 'llm_fresh_absent'
    WHEN f.ete_grade_fresh_llm = 'unable_to_determine'           THEN 'llm_unable'
    ELSE 'unresolved'
  END AS ete_grade_source_v3
FROM manuscript_workspace.ete_manuscript_analytic_v2 v2
LEFT JOIN manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 f
  ON f.research_id = v2.research_id;

-- 3. Deprecation log -----------------------------------------------------------
DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_53';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('manuscript_workspace.ete_manuscript_analytic_v2.ete_grade_final','column',
   'manuscript_workspace.ete_manuscript_analytic_v3.ete_grade_final_v3',
   'MANUSCRIPT_ETE_SUBGRADE_FRESH','prompt_53','column_only',DATE '2026-04-24',
   'Migration 54: fresh narrow 4-way LLM pass (gross/microscopic/absent/unable_to_determine) on 132 OPNOTE/HP/DC_SUM/ED_NOTE snippets across the 167 PTC unspec_remaining patients. Extraction via qwen3:14b on fleet server S8. Output in main.note_entities_llm_ete_subgrade_v1 → patient rollup ete_llm_fresh_subgrade_patient_v1 → layered into ete_manuscript_analytic_v3 (ete_grade_final_v3). Final patient-level buckets for PTC analytic-eligible documented in the post-load acceptance probe.',
   NULL,
   'Downstream ETE analyses should use ete_manuscript_analytic_v3.ete_grade_final_v3 ∈ {gross, microscopic, none, unspec_remaining} with provenance via ete_grade_source_v3 ∈ {structured, llm_subgrade, llm_fresh_subgrade, llm_fresh_absent, llm_unable, unresolved}. v2 preserved for audit; switch over in manuscript analytic scripts.');
