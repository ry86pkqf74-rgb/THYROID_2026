-- ============================================================================
-- Migration 53 — ETE LLM grade subclassification layer
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      MANUSCRIPT_ETE_SUBGRADE — 196 PTC present_unspecified patients
--                need microscopic-vs-gross grading for the ETE manuscript.
--                Existing main.note_entities_llm_pathology has 711 ETE entity
--                mentions; this migration surfaces the grade-bearing subset as
--                a patient-level layer that consumers can COALESCE over.
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Scope of this migration:
--   1. Build VIEW ete_llm_grade_patient_v1 — patient-level LLM grade rollup
--   2. Build VIEW ete_manuscript_analytic_v2 — layers ete_llm_grade on top of
--      ete_manuscript_analytic_v1 with a new column `ete_grade_final` that
--      resolves present_unspecified via LLM when available.
--   3. Register deprecation pointer for ete_manuscript_analytic_v1 →
--      ete_manuscript_analytic_v2 (additive pointer; v1 preserved for audit).
--
-- Not in scope:
--   - Fresh LLM pass for the 169 remaining unspec patients (separate Cursor
--     prompt; runs on ResearchFlow fleet, output lands in a new table).
--
-- Probe (2026-04-24) — 196 PTC analytic-eligible present_unspecified patients:
--   24 reclassified to 'gross_llm' via existing extract
--    3 reclassified to 'micro_llm'
--  169 still unspec → queued for fresh LLM pass (migration 54, TBD)
--
-- Final patient-level buckets after this migration (290 PTC analytic-eligible):
--   gross       40     (17 structured + 23 LLM)
--   microscopic 51     (48 structured + 3 LLM)
--   unspec_remaining 169
--   none        30
-- ============================================================================

-- 1. Patient-level LLM grade rollup --------------------------------------------
CREATE OR REPLACE VIEW manuscript_workspace.ete_llm_grade_patient_v1 AS
WITH ete_mentions AS (
  SELECT
    p.research_id,
    p.note_row_id,
    p.note_date,
    LOWER(TRIM(json_extract_string(e.value, '$.entity_value'))) AS entity_value_norm,
    json_extract_string(e.value, '$.presence')       AS presence,
    TRY_CAST(json_extract_string(e.value, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(e.value, '$.evidence_text')  AS evidence_text
  FROM main.note_entities_llm_pathology p,
       LATERAL (SELECT unnest(json_extract(result_json, '$.entities')::json[]) AS value) e
  WHERE json_extract_string(e.value, '$.entity_type') = 'extrathyroidal_extension'
),
classified AS (
  SELECT research_id, note_row_id, note_date, entity_value_norm, confidence, evidence_text,
    CASE
      -- Gross variants (deepest invasion)
      WHEN entity_value_norm ILIKE '%gross%'
        OR entity_value_norm = 'substernal extension'
        OR entity_value_norm ILIKE '%strap muscle%'
        OR entity_value_norm ILIKE '%trachea%'
        OR entity_value_norm ILIKE '%esophag%'
        OR entity_value_norm ILIKE '%recurrent laryngeal%'
        THEN 'gross'
      -- Microscopic / minimal variants
      WHEN entity_value_norm ILIKE '%microscop%'
        OR entity_value_norm ILIKE '%minimal%'
        OR entity_value_norm ILIKE '%perithyroid%fat%'
        OR entity_value_norm ILIKE '%focal%extension%'
        THEN 'microscopic'
      -- Negations / absent
      WHEN entity_value_norm = 'absent'
        OR entity_value_norm ILIKE 'no %'
        OR entity_value_norm ILIKE '%not identified%'
        OR entity_value_norm = 'negative'
        OR entity_value_norm ILIKE '%no ete%'
        THEN 'absent'
      ELSE NULL
    END AS llm_grade
  FROM ete_mentions
)
SELECT
  research_id,
  MAX(CASE WHEN llm_grade = 'gross'       THEN 1 ELSE 0 END)::BOOLEAN AS has_gross_llm,
  MAX(CASE WHEN llm_grade = 'microscopic' THEN 1 ELSE 0 END)::BOOLEAN AS has_micro_llm,
  MAX(CASE WHEN llm_grade = 'absent'      THEN 1 ELSE 0 END)::BOOLEAN AS has_absent_llm,
  -- Collapsed single-field for consumers (gross wins over micro wins over absent)
  CASE
    WHEN MAX(CASE WHEN llm_grade='gross' THEN 1 ELSE 0 END) = 1       THEN 'gross'
    WHEN MAX(CASE WHEN llm_grade='microscopic' THEN 1 ELSE 0 END) = 1 THEN 'microscopic'
    WHEN MAX(CASE WHEN llm_grade='absent' THEN 1 ELSE 0 END) = 1      THEN 'absent'
    ELSE NULL
  END AS ete_grade_llm,
  COUNT(*) AS n_ete_mentions
FROM classified
WHERE llm_grade IS NOT NULL
GROUP BY research_id;

-- 2. Layered analytic view -----------------------------------------------------
CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v2 AS
SELECT
  a.*,
  g.ete_grade_llm,
  g.has_gross_llm,
  g.has_micro_llm,
  g.n_ete_mentions AS llm_ete_mention_count,
  -- Final resolved grade for the manuscript
  CASE
    -- Structured has gross / extensive — trust it
    WHEN a.ete_norm = 'extensive' THEN 'gross'
    -- Structured has minimal/microscopic — trust it
    WHEN a.ete_norm IN ('microscopic','minimal') THEN 'microscopic'
    -- Structured is present_unspecified: try LLM subgrade
    WHEN a.ete_norm = 'present_unspecified' AND g.has_gross_llm THEN 'gross'
    WHEN a.ete_norm = 'present_unspecified' AND g.has_micro_llm THEN 'microscopic'
    WHEN a.ete_norm = 'present_unspecified' THEN 'unspec_remaining'
    -- Structured says none / absent
    WHEN a.ete_norm = 'none' THEN 'none'
    ELSE NULL
  END AS ete_grade_final,
  -- Provenance flag
  CASE
    WHEN a.ete_norm = 'extensive'                                    THEN 'structured'
    WHEN a.ete_norm IN ('microscopic','minimal')                     THEN 'structured'
    WHEN a.ete_norm = 'present_unspecified' AND g.has_gross_llm      THEN 'llm_subgrade'
    WHEN a.ete_norm = 'present_unspecified' AND g.has_micro_llm      THEN 'llm_subgrade'
    WHEN a.ete_norm = 'present_unspecified'                          THEN 'unresolved'
    WHEN a.ete_norm = 'none'                                         THEN 'structured'
    ELSE NULL
  END AS ete_grade_source
FROM manuscript_workspace.ete_manuscript_analytic_v1 a
LEFT JOIN manuscript_workspace.ete_llm_grade_patient_v1 g
  ON g.research_id = a.research_id;

-- 3. Deprecation log -----------------------------------------------------------
DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_52';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('manuscript_workspace.ete_manuscript_analytic_v1.ete_norm','column',
   'manuscript_workspace.ete_manuscript_analytic_v2.ete_grade_final',
   'MANUSCRIPT_ETE_SUBGRADE','prompt_52','column_only',DATE '2026-04-24',
   'Migration 53: layered LLM grade subclassification from main.note_entities_llm_pathology (711 ETE entities) onto the 196 PTC present_unspecified patients. Reclassified 24→gross + 3→microscopic via existing pathology LLM extract; 169 remain as unspec_remaining pending fresh targeted LLM pass (planned migration 54). Final patient-level buckets for PTC analytic-eligible (n=290): gross 40, microscopic 51, unspec_remaining 169, none 30.',
   NULL,
   'Downstream ETE analyses should use ete_manuscript_analytic_v2.ete_grade_final ∈ {gross, microscopic, unspec_remaining, none} with provenance via ete_grade_source ∈ {structured, llm_subgrade, unresolved}. v1 preserved for audit.');
