-- ============================================================================
-- Migration 28 — US04: inm_v1_only weak-resolution nodule rows
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      US04 — imaging analog of PATH16
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.canonical_us_nodule_v2.resolution_rule distribution:
--   inm_v1_only   32,145   weak
--   inm_v1+llm     4,812   strong
--   NULL             622   weak
--
-- US04 definition: rows with resolution_rule = 'inm_v1_only' OR NULL are
-- "weak" — the size/location/TIRADS came from the INM v1 parser alone with
-- no LLM second pass. Cohort downstream filters us_resolution_strength='strong'
-- when it needs a high-confidence nodule phenotype.
--
-- Output:
--   manuscript_workspace.canonical_us_nodule_v2_filtered
--     + us_resolution_strength ∈ {weak, strong, other}
--   (View body updated in place — us_row_type + us_resolution_strength coexist)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_us_nodule_v2_filtered AS
SELECT
  n.*,
  (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
   AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
    AS size_all_null_flag,
  (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
    AS location_all_null_flag,
  CASE
    WHEN n.is_aggregate_row THEN 'aggregate_row'
    WHEN (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
          AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
         AND (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
      THEN 'shell'
    WHEN (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
          AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
      THEN 'nodule_sizeless'
    WHEN (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
      THEN 'nodule_locationless'
    ELSE 'nodule_with_measures'
  END AS us_row_type,
  CASE
    WHEN n.resolution_rule = 'inm_v1_only' OR n.resolution_rule IS NULL THEN 'weak'
    WHEN n.resolution_rule LIKE '%llm%' THEN 'strong'
    ELSE 'other'
  END AS us_resolution_strength
FROM main.canonical_us_nodule_v2 n;

COMMENT ON COLUMN main.canonical_us_nodule_v2.resolution_rule IS
'Parser provenance: inm_v1_only (32,145 rows), inm_v1+llm (4,812), NULL (622). See manuscript_workspace.canonical_us_nodule_v2_filtered.us_resolution_strength = {weak (inm_v1_only or NULL), strong (any _llm), other}. Cohort analytics filter us_resolution_strength=strong when high-confidence nodule phenotype is required.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_27';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_nodule_v2.resolution_rule','column',
   'manuscript_workspace.canonical_us_nodule_v2_filtered',
   'US04','prompt_27','column_only',DATE '2026-04-23',
   '32,767 rows are weak-resolution (inm_v1_only + NULL); 4,812 rows are strong (inm_v1+llm). No queue emissions — this is a parser-provenance band for downstream cohort filters, not a defect list.',
   NULL,
   'us_resolution_strength ∈ {weak, strong, other} added to canonical_us_nodule_v2_filtered alongside us_row_type. Downstream filters us_resolution_strength=strong when needed.');
