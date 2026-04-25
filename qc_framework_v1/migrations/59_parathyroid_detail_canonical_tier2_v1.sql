-- ============================================================================
-- Migration 59 — Parathyroid detail (tier-2 canonical)
-- Project close-out: project_mig_58_parathyroid_detail_closeout.md
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Source:        main.note_entities_llm_parathyroid_detail_v1 (error = 0)
-- Delivers:      main.canonical_parathyroid_events_v1
--                main.canonical_parathyroid_patient_rollup_v1
-- Prerequisite:  Loader table present; err=0 rows only — verify with:
--                `uv run python scripts/llm_batch/_verify_loaded.py`
-- Date:          2026-04-24
-- `build_script` / provenance: mig_58_parathyroid_detail_20260424
-- SQL file index: 59 (58 = airway v2 canonical in this repo)
-- ============================================================================

CREATE OR REPLACE TABLE main.canonical_parathyroid_events_v1 AS
SELECT
  note_row_id                                                       AS parathyroid_event_id,
  research_id::VARCHAR                                              AS research_id,
  note_type,
  note_index,
  source_workbook,
  source_sheet,
  source_column,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_identified_count')       AS INTEGER) AS glands_identified_count,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_preserved_in_situ')      AS INTEGER) AS glands_preserved_in_situ,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_autotransplanted')        AS INTEGER) AS glands_autotransplanted,
  TRY_CAST(json_extract_string(parsed_json, '$.glands_inadvertently_removed')   AS INTEGER) AS glands_inadvertently_removed,
  json_extract_string(parsed_json, '$.autotransplant_location')               AS autotransplant_location,
  json_extract_string(parsed_json, '$.parathyroid_pathology')                 AS parathyroid_pathology,
  json_extract_string(parsed_json, '$.incidental_parathyroidectomy')          AS incidental_parathyroidectomy,
  json_extract_string(parsed_json, '$.hypocalcemia_postop')                   AS hypocalcemia_postop,
  json_extract_string(parsed_json, '$.hypoparathyroidism_permanent')          AS hypoparathyroidism_permanent,
  TRY_CAST(json_extract_string(parsed_json, '$.intact_pth_value_ngL')         AS DOUBLE)   AS intact_pth_value_ngL,
  json_extract_string(parsed_json, '$.confidence')                            AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')                      AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                           AS reasoning,
  llm_model,
  extracted_at,
  build_ts                                                                AS llm_build_ts,
  'mig_58_parathyroid_detail_20260424'                                    AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                    AS build_ts
FROM main.note_entities_llm_parathyroid_detail_v1
WHERE error = 0;

CREATE OR REPLACE TABLE main.canonical_parathyroid_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                                      AS n_events,
    MAX(glands_identified_count)                                                  AS max_glands_identified,
    MAX(glands_preserved_in_situ)                                                 AS max_glands_preserved,
    MAX(glands_autotransplanted)                                                  AS max_glands_autotransplanted,
    MAX(glands_inadvertently_removed)                                             AS max_glands_inadvertently_removed,
    STRING_AGG(DISTINCT autotransplant_location, ';')
      FILTER (WHERE autotransplant_location IS NOT NULL)                        AS autotransplant_locations,
    STRING_AGG(DISTINCT parathyroid_pathology, ';')
      FILTER (WHERE parathyroid_pathology IS NOT NULL)                          AS parathyroid_pathologies,
    BOOL_OR(incidental_parathyroidectomy = 'present')                           AS any_incidental_parathyroidectomy,
    BOOL_OR(hypocalcemia_postop = 'present')                                     AS any_hypocalcemia_postop,
    BOOL_OR(hypoparathyroidism_permanent = 'present')                           AS any_permanent_hypoparathyroidism,
    BOOL_OR(glands_autotransplanted >= 1)                                       AS any_autotransplant,
    MIN(intact_pth_value_ngL)                                                   AS min_intact_pth_value_ngL,
    MAX(intact_pth_value_ngL)                                                   AS max_intact_pth_value_ngL
  FROM main.canonical_parathyroid_events_v1
  GROUP BY research_id
)
SELECT
  research_id,
  n_events,
  max_glands_identified,
  max_glands_preserved,
  max_glands_autotransplanted,
  max_glands_inadvertently_removed,
  autotransplant_locations,
  parathyroid_pathologies,
  any_autotransplant,
  any_incidental_parathyroidectomy,
  any_hypocalcemia_postop,
  any_permanent_hypoparathyroidism,
  min_intact_pth_value_ngL,
  max_intact_pth_value_ngL,
  'mig_58_parathyroid_detail_20260424'                                          AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                        AS build_ts
FROM agg;

COMMENT ON TABLE main.canonical_parathyroid_events_v1 IS
'[domain=parathyroid_detail; grain=per_note] — mig_58_parathyroid_detail_20260424. Gland counts, autotransplant location, pathology, incidental PTX, postop hypocalcemia, permanent hypoparathyroidism, iPTH from main.note_entities_llm_parathyroid_detail_v1 (error=0).';

COMMENT ON TABLE main.canonical_parathyroid_patient_rollup_v1 IS
'[domain=parathyroid_detail; grain=per_patient] — mig_58_parathyroid_detail_20260424. MAX gland counts across notes; BOOL_OR for incidental PTX / hypocalcemia / permanent hypoparathyroidism; DISTINCT string lists for locations/pathologies; iPTH min/max.';

DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN (
  'canonical_parathyroid_events_v1',
  'canonical_parathyroid_patient_rollup_v1'
);

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_parathyroid_events_v1',
  'main',
  'research_id',
  'one row per note row (parathyroid detail: gland accounting, autotransplant, pathology, complications, iPTH)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'parathyroid / surgery NLP',
  'glands_identified_count, glands_preserved_in_situ, glands_autotransplanted, glands_inadvertently_removed, autotransplant_location, parathyroid_pathology, incidental_parathyroidectomy, hypocalcemia_postop, hypoparathyroidism_permanent, intact_pth_value_ngL, confidence',
  'Migration 59 SQL / mig_58_parathyroid_detail_20260424: tier-2 parathyroid detail from main.note_entities_llm_parathyroid_detail_v1. Foundation for parathyroid-complication analysis; unblocks complications canonical joined on surgery_episode_id.',
  'v1_0'
FROM main.canonical_parathyroid_events_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_parathyroid_patient_rollup_v1',
  'main',
  'research_id',
  'one row per patient',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'parathyroid / surgery NLP',
  'max_glands_*, autotransplant_locations, parathyroid_pathologies, any_autotransplant, any_incidental_parathyroidectomy, any_hypocalcemia_postop, any_permanent_hypoparathyroidism, min/max intact_pth_value_ngL',
  'Migration 59: patient rollup from canonical_parathyroid_events_v1 (max gland counts; BOOL_OR present flags). Retire Tier-1 nlp_* parathyroid flags after verification.',
  'v1_0'
FROM main.canonical_parathyroid_patient_rollup_v1;
