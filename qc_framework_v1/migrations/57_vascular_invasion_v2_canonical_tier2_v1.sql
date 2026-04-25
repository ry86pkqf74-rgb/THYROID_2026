-- ============================================================================
-- Migration 57 — Vascular / lymphatic / perineural invasion v2 (tier-2 canonical)
-- Project: mig_56 (project_mig_56_vascular_invasion_v2_closeout)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Source:        main.note_entities_llm_vascular_invasion_v2 (err=0; build 9b82651+)
-- Delivers:      main.canonical_vascular_invasion_events_v1
--                main.canonical_vascular_invasion_patient_rollup_v1
-- Deprecates:    main.note_entities_llm_vascular_invasion →
--                main._deprecated_note_entities_llm_vascular_invasion
-- Prerequisite:  main.note_entities_llm_vascular_invasion_v2 populated
-- Date:          2026-04-24
-- `build_script` / provenance: mig_56_vascular_invasion_v2_20260424
-- ============================================================================

-- Event grain: one row per (research_id, note_row_id) — note_row_id is unique
CREATE OR REPLACE TABLE main.canonical_vascular_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS vi_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type,
  note_index,
  source_workbook,
  source_sheet,
  source_column,
  json_extract_string(parsed_json, '$.vascular_invasion')            AS vascular_invasion,
  json_extract_string(parsed_json, '$.vascular_invasion_extent')  AS vascular_invasion_extent,
  TRY_CAST(json_extract_string(parsed_json, '$.vessel_count') AS INTEGER)
                                                                  AS vessel_count,
  json_extract_string(parsed_json, '$.lymphatic_invasion')         AS lymphatic_invasion,
  json_extract_string(parsed_json, '$.perineural_invasion')        AS perineural_invasion,
  json_extract_string(parsed_json, '$.lvi_collapsed')              AS lvi_collapsed,
  json_extract_string(parsed_json, '$.tumor_type_context')         AS tumor_type_context,
  json_extract_string(parsed_json, '$.confidence')                 AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')             AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                  AS reasoning,
  llm_model,
  extracted_at,
  build_ts                                                          AS llm_build_ts,
  'mig_56_vascular_invasion_v2_20260424'                          AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                              AS build_ts
FROM main.note_entities_llm_vascular_invasion_v2
WHERE error = 0;

-- Patient grain: one row per research_id
CREATE OR REPLACE TABLE main.canonical_vascular_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                              AS n_events,
    BOOL_OR(vascular_invasion = 'present')                                 AS any_vascular_invasion,
    BOOL_OR(lymphatic_invasion = 'present')                                 AS any_lymphatic_invasion,
    BOOL_OR(perineural_invasion = 'present')                                AS any_perineural_invasion,
    BOOL_OR(lvi_collapsed = 'present')                                      AS any_lvi_collapsed,
    MAX(
      CASE vascular_invasion_extent
        WHEN 'widely_invasive' THEN 4
        WHEN 'extensive'       THEN 3
        WHEN 'focal'           THEN 2
        WHEN 'minimal'         THEN 1
        ELSE 0
      END
    )                                                                     AS max_extent_rank,
    SUM(CASE WHEN vascular_invasion = 'present'   THEN 1 ELSE 0 END)        AS n_vi_events,
    SUM(CASE WHEN lymphatic_invasion = 'present'  THEN 1 ELSE 0 END)        AS n_li_events,
    SUM(CASE WHEN perineural_invasion = 'present' THEN 1 ELSE 0 END)        AS n_pni_events,
    MAX(vessel_count)                                                     AS max_vessel_count
  FROM main.canonical_vascular_invasion_events_v1
  GROUP BY research_id
),
ctx AS (
  SELECT
    research_id,
    string_agg(s, ';' ORDER BY s) AS tumor_type_contexts
  FROM (
    SELECT DISTINCT
      research_id,
      trim(tumor_type_context) AS s
    FROM main.canonical_vascular_invasion_events_v1
    WHERE
      tumor_type_context IS NOT NULL
      AND len(trim(tumor_type_context)) > 0
  ) d
  GROUP BY research_id
)
SELECT
  a.research_id,
  a.n_events,
  a.any_vascular_invasion,
  a.any_lymphatic_invasion,
  a.any_perineural_invasion,
  a.any_lvi_collapsed,
  CASE a.max_extent_rank
    WHEN 4 THEN 'widely_invasive'
    WHEN 3 THEN 'extensive'
    WHEN 2 THEN 'focal'
    WHEN 1 THEN 'minimal'
    ELSE NULL
  END                                                                    AS worst_extent,
  a.n_vi_events,
  a.n_li_events,
  a.n_pni_events,
  a.max_vessel_count,
  c.tumor_type_contexts,
  'mig_56_vascular_invasion_v2_20260424'                                 AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                 AS build_ts
FROM agg a
LEFT JOIN ctx c USING (research_id);

-- Legacy table rename + deprecated registry: run
--   `uv run python scripts/apply_mig_57_vascular_invasion_v2_extras.py`
-- after this file (idempotent: skips if v1 table already gone or already renamed).

COMMENT ON TABLE main.canonical_vascular_invasion_events_v1 IS
'[domain=invasion_vascular_v2; grain=per_note_mention] — mig_56_vascular_invasion_v2_20260424. Tier-2 VI/LI/PNI + extent from note_entities_llm_vascular_invasion_v2.';

COMMENT ON TABLE main.canonical_vascular_invasion_patient_rollup_v1 IS
'[domain=invasion_vascular_v2; grain=per_patient] — mig_56_vascular_invasion_v2_20260424. Rollup from canonical_vascular_invasion_events_v1; worst_extent tier from vascular_invasion_extent.';

-- Registry: new canonicals (idempotent)
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN (
  'canonical_vascular_invasion_events_v1',
  'canonical_vascular_invasion_patient_rollup_v1'
);

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_vascular_invasion_events_v1',
  'main',
  'research_id',
  'one row per note row (V2 VI/LI/PNI + extent from LLM on mentions)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'invasion / pathology NLP',
  'vascular_invasion, lymphatic_invasion, perineural_invasion, vascular_invasion_extent, lvi_collapsed, vessel_count, confidence',
  'Migration 57 (mig_56_vascular_invasion_v2_20260424): tier-2 mention-level vascular/lymphatic/perineural; provenance main.note_entities_llm_vascular_invasion_v2 (parsed_json).',
  'v1_0'
FROM main.canonical_vascular_invasion_events_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_vascular_invasion_patient_rollup_v1',
  'main',
  'research_id',
  'one row per patient',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'invasion / pathology NLP',
  'any_vascular_invasion, any_lymphatic_invasion, any_perineural_invasion, any_lvi_collapsed, worst_extent, n_* events, max_vessel_count, tumor_type_contexts',
  'Migration 57: patient rollup; worst extent rank over vascular_invasion_extent. Future: source canonical_invasion_patient_rollup_v1 mentions layer + path-synoptic.',
  'v1_0'
FROM main.canonical_vascular_invasion_patient_rollup_v1;
