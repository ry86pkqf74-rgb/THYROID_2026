-- ============================================================================
-- Migration 55 (tier-2 canonical; closes Migration 54 ETE subgrade workstream)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Source:         main.note_entities_llm_ete_subgrade_v1  (287 rows, parsed_json;
--                 gpt-oss-120b / build from loader; error=0)
-- Delivers:       main.canonical_ete_subgrade_events_v1
--                 main.canonical_ete_subgrade_patient_rollup_v1
--                 main.v_note_entities_llm_ete_subgrade_v1  (flattener)
-- Prerequisite:  Migration 54 loader table populated
-- Date:          2026-04-24
-- ============================================================================

-- Optional flattener over loader (column access without JSON in every query)
CREATE OR REPLACE VIEW main.v_note_entities_llm_ete_subgrade_v1 AS
SELECT
  note_row_id,
  research_id::VARCHAR        AS research_id,
  note_type,
  note_index,
  source_workbook,
  source_sheet,
  source_column,
  json_extract_string(parsed_json, '$.ete_grade')          AS ete_grade,
  json_extract_string(parsed_json, '$.ajcc8_implication')   AS ajcc8_implication,
  json_extract_string(parsed_json, '$.confidence')            AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')        AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')             AS reasoning,
  raw_llm_response,
  error,
  extracted_at,
  llm_model,
  elapsed_s,
  build_ts                                                    AS llm_build_ts
FROM main.note_entities_llm_ete_subgrade_v1;

-- Event grain: one row per (research_id, note_row_id) — here note_row_id is unique
CREATE OR REPLACE TABLE main.canonical_ete_subgrade_events_v1 AS
WITH parsed AS (
  SELECT
    note_row_id,
    research_id::VARCHAR                                          AS research_id,
    note_type,
    note_index,
    source_workbook,
    source_sheet,
    source_column,
    json_extract_string(parsed_json, '$.ete_grade')               AS ete_grade,
    json_extract_string(parsed_json, '$.ajcc8_implication')       AS ajcc8_implication,
    json_extract_string(parsed_json, '$.confidence')              AS confidence,
    json_extract_string(parsed_json, '$.evidence_quote')          AS evidence_quote,
    json_extract_string(parsed_json, '$.reasoning')              AS reasoning,
    llm_model,
    extracted_at,
    build_ts                                                      AS llm_build_ts
  FROM main.note_entities_llm_ete_subgrade_v1
  WHERE error = 0
)
SELECT
  note_row_id                                                     AS ete_event_id,
  research_id,
  note_type,
  note_index,
  source_workbook, source_sheet, source_column,
  CASE ete_grade
    WHEN 'gross'                 THEN 'gross'
    WHEN 'microscopic'           THEN 'microscopic'
    WHEN 'absent'                THEN 'absent'
    WHEN 'unable_to_determine'   THEN 'unknown'
    ELSE 'unknown'
  END                                                             AS ete_grade,
  ajcc8_implication,
  confidence,
  evidence_quote,
  reasoning,
  llm_model,
  extracted_at,
  llm_build_ts,
  'mig_54_ete_subgrade_20260424'                                  AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM parsed;

-- Patient grain: one row per research_id
CREATE OR REPLACE TABLE main.canonical_ete_subgrade_patient_rollup_v1 AS
WITH ranked AS (
  SELECT
    research_id,
    ete_grade,
    ajcc8_implication,
    confidence,
    -- priority: gross > microscopic > absent > unknown
    CASE ete_grade
      WHEN 'gross'       THEN 3
      WHEN 'microscopic' THEN 2
      WHEN 'absent'      THEN 1
      ELSE 0
    END AS grade_rank,
    -- AJCC8 priority: pT4b > pT4a > pT3b > pT3a_size_only > null
    CASE ajcc8_implication
      WHEN 'pT4b'           THEN 4
      WHEN 'pT4a'           THEN 3
      WHEN 'pT3b'           THEN 2
      WHEN 'pT3a_size_only' THEN 1
      ELSE 0
    END AS ajcc_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY
        CASE ete_grade WHEN 'gross' THEN 3 WHEN 'microscopic' THEN 2 WHEN 'absent' THEN 1 ELSE 0 END DESC,
        CASE ajcc8_implication
          WHEN 'pT4b' THEN 4 WHEN 'pT4a' THEN 3 WHEN 'pT3b' THEN 2 WHEN 'pT3a_size_only' THEN 1 ELSE 0 END DESC,
        CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM main.canonical_ete_subgrade_events_v1
),
worst AS (
  SELECT research_id, ete_grade AS worst_ete_grade, ajcc8_implication AS worst_ajcc8_implication, confidence AS worst_confidence
  FROM ranked WHERE rn = 1
),
agg AS (
  SELECT
    research_id,
    COUNT(*)                                                         AS n_events,
    SUM(CASE WHEN ete_grade='gross'       THEN 1 ELSE 0 END)         AS n_gross,
    SUM(CASE WHEN ete_grade='microscopic' THEN 1 ELSE 0 END)         AS n_microscopic,
    SUM(CASE WHEN ete_grade='absent'      THEN 1 ELSE 0 END)         AS n_absent,
    SUM(CASE WHEN ete_grade='unknown'     THEN 1 ELSE 0 END)         AS n_unknown,
    BOOL_OR(ete_grade='gross')                                       AS any_gross_ete,
    BOOL_OR(ete_grade='microscopic')                                 AS any_microscopic_ete,
    BOOL_OR(ajcc8_implication='pT4b')                                AS any_pT4b,
    BOOL_OR(ajcc8_implication='pT4a')                                AS any_pT4a,
    BOOL_OR(ajcc8_implication='pT3b')                                AS any_pT3b
  FROM main.canonical_ete_subgrade_events_v1
  GROUP BY research_id
)
SELECT
  a.research_id,
  w.worst_ete_grade,
  w.worst_ajcc8_implication,
  w.worst_confidence,
  a.n_events, a.n_gross, a.n_microscopic, a.n_absent, a.n_unknown,
  a.any_gross_ete, a.any_microscopic_ete,
  a.any_pT4b, a.any_pT4a, a.any_pT3b,
  'mig_54_ete_subgrade_20260424'                                      AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                AS build_ts
FROM agg a
LEFT JOIN worst w USING (research_id);

-- Registry: canonical ETE subgrade master (idempotent)
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN (
  'canonical_ete_subgrade_events_v1',
  'canonical_ete_subgrade_patient_rollup_v1',
  'v_note_entities_llm_ete_subgrade_v1'
);

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_ete_subgrade_events_v1',
  'main',
  'research_id',
  'one row per note entity (ETE subgrade from LLM on loader snippets)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / NLP',
  'ete_grade, ajcc8_implication, confidence; tier-2 ETE subgrade (gross/microscopic/absent/unknown)',
  'Migration 55 (mig_54_ete_subgrade_20260424): gpt-oss-120b tier-2 ETE subgrade; supersedes mixed ete_* rows in canonical_invasion_events_v1 for manuscript ETE work. Provenance: main.note_entities_llm_ete_subgrade_v1 (parsed_json).',
  'v1_0'
FROM main.canonical_ete_subgrade_events_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_ete_subgrade_patient_rollup_v1',
  'main',
  'research_id',
  'one row per patient',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / NLP',
  'worst_ete_grade, any_gross_ete, any_microscopic_ete, n_* tallies, AJCC8 flags; patient rollup for ETE manuscript cohort',
  'Migration 55: Rollup of canonical_ete_subgrade_events_v1; worst-mention logic (grade then AJCC8 then confidence). Future: feed canonical_invasion_patient_rollup_v1 ETE fields (Script 363+).',
  'v1_0'
FROM main.canonical_ete_subgrade_patient_rollup_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'v_note_entities_llm_ete_subgrade_v1',
  'main',
  'research_id',
  'one row per loader row (convenience view over parsed_json)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / NLP (loader)',
  'parsed_json fields as columns; same grain as main.note_entities_llm_ete_subgrade_v1',
  'View: flat columns over main.note_entities_llm_ete_subgrade_v1 for ad hoc QA and downstream',
  'v1_0'
FROM main.v_note_entities_llm_ete_subgrade_v1;

-- Prior adjudication layer: still authoritative for 45-cohort overrides; ETE grade from notes uses canonical_ete_subgrade_*
UPDATE manuscript_workspace.detail_table_registry_v1
SET description = description
  || ' | 2026-04-24: For document-derived ETE subgrade at scale, use canonical_ete_subgrade_patient_rollup_v1; ete_adjudication_v1 remains the curated 45-case override layer.'
WHERE detail_table_name = 'ete_adjudication_v1';
