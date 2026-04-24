-- ============================================================================
-- Migration 56 — T4b invasion tier-2 canonical (project mig_55_t4b_invasion)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Source:        main.note_entities_llm_t4b_invasion_v1 (parsed_json; error=0)
-- Delivers:      main.canonical_t4b_invasion_events_v1
--                main.canonical_t4b_invasion_patient_rollup_v1
--                main.v_note_entities_llm_t4b_invasion_v1 (flattener)
-- Downstream:    Rebuild main.canonical_ete_subgrade_patient_rollup_v1 with
--                T4b-layer crosswalk columns (ETE any_pT4b vs tier-2 anatomic).
-- Date:          2026-04-24
-- ============================================================================

CREATE OR REPLACE VIEW main.v_note_entities_llm_t4b_invasion_v1 AS
SELECT
  note_row_id,
  research_id::VARCHAR        AS research_id,
  note_type,
  note_index,
  source_workbook,
  source_sheet,
  source_column,
  json_extract_string(parsed_json, '$.prevertebral_fascia_invasion')   AS prevertebral_fascia_invasion,
  json_extract_string(parsed_json, '$.carotid_encasement')             AS carotid_encasement,
  json_extract_string(parsed_json, '$.mediastinal_vessel_invasion')    AS mediastinal_vessel_invasion,
  json_extract_string(parsed_json, '$.t4b_implication')                AS t4b_implication,
  json_extract_string(parsed_json, '$.confidence')                     AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')                 AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                      AS reasoning,
  raw_llm_response,
  error,
  extracted_at,
  llm_model,
  elapsed_s,
  build_ts                                                    AS llm_build_ts
FROM main.note_entities_llm_t4b_invasion_v1;

CREATE OR REPLACE TABLE main.canonical_t4b_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS t4b_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type, note_index,
  source_workbook, source_sheet, source_column,
  json_extract_string(parsed_json, '$.prevertebral_fascia_invasion')  AS prevertebral_fascia_invasion,
  json_extract_string(parsed_json, '$.carotid_encasement')             AS carotid_encasement,
  json_extract_string(parsed_json, '$.mediastinal_vessel_invasion')    AS mediastinal_vessel_invasion,
  json_extract_string(parsed_json, '$.t4b_implication')                AS t4b_implication,
  json_extract_string(parsed_json, '$.confidence')                     AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')                 AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                      AS reasoning,
  llm_model, extracted_at, build_ts AS llm_build_ts,
  'mig_55_t4b_invasion_20260424'                                      AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                AS build_ts
FROM main.note_entities_llm_t4b_invasion_v1
WHERE error = 0;

CREATE OR REPLACE TABLE main.canonical_t4b_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                                                  AS n_events,
    BOOL_OR(prevertebral_fascia_invasion='present')                                           AS any_prevertebral_fascia,
    BOOL_OR(carotid_encasement='present')                                                     AS any_carotid_encasement,
    BOOL_OR(mediastinal_vessel_invasion='present')                                            AS any_mediastinal_vessel,
    BOOL_OR(t4b_implication='pT4b')                                                           AS any_pT4b_direct,
    SUM(CASE WHEN t4b_implication='pT4b' THEN 1 ELSE 0 END)                                   AS n_pT4b_events,
    SUM(CASE WHEN prevertebral_fascia_invasion='present' THEN 1 ELSE 0 END)                   AS n_prevertebral_events,
    SUM(CASE WHEN carotid_encasement='present'           THEN 1 ELSE 0 END)                   AS n_carotid_events,
    SUM(CASE WHEN mediastinal_vessel_invasion='present'  THEN 1 ELSE 0 END)                   AS n_mediastinal_events
  FROM main.canonical_t4b_invasion_events_v1
  GROUP BY research_id
)
SELECT
  research_id,
  n_events,
  any_prevertebral_fascia,
  any_carotid_encasement,
  any_mediastinal_vessel,
  any_pT4b_direct,
  (any_prevertebral_fascia OR any_carotid_encasement OR any_mediastinal_vessel OR any_pT4b_direct)
                                                                                              AS any_pT4b_final,
  n_pT4b_events, n_prevertebral_events, n_carotid_events, n_mediastinal_events,
  'mig_55_t4b_invasion_20260424'                                                              AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                                        AS build_ts
FROM agg;

-- ETE patient rollup: same worst-mention logic as migration 55 (ETE tier-2), plus T4b crosswalk.
CREATE OR REPLACE TABLE main.canonical_ete_subgrade_patient_rollup_v1 AS
WITH ranked AS (
  SELECT
    research_id,
    ete_grade,
    ajcc8_implication,
    confidence,
    CASE ete_grade
      WHEN 'gross'       THEN 3
      WHEN 'microscopic' THEN 2
      WHEN 'absent'      THEN 1
      ELSE 0
    END AS grade_rank,
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
  t.any_pT4b_final                                               AS any_pT4b_from_t4b_invasion,
  CASE
    WHEN t.research_id IS NULL THEN NULL
    WHEN a.any_pT4b IS NULL THEN NULL
    ELSE (a.any_pT4b IS DISTINCT FROM t.any_pT4b_final)
  END                                                            AS pT4b_ete_vs_t4b_invasion_discordant,
  'mig_54_ete_subgrade_20260424'                                 AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                           AS build_ts
FROM agg a
LEFT JOIN worst w USING (research_id)
LEFT JOIN main.canonical_t4b_invasion_patient_rollup_v1 t USING (research_id);

-- Registry (idempotent)
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN (
  'canonical_t4b_invasion_events_v1',
  'canonical_t4b_invasion_patient_rollup_v1',
  'v_note_entities_llm_t4b_invasion_v1'
);

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_t4b_invasion_events_v1',
  'main',
  'research_id',
  'one row per note entity (T4b anatomic invasion from LLM)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / AJCC8 T-stage',
  'prevertebral_fascia_invasion, carotid_encasement, mediastinal_vessel_invasion, t4b_implication, confidence',
  'mig_55_t4b_invasion_20260424: tier-2 T4b (prevertebral fascia / carotid encasement / mediastinal vessels) from gpt-oss-120b; provenance main.note_entities_llm_t4b_invasion_v1.',
  'v1_0'
FROM main.canonical_t4b_invasion_events_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_t4b_invasion_patient_rollup_v1',
  'main',
  'research_id',
  'one row per patient',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / AJCC8 T-stage',
  'any_prevertebral_fascia, any_carotid_encasement, any_mediastinal_vessel, any_pT4b_final, n_* tallies',
  'mig_55_t4b_invasion_20260424: patient rollup; any_pT4b_final = direct pT4b implication OR any anatomic component present.',
  'v1_0'
FROM main.canonical_t4b_invasion_patient_rollup_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'v_note_entities_llm_t4b_invasion_v1',
  'main',
  'research_id',
  'one row per loader row (convenience view over parsed_json)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'ETE / AJCC8 T-stage (loader)',
  'parsed_json fields as columns; same grain as main.note_entities_llm_t4b_invasion_v1',
  'View: flat columns over main.note_entities_llm_t4b_invasion_v1 for QA',
  'v1_0'
FROM main.v_note_entities_llm_t4b_invasion_v1;

DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'canonical_ete_subgrade_patient_rollup_v1';

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
  'worst_ete_grade, any_gross_ete, any_microscopic_ete, any_pT4b/4a/3b, any_pT4b_from_t4b_invasion, pT4b_ete_vs_t4b_invasion_discordant',
  'Rollup of canonical_ete_subgrade_events_v1; worst-mention logic. 2026-04-24: crosswalk to canonical_t4b_invasion_patient_rollup_v1.any_pT4b_final (any_pT4b_from_t4b_invasion). pT4b_ete_vs_t4b_invasion_discordant is non-null only when ETE any_pT4b is known and differs from tier-2.',
  'v1_0'
FROM main.canonical_ete_subgrade_patient_rollup_v1;
