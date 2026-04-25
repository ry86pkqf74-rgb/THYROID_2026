-- ============================================================================
-- Migration 58 — Airway invasion v2 (tier-2 canonical)
-- Project close-out: project_mig_57_airway_invasion_v2_closeout.md
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Source:        main.note_entities_llm_airway_invasion_v2 (error = 0; build 9b82651+)
-- Delivers:      main.canonical_airway_invasion_events_v1
--                main.canonical_airway_invasion_patient_rollup_v1
-- Deprecates:    main.note_entities_llm_airway_invasion →
--                main._deprecated_note_entities_llm_airway_invasion
--                (rename + registry via scripts/apply_mig_58_airway_invasion_v2_extras.py)
-- Prerequisite:  main.note_entities_llm_airway_invasion_v2 populated
-- Date:          2026-04-24
-- `build_script` / provenance: mig_57_airway_invasion_v2_20260424
-- Esophagus:     `esophageal_invasion` here is airway-run context; tier-2 esophagus remains
--                `canonical_esophageal_invasion_*_v1` — overlap possible; keep both (documented).
-- ============================================================================

CREATE OR REPLACE TABLE main.canonical_airway_invasion_events_v1 AS
SELECT
  note_row_id                                                     AS airway_event_id,
  research_id::VARCHAR                                            AS research_id,
  note_type,
  note_index,
  source_workbook,
  source_sheet,
  source_column,
  json_extract_string(parsed_json, '$.tracheal_invasion')          AS tracheal_invasion,
  json_extract_string(parsed_json, '$.tracheal_invasion_depth')    AS tracheal_invasion_depth,
  json_extract_string(parsed_json, '$.laryngeal_invasion')       AS laryngeal_invasion,
  json_extract_string(parsed_json, '$.cricoid_invasion')         AS cricoid_invasion,
  json_extract_string(parsed_json, '$.rln_invasion')             AS rln_invasion,
  json_extract_string(parsed_json, '$.rln_paralysis_preop')      AS rln_paralysis_preop,
  json_extract_string(parsed_json, '$.esophageal_invasion')      AS esophageal_invasion,
  json_extract_string(parsed_json, '$.t4a_implication')          AS t4a_implication,
  json_extract_string(parsed_json, '$.confidence')               AS confidence,
  json_extract_string(parsed_json, '$.evidence_quote')           AS evidence_quote,
  json_extract_string(parsed_json, '$.reasoning')                  AS reasoning,
  llm_model,
  extracted_at,
  build_ts                                                          AS llm_build_ts,
  'mig_57_airway_invasion_v2_20260424'                            AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM main.note_entities_llm_airway_invasion_v2
WHERE error = 0;

CREATE OR REPLACE TABLE main.canonical_airway_invasion_patient_rollup_v1 AS
WITH agg AS (
  SELECT
    research_id,
    COUNT(*)                                                              AS n_events,
    BOOL_OR(tracheal_invasion   IN ('present', 'shaved'))                 AS any_tracheal_involvement,
    BOOL_OR(tracheal_invasion   =  'present')                             AS any_tracheal_invasion_present,
    BOOL_OR(tracheal_invasion   =  'shaved')                              AS any_tracheal_shave,
    BOOL_OR(laryngeal_invasion  =  'present')                             AS any_laryngeal_invasion,
    BOOL_OR(cricoid_invasion    =  'present')                             AS any_cricoid_invasion,
    BOOL_OR(rln_invasion        =  'present')                             AS any_rln_invasion,
    BOOL_OR(rln_paralysis_preop =  'present')                             AS any_rln_paralysis_preop,
    BOOL_OR(esophageal_invasion =  'present')                             AS any_esophageal_invasion,
    BOOL_OR(t4a_implication     =  'pT4a')                                AS any_pT4a_direct,
    MAX(
      CASE tracheal_invasion_depth
        WHEN 'full_thickness' THEN 4
        WHEN 'cartilage'      THEN 3
        WHEN 'adventitia'     THEN 2
        WHEN 'mucosal'        THEN 1
        ELSE 0
      END
    )                                                                     AS max_tracheal_depth_rank,
    SUM(CASE WHEN tracheal_invasion = 'present'  THEN 1 ELSE 0 END)       AS n_tracheal_present,
    SUM(CASE WHEN tracheal_invasion = 'shaved'   THEN 1 ELSE 0 END)       AS n_tracheal_shaved,
    SUM(CASE WHEN laryngeal_invasion = 'present' THEN 1 ELSE 0 END)       AS n_laryngeal_present,
    SUM(CASE WHEN rln_invasion = 'present'       THEN 1 ELSE 0 END)       AS n_rln_present,
    SUM(CASE WHEN t4a_implication = 'pT4a'       THEN 1 ELSE 0 END)       AS n_pT4a_events
  FROM main.canonical_airway_invasion_events_v1
  GROUP BY research_id
)
SELECT
  research_id,
  n_events,
  any_tracheal_involvement,
  any_tracheal_invasion_present,
  any_tracheal_shave,
  any_laryngeal_invasion,
  any_cricoid_invasion,
  any_rln_invasion,
  any_rln_paralysis_preop,
  any_esophageal_invasion,
  any_pT4a_direct,
  (
    any_tracheal_invasion_present
    OR any_laryngeal_invasion
    OR any_cricoid_invasion
    OR any_rln_invasion
    OR any_esophageal_invasion
    OR any_pT4a_direct
  )                                                                     AS any_pT4a_final,
  CASE max_tracheal_depth_rank
    WHEN 4 THEN 'full_thickness'
    WHEN 3 THEN 'cartilage'
    WHEN 2 THEN 'adventitia'
    WHEN 1 THEN 'mucosal'
    ELSE NULL
  END                                                                   AS worst_tracheal_depth,
  n_tracheal_present,
  n_tracheal_shaved,
  n_laryngeal_present,
  n_rln_present,
  n_pT4a_events,
  'mig_57_airway_invasion_v2_20260424'                                  AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                  AS build_ts
FROM agg;

COMMENT ON TABLE main.canonical_airway_invasion_events_v1 IS
'[domain=invasion_airway_v2; grain=per_note_mention] — mig_57_airway_invasion_v2_20260424. Tracheal (incl. depth + shaved), laryngeal, cricoid, RLN, esophageal (airway run), t4a_implication from note_entities_llm_airway_invasion_v2. Esophagus-specific tier-2: canonical_esophageal_invasion_*_v1 (separate; may overlap).';

COMMENT ON TABLE main.canonical_airway_invasion_patient_rollup_v1 IS
'[domain=invasion_airway_v2; grain=per_patient] — mig_57_airway_invasion_v2_20260424. Rollup from canonical_airway_invasion_events_v1; worst_tracheal_depth rank; any_pT4a_final = structural OR direct pT4a flag.';

DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN (
  'canonical_airway_invasion_events_v1',
  'canonical_airway_invasion_patient_rollup_v1'
);

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_airway_invasion_events_v1',
  'main',
  'research_id',
  'one row per note row (airway v2: tracheal depth ladder, larynx, cricoid, RLN, esophagus-in-run, t4a)',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'invasion / pathology NLP',
  'tracheal_invasion, tracheal_invasion_depth, laryngeal_invasion, cricoid_invasion, rln_invasion, rln_paralysis_preop, esophageal_invasion, t4a_implication, confidence',
  'Migration 58 (mig_57_airway_invasion_v2_20260424): tier-2 airway anatomy + tracheal depth + pT4a implication; source main.note_entities_llm_airway_invasion_v2. Esophageal mentions also appear in canonical_esophageal_invasion_*_v1 — keep both layers.',
  'v1_0'
FROM main.canonical_airway_invasion_events_v1;

INSERT INTO manuscript_workspace.detail_table_registry_v1
  (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
   domain, feeds_master_columns, description, canonical_version)
SELECT
  'canonical_airway_invasion_patient_rollup_v1',
  'main',
  'research_id',
  'one row per patient',
  COUNT(*),
  COUNT(DISTINCT research_id),
  'invasion / pathology NLP',
  'any_tracheal_involvement, any_tracheal_invasion_present, any_tracheal_shave, any_laryngeal_invasion, any_cricoid_invasion, any_rln_invasion, any_rln_paralysis_preop, any_esophageal_invasion, any_pT4a_direct, any_pT4a_final, worst_tracheal_depth, n_*',
  'Migration 58: patient rollup; max tracheal depth; any_pT4a_final combines structural invasion OR direct pT4a. Future: rebuild canonical_invasion_patient_rollup_v1 any_airway_* / any_tracheal_* from this + structured feeder.',
  'v1_0'
FROM main.canonical_airway_invasion_patient_rollup_v1;
