-- ============================================================================
-- Migration 20 — AJCC01/02/03: staging integrity flags
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     AJCC01 (AJCC7 calc_flag=true but TNM component NULL) — 220 rows
--                AJCC02 (AJCC8 calc_flag=true but TNM component NULL) —  55 rows
--                AJCC03 (all TNM components NON-NULL but overall_stage NULL)
--                       — 384 (ajcc7) + 592 (ajcc8) rows
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Flags integrity violations in the AJCC7 and AJCC8 staging columns on the
-- path event master. All 6 staging-component columns live on
-- canonical_path_malignant_events_v1 (per-tumor grain).
--
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag
--     — all rows + 4 flag columns:
--         ajcc7_calc_flag_inconsistent
--         ajcc8_calc_flag_inconsistent
--         ajcc7_overall_missing_despite_components
--         ajcc8_overall_missing_despite_components
--
-- Queue: 3 issue_ids (AJCC01/02/03). Each row can emit at most one queue
-- entry (most-severe first): AJCC01 > AJCC02 > AJCC03. A single row can
-- emit separate entries for ajcc7 and ajcc8 if both systems violate.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag AS
SELECT
    e.*,
    (e.ajcc7_stage_calculable_flag
       AND (e.t_stage_ajcc7 IS NULL OR e.n_stage_ajcc7 IS NULL OR e.m_stage_ajcc7 IS NULL))
        AS ajcc7_calc_flag_inconsistent,
    (e.ajcc8_stage_calculable_flag
       AND (e.t_stage_ajcc8 IS NULL OR e.n_stage_ajcc8 IS NULL OR e.m_stage_ajcc8 IS NULL))
        AS ajcc8_calc_flag_inconsistent,
    (e.t_stage_ajcc7 IS NOT NULL AND e.n_stage_ajcc7 IS NOT NULL
       AND e.m_stage_ajcc7 IS NOT NULL AND e.overall_stage_ajcc7 IS NULL)
        AS ajcc7_overall_missing_despite_components,
    (e.t_stage_ajcc8 IS NOT NULL AND e.n_stage_ajcc8 IS NOT NULL
       AND e.m_stage_ajcc8 IS NOT NULL AND e.overall_stage_ajcc8 IS NULL)
        AS ajcc8_overall_missing_despite_components
FROM main.canonical_path_malignant_events_v1 e;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('AJCC01','AJCC02','AJCC03');

-- AJCC01: ajcc7 calc_flag true but a component is NULL
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'AJCC01',
    CAST(research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|', CAST(research_id AS VARCHAR), CAST(surgery_date AS VARCHAR),
             CAST(tumor_ordinal AS VARCHAR), CAST(COALESCE(specimen_id, '') AS VARCHAR), 'ajcc7'),
    TO_JSON(struct_pack(
        t_stage_ajcc7 := t_stage_ajcc7,
        n_stage_ajcc7 := n_stage_ajcc7,
        m_stage_ajcc7 := m_stage_ajcc7,
        overall_stage_ajcc7 := overall_stage_ajcc7,
        ajcc7_stage_calculable_flag := ajcc7_stage_calculable_flag
    )),
    'AJCC7 calc_flag=true but TNM component NULL',
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_path_malignant_events_v1
WHERE ajcc7_stage_calculable_flag
  AND (t_stage_ajcc7 IS NULL OR n_stage_ajcc7 IS NULL OR m_stage_ajcc7 IS NULL);

-- AJCC02: ajcc8 calc_flag true but a component is NULL
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'AJCC02',
    CAST(research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|', CAST(research_id AS VARCHAR), CAST(surgery_date AS VARCHAR),
             CAST(tumor_ordinal AS VARCHAR), CAST(COALESCE(specimen_id, '') AS VARCHAR), 'ajcc8'),
    TO_JSON(struct_pack(
        t_stage_ajcc8 := t_stage_ajcc8,
        n_stage_ajcc8 := n_stage_ajcc8,
        m_stage_ajcc8 := m_stage_ajcc8,
        overall_stage_ajcc8 := overall_stage_ajcc8,
        ajcc8_stage_calculable_flag := ajcc8_stage_calculable_flag
    )),
    'AJCC8 calc_flag=true but TNM component NULL',
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_path_malignant_events_v1
WHERE ajcc8_stage_calculable_flag
  AND (t_stage_ajcc8 IS NULL OR n_stage_ajcc8 IS NULL OR m_stage_ajcc8 IS NULL);

-- AJCC03: TNM components all present but overall_stage NULL (ajcc7 OR ajcc8)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'AJCC03',
    CAST(research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|', CAST(research_id AS VARCHAR), CAST(surgery_date AS VARCHAR),
             CAST(tumor_ordinal AS VARCHAR), CAST(COALESCE(specimen_id, '') AS VARCHAR),
             CASE WHEN t_stage_ajcc7 IS NOT NULL AND n_stage_ajcc7 IS NOT NULL
                    AND m_stage_ajcc7 IS NOT NULL AND overall_stage_ajcc7 IS NULL THEN 'ajcc7' ELSE 'ajcc8' END),
    TO_JSON(struct_pack(
        t_stage_ajcc7 := t_stage_ajcc7, n_stage_ajcc7 := n_stage_ajcc7,
        m_stage_ajcc7 := m_stage_ajcc7, overall_stage_ajcc7 := overall_stage_ajcc7,
        t_stage_ajcc8 := t_stage_ajcc8, n_stage_ajcc8 := n_stage_ajcc8,
        m_stage_ajcc8 := m_stage_ajcc8, overall_stage_ajcc8 := overall_stage_ajcc8
    )),
    CASE
        WHEN t_stage_ajcc7 IS NOT NULL AND n_stage_ajcc7 IS NOT NULL
          AND m_stage_ajcc7 IS NOT NULL AND overall_stage_ajcc7 IS NULL
            THEN 'AJCC7 T/N/M all present but overall_stage_ajcc7 NULL'
        ELSE 'AJCC8 T/N/M all present but overall_stage_ajcc8 NULL'
    END,
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_path_malignant_events_v1
WHERE (t_stage_ajcc7 IS NOT NULL AND n_stage_ajcc7 IS NOT NULL
         AND m_stage_ajcc7 IS NOT NULL AND overall_stage_ajcc7 IS NULL)
   OR (t_stage_ajcc8 IS NOT NULL AND n_stage_ajcc8 IS NOT NULL
         AND m_stage_ajcc8 IS NOT NULL AND overall_stage_ajcc8 IS NULL);

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.ajcc7_stage_calculable_flag IS
'Boolean indicator that AJCC7 TNM components SHOULD be populated. 220 rows violate (flag=true but component NULL) — AJCC01. See manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.ajcc8_stage_calculable_flag IS
'Boolean indicator that AJCC8 TNM components SHOULD be populated. 55 rows violate — AJCC02. See manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_19';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.(ajcc7_stage_calculable_flag,t/n/m_stage_ajcc7,overall_stage_ajcc7)','column_group',
   'manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag',
   'AJCC01,AJCC03','prompt_19','column_only',DATE '2026-04-23',
   '220 rows with calc_flag=true but component NULL (AJCC01); 384 rows with components present but overall NULL (AJCC03 ajcc7 variant).',
   NULL,
   '2 flag columns (calc_flag_inconsistent, overall_missing_despite_components) surface violations.'),
  ('main.canonical_path_malignant_events_v1.(ajcc8_stage_calculable_flag,t/n/m_stage_ajcc8,overall_stage_ajcc8)','column_group',
   'manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag',
   'AJCC02,AJCC03','prompt_19','column_only',DATE '2026-04-23',
   '55 rows with calc_flag=true but component NULL (AJCC02); 592 rows with components present but overall NULL (AJCC03 ajcc8 variant).',
   NULL,
   '2 flag columns surface violations. AJCC03 queue emits one row per event covering whichever system (ajcc7/ajcc8) violates.');
