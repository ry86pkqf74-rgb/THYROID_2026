-- scripts/17_timeline_rescue_unified_v3.sql
-- Unified timeline rescue view covering all 6 note_entities_* tables
-- with full date provenance taxonomy.
USE thyroid_research_2026;

CREATE OR REPLACE VIEW timeline_rescue_v3_mv AS
WITH all_entities AS (
    -- 1. Genetics
    SELECT
        'genetics' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year) AS genetic_year,
        COALESCE(ps.surg_date) AS surg_date,
        f.fna_date AS fna_date
    FROM thyroid_share.note_entities_genetics e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
    LEFT JOIN genetic_testing gt ON e.research_id = gt.research_id
    LEFT JOIN path_synoptics ps ON e.research_id = ps.research_id
    LEFT JOIN fna_cytology f ON e.research_id = f.research_id

    UNION ALL

    -- 2. Procedures
    SELECT
        'procedures' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        NULL AS genetic_year,
        COALESCE(ps.surg_date) AS surg_date,
        NULL AS fna_date
    FROM thyroid_share.note_entities_procedures e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
    LEFT JOIN path_synoptics ps ON e.research_id = ps.research_id

    UNION ALL

    -- 3. Staging
    SELECT
        'staging' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        NULL AS genetic_year,
        COALESCE(ps.surg_date) AS surg_date,
        NULL AS fna_date
    FROM thyroid_share.note_entities_staging e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
    LEFT JOIN path_synoptics ps ON e.research_id = ps.research_id

    UNION ALL

    -- 4. Complications
    SELECT
        'complications' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        NULL AS genetic_year,
        COALESCE(ps.surg_date) AS surg_date,
        NULL AS fna_date
    FROM thyroid_share.note_entities_complications e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
    LEFT JOIN path_synoptics ps ON e.research_id = ps.research_id

    UNION ALL

    -- 5. Medications
    SELECT
        'medications' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        NULL AS genetic_year,
        NULL AS surg_date,
        NULL AS fna_date
    FROM thyroid_share.note_entities_medications e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id

    UNION ALL

    -- 6. Problem List
    SELECT
        'problem_list' AS entity_type,
        e.research_id,
        e.entity_date,
        e.evidence_span,
        n.note_date,
        NULL AS genetic_year,
        NULL AS surg_date,
        NULL AS fna_date
    FROM thyroid_share.note_entities_problem_list e
    LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
)
SELECT
    entity_type,
    research_id,
    entity_date,
    evidence_span,
    note_date,
    genetic_year,
    surg_date,
    fna_date,
    CASE
        WHEN entity_date IS NOT NULL THEN 'exact_source_date'
        WHEN note_date IS NOT NULL THEN 'inferred_day_level_date'
        WHEN COALESCE(CAST(genetic_year AS VARCHAR), CAST(surg_date AS VARCHAR), CAST(fna_date AS VARCHAR)) IS NOT NULL
             THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    (entity_date IS NOT NULL) AS date_is_source_native_flag,
    (note_date IS NOT NULL AND entity_date IS NULL) AS date_is_inferred_flag,
    (entity_date IS NULL AND note_date IS NULL
     AND COALESCE(CAST(genetic_year AS VARCHAR), CAST(surg_date AS VARCHAR), CAST(fna_date AS VARCHAR)) IS NULL) AS date_requires_manual_review_flag,
    COALESCE(
        TRY_CAST(entity_date AS DATE),
        TRY_CAST(note_date AS DATE),
        TRY_CAST(FORMAT('{}-01-01', genetic_year) AS DATE),
        TRY_CAST(surg_date AS DATE),
        TRY_CAST(fna_date AS DATE)
    ) AS inferred_event_date
FROM all_entities;
