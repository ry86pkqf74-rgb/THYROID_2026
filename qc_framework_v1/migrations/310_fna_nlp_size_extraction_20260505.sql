-- =============================================================================
-- mig_310 v2: FNA NLP size extraction via HP-note keyword corpus
-- Date: 2026-05-05  (v2 supersedes v1 same date)
-- Closes: CF-FNA-SIZE-CM-NULL
-- Author: cursor_composer_mig310_v2
-- =============================================================================
-- v2 Correction (probe 2026-05-05)
-- ---------------------------------
-- v1 assumed a note_type='FNA_CYTOLOGY' corpus in clinical_notes_long.
-- Probe result: no FNA-typed notes exist anywhere.  Corpus:
--   MotherDuck clinical_notes_long top types: hp 2810, opnote 857,
--   other_history 106, endocrine_fm 77, dc_sum 24
--   Snowflake CLINICAL_NOTES_SEARCH_V1 top types: OPNOTE 4727, HP 4280, …
-- FNA cytology text is *embedded* in HP notes.  v2 uses a keyword-relevance
-- filter on HP/OPNOTE/ENDOCRINE_FM/OTHER_HISTORY, then links each canonical
-- FNA event to its nearest in-time high-relevance note (≤60 days).
--
-- Execution: driven by Python script
--   snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md [--pilot]
-- Direct SQL below is for reference / manual re-run only.
-- =============================================================================

USE DATABASE THYROID_VALIDATION;
USE SCHEMA PUBLIC;

-- ---------------------------------------------------------------------------
-- Step 0 — Reference: probe note_type distribution
-- ---------------------------------------------------------------------------
-- SELECT DISTINCT NOTE_TYPE, COUNT(*) AS n
--   FROM CLINICAL_NOTES_SEARCH_V1
--  GROUP BY 1 ORDER BY 2 DESC;

-- ---------------------------------------------------------------------------
-- Step 1 — Working table (v2: includes FNA_EVENT_ID for event-level tracking)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FNA_NOTES_MIG310_V2 (
    RESEARCH_ID   VARCHAR,
    FNA_EVENT_ID  VARCHAR,   -- canonical FNA event key from linkage view
    NOTE_TYPE     VARCHAR,
    NOTE_DATE     VARCHAR,   -- kept as VARCHAR to match source (mixed format)
    NOTE_TEXT     VARCHAR
);

-- ---------------------------------------------------------------------------
-- Step 2 — Core extraction: 4 EXTRACT_ANSWER calls per note
--   Adds bethesda_match vs v1 (was 3 fields; now 4)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE NLP_FNA_SIZE_FULL_RESULTS_v1 AS
WITH
extracted AS (
    SELECT
        RESEARCH_ID,
        FNA_EVENT_ID,
        NOTE_TYPE,
        NOTE_DATE,

        -- Size: largest nodule dimension in cm
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'What is the size (largest dimension) of the aspirated thyroid nodule '
            'in centimeters? Provide only the numeric value as a decimal (e.g. 1.5). '
            'Convert mm to cm if needed. If multiple nodules, report the largest. '
            'Return NULL if not stated.'
        )                               AS _size_raw,

        -- Laterality
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'What is the laterality (side) of the thyroid nodule sampled in this '
            'FNA? Answer with exactly one word: right, left, isthmus, or bilateral. '
            'Return NULL if not stated.'
        )                               AS _lat_raw,

        -- Nodule count
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'How many distinct thyroid nodules were sampled in this FNA procedure? '
            'Answer with a whole number; default to 1 if a single nodule is '
            'described. Return NULL if completely unclear.'
        )                               AS _count_raw,

        -- Bethesda (bonus cross-validation field)
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'What is the Bethesda category of the FNA cytology result? '
            'Return the integer (1-6) if explicitly stated in the note. '
            'Examples: Bethesda II or Category II = 2; Bethesda VI = 6. '
            'Return NULL if not mentioned.'
        )                               AS _bethesda_raw,

        CURRENT_TIMESTAMP               AS extracted_at,
        'cortex_extract_answer_mig_310_v2' AS extraction_source

    FROM FNA_NOTES_MIG310_V2
),

parsed AS (
    SELECT
        RESEARCH_ID,
        FNA_EVENT_ID,
        NOTE_TYPE,
        NOTE_DATE,
        extracted_at,
        extraction_source,

        TRY_TO_DOUBLE(
            NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), '')
        )                               AS extracted_size_cm,

        CASE
            WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR))  LIKE '%right%'    THEN 'right'
            WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR))  LIKE '%left%'     THEN 'left'
            WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR))  LIKE '%isthmus%'  THEN 'isthmus'
            WHEN LOWER(TRIM(_lat_raw[0]:answer::VARCHAR))  LIKE '%bilateral%' THEN 'bilateral'
            ELSE NULL
        END                             AS extracted_laterality,

        TRY_TO_NUMBER(
            NULLIF(TRIM(_count_raw[0]:answer::VARCHAR), ''),
            10, 0
        )                               AS extracted_nodule_count,

        TRY_TO_NUMBER(
            NULLIF(TRIM(_bethesda_raw[0]:answer::VARCHAR), ''),
            1, 0
        )                               AS extracted_bethesda,

        CASE
            WHEN _size_raw[0]:score::FLOAT > 0.80
             AND _lat_raw[0]:score::FLOAT  > 0.80  THEN 'high'
            WHEN _size_raw[0]:score::FLOAT > 0.50
              OR _lat_raw[0]:score::FLOAT  > 0.50  THEN 'medium'
            ELSE 'low'
        END                             AS extraction_confidence,

        _size_raw[0]:score::FLOAT       AS size_extract_score,
        _lat_raw[0]:score::FLOAT        AS lat_extract_score,
        _count_raw[0]:score::FLOAT      AS count_extract_score,
        _bethesda_raw[0]:score::FLOAT   AS bethesda_extract_score

    FROM extracted
)

SELECT * FROM parsed;

-- ---------------------------------------------------------------------------
-- Step 3 — Per-event rollup (best record per patient×event)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE NLP_FNA_SIZE_PATIENT_ROLLUP_v1 AS
SELECT
    RESEARCH_ID,
    FNA_EVENT_ID,
    NOTE_DATE                                               AS fna_date,
    FIRST_VALUE(extracted_size_cm) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        ORDER BY
            CASE extraction_confidence WHEN 'high' THEN 2
                                       WHEN 'medium' THEN 1
                                       ELSE 0 END DESC,
            size_extract_score DESC NULLS LAST,
            extracted_size_cm DESC NULLS LAST
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                       AS extracted_size_cm,
    FIRST_VALUE(extracted_laterality) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        ORDER BY
            CASE extraction_confidence WHEN 'high' THEN 2
                                       WHEN 'medium' THEN 1
                                       ELSE 0 END DESC,
            lat_extract_score DESC NULLS LAST
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                       AS extracted_laterality,
    MAX(COALESCE(extracted_nodule_count, 1)) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
    )                                                       AS extracted_nodule_count,
    FIRST_VALUE(extracted_bethesda) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        ORDER BY bethesda_extract_score DESC NULLS LAST
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                       AS extracted_bethesda,
    FIRST_VALUE(extraction_confidence) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
        ORDER BY
            CASE extraction_confidence WHEN 'high' THEN 2
                                       WHEN 'medium' THEN 1
                                       ELSE 0 END DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                       AS extraction_confidence,
    COUNT(*) OVER (PARTITION BY RESEARCH_ID, FNA_EVENT_ID) AS n_notes_aggregated,
    MAX(size_extract_score) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
    )                                                       AS max_size_score,
    MAX(lat_extract_score) OVER (
        PARTITION BY RESEARCH_ID, FNA_EVENT_ID
    )                                                       AS max_lat_score,
    'cortex_extract_answer_mig_310_v2'                      AS extraction_source,
    CURRENT_TIMESTAMP                                       AS rollup_built_at
FROM NLP_FNA_SIZE_FULL_RESULTS_v1
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RESEARCH_ID, FNA_EVENT_ID
    ORDER BY size_extract_score DESC NULLS LAST
) = 1;

-- ---------------------------------------------------------------------------
-- Step 4 — Validation probe (run after creation)
-- ---------------------------------------------------------------------------
-- SELECT
--     COUNT(*)                                                AS total_rows,
--     COUNT(extracted_size_cm)                                AS size_populated,
--     COUNT(extracted_laterality)                             AS lat_populated,
--     ROUND(100.0 * COUNT(extracted_size_cm)   / COUNT(*), 1) AS size_fill_pct,
--     ROUND(100.0 * COUNT(extracted_laterality)/ COUNT(*), 1) AS lat_fill_pct,
--     COUNT_IF(extracted_size_cm BETWEEN 0.1 AND 15.0)        AS size_plausible_n,
--     COUNT_IF(extraction_confidence = 'high')                AS high_conf_n,
--     ROUND(AVG(max_size_score), 3)                           AS avg_size_score,
--     ROUND(AVG(max_lat_score), 3)                            AS avg_lat_score
-- FROM NLP_FNA_SIZE_PATIENT_ROLLUP_v1;

-- ---------------------------------------------------------------------------
-- Step 5 — Signoff (inserted by script after QA pass)
-- ---------------------------------------------------------------------------
-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
-- VALUES ('mig_310', CURRENT_TIMESTAMP, 'cursor_composer_mig310_v2',
--   'mig_310 v2: FNA NLP size extraction via HP-note keyword corpus. '
--   'fna_content_corpus_v1 + fna_event_note_linkage_v1 built in '
--   'manuscript_workspace. SF NLP_FNA_SIZE_FULL_RESULTS_v1 + '
--   'NLP_FNA_SIZE_PATIENT_ROLLUP_v1 via Cortex EXTRACT_ANSWER (4 fields). '
--   'Mirrored to manuscript_workspace.nlp_fna_size_rollup_v1. '
--   'imaging_fna_linkage_v4 with size_score_v4. Closes CF-FNA-SIZE-CM-NULL.');
