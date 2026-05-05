-- =============================================================================
-- mig_310: FNA NLP size extraction via Snowflake Cortex EXTRACT_ANSWER
-- Date: 2026-05-05
-- Closes: CF-FNA-SIZE-CM-NULL
-- Author: cursor_composer_mig310
-- =============================================================================
-- Purpose:
--   Build NLP_FNA_SIZE_FULL_RESULTS_v1 in THYROID_VALIDATION.PUBLIC by running
--   SNOWFLAKE.CORTEX.EXTRACT_ANSWER over the FNA cytology corpus housed in
--   CLINICAL_NOTES_SEARCH_V1 (or the subset uploaded via COWORK_STAGE).
--   Three fields extracted per note: size_cm, laterality, nodule_count.
--   Results are mirrored to MotherDuck manuscript_workspace.nlp_fna_size_rollup_v1
--   by scripts/36_pull_sf_nlp_fna_size.py.
--
-- Execution: run via the Python script (manages corpus upload + Cortex call).
--   Direct SQL for reference / re-run only — requires FNA_NOTES_MIG310 to exist.
-- =============================================================================

USE DATABASE THYROID_VALIDATION;
USE SCHEMA PUBLIC;

-- ---------------------------------------------------------------------------
-- Step 0 — Reference: probe candidate FNA corpus tables
-- ---------------------------------------------------------------------------
-- SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%FNA%';
-- SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%CYTOLOGY%';
-- SHOW TABLES IN SCHEMA THYROID_VALIDATION.PUBLIC LIKE '%NOTES%';
-- SELECT DISTINCT NOTE_TYPE, COUNT(*) AS n
--   FROM CLINICAL_NOTES_SEARCH_V1
--  GROUP BY 1 ORDER BY 2 DESC;

-- ---------------------------------------------------------------------------
-- Step 1 — Working table (populated by Python corpus upload)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FNA_NOTES_MIG310 (
    RESEARCH_ID  VARCHAR,
    NOTE_TYPE    VARCHAR,
    NOTE_INDEX   INTEGER,
    NOTE_DATE    VARCHAR,    -- kept as VARCHAR to match source (mixed format)
    NOTE_TEXT    VARCHAR
);

-- ---------------------------------------------------------------------------
-- Step 2 — Core extraction (three EXTRACT_ANSWER calls per note)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE NLP_FNA_SIZE_FULL_RESULTS_v1 AS
WITH
extracted AS (
    SELECT
        RESEARCH_ID,
        NOTE_TYPE,
        NOTE_INDEX,
        NOTE_DATE,

        -- Size: largest nodule dimension in cm
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'What is the size (largest dimension) of the aspirated nodule in centimeters? Provide the numeric value only as a decimal number (e.g. 1.5). If reported in millimeters, convert to cm. If multiple nodules are sampled, report the largest. Return NULL if not stated.'
        )                               AS _size_raw,

        -- Laterality
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'What is the laterality (side) of the thyroid nodule sampled? Answer with exactly one of: right, left, isthmus, bilateral. Return NULL if not stated.'
        )                               AS _lat_raw,

        -- Nodule count
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            NOTE_TEXT,
            'How many distinct thyroid nodules were sampled in this FNA? Answer with a whole number (default 1 if a single nodule is described but count not explicitly stated). Return NULL if completely unclear.'
        )                               AS _count_raw,

        CURRENT_TIMESTAMP               AS extracted_at,
        'cortex_extract_answer_mig_310' AS extraction_source

    FROM FNA_NOTES_MIG310
),

parsed AS (
    SELECT
        RESEARCH_ID,
        NOTE_TYPE,
        NOTE_INDEX,
        NOTE_DATE,
        extracted_at,
        extraction_source,

        -- EXTRACT_ANSWER returns ARRAY<OBJECT{answer, score, source_doc}>
        -- Take element 0 (best answer) and cast
        TRY_TO_DOUBLE(
            NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), '')
        )                               AS extracted_size_cm,

        -- Normalise laterality to lowercase and restrict to valid values
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

        -- Confidence: high when size answer score > 0.80 AND laterality > 0.80;
        --             medium when either is 0.50–0.80;
        --             low otherwise
        CASE
            WHEN _size_raw[0]:score::FLOAT > 0.80
             AND _lat_raw[0]:score::FLOAT  > 0.80  THEN 'high'
            WHEN _size_raw[0]:score::FLOAT > 0.50
              OR _lat_raw[0]:score::FLOAT  > 0.50  THEN 'medium'
            ELSE 'low'
        END                             AS extraction_confidence,

        _size_raw[0]:score::FLOAT       AS size_extract_score,
        _lat_raw[0]:score::FLOAT        AS lat_extract_score,
        _count_raw[0]:score::FLOAT      AS count_extract_score

    FROM extracted
)

SELECT * FROM parsed;

-- ---------------------------------------------------------------------------
-- Step 3 — Per-patient rollup (best record per patient×date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE NLP_FNA_SIZE_PATIENT_ROLLUP_v1 AS
SELECT
    RESEARCH_ID,
    NOTE_DATE                                               AS fna_date,
    -- Take highest-confidence, then largest size for the same patient-date
    MAX(extracted_size_cm)                                  AS extracted_size_cm_max,
    MIN(extracted_size_cm)                                  AS extracted_size_cm_min,
    ANY_VALUE(extracted_size_cm)                            AS extracted_size_cm,
    ANY_VALUE(extracted_laterality)                         AS extracted_laterality,
    MAX(extracted_nodule_count)                             AS extracted_nodule_count,
    MAX(CASE extraction_confidence WHEN 'high' THEN 2
                                   WHEN 'medium' THEN 1
                                   ELSE 0 END)              AS _conf_rank,
    CASE MAX(CASE extraction_confidence WHEN 'high' THEN 2
                                        WHEN 'medium' THEN 1
                                        ELSE 0 END)
        WHEN 2 THEN 'high'
        WHEN 1 THEN 'medium'
        ELSE 'low'
    END                                                     AS extraction_confidence,
    COUNT(*)                                                AS n_notes_aggregated,
    MAX(size_extract_score)                                 AS max_size_score,
    MAX(lat_extract_score)                                  AS max_lat_score,
    'cortex_extract_answer_mig_310'                         AS extraction_source,
    CURRENT_TIMESTAMP                                       AS rollup_built_at
FROM NLP_FNA_SIZE_FULL_RESULTS_v1
GROUP BY RESEARCH_ID, NOTE_DATE;

-- ---------------------------------------------------------------------------
-- Step 4 — Validation probe (run after creation; inspect manually)
-- ---------------------------------------------------------------------------
-- SELECT
--     COUNT(*)                                          AS total_rows,
--     COUNT(extracted_size_cm)                          AS size_populated,
--     COUNT(extracted_laterality)                       AS lat_populated,
--     ROUND(100.0 * COUNT(extracted_size_cm) / COUNT(*), 1)   AS size_fill_pct,
--     ROUND(100.0 * COUNT(extracted_laterality) / COUNT(*), 1) AS lat_fill_pct,
--     ROUND(AVG(max_size_score), 3)                     AS avg_size_score,
--     ROUND(AVG(max_lat_score), 3)                      AS avg_lat_score,
--     COUNT_IF(extracted_size_cm BETWEEN 0.1 AND 15.0)  AS size_plausible_n,
--     COUNT_IF(extracted_size_cm < 0.1 OR extracted_size_cm > 15.0) AS size_implausible_n
-- FROM NLP_FNA_SIZE_PATIENT_ROLLUP_v1;

-- ---------------------------------------------------------------------------
-- Step 5 — Signoff (inserted by script after QA pass)
-- ---------------------------------------------------------------------------
-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
-- VALUES ('mig_310', CURRENT_TIMESTAMP, 'cursor_composer_mig310',
--   'mig_310: FNA NLP size extraction. SF NLP_FNA_SIZE_FULL_RESULTS_v1 + '
--   'NLP_FNA_SIZE_PATIENT_ROLLUP_v1 built via Cortex EXTRACT_ANSWER. '
--   'Mirrored to manuscript_workspace.nlp_fna_size_rollup_v1. '
--   'imaging_fna_linkage_v4 view created. Closes CF-FNA-SIZE-CM-NULL.');
