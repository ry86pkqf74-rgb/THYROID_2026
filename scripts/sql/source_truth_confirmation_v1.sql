-- source_truth_confirmation_v1.sql
-- Non-destructive views for fail-closed completeness classification.
-- Deploy: scripts/151_source_truth_confirmation_v1.py --md

-- 1) Bethesda: COALESCE(episode, fna_cytology) + explicit unscorable reason for remainder
CREATE OR REPLACE VIEW v_fna_episode_bethesda_resolved_v1 AS
WITH cy AS (
    SELECT
        research_id,
        fna_index,
        category_num,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, fna_index
            ORDER BY confidence DESC NULLS LAST, ingested_at_utc DESC NULLS LAST
        ) AS rn
    FROM fna_cytology
)
SELECT
    e.research_id,
    e.fna_episode_id,
    e.resolved_fna_date,
    e.bethesda_raw,
    e.bethesda_category AS bethesda_episode_num,
    c.category_num AS bethesda_cytology_num,
    COALESCE(e.bethesda_category, c.category_num) AS bethesda_resolved_num,
    CASE
        WHEN e.bethesda_category IS NOT NULL THEN 'fna_episode_master_v2'
        WHEN c.category_num IS NOT NULL THEN 'fna_cytology_join'
        ELSE NULL
    END AS bethesda_value_source,
    CASE
        WHEN COALESCE(e.bethesda_category, c.category_num) IS NOT NULL THEN NULL
        WHEN e.bethesda_raw IS NOT NULL
             AND TRIM(CAST(e.bethesda_raw AS VARCHAR)) <> ''
             AND TRY_CAST(TRIM(CAST(e.bethesda_raw AS VARCHAR)) AS INTEGER) IS NULL
            THEN 'non_numeric_or_asterisk_bethesda_raw'
        WHEN e.pathology_diagnosis IS NOT NULL
             AND TRIM(CAST(e.pathology_diagnosis AS VARCHAR)) <> ''
            THEN 'pathology_present_bethesda_unparsed'
        ELSE 'no_episode_or_cytology_bethesda'
    END AS bethesda_unscorable_reason
FROM fna_episode_master_v2 e
LEFT JOIN cy c
    ON CAST(e.research_id AS BIGINT) = CAST(c.research_id AS BIGINT)
   AND CAST(e.fna_episode_id AS BIGINT) = CAST(c.fna_index AS BIGINT)
   AND c.rn = 1;


-- 2) Imaging nodule linkage: exhaustive classification (every row in imaging_nodule_master_v1)
CREATE OR REPLACE VIEW v_imaging_nodule_linkage_classification_v1 AS
WITH
prim AS (
    SELECT nodule_id, MAX(fna_episode_id) AS fna_episode_id
    FROM imaging_fna_linkage_mm_v1
    WHERE is_primary_link
    GROUP BY 1
),
pfna AS (
    SELECT research_id, COUNT(*) AS n_fna
    FROM fna_episode_master_v2
    WHERE resolved_fna_date IS NOT NULL
    GROUP BY 1
),
fs AS (
    SELECT
        research_id,
        MIN(TRY_CAST(surg_date AS DATE)) AS first_surg
    FROM path_synoptics
    WHERE surg_date IS NOT NULL
      AND TRIM(CAST(surg_date AS VARCHAR)) <> ''
    GROUP BY 1
),
img AS (
    SELECT
        nodule_id,
        research_id,
        exam_id,
        CAST(exam_date AS DATE) AS exam_d
    FROM imaging_nodule_master_v1
),
enriched AS (
    SELECT
        i.nodule_id,
        i.research_id,
        i.exam_id,
        i.exam_d,
        p.fna_episode_id AS primary_linked_fna,
        COALESCE(pf.n_fna, 0) AS n_fna_episodes_patient,
        fs.first_surg,
        EXISTS (
            SELECT 1
            FROM fna_episode_master_v2 f
            WHERE CAST(f.research_id AS BIGINT) = CAST(i.research_id AS BIGINT)
              AND f.resolved_fna_date IS NOT NULL
              AND i.exam_d IS NOT NULL
              AND f.resolved_fna_date >= i.exam_d
              AND DATEDIFF('day', i.exam_d, f.resolved_fna_date) BETWEEN 0 AND 90
        ) AS has_fna_0_90d_after_exam,
        NOT EXISTS (
            SELECT 1
            FROM fna_episode_master_v2 f
            WHERE CAST(f.research_id AS BIGINT) = CAST(i.research_id AS BIGINT)
              AND f.resolved_fna_date IS NOT NULL
              AND i.exam_d IS NOT NULL
              AND f.resolved_fna_date >= i.exam_d
        ) AS all_fna_strictly_before_exam,
        EXISTS (
            SELECT 1
            FROM fna_episode_master_v2 f
            WHERE CAST(f.research_id AS BIGINT) = CAST(i.research_id AS BIGINT)
              AND f.resolved_fna_date IS NOT NULL
              AND i.exam_d IS NOT NULL
              AND f.resolved_fna_date >= i.exam_d
              AND DATEDIFF('day', i.exam_d, f.resolved_fna_date) > 90
        ) AS has_fna_after_exam_beyond_90d
    FROM img i
    LEFT JOIN prim p ON i.nodule_id = p.nodule_id
    LEFT JOIN pfna pf ON CAST(i.research_id AS BIGINT) = CAST(pf.research_id AS BIGINT)
    LEFT JOIN fs ON CAST(i.research_id AS BIGINT) = CAST(fs.research_id AS BIGINT)
)
SELECT
    nodule_id,
    research_id,
    exam_id,
    exam_d,
    primary_linked_fna,
    CASE
        WHEN primary_linked_fna IS NOT NULL THEN 'linked_to_fna'
        WHEN COALESCE(n_fna_episodes_patient, 0) = 0
            THEN 'no_eligible_fna'
        WHEN exam_d IS NOT NULL AND first_surg IS NOT NULL AND exam_d > first_surg
            THEN 'no_eligible_fna'
        WHEN all_fna_strictly_before_exam
            THEN 'no_eligible_fna'
        WHEN has_fna_0_90d_after_exam
            THEN 'unresolved_linkage_gap'
        WHEN has_fna_after_exam_beyond_90d AND NOT COALESCE(has_fna_0_90d_after_exam, FALSE)
            THEN 'no_eligible_fna'
        ELSE 'unresolved_linkage_gap'
    END AS linkage_state,
    CASE
        WHEN primary_linked_fna IS NOT NULL THEN 'primary_row_in_imaging_fna_linkage_mm_v1'
        WHEN COALESCE(n_fna_episodes_patient, 0) = 0 THEN 'patient_has_no_dated_fna_episode'
        WHEN exam_d IS NOT NULL AND first_surg IS NOT NULL AND exam_d > first_surg THEN 'index_us_after_first_surgery'
        WHEN all_fna_strictly_before_exam THEN 'all_fna_before_index_us_exam'
        WHEN has_fna_0_90d_after_exam THEN 'candidate_fna_in_90d_window_but_no_mm_link'
        WHEN has_fna_after_exam_beyond_90d AND NOT COALESCE(has_fna_0_90d_after_exam, FALSE)
            THEN 'only_fna_beyond_90d_after_index_us'
        ELSE 'ambiguous_requires_manual_review'
    END AS linkage_reason_code
FROM enriched;


-- 3) TI-RADS: sufficient ACR criteria but null stored scores (should be zero)
CREATE OR REPLACE VIEW v_imaging_nodule_tirads_gap_v1 AS
SELECT
    m.nodule_id,
    m.research_id,
    m.exam_date,
    (
        (CASE WHEN m.composition IS NOT NULL AND TRIM(CAST(m.composition AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
      + (CASE WHEN m.echogenicity IS NOT NULL AND TRIM(CAST(m.echogenicity AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
      + (CASE WHEN m.shape IS NOT NULL AND TRIM(CAST(m.shape AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
      + (CASE WHEN m.margins IS NOT NULL AND TRIM(CAST(m.margins AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
      + (CASE WHEN m.calcifications IS NOT NULL AND TRIM(CAST(m.calcifications AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
    ) AS n_populated_acr_fields,
    m.tirads_reported,
    m.tirads_acr_recalculated,
    CASE
        WHEN (
            (CASE WHEN m.composition IS NOT NULL AND TRIM(CAST(m.composition AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
          + (CASE WHEN m.echogenicity IS NOT NULL AND TRIM(CAST(m.echogenicity AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
          + (CASE WHEN m.shape IS NOT NULL AND TRIM(CAST(m.shape AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
          + (CASE WHEN m.margins IS NOT NULL AND TRIM(CAST(m.margins AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
          + (CASE WHEN m.calcifications IS NOT NULL AND TRIM(CAST(m.calcifications AS VARCHAR)) <> '' THEN 1 ELSE 0 END)
        ) >= 5
         AND m.tirads_reported IS NULL
         AND m.tirads_acr_recalculated IS NULL
            THEN TRUE
        ELSE FALSE
    END AS gap_sufficient_features_null_scores
FROM imaging_nodule_master_v1 m;


-- 4) Scope: single-row KPI for COMPLETE workbook parity (expect 19891 = 19891)
CREATE OR REPLACE VIEW v_canonical_us_nodule_scope_v1 AS
SELECT
    'COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx→raw_us_tirads_excel_v1→imaging_nodule_master_v1'::VARCHAR AS canonical_corpus,
    COUNT(*)::BIGINT AS n_nodule_rows,
    COUNT(DISTINCT source_table)::BIGINT AS n_distinct_source_table,
    MIN(source_table)::VARCHAR AS source_table_example
FROM imaging_nodule_master_v1;
