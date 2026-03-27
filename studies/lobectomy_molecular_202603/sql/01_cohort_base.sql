-- Lobectomy vs total thyroidectomy analytic cohort
-- Database: thyroid_master.duckdb (local DuckDB)
--
-- Inclusion:
--   * First operative_episode_detail_v2 row only; procedure hemithyroidectomy OR total_thyroidectomy.
--   * Dominant nodule 2.0–4.0 cm: MAX(imaging_nodule_master_v1.max_dimension_cm) on exams with
--     exam_date <= surgery_anchor, else fallback patient.imaging_nodule_size_cm.
--   * No preoperative CT/MRI with pathologic_lymph_nodes = TRUE (exam date <= surgery_anchor).
--   * No distant disease: path_m_stage_raw M1 (common variants) OR metastatic histology text.
--
-- Excludes unknown/other procedure_normalized rows from first_ep join (clean lobectomy vs TT).

WITH first_op AS (
    SELECT
        research_id,
        procedure_normalized,
        procedure_raw,
        COALESCE(
            TRY_CAST(resolved_surgery_date AS DATE),
            surgery_date_native
        ) AS sx_date_episode,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native) NULLS LAST
        ) AS rn
    FROM operative_episode_detail_v2
),
first_ep AS (
    SELECT *
    FROM first_op
    WHERE rn = 1
        AND procedure_normalized IN ('hemithyroidectomy', 'total_thyroidectomy')
),
cohort_spine AS (
    SELECT
        p.research_id,
        fe.procedure_normalized AS first_procedure,
        fe.procedure_raw AS first_procedure_raw,
        COALESCE(fe.sx_date_episode, p.surg_first_date, p.first_surgery_date) AS surgery_anchor,
        p.age_at_surgery,
        p.sex,
        p.race,
        p.fna_bethesda_final,
        p.histology_final,
        p.ata_risk_category,
        p.path_m_stage_raw,
        p.path_tumor_size_cm,
        p.imaging_nodule_size_cm,
        p.mol_platform,
        CASE WHEN fe.procedure_normalized = 'total_thyroidectomy' THEN 1 ELSE 0 END AS total_thyroidectomy
    FROM patient_analysis_resolved_v1 AS p
    INNER JOIN first_ep AS fe ON p.research_id = fe.research_id
    WHERE COALESCE(p.surg_first_date, p.first_surgery_date) IS NOT NULL
),
preop_imaging_max AS (
    SELECT
        i.research_id,
        MAX(i.max_dimension_cm) AS preop_imaging_max_cm
    FROM imaging_nodule_master_v1 AS i
    INNER JOIN cohort_spine AS c ON i.research_id = c.research_id
    WHERE i.exam_date IS NOT NULL
        AND c.surgery_anchor IS NOT NULL
        AND i.exam_date <= c.surgery_anchor
    GROUP BY i.research_id
),
ct_preop_ln_flag AS (
    SELECT DISTINCT TRY_CAST(ct.research_id AS BIGINT) AS research_id
    FROM ct_imaging AS ct
    INNER JOIN cohort_spine AS c ON TRY_CAST(ct.research_id AS BIGINT) = c.research_id
    WHERE ct.pathologic_lymph_nodes IS TRUE
        AND TRY_CAST(ct.date_of_exam AS DATE) IS NOT NULL
        AND c.surgery_anchor IS NOT NULL
        AND TRY_CAST(ct.date_of_exam AS DATE) <= c.surgery_anchor
),
mri_preop_ln_flag AS (
    SELECT DISTINCT TRY_CAST(mr.research_id AS BIGINT) AS research_id
    FROM mri_imaging AS mr
    INNER JOIN cohort_spine AS c ON TRY_CAST(mr.research_id AS BIGINT) = c.research_id
    WHERE mr.pathologic_lymph_nodes IS TRUE
        AND TRY_CAST(mr.date_of_exam AS DATE) IS NOT NULL
        AND c.surgery_anchor IS NOT NULL
        AND TRY_CAST(mr.date_of_exam AS DATE) <= c.surgery_anchor
),
tumor_ranked AS (
    SELECT
        research_id,
        LOWER(COALESCE(procedure_raw, '')) AS pr,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY surgery_date NULLS LAST, surgery_episode_id
        ) AS rn
    FROM tumor_episode_master_v2
    WHERE surgery_date IS NOT NULL
),
completion_infer AS (
    SELECT
        t1.research_id,
        TRUE AS completion_after_initial_lobe
    FROM tumor_ranked AS t1
    INNER JOIN tumor_ranked AS t2
        ON t1.research_id = t2.research_id AND t1.rn = 1 AND t2.rn = 2
    WHERE (
        t1.pr LIKE '%hemithyroid%'
        OR t1.pr LIKE '%thyroid lobectomy%'
        OR t1.pr LIKE '%lobectomy%'
        OR t1.pr LIKE '%(rl)%'
        OR t1.pr LIKE '%(ll)%'
    )
        AND t1.pr NOT LIKE '%total%'
        AND (
            t2.pr LIKE '%completion%'
            OR t2.pr LIKE '%total thyroid%'
            OR t2.pr LIKE '%total thyroidectomy%'
        )
),
preop_mol AS (
    SELECT
        m.research_id,
        m.platform,
        m.overall_result_class,
        m.result AS molecular_result_text,
        m.high_risk_marker_flag,
        ROW_NUMBER() OVER (
            PARTITION BY m.research_id
            ORDER BY COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) DESC
        ) AS rn
    FROM molecular_test_episode_v2 AS m
    INNER JOIN cohort_spine AS c ON m.research_id = c.research_id
    WHERE m.platform IN ('ThyroSeq', 'Afirma')
        AND COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) IS NOT NULL
        AND c.surgery_anchor IS NOT NULL
        AND COALESCE(TRY_CAST(m.resolved_test_date AS DATE), m.test_date_native) < c.surgery_anchor
),
preop_mol_one AS (
    SELECT * FROM preop_mol WHERE rn = 1
),
assembled AS (
    SELECT
        c.research_id,
        c.surgery_anchor,
        c.first_procedure,
        c.first_procedure_raw,
        c.total_thyroidectomy,
        c.age_at_surgery,
        c.sex,
        c.race,
        c.fna_bethesda_final,
        c.histology_final,
        c.ata_risk_category,
        c.path_m_stage_raw,
        c.path_tumor_size_cm,
        c.imaging_nodule_size_cm,
        c.mol_platform AS patient_mol_platform_resolved,
        img.preop_imaging_max_cm,
        COALESCE(img.preop_imaging_max_cm, c.imaging_nodule_size_cm) AS exact_size_cm_primary,
        CASE
            WHEN img.preop_imaging_max_cm IS NOT NULL THEN 'imaging_nodule_master_preop'
            WHEN c.imaging_nodule_size_cm IS NOT NULL THEN 'patient_imaging_nodule_size_cm'
            ELSE NULL
        END AS size_primary_source,
        c.path_tumor_size_cm AS exact_size_cm_path_sensitivity,
        pm.platform AS preop_molecular_platform,
        pm.overall_result_class AS preop_result_class,
        pm.molecular_result_text,
        pm.high_risk_marker_flag AS preop_high_risk_marker_raw,
        COALESCE(ci.completion_after_initial_lobe, FALSE) AS completion_after_initial_lobe,
        (ct.research_id IS NOT NULL) AS ct_preop_path_ln_positive,
        (mr.research_id IS NOT NULL) AS mri_preop_path_ln_positive,
        (
            UPPER(TRIM(COALESCE(c.path_m_stage_raw, ''))) IN ('M1', '1')
            OR LOWER(TRIM(COALESCE(c.path_m_stage_raw, ''))) = 'm1'
        ) AS distant_m_stage_flag,
        (LOWER(COALESCE(c.histology_final, '')) LIKE '%metastatic%') AS metastatic_histology_flag
    FROM cohort_spine AS c
    LEFT JOIN preop_imaging_max AS img ON c.research_id = img.research_id
    LEFT JOIN preop_mol_one AS pm ON c.research_id = pm.research_id
    LEFT JOIN completion_infer AS ci ON c.research_id = ci.research_id
    LEFT JOIN ct_preop_ln_flag AS ct ON c.research_id = ct.research_id
    LEFT JOIN mri_preop_ln_flag AS mr ON c.research_id = mr.research_id
)
SELECT *
FROM assembled
WHERE exact_size_cm_primary BETWEEN 2.0 AND 4.0
    AND NOT ct_preop_path_ln_positive
    AND NOT mri_preop_path_ln_positive
    AND NOT distant_m_stage_flag
    AND NOT metastatic_histology_flag;
