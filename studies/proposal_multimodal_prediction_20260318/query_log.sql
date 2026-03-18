-- [exists_master_cohort]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'master_cohort' AND table_schema = 'main'

-- [count_master_cohort]
SELECT COUNT(*) AS n FROM master_cohort

-- [cols_master_cohort]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'master_cohort' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_manuscript_cohort_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'manuscript_cohort_v1' AND table_schema = 'main'

-- [count_manuscript_cohort_v1]
SELECT COUNT(*) AS n FROM manuscript_cohort_v1

-- [cols_manuscript_cohort_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'manuscript_cohort_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_analysis_cancer_cohort_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'analysis_cancer_cohort_v1' AND table_schema = 'main'

-- [count_analysis_cancer_cohort_v1]
SELECT COUNT(*) AS n FROM analysis_cancer_cohort_v1

-- [cols_analysis_cancer_cohort_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'analysis_cancer_cohort_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_patient_analysis_resolved_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'patient_analysis_resolved_v1' AND table_schema = 'main'

-- [count_patient_analysis_resolved_v1]
SELECT COUNT(*) AS n FROM patient_analysis_resolved_v1

-- [cols_patient_analysis_resolved_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'patient_analysis_resolved_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_episode_analysis_resolved_v1_dedup]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'episode_analysis_resolved_v1_dedup' AND table_schema = 'main'

-- [count_episode_analysis_resolved_v1_dedup]
SELECT COUNT(*) AS n FROM episode_analysis_resolved_v1_dedup

-- [cols_episode_analysis_resolved_v1_dedup]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'episode_analysis_resolved_v1_dedup' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_imaging_nodule_master_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'imaging_nodule_master_v1' AND table_schema = 'main'

-- [count_imaging_nodule_master_v1]
SELECT COUNT(*) AS n FROM imaging_nodule_master_v1

-- [cols_imaging_nodule_master_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'imaging_nodule_master_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_imaging_patient_summary_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'imaging_patient_summary_v1' AND table_schema = 'main'

-- [count_imaging_patient_summary_v1]
SELECT COUNT(*) AS n FROM imaging_patient_summary_v1

-- [cols_imaging_patient_summary_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'imaging_patient_summary_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_extracted_tirads_validated_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'extracted_tirads_validated_v1' AND table_schema = 'main'

-- [count_extracted_tirads_validated_v1]
SELECT COUNT(*) AS n FROM extracted_tirads_validated_v1

-- [cols_extracted_tirads_validated_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'extracted_tirads_validated_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_operative_episode_detail_v2]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'operative_episode_detail_v2' AND table_schema = 'main'

-- [count_operative_episode_detail_v2]
SELECT COUNT(*) AS n FROM operative_episode_detail_v2

-- [cols_operative_episode_detail_v2]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'operative_episode_detail_v2' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_molecular_test_episode_v2]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'molecular_test_episode_v2' AND table_schema = 'main'

-- [count_molecular_test_episode_v2]
SELECT COUNT(*) AS n FROM molecular_test_episode_v2

-- [cols_molecular_test_episode_v2]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'molecular_test_episode_v2' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_rai_treatment_episode_v2]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'rai_treatment_episode_v2' AND table_schema = 'main'

-- [count_rai_treatment_episode_v2]
SELECT COUNT(*) AS n FROM rai_treatment_episode_v2

-- [cols_rai_treatment_episode_v2]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'rai_treatment_episode_v2' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_longitudinal_lab_canonical_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'longitudinal_lab_canonical_v1' AND table_schema = 'main'

-- [count_longitudinal_lab_canonical_v1]
SELECT COUNT(*) AS n FROM longitudinal_lab_canonical_v1

-- [cols_longitudinal_lab_canonical_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'longitudinal_lab_canonical_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_recurrence_risk_features_mv]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'recurrence_risk_features_mv' AND table_schema = 'main'

-- [count_recurrence_risk_features_mv]
SELECT COUNT(*) AS n FROM recurrence_risk_features_mv

-- [cols_recurrence_risk_features_mv]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'recurrence_risk_features_mv' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_provenance_enriched_events_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'provenance_enriched_events_v1' AND table_schema = 'main'

-- [count_provenance_enriched_events_v1]
SELECT COUNT(*) AS n FROM provenance_enriched_events_v1

-- [cols_provenance_enriched_events_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'provenance_enriched_events_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_patient_refined_master_clinical_v12]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'patient_refined_master_clinical_v12' AND table_schema = 'main'

-- [count_patient_refined_master_clinical_v12]
SELECT COUNT(*) AS n FROM patient_refined_master_clinical_v12

-- [cols_patient_refined_master_clinical_v12]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'patient_refined_master_clinical_v12' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_complication_phenotype_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'complication_phenotype_v1' AND table_schema = 'main'

-- [count_complication_phenotype_v1]
SELECT COUNT(*) AS n FROM complication_phenotype_v1

-- [cols_complication_phenotype_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'complication_phenotype_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_complication_patient_summary_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'complication_patient_summary_v1' AND table_schema = 'main'

-- [count_complication_patient_summary_v1]
SELECT COUNT(*) AS n FROM complication_patient_summary_v1

-- [cols_complication_patient_summary_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'complication_patient_summary_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_extracted_recurrence_refined_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'extracted_recurrence_refined_v1' AND table_schema = 'main'

-- [count_extracted_recurrence_refined_v1]
SELECT COUNT(*) AS n FROM extracted_recurrence_refined_v1

-- [cols_extracted_recurrence_refined_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'extracted_recurrence_refined_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_extracted_fna_bethesda_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'extracted_fna_bethesda_v1' AND table_schema = 'main'

-- [count_extracted_fna_bethesda_v1]
SELECT COUNT(*) AS n FROM extracted_fna_bethesda_v1

-- [cols_extracted_fna_bethesda_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'extracted_fna_bethesda_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [exists_thyroid_scoring_py_v1]
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = 'thyroid_scoring_py_v1' AND table_schema = 'main'

-- [count_thyroid_scoring_py_v1]
SELECT COUNT(*) AS n FROM thyroid_scoring_py_v1

-- [cols_thyroid_scoring_py_v1]
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'thyroid_scoring_py_v1' AND table_schema = 'main'
ORDER BY ordinal_position

-- [cohort_master_cohort_patients]
SELECT COUNT(DISTINCT research_id) FROM master_cohort

-- [cohort_manuscript_cohort_v1_patients]
SELECT COUNT(DISTINCT research_id) FROM manuscript_cohort_v1

-- [cohort_analysis_cancer_cohort_v1_patients]
SELECT COUNT(DISTINCT research_id) FROM analysis_cancer_cohort_v1

-- [cohort_patient_analysis_resolved_v1_patients]
SELECT COUNT(DISTINCT research_id) FROM patient_analysis_resolved_v1

-- [cohort_episode_dedup_episodes]
SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup

-- [cohort_imaging_nodule_master_v1_rows]
SELECT COUNT(*) FROM imaging_nodule_master_v1

-- [cohort_imaging_patient_summary_v1_patients]
SELECT COUNT(DISTINCT research_id) FROM imaging_patient_summary_v1

-- [cohort_extracted_tirads_validated_v1_patients]
SELECT COUNT(DISTINCT research_id) FROM extracted_tirads_validated_v1

-- [cohort_molecular_tested_patients]
SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2

-- [cohort_rai_episodes]
SELECT COUNT(*) FROM rai_treatment_episode_v2

-- [cohort_lab_canonical_rows]
SELECT COUNT(*) FROM longitudinal_lab_canonical_v1

-- [cohort_lab_canonical_patients]
SELECT COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1

-- [cohort_recurrence_risk_features_patients]
SELECT COUNT(DISTINCT research_id) FROM recurrence_risk_features_mv

-- [cohort_provenance_events_rows]
SELECT COUNT(*) FROM provenance_enriched_events_v1

-- [cohort_complication_phenotype_v1_rows]
SELECT COUNT(*) FROM complication_phenotype_v1

-- [cohort_scoring_patients]
SELECT COUNT(DISTINCT research_id) FROM thyroid_scoring_py_v1

-- [analysis_table]
-- One-row-per-patient multimodal analysis table for cancer cohort
WITH cancer AS (
    SELECT DISTINCT research_id
    FROM analysis_cancer_cohort_v1
),
pat AS (
    SELECT
        p.research_id,
        -- Demographics (actual column names from patient_analysis_resolved_v1)
        p.age_at_surgery,
        p.sex,
        p.race,
        -- Core pathology / staging
        p.histology_final,
        p.path_t_stage_raw               AS t_stage,
        p.path_n_stage_raw               AS n_stage,
        p.path_m_stage_raw               AS m_stage,
        p.ete_grade_final                AS ete_grade,
        p.path_multifocal_flag           AS multifocal_flag,
        p.path_tumor_size_cm             AS tumor_size_cm,
        p.path_ln_positive_raw           AS ln_positive_count,
        p.path_ln_examined_raw           AS ln_examined_count,
        p.margin_status_final            AS margin_status,
        p.vascular_invasion_final        AS vascular_invasion,
        -- Molecular
        p.braf_positive_final            AS braf_positive,
        p.ras_positive_final             AS ras_positive,
        p.tert_positive_final            AS tert_positive,
        p.mol_platform                   AS molecular_platform,
        -- Scoring (from resolved layer itself — it already has these)
        p.ajcc8_stage_group              AS ajcc8_stage,
        p.ata_risk_category              AS ata_risk,
        p.macis_score                    AS macis_score,
        p.macis_risk_group,
        p.ames_risk_group,
        p.ages_score,
        p.molecular_risk_tier,
        -- Recurrence (from resolved layer)
        COALESCE(p.any_recurrence_flag, FALSE) AS recurrence_flag,
        p.recurrence_date                AS first_recurrence_date,
        p.structural_recurrence_flag,
        p.biochemical_recurrence_flag,
        -- Complications (from resolved layer)
        p.any_confirmed_complication     AS has_complication_record,
        p.hypocalcemia_status,
        p.rln_status,
        -- Labs (from resolved layer)
        p.tg_nadir,
        p.tg_last_value,
        p.tg_rising_flag,
        p.lab_completeness_score,
        -- Eligibility flags
        p.analysis_eligible_flag,
        p.molecular_eligible_flag,
        p.rai_eligible_flag,
        p.survival_eligible_flag,
        p.scoring_ajcc8_flag,
        p.scoring_ata_flag,
        -- Imaging from resolved layer
        p.imaging_tirads_worst           AS tirads_worst,
        p.imaging_tirads_category        AS tirads_worst_category,
        p.imaging_nodule_size_cm,
        p.imaging_n_nodule_records       AS n_nodules_imaged,
        -- RAI from resolved layer
        p.rai_received_flag              AS has_rai_data,
        p.rai_max_dose_mci,
        -- FNA from resolved layer
        p.fna_bethesda_final             AS bethesda_worst,
        -- Surgery
        p.surg_procedure_type,
        p.surg_n_procedures
    FROM patient_analysis_resolved_v1 p
    WHERE p.research_id IN (SELECT research_id FROM cancer)
),
-- Imaging availability (validated TIRADS — richer detail)
img AS (
    SELECT
        research_id,
        TRUE                             AS has_tirads_validated,
        tirads_worst_score               AS tirads_validated_worst,
        n_sources                        AS tirads_n_sources,
        nodule_size_max_mm               AS tirads_nodule_max_mm,
        concordant_count                 AS tirads_concordant_ct,
        mismatch_count                   AS tirads_mismatch_ct
    FROM extracted_tirads_validated_v1
),
-- Imaging patient summary (from nodule master)
img_sum AS (
    SELECT
        research_id,
        TRUE                             AS has_nodule_master
    FROM imaging_patient_summary_v1
    GROUP BY research_id
),
-- Molecular availability
mol AS (
    SELECT
        research_id,
        TRUE                             AS has_molecular_episode,
        COUNT(*)                         AS n_molecular_tests
    FROM molecular_test_episode_v2
    GROUP BY research_id
),
-- Lab availability
lab AS (
    SELECT
        research_id,
        TRUE                             AS has_labs,
        COUNT(*)                         AS n_lab_values,
        COUNT(DISTINCT analyte_group)    AS n_analyte_groups
    FROM longitudinal_lab_canonical_v1
    GROUP BY research_id
),
-- FNA detail
fna AS (
    SELECT
        research_id,
        TRUE                             AS has_fna_bethesda
    FROM extracted_fna_bethesda_v1
    GROUP BY research_id
)
SELECT
    pat.*,
    -- Imaging flags (validated TIRADS enrichment)
    COALESCE(img.has_tirads_validated, FALSE)     AS has_tirads_validated,
    img.tirads_validated_worst,
    img.tirads_n_sources,
    img.tirads_nodule_max_mm,
    COALESCE(img_sum.has_nodule_master, FALSE)    AS has_nodule_master,
    -- Molecular episode flags
    COALESCE(mol.has_molecular_episode, FALSE)    AS has_molecular_data,
    mol.n_molecular_tests,
    -- Lab flags
    COALESCE(lab.has_labs, FALSE)                  AS has_lab_data,
    lab.n_lab_values,
    lab.n_analyte_groups,
    -- FNA flags
    COALESCE(fna.has_fna_bethesda, FALSE)         AS has_fna_data,
    -- Modality summary
    CASE
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE) AND COALESCE(lab.has_labs, FALSE)
        THEN 'all_three'
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'imaging_and_molecular'
        WHEN COALESCE(img.has_tirads_validated, FALSE)
        THEN 'imaging_only'
        WHEN COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'molecular_only'
        ELSE 'structured_only'
    END AS modality_group
FROM pat
LEFT JOIN img ON pat.research_id = img.research_id
LEFT JOIN img_sum ON pat.research_id = img_sum.research_id
LEFT JOIN mol ON pat.research_id = mol.research_id
LEFT JOIN lab ON pat.research_id = lab.research_id
LEFT JOIN fna ON pat.research_id = fna.research_id

-- [explain_explain_plan_01]
EXPLAIN ANALYZE 
-- One-row-per-patient multimodal analysis table for cancer cohort
WITH cancer AS (
    SELECT DISTINCT research_id
    FROM analysis_cancer_cohort_v1
),
pat AS (
    SELECT
        p.research_id,
        -- Demographics (actual column names from patient_analysis_resolved_v1)
        p.age_at_surgery,
        p.sex,
        p.race,
        -- Core pathology / staging
        p.histology_final,
        p.path_t_stage_raw               AS t_stage,
        p.path_n_stage_raw               AS n_stage,
        p.path_m_stage_raw               AS m_stage,
        p.ete_grade_final                AS ete_grade,
        p.path_multifocal_flag           AS multifocal_flag,
        p.path_tumor_size_cm             AS tumor_size_cm,
        p.path_ln_positive_raw           AS ln_positive_count,
        p.path_ln_examined_raw           AS ln_examined_count,
        p.margin_status_final            AS margin_status,
        p.vascular_invasion_final        AS vascular_invasion,
        -- Molecular
        p.braf_positive_final            AS braf_positive,
        p.ras_positive_final             AS ras_positive,
        p.tert_positive_final            AS tert_positive,
        p.mol_platform                   AS molecular_platform,
        -- Scoring (from resolved layer itself — it already has these)
        p.ajcc8_stage_group              AS ajcc8_stage,
        p.ata_risk_category              AS ata_risk,
        p.macis_score                    AS macis_score,
        p.macis_risk_group,
        p.ames_risk_group,
        p.ages_score,
        p.molecular_risk_tier,
        -- Recurrence (from resolved layer)
        COALESCE(p.any_recurrence_flag, FALSE) AS recurrence_flag,
        p.recurrence_date                AS first_recurrence_date,
        p.structural_recurrence_flag,
        p.biochemical_recurrence_flag,
        -- Complications (from resolved layer)
        p.any_confirmed_complication     AS has_complication_record,
        p.hypocalcemia_status,
        p.rln_status,
        -- Labs (from resolved layer)
        p.tg_nadir,
        p.tg_last_value,
        p.tg_rising_flag,
        p.lab_completeness_score,
        -- Eligibility flags
        p.analysis_eligible_flag,
        p.molecular_eligible_flag,
        p.rai_eligible_flag,
        p.survival_eligible_flag,
        p.scoring_ajcc8_flag,
        p.scoring_ata_flag,
        -- Imaging from resolved layer
        p.imaging_tirads_worst           AS tirads_worst,
        p.imaging_tirads_category        AS tirads_worst_category,
        p.imaging_nodule_size_cm,
        p.imaging_n_nodule_records       AS n_nodules_imaged,
        -- RAI from resolved layer
        p.rai_received_flag              AS has_rai_data,
        p.rai_max_dose_mci,
        -- FNA from resolved layer
        p.fna_bethesda_final             AS bethesda_worst,
        -- Surgery
        p.surg_procedure_type,
        p.surg_n_procedures
    FROM patient_analysis_resolved_v1 p
    WHERE p.research_id IN (SELECT research_id FROM cancer)
),
-- Imaging availability (validated TIRADS — richer detail)
img AS (
    SELECT
        research_id,
        TRUE                             AS has_tirads_validated,
        tirads_worst_score               AS tirads_validated_worst,
        n_sources                        AS tirads_n_sources,
        nodule_size_max_mm               AS tirads_nodule_max_mm,
        concordant_count                 AS tirads_concordant_ct,
        mismatch_count                   AS tirads_mismatch_ct
    FROM extracted_tirads_validated_v1
),
-- Imaging patient summary (from nodule master)
img_sum AS (
    SELECT
        research_id,
        TRUE                             AS has_nodule_master
    FROM imaging_patient_summary_v1
    GROUP BY research_id
),
-- Molecular availability
mol AS (
    SELECT
        research_id,
        TRUE                             AS has_molecular_episode,
        COUNT(*)                         AS n_molecular_tests
    FROM molecular_test_episode_v2
    GROUP BY research_id
),
-- Lab availability
lab AS (
    SELECT
        research_id,
        TRUE                             AS has_labs,
        COUNT(*)                         AS n_lab_values,
        COUNT(DISTINCT analyte_group)    AS n_analyte_groups
    FROM longitudinal_lab_canonical_v1
    GROUP BY research_id
),
-- FNA detail
fna AS (
    SELECT
        research_id,
        TRUE                             AS has_fna_bethesda
    FROM extracted_fna_bethesda_v1
    GROUP BY research_id
)
SELECT
    pat.*,
    -- Imaging flags (validated TIRADS enrichment)
    COALESCE(img.has_tirads_validated, FALSE)     AS has_tirads_validated,
    img.tirads_validated_worst,
    img.tirads_n_sources,
    img.tirads_nodule_max_mm,
    COALESCE(img_sum.has_nodule_master, FALSE)    AS has_nodule_master,
    -- Molecular episode flags
    COALESCE(mol.has_molecular_episode, FALSE)    AS has_molecular_data,
    mol.n_molecular_tests,
    -- Lab flags
    COALESCE(lab.has_labs, FALSE)                  AS has_lab_data,
    lab.n_lab_values,
    lab.n_analyte_groups,
    -- FNA flags
    COALESCE(fna.has_fna_bethesda, FALSE)         AS has_fna_data,
    -- Modality summary
    CASE
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE) AND COALESCE(lab.has_labs, FALSE)
        THEN 'all_three'
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'imaging_and_molecular'
        WHEN COALESCE(img.has_tirads_validated, FALSE)
        THEN 'imaging_only'
        WHEN COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'molecular_only'
        ELSE 'structured_only'
    END AS modality_group
FROM pat
LEFT JOIN img ON pat.research_id = img.research_id
LEFT JOIN img_sum ON pat.research_id = img_sum.research_id
LEFT JOIN mol ON pat.research_id = mol.research_id
LEFT JOIN lab ON pat.research_id = lab.research_id
LEFT JOIN fna ON pat.research_id = fna.research_id

-- [explain_explain_plan_02]
EXPLAIN ANALYZE 
SELECT p.research_id, t.tirads_worst_score, t.tirads_worst_category,
       m.research_id IS NOT NULL AS has_molecular
FROM patient_analysis_resolved_v1 p
LEFT JOIN extracted_tirads_validated_v1 t ON p.research_id = t.research_id
LEFT JOIN (SELECT DISTINCT research_id FROM molecular_test_episode_v2) m ON p.research_id = m.research_id
WHERE p.research_id IN (SELECT research_id FROM analysis_cancer_cohort_v1)

-- [explain_explain_plan_03]
EXPLAIN ANALYZE 
SELECT
    r.research_id,
    BOOL_OR(r.recurrence_flag) AS recurrence_any,
    MIN(r.first_recurrence_date) AS first_recurrence_date,
    MAX(r.tg_max) AS tg_max,
    MAX(r.tg_rising_flag) AS tg_rising
FROM recurrence_risk_features_mv r
WHERE r.research_id IN (SELECT research_id FROM analysis_cancer_cohort_v1)
GROUP BY r.research_id
