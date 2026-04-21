
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m075_tirads_multi_nodule_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
-- DROPPED per Logan's Q1 directive (per-nodule concordance lives on
-- canonical_us_nodule_v2 now; per-patient counts retired):
--   tirads_concordant_count_v12  (use canonical_us_nodule_v2.acr2017_vs_updated_concordant
--                                 with COUNT(*) FILTER (WHERE flag) for per-patient analysis)
--   tirads_mismatch_count_v12    (use canonical_us_nodule_v2.acr2017_vs_updated_concordant
--                                 with COUNT(*) FILTER (WHERE NOT flag) for per-patient analysis)
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.surg_procedure_type,
    p.is_malignant, p.histology_final,
    p.path_tumor_size_cm AS tumor_size_cm,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    CAST(SUBSTR(cupm.max_tirads_category_ever, 3)      AS BIGINT)
                                             AS tirads_worst_score_v12,
    cupm.max_nodule_size_mm                  AS tirads_nodule_size_max_mm_v12,
    cupm.n_nodule_records                    AS tirads_n_nodule_records_v12,
    p.bethesda_final, p.n_fna_episodes, p.fna_path_concordance_category,
    p.molecular_tested_confirmed,
    p.ajcc8_stage_group, p.ata_risk_category, p.ln_positive_flag,
    p.any_recurrence_flag, p.overall_survival_years
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_first_exam IS NOT NULL

