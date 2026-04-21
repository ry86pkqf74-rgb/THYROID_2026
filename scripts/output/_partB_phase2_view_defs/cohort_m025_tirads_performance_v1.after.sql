
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
-- DROPPED per Logan's Q1 directive (concept retired in v2 pipeline; cohort
-- redesigned to use what cupm_v2 provides):
--   tirads_n_sources_v12     (no v2 surrogate)
--   tirads_reliability_v12   (no v2 surrogate)
-- Substituted: tirads_worst_rank_source surfaces the canonical worst-rank
-- derivation source as a nearest-signal "richness" indicator.
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.race,
    cupm.tirads_category_at_last_preop_exam  AS preop_tirads_category,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    CAST(SUBSTR(cupm.max_tirads_category_ever, 3)      AS BIGINT)
                                             AS tirads_worst_score_v12,
    cupm.tirads_worst_rank_source            AS tirads_worst_rank_source,
    cupm.n_us_exams                          AS n_us_exams,
    p.dominant_nodule_size_cm AS imaging_nodule_size_cm,
    p.dominant_nodule_size_cm,
    p.bethesda_final, p.bethesda_final_name,
    p.histology_final, p.is_malignant,
    p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm,
    p.fna_path_concordance_category, p.fna_path_concordant,
    p.surg_procedure_type, p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_last_preop_exam IS NOT NULL
   OR cupm.tirads_category_at_first_exam      IS NOT NULL

