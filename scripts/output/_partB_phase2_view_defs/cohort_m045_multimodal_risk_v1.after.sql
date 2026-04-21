
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m045_multimodal_risk_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
SELECT
    p.research_id, p.age_at_surgery, p.sex,
    cupm.tirads_category_at_last_preop_exam  AS preop_tirads_category,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    p.bethesda_final, p.bethesda_final_name,
    p.molecular_tested_confirmed, p.molecular_risk_tier,
    p.histology_final, p.is_malignant,
    p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path,
    p.ete_grade_final, p.ln_positive_flag,
    p.ajcc8_stage_group, p.ata_risk_category, p.surg_procedure_type,
    p.any_recurrence_flag, p.followup_years, p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_v2 AS cupm USING (research_id)
WHERE p.bethesda_final IS NOT NULL
  AND p.histology_final IS NOT NULL
  AND (cupm.tirads_category_at_last_preop_exam IS NOT NULL
       OR cupm.tirads_category_at_first_exam   IS NOT NULL)

