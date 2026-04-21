
CREATE OR REPLACE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS
-- Migrated 2026-04-21: TIRADS columns now sourced from canonical_us_patient_master_VIEW_v2.
-- Legacy column names preserved as aliases for downstream consumer compatibility.
-- Source-of-truth shift: tirads_best_category_v12, tirads_worst_category_v12,
-- tirads_best_score_v12, tirads_nodule_size_max_mm_v12 now reflect cupm_v2
-- derivations (TR rank "TR1"-"TR5", not legacy "TR4_Moderately_Suspicious"
-- vocabulary; per-record nodule size max via GREATEST/size_cm_max fallback).
-- See: CPM TIRADS Part B (2026-04-21).
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.race, p.bmi_combined,
    p.surg_procedure_type, p.surg_hemithyroidectomy, p.surg_total_thyroidectomy,
    p.first_surgery_date, p.n_surgeries, p.op_reoperative_any,
    p.histology_final, p.diagnosis_primary, p.diagnosis_variant, p.is_malignant,
    p.multifocal_flag_path, p.n_tumors,
    p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm,
    p.laterality, p.bilateral_disease_flag,
    p.ajcc8_stage_group, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_m_stage,
    p.ata_risk_category, p.ata_initial_risk, p.ata_response_category,
    p.macis_score, p.macis_risk_group, p.ages_score, p.ames_risk,
    p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio,
    p.ln_burden_band, p.ln_lateral_dissected,
    p.ln_rollup_total_positive, p.ln_rollup_total_examined,
    p.ln_rollup_central_examined, p.ln_rollup_central_positive, p.ln_rollup_ene,
    p.ete_grade, p.ete_refined_grade, p.gross_ete_flag, p.worst_ete_v10,
    p.margin_status, p.r_class_true, p.closest_margin_mm,
    p.capsular_invasion_refined, p.lvi_grade, p.lvi_ordinal_worst,
    p.vasc_grade, p.vasc_grade_final_v13, p.pni_positive,
    p.syn_frozen_section, p.syn_frozen_section_result, p.syn_carcinoma_on_frozen,
    p.syn_graves, p.syn_hashimoto, p.syn_chronic_thyroiditis,
    p.syn_follicular_adenoma, p.syn_hurthle_cell_change,
    p.syn_multinodular_goiter, p.syn_hyperplastic_nodules,
    p.syn_capsular_invasion_clean, p.syn_lymphatic_invasion_clean,
    p.syn_margin_status_synoptic, p.syn_margin_distance_mm_num,
    p.syn_n_parathyroid_identified, p.syn_parathyroid_in_specimen,
    p.syn_histologic_grade, p.syn_ki67_index,
    p.syn_isthmus_size_cm, p.syn_left_lobe_size_cm, p.syn_right_lobe_size_cm,
    p.syn_total_weight_g, p.syn_left_lobe_weight_g, p.syn_right_lobe_weight_g,
    p.syn_has_second_tumor, p.syn_tumor2_histologic_type, p.syn_tumor2_size_cm,
    p.syn_central_dissection, p.syn_bilateral_neck_dissection,
    p.gland_weight_final_g, p.preop_imaging_size_cm, p.dominant_nodule_size_cm,
    p.bethesda_final, p.bethesda_final_name, p.n_fna_episodes,
    p.fna_path_concordance_category, p.fna_path_concordant, p.cross_fna_concordance,
    -- ── TIRADS columns (migrated to cupm_v2 sources, legacy names preserved) ──
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    cupm.max_nodule_size_mm                  AS tirads_nodule_size_max_mm_v12,
    -- ── end TIRADS ──
    p.mol_platform, p.mol_genes_list, p.mol_has_thyroseq, p.mol_has_afirma,
    p.braf_positive_final, p.ras_positive_final, p.tert_positive_final,
    p.molecular_tested_confirmed, p.molecular_risk_tier, p.mol_n_tests,
    p.para_specimen_included, p.para_incidental_status_refined,
    p.para_abnormality_type, p.para_n_glands_identified,
    p.para_has_pathologic_glands, p.para_removal_intent,
    p.rai_received_reconciled AS rai_received_flag,
    p.rai_max_dose_mci, p.rai_total_cumulative_dose_mci, p.n_rai_episodes,
    p.rai_avid_flag, p.rai_intent_v9,
    p.tg_n_measurements, p.tg_trajectory_class, p.tg_nadir, p.tg_last_value,
    p.tg_rising_flag, p.tg_peak, p.days_first_to_last_tg,
    p.lab_tsh_n_measurements, p.lab_tsh_most_recent,
    p.lab_pth_n_measurements, p.lab_pth_most_recent,
    p.lab_calcium_n_measurements, p.lab_calcium_most_recent,
    p.any_recurrence_flag, p.recurrence_type, p.recurrence_site,
    p.time_to_recurrence_days, p.structural_recurrence_flag,
    p.overall_survival_years, p.vital_status, p.death_occurred,
    p.followup_years, p.followup_category,
    p.comp_hypoparathyroidism_confirmed, p.comp_hypocalcemia_confirmed,
    p.comp_rln_injury_confirmed, p.comp_hematoma_confirmed,
    p.pmhx_nlp_men_syndrome, p.pmhx_nlp_autoimmune_thyroid_hx,
    p.pmhx_nlp_prior_cancer_hx, p.pmhx_nlp_radiation_exposure,
    p.pmhx_nlp_hypothyroidism, p.pmhx_nlp_hyperthyroidism,
    p.pmhx_nlp_family_hx_thyroid, p.pmhx_nlp_family_hx_cancer,
    p.nlp_frozensec_has_data, p.nlp_frozensec_key_finding,
    p.op_nlp_parathyroid_managed, p.op_nlp_parathyroid_autograft,
    p.op_nlp_nerve_monitoring_used, p.op_nlp_reoperative_field,
    p.ajcc8_calculable_flag, p.ata_calculable_flag, p.macis_calculable_flag
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)

