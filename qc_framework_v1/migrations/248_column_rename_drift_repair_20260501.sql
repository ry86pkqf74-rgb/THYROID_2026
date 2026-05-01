-- =============================================================================
-- mig_248 — Column-rename drift repair across manuscript cohort views
-- Date:    2026-05-01
-- Lane:    mig_248
-- =============================================================================
--
-- Scope:
--   Repairs manuscript_workspace views that failed at query time because their
--   view bodies referenced canonical_patient_master columns renamed by mig_173.
--   This is view-DDL only: no canonical base-table, registry, or signoff writes.
--
-- Rename treatment:
--   * syn_right_lobe_size_cm -> syn_right_lobe_size_cm_legacy_raw (mig_173_syn_size_cm_dtype_reform_20260429); legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.
--   * syn_left_lobe_size_cm -> syn_left_lobe_size_cm_legacy_raw (mig_173_syn_size_cm_dtype_reform_20260429); legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.
--   * syn_isthmus_size_cm -> syn_isthmus_size_cm_legacy_raw (mig_173_syn_size_cm_dtype_reform_20260429); legacy_raw_preserves_original VARCHAR 3-axis manuscript-view semantics; typed axis/volume siblings exist for numeric analysis.
--
-- =============================================================================

-- Repair: manuscript_workspace.cohort_descriptive_full_cohort_v1
--   syn_right_lobe_size_cm -> syn_right_lobe_size_cm_legacy_raw
--   syn_left_lobe_size_cm -> syn_left_lobe_size_cm_legacy_raw
--   syn_isthmus_size_cm -> syn_isthmus_size_cm_legacy_raw
CREATE OR REPLACE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.race, p.bmi_combined, p.surg_procedure_type, p.surg_hemithyroidectomy, p.surg_total_thyroidectomy, p.first_surgery_date, p.n_surgeries, p.op_reoperative_any, p.histology_final, p.diagnosis_primary, p.diagnosis_variant, p.is_malignant, p.multifocal_flag_path, p.n_tumors, p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm, p.laterality, p.bilateral_disease_flag, p.ajcc8_stage_group, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_m_stage, p.ata_risk_category, p.ata_initial_risk, p.ata_response_category, p.macis_score, p.macis_risk_group, p.ages_score, p.ames_risk, p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio, p.ln_burden_band, p.ln_lateral_dissected, p.ln_rollup_total_positive, p.ln_rollup_total_examined, p.ln_rollup_central_examined, p.ln_rollup_central_positive, p.ln_rollup_ene, p.ete_grade, p.ete_refined_grade, p.gross_ete_flag, p.worst_ete_v10, p.margin_status, p.r_class_true, p.closest_margin_mm, p.capsular_invasion_refined, p.lvi_grade, p.lvi_ordinal_worst, p.vasc_grade, p.vasc_grade_final_v13, p.pni_positive, p.syn_frozen_section, p.syn_frozen_section_result, p.syn_carcinoma_on_frozen, p.syn_graves, p.syn_hashimoto, p.syn_chronic_thyroiditis, p.syn_follicular_adenoma, p.syn_hurthle_cell_change, p.syn_multinodular_goiter, p.syn_hyperplastic_nodules, p.syn_capsular_invasion_clean, p.syn_lymphatic_invasion_clean, p.syn_margin_status_synoptic, p.syn_margin_distance_mm_num, p.syn_n_parathyroid_identified, p.syn_parathyroid_in_specimen, p.syn_histologic_grade, p.syn_ki67_index, p.syn_isthmus_size_cm_legacy_raw, p.syn_left_lobe_size_cm_legacy_raw, p.syn_right_lobe_size_cm_legacy_raw, p.syn_total_weight_g, p.syn_left_lobe_weight_g, p.syn_right_lobe_weight_g, p.syn_has_second_tumor, p.syn_tumor2_histologic_type, p.syn_tumor2_size_cm, p.syn_central_dissection, p.syn_bilateral_neck_dissection, p.gland_weight_final_g, p.preop_imaging_size_cm, p.dominant_nodule_size_cm, p.bethesda_final, p.bethesda_final_name, p.n_fna_episodes, p.fna_path_concordance_category, p.fna_path_concordant, p.cross_fna_concordance, cupm.tirads_category_at_first_exam AS tirads_best_category_v12, cupm.max_tirads_category_ever AS tirads_worst_category_v12, CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12, cupm.max_nodule_size_mm AS tirads_nodule_size_max_mm_v12, p.mol_platform, p.mol_genes_list, p.mol_has_thyroseq, p.mol_has_afirma, p.braf_positive_final, p.ras_positive_final, p.tert_positive_final, p.molecular_tested_confirmed, p.molecular_risk_tier, p.mol_n_tests, p.para_specimen_included, p.para_incidental_status_refined, p.para_abnormality_type, p.para_n_glands_identified, p.para_has_pathologic_glands, p.para_removal_intent, p.rai_received_reconciled AS rai_received_flag, p.rai_max_dose_mci, p.rai_total_cumulative_dose_mci, p.n_rai_episodes, p.rai_avid_flag, p.rai_intent_v9, p.tg_n_measurements, p.tg_trajectory_class, p.tg_nadir, p.tg_last_value, p.tg_rising_flag, p.tg_peak, p.days_first_to_last_tg, p.lab_tsh_n_measurements, p.lab_tsh_most_recent, p.lab_pth_n_measurements, p.lab_pth_most_recent, p.lab_calcium_n_measurements, p.lab_calcium_most_recent, p.any_recurrence_flag, p.recurrence_type, p.recurrence_site, p.time_to_recurrence_days, p.structural_recurrence_flag, p.overall_survival_years, p.vital_status, p.death_occurred, p.followup_years, p.followup_category, p.comp_hypoparathyroidism_confirmed, p.comp_hypocalcemia_confirmed, p.comp_rln_injury_confirmed, p.comp_hematoma_confirmed, p.pmhx_nlp_men_syndrome, p.pmhx_nlp_autoimmune_thyroid_hx, p.pmhx_nlp_prior_cancer_hx, p.pmhx_nlp_radiation_exposure, p.pmhx_nlp_hypothyroidism, p.pmhx_nlp_hyperthyroidism, p.pmhx_nlp_family_hx_thyroid, p.pmhx_nlp_family_hx_cancer, p.nlp_frozensec_has_data, p.nlp_frozensec_key_finding, p.op_nlp_parathyroid_managed, p.op_nlp_parathyroid_autograft, p.op_nlp_nerve_monitoring_used, p.op_nlp_reoperative_field, p.ajcc8_calculable_flag, p.ata_calculable_flag, p.macis_calculable_flag FROM main.canonical_patient_master AS p LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id);

-- Repair: manuscript_workspace.cohort_m049_pyramidal_lobe_v1
--   syn_isthmus_size_cm -> syn_isthmus_size_cm_legacy_raw
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m049_pyramidal_lobe_v1 AS SELECT research_id, age_at_surgery, sex, race, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, laterality, syn_isthmus_size_cm_legacy_raw, gland_weight_final_g, syn_total_weight_g, ajcc8_stage_group, ata_risk_category, any_recurrence_flag, overall_survival_years FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;

-- Repair: manuscript_workspace.cohort_m058_thyroid_size_weight_v1
--   syn_right_lobe_size_cm -> syn_right_lobe_size_cm_legacy_raw
--   syn_left_lobe_size_cm -> syn_left_lobe_size_cm_legacy_raw
--   syn_isthmus_size_cm -> syn_isthmus_size_cm_legacy_raw
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m058_thyroid_size_weight_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, gland_weight_final_g, syn_total_weight_g, syn_left_lobe_weight_g, syn_right_lobe_weight_g, syn_isthmus_size_cm_legacy_raw, syn_left_lobe_size_cm_legacy_raw, syn_right_lobe_size_cm_legacy_raw, preop_imaging_size_cm, syn_multinodular_goiter, ajcc8_stage_group, ata_risk_category, ln_positive_flag, any_recurrence_flag, overall_survival_years FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;

-- =============================================================================
-- Post-repair cohort-size table (run after all cohort_m0% views are queryable)
-- =============================================================================

CREATE OR REPLACE TABLE manuscript_workspace.dive_cohort_size_v1 AS
SELECT 'cohort_m001_indeterminate_genetics_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m001_indeterminate_genetics_v1
UNION ALL
SELECT 'cohort_m004_graves_hashimoto_cancer_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m004_graves_hashimoto_cancer_v1
UNION ALL
SELECT 'cohort_m006_molecular_surg_decision_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m006_molecular_surg_decision_v1
UNION ALL
SELECT 'cohort_m007_rss_reclassification_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m007_rss_reclassification_v1
UNION ALL
SELECT 'cohort_m009_parathyroid_final_path_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m009_parathyroid_final_path_v1
UNION ALL
SELECT 'cohort_m011_tirads_fna_genetics_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m011_tirads_fna_genetics_v1
UNION ALL
SELECT 'cohort_m016_graves_carcinoma_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m016_graves_carcinoma_v1
UNION ALL
SELECT 'cohort_m017_eucalcemic_hypopara_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m017_eucalcemic_hypopara_v1
UNION ALL
SELECT 'cohort_m018_molecular_beth56_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m018_molecular_beth56_v1
UNION ALL
SELECT 'cohort_m019_rai_outcomes_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m019_rai_outcomes_v1
UNION ALL
SELECT 'cohort_m023_preop_genetics_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m023_preop_genetics_v1
UNION ALL
SELECT 'cohort_m025_tirads_performance_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m025_tirads_performance_v1
UNION ALL
SELECT 'cohort_m028_bethesda_iii_iv_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m028_bethesda_iii_iv_v1
UNION ALL
SELECT 'cohort_m029_fna_concordance_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m029_fna_concordance_v1
UNION ALL
SELECT 'cohort_m030_genetic_predictive_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m030_genetic_predictive_v1
UNION ALL
SELECT 'cohort_m031_nuclear_medicine_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m031_nuclear_medicine_v1
UNION ALL
SELECT 'cohort_m032_descriptive_25yr_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
UNION ALL
SELECT 'cohort_m033_afirma_thyroseq_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m033_afirma_thyroseq_v1
UNION ALL
SELECT 'cohort_m035_bethesda_v_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m035_bethesda_v_v1
UNION ALL
SELECT 'cohort_m036_ata_risk_comparison_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m036_ata_risk_comparison_v1
UNION ALL
SELECT 'cohort_m037_ln_metastasis_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1
UNION ALL
SELECT 'cohort_m038_massive_goiter_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m038_massive_goiter_v1
UNION ALL
SELECT 'cohort_m039_pth_calcium_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m039_pth_calcium_v1
UNION ALL
SELECT 'cohort_m040_reoperative_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m040_reoperative_v1
UNION ALL
SELECT 'cohort_m042_incidental_parathyroid_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m042_incidental_parathyroid_v1
UNION ALL
SELECT 'cohort_m043_ln_predictors_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m043_ln_predictors_v1
UNION ALL
SELECT 'cohort_m044_ajcc_ete_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
UNION ALL
SELECT 'cohort_m045_multimodal_risk_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m045_multimodal_risk_v1
UNION ALL
SELECT 'cohort_m046_niftp_era_bethesda_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m046_niftp_era_bethesda_v1
UNION ALL
SELECT 'cohort_m047_frozen_section_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m047_frozen_section_v1
UNION ALL
SELECT 'cohort_m048_tnm_multifocal_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m048_tnm_multifocal_v1
UNION ALL
SELECT 'cohort_m049_pyramidal_lobe_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m049_pyramidal_lobe_v1
UNION ALL
SELECT 'cohort_m050_tumor_size_volume_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m050_tumor_size_volume_v1
UNION ALL
SELECT 'cohort_m051_ete_ln_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m051_ete_ln_v1
UNION ALL
SELECT 'cohort_m052_mrlnd_ln_count_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m052_mrlnd_ln_count_v1
UNION ALL
SELECT 'cohort_m053_nondiagnostic_fna_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m053_nondiagnostic_fna_v1
UNION ALL
SELECT 'cohort_m054_niftp_reclass_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m054_niftp_reclass_v1
UNION ALL
SELECT 'cohort_m055_recurrence_rai_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m055_recurrence_rai_v1
UNION ALL
SELECT 'cohort_m056_age_epidemiology_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m056_age_epidemiology_v1
UNION ALL
SELECT 'cohort_m057_risk_stratification_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m057_risk_stratification_v1
UNION ALL
SELECT 'cohort_m058_thyroid_size_weight_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m058_thyroid_size_weight_v1
UNION ALL
SELECT 'cohort_m059_prognostic_scoring_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m059_prognostic_scoring_v1
UNION ALL
SELECT 'cohort_m060_adenoma_ftump_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m060_adenoma_ftump_v1
UNION ALL
SELECT 'cohort_m061_thyroiditis_outcomes_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m061_thyroiditis_outcomes_v1
UNION ALL
SELECT 'cohort_m062_incidental_frozen_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m062_incidental_frozen_v1
UNION ALL
SELECT 'cohort_m063_frozen_false_neg_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m063_frozen_false_neg_v1
UNION ALL
SELECT 'cohort_m064_frozen_decision_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m064_frozen_decision_v1
UNION ALL
SELECT 'cohort_m065_frozen_tt_vs_lob_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m065_frozen_tt_vs_lob_v1
UNION ALL
SELECT 'cohort_m066_parathyroid_id_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m066_parathyroid_id_v1
UNION ALL
SELECT 'cohort_m067_tsh_tg_tumorigenesis_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m067_tsh_tg_tumorigenesis_v1
UNION ALL
SELECT 'cohort_m068_mutation_labs_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m068_mutation_labs_v1
UNION ALL
SELECT 'cohort_m069_graves_hashimoto_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m069_graves_hashimoto_v1
UNION ALL
SELECT 'cohort_m070_hereditary_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m070_hereditary_v1
UNION ALL
SELECT 'cohort_m071_immunologic_meds_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m071_immunologic_meds_v1
UNION ALL
SELECT 'cohort_m072_molecular_surg_impact_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m072_molecular_surg_impact_v1
UNION ALL
SELECT 'cohort_m073_tg_lob_vs_tt_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m073_tg_lob_vs_tt_v1
UNION ALL
SELECT 'cohort_m075_tirads_multi_nodule_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
UNION ALL
SELECT 'cohort_m076_ln_surveillance_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m076_ln_surveillance_v1
UNION ALL
SELECT 'cohort_m078_graves_survival_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m078_graves_survival_v1
UNION ALL
SELECT 'cohort_m079_eucalcemic_outcomes_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m079_eucalcemic_outcomes_v1
UNION ALL
SELECT 'cohort_m080_molecular_beth56_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m080_molecular_beth56_v1
UNION ALL
SELECT 'cohort_m081_rai_resistant_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m081_rai_resistant_v1
UNION ALL
SELECT 'cohort_m082_parathyroid_tumors_v1' AS cohort_view_name, COUNT(*) AS current_row_count, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS measured_at
FROM manuscript_workspace.cohort_m082_parathyroid_tumors_v1;

-- =============================================================================
-- Verification
-- =============================================================================
-- 1) Previously broken in-scope repaired views:
-- SELECT 'manuscript_workspace.cohort_descriptive_full_cohort_v1' AS view_name, COUNT(*) AS row_count FROM manuscript_workspace.cohort_descriptive_full_cohort_v1
-- UNION ALL
-- SELECT 'manuscript_workspace.cohort_m049_pyramidal_lobe_v1' AS view_name, COUNT(*) AS row_count FROM manuscript_workspace.cohort_m049_pyramidal_lobe_v1
-- UNION ALL
-- SELECT 'manuscript_workspace.cohort_m058_thyroid_size_weight_v1' AS view_name, COUNT(*) AS row_count FROM manuscript_workspace.cohort_m058_thyroid_size_weight_v1;
-- 2) dive_cohort_size_v1 row count should match distinct non-null cohort_view_name count in manuscript_dive_map_v1.
-- 3) semantic_publication.vw_publication_qc_status_VIEW_v1 gate1 should remain unchanged by this manuscript_workspace-only lane.

-- Out-of-scope unresolved manuscript_workspace failures observed during scan:
--   manuscript_workspace.canonical_detail_pointer_v1: Binder Error: Referenced column "feeds_master_columns_normalized" not found in FROM clause! | Candidate bindings: "feeds_master_columns_secondary", "feeds_master_columns", "feeds_master_columns_array", "needs_manual_review", "domain"
--   manuscript_workspace.ete_manuscript_analytic_v1: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
--   manuscript_workspace.ete_manuscript_analytic_v2: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
--   manuscript_workspace.ete_manuscript_analytic_v3: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
--   manuscript_workspace.ete_manuscript_analytic_v4: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
--   manuscript_workspace.ete_manuscript_analytic_v6: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
--   manuscript_workspace.ete_manuscript_analytic_v7: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
