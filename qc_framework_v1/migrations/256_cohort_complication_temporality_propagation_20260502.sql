-- =============================================================================
-- mig_256 — Propagate complication temporality / preop-aligned CPM passthrough columns
--           (mig_255 pattern) across manuscript_workspace cohort spine + dependents.
--
-- Date:    2026-05-02
-- Mirrors: qc_framework_v1/migrations/255_cohort_m038_complication_temporality_columns_20260502.sql
--
-- VERIFY (post-apply):
--   SELECT COUNT(*) FROM manuscript_workspace.cohort_descriptive_full_cohort_v1;  -- 10871 (full spine)
--   cohort_m032_descriptive_25yr_v1 / cohort_m066_parathyroid_id_v1 also 10871; other targets are predicates.
--   DESCRIBE SELECT comp_hypocalcemia_timing_window FROM manuscript_workspace.cohort_m066_parathyroid_id_v1 LIMIT 1;
--
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.race, p.bmi_combined, p.surg_procedure_type, p.surg_hemithyroidectomy, p.surg_total_thyroidectomy, p.first_surgery_date, p.n_surgeries, p.op_reoperative_any, p.histology_final, p.diagnosis_primary, p.diagnosis_variant, p.is_malignant, p.multifocal_flag_path, p.n_tumors, p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm, p.laterality, p.bilateral_disease_flag, p.ajcc8_stage_group, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_m_stage, p.ata_risk_category, p.ata_initial_risk, p.ata_response_category, p.macis_score, p.macis_risk_group, p.ages_score, p.ames_risk, p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio, p.ln_burden_band, p.ln_lateral_dissected, p.ln_rollup_total_positive, p.ln_rollup_total_examined, p.ln_rollup_central_examined, p.ln_rollup_central_positive, p.ln_rollup_ene, p.ete_grade, p.ete_refined_grade, p.gross_ete_flag, p.worst_ete_v10, p.margin_status, p.r_class_true, p.closest_margin_mm, p.capsular_invasion_refined, p.lvi_grade, p.lvi_ordinal_worst, p.vasc_grade, p.vasc_grade_final_v13, p.pni_positive, p.syn_frozen_section, p.syn_frozen_section_result, p.syn_carcinoma_on_frozen, p.syn_graves, p.syn_hashimoto, p.syn_chronic_thyroiditis, p.syn_follicular_adenoma, p.syn_hurthle_cell_change, p.syn_multinodular_goiter, p.syn_hyperplastic_nodules, p.syn_capsular_invasion_clean, p.syn_lymphatic_invasion_clean, p.syn_margin_status_synoptic, p.syn_margin_distance_mm_num, p.syn_n_parathyroid_identified, p.syn_parathyroid_in_specimen, p.syn_histologic_grade, p.syn_ki67_index, p.syn_isthmus_size_cm_legacy_raw, p.syn_left_lobe_size_cm_legacy_raw, p.syn_right_lobe_size_cm_legacy_raw, p.syn_total_weight_g, p.syn_left_lobe_weight_g, p.syn_right_lobe_weight_g, p.syn_has_second_tumor, p.syn_tumor2_histologic_type, p.syn_tumor2_size_cm, p.syn_central_dissection, p.syn_bilateral_neck_dissection, p.gland_weight_final_g, p.preop_imaging_size_cm, p.dominant_nodule_size_cm, p.bethesda_final, p.bethesda_final_name, p.n_fna_episodes, p.fna_path_concordance_category, p.fna_path_concordant, p.cross_fna_concordance, cupm.tirads_category_at_first_exam AS tirads_best_category_v12, cupm.max_tirads_category_ever AS tirads_worst_category_v12, CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12, cupm.max_nodule_size_mm AS tirads_nodule_size_max_mm_v12, p.mol_platform, p.mol_genes_list, p.mol_has_thyroseq, p.mol_has_afirma, p.braf_positive_final, p.ras_positive_final, p.tert_positive_final, p.molecular_tested_confirmed, p.molecular_risk_tier, p.mol_n_tests, p.para_specimen_included, p.para_incidental_status_refined, p.para_abnormality_type, p.para_n_glands_identified, p.para_has_pathologic_glands, p.para_removal_intent, p.rai_received_reconciled AS rai_received_flag, p.rai_max_dose_mci, p.rai_total_cumulative_dose_mci, p.n_rai_episodes, p.rai_avid_flag, p.rai_intent_v9, p.tg_n_measurements, p.tg_trajectory_class, p.tg_nadir, p.tg_last_value, p.tg_rising_flag, p.tg_peak, p.days_first_to_last_tg, p.lab_tsh_n_measurements, p.lab_tsh_most_recent, p.lab_pth_n_measurements, p.lab_pth_most_recent, p.lab_calcium_n_measurements, p.lab_calcium_most_recent, p.any_recurrence_flag, p.recurrence_type, p.recurrence_site, p.time_to_recurrence_days, p.structural_recurrence_flag, p.overall_survival_years, p.vital_status, p.death_occurred, p.followup_years, p.followup_category, p.comp_hypoparathyroidism_confirmed, p.comp_hypocalcemia_confirmed, p.comp_rln_injury_confirmed, p.comp_hematoma_confirmed, p.comp_hypoparathyroidism_transient, p.comp_hypopara_permanent_limitation_note, p.comp_hypoparathyroidism_timing_window, p.comp_hypoparathyroidism_preexisting, p.comp_hypoparathyroidism_new_postop, p.comp_hypocalcemia_timing_window, p.comp_hypocalcemia_transient, p.comp_hypocalcemia_clinical_preexisting, p.comp_rln_injury_timing_window, p.comp_rln_injury_transient, p.comp_vc_paralysis_timing_window, p.comp_vc_paresis_timing_window, p.pmhx_nlp_men_syndrome, p.pmhx_nlp_autoimmune_thyroid_hx, p.pmhx_nlp_prior_cancer_hx, p.pmhx_nlp_radiation_exposure, p.pmhx_nlp_hypothyroidism, p.pmhx_nlp_hyperthyroidism, p.pmhx_nlp_family_hx_thyroid, p.pmhx_nlp_family_hx_cancer, p.nlp_frozensec_has_data, p.nlp_frozensec_key_finding, p.op_nlp_parathyroid_managed, p.op_nlp_parathyroid_autograft, p.op_nlp_nerve_monitoring_used, p.op_nlp_reoperative_field, p.ajcc8_calculable_flag, p.ata_calculable_flag, p.macis_calculable_flag FROM main.canonical_patient_master AS p LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id);

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m009_parathyroid_final_path_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  surg_procedure_type,
  is_malignant,
  histology_final,
  para_specimen_included,
  para_incidental_status_refined,
  para_abnormality_type,
  para_n_glands_identified,
  para_has_pathologic_glands,
  para_removal_intent,
  para_n_glands_biopsied,
  para_n_glands_excised,
  para_max_cellularity_pct,
  para_max_gland_weight_g,
  syn_n_parathyroid_identified,
  syn_parathyroid_in_specimen,
  op_nlp_parathyroid_managed,
  op_nlp_parathyroid_autograft,
  comp_hypoparathyroidism_confirmed,
  comp_hypocalcemia_confirmed
  , comp_hypoparathyroidism_transient
  , comp_hypopara_permanent_limitation_note
  , comp_hypoparathyroidism_timing_window
  , comp_hypoparathyroidism_preexisting
  , comp_hypoparathyroidism_new_postop
  , comp_hypocalcemia_timing_window
  , comp_hypocalcemia_transient
  , comp_hypocalcemia_clinical_preexisting
  , comp_rln_injury_timing_window
  , comp_rln_injury_transient
  , comp_vc_paralysis_timing_window
  , comp_vc_paresis_timing_window
,
  lab_pth_n_measurements,
  lab_pth_most_recent,
  lab_calcium_n_measurements,
  lab_calcium_most_recent
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE ((para_specimen_included = CAST('t' AS BOOLEAN)) OR (syn_parathyroid_in_specimen = CAST('t' AS BOOLEAN)));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m017_eucalcemic_hypopara_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  surg_procedure_type,
  surg_total_thyroidectomy,
  is_malignant,
  histology_final,
  lab_pth_n_measurements,
  lab_pth_min,
  lab_pth_max,
  lab_pth_most_recent,
  lab_calcium_n_measurements,
  lab_calcium_min,
  lab_calcium_max,
  lab_calcium_most_recent,
  pth_nadir,
  pth_nadir_30d,
  calcium_nadir,
  calcium_nadir_30d,
  comp_hypoparathyroidism_confirmed,
  comp_hypoparathyroidism_permanent,
  comp_hypoparathyroidism_transient,
  comp_hypocalcemia_confirmed,
  comp_hypocalcemia_permanent
  , comp_hypopara_permanent_limitation_note
  , comp_hypoparathyroidism_timing_window
  , comp_hypoparathyroidism_preexisting
  , comp_hypoparathyroidism_new_postop
  , comp_hypocalcemia_timing_window
  , comp_hypocalcemia_transient
  , comp_hypocalcemia_clinical_preexisting
  , comp_rln_injury_timing_window
  , comp_rln_injury_transient
  , comp_vc_paralysis_timing_window
  , comp_vc_paresis_timing_window
,
  calcium_supplement_required,
  para_n_glands_identified,
  op_nlp_parathyroid_autograft
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE ((surg_total_thyroidectomy = CAST('t' AS BOOLEAN)) AND (lab_pth_n_measurements > 0));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m032_descriptive_25yr_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  p.surg_first_date,
  p.surg_procedure_type,
  p.surg_total_thyroidectomy,
  p.surg_hemithyroidectomy,
  p.surg_n_procedures,
  p.n_surgeries,
  p.bethesda_final,
  p.bethesda_final_name,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.multifocal_flag_path AS multifocal_flag,
  p.bilateral_disease_flag,
  p.ete_grade_final,
  p.ln_positive_flag,
  p.ln_total_examined,
  p.ln_total_positive,
  p.ajcc8_stage_group,
  p.ata_risk_category,
  p.rai_received_flag,
  p.confirmed_rai_episodes,
  p.any_recurrence_flag,
  p.followup_years,
  p.death_occurred,
  p.overall_survival_years,
  p.any_confirmed_complication_flag,
  p.comp_hypocalcemia_confirmed,
  p.comp_rln_injury_confirmed,
  p.comp_hematoma_confirmed,
  p.comp_hypoparathyroidism_confirmed,
  p.comp_hypoparathyroidism_transient
  , p.comp_hypopara_permanent_limitation_note
  , p.comp_hypoparathyroidism_timing_window
  , p.comp_hypoparathyroidism_preexisting
  , p.comp_hypoparathyroidism_new_postop
  , p.comp_hypocalcemia_timing_window
  , p.comp_hypocalcemia_transient
  , p.comp_hypocalcemia_clinical_preexisting
  , p.comp_rln_injury_timing_window
  , p.comp_rln_injury_transient
  , p.comp_vc_paralysis_timing_window
  , p.comp_vc_paresis_timing_window

FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m039_pth_calcium_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.surg_procedure_type,
  p.surg_total_thyroidectomy,
  p.surg_first_date,
  p.lab_pth_min,
  p.lab_pth_max,
  p.lab_pth_n_measurements,
  p.lab_calcium_min,
  p.lab_calcium_max,
  p.lab_calcium_n_measurements,
  p.postop_pth_min_value,
  p.postop_pth_min_days_postop,
  p.postop_pth_n_measurements,
  p.postop_calcium_min_value,
  p.postop_calcium_min_days_postop,
  p.postop_calcium_n_measurements,
  p.pth_nadir,
  p.pth_nadir_30d,
  p.pth_nadir_days_postop,
  p.calcium_nadir,
  p.calcium_nadir_30d,
  p.calcium_nadir_days_postop,
  p.has_low_pth_flag,
  p.has_low_calcium_flag,
  p.comp_hypoparathyroidism_confirmed,
  p.comp_hypoparathyroidism_transient,
  p.comp_hypoparathyroidism_permanent,
  p.comp_hypocalcemia_confirmed,
  p.comp_hypocalcemia_transient,
  p.comp_hypocalcemia_permanent
  , p.comp_hypopara_permanent_limitation_note
  , p.comp_hypoparathyroidism_timing_window
  , p.comp_hypoparathyroidism_preexisting
  , p.comp_hypoparathyroidism_new_postop
  , p.comp_hypocalcemia_timing_window
  , p.comp_hypocalcemia_transient
  , p.comp_hypocalcemia_clinical_preexisting
  , p.comp_rln_injury_timing_window
  , p.comp_rln_injury_transient
  , p.comp_vc_paralysis_timing_window
  , p.comp_vc_paresis_timing_window
,
  p.calcium_supplement_required,
  p.med_nlp_calcium_supplement,
  p.med_nlp_calcitriol,
  p.histology_final,
  p.is_malignant,
  p.ln_total_examined,
  p.followup_years
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p
WHERE (p.surg_total_thyroidectomy = CAST('t' AS BOOLEAN));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m040_reoperative_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.surg_procedure_type,
  p.surg_first_date,
  p.n_surgeries,
  p.surg_n_procedures,
  p.second_surgery_date,
  p.days_between_first_second_surgery,
  p.completion_reason,
  p.completion_reason_confidence,
  p.completion_histology_type,
  p.completion_prior_histology,
  p.op_reoperative_any,
  p.op_nlp_reoperative_field,
  p.pshx_nlp_prior_thyroidectomy,
  p.pshx_nlp_prior_neck_surgery,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.any_confirmed_complication_flag,
  p.comp_rln_injury_confirmed,
  p.comp_hypoparathyroidism_confirmed,
  p.comp_hematoma_confirmed,
  p.comp_hypocalcemia_confirmed,
  p.comp_hypoparathyroidism_transient
  , p.comp_hypopara_permanent_limitation_note
  , p.comp_hypoparathyroidism_timing_window
  , p.comp_hypoparathyroidism_preexisting
  , p.comp_hypoparathyroidism_new_postop
  , p.comp_hypocalcemia_timing_window
  , p.comp_hypocalcemia_transient
  , p.comp_hypocalcemia_clinical_preexisting
  , p.comp_rln_injury_timing_window
  , p.comp_rln_injury_transient
  , p.comp_vc_paralysis_timing_window
  , p.comp_vc_paresis_timing_window
,
  p.any_recurrence_flag,
  p.followup_years
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p
WHERE ((p.n_surgeries > 1) OR (p.op_reoperative_any = CAST('t' AS BOOLEAN))
   OR (p.pshx_nlp_prior_thyroidectomy = CAST('t' AS BOOLEAN)));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m042_incidental_parathyroid_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.surg_procedure_type,
  p.surg_total_thyroidectomy,
  p.surg_first_date,
  p.syn_parathyroid_in_specimen,
  p.syn_n_parathyroid_identified,
  p.para_specimen_included,
  p.para_incidental_status_refined,
  p.para_has_pathologic_glands,
  p.para_abnormality_type,
  p.para_n_glands_identified,
  p.para_n_glands_excised,
  p.para_max_gland_weight_g,
  p.nlp_parathyroid_has_data,
  p.nlp_parathyroid_key_finding,
  p.op_nlp_parathyroid_autograft,
  p.op_nlp_parathyroid_managed,
  p.histology_final,
  p.is_malignant,
  p.comp_hypoparathyroidism_confirmed,
  p.comp_hypocalcemia_confirmed,
  p.comp_hypoparathyroidism_transient
  , p.comp_hypopara_permanent_limitation_note
  , p.comp_hypoparathyroidism_timing_window
  , p.comp_hypoparathyroidism_preexisting
  , p.comp_hypoparathyroidism_new_postop
  , p.comp_hypocalcemia_timing_window
  , p.comp_hypocalcemia_transient
  , p.comp_hypocalcemia_clinical_preexisting
  , p.comp_rln_injury_timing_window
  , p.comp_rln_injury_transient
  , p.comp_vc_paralysis_timing_window
  , p.comp_vc_paresis_timing_window
,
  p.postop_pth_min_value,
  p.postop_calcium_min_value,
  p.followup_years
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p
WHERE ((p.syn_parathyroid_in_specimen = CAST('t' AS BOOLEAN))
   OR (p.para_specimen_included = CAST('t' AS BOOLEAN))
   OR (p.nlp_parathyroid_has_data = CAST('t' AS BOOLEAN)));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m066_parathyroid_id_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  surg_procedure_type,
  is_malignant,
  histology_final,
  para_specimen_included,
  para_incidental_status_refined,
  para_abnormality_type,
  para_n_glands_identified,
  para_has_pathologic_glands,
  para_removal_intent,
  syn_n_parathyroid_identified,
  syn_parathyroid_in_specimen,
  op_nlp_parathyroid_managed,
  op_nlp_parathyroid_autograft,
  comp_hypoparathyroidism_confirmed,
  comp_hypocalcemia_confirmed
  , comp_hypoparathyroidism_transient
  , comp_hypopara_permanent_limitation_note
  , comp_hypoparathyroidism_timing_window
  , comp_hypoparathyroidism_preexisting
  , comp_hypoparathyroidism_new_postop
  , comp_hypocalcemia_timing_window
  , comp_hypocalcemia_transient
  , comp_hypocalcemia_clinical_preexisting
  , comp_rln_injury_timing_window
  , comp_rln_injury_transient
  , comp_vc_paralysis_timing_window
  , comp_vc_paresis_timing_window
,
  lab_pth_n_measurements,
  lab_calcium_n_measurements
FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m079_eucalcemic_outcomes_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  surg_procedure_type,
  surg_total_thyroidectomy,
  is_malignant,
  histology_final,
  lab_pth_n_measurements,
  lab_pth_min,
  lab_pth_max,
  lab_pth_most_recent,
  lab_calcium_n_measurements,
  lab_calcium_min,
  lab_calcium_max,
  lab_calcium_most_recent,
  pth_nadir,
  pth_nadir_30d,
  calcium_nadir,
  calcium_nadir_30d,
  comp_hypoparathyroidism_confirmed,
  comp_hypoparathyroidism_permanent,
  comp_hypoparathyroidism_transient,
  comp_hypocalcemia_confirmed,
  comp_hypocalcemia_permanent
  , comp_hypopara_permanent_limitation_note
  , comp_hypoparathyroidism_timing_window
  , comp_hypoparathyroidism_preexisting
  , comp_hypoparathyroidism_new_postop
  , comp_hypocalcemia_timing_window
  , comp_hypocalcemia_transient
  , comp_hypocalcemia_clinical_preexisting
  , comp_rln_injury_timing_window
  , comp_rln_injury_transient
  , comp_vc_paralysis_timing_window
  , comp_vc_paresis_timing_window
,
  calcium_supplement_required,
  followup_years
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE ((surg_total_thyroidectomy = CAST('t' AS BOOLEAN)) AND (lab_pth_n_measurements > 0));

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m082_parathyroid_tumors_v1 AS
SELECT
  research_id,
  age_at_surgery,
  sex,
  surg_procedure_type,
  is_malignant,
  histology_final,
  para_specimen_included,
  para_incidental_status_refined,
  para_abnormality_type,
  para_n_glands_identified,
  para_has_pathologic_glands,
  para_removal_intent,
  para_n_glands_excised,
  para_max_cellularity_pct,
  para_max_gland_weight_g,
  syn_parathyroid_in_specimen,
  comp_hypoparathyroidism_confirmed,
  comp_hypocalcemia_confirmed
  , comp_hypoparathyroidism_transient
  , comp_hypopara_permanent_limitation_note
  , comp_hypoparathyroidism_timing_window
  , comp_hypoparathyroidism_preexisting
  , comp_hypoparathyroidism_new_postop
  , comp_hypocalcemia_timing_window
  , comp_hypocalcemia_transient
  , comp_hypocalcemia_clinical_preexisting
  , comp_rln_injury_timing_window
  , comp_rln_injury_transient
  , comp_vc_paralysis_timing_window
  , comp_vc_paresis_timing_window
,
  lab_pth_n_measurements,
  lab_pth_most_recent,
  lab_calcium_n_measurements,
  lab_calcium_most_recent
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE ((para_specimen_included = CAST('t' AS BOOLEAN)) OR (para_has_pathologic_glands = CAST('t' AS BOOLEAN)));
