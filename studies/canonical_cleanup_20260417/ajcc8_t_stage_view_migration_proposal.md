# ajcc8_t_stage view migration proposal

Generated 2026-04-17 by canonical cleanup Phase 4.6 PRE-GATE.

Each section shows the live view DDL and the proposed migrated DDL.
Migration: bare `ajcc8_t_stage` -> `ajcc8_t_stage_corrected`.
After applying these CREATE OR REPLACE VIEW statements, the rename in
`scripts/274b_canonical_cleanup_phase4_6_rename.py` becomes safe.

## `manuscript_workspace.cohort_descriptive_full_cohort_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS SELECT research_id, age_at_surgery, sex, race, bmi_combined, surg_procedure_type, surg_hemithyroidectomy, surg_total_thyroidectomy, first_surgery_date, n_surgeries, op_reoperative_any, histology_final, diagnosis_primary, diagnosis_variant, is_malignant, multifocal_flag_path, n_tumors, path_tumor_size_cm AS tumor_size_cm, path_tumor_size_cm, laterality, bilateral_disease_flag, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ata_risk_category, ata_initial_risk, ata_response_category, macis_score, macis_risk_group, ages_score, ames_risk, ln_positive_flag, ln_total_examined, ln_total_positive, ln_ratio, ln_burden_band, ln_lateral_dissected, ln_rollup_total_positive, ln_rollup_total_examined, ln_rollup_central_examined, ln_rollup_central_positive, ln_rollup_ene, ete_grade, ete_refined_grade, gross_ete_flag, worst_ete_v10, margin_status, r_class_true, closest_margin_mm, capsular_invasion_refined, lvi_grade, lvi_ordinal_worst, vasc_grade, vasc_grade_final_v13, pni_positive, syn_frozen_section, syn_frozen_section_result, syn_carcinoma_on_frozen, syn_graves, syn_hashimoto, syn_chronic_thyroiditis, syn_follicular_adenoma, syn_hurthle_cell_change, syn_multinodular_goiter, syn_hyperplastic_nodules, syn_capsular_invasion_clean, syn_lymphatic_invasion_clean, syn_margin_status_synoptic, syn_margin_distance_mm_num, syn_n_parathyroid_identified, syn_parathyroid_in_specimen, syn_histologic_grade, syn_ki67_index, syn_isthmus_size_cm, syn_left_lobe_size_cm, syn_right_lobe_size_cm, syn_total_weight_g, syn_left_lobe_weight_g, syn_right_lobe_weight_g, syn_has_second_tumor, syn_tumor2_histologic_type, syn_tumor2_size_cm, syn_central_dissection, syn_bilateral_neck_dissection, gland_weight_final_g, preop_imaging_size_cm, dominant_nodule_size_cm, bethesda_final, bethesda_final_name, n_fna_episodes, fna_path_concordance_category, fna_path_concordant, cross_fna_concordance, tirads_best_category_v12, tirads_worst_category_v12, tirads_best_score_v12, tirads_nodule_size_max_mm_v12, mol_platform, mol_genes_list, mol_has_thyroseq, mol_has_afirma, braf_positive_final, ras_positive_final, tert_positive_final, molecular_tested_confirmed, molecular_risk_tier, mol_n_tests, para_specimen_included, para_incidental_status_refined, para_abnormality_type, para_n_glands_identified, para_has_pathologic_glands, para_removal_intent, rai_received_reconciled AS rai_received_flag, rai_max_dose_mci, rai_total_cumulative_dose_mci, n_rai_episodes, rai_avid_flag, rai_intent_v9, tg_n_measurements, tg_trajectory_class, tg_nadir, tg_last_value, tg_rising_flag, tg_peak, days_first_to_last_tg, lab_tsh_n_measurements, lab_tsh_most_recent, lab_pth_n_measurements, lab_pth_most_recent, lab_calcium_n_measurements, lab_calcium_most_recent, any_recurrence_flag, recurrence_type, recurrence_site, time_to_recurrence_days, structural_recurrence_flag, overall_survival_years, vital_status, death_occurred, followup_years, followup_category, comp_hypoparathyroidism_confirmed, comp_hypocalcemia_confirmed, comp_rln_injury_confirmed, comp_hematoma_confirmed, pmhx_nlp_men_syndrome, pmhx_nlp_autoimmune_thyroid_hx, pmhx_nlp_prior_cancer_hx, pmhx_nlp_radiation_exposure, pmhx_nlp_hypothyroidism, pmhx_nlp_hyperthyroidism, pmhx_nlp_family_hx_thyroid, pmhx_nlp_family_hx_cancer, nlp_frozensec_has_data, nlp_frozensec_key_finding, op_nlp_parathyroid_managed, op_nlp_parathyroid_autograft, op_nlp_nerve_monitoring_used, op_nlp_reoperative_field, ajcc8_calculable_flag, ata_calculable_flag, macis_calculable_flag FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS
CREATE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS SELECT research_id, age_at_surgery, sex, race, bmi_combined, surg_procedure_type, surg_hemithyroidectomy, surg_total_thyroidectomy, first_surgery_date, n_surgeries, op_reoperative_any, histology_final, diagnosis_primary, diagnosis_variant, is_malignant, multifocal_flag_path, n_tumors, path_tumor_size_cm AS tumor_size_cm, path_tumor_size_cm, laterality, bilateral_disease_flag, ajcc8_stage_group, ajcc8_t_stage_corrected, ajcc8_n_stage, ajcc8_m_stage, ata_risk_category, ata_initial_risk, ata_response_category, macis_score, macis_risk_group, ages_score, ames_risk, ln_positive_flag, ln_total_examined, ln_total_positive, ln_ratio, ln_burden_band, ln_lateral_dissected, ln_rollup_total_positive, ln_rollup_total_examined, ln_rollup_central_examined, ln_rollup_central_positive, ln_rollup_ene, ete_grade, ete_refined_grade, gross_ete_flag, worst_ete_v10, margin_status, r_class_true, closest_margin_mm, capsular_invasion_refined, lvi_grade, lvi_ordinal_worst, vasc_grade, vasc_grade_final_v13, pni_positive, syn_frozen_section, syn_frozen_section_result, syn_carcinoma_on_frozen, syn_graves, syn_hashimoto, syn_chronic_thyroiditis, syn_follicular_adenoma, syn_hurthle_cell_change, syn_multinodular_goiter, syn_hyperplastic_nodules, syn_capsular_invasion_clean, syn_lymphatic_invasion_clean, syn_margin_status_synoptic, syn_margin_distance_mm_num, syn_n_parathyroid_identified, syn_parathyroid_in_specimen, syn_histologic_grade, syn_ki67_index, syn_isthmus_size_cm, syn_left_lobe_size_cm, syn_right_lobe_size_cm, syn_total_weight_g, syn_left_lobe_weight_g, syn_right_lobe_weight_g, syn_has_second_tumor, syn_tumor2_histologic_type, syn_tumor2_size_cm, syn_central_dissection, syn_bilateral_neck_dissection, gland_weight_final_g, preop_imaging_size_cm, dominant_nodule_size_cm, bethesda_final, bethesda_final_name, n_fna_episodes, fna_path_concordance_category, fna_path_concordant, cross_fna_concordance, tirads_best_category_v12, tirads_worst_category_v12, tirads_best_score_v12, tirads_nodule_size_max_mm_v12, mol_platform, mol_genes_list, mol_has_thyroseq, mol_has_afirma, braf_positive_final, ras_positive_final, tert_positive_final, molecular_tested_confirmed, molecular_risk_tier, mol_n_tests, para_specimen_included, para_incidental_status_refined, para_abnormality_type, para_n_glands_identified, para_has_pathologic_glands, para_removal_intent, rai_received_reconciled AS rai_received_flag, rai_max_dose_mci, rai_total_cumulative_dose_mci, n_rai_episodes, rai_avid_flag, rai_intent_v9, tg_n_measurements, tg_trajectory_class, tg_nadir, tg_last_value, tg_rising_flag, tg_peak, days_first_to_last_tg, lab_tsh_n_measurements, lab_tsh_most_recent, lab_pth_n_measurements, lab_pth_most_recent, lab_calcium_n_measurements, lab_calcium_most_recent, any_recurrence_flag, recurrence_type, recurrence_site, time_to_recurrence_days, structural_recurrence_flag, overall_survival_years, vital_status, death_occurred, followup_years, followup_category, comp_hypoparathyroidism_confirmed, comp_hypocalcemia_confirmed, comp_rln_injury_confirmed, comp_hematoma_confirmed, pmhx_nlp_men_syndrome, pmhx_nlp_autoimmune_thyroid_hx, pmhx_nlp_prior_cancer_hx, pmhx_nlp_radiation_exposure, pmhx_nlp_hypothyroidism, pmhx_nlp_hyperthyroidism, pmhx_nlp_family_hx_thyroid, pmhx_nlp_family_hx_cancer, nlp_frozensec_has_data, nlp_frozensec_key_finding, op_nlp_parathyroid_managed, op_nlp_parathyroid_autograft, op_nlp_nerve_monitoring_used, op_nlp_reoperative_field, ajcc8_calculable_flag, ata_calculable_flag, macis_calculable_flag FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
```

## `manuscript_workspace.cohort_m007_rss_reclassification_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m007_rss_reclassification_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, path_tumor_size_cm AS tumor_size_cm, ata_risk_category, ata_initial_risk, ata_response_category, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, macis_score, macis_risk_group, ages_score, ames_risk, ln_positive_flag, ete_grade, rai_received_reconciled AS rai_received_flag, any_recurrence_flag, recurrence_type, time_to_recurrence_days, structural_recurrence_flag, overall_survival_years, vital_status, followup_years FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master WHERE (ata_risk_category IS NOT NULL);
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m007_rss_reclassification_v1 AS
CREATE VIEW manuscript_workspace.cohort_m007_rss_reclassification_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, path_tumor_size_cm AS tumor_size_cm, ata_risk_category, ata_initial_risk, ata_response_category, ajcc8_stage_group, ajcc8_t_stage_corrected, ajcc8_n_stage, macis_score, macis_risk_group, ages_score, ames_risk, ln_positive_flag, ete_grade, rai_received_reconciled AS rai_received_flag, any_recurrence_flag, recurrence_type, time_to_recurrence_days, structural_recurrence_flag, overall_survival_years, vital_status, followup_years FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master WHERE (ata_risk_category IS NOT NULL);
```

## `manuscript_workspace.cohort_m036_ata_risk_comparison_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m036_ata_risk_comparison_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.histology_final, p.is_malignant, p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path, p.bilateral_disease_flag, p.ete_grade_final, p.gross_ete_flag, p.lvi_grade, p.vascular_invasion_final, p.ln_positive_flag, p.ln_total_positive, p.ln_total_examined, p.ln_ene_status, p.ata_risk_category, p.ata_initial_risk, p.ata_response_category, p.ajcc8_stage_group, p.ajcc8_t_stage, p.ajcc8_n_stage, p.rai_received_reconciled AS rai_received_flag, p.rai_total_cumulative_dose_mci, p.tg_nadir, p.tg_rising_flag, p.any_recurrence_flag, p.structural_recurrence_flag, p.followup_years, p.overall_survival_years, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE (p.is_malignant = CAST('t' AS BOOLEAN));
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m036_ata_risk_comparison_v1 AS
CREATE VIEW manuscript_workspace.cohort_m036_ata_risk_comparison_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.histology_final, p.is_malignant, p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path, p.bilateral_disease_flag, p.ete_grade_final, p.gross_ete_flag, p.lvi_grade, p.vascular_invasion_final, p.ln_positive_flag, p.ln_total_positive, p.ln_total_examined, p.ln_ene_status, p.ata_risk_category, p.ata_initial_risk, p.ata_response_category, p.ajcc8_stage_group, p.ajcc8_t_stage_corrected, p.ajcc8_n_stage, p.rai_received_reconciled AS rai_received_flag, p.rai_total_cumulative_dose_mci, p.tg_nadir, p.tg_rising_flag, p.any_recurrence_flag, p.structural_recurrence_flag, p.followup_years, p.overall_survival_years, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE (p.is_malignant = CAST('t' AS BOOLEAN));
```

## `manuscript_workspace.cohort_m043_ln_predictors_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m043_ln_predictors_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.race, p.histology_final, p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path, p.ete_grade_final, p.gross_ete_flag, p.lvi_grade, p.vascular_invasion_final, p.perineural_invasion, p.braf_positive_final, p.ras_positive_final, p.tert_positive_final, p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio, p.ln_ene_status, p.ln_burden_band, p.ln_rollup_central_positive, p.ln_rollup_central_examined, p.ln_rollup_lateral_right_positive, p.ln_rollup_lateral_left_positive, p.ln_rollup_total_levels_involved, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_stage_group, p.ata_risk_category, p.any_recurrence_flag, p.followup_years, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE (p.is_malignant = CAST('t' AS BOOLEAN));
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m043_ln_predictors_v1 AS
CREATE VIEW manuscript_workspace.cohort_m043_ln_predictors_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.race, p.histology_final, p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path, p.ete_grade_final, p.gross_ete_flag, p.lvi_grade, p.vascular_invasion_final, p.perineural_invasion, p.braf_positive_final, p.ras_positive_final, p.tert_positive_final, p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio, p.ln_ene_status, p.ln_burden_band, p.ln_rollup_central_positive, p.ln_rollup_central_examined, p.ln_rollup_lateral_right_positive, p.ln_rollup_lateral_left_positive, p.ln_rollup_total_levels_involved, p.ajcc8_t_stage_corrected, p.ajcc8_n_stage, p.ajcc8_stage_group, p.ata_risk_category, p.any_recurrence_flag, p.followup_years, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE (p.is_malignant = CAST('t' AS BOOLEAN));
```

## `manuscript_workspace.cohort_m044_ajcc_ete_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m044_ajcc_ete_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.histology_final, p.path_tumor_size_cm AS tumor_size_cm, p.ete_grade_final, p.ete_grade, p.ete_grade_source, p.gross_ete_flag, p.path_gross_ete_flag, p.ete_op_note_grade, p.ete_original_grade, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_m_stage, p.ajcc8_stage_group, p.ln_positive_flag, p.ln_total_positive, p.lvi_grade, p.vascular_invasion_final, p.ata_risk_category, p.rai_received_reconciled AS rai_received_flag, p.any_recurrence_flag, p.structural_recurrence_flag, p.followup_years, p.overall_survival_years, p.death_occurred, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE ((p.is_malignant = CAST('t' AS BOOLEAN)) AND (p.ajcc8_stage_group IS NOT NULL));
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m044_ajcc_ete_v1 AS
CREATE VIEW manuscript_workspace.cohort_m044_ajcc_ete_v1 AS SELECT p.research_id, p.age_at_surgery, p.sex, p.histology_final, p.path_tumor_size_cm AS tumor_size_cm, p.ete_grade_final, p.ete_grade, p.ete_grade_source, p.gross_ete_flag, p.path_gross_ete_flag, p.ete_op_note_grade, p.ete_original_grade, p.ajcc8_t_stage_corrected, p.ajcc8_n_stage, p.ajcc8_m_stage, p.ajcc8_stage_group, p.ln_positive_flag, p.ln_total_positive, p.lvi_grade, p.vascular_invasion_final, p.ata_risk_category, p.rai_received_reconciled AS rai_received_flag, p.any_recurrence_flag, p.structural_recurrence_flag, p.followup_years, p.overall_survival_years, p.death_occurred, p.surg_procedure_type, p.surg_first_date FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p WHERE ((p.is_malignant = CAST('t' AS BOOLEAN)) AND (p.ajcc8_stage_group IS NOT NULL));
```

## `manuscript_workspace.cohort_m048_tnm_multifocal_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m048_tnm_multifocal_v1 AS SELECT research_id, age_at_surgery, sex, race, surg_procedure_type, is_malignant, histology_final, multifocal_flag_path, n_tumors, tumor_size_cm, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ln_positive_flag, ln_total_examined, ln_total_positive, ete_grade, any_recurrence_flag, overall_survival_years, vital_status, bilateral_disease_flag, syn_has_second_tumor, syn_tumor2_histologic_type, syn_tumor2_size_cm FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m048_tnm_multifocal_v1 AS
CREATE VIEW manuscript_workspace.cohort_m048_tnm_multifocal_v1 AS SELECT research_id, age_at_surgery, sex, race, surg_procedure_type, is_malignant, histology_final, multifocal_flag_path, n_tumors, tumor_size_cm, ajcc8_stage_group, ajcc8_t_stage_corrected, ajcc8_n_stage, ajcc8_m_stage, ln_positive_flag, ln_total_examined, ln_total_positive, ete_grade, any_recurrence_flag, overall_survival_years, vital_status, bilateral_disease_flag, syn_has_second_tumor, syn_tumor2_histologic_type, syn_tumor2_size_cm FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

## `manuscript_workspace.cohort_m050_tumor_size_volume_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m050_tumor_size_volume_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, path_tumor_size_cm, preop_imaging_size_cm, dominant_nodule_size_cm, tirads_nodule_size_max_mm_v12, ajcc8_t_stage, ajcc8_stage_group, ata_risk_category, ln_positive_flag, ete_grade, any_recurrence_flag, overall_survival_years FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m050_tumor_size_volume_v1 AS
CREATE VIEW manuscript_workspace.cohort_m050_tumor_size_volume_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, path_tumor_size_cm, preop_imaging_size_cm, dominant_nodule_size_cm, tirads_nodule_size_max_mm_v12, ajcc8_t_stage_corrected, ajcc8_stage_group, ata_risk_category, ln_positive_flag, ete_grade, any_recurrence_flag, overall_survival_years FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

## `manuscript_workspace.cohort_m051_ete_ln_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m051_ete_ln_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, ete_grade, ete_refined_grade, gross_ete_flag, worst_ete_v10, ln_positive_flag, ln_total_examined, ln_total_positive, ln_ratio, ln_burden_band, ln_rollup_ene, capsular_invasion_refined, lvi_ordinal_worst, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ata_risk_category, any_recurrence_flag, overall_survival_years, vital_status FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m051_ete_ln_v1 AS
CREATE VIEW manuscript_workspace.cohort_m051_ete_ln_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, ete_grade, ete_refined_grade, gross_ete_flag, worst_ete_v10, ln_positive_flag, ln_total_examined, ln_total_positive, ln_ratio, ln_burden_band, ln_rollup_ene, capsular_invasion_refined, lvi_ordinal_worst, ajcc8_stage_group, ajcc8_t_stage_corrected, ajcc8_n_stage, ata_risk_category, any_recurrence_flag, overall_survival_years, vital_status FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

## `manuscript_workspace.cohort_m059_prognostic_scoring_v1`

### Live definition

```sql
CREATE VIEW manuscript_workspace.cohort_m059_prognostic_scoring_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ata_risk_category, ata_initial_risk, ata_response_category, macis_score, macis_risk_group, ages_score, ames_risk, ajcc8_calculable_flag, ata_calculable_flag, macis_calculable_flag, ln_positive_flag, ete_grade, rai_received_flag, any_recurrence_flag, time_to_recurrence_days, overall_survival_years, vital_status FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

### Proposed migrated definition

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m059_prognostic_scoring_v1 AS
CREATE VIEW manuscript_workspace.cohort_m059_prognostic_scoring_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, ajcc8_stage_group, ajcc8_t_stage_corrected, ajcc8_n_stage, ata_risk_category, ata_initial_risk, ata_response_category, macis_score, macis_risk_group, ages_score, ames_risk, ajcc8_calculable_flag, ata_calculable_flag, macis_calculable_flag, ln_positive_flag, ete_grade, rai_received_flag, any_recurrence_flag, time_to_recurrence_days, overall_survival_years, vital_status FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
```

