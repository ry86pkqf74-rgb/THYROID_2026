# Schema Inventory

Generated: 20260318_0604

## Table-by-Table

### master_cohort (actual: master_cohort)

- **Rows**: 11,673
- **Columns**: 17
- **Column list**: research_id, age_at_surgery, sex, surgery_date, has_anti_thyroglobulin_labs, has_benign_pathology, has_ct_imaging, has_fna_cytology, has_frozen_sections, has_mri_imaging, has_nuclear_med, has_parathyroid, has_thyroglobulin_labs, has_thyroid_sizes, has_thyroid_weights, has_tumor_pathology, has_ultrasound_reports


### manuscript_cohort_v1 (actual: manuscript_cohort_v1)

- **Rows**: 10,871
- **Columns**: 150
- **Column list**: research_id, age_at_surgery, sex, race, demo_source, demo_confidence, path_histology_raw, histology_final, histology_source, path_histology_variant_raw, path_t_stage_raw, path_n_stage_raw, path_m_stage_raw, path_stage_raw, path_tumor_size_cm, path_ete_raw, ete_grade_final, ete_grade_source, path_gross_ete_flag, path_vascular_invasion_raw, vascular_invasion_final, vascular_vessel_count, path_lvi_raw, path_pni_raw, path_margin_raw, margin_status_final, closest_margin_mm, path_ln_positive_raw, path_ln_examined_raw, ln_positive_final, path_ene_raw, path_laterality, path_multifocal_flag, path_n_tumors, first_surgery_date, lateral_neck_dissected, mol_platform, mol_test_date, mol_n_tests, braf_positive_final, braf_source, braf_variant_raw, ras_positive_final, ras_subtype_raw, tert_positive_final, fna_bethesda_final, fna_bethesda_source, fna_bethesda_confidence, imaging_tirads_best, imaging_tirads_worst, imaging_tirads_category, imaging_tirads_source, imaging_nodule_size_cm, imaging_n_nodule_records, surg_procedure_type, surg_first_date, surg_n_procedures, surg_total_thyroidectomy, surg_hemithyroidectomy, rai_received_flag, rai_first_date, rai_max_dose_mci, rai_assertion_statuses, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, ajcc8_calculable_flag, ajcc8_missing_components, ata_risk_category, ata_calculable_flag, ata_response_category, ata_response_calculable_flag, macis_score, macis_risk_group, macis_calculable_flag, ages_score, ames_risk_group, ln_ratio, ln_burden_band, molecular_risk_tier, hypocalcemia_status, hypoparathyroidism_status, rln_status, hematoma_status, seroma_status, chyle_leak_status, wound_infection_status, any_confirmed_complication, n_confirmed_complications, calcium_supplement_required, rln_permanent_flag, rln_transient_flag, tg_nadir, tg_last_value, tg_peak, tg_n_measurements, tg_rising_flag, tg_below_threshold_ever, anti_tg_nadir, anti_tg_rising_flag, tsh_suppressed_ever, pth_nadir, calcium_nadir, postop_low_pth_flag, postop_low_calcium_flag, lab_completeness_score, any_recurrence_flag, recurrence_date, structural_recurrence_flag, biochemical_recurrence_flag, recurrence_type_primary, recurrence_site_primary, recurrence_source, date_traceability_status, provenance_confidence, analysis_eligible_flag, molecular_eligible_flag, rai_eligible_flag, survival_eligible_flag, scoring_ajcc8_flag, scoring_ata_flag, scoring_macis_flag, resolved_layer_version, source_script, resolved_at, surgery_date, path_ete_final, mol_braf_positive_final, mol_tert_positive_final, path_multifocal_final, path_age_at_surgery_raw, demo_sex_final, demo_race_final, cohort_build_timestamp, cohort_resolved_layer_version, freeze_git_sha, local DuckDB_database, pipeline_version, op_rln_monitoring_any, op_drain_placed_any, op_strap_muscle_any, op_reoperative_any, op_parathyroid_autograft_any, op_local_invasion_any, op_tracheal_inv_any, op_esophageal_inv_any, op_intraop_gross_ete_any, op_n_surgeries_with_findings, op_findings_summary


### analysis_cancer_cohort_v1 (actual: analysis_cancer_cohort_v1)

- **Rows**: 4,136
- **Columns**: 136
- **Column list**: research_id, age_at_surgery, sex, race, demo_source, demo_confidence, path_histology_raw, histology_final, histology_source, path_histology_variant_raw, path_t_stage_raw, path_n_stage_raw, path_m_stage_raw, path_stage_raw, path_tumor_size_cm, path_ete_raw, ete_grade_final, ete_grade_source, path_gross_ete_flag, path_vascular_invasion_raw, vascular_invasion_final, vascular_vessel_count, path_lvi_raw, path_pni_raw, path_margin_raw, margin_status_final, closest_margin_mm, path_ln_positive_raw, path_ln_examined_raw, ln_positive_final, path_ene_raw, path_laterality, path_multifocal_flag, path_n_tumors, first_surgery_date, lateral_neck_dissected, mol_platform, mol_test_date, mol_n_tests, braf_positive_final, braf_source, braf_variant_raw, ras_positive_final, ras_subtype_raw, tert_positive_final, fna_bethesda_final, fna_bethesda_source, fna_bethesda_confidence, imaging_tirads_best, imaging_tirads_worst, imaging_tirads_category, imaging_tirads_source, imaging_nodule_size_cm, imaging_n_nodule_records, surg_procedure_type, surg_first_date, surg_n_procedures, surg_total_thyroidectomy, surg_hemithyroidectomy, rai_received_flag, rai_first_date, rai_max_dose_mci, rai_assertion_statuses, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, ajcc8_calculable_flag, ajcc8_missing_components, ata_risk_category, ata_calculable_flag, ata_response_category, ata_response_calculable_flag, macis_score, macis_risk_group, macis_calculable_flag, ages_score, ames_risk_group, ln_ratio, ln_burden_band, molecular_risk_tier, hypocalcemia_status, hypoparathyroidism_status, rln_status, hematoma_status, seroma_status, chyle_leak_status, wound_infection_status, any_confirmed_complication, n_confirmed_complications, calcium_supplement_required, rln_permanent_flag, rln_transient_flag, tg_nadir, tg_last_value, tg_peak, tg_n_measurements, tg_rising_flag, tg_below_threshold_ever, anti_tg_nadir, anti_tg_rising_flag, tsh_suppressed_ever, pth_nadir, calcium_nadir, postop_low_pth_flag, postop_low_calcium_flag, lab_completeness_score, any_recurrence_flag, recurrence_date, structural_recurrence_flag, biochemical_recurrence_flag, recurrence_type_primary, recurrence_site_primary, recurrence_source, date_traceability_status, provenance_confidence, analysis_eligible_flag, molecular_eligible_flag, rai_eligible_flag, survival_eligible_flag, scoring_ajcc8_flag, scoring_ata_flag, scoring_macis_flag, resolved_layer_version, source_script, resolved_at, ajcc8_stage_group_1, ajcc8_t_stage_1, ajcc8_n_stage_1, ajcc8_m_stage_1, ata_initial_risk, macis_score_1, macis_risk_group_1, ages_score_1, ames_risk, molecular_risk_tier_1


### patient_analysis_resolved_v1 (actual: patient_analysis_resolved_v1)

- **Rows**: 10,871
- **Columns**: 145
- **Column list**: research_id, age_at_surgery, sex, race, demo_source, demo_confidence, path_histology_raw, histology_final, histology_source, path_histology_variant_raw, path_t_stage_raw, path_n_stage_raw, path_m_stage_raw, path_stage_raw, path_tumor_size_cm, path_ete_raw, ete_grade_final, ete_grade_source, path_gross_ete_flag, path_vascular_invasion_raw, vascular_invasion_final, vascular_vessel_count, path_lvi_raw, path_pni_raw, path_margin_raw, margin_status_final, closest_margin_mm, path_ln_positive_raw, path_ln_examined_raw, ln_positive_final, path_ene_raw, path_laterality, path_multifocal_flag, path_n_tumors, first_surgery_date, lateral_neck_dissected, mol_platform, mol_test_date, mol_test_date_source, mol_n_tests, braf_positive_final, braf_source, braf_detection_method, braf_variant_raw, ras_positive_final, ras_subtype_raw, tert_positive_final, fna_bethesda_final, fna_bethesda_source, fna_bethesda_confidence, imaging_tirads_best, imaging_tirads_worst, imaging_tirads_category, imaging_tirads_source, imaging_nodule_size_cm, imaging_n_nodule_records, surg_procedure_type, surg_first_date, surg_n_procedures, surg_total_thyroidectomy, surg_hemithyroidectomy, rai_received_flag, rai_first_date, rai_max_dose_mci, rai_assertion_statuses, rai_date_source, rai_date_confidence, rai_validation_tier, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, ajcc8_calculable_flag, ajcc8_missing_components, ata_risk_category, ata_calculable_flag, ata_response_category, ata_response_calculable_flag, macis_score, macis_risk_group, macis_calculable_flag, ages_score, ames_risk_group, ln_ratio, ln_burden_band, molecular_risk_tier, hypocalcemia_status, hypoparathyroidism_status, rln_status, hematoma_status, seroma_status, chyle_leak_status, wound_infection_status, any_confirmed_complication, n_confirmed_complications, calcium_supplement_required, rln_permanent_flag, rln_transient_flag, tg_nadir, tg_last_value, tg_peak, tg_n_measurements, tg_rising_flag, tg_below_threshold_ever, anti_tg_nadir, anti_tg_rising_flag, tsh_suppressed_ever, pth_nadir, calcium_nadir, postop_low_pth_flag, postop_low_calcium_flag, lab_completeness_score, any_recurrence_flag, recurrence_date, recurrence_date_source, structural_recurrence_flag, biochemical_recurrence_flag, recurrence_type_primary, recurrence_site_primary, recurrence_source, date_traceability_status, provenance_confidence, analysis_eligible_flag, molecular_eligible_flag, rai_eligible_flag, survival_eligible_flag, scoring_ajcc8_flag, scoring_ata_flag, scoring_macis_flag, resolved_layer_version, source_script, resolved_at, source_table, provenance_note, op_rln_monitoring_any, op_drain_placed_any, op_strap_muscle_any, op_reoperative_any, op_parathyroid_autograft_any, op_local_invasion_any, op_tracheal_inv_any, op_esophageal_inv_any, op_intraop_gross_ete_any, op_n_surgeries_with_findings, op_findings_summary


### episode_analysis_resolved_v1_dedup (actual: episode_analysis_resolved_v1_dedup)

- **Rows**: 9,368
- **Columns**: 45
- **Column list**: research_id, surgery_episode_id, surgery_date, procedure_type, surg_laterality, rln_monitoring_flag, rln_finding_raw, intraop_gross_ete, parathyroid_resection_flag, drain_flag, linked_path_tumor_ordinal, path_link_confidence_v2, path_link_score_v3, path_link_tier_v3, path_link_reason, path_link_eligible, histology, t_stage, n_stage, tumor_size_cm, extrathyroidal_extension, gross_ete, vascular_invasion, margin_status, ln_positive, ln_examined, linked_fna_episode_id, linked_fna_type, fna_link_confidence_v2, fna_link_score_v3, linked_rai_episode_id, rai_link_confidence_v2, rai_link_score_v3, rai_assertion_status, rai_dose_mci, episode_analysis_eligible_flag, resolved_layer_version, resolved_at, parathyroid_autograft_flag, local_invasion_flag, tracheal_involvement_flag, esophageal_involvement_flag, strap_muscle_involvement_flag, reoperative_field_flag, operative_findings_raw


### imaging_nodule_master_v1 (actual: imaging_nodule_master_v1)

- **Rows**: 19,891
- **Columns**: 25
- **Column list**: research_id, exam_date, nodule_number, exam_id, nodule_id, tirads_reported, tirads_acr_recalculated, composition, echogenicity, shape, margins, calcifications, length_mm, width_mm, height_mm, volume_ml, location_raw, laterality, source_table, tirads_concordant_flag, max_dimension_cm, tirads_category, suspicious_flag, linked_fna_episode_id, fna_link_score_v3


### imaging_patient_summary_v1 (actual: imaging_patient_summary_v1)

- **Rows**: 3,474
- **Columns**: 13
- **Column list**: research_id, n_exams, n_total_nodules, max_tirads_ever, bilateral_disease_flag, dominant_nodule_size_cm, multifocal_flag, has_suspicious_candidate, first_exam_date, last_exam_date, worst_tirads_category, longitudinal_assessment_available, created_at


### extracted_tirads_validated_v1 (actual: extracted_tirads_validated_v1)

- **Rows**: 3,474
- **Columns**: 15
- **Column list**: research_id, tirads_best_score, tirads_worst_score, tirads_best_category, tirads_worst_category, tirads_source, tirads_reliability, has_acr_recalculation, has_scored_excel, has_nlp, n_sources, n_nodule_records, concordant_count, mismatch_count, nodule_size_max_mm


### operative_episode_detail_v2 (actual: operative_episode_detail_v2)

- **Rows**: 9,371
- **Columns**: 39
- **Column list**: research_id, surgery_episode_id, surgery_date_native, resolved_surgery_date, date_status, procedure_raw, procedure_normalized, laterality, central_neck_dissection_flag, lateral_neck_dissection_flag, rln_monitoring_flag, rln_finding_raw, parathyroid_autograft_flag, parathyroid_autograft_count, parathyroid_autograft_site, parathyroid_resection_flag, gross_ete_flag, local_invasion_flag, tracheal_involvement_flag, esophageal_involvement_flag, strap_muscle_involvement_flag, reoperative_field_flag, ebl_ml, drain_flag, operative_findings_raw, source_tables, op_confidence, note_date_resolved, note_date_source, note_date_confidence, parathyroid_identified_count, frozen_section_flag, berry_ligament_flag, ebl_ml_nlp, op_enrichment_source, linked_pathology_episode_id, path_link_score_v3, linked_fna_episode_id, fna_link_score_v3


### molecular_test_episode_v2 (actual: molecular_test_episode_v2)

- **Rows**: 10,126
- **Columns**: 42
- **Column list**: research_id, molecular_episode_id, platform_raw, platform, result, mutation, detailed_findings_raw, test_date_native, date_status, date_confidence, overall_result_class, braf_flag, braf_variant, ras_flag, ras_subtype, ret_flag, ret_fusion_flag, tert_flag, ntrk_flag, eif1ax_flag, tp53_flag, pax8_pparg_flag, cna_flag, fusion_flag, loh_flag, alk_flag, high_risk_marker_flag, inadequate_flag, cancelled_flag, source_table, resolved_test_date, linked_fna_episode_id, linked_nodule_id, linked_surgery_episode_id, specimen_site_raw, specimen_site_normalized, bethesda_category, platform_version, risk_language_raw, molecular_confidence, adjudication_status, fna_link_score_v3


### rai_treatment_episode_v2 (actual: rai_treatment_episode_v2)

- **Rows**: 1,857
- **Columns**: 32
- **Column list**: research_id, rai_episode_id, rai_mention_raw, rai_term_normalized, rai_date_native, note_date_parsed, resolved_rai_date, date_status, date_confidence, dose_mci, dose_text_raw, rai_assertion_status, rai_intent, completion_status, rai_confidence, source_note_id, source_note_type, source_table, adjudication_status, linked_surgery_episode_id, linked_pathology_episode_id, linked_recurrence_episode_id, pre_scan_flag, post_therapy_scan_flag, scan_findings_raw, iodine_avidity_flag, stimulated_tg, stimulated_tsh, dose_source, dose_confidence, surgery_link_score_v3, dose_missingness_reason


### longitudinal_lab_canonical_v1 (actual: longitudinal_lab_canonical_v1)

- **Rows**: 39,961
- **Columns**: 18
- **Column list**: research_id, lab_date, lab_date_status, lab_name_raw, lab_name_standardized, analyte_group, value_raw, value_numeric, unit_raw, unit_standardized, reference_range, abnormal_flag, is_censored, source_table, source_script, ingestion_wave, data_completeness_tier, provenance_note


### recurrence_risk_features_mv (actual: recurrence_risk_features_mv)

- **Rows**: 4,976
- **Columns**: 25
- **Column list**: research_id, surgery_date, histology_1_type, pt_stage, pn_stage, overall_stage, ete, gross_ete, tumor_size_cm, ln_positive, ln_examined, ln_ratio, braf_positive, ras_positive, ret_positive, tert_positive, tg_first, tg_last, tg_max, tg_mean, tg_measurement_count, tg_annual_log_slope, recurrence_flag, first_recurrence_date, recurrence_risk_band


### provenance_enriched_events_v1 (actual: provenance_enriched_events_v1)

- **Rows**: 50,297
- **Columns**: 17
- **Column list**: research_id, event_type, event_subtype, event_value, event_unit, event_date, event_text, source_column, followup_date, days_since_nearest_surgery, nearest_surgery_number, confidence_score, specimen_collect_dt, event_date_correct, date_status_final, direct_source_link, provenance_created_at


### patient_refined_master_clinical_v12 (actual: patient_refined_master_clinical_v12)

- **Rows**: 12,886
- **Columns**: 272
- **Column list**: research_id, ete_path_confirmed, ete_grade_v3, margin_status_refined, closest_margin_mm, vascular_invasion_refined, lvi_refined, perineural_invasion_refined, capsular_invasion_refined, tumor_size_path_cm, tumor_size_imaging_cm, braf_positive_v3, ras_positive_v3, tert_positive_v3, molecular_platform_v3, recurrence_confirmed, recurrence_risk_band, ete_grade_v5, ete_op_note_subgrade, subgrade_method, tert_positive_v5, tert_source, tert_tested, braf_positive_v5, braf_source, ras_positive_v5, ret_positive_refined, ntrk_positive_refined, tp53_positive_refined, high_risk_marker_any, platforms_used, n_molecular_tests, pth_nadir_value, pth_nadir_days_postop, calcium_nadir_value, calcium_nadir_days_postop, n_rai_episodes, confirmed_rai_episodes, max_dose_mci, first_rai_date, rai_avidity, max_stimulated_tg, rai_validation_tier, rai_source_reliability, ene_status_refined, ene_levels, ene_positive, refined_rln_injury, confirmed_rln_injury, refined_hypocalcemia, confirmed_hypocalcemia, refined_hypoparathyroidism, confirmed_hypoparathyroidism, refined_chyle_leak, refined_seroma, refined_hematoma, refined_at, margin_r_classification, margin_with_gross_ete, margin_source, margin_confidence, vascular_who_2022_grade, vascular_vessel_count, vascular_positive, lvi_positive, pni_refined_v6, pni_positive, capsular_invasion_v6, ln_total_examined, ln_total_positive, ln_ratio, ln_positive_v6, ln_central_dissected, ln_lateral_dissected, ln_levels_raw, ln_source, ln_confidence, ene_grade_v6, ene_source_v6, ene_concordance_v6, ene_confidence_v6, bethesda_final, bethesda_final_name, worst_bethesda_num, n_fna_episodes, fna_n_sources, fna_source_tables, first_fna_date, last_fna_date, cross_fna_concordance, fna_confidence, braf_positive_v7, braf_status_v7, braf_variants, tert_positive_v7, tert_status_v7, tert_promoter_types, ras_positive_v7, ras_subtypes, ret_positive_v7, ret_fusion_positive, ntrk_positive_v7, alk_positive_v7, tp53_positive_v7, eif1ax_positive, pax8_pparg_positive, any_fusion_positive, high_risk_molecular_v7, molecular_methods, molecular_platforms_v7, n_molecular_tests_v7, molecular_risk_category, molecular_tested_v7, fna_path_outcome, fna_path_concordance_category, fna_path_concordant, preop_imaging_size_cm, preop_tirads_score, preop_tirads_category, preop_composition, preop_echogenicity, preop_imaging_date, imaging_days_before_surgery, imaging_suspicious_node, size_discrepancy_cm, size_concordance, ete_imaging_path_concordance, imaging_data_completeness, recurrence_flag_structured, recurrence_any, recurrence_detection_category, recurrence_site_inferred, tg_nadir, tg_max, tg_last_value, tg_rising_flag, n_tg_measurements, rai_treatment_count, max_rai_dose_mci, rai_avid_flag, rai_scan_findings, recurrence_data_confidence, n_recurrence_sources, ata_response_category, rai_response_confidence, post_rai_tg_nadir, post_rai_tg_last, post_rai_tg_count, rai_stimulated_tg, rai_stimulated_tsh, structural_disease_flag, rln_worst_grade, rln_sides, n_rln_assessments, voice_outcome_category, has_voice_data, voice_followup_completeness, days_to_first_laryngoscopy, days_to_last_laryngoscopy, voice_data_confidence, completion_reason, completion_reason_confidence, completion_histology_type, completion_t_stage, completion_prior_histology, completion_braf_positive, completion_tert_positive, followup_completeness_score, followup_tg_labs, tg_adequate_followup, followup_clinical_events, followup_has_complications, pth_n_values, pth_nadir, pth_nadir_30d, hypoparathyroidism_lab_flag, calcium_n_values, calcium_nadir, calcium_nadir_30d, hypocalcemia_lab_flag, ica_n_values, ica_nadir, total_postop_lab_values, lab_extraction_methods, rai_dose_v9, rai_intent_v9, rai_dose_source, rai_dose_linkage, rai_scan_findings_v9, rai_avid_v9, ete_grade_v9, ete_rule_applied, tert_positive_v9, tert_variant_v9, tert_platforms_v9, tert_test_count_v9, ene_grade_v9, ene_levels_v9, ene_record_count_v9, best_ene_grade, ene_path_synoptic, ene_path_nlp, ene_deposit_cm, ene_path_levels, ene_op_intraop, ene_ct, ene_us, ene_pet, ene_rai_scan, ene_n_sources, ene_path_ct_concordance, margin_r_class_v10, margin_status_v10, closest_margin_mm_v10, margin_source_v10, vascular_who_grade_v10, vascular_invasion_v10, vessel_count_v10, vascular_source_v10, lvi_grade_v10, lvi_source_v10, lateral_neck_dissected_v10, lateral_detection_method, lateral_levels_v10, lateral_side_v10, lateral_source_v10, n_tumors_v10, worst_ete_v10, max_tumor_size_cm_v10, total_ln_positive_v10, tirads_score_v11, tirads_category_v11, tirads_confidence_v11, imaging_nodule_size_cm_v11, ras_positive_v11, nras_positive_v11, hras_positive_v11, kras_positive_v11, ras_primary_subtype_v11, ras_protein_change_v11, ras_allele_freq_v11, braf_recovered_status_v11, braf_recovered_variant_v11, braf_detection_method_v11, preop_sweep_genes_found_v11, ras_positive_final, braf_positive_final, tirads_best_score_v12, tirads_worst_score_v12, tirads_best_category_v12, tirads_worst_category_v12, tirads_source_v12, tirads_reliability_v12, tirads_has_acr_recalc_v12, tirads_n_sources_v12, tirads_n_nodule_records_v12, tirads_concordant_count_v12, tirads_mismatch_count_v12, tirads_nodule_size_max_mm_v12, vasc_grade_final_v13, vasc_vessel_count_v13, vasc_source_final_v13, vasc_confidence_final_v13, lvi_grade_final_v13, ihc_braf_result_v13, ihc_braf_note_type_v13, ihc_braf_confidence_v13, ras_resolved_gene_v13, ras_resolved_variant_v13, ras_resolved_af_v13, ras_resolution_source_v13, ras_resolution_confidence_v13


### complication_phenotype_v1 (actual: complication_phenotype_v1)

- **Rows**: 5,928
- **Columns**: 28
- **Column list**: research_id, complication_entity, note_mention_flag, n_raw_nlp_mentions, n_valid_nlp_mentions, suspected_flag, confirmed_flag, transient_flag, permanent_flag, surgery_related_flag, historical_only_flag, timing_days_post_surgery, timing_window, final_complication_status, analysis_eligible_flag, biochemical_low_pth, pth_nadir, biochemical_low_ca, ca_nadir, treatment_requiring_flag, voice_resolution_noted, voice_permanence_noted, evidence_tier, source_tier_label, detection_date, first_surgery_date, phenotyped_at, phenotype_version


### complication_patient_summary_v1 (actual: complication_patient_summary_v1)

- **Rows**: 2,892
- **Columns**: 18
- **Column list**: research_id, hypocalcemia_status, hypoparathyroidism_status, rln_status, hematoma_status, seroma_status, chyle_leak_status, wound_infection_status, any_confirmed_complication_flag, any_analysis_eligible_complication, n_confirmed_complications, earliest_complication_days, has_low_pth_flag, has_low_calcium_flag, calcium_supplement_required, rln_permanent_flag, rln_transient_flag, summarized_at


### extracted_recurrence_refined_v1 (actual: extracted_recurrence_refined_v1)

- **Rows**: 10,871
- **Columns**: 29
- **Column list**: research_id, recurrence_flag_structured, first_recurrence_date, recurrence_risk_band, n_tg_measurements, tg_nadir, tg_max, tg_last_value, tg_rising_flag, first_tg_date, last_tg_date, recurrence_any, detection_category, has_scan_findings, rai_avid, max_rai_dose_mci, n_rai_treatments, scan_findings_combined, last_stimulated_tg, last_stimulated_tsh, recurrence_source, tg_source, rai_source, recurrence_site_inferred, recurrence_data_confidence, n_recurrence_sources, recurrence_date_best, recurrence_date_status, recurrence_date_confidence


### extracted_fna_bethesda_v1 (actual: extracted_fna_bethesda_v1)

- **Rows**: 5,249
- **Columns**: 15
- **Column list**: research_id, bethesda_final, worst_bethesda_num, best_bethesda_num, n_fna_episodes, n_sources, source_tables, first_fna_date, last_fna_date, best_source_reliability, total_records, bethesda_final_name, cross_fna_concordance, confidence, refined_at


### thyroid_scoring_py_v1 (actual: thyroid_scoring_py_v1)

- **Rows**: 10,871
- **Columns**: 51
- **Column list**: research_id, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, ajcc8_t_stage_calculable_flag, ajcc8_stage_calculable_flag, ata_initial_risk, ata_risk_calculable_flag, ata_response_provisional, ata_response_is_provisional, macis_score, macis_risk_group, macis_calculable_flag, macis_missing_components, ages_score, ages_calculable_flag, ames_risk, ames_calculable_flag, ln_ratio, ln_burden_band, ln_burden_n_positive, ln_burden_n_examined, molecular_risk_tier, molecular_risk_calculable_flag, braf_positive_final, tert_positive, ras_positive_final, bethesda_num, bethesda_category, bethesda_confidence, bethesda_source, tumor_size_cm, age_at_surgery, sex, histology, ete_grade, gross_ete_flag, vasc_grade, margin_r_class, aggressive_variant_flag, multifocal_flag, ln_positive, ln_examined, rai_received, max_rai_dose, tg_nadir, tg_max, distant_mets_proxy, recurrence_flag, first_recurrence_date

