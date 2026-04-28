# Date normalization pass 1 -- thyroid_canonical_publication_v1_0

Generated 2026-04-27 by qc_framework_v1/scripts/normalize_dates_v1_0_pass1.py

## Summary by action

| action | n_columns |
|---|---|
| `timestamp_truncate_to_day` | 191 |
| `skip_already_date_typed` | 129 |
| `varchar_normalized_mm_dd_yyyy` | 62 |
| `skip_metadata_name` | 25 |
| `skip_not_date_values` | 20 |
| `skip_empty` | 8 |

## skip_already_date_typed (129 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | canonical_cervical_ln_clinical_events_v1 | `entity_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_complications_events_v1 | `finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_complications_patient_rollup_v1 | `first_complication_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_complications_patient_rollup_v1 | `last_complication_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_esophageal_invasion_events_v1 | `entity_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_ete_event_resolved_v1 | `recurrence_imaging_suspicious_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_ete_event_resolved_v1 | `recurrence_path_proven_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_fna_events_v1 | `fna_date_resolved` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_fna_patient_rollup_v1 | `first_fna_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_fna_patient_rollup_v1 | `last_fna_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_invasion_events_v1 | `finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_medications_events_v1 | `finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_medications_events_v1 | `mention_note_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_medications_patient_rollup_v1 | `first_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_medications_patient_rollup_v1 | `last_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_operative_patient_rollup_v1 | `earliest_surgery_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_operative_patient_rollup_v1 | `latest_surgery_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_operative_procedure_codes_v1 | `note_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_path_benign_events_v1 | `path_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_path_benign_patient_rollup_v1 | `earliest_benign_path_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_path_benign_patient_rollup_v1 | `latest_benign_path_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_path_gland_events_v1 | `path_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_pathology_clinical_events_v1 | `entity_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `biochemical_concern_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `ct_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `ct_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `death_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `first_surgery_date_v2` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `first_tg_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `followup_or_death_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_calcium_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_calcium_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_calcium_most_recent_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_pth_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_pth_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_pth_most_recent_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_tsh_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_tsh_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_tsh_most_recent_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_vitd_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_vitd_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lab_vitd_most_recent_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `last_tg_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lnus_first_abnormal_us_ln_date_v2` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lnus_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `lnus_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `max_stimulated_tg_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `med_nlp_calcitriol_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `med_nlp_calcium_supplement_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `med_nlp_levothyroxine_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `mri_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `mri_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `nlp_rec_earliest_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_esophageal_inv_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_berry_ligament_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_drain_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_ebl_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_intraop_complication_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_nerve_monitoring_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_parathyroid_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `op_nlp_rln_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pet_other_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pet_other_last_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_diabetes_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_hypertension_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_hyperthyroidism_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_hypothyroidism_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_obesity_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pmhx_nlp_radiation_exposure_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `prm_first_fna_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `prm_last_fna_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `proc_nlp_laryngoscopy_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `proc_nlp_tracheostomy_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pshx_nlp_prior_rai_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `pshx_nlp_prior_thyroidectomy_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `rai_first_episode_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `rai_last_episode_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `recurrence_date_v2` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `rln_injury_detection_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `second_surgery_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `second_surgery_date_v2` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `third_surgery_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `third_surgery_date_v2` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `tsh_suppressed_first_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `us_first_exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `us_last_exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_patient_master | `us_most_recent_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_pmh_events_v1 | `finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_pmh_events_v1 | `mention_note_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_pmh_patient_rollup_v1 | `first_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_pmh_patient_rollup_v1 | `last_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_psh_events_v1 | `finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_psh_events_v1 | `mention_note_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_psh_patient_rollup_v1 | `first_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_psh_patient_rollup_v1 | `last_finding_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_recurrence_resolved_v1 | `first_surg_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_recurrence_resolved_v1 | `recurrence_imaging_suspicious_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_recurrence_resolved_v1 | `recurrence_path_proven_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_recurrence_v1 | `recurrence_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_survival_followup_v1 | `death_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_survival_followup_v1 | `first_surgery_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_survival_followup_v1 | `last_known_alive_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_us_lymph_node_v2 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_us_nodule_v2 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | canonical_us_thyroid_gland_v2 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | imaging_exam_master_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | imaging_fna_linkage_v3 | `fna_date` | DATE |  |  |  | DATE storage is canonical |
| main | imaging_patient_summary_v1 | `first_exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | imaging_patient_summary_v1 | `last_exam_date` | DATE |  |  |  | DATE storage is canonical |
| main | specimen_tumor_focus_v1 | `surg_date_canonical` | DATE |  |  |  | DATE storage is canonical |
| main | tg_timeline_patient_summary_v1 | `first_tg_date` | DATE |  |  |  | DATE storage is canonical |
| main | tg_timeline_patient_summary_v1 | `first_tgab_date` | DATE |  |  |  | DATE storage is canonical |
| main | tg_timeline_patient_summary_v1 | `last_tg_date` | DATE |  |  |  | DATE storage is canonical |
| main | tg_timeline_patient_summary_v1 | `last_tgab_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | biochemical_concern_backfill_v1 | `first_concern_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | canonical_deprecation_log_v1 | `deprecated_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | max_stimulated_tg_backfill_v1 | `max_stimulated_tg_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | n_surgeries_v1_v2_conflict_v1 | `second_surgery_date_v2` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | n_surgeries_v1_v2_conflict_v1 | `third_surgery_date_v2` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | qc_tir03_llm_candidates_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | qc_usln01_llm_candidates_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | recurrence_imaging_suspicious_candidates_v1 | `img_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | recurrence_path_proven_candidates_v1 | `path_proven_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | script_396_prestate_benign_v1 | `path_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | script_396_prestate_gland_v1 | `path_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | tsh_suppressed_backfill_v1 | `tsh_suppressed_first_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | us_nodule_conflict_queue_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | us_raw_index0_conflict_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |
| manuscript_workspace | us_raw_index_mismatch_v1 | `exam_date` | DATE |  |  |  | DATE storage is canonical |

## skip_empty (8 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | canonical_table_signoff_registry_v1 | `signed_off_ts` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | canonical_logan_review_log_v1 | `change_ts` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | cpm_ete_self_contradiction_queue_v1 | `flagged_at` | TIMESTAMP WITH TIME ZONE | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | path_tumor_size_chart_review_queue_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | qc_manual_review_queue_v1 | `resolved_at` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | schema_reorg_orphan_references_v1 | `detected_at` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | script_389_prestate_v1 | `build_ts` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |
| manuscript_workspace | v1_1_finalization_audit_v1 | `run_ts` | TIMESTAMP | 0 | 0 | 0 | no non-null values to sample |

## skip_metadata_name (25 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | canonical_cervical_ln_clinical_events_v1 | `date_source_keyword` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_esophageal_invasion_events_v1 | `date_source_keyword` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_fna_events_v1 | `fna_date_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_operative_events_v1 | `date_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_operative_events_v1 | `note_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_pathology_clinical_events_v1 | `date_source_keyword` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `biochemical_concern_first_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `date_traceability_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `gm_rai_date_confidence` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `gm_rai_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `gm_recurrence_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `mol_test_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `rai_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_patient_master | `recurrence_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | canonical_survival_followup_v1 | `death_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | clinical_note_ln_extracted_v1 | `date_source_keyword` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | manuscript_cohort_v1 | `date_traceability_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | rai_treatment_episode_v2 | `date_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| main | specimen_master_v1 | `source_candidate_kind` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | fna_source_long_v1_step_b | `source_col_name_date` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | patient_analysis_resolved_v1 | `date_traceability_status` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | patient_analysis_resolved_v1 | `mol_test_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | patient_analysis_resolved_v1 | `rai_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | patient_analysis_resolved_v1 | `recurrence_date_source` | VARCHAR |  |  |  | name pattern matches metadata regex |
| manuscript_workspace | script_396_prestate_v1 | `source_candidate_kind` | VARCHAR |  |  |  | name pattern matches metadata regex |

## skip_not_date_values (20 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | canonical_cervical_ln_clinical_events_v1 | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | canonical_esophageal_invasion_events_v1 | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | canonical_pathology_clinical_events_v1 | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | canonical_patient_master | `cnln_earliest_date` | VARCHAR |  |  |  | only 20/50 sampled values parse as dates (threshold=70%) |
| main | canonical_patient_master | `cnln_img_first_date` | VARCHAR |  |  |  | only 23/50 sampled values parse as dates (threshold=70%) |
| main | canonical_patient_master | `cnln_img_last_date` | VARCHAR |  |  |  | only 27/50 sampled values parse as dates (threshold=70%) |
| main | canonical_patient_master | `cnln_surg_first_date` | VARCHAR |  |  |  | only 20/50 sampled values parse as dates (threshold=70%) |
| main | canonical_us_nodule_v2 | `updated_tirads_category` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_cervical_ln_detail | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_dynamic_risk_response | `note_date` | VARCHAR |  |  |  | only 15/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_esophageal_invasion | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_past_medical_hx | `note_date` | VARCHAR |  |  |  | only 17/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_past_surgical_hx | `note_date` | VARCHAR |  |  |  | only 17/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_pathology | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_presenting_symptoms | `note_date` | VARCHAR |  |  |  | only 21/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_rai_detailed | `note_date` | VARCHAR |  |  |  | only 18/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_recurrence | `note_date` | VARCHAR |  |  |  | only 17/50 sampled values parse as dates (threshold=70%) |
| main | note_entities_llm_tirads_granular | `note_date` | VARCHAR |  |  |  | only 0/50 sampled values parse as dates (threshold=70%) |
| manuscript_workspace | archive_candidate_review_v1 | `candidate_name` | VARCHAR |  |  |  | only 0/4 sampled values parse as dates (threshold=70%) |
| manuscript_workspace | archive_candidate_review_v1 | `candidate_schema` | VARCHAR |  |  |  | only 0/4 sampled values parse as dates (threshold=70%) |

## timestamp_truncate_to_day (191 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | __readme | `updated_at` | TIMESTAMP | 31 | 31 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_airway_invasion_events_v1 | `build_ts` | TIMESTAMP | 6054 | 6054 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_airway_invasion_events_v1 | `extracted_at` | TIMESTAMP | 6054 | 6054 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_airway_invasion_events_v1 | `llm_build_ts` | TIMESTAMP | 6054 | 6054 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_airway_invasion_patient_rollup_v1 | `build_ts` | TIMESTAMP | 2820 | 2820 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_cervical_ln_clinical_events_v1 | `build_ts` | TIMESTAMP | 4493 | 4493 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_cervical_ln_clinical_patient_rollup_v1 | `build_ts` | TIMESTAMP | 1643 | 1643 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_column_verification_registry_v1 | `registered_ts` | TIMESTAMP | 5496 | 5496 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_column_verification_registry_v1 | `verified_ts` | TIMESTAMP | 1 | 1 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_complications_events_v1 | `build_ts` | TIMESTAMP | 10954 | 10954 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_complications_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_esophageal_invasion_events_v1 | `build_ts` | TIMESTAMP | 188 | 188 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_esophageal_invasion_patient_rollup_v1 | `build_ts` | TIMESTAMP | 60 | 60 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_event_resolved_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 6689 | 6689 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_event_resolved_v1 | `last_known_alive_date` | TIMESTAMP | 6689 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_inline_adjudication_v1 | `build_ts` | TIMESTAMP | 3021 | 3021 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_subgrade_events_v1 | `build_ts` | TIMESTAMP | 287 | 287 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_subgrade_events_v1 | `extracted_at` | TIMESTAMP | 287 | 287 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_subgrade_events_v1 | `llm_build_ts` | TIMESTAMP | 287 | 287 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_ete_subgrade_patient_rollup_v1 | `build_ts` | TIMESTAMP | 151 | 151 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_fna_events_v1 | `ingested_at_utc` | TIMESTAMP WITH TIME ZONE | 8114 | 8114 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_invasion_events_v1 | `build_ts` | TIMESTAMP | 51773 | 51773 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_invasion_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_calcium_v1 | `ingestion_date` | TIMESTAMP | 187 | 187 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_calcium_v1 | `lab_datetime` | TIMESTAMP | 187 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_pth_v1 | `ingestion_date` | TIMESTAMP | 200 | 200 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_pth_v1 | `lab_datetime` | TIMESTAMP | 200 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_thyroglobulin_v1 | `ingestion_date` | TIMESTAMP | 53006 | 53006 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_thyroglobulin_v1 | `lab_datetime` | TIMESTAMP | 53006 | 52999 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_tsh_v1 | `ingestion_date` | TIMESTAMP | 556 | 556 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_tsh_v1 | `lab_datetime` | TIMESTAMP | 556 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_vitamin_d_v1 | `ingestion_date` | TIMESTAMP | 86 | 86 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_labs_vitamin_d_v1 | `lab_datetime` | TIMESTAMP | 86 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_medications_events_v1 | `build_ts` | TIMESTAMP | 7501 | 7501 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_medications_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_molecular_genetics_from_notes_v2 | `built_at` | TIMESTAMP WITH TIME ZONE | 1738 | 1738 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_molecular_genetics_v2 | `built_at` | TIMESTAMP | 1384 | 1384 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_molecular_genetics_v2 | `test_date_native` | TIMESTAMP | 481 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_operative_events_v1 | `build_ts` | TIMESTAMP | 11773 | 11773 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_operative_events_v1 | `note_date_resolved` | TIMESTAMP | 11773 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_operative_events_v1 | `surgery_date_native` | TIMESTAMP | 11773 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_operative_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_operative_procedure_codes_v1 | `build_ts` | TIMESTAMP | 21691 | 21691 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_parathyroid_events_v1 | `build_ts` | TIMESTAMP | 8697 | 8697 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_parathyroid_events_v1 | `extracted_at` | TIMESTAMP | 8697 | 8697 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_parathyroid_events_v1 | `llm_build_ts` | TIMESTAMP | 8697 | 8697 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_parathyroid_patient_rollup_v1 | `build_ts` | TIMESTAMP | 4443 | 4443 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_benign_events_v1 | `build_ts` | TIMESTAMP | 11688 | 11688 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_benign_patient_rollup_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_gland_events_v1 | `build_ts` | TIMESTAMP | 28724 | 28724 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_gland_patient_rollup_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 10731 | 10731 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_malignant_events_v1 | `build_ts` | TIMESTAMP | 6689 | 6689 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_malignant_events_v1 | `surgery_date` | TIMESTAMP | 6689 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_malignant_patient_rollup_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 4137 | 4137 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_malignant_patient_rollup_v1 | `earliest_malignant_path_date` | TIMESTAMP | 4137 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_path_malignant_patient_rollup_v1 | `latest_malignant_path_date` | TIMESTAMP | 4137 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_pathology_clinical_events_v1 | `build_ts` | TIMESTAMP | 13358 | 13358 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_pathology_clinical_patient_rollup_v1 | `build_ts` | TIMESTAMP | 3382 | 3382 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `cpm_built_at` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `first_recurrence_date` | TIMESTAMP | 54 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `first_surgery_date` | TIMESTAMP | 10871 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `gm_path_stage_raw_derived_at` | TIMESTAMP | 64 | 64 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `last_contact_date` | TIMESTAMP | 10871 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `mol_first_test_date` | TIMESTAMP | 706 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `mol_test_date` | TIMESTAMP | 809 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `path_stage_raw_derived_at` | TIMESTAMP | 64 | 64 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `rai_first_date` | TIMESTAMP | 581 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `recurrence_date` | TIMESTAMP | 802 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `resolved_at` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `rollup_built_at` | TIMESTAMP WITH TIME ZONE | 8422 | 8422 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_patient_master | `surg_first_date` | TIMESTAMP | 8731 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_pmh_events_v1 | `build_ts` | TIMESTAMP | 12444 | 12444 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_pmh_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_psh_events_v1 | `build_ts` | TIMESTAMP | 3919 | 3919 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_psh_patient_rollup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_recurrence_resolved_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_recurrence_v1 | `first_surgery_date` | TIMESTAMP | 8731 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_survival_followup_v1 | `build_ts` | TIMESTAMP | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_t4b_invasion_events_v1 | `build_ts` | TIMESTAMP | 944 | 944 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_t4b_invasion_events_v1 | `extracted_at` | TIMESTAMP | 944 | 944 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_t4b_invasion_events_v1 | `llm_build_ts` | TIMESTAMP | 944 | 944 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_t4b_invasion_patient_rollup_v1 | `build_ts` | TIMESTAMP | 434 | 434 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_table_signoff_registry_v1 | `registered_ts` | TIMESTAMP | 175 | 175 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_us_lymph_node_v2 | `extracted_at` | TIMESTAMP | 6801 | 6801 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_us_thyroid_gland_v2 | `extracted_at` | TIMESTAMP WITH TIME ZONE | 13578 | 13578 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_vascular_invasion_events_v1 | `build_ts` | TIMESTAMP | 3861 | 3861 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_vascular_invasion_events_v1 | `extracted_at` | TIMESTAMP | 3861 | 3861 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_vascular_invasion_events_v1 | `llm_build_ts` | TIMESTAMP | 3861 | 3861 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | canonical_vascular_invasion_patient_rollup_v1 | `build_ts` | TIMESTAMP | 3745 | 3745 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | cupm_v2_canonical_backfill_v1 | `backfilled_at_utc` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | data_dictionary_v279 | `rebuilt_at` | TIMESTAMP | 1591 | 1591 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | imaging_fna_linkage_v3 | `img_date` | TIMESTAMP | 9911 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | imaging_patient_summary_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 6126 | 6126 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `first_surgery_date` | TIMESTAMP | 10870 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `mol_test_date` | TIMESTAMP | 809 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `rai_first_date` | TIMESTAMP | 33 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `recurrence_date` | TIMESTAMP | 182 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `resolved_at` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `surg_first_date` | TIMESTAMP | 8731 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | manuscript_cohort_v1 | `surgery_date` | TIMESTAMP | 8731 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_airway_invasion_v2 | `build_ts` | TIMESTAMP | 6054 | 6054 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_airway_invasion_v2 | `extracted_at` | TIMESTAMP | 6054 | 6054 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_ete_subgrade_v1 | `build_ts` | TIMESTAMP | 287 | 287 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_ete_subgrade_v1 | `extracted_at` | TIMESTAMP | 287 | 287 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_parathyroid_detail_v1 | `build_ts` | TIMESTAMP | 8697 | 8697 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_parathyroid_detail_v1 | `extracted_at` | TIMESTAMP | 8697 | 8697 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_t4b_invasion_v1 | `build_ts` | TIMESTAMP | 944 | 944 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_t4b_invasion_v1 | `extracted_at` | TIMESTAMP | 944 | 944 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_vascular_invasion_v2 | `build_ts` | TIMESTAMP | 3861 | 3861 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | note_entities_llm_vascular_invasion_v2 | `extracted_at` | TIMESTAMP | 3861 | 3861 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | nsqip_enrichment | `nsqip_operation_date` | TIMESTAMP_NS | 1275 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | nsqip_patient_summary | `nsqip_operation_date` | TIMESTAMP_NS | 1261 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | path_synoptics | `surg_date` | TIMESTAMP | 11686 | 126 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | patient_completion_oed_path_linkage_v1 | `index_surgery_date` | TIMESTAMP_NS | 8368 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | patient_cross_domain_timeline_v2 | `event_date` | TIMESTAMP | 49512 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | rai_treatment_episode_v2 | `note_date_parsed` | TIMESTAMP | 1190 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | rai_treatment_episode_v2 | `rai_date_native` | TIMESTAMP | 434 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | rai_treatment_episode_v2 | `resolved_rai_date` | TIMESTAMP | 1272 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | recurrence_event_clean_v1 | `cleaned_at` | TIMESTAMP WITH TIME ZONE | 1946 | 1946 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | recurrence_event_clean_v1 | `recurrence_date` | TIMESTAMP | 182 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_genomic_assay_v1 | `materialized_at` | TIMESTAMP WITH TIME ZONE | 10370 | 10370 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_genomic_assay_v1 | `test_date_native` | TIMESTAMP | 1029 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_master_v1 | `identity_built_at` | TIMESTAMP | 10139 | 10139 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_master_v1 | `materialized_at` | TIMESTAMP WITH TIME ZONE | 10139 | 10139 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_source_xref_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 11273 | 11273 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_tumor_focus_v1 | `identity_built_at` | TIMESTAMP | 11103 | 11103 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_tumor_focus_v1 | `materialized_at` | TIMESTAMP WITH TIME ZONE | 11103 | 11103 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | specimen_tumor_focus_v1 | `surg_date` | TIMESTAMP | 11102 | 145 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | tg_postop_surveillance_windows_v1 | `window_first_date` | TIMESTAMP_NS | 16184 | 16182 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| main | tg_postop_surveillance_windows_v1 | `window_last_date` | TIMESTAMP_NS | 16184 | 16183 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | agent_adjudication_log_v1 | `adjudicated_at` | TIMESTAMP WITH TIME ZONE | 220 | 220 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | archive_move_log_v1 | `moved_at` | TIMESTAMP | 123 | 123 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | canonical_cleanup_audit_v1 | `classified_at` | TIMESTAMP WITH TIME ZONE | 120 | 120 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | canonical_cleanup_audit_v1 | `last_modified_in_db` | TIMESTAMP | 26 | 26 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_ajcc_dominant_discordance_canonical_v1 | `captured_at` | TIMESTAMP WITH TIME ZONE | 155 | 155 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_ajcc_dominant_vs_tp_hist1_discordance_v1 | `captured_at` | TIMESTAMP WITH TIME ZONE | 2195 | 2195 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_backfill_log_v1 | `backfilled_at` | TIMESTAMP | 26 | 26 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_ete_self_contradiction_queue_v1 | `queued_at` | TIMESTAMP | 2790 | 2790 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_histologic_classification_audit_v1 | `snapshot_ts` | TIMESTAMP | 1917 | 1917 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_hypopara_adjudication_log_v1 | `decided_at` | TIMESTAMP WITH TIME ZONE | 4 | 4 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_hypopara_adjudication_queue_v1 | `flagged_at` | TIMESTAMP WITH TIME ZONE | 4 | 4 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_is_malignant_flag_review_v1 | `flagged_at` | TIMESTAMP WITH TIME ZONE | 5 | 5 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_missing_data_provenance_v1 | `audited_at` | TIMESTAMP | 15 | 15 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_reconciliation_provenance_v1 | `ended_at` | TIMESTAMP WITH TIME ZONE | 10 | 10 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_reconciliation_provenance_v1 | `started_at` | TIMESTAMP WITH TIME ZONE | 10 | 10 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_stage_group_manual_review_v1 | `inserted_at` | TIMESTAMP | 6 | 6 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | cpm_tnm_cross_source_disagreements_v1 | `snapshot_ts` | TIMESTAMP | 4256 | 4256 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | episode_analysis_resolved_v1_dedup | `resolved_at` | TIMESTAMP WITH TIME ZONE | 9368 | 9368 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | episode_analysis_resolved_v1_dedup | `surgery_date` | TIMESTAMP | 9364 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lab_orphan_audit_v1 | `first_lab` | TIMESTAMP_NS | 403 | 403 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lab_orphan_audit_v1 | `last_lab` | TIMESTAMP_NS | 403 | 403 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lab_orphan_cohort_review_v1 | `first_tg_dt` | TIMESTAMP_NS | 403 | 403 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lab_orphan_cohort_review_v1 | `last_tg_dt` | TIMESTAMP_NS | 403 | 403 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lab_orphan_cohort_review_v1 | `surfaced_at` | TIMESTAMP WITH TIME ZONE | 403 | 403 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | lesion_analysis_resolved_v1 | `surgery_date` | TIMESTAMP | 11848 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | main_schema_keep_list_v1 | `registered_at` | TIMESTAMP | 2 | 2 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | manuscript_feasibility_v1 | `scored_at` | TIMESTAMP WITH TIME ZONE | 83 | 83 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | n_surgeries_v1_v2_conflict_v1 | `first_surgery_date` | TIMESTAMP | 599 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | object_domain_map_v1 | `as_of` | TIMESTAMP | 237 | 237 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | path_tumor_size_correction_queue_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 80 | 80 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | path_tumor_size_multifocal_enumeration_notes_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 13 | 13 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `first_surgery_date` | TIMESTAMP | 10870 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `mol_test_date` | TIMESTAMP | 809 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `rai_first_date` | TIMESTAMP | 581 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `recurrence_date` | TIMESTAMP | 182 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `resolved_at` | TIMESTAMP WITH TIME ZONE | 10871 | 10871 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | patient_analysis_resolved_v1 | `surg_first_date` | TIMESTAMP | 8731 | 0 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | pi_review_queue_v1 | `logged_at` | TIMESTAMP | 7 | 7 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | qc_event_issues_v1 | `detected_at` | TIMESTAMP | 6147 | 6147 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | qc_manual_review_queue_v1 | `created_at` | TIMESTAMP | 37719 | 37719 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | qc_tir03_llm_candidates_v1 | `candidate_built_at` | TIMESTAMP | 448 | 448 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | qc_usln01_llm_candidates_v1 | `candidate_built_at` | TIMESTAMP | 855 | 855 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | qc_violations_v1 | `detected_at` | TIMESTAMP | 18422 | 18422 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | schema_reorg_move_log_v1 | `moved_at` | TIMESTAMP | 58 | 58 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_387_dedup_probe_v1 | `build_ts` | TIMESTAMP | 36 | 36 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_387_prestate_v1 | `build_ts` | TIMESTAMP | 28 | 28 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_388_archive_move_log_v1 | `move_ts` | TIMESTAMP | 9 | 9 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_389_archive_move_log_v1 | `move_ts` | TIMESTAMP | 4 | 4 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_396_prestate_benign_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 11688 | 11688 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_396_prestate_gland_v1 | `build_ts` | TIMESTAMP WITH TIME ZONE | 28724 | 28724 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_396_prestate_v1 | `identity_built_at` | TIMESTAMP | 10139 | 10139 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | script_396_prestate_v1 | `materialized_at` | TIMESTAMP WITH TIME ZONE | 10139 | 10139 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | tg_orphan_cancer_text_investigation_queue_v1 | `created_at` | TIMESTAMP WITH TIME ZONE | 83 | 83 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | tg_orphan_cancer_text_investigation_queue_v1 | `first_tg_dt` | TIMESTAMP | 83 | 83 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | tg_orphan_cancer_text_investigation_queue_v1 | `last_tg_dt` | TIMESTAMP | 83 | 83 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | tier2_completeness_v1 | `checked_at` | TIMESTAMP | 22 | 22 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | us_llm_absorption_deferred_multi_nodule_v1 | `deferred_at` | TIMESTAMP WITH TIME ZONE | 825 | 825 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | us_llm_absorption_gap_v1 | `rebuilt_at` | TIMESTAMP WITH TIME ZONE | 60 | 60 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | us_raw_index0_conflict_v1 | `detected_at` | TIMESTAMP | 32146 | 32146 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | us_raw_index_mismatch_v1 | `detected_at` | TIMESTAMP | 13166 | 13166 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |
| manuscript_workspace | verification_low_concordance_v1 | `flagged_at` | TIMESTAMP | 17 | 17 |  | DATE_TRUNC('day', col) applied; time set to 00:00:00 |

## varchar_normalized_mm_dd_yyyy (62 cols)

| schema | table | column | dtype | n_rows | n_normalized | n_unparseable | note |
|---|---|---|---|---|---|---|---|
| main | canonical_fna_events_v1 | `fna_date_raw` | VARCHAR | 8052 | 7255 | 53 | left 53 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_events_v1 | `frozen_section_date` | VARCHAR | 7080 | 7080 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_10_date` | VARCHAR | 5 | 5 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_11_date` | VARCHAR | 4 | 4 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_12_date` | VARCHAR | 2 | 2 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_1_date` | VARCHAR | 4116 | 4116 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_2_date` | VARCHAR | 1249 | 1249 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_3_date` | VARCHAR | 728 | 728 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_4_date` | VARCHAR | 447 | 447 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_5_date` | VARCHAR | 239 | 239 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_6_date` | VARCHAR | 134 | 134 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_7_date` | VARCHAR | 86 | 86 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_8_date` | VARCHAR | 42 | 42 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_9_date` | VARCHAR | 26 | 26 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_section_first_date` | VARCHAR | 4116 | 4116 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_frozen_section_patient_rollup_v1 | `frozen_section_last_date` | VARCHAR | 4116 | 4116 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_molecular_genetics_from_notes_v2 | `note_date` | VARCHAR | 1079 | 1079 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_molecular_genetics_v2 | `resolved_test_date` | VARCHAR | 481 | 481 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_operative_events_v1 | `resolved_surgery_date` | VARCHAR | 11773 | 11773 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `cnln_latest_date` | VARCHAR | 1436 | 849 | 587 | left 587 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `cnln_surg_last_date` | VARCHAR | 1241 | 721 | 520 | left 520 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `nsqip_admission_date` | VARCHAR | 1261 | 0 | 226 | left 226 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `nsqip_discharge_date` | VARCHAR | 1261 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `nsqip_first_readmission_date` | VARCHAR | 29 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `nsqip_operation_date` | VARCHAR | 1261 | 1261 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `ops_surg_date` | VARCHAR | 8732 | 8731 | 1 | left 1 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `pet_first_date` | VARCHAR | 290 | 290 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | canonical_patient_master | `pet_last_date` | VARCHAR | 289 | 289 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | clinical_note_ln_extracted_v1 | `entity_date` | VARCHAR | 4109 | 4108 | 1 | left 1 unparseable rows in place (cleanup-csv candidates) |
| main | clinical_note_ln_extracted_v1 | `note_date` | VARCHAR | 7751 | 3876 | 3875 | left 3875 unparseable rows in place (cleanup-csv candidates) |
| main | ct_imaging | `date_of_exam` | VARCHAR | 7651 | 7651 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | mri_imaging | `date_of_exam` | VARCHAR | 590 | 590 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_cervical_ln_detail | `linkage_date` | VARCHAR | 10084 | 10084 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_dynamic_risk_response | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_esophageal_invasion | `linkage_date` | VARCHAR | 4409 | 4409 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_frozen_section_detail | `linkage_date` | VARCHAR | 32408 | 32408 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_frozen_section_detail | `note_date` | VARCHAR | 32408 | 32400 | 8 | left 8 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_past_medical_hx | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_past_surgical_hx | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_pathology | `linkage_date` | VARCHAR | 10084 | 10084 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_presenting_symptoms | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_rai_detailed | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_recurrence | `linkage_date` | VARCHAR | 11037 | 11037 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_llm_tirads_granular | `linkage_date` | VARCHAR | 10084 | 10084 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_operative_detail | `entity_date` | VARCHAR | 89 | 89 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_operative_detail | `note_date` | VARCHAR | 9182 | 9182 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_procedures | `entity_date` | VARCHAR | 3538 | 3538 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | note_entities_procedures | `note_date` | VARCHAR | 13148 | 13148 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_enrichment | `nsqip_admission_date` | VARCHAR | 1275 | 0 | 229 | left 229 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_enrichment | `nsqip_death_date` | VARCHAR | 1 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_enrichment | `nsqip_discharge_date` | VARCHAR | 1275 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_enrichment | `nsqip_first_readmission_date` | VARCHAR | 30 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_patient_summary | `nsqip_admission_date` | VARCHAR | 1261 | 0 | 226 | left 226 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_patient_summary | `nsqip_death_date` | VARCHAR | 1 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_patient_summary | `nsqip_discharge_date` | VARCHAR | 1261 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nsqip_patient_summary | `nsqip_first_readmission_date` | VARCHAR | 29 | 0 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | nuclear_med | `scandate` | VARCHAR | 2219 | 2219 | 0 | left 0 unparseable rows in place (cleanup-csv candidates) |
| main | specimen_master_v1 | `procedure_date_day` | VARCHAR | 10139 | 10138 | 1 | left 1 unparseable rows in place (cleanup-csv candidates) |
| main | thyroid_sizes | `surg_date` | VARCHAR | 11670 | 11668 | 2 | left 2 unparseable rows in place (cleanup-csv candidates) |
| main | thyroid_weights | `date_of_surgery` | VARCHAR | 9999 | 9998 | 1 | left 1 unparseable rows in place (cleanup-csv candidates) |
| manuscript_workspace | fna_source_long_v1_step_b | `date_raw` | VARCHAR | 8057 | 7255 | 58 | left 58 unparseable rows in place (cleanup-csv candidates) |
| manuscript_workspace | script_396_prestate_v1 | `procedure_date_day` | VARCHAR | 10139 | 10138 | 1 | left 1 unparseable rows in place (cleanup-csv candidates) |

