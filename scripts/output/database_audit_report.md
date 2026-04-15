
# THYROID_2026 — Database Audit Report

**Database:** `thyroid_ete_fix_20260413`  |  **Generated:** 2026-04-15 00:32  |  **Script:** `scripts/210_database_audit_backup.py`  |  **Dry-run:** no


## 1. Table Inventory by Tier

| Tier | Tables | Total rows |
|------|-------:|-----------:|
| CANONICAL | 7 | 56,566 |
| SOURCE_STRUCTURED | 48 | 545,124 |
| EXTRACTED_SCORED | 22 | 247,440 |
| NLP_ENTITIES | 30 | 382,724 |
| LINKAGE_QC | 13 | 76,961 |
| ANALYSIS_SUBSETS | 8 | 62,542 |
| OTHER | 80 | 616,282 |


### Tier: CANONICAL (7 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `canonical_benign_diagnosis_v1` | BASE TABLE | 6,818 | 6,734.0 | 18 | ✗ |
| `canonical_diagnosis_unified_v1` | BASE TABLE | 10,955 | 10,871.0 | 7 | ✗ |
| `canonical_malignant_diagnosis_v1` | BASE TABLE | 4,137 | 4,137.0 | 12 | ✗ |
| `canonical_molecular_tested_v1` | BASE TABLE | 1,286 | 1,286.0 | 17 | ✓ |
| `canonical_patient_master_v1` | BASE TABLE | 10,871 | 10,871.0 | 407 | ✓ |
| `canonical_recurrence_v1` | BASE TABLE | 10,995 | 10,995.0 | 12 | ✓ |
| `canonical_survival_followup_v1` | BASE TABLE | 11,504 | 11,504.0 | 13 | ✓ |


### Tier: SOURCE_STRUCTURED (48 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `clinical_notes_long` | BASE TABLE | 11,050 | 5,593.0 | 11 | ✗ |
| `ct_imaging` | BASE TABLE | 7,701 | 3,086.0 | 40 | ✓ |
| `extracted_fna_bethesda_v1` | BASE TABLE | 5,249 | 5,249.0 | 15 | ✓ |
| `fna_cytology` | BASE TABLE | 8,063 | 5,240.0 | 27 | ✓ |
| `fna_episode_master_v2` | BASE TABLE | 8,119 | 5,266.0 | 17 | ✓ |
| `fna_episode_master_v2_backup_20260414` | BASE TABLE | 0 | — | 17 | ✓ |
| `fna_history` | BASE TABLE | 8,119 | 5,266.0 | 10 | ✓ |
| `fna_molecular_linkage_v3` | BASE TABLE | 809 | 665.0 | 15 | ✓ |
| `gold_master_episode_events_v1` | VIEW | 9,368 | 9,368.0 | 46 | ✓ |
| `gold_master_patient_facts_v1` | VIEW | 10,871 | 10,871.0 | 146 | ✓ |
| `imaging_exam_master_v1` | BASE TABLE | 13,347 | 6,126.0 | 10 | ✓ |
| `imaging_fna_linkage_mm_v1` | BASE TABLE | 7,305 | 1,638.0 | 24 | ✓ |
| `imaging_fna_linkage_v3` | BASE TABLE | 9,911 | 1,938.0 | 20 | ✓ |
| `imaging_nodule_long_v2` | BASE TABLE | 19,891 | 3,439.0 | 33 | ✓ |
| `imaging_nodule_master_v1` | BASE TABLE | 37,016 | 6,126.0 | 25 | ✓ |
| `imaging_patient_summary_v1` | BASE TABLE | 6,126 | 6,126.0 | 13 | ✓ |
| `longitudinal_lab_canonical_v1` | BASE TABLE | 77,960 | 3,690.0 | 18 | ✓ |
| `longitudinal_lab_deduped_v` | VIEW | 56,198 | 3,690.0 | 18 | ✓ |
| `md_extracted_fna_bethesda_v1` | BASE TABLE | 5,249 | 5,249.0 | 15 | ✓ |
| `md_path_synoptics_encounter_qc_v1` | BASE TABLE | 11,688 | 10,871.0 | 274 | ✓ |
| `molecular_results` | BASE TABLE | 10,862 | 10,862.0 | 27 | ✓ |
| `molecular_results_contract_v` | VIEW | 10,862 | 10,862.0 | 27 | ✓ |
| `molecular_results_contract_v1` | VIEW | 10,862 | 10,862.0 | 27 | ✓ |
| `molecular_results_enriched_v1` | VIEW | 10,862 | 10,862.0 | 28 | ✓ |
| `molecular_results_unified_v` | VIEW | 12,502 | 10,862.0 | 38 | ✓ |
| `molecular_test_episode_v2` | BASE TABLE | 10,126 | 10,026.0 | 42 | ✓ |
| `molecular_variant_contract_v` | VIEW | 1,640 | 703.0 | 22 | ✗ |
| `molecular_variant_long` | BASE TABLE | 1,640 | 703.0 | 22 | ✗ |
| `molecular_variant_long_contract_v1` | VIEW | 1,640 | 703.0 | 22 | ✗ |
| `nuclear_med` | BASE TABLE | 2,220 | 1,148.0 | 17 | ✓ |
| `operative_episode_detail_v2` | BASE TABLE | 9,371 | 9,368.0 | 39 | ✓ |
| `path_synoptics` | BASE TABLE | 11,688 | 10,871.0 | 271 | ✓ |
| `path_synoptics_encounter_qc_v1` | VIEW | 11,688 | 10,871.0 | 274 | ✓ |
| `patient_refined_master_clinical_v12` | BASE TABLE | 12,886 | 10,871.0 | 272 | ✓ |
| `patient_refined_master_clinical_v12_ln_backup_20260414` | BASE TABLE | 0 | — | 8 | ✗ |
| `patient_refined_master_clinical_v12_outcome_backup_20260415` | BASE TABLE | 0 | — | 2 | ✗ |
| `review_queue_imaging_fna_mm_v1` | BASE TABLE | 742 | 180.0 | 9 | ✓ |
| `specimen_master_v1` | BASE TABLE | 10,139 | 8,422.0 | 17 | ✓ |
| `tumor_pathology` | BASE TABLE | 4,290 | 3,986.0 | 249 | ✓ |
| `ultrasound_reports` | BASE TABLE | 6,793 | 4,074.0 | 223 | ✓ |
| `v_fna_bethesda_episode_vs_resolved_v1` | VIEW | 0 | — | 9 | ✗ |
| `v_fna_bethesda_surface` | VIEW | 0 | — | 18 | ✓ |
| `v_fna_episode_bethesda_multiera_v1` | VIEW | 8,119 | 5,266.0 | 24 | ✓ |
| `v_fna_episode_bethesda_resolved_v1` | VIEW | 8,119 | 5,266.0 | 9 | ✓ |
| `v_fna_surgery_window` | VIEW | 0 | — | 23 | ✓ |
| `v_imaging_nodule_linkage_classification_v1` | VIEW | 37,016 | 6,126.0 | 7 | ✗ |
| `v_imaging_nodule_tirads_gap_v1` | VIEW | 37,016 | 6,126.0 | 7 | ✓ |
| `val_imaging_fna_linkage_audit_v1` | BASE TABLE | 1 | — | 9 | ✓ |


### Tier: EXTRACTED_SCORED (22 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `canonical_extracted_fact_long_v1` | BASE TABLE | 0 | 0.0 | 72 | ✓ |
| `canonical_extracted_fact_long_v2` | BASE TABLE | 55,500 | 5,141.0 | 72 | ✓ |
| `complication_patient_summary_v1` | BASE TABLE | 2,892 | 2,892.0 | 18 | ✗ |
| `complication_phenotype_v1` | BASE TABLE | 5,928 | 2,892.0 | 28 | ✓ |
| `extracted_braf_recovery_v1` | BASE TABLE | 730 | 376.0 | 7 | ✗ |
| `extracted_complications_refined_v5` | BASE TABLE | 358 | 287.0 | 9 | ✓ |
| `extracted_ete_subgraded_v1` | BASE TABLE | 3,558 | 3,558.0 | 9 | ✗ |
| `extracted_postop_labs_expanded_v1` | BASE TABLE | 1,395 | 1,051.0 | 12 | ✓ |
| `extracted_ras_patient_summary_v1` | BASE TABLE | 321 | 321.0 | 7 | ✗ |
| `extracted_rln_injury_refined_v2` | BASE TABLE | 92 | 92.0 | 12 | ✓ |
| `extracted_tirads_validated_v1` | BASE TABLE | 3,439 | 3,439.0 | 15 | ✗ |
| `ln_crossval_v1` | BASE TABLE | 0 | — | 19 | ✗ |
| `ln_master_rollup_v1` | BASE TABLE | 4,290 | 3,986.0 | 78 | ✓ |
| `rai_treatment_episode_v2` | BASE TABLE | 1,857 | 862.0 | 32 | ✓ |
| `rai_treatment_episode_v2_backup_20260415` | BASE TABLE | 0 | — | 32 | ✓ |
| `recurrence_event_clean_v1` | BASE TABLE | 1,946 | 1,946.0 | 11 | ✓ |
| `survival_cohort_enriched` | BASE TABLE | 61,134 | 10,507.0 | 28 | ✓ |
| `tg_timeline_patient_summary_v1` | BASE TABLE | 3,258 | 3,258.0 | 20 | ✓ |
| `thyroglobulin_lab_canonical_v1` | BASE TABLE | 76,971 | 3,258.0 | 21 | ✓ |
| `thyroid_scoring_py_v1` | BASE TABLE | 10,871 | 10,871.0 | 51 | ✓ |
| `tirads_llm_extracted_v2` | BASE TABLE | 12,900 | 1,429.0 | 28 | ✓ |
| `tirads_llm_validation_v2` | BASE TABLE | 0 | — | 27 | ✗ |


### Tier: NLP_ENTITIES (30 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `note_entities_complications` | BASE TABLE | 9,359 | 2,840.0 | 48 | ✓ |
| `note_entities_genetics` | BASE TABLE | 1,738 | 605.0 | 48 | ✓ |
| `note_entities_llm_airway_invasion` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_cervical_ln_detail` | BASE TABLE | 36,964 | 6,632.0 | 23 | ✓ |
| `note_entities_llm_dynamic_risk_response` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_frozen_section_detail` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_functional_outcomes` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_imaging` | BASE TABLE | 11,037 | 5,641.0 | 21 | ✓ |
| `note_entities_llm_labs` | BASE TABLE | 11,037 | 5,641.0 | 21 | ✓ |
| `note_entities_llm_parathyroid_detail` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_past_medical_hx` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_past_surgical_hx` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_pathology` | BASE TABLE | 29,236 | 5,884.0 | 21 | ✓ |
| `note_entities_llm_patient_decision_adherence` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_physical_exam` | BASE TABLE | 11,037 | 5,641.0 | 21 | ✓ |
| `note_entities_llm_presenting_symptoms` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_rad_treatment` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_rai_detailed` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_recurrence` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_survival_followup` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_synoptic_pathology_enrichment` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_tg_kinetics` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_tirads_granular` | BASE TABLE | 27,707 | 6,261.0 | 23 | ✓ |
| `note_entities_llm_us_nodule_dynamics` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_llm_vascular_invasion` | BASE TABLE | 11,037 | 5,641.0 | 23 | ✓ |
| `note_entities_medications` | BASE TABLE | 7,501 | 2,070.0 | 48 | ✓ |
| `note_entities_operative_detail` | BASE TABLE | 12,151 | 4,032.0 | 48 | ✓ |
| `note_entities_problem_list` | BASE TABLE | 11,579 | 4,037.0 | 48 | ✓ |
| `note_entities_procedures` | BASE TABLE | 21,942 | 4,723.0 | 48 | ✓ |
| `note_entities_staging` | BASE TABLE | 3,807 | 1,639.0 | 48 | ✓ |


### Tier: LINKAGE_QC (13 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `canonical_nodule_linkage_study_v1` | VIEW | 34,955 | 4,082.0 | 32 | ✓ |
| `linkage_ambiguity_review_v1` | BASE TABLE | 5,593 | 1,191.0 | 10 | ✓ |
| `linkage_master_v1` | BASE TABLE | 11,506 | 11,506.0 | 6 | ✗ |
| `linkage_summary_v` | VIEW | 10,871 | 10,871.0 | 5 | ✗ |
| `linkage_summary_v3` | BASE TABLE | 5 | — | 12 | ✗ |
| `md_val_path_synoptic_encounter_isolation_v1` | BASE TABLE | 3 | 3.0 | 8 | ✓ |
| `pathology_rai_linkage_v3` | BASE TABLE | 23 | 19.0 | 15 | ✓ |
| `preop_surgery_linkage_v3` | BASE TABLE | 3,554 | 3,163.0 | 16 | ✓ |
| `surgery_pathology_linkage_v3` | BASE TABLE | 9,409 | 8,733.0 | 16 | ✓ |
| `tg_lab_review_queue_v1` | BASE TABLE | 1,035 | 279.0 | 5 | ✓ |
| `val_llm_concordance_summary` | BASE TABLE | 0 | — | 7 | ✗ |
| `val_path_synoptic_encounter_isolation_v1` | BASE TABLE | 3 | 3.0 | 8 | ✓ |
| `val_phase12_tirads_validation` | BASE TABLE | 4 | — | 5 | ✗ |


### Tier: ANALYSIS_SUBSETS (8 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `analysis_cancer_cohort_v1` | BASE TABLE | 4,136 | 4,136.0 | 137 | ✓ |
| `analysis_molecular_subset_v1` | BASE TABLE | 10,025 | 10,025.0 | 126 | ✓ |
| `analysis_recurrence_subset_v1` | BASE TABLE | 1,946 | 1,946.0 | 133 | ✓ |
| `analysis_tirads_subset_v1` | BASE TABLE | 3,474 | 3,474.0 | 131 | ✓ |
| `episode_analysis_resolved_v1_dedup` | BASE TABLE | 9,368 | 9,368.0 | 46 | ✓ |
| `lesion_analysis_resolved_v1` | BASE TABLE | 11,851 | 10,871.0 | 28 | ✓ |
| `manuscript_cohort_v1` | BASE TABLE | 10,871 | 10,871.0 | 151 | ✓ |
| `patient_analysis_resolved_v1` | BASE TABLE | 10,871 | 10,871.0 | 146 | ✓ |


### Tier: OTHER (80 tables)

| Table | Type | Rows | Patients | Cols | Has Date |
|-------|------|-----:|---------:|-----:|---------|
| `_specimen_path_surgery_link_v1` | VIEW | 11,103 | 8,422.0 | 16 | ✓ |
| `_specimen_synoptic_spine_v1` | VIEW | 11,103 | 8,422.0 | 10 | ✓ |
| `canonical_fact_quarantine_v1` | BASE TABLE | 0 | 0.0 | 74 | ✓ |
| `canonical_fact_quarantine_v2` | BASE TABLE | 199 | 119.0 | 74 | ✓ |
| `database_snapshots` | VIEW | 0 | — | 10 | ✗ |
| `databases` | VIEW | 0 | — | 6 | ✗ |
| `episode_completeness_summary_v` | VIEW | 4 | — | 3 | ✗ |
| `event_date_audit_v2` | BASE TABLE | 61,055 | 11,506.0 | 8 | ✓ |
| `fhir_bundle_specimen_export_v1` | BASE TABLE | 10,139 | — | 4 | ✗ |
| `fhir_encounter_v1` | BASE TABLE | 10,139 | — | 6 | ✗ |
| `fhir_episode_of_care_v1` | BASE TABLE | 9,486 | — | 7 | ✗ |
| `fhir_patient_deid_map_v1` | BASE TABLE | 8,422 | 8,422.0 | 2 | ✗ |
| `fhir_procedure_collection_v1` | BASE TABLE | 10,139 | — | 7 | ✗ |
| `fhir_specimen_v1` | BASE TABLE | 10,139 | — | 8 | ✗ |
| `gold_llm_verified_facts` | BASE TABLE | 178 | 127.0 | 75 | ✓ |
| `lab_cross_wave_dedup_map_v1` | BASE TABLE | 21,761 | 1,796.0 | 9 | ✓ |
| `lab_cross_wave_review_v1` | BASE TABLE | 0 | 0.0 | 13 | ✓ |
| `lab_same_day_value_review_v1` | BASE TABLE | 288 | 205.0 | 13 | ✓ |
| `ln_x_marker_audit_v1` | BASE TABLE | 0 | — | 9 | ✗ |
| `master_fact_long_verified_v1` | VIEW | 55,500 | 5,141.0 | 31 | ✓ |
| `master_patient_rollup_verified_v1` | VIEW | 5,141 | 5,141.0 | 16 | ✗ |
| `master_source_lineage_v1` | VIEW | 55,500 | 5,141.0 | 22 | ✓ |
| `md_synoptic_tumor_long_v1` | BASE TABLE | 11,103 | 8,422.0 | 22 | ✓ |
| `molecular_assay_dictionary` | BASE TABLE | 4 | — | 11 | ✗ |
| `molecular_code_crosswalk` | BASE TABLE | 44 | — | 6 | ✗ |
| `molecular_fact_lineage_qa_duplicate_candidates_v` | VIEW | 0 | 0.0 | 11 | ✓ |
| `molecular_fact_long_base_v` | VIEW | 12,502 | 10,862.0 | 34 | ✓ |
| `molecular_fact_long_v` | VIEW | 12,502 | 10,862.0 | 38 | ✓ |
| `molecular_ingestion_runs` | BASE TABLE | 1 | — | 7 | ✗ |
| `molecular_normalization_review_v1` | VIEW | 116 | 116.0 | 27 | ✓ |
| `molecular_patient_rollup_v` | VIEW | 10,862 | 10,862.0 | 10 | ✓ |
| `molecular_qc_summary_v` | VIEW | 2 | — | 8 | ✗ |
| `molecular_testing` | BASE TABLE | 10,862 | 10,862.0 | 11 | ✓ |
| `mrn_crosswalk_v1` | BASE TABLE | 0 | 0.0 | 6 | ✗ |
| `note_extraction_runs` | BASE TABLE | 3 | — | 14 | ✗ |
| `notes_entity_summary` | VIEW | 5,272 | 5,272.0 | 11 | ✗ |
| `owned_shares` | VIEW | 0 | — | 9 | ✓ |
| `path_outcome_classification_v1` | BASE TABLE | 0 | — | 9 | ✗ |
| `patient_cross_domain_timeline_v2` | BASE TABLE | 61,055 | 11,506.0 | 6 | ✓ |
| `query_history` | VIEW | 0 | — | 23 | ✓ |
| `rai_dose_recovery_v1` | BASE TABLE | 0 | — | 15 | ✓ |
| `raw_imaging_12_slots_v1` | BASE TABLE | 21,079 | 6,123.0 | 16 | ✓ |
| `raw_us_tirads_excel_v1` | BASE TABLE | 19,891 | 3,439.0 | 40 | ✓ |
| `raw_us_tirads_scored_v1` | BASE TABLE | 19,549 | 3,434.0 | 10 | ✓ |
| `recent_queries` | VIEW | 0 | — | 23 | ✓ |
| `rosflow_dpo_exports` | BASE TABLE | 0 | — | 13 | ✓ |
| `rosflow_memory_events` | BASE TABLE | 0 | — | 12 | ✓ |
| `rosflow_training_failures` | BASE TABLE | 0 | — | 10 | ✓ |
| `rosflow_training_iterations` | BASE TABLE | 0 | — | 23 | ✓ |
| `rosflow_training_runs` | BASE TABLE | 0 | — | 17 | ✗ |
| `rosflow_training_scores` | BASE TABLE | 0 | — | 10 | ✓ |
| `serial_imaging_us` | BASE TABLE | 0 | — | 6 | ✓ |
| `shared_with_me` | VIEW | 0 | — | 8 | ✓ |
| `specimen_genomic_assay_v1` | BASE TABLE | 10,370 | 10,026.0 | 24 | ✓ |
| `specimen_source_xref_v1` | BASE TABLE | 11,273 | — | 9 | ✗ |
| `specimen_tumor_focus_v1` | BASE TABLE | 11,103 | 8,422.0 | 21 | ✓ |
| `stg_thyroseq_excel_raw` | BASE TABLE | 11,374 | — | 48 | ✓ |
| `stg_thyroseq_match_results` | BASE TABLE | 11,374 | — | 7 | ✗ |
| `stg_thyroseq_parsed` | BASE TABLE | 11,374 | — | 45 | ✓ |
| `storage_info` | VIEW | 0 | — | 12 | ✗ |
| `storage_info_history` | VIEW | 0 | — | 12 | ✗ |
| `synoptic_tumor_long_v1` | BASE TABLE | 11,103 | 8,422.0 | 22 | ✓ |
| `tg_postop_surveillance_windows_v1` | BASE TABLE | 16,184 | 3,250.0 | 12 | ✓ |
| `thyroseq_molecular_enrichment` | BASE TABLE | 10,862 | 10,862.0 | 25 | ✗ |
| `thyroseq_review_queue` | BASE TABLE | 628 | — | 6 | ✗ |
| `tumor_episode_master_v2` | BASE TABLE | 11,691 | 10,871.0 | 37 | ✓ |
| `us_nodules_tirads` | BASE TABLE | 10,862 | 10,862.0 | 36 | ✓ |
| `v_canonical_us_nodule_scope_v1` | VIEW | 1 | — | 4 | ✗ |
| `v_ln_finalization_by_cancer_type_v1` | VIEW | 4,228 | 3,947.0 | 18 | ✓ |
| `v_ln_imaging_separated_v1` | VIEW | 14,480 | 5,035.0 | 9 | ✓ |
| `v_ln_pathology_separated_v1` | VIEW | 4,227 | 3,946.0 | 40 | ✓ |
| `v_patient_surgery_timeline` | VIEW | 0 | — | 9 | ✓ |
| `v_rosflow_dimension_averages` | VIEW | 0 | — | 9 | ✗ |
| `v_rosflow_domain_performance` | VIEW | 0 | — | 6 | ✗ |
| `v_rosflow_dpo_export_rate` | VIEW | 0 | — | 7 | ✗ |
| `v_rosflow_failure_hotspots` | VIEW | 0 | — | 6 | ✗ |
| `v_rosflow_fallback_usage` | VIEW | 0 | — | 6 | ✗ |
| `v_rosflow_golden_rate` | VIEW | 0 | — | 5 | ✗ |
| `v_rosflow_score_trends` | VIEW | 0 | — | 9 | ✗ |
| `vw_us_nodule_tirads_validated` | BASE TABLE | 5 | — | 9 | ✗ |


## 2. Column Gap Analysis (Source → Canonical)

`canonical_patient_master_v1` has **407 columns** (see Phase B for per-table detail).


### `complication_phenotype_v1`

*Per-complication-type detail (5,928 rows, 2,892 patients)*

- **Rows:** 5,928  |  **Patients:** 2892

- **Source columns:** 28

- **Columns NOT in canonical:** 24

- **Canonical analogue coverage:** 100.0%  (5/5 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  biochemical_low_ca,  biochemical_low_pth,  ca_nadir,  complication_entity,  confirmed_flag
  detection_date,  evidence_tier,  final_complication_status,  historical_only_flag,  n_raw_nlp_mentions
  n_valid_nlp_mentions,  note_mention_flag,  permanent_flag,  phenotype_version,  phenotyped_at
  source_tier_label,  surgery_related_flag,  suspected_flag,  timing_days_post_surgery,  timing_window
  transient_flag,  treatment_requiring_flag,  voice_permanence_noted,  voice_resolution_noted
```
**Already covered by canonical:** `any_confirmed_complication`, `n_confirmed_complications`, `has_low_pth_flag`, `has_low_calcium_flag`, `rln_status`


### `recurrence_event_clean_v1`

*Event-level recurrence (1,946 rows)*

- **Rows:** 1,946  |  **Patients:** 1946

- **Source columns:** 11

- **Columns NOT in canonical:** 5

- **Canonical analogue coverage:** 60.0%  (3/5 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  cleaned_at,  event_rank,  source_priority,  source_table,  structural_recurrence_flag
```
**Already covered by canonical:** `recurrence_confirmed`, `recurrence_type`, `recurrence_date`

**Canonical analogues NOT found:** `recurrence_source`, `recurrence_detection`


### `rai_treatment_episode_v2`

*Per-episode RAI detail (1,857 rows, 862 patients)*

- **Rows:** 1,857  |  **Patients:** 862

- **Source columns:** 32

- **Columns NOT in canonical:** 31

- **Canonical analogue coverage:** 60.0%  (3/5 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  adjudication_status,  completion_status,  date_confidence,  date_status,  dose_confidence
  dose_mci,  dose_missingness_reason,  dose_source,  dose_text_raw,  iodine_avidity_flag
  linked_pathology_episode_id,  linked_recurrence_episode_id,  linked_surgery_episode_id,  note_date_parsed,  post_therapy_scan_flag
  pre_scan_flag,  rai_assertion_status,  rai_confidence,  rai_date_native,  rai_episode_id
  rai_intent,  rai_mention_raw,  rai_term_normalized,  resolved_rai_date,  scan_findings_raw
  source_note_id,  source_note_type,  source_table,  stimulated_tg,  stimulated_tsh
  surgery_link_score_v3
```
**Already covered by canonical:** `n_rai_episodes`, `rai_dose_v9`, `rai_intent_v9`

**Canonical analogues NOT found:** `rai_date_v9`, `rai_response_v9`


### `survival_cohort_enriched`

*Survival cohort (61,134 rows, 10,507 patients)*

- **Rows:** 61,134  |  **Patients:** 10507

- **Source columns:** 28

- **Columns NOT in canonical:** 24

- **Canonical analogue coverage:** 0.0%  (0/5 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  age_at_diagnosis,  ajcc_stage_8,  braf_status,  date_confidence,  date_source
  diagnosis_year,  ete_type,  event,  event_type,  histology
  lineage_version,  ln_examined,  ln_positive,  provenance_note,  ras_status
  recurrence_risk_band,  resolved_layer_version,  ret_status,  source_script,  source_table
  tert_status,  tg_annual_log_slope,  time_days,  time_days_capped
```
**Canonical analogues NOT found:** `survival_time_days`, `survival_event`, `death_occurred`, `follow_up_complete`, `follow_up_months`


### `clinical_notes_long`

*Source notes for NLP (rows, 5,593 patients)*

- **Rows:** 11,050  |  **Patients:** 5593

- **Source columns:** 11

- **Columns NOT in canonical:** 10


#### Columns NOT yet in canonical_patient_master_v1

```
  excel_row_0based,  ingest_script_version,  ingest_sheet_spec,  ingested_at_utc,  note_index
  note_text,  note_type,  source_column,  source_sheet,  source_workbook
```

### `molecular_variant_long`

*Per-variant molecular results*

- **Rows:** 1,640  |  **Patients:** 703

- **Source columns:** 22

- **Columns NOT in canonical:** 21

- **Canonical analogue coverage:** 66.7%  (2/3 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  allele_fraction,  canonical_hgvs,  cdna_hgvs,  fusion_partner,  gene_symbol
  genomic_hgvs,  ingestion_ts,  interpretation_text,  lineage_id,  molecular_result_id
  molecular_variant_id,  normalization_status,  parse_status,  partner_gene_symbol,  protein_hgvs
  qc_flags,  raw_variant_token,  risk_call,  transcript_id,  variant_class
  zygosity
```
**Already covered by canonical:** `braf_positive_final`, `ras_positive_v7`

**Canonical analogues NOT found:** `tert_positive_v9`


### `thyroglobulin_lab_canonical_v1`

*Longitudinal Tg lab (76,971 rows, 3,258 patients)*

- **Rows:** 76,971  |  **Patients:** 3258

- **Source columns:** 21

- **Columns NOT in canonical:** 18

- **Canonical analogue coverage:** 50.0%  (2/4 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  analyte,  assay_method,  days_from_surgery,  disambiguation_confidence,  disambiguation_method
  gender,  ingestion_date,  ingestion_script,  order_dt,  result_flag
  result_numeric,  result_qualifier,  result_raw,  specimen_collect_dt,  surg_date
  temporal_window,  test_name_raw,  thyroid_procedure
```
**Already covered by canonical:** `tg_nadir`, `tg_rising_flag`

**Canonical analogues NOT found:** `tg_max`, `tg_last`


### `extracted_postop_labs_expanded_v1`

*Post-op PTH/Ca detail*

- **Rows:** 1,395  |  **Patients:** 1051

- **Source columns:** 12

- **Columns NOT in canonical:** 11

- **Canonical analogue coverage:** 100.0%  (4/4 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  days_postop,  extraction_method,  first_surg_date,  lab_date,  lab_type
  refined_at,  source_note_type,  source_reliability,  unit,  value
  value_in_range
```
**Already covered by canonical:** `has_low_pth_flag`, `has_low_calcium_flag`, `pth_nadir`, `calcium_nadir`


### `extracted_ete_subgraded_v1`

*ETE subgrading detail*

- **Rows:** 3,558  |  **Patients:** 3558

- **Source columns:** 9

- **Columns NOT in canonical:** 8

- **Canonical analogue coverage:** 33.3%  (1/3 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  grading_source_note,  op_note_confidence,  op_note_grade,  original_grade,  original_source
  refined_at,  refined_ete_grade,  subgrade_method
```
**Already covered by canonical:** `ete_grade`

**Canonical analogues NOT found:** `ete_grade_v9`, `ete_microscopic_confirmed`


### `extracted_braf_recovery_v1`

*BRAF multi-source recovery*

- **Rows:** 730  |  **Patients:** 376

- **Source columns:** 7

- **Columns NOT in canonical:** 5

- **Canonical analogue coverage:** 100.0%  (2/2 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  braf_status,  confidence,  detection_method,  extracted_at,  source
```
**Already covered by canonical:** `braf_positive_final`, `braf_detection_method_v11`


### `extracted_ras_patient_summary_v1`

*RAS subtype summary*

- **Rows:** 321  |  **Patients:** 321

- **Source columns:** 7

- **Columns NOT in canonical:** 5

- **Canonical analogue coverage:** 100.0%  (5/5 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  allele_frequency_pct,  confidence,  ras_primary_subtype,  ras_protein_change,  source
```
**Already covered by canonical:** `ras_positive_v7`, `nras_positive_v11`, `hras_positive_v11`, `kras_positive_v11`, `ras_primary_subtype_v11`


### `extracted_rln_injury_refined_v2`

*Refined RLN injury classification*

- **Rows:** 92  |  **Patients:** 92

- **Source columns:** 12

- **Columns NOT in canonical:** 9

- **Canonical analogue coverage:** 50.0%  (1/2 matched)


#### Columns NOT yet in canonical_patient_master_v1

```
  classification,  days_post_surgery,  detection_date,  injury_type,  rln_injury_evidence_strength
  rln_injury_is_confirmed,  rln_injury_tier,  temporal_window,  temporality
```
**Already covered by canonical:** `rln_status`

**Canonical analogues NOT found:** `rln_injury_tier`


## 3. clinical_notes_long — Note Type Breakdown

| Note type | Notes | Patients |
|-----------|------:|---------:|
| OPNOTE | 4,727 | 4,486 |
| HP | 4,280 | 4,058 |
| OTHER_HISTORY | 525 | 525 |
| ENDOCRINE_FM | 522 | 522 |
| ED_NOTE | 498 | 495 |
| DC_SUM | 185 | 169 |
| OTHER_NOTES | 160 | 160 |
| DEATH | 153 | 153 |


## 4. Parquet Backup Results

- **Tables backed up:** 79 / 80

- **Tables skipped (not found):** 1

- **Failures:** 0

- **Total rows exported:** 936,501

- **Total size:** 51.8 MB

| Table | Status | Rows | Size (MB) |
|-------|--------|-----:|----------:|
| `canonical_patient_master_v1` | OK | 10,871 | 0.7 |
| `canonical_diagnosis_unified_v1` | OK | 10,955 | 0.0 |
| `canonical_recurrence_v1` | OK | 10,995 | 0.1 |
| `canonical_survival_followup_v1` | OK | 11,504 | 0.2 |
| `canonical_molecular_tested_v1` | OK | 1,286 | 0.0 |
| `canonical_benign_diagnosis_v1` | OK | 6,818 | 0.3 |
| `canonical_malignant_diagnosis_v1` | OK | 4,137 | 0.0 |
| `gold_master_patient_facts_v1` | OK | 10,871 | 0.3 |
| `patient_refined_master_clinical_v12` | OK | 12,886 | 0.4 |
| `tumor_pathology` | OK | 4,290 | 0.8 |
| `path_synoptics` | OK | 11,688 | 6.3 |
| `ultrasound_reports` | OK | 6,793 | 1.5 |
| `ct_imaging` | OK | 7,701 | 4.3 |
| `nuclear_med` | OK | 2,220 | 0.8 |
| `fna_cytology` | OK | 8,063 | 1.1 |
| `fna_episode_master_v2` | OK | 8,119 | 0.8 |
| `fna_history` | OK | 8,119 | 0.9 |
| `operative_episode_detail_v2` | OK | 9,371 | 0.1 |
| `imaging_nodule_master_v1` | OK | 37,016 | 1.6 |
| `imaging_patient_summary_v1` | OK | 6,126 | 0.1 |
| `imaging_exam_master_v1` | OK | 13,347 | 0.5 |
| `longitudinal_lab_canonical_v1` | OK | 77,960 | 0.4 |
| `molecular_results` | OK | 10,862 | 1.1 |
| `molecular_variant_long` | OK | 1,640 | 0.1 |
| `molecular_test_episode_v2` | OK | 10,126 | 0.2 |
| `specimen_master_v1` | OK | 10,139 | 0.9 |
| `clinical_notes_long` | OK | 11,050 | 9.4 |
| `extracted_tirads_validated_v1` | OK | 3,439 | 0.0 |
| `tirads_llm_extracted_v2` | OK | 12,900 | 0.5 |
| `extracted_ete_subgraded_v1` | OK | 3,558 | 0.0 |
| `extracted_braf_recovery_v1` | OK | 730 | 0.0 |
| `extracted_ras_patient_summary_v1` | OK | 321 | 0.0 |
| `extracted_rln_injury_refined_v2` | OK | 92 | 0.0 |
| `extracted_complications_refined_v5` | OK | 358 | 0.0 |
| `extracted_postop_labs_expanded_v1` | OK | 1,395 | 0.0 |
| `extracted_fna_bethesda_v1` | OK | 5,249 | 0.1 |
| `thyroid_scoring_py_v1` | OK | 10,871 | 0.1 |
| `tg_timeline_patient_summary_v1` | OK | 3,258 | 0.1 |
| `thyroglobulin_lab_canonical_v1` | OK | 76,971 | 0.8 |
| `complication_patient_summary_v1` | OK | 2,892 | 0.0 |
| `complication_phenotype_v1` | OK | 5,928 | 0.0 |
| `recurrence_event_clean_v1` | OK | 1,946 | 0.0 |
| `survival_cohort_enriched` | OK | 61,134 | 0.2 |
| `rai_treatment_episode_v2` | OK | 1,857 | 0.1 |
| `ln_master_rollup_v1` | OK | 4,290 | 0.2 |
| `ln_crossval_v1` | SKIP_NOT_FOUND | 0 | 0.0 |
| `note_entities_llm_tirads_granular` | OK | 27,707 | 1.1 |
| `note_entities_llm_cervical_ln_detail` | OK | 36,964 | 1.0 |
| `note_entities_llm_pathology` | OK | 29,236 | 1.9 |
| `note_entities_llm_recurrence` | OK | 11,037 | 0.5 |
| `note_entities_llm_survival_followup` | OK | 11,037 | 0.7 |
| `note_entities_llm_rai_detailed` | OK | 11,037 | 0.4 |
| `note_entities_llm_tg_kinetics` | OK | 11,037 | 0.4 |
| `note_entities_llm_imaging` | OK | 11,037 | 0.8 |
| `note_entities_llm_labs` | OK | 11,037 | 0.6 |
| `note_entities_llm_frozen_section_detail` | OK | 11,037 | 0.4 |
| `note_entities_llm_airway_invasion` | OK | 11,037 | 0.5 |
| `note_entities_llm_vascular_invasion` | OK | 11,037 | 0.5 |
| `note_entities_llm_functional_outcomes` | OK | 11,037 | 0.5 |
| `note_entities_llm_parathyroid_detail` | OK | 11,037 | 0.5 |
| `note_entities_llm_dynamic_risk_response` | OK | 11,037 | 0.4 |
| `note_entities_llm_past_medical_hx` | OK | 11,037 | 0.6 |
| `note_entities_llm_past_surgical_hx` | OK | 11,037 | 0.7 |
| `note_entities_llm_presenting_symptoms` | OK | 11,037 | 0.6 |
| `note_entities_llm_physical_exam` | OK | 11,037 | 0.6 |
| `note_entities_llm_rad_treatment` | OK | 11,037 | 0.5 |
| `note_entities_llm_patient_decision_adherence` | OK | 11,037 | 0.5 |
| `note_entities_llm_synoptic_pathology_enrichment` | OK | 11,037 | 0.5 |
| `note_entities_llm_us_nodule_dynamics` | OK | 11,037 | 0.5 |
| `note_entities_complications` | OK | 9,359 | 0.2 |
| `note_entities_genetics` | OK | 1,738 | 0.1 |
| `note_entities_medications` | OK | 7,501 | 0.2 |
| `note_entities_operative_detail` | OK | 12,151 | 0.5 |
| `note_entities_problem_list` | OK | 11,579 | 0.3 |
| `note_entities_procedures` | OK | 21,942 | 0.5 |
| `note_entities_staging` | OK | 3,807 | 0.1 |
| `linkage_master_v1` | OK | 11,506 | 0.1 |
| `imaging_fna_linkage_v3` | OK | 9,911 | 0.2 |
| `surgery_pathology_linkage_v3` | OK | 9,409 | 0.1 |
| `fna_molecular_linkage_v3` | OK | 809 | 0.0 |


## 5. Recommendations — Gap Fill Priority

| Priority | Table | Recommendation |
|----------|-------|----------------|
| HIGH | `complication_phenotype_v1` | Add per-entity confirmed/transient/permanent flags; currently canonical only has aggregate counts |
| HIGH | `extracted_rln_injury_refined_v2` | RLN tier + confidence not in canonical; useful for complications manuscript |
| HIGH | `extracted_braf_recovery_v1` | braf_detection_method not in canonical; needed for molecular source attribution |
| HIGH | `extracted_ras_patient_summary_v1` | ras_subtype (NRAS/HRAS/KRAS) not in canonical; already in v11 columns but verify |
| MEDIUM | `rai_treatment_episode_v2` | Per-episode RAI intent/response useful for multi-RAI patients; canonical has rollup only |
| MEDIUM | `thyroglobulin_lab_canonical_v1` | Tg velocity / doubling time not in canonical; consider adding tg_velocity_per_year |
| MEDIUM | `extracted_postop_labs_expanded_v1` | PTH/calcium nadir day and exact value not in canonical; only flags present |
| LOW | `survival_cohort_enriched` | Survival table has duplicate rows per patient; canonical_survival_followup_v1 is the SSOT |
| LOW | `molecular_variant_long` | Variant-level data (allele freq, codon) lost at patient rollup — acceptable for manuscript |
| LOW | `extracted_ete_subgraded_v1` | ete_grade_v9 in canonical covers this; subgrading source label may be worth adding |
