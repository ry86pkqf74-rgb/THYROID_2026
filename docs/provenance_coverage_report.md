# Provenance Coverage Report

Generated: 2026-03-12 22:11:53

## Summary

| Metric | Value |
|--------|-------|
| Total tables audited | 336 |
| Avg provenance coverage | 5.9% |
| Tables fully covered (100%) | 0 |
| Tables partially covered | 64 |
| Tables with no provenance | 272 |
| `provenance_enriched_events_v1` rows | 50,297 |
| `lineage_audit_v1` rows | 10,871 |

## Provenance Columns Checked

- `source_table`
- `source_column`
- `date_source`
- `date_confidence`
- `inferred_event_date`
- `extraction_method`
- `evidence_span`

## Tables — Missing Provenance Columns

| Table | Coverage % | Missing Columns |
|-------|-----------|----------------|
| adjudication_decision_history | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| adjudication_decisions | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_sorted | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_v3 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_v4 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_v4_sorted | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| advanced_features_view | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| anti_thyroglobulin_labs | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| benign_pathology | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| benign_vs_malignant_comparison | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| clinical_notes | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| complication_severity_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| complications | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| ct_imaging | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| cure_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| cure_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| dashboard_patient_timeline_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| data_completeness | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| data_completeness_by_year | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| database_snapshots | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| databases | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| date_rescue_rate_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_braf_recovery_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_chyle_leak_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_completion_reasons_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_complications_exclusion_audit_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_complications_refined_v5 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ene_multisource_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ene_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ete_ene_tert_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ete_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ete_subgraded_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_fna_bethesda_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_fna_path_concordance_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_followup_audit_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_hematoma_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_hypocalcemia_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_hypoparathyroidism_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_imaging_molecular_final_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_invasion_grading_recovery_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_invasion_profile_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_lateral_neck_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ln_yield_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_longterm_outcomes_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_margin_r0_recovery_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_margins_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_mice_summary_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_missed_data_sweep_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_molecular_panel_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_molecular_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_multi_tumor_aggregate_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_nodule_sizes_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_postop_labs_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_preop_imaging_concordance_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rai_response_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rai_validated_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ras_patient_summary_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ras_subtypes_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_recurrence_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rln_exclusion_audit_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rln_injury_refined_summary_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rln_injury_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_seroma_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_staging_details_refined_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_staging_recovery_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_us_tirads_v1 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_variables_refined_v6 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_wound_infection_refined_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fna_accuracy_view | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fna_cytology | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fna_history | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fna_molecular_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fnas_detailed | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| frozen_sections | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| genetic_testing | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| genetic_testing_clean | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| genetics_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| genotype_stratified_outcomes_v3_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_analysis_cohort_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_discordance_summary_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_manual_review_queue_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_post_review_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_reconciliation_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| histology_reconciliation_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_pathology_concordance_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_pathology_correlation | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_reports | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_timeline | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| lab_timeline | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| longitudinal_lab_view | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| lymph_node_metastasis_view | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_histology_cohort_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_molecular_cohort_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_patient_summary_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_rai_cohort_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_table1_demographics_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_table2_survival_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_table3_genotype_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| manuscript_tables_v3_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| master_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| master_timeline | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_adjudication_domain_counts | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_adjudication_progress_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_date_quality_summary_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_date_rescue_rate_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_fna_molecular_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_histology_analysis_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_histology_discordance_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_histology_manual_review_queue | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_histology_post_review | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_histology_reconciliation_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_imaging_path_concordance_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_manuscript_histology_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_manuscript_molecular_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_manuscript_patient_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_manuscript_rai_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_episode_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_linkage_failure_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_linkage_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_manual_review_queue | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_post_review | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_op_path_recon_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_oper_episode_detail_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_pathology_rai_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_pathology_recon_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_patient_cross_domain_timeline_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_patient_manual_review_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_patient_master_timeline_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_patient_reconciliation_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_patient_validation_rollup_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_preop_surgery_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_qa_high_priority_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_qa_issues_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_qa_summary_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_adjudication_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_linkage_failure_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_manual_review_queue | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_post_review | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_reviewer_resolved_patient_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_streamlit_cohort_qc_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_streamlit_patient_conflicts | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_streamlit_patient_header | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_streamlit_patient_manual_review | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_streamlit_patient_timeline | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_surgery_pathology_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_timeline_manual_review_queue | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_top_priority_review_batches | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_unresolved_high_value_cases | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| mixture_cure_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| mixture_cure_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_episode_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_episode_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_path_risk_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_testing | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_unresolved_audit_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| mri_imaging | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| nodule_laterality_cleaned_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| nuclear_med | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| operative_details | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| operative_episode_detail_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| operative_pathology_reconciliation_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| overview_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| owned_shares | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| parathyroid | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| path_synoptics | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| pathology_rai_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| pathology_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_cross_domain_timeline_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_episode_audit_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_level_summary_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_master_timeline_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_reconciliation_summary_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_complication_flags_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v10 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v4 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v5 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v6 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v7 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v8 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_master_clinical_v9 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_refined_staging_flags_v3 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| patient_validation_rollup_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| performance_optimization_log | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| preop_surgery_linkage_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| promotion_cure_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| promotion_cure_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| ptc_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| publication_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_dashboard_summary_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_date_completeness_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_high_priority_review_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_issues | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_issues_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_laterality_mismatches | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_missing_demographics | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_report_matching | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| qa_summary_by_domain_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| query_history | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_clinical_notes | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_complications | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_fna_history | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_molecular_testing | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_operative_details | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_path_synoptics | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_serial_imaging | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| raw_us_nodules_tirads | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| recent_queries | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| recurrence_free_survival_v3_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| recurrence_risk_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| recurrence_risk_features_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| reviewer_resolved_patient_summary_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| risk_enriched_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| serial_imaging_us | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| serial_nodule_tracking_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| shared_with_me | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| stg_thyroseq_excel_raw | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| stg_thyroseq_match_results | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| stg_thyroseq_parsed | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| storage_info | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| storage_info_history | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| streamlit_cohort_qc_summary_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| streamlit_patient_header_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| streamlit_patient_timeline_v | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| survival_cohort | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| survival_cohort_enriched | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| survival_cohort_ready_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| survival_kpis | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| synoptic_pathology | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| tg_trend_long_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroglobulin_labs | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroid_sizes | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroid_weights | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroseq_fill_actions | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroseq_followup_events | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroseq_followup_labs | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroseq_molecular_enrichment | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| thyroseq_review_queue | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| time_to_rai_v3_mv | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| tumor_pathology | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| ultrasound_reports | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| us_nodules_tirads | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| val_chronology_anomalies | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| val_completeness_scorecard | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| val_molecular_confirmation | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| val_rai_confirmation | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| validation_failures_v2 | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_braf_audit | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_completion_reasons | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_confirmed_postop_rln_injury | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_confirmed_postop_rln_injury_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_ene_concordance | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_ene_source_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_ete_by_source | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_ete_microscopic_rule | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_fna_by_source | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_invasion_profile | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_lateral_neck | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_ln_yield_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_longterm_outcomes | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_margin_r0_recovery | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_margins_by_source | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_molecular_subtypes | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_patient_postop_rln_injury_detail | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_postop_lab_expanded | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_postop_lab_nadir | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_preop_molecular_panel | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_rai_response_summary | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_recurrence_by_detection_method | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_staging_refined | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_us_tirads | 0% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| clinical_notes_long | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| date_recovery_summary | 14% | `source_table`, `source_column`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_clinical_events | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_clinical_events_v2 | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_clinical_events_v3 | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_clinical_events_v4 | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_ene_refined_v1 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_postop_labs_expanded_v1 | 14% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `evidence_span` |
| extracted_preop_sweep_v1 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_rai_dose_refined_v1 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| extracted_remaining_events | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_exam_summary_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| imaging_nodule_long_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| linkage_summary_v2 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_imaging_exam_summary_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_imaging_nodule_long_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_linkage_summary_v2 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_analysis_cohort | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_episode_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_analysis_cohort | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_episode_v2 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_episode_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_tumor_episode_master_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_validation_failures_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_analysis_cohort_v | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_episode_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| provenance_enriched_events_v1 | 14% | `source_table`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| rai_episode_mv | 14% | `source_table`, `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method` |
| rai_episode_v2 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| rai_episode_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| rai_unresolved_audit_mv | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| timeline_unresolved_summary_mv | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| timeline_unresolved_summary_v2_mv | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| tumor_episode_master_v2 | 14% | `source_table`, `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| validation_failures_v3 | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| vw_rai_dose_by_source | 14% | `source_column`, `date_source`, `date_confidence`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| enriched_patient_timeline_v3_mv | 29% | `source_table`, `source_column`, `date_source`, `date_confidence`, `extraction_method` |
| event_date_audit_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| fna_episode_master_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_event_date_audit_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_fna_episode_master_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_molecular_test_episode_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| md_rai_treatment_episode_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| molecular_test_episode_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| rai_treatment_episode_v2 | 29% | `source_column`, `date_source`, `inferred_event_date`, `extraction_method`, `evidence_span` |
| timeline_rescue_v3_mv | 29% | `source_table`, `source_column`, `date_source`, `date_confidence`, `extraction_method` |
| enriched_master_timeline | 43% | `source_table`, `source_column`, `extraction_method`, `evidence_span` |
| missing_date_associations_audit | 43% | `source_table`, `source_column`, `extraction_method`, `evidence_span` |
| validation_failures_mv | 43% | `source_table`, `source_column`, `extraction_method`, `evidence_span` |
| timeline_rescue_mv | 57% | `source_column`, `extraction_method`, `evidence_span` |
| timeline_rescue_v2_mv | 57% | `source_column`, `extraction_method`, `evidence_span` |
| enriched_note_entities_complications | 71% | `source_table`, `source_column` |
| enriched_note_entities_genetics | 71% | `source_table`, `source_column` |
| enriched_note_entities_medications | 71% | `source_table`, `source_column` |
| enriched_note_entities_problem_list | 71% | `source_table`, `source_column` |
| enriched_note_entities_procedures | 71% | `source_table`, `source_column` |
| enriched_note_entities_staging | 71% | `source_table`, `source_column` |
| lineage_audit_v1 | 71% | `source_table`, `source_column` |
| note_entities_complications | 71% | `source_table`, `source_column` |
| note_entities_genetics | 71% | `source_table`, `source_column` |
| note_entities_medications | 71% | `source_table`, `source_column` |
| note_entities_problem_list | 71% | `source_table`, `source_column` |
| note_entities_procedures | 71% | `source_table`, `source_column` |
| note_entities_staging | 71% | `source_table`, `source_column` |

## Core NLP Provenance Tables (note_entities_*)

These 6 tables have the richest provenance via scripts 15/17/27:
- `note_entities_staging`, `note_entities_genetics`, `note_entities_procedures`
- `note_entities_complications`, `note_entities_medications`, `note_entities_problem_list`

Provenance columns present: `inferred_event_date`, `date_source`, `date_granularity`, `date_confidence`
Enriched views add: `date_status`, `date_anchor_type`, `date_anchor_table`

## Lab Date Accuracy Audit

| Lab Type | Total | Has Lab Date | Note-Date Fallback | No Date | Correct % | Fallback % |
|----------|-------|-------------|-------------------|---------|-----------|------------|
| thyroglobulin | 21,257 | 21,152 | 0 | 105 | 99.5% | 0.0% |
| TSH | 3,196 | 0 | 0 | 3,196 | 0.0% | 0.0% |
| vitamin_D | 1,915 | 0 | 0 | 1,915 | 0.0% | 0.0% |
| PTH | 860 | 0 | 0 | 860 | 0.0% | 0.0% |
| anti_thyroglobulin | 766 | 748 | 0 | 18 | 97.7% | 0.0% |
| calcium | 707 | 0 | 0 | 707 | 0.0% | 0.0% |

**Overall:** 76.3% use lab-specific date; 0.0% fall back to note_date.

## Strict Date Precedence Rule

```sql
-- Enforced in provenance_enriched_events_v1
COALESCE(
    TRY_CAST(specimen_collect_dt AS DATE),   -- 1. Lab collection date (confidence 1.0)
    TRY_CAST(event_date AS DATE),             -- 2. Entity-extracted date (confidence 0.7)
    followup_date                             -- 3. Note encounter date (fallback)
) AS event_date_correct
```

## Remediation Guidance

Tables missing `source_table` / `source_column` should have these added via ALTER TABLE
in `scripts/27_date_provenance_formalization.sql`.

Tables missing `extraction_method` / `evidence_span` are non-NLP tables (structured Excel).
For structured tables, use `source_table = 'table_name'` and `source_column = 'column_name'`
as the provenance link in downstream analytic views.

---

_Generated by `scripts/46_provenance_audit.py`_