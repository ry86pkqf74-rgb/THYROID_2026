-- View: manuscript_workspace.cohort_m064_frozen_decision_v1
-- Pulled: 2026-04-21 Part B Phase 2 recon

CREATE VIEW manuscript_workspace.cohort_m064_frozen_decision_v1 AS SELECT research_id, age_at_surgery, sex, surg_procedure_type, is_malignant, histology_final, tumor_size_cm, syn_frozen_section, syn_frozen_section_result, syn_carcinoma_on_frozen, nlp_frozensec_has_data, nlp_frozensec_key_finding, bethesda_final, tirads_best_category_v12, molecular_tested_confirmed, surg_hemithyroidectomy, surg_total_thyroidectomy, n_surgeries, ajcc8_stage_group, ata_risk_category, any_recurrence_flag, overall_survival_years FROM thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_descriptive_full_cohort_v1;
