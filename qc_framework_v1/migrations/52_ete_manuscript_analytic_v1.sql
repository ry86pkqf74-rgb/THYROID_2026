-- ----------------------------------------------------------------------------
-- Repair: cohort_descriptive_full_cohort_v1 TIRADS join (cupm rename; Part B 2026-04-21)
-- Source: scripts/output/.../cohort_descriptive_full_cohort_v1.after.sql
-- ----------------------------------------------------------------------------
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
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id);

-- ============================================================================
-- Migration 52 — ETE manuscript analytic view (ete_manuscript_analytic_v1)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      MANUSCRIPT_ETE
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Row key:  path_event_fingerprint = MD5 over 60 main path columns, unit separator 0x1F.
-- Prerequisite views: each is its own CREATE so MotherDuck can bind the plan
-- (single megastatement with 8x repeated 3.7KB join predicates OOMs the binder).
-- ete_norm: maps ete_clean.ete_grade per manuscript naming.
-- gross_ete_effective: from path_event_discordance (dedup) when present.
-- ============================================================================

-- One row in main -> one (rowid, md5) pair
CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_event_fingerprint_v1 AS
SELECT
  m.rowid AS path_malignant_rowid,
  md5(concat(COALESCE(CAST(m.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(m.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint
FROM main.canonical_path_malignant_events_v1 m;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_ete_clean_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_ete_clean t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_global_epi_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_global_epi t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_histology_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_histology_clean t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_variant_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_variant_clean t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_size_flag_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_size_flag t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_laterality_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_invasion_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean t;

CREATE OR REPLACE VIEW manuscript_workspace.path_malignant_overlay_ln_denom_w_fp_v1 AS
SELECT
  md5(concat(COALESCE(CAST(t.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(t.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  t.*
FROM manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag t;

CREATE OR REPLACE VIEW manuscript_workspace.path_event_discordance_dedup_ete_v1 AS
SELECT
  md5(concat(COALESCE(CAST(pk.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.consolidation_source AS VARCHAR), ''))) AS path_event_fingerprint,
  pdd.reported_t_stage_ajcc8,
  pdd.derived_t_stage_ajcc8,
  pdd.discordance_t_stage_flag,
  pdd.gross_ete_effective
FROM manuscript_workspace.path_event_discordance_v1 pdd
INNER JOIN manuscript_workspace.canonical_path_malignant_events_v1_keyed pk
  ON pdd.research_id = pk.research_id
 AND pdd.path_surgery_id IS NOT DISTINCT FROM pk.path_surgery_id
 AND pdd.tumor_ordinal IS NOT DISTINCT FROM pk.tumor_ordinal
 AND pdd.specimen_id IS NOT DISTINCT FROM pk.specimen_id
 AND pdd.synoptic_row_ix IS NOT DISTINCT FROM pk.synoptic_row_ix
 AND pdd.surgery_episode_uid = CAST(pk.surgery_episode_uid AS VARCHAR)
QUALIFY row_number() OVER (PARTITION BY md5(concat(COALESCE(CAST(pk.research_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.surgery_episode_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.tumor_ordinal AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.surgery_date AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.path_surgery_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.specimen_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.synoptic_row_ix AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.laterality AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.site AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.size_greatest_dimension_cm AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.tumor_size_cm_per_surgery AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.primary_histology AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.histology_variant AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.histology_source AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_deprecated_un_versioned_20260417 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.extrathyroidal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.gross_ete AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.lymphatic_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.vascular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.angioinvasion_quantify AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.perineural_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.capsular_invasion AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.margin_status AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ln_examined AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ln_involved AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.nodal_disease_positive_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.nodal_disease_total_count AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.extranodal_extension AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.number_of_tumors AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.multifocality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.source_tables AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.resolution_rule AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.data_completeness_pct AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_group_ajcc7 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.t_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.n_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.m_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.overall_stage_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_group_ajcc8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ajcc7_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.ajcc8_stage_calculable_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.staging_source_note AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.stage_migration_7_to_8 AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_histology_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_t_stage_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_laterality_flag AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.discordance_notes AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.specimen_focus_id AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.linkage_confidence_tier AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.linkage_score AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.build_script AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.build_ts AS VARCHAR), ''), '\x1F', COALESCE(CAST(pk.consolidation_source AS VARCHAR), ''))) ORDER BY pdd.reported_t_stage_ajcc8 NULLS LAST) = 1;

CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v1 AS
WITH
path_event_fingerprint AS (
  SELECT
    h.path_event_fingerprint,
    m.*
  FROM main.canonical_path_malignant_events_v1 m
  INNER JOIN manuscript_workspace.path_malignant_event_fingerprint_v1 h
    ON m.rowid = h.path_malignant_rowid
),
molecular_patient_dedup AS (
  SELECT *
  FROM manuscript_workspace.canonical_molecular_genetics_v2_braf_variant
  QUALIFY row_number() OVER (PARTITION BY research_id ORDER BY test_date_native DESC NULLS LAST) = 1
)
SELECT
  p.research_id,
  p.path_surgery_id,
  g.surgery_episode_id_global,
  p.tumor_ordinal,
  p.specimen_id,
  p.synoptic_row_ix,
  (ptc.research_id IS NOT NULL) AS cohort_ptc,
  (cdf.research_id IS NOT NULL) AS cohort_descriptive_full,
  (
    e.ete_grade IS NOT NULL
    AND g.surgery_episode_id_global IS NOT NULL
    AND g.surgery_episode_uid_source IN ('already_match', 'op_rebind')
    AND p.size_greatest_dimension_cm IS NOT NULL
    AND h.primary_histology_clean IS NOT NULL
    AND h.primary_histology_clean NOT IN (
      'NIFTP', 'FTUMP', 'follicular adenoma',
      'atypical follicular / hurthle neoplasm',
      'uncertain malignant potential (non-FTUMP)'
    )
  ) AS analytic_eligible,
  p.extrathyroidal_extension AS ete_raw,
  e.ete_grade AS ete_norm,
  ddisc.gross_ete_effective,
  EXISTS (
    SELECT 1 FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
    WHERE q.research_id = p.research_id
  ) AS ete_cpm_self_contradiction_flag,
  h.primary_histology_clean AS primary_histology_trusted,
  v.histology_variant_clean AS histology_variant_trusted,
  p.size_greatest_dimension_cm AS size_greatest_dimension_cm_trusted,
  CASE
    WHEN COALESCE(szf.size_disagreement_any_flag, FALSE) THEN 'under_review'
    ELSE 'unflagged'
  END AS size_flag_queue_status,
  lat.derived_laterality_final AS laterality_trusted,
  (mfo.focality = 'multifocal') AS multifocal_flag,
  inv.vascular_invasion_clean   AS vascular_invasion_trusted,
  inv.lymphatic_invasion_clean  AS lymphatic_invasion_trusted,
  inv.perineural_invasion_clean AS perineural_invasion_trusted,
  ln.ln_path_examined AS ln_examined_total,
  ln.ln_path_positive AS ln_positive_total,
  (NOT COALESCE(lnf.ln_denom_missing_any_flag, FALSE)) AS ln_denominator_reliable_flag,
  ddisc.reported_t_stage_ajcc8,
  ddisc.derived_t_stage_ajcc8,
  ddisc.discordance_t_stage_flag AS t_stage_discordance_flag,
  COALESCE(p.overall_stage_ajcc8, p.overall_stage_ajcc7) AS ajcc_overall_stage_trusted,
  op.procedure_normalized_trusted,
  op.surgery_date_native AS surgery_date_native,
  op.laterality AS surgery_laterality_trusted,
  fna.bethesda_final_recomputed AS max_preop_bethesda,
  mol.braf_variant_derived,
  mol.ras_flag,
  mol.tert_flag,
  mol.ret_fusion_flag,
  rc.any_recurrence_final AS recurrence_ever_trusted,
  CASE
    WHEN mc.first_surgery_date IS NOT NULL AND mc.recurrence_date IS NOT NULL
    THEN date_diff('day', CAST(mc.first_surgery_date AS DATE), CAST(mc.recurrence_date AS DATE))
  END AS days_to_first_recurrence,
  cpm.last_contact_date AS last_known_alive_date,
  cpm.vital_status AS vital_status_trusted,
  'manuscript_workspace.ete_manuscript_analytic_v1: 9 path_fp helpers + path_event_discordance_dedup_ete_v1' AS ete_source_table,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM path_event_fingerprint p
LEFT JOIN manuscript_workspace.path_malignant_overlay_ete_clean_w_fp_v1 e
  ON p.path_event_fingerprint = e.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_global_epi_w_fp_v1 g
  ON p.path_event_fingerprint = g.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_histology_w_fp_v1 h
  ON p.path_event_fingerprint = h.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_variant_w_fp_v1 v
  ON p.path_event_fingerprint = v.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_size_flag_w_fp_v1 szf
  ON p.path_event_fingerprint = szf.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_laterality_w_fp_v1 lat
  ON p.path_event_fingerprint = lat.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_invasion_w_fp_v1 inv
  ON p.path_event_fingerprint = inv.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_malignant_overlay_ln_denom_w_fp_v1 lnf
  ON p.path_event_fingerprint = lnf.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_event_discordance_dedup_ete_v1 ddisc
  ON p.path_event_fingerprint = ddisc.path_event_fingerprint
LEFT JOIN manuscript_workspace.path_episode_multifocality_v1 mfo
  ON p.research_id = mfo.research_id
 AND mfo.surgery_episode_uid = CAST(g.surgery_episode_id_global AS VARCHAR)
LEFT JOIN main.canonical_operative_events_v1 op0
  ON op0.surgery_episode_id = g.surgery_episode_id_global
 AND op0.research_id = p.research_id
LEFT JOIN manuscript_workspace.canonical_operative_events_v1_rule_clean op
  ON op0.research_id = op.research_id
 AND op0.surgery_episode_id = op.surgery_episode_id
LEFT JOIN manuscript_workspace.ln_per_patient_multisource_v1 ln
  ON CAST(p.research_id AS VARCHAR) = ln.research_id
LEFT JOIN manuscript_workspace.canonical_fna_patient_rollup_v1_clean fna
  ON p.research_id = fna.research_id
LEFT JOIN molecular_patient_dedup mol
  ON p.research_id = mol.research_id
LEFT JOIN manuscript_workspace.manuscript_cohort_v1_recurrence_clean rc
  ON p.research_id = rc.research_id
LEFT JOIN main.manuscript_cohort_v1 mc
  ON p.research_id = mc.research_id
LEFT JOIN main.canonical_patient_master cpm
  ON p.research_id = cpm.research_id
LEFT JOIN manuscript_workspace.qc_manuscript_cohort_v2_ptc ptc
  ON p.research_id = ptc.research_id
LEFT JOIN manuscript_workspace.cohort_descriptive_full_cohort_v1 cdf
  ON p.research_id = cdf.research_id
;


-- Probes: row parity; PTC N; elig+ETE; discord% among eligible; OP orphans
DELETE FROM manuscript_workspace.canonical_deprecation_log_v1
 WHERE closing_prompt = 'prompt_51' AND issue_id = 'MANUSCRIPT_ETE';

-- DB NOT NULL on deprecated_object: use self-pointer for additive analytic (prompt text: NULL intent)
INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('manuscript_workspace.ete_manuscript_analytic_v1',
   'analytic_view',
   'manuscript_workspace.ete_manuscript_analytic_v1',
   'MANUSCRIPT_ETE',
   'prompt_51',
   'pointer_only',
   DATE '2026-04-24',
   'ETE per-event view + path_malignant_event_fingerprint_v1 and overlay w_fp + path_event_discordance_dedup_ete_v1. Fuses ete_clean, global_epi, hist/variant/size/lat/invasion/ln_denom, path_event_discordance, op, fna, mol, cohorts.',
   NULL,
   'Caveats: REC05/REC02; AJCC COALESCE(8,7) on event; T-stage from discordance; cpm_ete queue patient-level. Helper views: path_malignant_overlay_*_w_fp_v1, path_event_discordance_dedup_ete_v1.');


