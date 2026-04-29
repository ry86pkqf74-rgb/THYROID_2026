-- =============================================================================
-- Migration 130 -- canonical_patient_master OPERATIVE CLUSTER sign-off (partial)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   22 — FIRST patient_master thematic slice (operative / surgery / NSQIP
--         / thyroid op sheet / PMH prior-surgery NLP bleed from probe).
--
-- Scope:  233 columns matching the Lane-22 probe predicate on
--         main.canonical_patient_master (see header block in Cursor task).
--         Live MotherDuck cardinality exceeds the pre-audit "~125" estimate
--         because (a) `column_name LIKE 'op_%'` also matched `ops_*` thyroid
--         op sheet fields (48), and (b) NSQIP linkage carries 102 `nsqip_*`
--         columns on CPM, not ~4.
--
-- NOT the full CPM verification: 1,365 `not_started` columns remain in other
-- clusters (pathology, lymph_node, labs, imaging, rai, recurrence, fna, ete,
-- survival, medications, molecular, complications, frozen_section, demographics,
-- other). canonical_table_signoff_registry_v1.table_status stays in_progress.
--
-- Methodology (Protocol v2):
--   * op_* (60, excl. ops_*): material agreement probe vs
--       canonical_operative_events_v1 BOOL_OR aggregates on TRUE/FALSE PM
--       rows — 0 rows where PM=TRUE and event=FALSE or PM=FALSE and event=TRUE;
--       bulk of PM cells are NULL (unknown / tri-state downstream semantics).
--   * ops_* (48): structured thyroid operative sheet feed (pre-/intra-op fields);
--       verification = source_lineage_operative_sheet_columns_on_cpm.
--   * nsqip_* (102): NSQIP study linkage / registry columns; verification =
--       external_registry_nsqip_study_export_provenance.
--   * surg_* (6): procedure-class flags/counts; derivation vs operative spine
--       + procedure-code family (mig_118 verified).
--   * nlp_ne_procedures_* (2): note_entities_procedures cluster rollups.
--   * pshx_nlp_* prior neck (3): PMH NLP extraction lane bleed-through in probe;
--       verification = nlp_extraction_faithfulness_pmh_prior_neck_context.
--   * Surgery spine (12): MIN/interval dates vs canonical_operative_events_v1
--       plus cross-source anchors; calendar drift documented (102 rows
--       first_surgery_date_v2 vs MIN(surgery_date_native)::DATE) — spine is
--       not operative-events-only. TIMESTAMP first_surgery_date: carry-forward
--       CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE (umbrella CF-100-DATE-RETYPE).
--
-- Cohort parity: COUNT(*)=10,871 = COUNT(DISTINCT research_id) (pre-apply probe).
--
-- 5-gate audit expectation post-apply:
--   Gate 1 verified_tables_total: unchanged (CPM not table-verified).
--   Gate 4 verified_cols_missing_metadata: 0 (every flipped col gets by/method/batch).
--
-- Executed on MotherDuck RW (thyroid_canonical_publication_v1_0) 2026-04-29:
--   column registry + CPM table_signoff refreshed; CF notes applied in 130i.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 130a — 60 cols — derivation_replay_vs_canonical_operative_events_v1_tri_state...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_replay_vs_canonical_operative_events_v1_tri_state_null',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'op_* rollup/NLP-enriched cols; material TRUE/FALSE agreement vs BOOL_OR on operative events = 0; NULL bulk.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('op_drain_placed_any', 'op_esophageal_inv_any', 'op_esophageal_inv_first_date', 'op_esophageal_inv_first_evidence_text', 'op_esophageal_inv_first_source_note_ref', 'op_esophageal_inv_n_notes_documenting', 'op_esophageal_inv_source_table', 'op_findings_summary', 'op_intraop_gross_ete_any', 'op_local_invasion_any', 'op_n_surgeries_with_findings', 'op_nlp_berry_ligament_date', 'op_nlp_berry_ligament_days_from_surg', 'op_nlp_berry_ligament_dissected', 'op_nlp_berry_ligament_mentioned', 'op_nlp_berry_ligament_n_mentions', 'op_nlp_drain_date', 'op_nlp_drain_days_from_surg', 'op_nlp_drain_placed', 'op_nlp_drain_placed_n_mentions', 'op_nlp_ebl_date', 'op_nlp_ebl_days_from_surg', 'op_nlp_ebl_ml', 'op_nlp_ebl_n_mentions', 'op_nlp_esophageal_involvement', 'op_nlp_esophageal_n_mentions', 'op_nlp_extraction_method', 'op_nlp_gross_invasion', 'op_nlp_intraop_complication', 'op_nlp_intraop_complication_date', 'op_nlp_intraop_complication_days_from_surg', 'op_nlp_intraop_complication_n_mentions', 'op_nlp_n_source_notes', 'op_nlp_nerve_monitoring_date', 'op_nlp_nerve_monitoring_days_from_surg', 'op_nlp_nerve_monitoring_n_mentions', 'op_nlp_nerve_monitoring_type', 'op_nlp_nerve_monitoring_used', 'op_nlp_note_types', 'op_nlp_parathyroid_autograft', 'op_nlp_parathyroid_autograft_n_mentions', 'op_nlp_parathyroid_date', 'op_nlp_parathyroid_days_from_surg', 'op_nlp_parathyroid_managed', 'op_nlp_parathyroid_managed_n_mentions', 'op_nlp_reoperative_field', 'op_nlp_reoperative_n_mentions', 'op_nlp_rln_finding', 'op_nlp_rln_finding_date', 'op_nlp_rln_finding_days_from_surg', 'op_nlp_rln_finding_n_mentions', 'op_nlp_strap_muscle_involved', 'op_nlp_strap_muscle_n_mentions', 'op_nlp_tracheal_involvement', 'op_nlp_tracheal_n_mentions', 'op_parathyroid_autograft_any', 'op_reoperative_any', 'op_rln_monitoring_any', 'op_strap_muscle_any', 'op_tracheal_inv_any');


-- -----------------------------------------------------------------------------
-- 130b — 48 cols — source_lineage_thyroid_operative_sheet_feed_on_cpm...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'source_lineage_thyroid_operative_sheet_feed_on_cpm',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'ops_* preop/intraop structured sheet fields on CPM.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('ops_anticoagulation_meds', 'ops_bmi', 'ops_cervical_ln_us_performed', 'ops_difficult_airway', 'ops_dominant_nodule_bethesda', 'ops_dominant_nodule_location', 'ops_dominant_nodule_size_us', 'ops_ebl_ml', 'ops_family_hx_thyroid_ca', 'ops_head_neck_us_findings', 'ops_intraop_appearance', 'ops_intraop_nodule_count', 'ops_io_tumor_appearance', 'ops_ll_ag', 'ops_ll_para_visualized', 'ops_ll_resection', 'ops_lu_ag', 'ops_lu_para_visualized', 'ops_lu_resection', 'ops_max_diameter_cm', 'ops_nerve_stim_final', 'ops_nerve_stim_left', 'ops_nerve_stim_right', 'ops_other_ag', 'ops_palpable_lesion', 'ops_para_ag_performed', 'ops_parathyroid_ag_notes', 'ops_parathyroidectomy', 'ops_periop_complications', 'ops_preop_diagnosis', 'ops_preop_imaging_performed', 'ops_preop_laryngoscopy', 'ops_preop_nodules_count_size', 'ops_preop_symptoms', 'ops_prior_neck_irradiation', 'ops_prior_neck_operation', 'ops_rl_ag', 'ops_rl_para_visualized', 'ops_rl_resection', 'ops_ru_ag', 'ops_ru_para_visualized', 'ops_ru_resection', 'ops_skin_to_skin_min', 'ops_supranumerary_para', 'ops_surg_date', 'ops_surgeon', 'ops_thyroid_scintigraphy', 'ops_tumor_side');


-- -----------------------------------------------------------------------------
-- 130c — 102 cols — external_registry_nsqip_study_linkage_on_cpm...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'external_registry_nsqip_study_linkage_on_cpm',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'nsqip_* study/registry join columns.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('nsqip_admission_date', 'nsqip_age_at_surgery', 'nsqip_albumin', 'nsqip_alk_phos', 'nsqip_asa_class', 'nsqip_ast', 'nsqip_bleeding_disorder', 'nsqip_bmi', 'nsqip_bun', 'nsqip_calcium_checked', 'nsqip_calcium_vitd_category', 'nsqip_calcium_vitd_last_check', 'nsqip_calcium_vitd_replacement', 'nsqip_central_neck_dissection', 'nsqip_copd', 'nsqip_cpt_code', 'nsqip_cpt_description', 'nsqip_creatinine', 'nsqip_death_30d', 'nsqip_deep_ssi', 'nsqip_diabetes', 'nsqip_discharge_date', 'nsqip_discharge_destination', 'nsqip_disseminated_cancer', 'nsqip_drain_usage', 'nsqip_dvt', 'nsqip_final_pathology', 'nsqip_first_readmission_date', 'nsqip_functional_status', 'nsqip_hba1c', 'nsqip_heart_failure', 'nsqip_height_in', 'nsqip_hematocrit', 'nsqip_hematoma_flag', 'nsqip_hemoglobin', 'nsqip_hospital_los_days', 'nsqip_hypertension', 'nsqip_hypocalcemia', 'nsqip_hypocalcemia_event', 'nsqip_hypocalcemia_event_type', 'nsqip_hypocalcemia_flag', 'nsqip_hypocalcemia_last_check', 'nsqip_hypocalcemia_postdischarge', 'nsqip_hypocalcemia_predischarge', 'nsqip_hypocalcemia_recovered_flag', 'nsqip_hypoparathyroidism_recovered_flag', 'nsqip_inpatient_outpatient', 'nsqip_inr', 'nsqip_iv_calcium', 'nsqip_lateral_neck_dissection', 'nsqip_length_of_stay_days', 'nsqip_m_classification', 'nsqip_match_method', 'nsqip_molecular_result', 'nsqip_molecular_testing', 'nsqip_multifocal', 'nsqip_n_classification', 'nsqip_neck_hematoma', 'nsqip_neoplasm', 'nsqip_neoplasm_type', 'nsqip_nodes_positive', 'nsqip_nodes_removed', 'nsqip_operation_date', 'nsqip_operative_approach', 'nsqip_operative_duration_min', 'nsqip_organ_space_ssi', 'nsqip_pe', 'nsqip_platelet_count', 'nsqip_pneumonia', 'nsqip_preop_biopsy_result', 'nsqip_primary_indication', 'nsqip_prior_neck_surgery', 'nsqip_pth_checked', 'nsqip_ptt', 'nsqip_readmission_30d_flag', 'nsqip_readmission_count', 'nsqip_related_readmission_count', 'nsqip_rln_injury', 'nsqip_rln_injury_flag', 'nsqip_rln_monitoring', 'nsqip_same_day_discharge_flag', 'nsqip_sepsis', 'nsqip_sex', 'nsqip_smoker', 'nsqip_sodium', 'nsqip_source', 'nsqip_superficial_ssi', 'nsqip_surgery_finish_time', 'nsqip_surgery_start_time', 'nsqip_surgical_los_days', 'nsqip_t_classification', 'nsqip_thyroidectomy_has_data', 'nsqip_thyroidectomy_source_script', 'nsqip_tobacco_use', 'nsqip_total_bilirubin', 'nsqip_transfusion', 'nsqip_unplanned_intubation', 'nsqip_unplanned_readmission_count', 'nsqip_unplanned_return_or', 'nsqip_vessel_sealant', 'nsqip_wbc', 'nsqip_weight_lbs');


-- -----------------------------------------------------------------------------
-- 130d — 6 cols — derivation_procedure_spine_vs_operative_events_mig118_family...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_procedure_spine_vs_operative_events_mig118_family',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'surg_* procedure flags/counts vs operative spine + procedure codes family (mig_118).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('surg_first_date', 'surg_first_days_from_surg', 'surg_hemithyroidectomy', 'surg_n_procedures', 'surg_procedure_type', 'surg_total_thyroidectomy');


-- -----------------------------------------------------------------------------
-- 130e — 2 cols — rollup_vs_note_entities_procedures_cluster...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'rollup_vs_note_entities_procedures_cluster',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'nlp_ne_procedures_* mention counts.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('nlp_ne_procedures_has_data', 'nlp_ne_procedures_n_rows');


-- -----------------------------------------------------------------------------
-- 130f — 3 cols — nlp_extraction_faithfulness_pmh_prior_neck_in_operative_prob...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'nlp_extraction_faithfulness_pmh_prior_neck_in_operative_probe',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'pshx_nlp_* prior neck — PMH NLP lane included because %surgery% probe.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('pshx_nlp_n_prior_procedures', 'pshx_nlp_prior_neck_surgery', 'pshx_nlp_prior_neck_surgery_n_mentions');


-- -----------------------------------------------------------------------------
-- 130g1 — 1 cols — demographics_age_at_surgery_anchor_multi_source...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'demographics_age_at_surgery_anchor_multi_source',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'age_at_surgery — demographics at surgery anchor (operative probe boundary).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('age_at_surgery');


-- -----------------------------------------------------------------------------
-- 130g2 — 1 cols — post_surgery_lab_anchor_tg_nadir_cross_domain...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'post_surgery_lab_anchor_tg_nadir_cross_domain',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'biochemical_tg nadir after surgery — lab/surveillance cross-domain.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('biochemical_tg_nadir_after_surgery');


-- -----------------------------------------------------------------------------
-- 130g3 — 10 cols — surgery_spine_dates_intervals_vs_operative_events_and_cpm_an...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'surgery_spine_dates_intervals_vs_operative_events_and_cpm_anchor',
    batch_id            = 'mig_130_patient_master_operative_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_130 operative cluster (Lane 22). '
                          || 'first/second/third surgery dates and day intervals; v2 DATE clean; first_surgery_date TIMESTAMP → CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE; 102-pt calendar drift vs MIN(operative_events) documented (multi-source spine).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('days_between_first_second_surgery', 'days_between_first_second_surgery_v2', 'first_surgery_date', 'first_surgery_date_v2', 'second_surgery_date', 'second_surgery_date_v2', 'second_surgery_days_from_surg', 'third_surgery_date', 'third_surgery_date_v2', 'third_surgery_days_from_surg');


-- -----------------------------------------------------------------------------
-- 130h — refresh canonical_table_signoff_registry_v1 for CPM (partial progress)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_130: Operative thematic cluster CLOSED (233 cols). '
                        || 'Pathology / lymph_node / labs / pmh_psh / us_imaging / rai / '
                        || 'recurrence / fna / ete / survival / medications / molecular / '
                        || 'complications / frozen_section / demographics / other remain. '
                        || 'Gate-1 table-verified tally unchanged until all CPM cols closed.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 130i — Carry-forward notes: TIMESTAMP clinical-adjacent surgery anchors (DATE policy)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE: clinical calendar spine '
            || 'prefers DATE (first_surgery_date_v2). This col remains TIMESTAMP until '
            || 'batch CF-100 / Script 413 family; joins should CAST AS DATE vs DATE SSOT. '
            || 'Umbrella CF-100-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND column_name IN ('first_surgery_date', 'surg_first_date');


-- =============================================================================
-- end migration 130 — CPM operative cluster verified (233 / 1,598 cols)
-- =============================================================================
