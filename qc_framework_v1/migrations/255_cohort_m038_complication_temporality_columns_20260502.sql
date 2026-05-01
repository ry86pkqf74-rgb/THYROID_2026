-- =============================================================================
-- mig_255 — manuscript_workspace.cohort_m038_massive_goiter_v1
--           complication temporality / preop signal columns (M038 standing rule)
-- =============================================================================
-- Date:   2026-05-02
-- Lane:   CF-COHORT-VIEW-COMPLICATION-TEMPORALITY-COLUMNS (closes Option A from
--          memory/feedback_complications_transient_vs_permanent.md)
--
-- CONTEXT:
--   mig_251 extended cohort_m038 to ~117 cols but omitted hypopara/hypocal/RLN/VC
--   temporality fields that exist on main.canonical_patient_master and are
--   required for Table 4 splits (hypopara transient vs permanent; hypocal
--   pre_surgery timing_window; timing_window for voice/RLN readout).
--
-- SCOPE:
--   CREATE OR REPLACE VIEW only — zero CPM mutations.
--
-- GOVERNANCE:
--   Database: thyroid_canonical_publication_v1_0
--
-- VERIFY (post-apply):
--   SELECT COUNT(*) FROM manuscript_workspace.cohort_m038_massive_goiter_v1;  -- 10871
--   DESCRIBE SELECT comp_hypoparathyroidism_transient, comp_hypocalcemia_timing_window
--     FROM manuscript_workspace.cohort_m038_massive_goiter_v1 LIMIT 1;
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m038_massive_goiter_v1 AS
SELECT
  -- ------------------------------------------------------------------
  -- Original 24 columns (preserved, in original order)
  -- ------------------------------------------------------------------
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  p.surg_procedure_type,
  p.surg_first_date,
  p.gland_weight_final_g,
  p.gland_weight_total_reported_g,
  p.ct_substernal_extension_any,
  p.mri_substernal_any,
  p.ct_tracheal_deviation_any,
  p.ct_tracheal_narrowing_any,
  p.ct_airway_compromise_any,
  p.ct_goiter_present_any,
  p.nlp_airway_has_data,
  p.nlp_airway_key_finding,
  p.histology_final,
  p.is_malignant,
  p.op_findings_summary,
  p.any_confirmed_complication_flag,
  p.comp_hematoma_confirmed,
  p.comp_rln_injury_confirmed,
  p.death_occurred,
  p.followup_years,

  -- ------------------------------------------------------------------
  -- DEMOGRAPHICS (extension; required by Table 1 standing rule)
  -- ------------------------------------------------------------------
  p.bmi_combined,
  p.bmi_source,
  p.bmi_missingness_reason,
  p.nsqip_bmi,
  p.nsqip_height_in,
  p.nsqip_weight_lbs,
  p.nsqip_smoker,
  p.nsqip_tobacco_use,
  p.pmhx_nlp_smoking_status,

  -- ------------------------------------------------------------------
  -- COMORBIDITIES & PAST HISTORY
  -- ------------------------------------------------------------------
  p.nsqip_asa_class,
  p.nsqip_diabetes,
  p.nsqip_hypertension,
  p.nsqip_copd,
  p.nsqip_heart_failure,
  p.nsqip_bleeding_disorder,
  p.nsqip_disseminated_cancer,
  p.nsqip_functional_status,
  p.pmhx_nlp_diabetes,
  p.pmhx_nlp_hypertension,
  p.pmhx_nlp_cad,
  p.pmhx_nlp_ckd,
  p.pmhx_nlp_copd,
  p.pmhx_nlp_n_comorbidities,
  p.pmhx_nlp_autoimmune_thyroid_hx,
  p.syn_graves,
  p.syn_hashimoto,
  p.ops_anticoagulation_meds,
  p.pshx_nlp_prior_thyroidectomy,
  p.pshx_nlp_prior_neck_surgery,

  -- ------------------------------------------------------------------
  -- SURGICAL CONTEXT (operative complexity proxies)
  -- ------------------------------------------------------------------
  p.surg_total_thyroidectomy,
  p.surg_hemithyroidectomy,
  p.surg_n_procedures,
  p.nsqip_central_neck_dissection,
  p.nsqip_lateral_neck_dissection,
  p.nsqip_operative_approach,
  p.nsqip_operative_duration_min,
  p.nsqip_drain_usage,
  p.nsqip_vessel_sealant,
  p.nsqip_rln_monitoring,
  p.ops_difficult_airway,
  p.ops_surgeon,
  p.ops_surg_date,
  p.nsqip_inpatient_outpatient,
  p.nsqip_same_day_discharge_flag,
  p.nsqip_primary_indication,

  -- ------------------------------------------------------------------
  -- ANATOMY (lobe size — corroborates the weight-based exposure)
  -- ------------------------------------------------------------------
  p.syn_isthmus_height_cm,
  p.syn_left_lobe_height_cm,
  p.syn_right_lobe_height_cm,

  -- ------------------------------------------------------------------
  -- PATHOLOGY (descriptive context; supports incidental-malignancy panel)
  -- ------------------------------------------------------------------
  p.bilateral_disease_flag,
  p.bilateral_path_flag,
  p.closest_margin_mm,

  -- ------------------------------------------------------------------
  -- LENGTH OF STAY & DISPOSITION
  -- ------------------------------------------------------------------
  p.nsqip_hospital_los_days,
  p.nsqip_length_of_stay_days,
  p.nsqip_surgical_los_days,
  p.nsqip_admission_date,
  p.nsqip_discharge_date,
  p.nsqip_discharge_destination,

  -- ------------------------------------------------------------------
  -- EXPANDED CONFIRMED COMPLICATIONS (descriptive panel)
  -- mig_255: transient / timing_window / preop-ish signals passthrough from CPM.
  -- ------------------------------------------------------------------
  p.comp_airway_complication_definitive,
  p.comp_pneumothorax_definitive,
  p.comp_vc_paresis_confirmed,
  p.comp_vc_paresis_permanent,
  p.comp_vc_paralysis_confirmed,
  p.comp_vc_paralysis_permanent,
  p.comp_hypocalcemia_confirmed,
  p.comp_hypocalcemia_permanent,
  p.comp_hypoparathyroidism_confirmed,
  p.comp_hypoparathyroidism_permanent,
  p.comp_chyle_leak_confirmed,
  p.comp_seroma_confirmed,
  p.comp_mortality_definitive,
  -- mig_255 passthrough — temporality / preop-aligned signals from CPM
  p.comp_hypoparathyroidism_transient,
  p.comp_hypopara_permanent_limitation_note,
  p.comp_hypoparathyroidism_timing_window,
  p.comp_hypoparathyroidism_preexisting,
  p.comp_hypoparathyroidism_new_postop,
  p.comp_hypocalcemia_timing_window,
  p.comp_hypocalcemia_transient,
  p.comp_hypocalcemia_clinical_preexisting,
  p.comp_rln_injury_timing_window,
  p.comp_rln_injury_transient,
  p.comp_vc_paralysis_timing_window,
  p.comp_vc_paresis_timing_window,

  -- ------------------------------------------------------------------
  -- NSQIP-DERIVED PERIOPERATIVE OUTCOMES (descriptive — sparse coverage)
  -- ------------------------------------------------------------------
  p.nsqip_transfusion,
  p.nsqip_neck_hematoma,
  p.nsqip_hematoma_flag,
  p.nsqip_rln_injury_flag,
  p.nsqip_hypocalcemia_flag,
  p.nsqip_unplanned_intubation,
  p.nsqip_unplanned_return_or,
  p.nsqip_readmission_30d_flag,
  p.nsqip_readmission_count,
  p.nsqip_death_30d,
  p.nsqip_pneumonia,
  p.nsqip_dvt,
  p.nsqip_pe,
  p.nsqip_sepsis,
  p.nsqip_superficial_ssi,
  p.nsqip_deep_ssi,
  p.nsqip_organ_space_ssi,

  -- ------------------------------------------------------------------
  -- TRACHEOSTOMY (NLP-derived; relevant secondary outcome for massive goiter)
  -- ------------------------------------------------------------------
  p.proc_nlp_tracheostomy,
  p.proc_nlp_tracheostomy_date,
  p.proc_nlp_tracheostomy_days_from_surg,
  p.proc_nlp_tracheostomy_n_mentions,

  -- ------------------------------------------------------------------
  -- RECURRENCE (descriptive only — median follow-up = 0 yr in cohort)
  -- ------------------------------------------------------------------
  p.any_recurrence_flag,
  p.biochemical_recurrence_flag

FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p;

-- =============================================================================
-- Post-apply (informational)
-- =============================================================================
-- SELECT COUNT(*) FROM information_schema.columns
-- WHERE table_catalog = CURRENT_DATABASE()
--   AND table_schema = 'manuscript_workspace'
--   AND table_name = 'cohort_m038_massive_goiter_v1';
-- -- expected: prior ~117 + 11 = ~128 columns (exact count depends on driver)
--
-- SPLIT SANITY vs M038 Table 4 (massive arm n=2501 predicate = composite gate1):
--   Uses analysis-time massive flag expression from manuscript lineage, not reproduced here.
-- =============================================================================
-- End mig_255
-- =============================================================================
