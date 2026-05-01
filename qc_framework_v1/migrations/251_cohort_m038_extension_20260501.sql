-- =============================================================================
-- mig_251 — manuscript_workspace.cohort_m038_massive_goiter_v1 EXTENSION
-- Date:    2026-05-01
-- Author:  Cowork (post mig_250)
-- Lane:    Cowork-direct
-- Tip of origin/main at draft: 0ae2881 (post-mig_250)
-- =============================================================================
--
-- BACKGROUND:
--   The original cohort_m038_massive_goiter_v1 (Script 281 auto-stub) projected
--   24 columns from canonical_patient_master. M038's RQ has been reframed as a
--   "definition paper" comparing weight-based vs. anatomic-compression
--   operationalizations of 'massive' goiter as predictors of perioperative
--   complications (composite any_confirmed_complication_flag; n=146 events in
--   the n=475 ≥200g focal cohort).
--
--   Per Logan's standing rule (2026-05-01): every manuscript must (1) include a
--   demographic Table 1 and (2) carry a systematic full-dataset column review
--   into its cohort view. Feedback file:
--     memory/feedback_manuscript_demographics_and_full_column_review.md
--
--   The original view omitted a large set of relevant columns that exist on
--   canonical_patient_master, including the Table-1 demographic/comorbidity
--   set, NSQIP perioperative outcomes (operative time, transfusion, LOS,
--   unplanned intubation, readmission), the full comp_*_confirmed complication
--   panel, tracheostomy event data, surgical-context details (drain, vessel
--   sealant, RLN monitoring), and lobe-anatomy continuous variables.
--
-- DELIVERABLE:
--   Recreate manuscript_workspace.cohort_m038_massive_goiter_v1 to include the
--   extended column set, preserving all 24 original columns.
--
--   Expected column count post-migration: ~95 columns.
--
-- DESIGN NOTES:
--   * No WHERE filter — the view continues to project the full cohort
--     (10,871 rows) and analysis-time queries subset by gland_weight_final_g.
--     This preserves the comparison set for the definition-paper analysis.
--   * Pure column passthrough from canonical_patient_master (no joins to other
--     canonical tables). All sourced columns are read directly off the master.
--   * No new derivations — keeping computed exposure flags (e.g.,
--     "massive_by_weight", "any_substernal") at analysis-query time, not in
--     the view, to avoid baking a single threshold into the registry layer.
--
-- DEPENDENCIES (verified pre-migration):
--   * canonical_patient_master.bmi_combined         (DOUBLE)
--   * canonical_patient_master.nsqip_asa_class      (VARCHAR)
--   * canonical_patient_master.nsqip_operative_duration_min (BIGINT)
--   * canonical_patient_master.comp_*_confirmed     (BOOLEAN, 6 family flags)
--   * canonical_patient_master.proc_nlp_tracheostomy + date + days_from_surg
--   * canonical_patient_master.syn_left_lobe_height_cm + right + isthmus
--   * canonical_patient_master.pmhx_nlp_*           (10 NLP comorbidity flags)
--   * canonical_patient_master.pshx_nlp_prior_thyroidectomy + prior_neck_surgery
--   ...all confirmed via information_schema lookup pre-write.
--
-- COLUMN COVERAGE IN THE n=475 ≥200g FOCAL SUBSET (2026-05-01 audit):
--   bmi_combined                  80/475 (16.8%)
--   nsqip_asa_class               42/475 ( 8.8%)
--   nsqip_operative_duration_min  68/475 (14.3%)
--   nsqip_length_of_stay_days     42/475 ( 8.8%)
--   nsqip_transfusion             68/475 (14.3%)
--   pmhx_nlp_diabetes              77 events
--   pmhx_nlp_hypertension         124 events
--   syn_graves                     11 events
--   syn_hashimoto                   7 events
--   pshx_nlp_prior_thyroidectomy   19 events
--   comp_seroma_confirmed          39 events
--   comp_vc_paralysis_confirmed     2 events
--   nsqip_unplanned_intubation      2 events
--   proc_nlp_tracheostomy          14 events (full massive subset)
--
--   Sparse columns are preserved deliberately — they belong in Table 1 with
--   "n with data" footnotes; alternatives (drop them) would force a worse
--   reviewer-facing table.
--
-- POST-MIGRATION VERIFICATION:
--   * gate1 expected unchanged at 218 (no registry impact).
--   * gates 2-5 expected at 0.
--   * cohort_parity TRUE (10871×3) — view continues to project full cohort.
--   * Column count check: target ~95 columns.
--   * Sample query: confirm primary outcome events still 146/475 in ≥200g.
--
-- ROLLBACK:
--   The original 24-column projection is reproduced in the
--   --- ROLLBACK STUB --- block at the bottom of this file.
--
-- =============================================================================

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
  -- Note: any_confirmed_complication_flag (above) remains the primary outcome.
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
-- POST-APPLY VERIFICATION (run as separate query after CREATE OR REPLACE):
-- =============================================================================
--
-- 1. Cohort still 10,871 rows:
--    SELECT COUNT(*) FROM manuscript_workspace.cohort_m038_massive_goiter_v1;
--    -- expected: 10871
--
-- 2. Column count expanded:
--    SELECT COUNT(*) FROM information_schema.columns
--    WHERE table_schema='manuscript_workspace' AND table_name='cohort_m038_massive_goiter_v1';
--    -- expected: ~95 (was 24)
--
-- 3. Primary outcome unchanged:
--    SELECT COUNT(*) AS n, SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS comps
--    FROM manuscript_workspace.cohort_m038_massive_goiter_v1
--    WHERE gland_weight_final_g >= 200;
--    -- expected: 475, 146
--
-- 4. Gates unchanged:
--    SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
--    -- expected: gate1=218, gates 2-5 = 0
--
-- 5. Smoke-test new columns:
--    SELECT COUNT(bmi_combined) AS n_bmi,
--           COUNT(nsqip_asa_class) AS n_asa,
--           COUNT(nsqip_operative_duration_min) AS n_op_time,
--           SUM(CASE WHEN proc_nlp_tracheostomy THEN 1 ELSE 0 END) AS n_trach
--    FROM manuscript_workspace.cohort_m038_massive_goiter_v1
--    WHERE gland_weight_final_g >= 200;
--    -- expected: 80, 42, 68, 14

-- =============================================================================
-- ROLLBACK STUB (revert to original 24-column projection if needed):
-- =============================================================================
--
-- CREATE OR REPLACE VIEW manuscript_workspace.cohort_m038_massive_goiter_v1 AS
-- SELECT p.research_id, p.age_at_surgery, p.sex, p.race, p.surg_procedure_type,
--        p.surg_first_date, p.gland_weight_final_g, p.gland_weight_total_reported_g,
--        p.ct_substernal_extension_any, p.mri_substernal_any,
--        p.ct_tracheal_deviation_any, p.ct_tracheal_narrowing_any,
--        p.ct_airway_compromise_any, p.ct_goiter_present_any,
--        p.nlp_airway_has_data, p.nlp_airway_key_finding, p.histology_final,
--        p.is_malignant, p.op_findings_summary, p.any_confirmed_complication_flag,
--        p.comp_hematoma_confirmed, p.comp_rln_injury_confirmed, p.death_occurred,
--        p.followup_years
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master AS p;
