# V2 Domain Promotion Gate — Validation Report

- Run: `20260403_promotion_attempt2`
- Generated: `2026-04-03T09:51:08.436270+00:00`
- Domains in registry: `44`
- Domains with parquets on disk: `34`
- Total rows validated: `55,331`
- Overall verdict: **FAIL**

## Gate Results

- **G1** [FAIL] Domain completeness (v2 only): Missing v2 canonical parquets: ['us_nodule_dynamics', 'frozen_section_detail']
- **G2** [FAIL] Schema compliance: Schema failures: ['imaging', 'tirads_granular', 'labs', 'tg_kinetics', 'pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'rad_treatment', 'parathyroid_detail', 'recurrence', 'survival_followup', 'cervical_ln_detail', 'functional_outcomes', 'past_medical_hx', 'past_surgical_hx', 'presenting_symptoms', 'physical_exam', 'vascular_invasion', 'airway_invasion', 'dynamic_risk_response', 'patient_decision_adherence']
- **G3** [FAIL] Provenance columns: NO domain has provenance columns (('preprocess_batch_id', 'preprocess_script_version', 'preprocessed_at_utc')). This is a structural gap in the extraction pipeline.
- **G4** [FAIL] Duplicate rate: Domains with >5% duplicates: [{'domain_name': 'labs', 'dup_rate': 0.122}, {'domain_name': 'tg_kinetics', 'dup_rate': 0.104}, {'domain_name': 'cervical_ln_detail', 'dup_rate': 0.0962}, {'domain_name': 'patient_decision_adherence', 'dup_rate': 0.0655}]
- **G5** [PASS] Date coverage (critical domains): All critical domains have date coverage (entity_date or note_date)
- **G6** [PASS] Concordance floor (critical domains): All critical domains meet 30% concordance floor
- **G7** [FAIL] Unresolved discordance: 2896 discordant rows in review queue — all require manual verification before promotion
- **G8** [FAIL] MotherDuck v2_stage parity: Parity failures: ['v2_stage.note_entities_llm_imaging', 'v2_stage.note_entities_llm_tirads_granular', 'v2_stage.note_entities_llm_labs', 'v2_stage.note_entities_llm_tg_kinetics', 'v2_stage.note_entities_llm_pathology', 'v2_stage.note_entities_llm_synoptic_pathology_enrichment', 'v2_stage.note_entities_llm_rai_detailed', 'v2_stage.note_entities_llm_rad_treatment', 'v2_stage.note_entities_llm_parathyroid_detail', 'v2_stage.note_entities_llm_recurrence', 'v2_stage.note_entities_llm_survival_followup', 'v2_stage.note_entities_llm_cervical_ln_detail', 'v2_stage.note_entities_llm_functional_outcomes', 'v2_stage.note_entities_llm_past_medical_hx', 'v2_stage.note_entities_llm_past_surgical_hx', 'v2_stage.note_entities_llm_presenting_symptoms', 'v2_stage.note_entities_llm_physical_exam', 'v2_stage.note_entities_llm_vascular_invasion', 'v2_stage.note_entities_llm_airway_invasion', 'v2_stage.note_entities_llm_dynamic_risk_response', 'v2_stage.note_entities_llm_patient_decision_adherence']
