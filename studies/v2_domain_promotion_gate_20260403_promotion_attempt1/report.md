# V2 Domain Promotion Gate — Validation Report

- Run: `20260403_promotion_attempt1`
- Generated: `2026-04-03T09:27:27.260140+00:00`
- Domains in registry: `44`
- Domains with parquets on disk: `32`
- Total rows validated: `286,962`
- Overall verdict: **FAIL**

## Gate Results

- **G1** [FAIL] Domain completeness: Missing canonical parquets: ['staging', 'genetics', 'procedures', 'operative_detail', 'complications', 'medications', 'problem_list', 'us_nodule_dynamics', 'cervical_ln_detail', 'presenting_symptoms', 'frozen_section_detail']
- **G2** [FAIL] Schema compliance: Schema failures: ['imaging', 'tirads_granular', 'labs', 'tg_kinetics', 'pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'rad_treatment', 'parathyroid_detail', 'recurrence', 'survival_followup', 'functional_outcomes', 'past_medical_hx', 'past_surgical_hx', 'physical_exam', 'vascular_invasion', 'airway_invasion', 'dynamic_risk_response', 'patient_decision_adherence', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED']
- **G3** [PASS] Provenance columns: All domains have at least one provenance column
- **G4** [PASS] Duplicate rate: All domains below 5% duplicate threshold
- **G5** [FAIL] Date coverage (critical domains): Critical domains with 0% entity_date fill: ['pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'recurrence', 'vascular_invasion']
- **G6** [PASS] Concordance floor (critical domains): All critical domains meet 30% concordance floor
- **G7** [FAIL] Unresolved discordance: 2869 discordant rows in review queue — all require manual verification before promotion
- **G8** [PASS] MotherDuck v2_stage parity: Skipped (--motherduck-check not set or MOTHERDUCK_TOKEN missing)
