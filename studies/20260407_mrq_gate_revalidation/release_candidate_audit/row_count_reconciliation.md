# Row count reconciliation

| domain                        | stem                                            | qa_tier       |   local_parquet |   v2_stage |   main | stage_eq_local   | main_eq_local   |
|:------------------------------|:------------------------------------------------|:--------------|----------------:|-----------:|-------:|:-----------------|:----------------|
| imaging                       | note_entities_llm_imaging                       | standard      |           11037 |      11037 |  11037 | True             | True            |
| tirads_granular               | note_entities_llm_tirads_granular               | standard      |           11037 |      11037 |  11037 | True             | True            |
| us_nodule_dynamics            | note_entities_llm_us_nodule_dynamics            | standard      |           11037 |      11037 |  11037 | True             | True            |
| labs                          | note_entities_llm_labs                          | standard      |           11037 |      11037 |  11037 | True             | True            |
| tg_kinetics                   | note_entities_llm_tg_kinetics                   | standard      |           11037 |      11037 |  11037 | True             | True            |
| pathology                     | note_entities_llm_pathology                     | critical      |           11037 |      11037 |  11037 | True             | True            |
| synoptic_pathology_enrichment | note_entities_llm_synoptic_pathology_enrichment | critical      |           11037 |      11037 |  11037 | True             | True            |
| rai_detailed                  | note_entities_llm_rai_detailed                  | critical      |           11037 |      11037 |  11037 | True             | True            |
| rad_treatment                 | note_entities_llm_rad_treatment                 | standard      |           11037 |      11037 |  11037 | True             | True            |
| parathyroid_detail            | note_entities_llm_parathyroid_detail            | standard      |           11037 |      11037 |  11037 | True             | True            |
| recurrence                    | note_entities_llm_recurrence                    | critical      |           11037 |      11037 |  11037 | True             | True            |
| survival_followup             | note_entities_llm_survival_followup             | standard      |           11037 |      11037 |  11037 | True             | True            |
| cervical_ln_detail            | note_entities_llm_cervical_ln_detail            | standard      |           11037 |      11037 |  11037 | True             | True            |
| functional_outcomes           | note_entities_llm_functional_outcomes           | informational |           11037 |      11037 |  11037 | True             | True            |
| past_medical_hx               | note_entities_llm_past_medical_hx               | informational |           11037 |      11037 |  11037 | True             | True            |
| past_surgical_hx              | note_entities_llm_past_surgical_hx              | informational |           11037 |      11037 |  11037 | True             | True            |
| presenting_symptoms           | note_entities_llm_presenting_symptoms           | informational |           11037 |      11037 |  11037 | True             | True            |
| physical_exam                 | note_entities_llm_physical_exam                 | informational |           11037 |      11037 |  11037 | True             | True            |
| vascular_invasion             | note_entities_llm_vascular_invasion             | critical      |           11037 |      11037 |  11037 | True             | True            |
| airway_invasion               | note_entities_llm_airway_invasion               | standard      |           11037 |      11037 |  11037 | True             | True            |
| frozen_section_detail         | note_entities_llm_frozen_section_detail         | standard      |           11037 |      11037 |  11037 | True             | True            |
| dynamic_risk_response         | note_entities_llm_dynamic_risk_response         | standard      |           11037 |      11037 |  11037 | True             | True            |
| patient_decision_adherence    | note_entities_llm_patient_decision_adherence    | informational |           11037 |      11037 |  11037 | True             | True            |

## Interpretation

All v2 domains share the same row count on disk and in MD (here: one count across all stems). This matches **current** parquets — not an MD loader bug vs an older promotion report. Typical explanation: note-level (one row per note) exports across domains for the same cohort. Validate grain with COUNT(*) vs COUNT(DISTINCT note_row_id) per stem if needed.
