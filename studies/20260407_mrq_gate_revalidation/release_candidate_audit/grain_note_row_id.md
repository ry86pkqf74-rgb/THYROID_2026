# Grain check (note_row_id)

| domain                        | stem                                            | schema   |   count_star |   distinct_note_row_id | one_row_per_note   |
|:------------------------------|:------------------------------------------------|:---------|-------------:|-----------------------:|:-------------------|
| imaging                       | note_entities_llm_imaging                       | v2_stage |        11037 |                  11037 | True               |
| imaging                       | note_entities_llm_imaging                       | main     |        11037 |                  11037 | True               |
| tirads_granular               | note_entities_llm_tirads_granular               | v2_stage |        11037 |                  11037 | True               |
| tirads_granular               | note_entities_llm_tirads_granular               | main     |        11037 |                  11037 | True               |
| us_nodule_dynamics            | note_entities_llm_us_nodule_dynamics            | v2_stage |        11037 |                  11037 | True               |
| us_nodule_dynamics            | note_entities_llm_us_nodule_dynamics            | main     |        11037 |                  11037 | True               |
| labs                          | note_entities_llm_labs                          | v2_stage |        11037 |                  11037 | True               |
| labs                          | note_entities_llm_labs                          | main     |        11037 |                  11037 | True               |
| tg_kinetics                   | note_entities_llm_tg_kinetics                   | v2_stage |        11037 |                  11037 | True               |
| tg_kinetics                   | note_entities_llm_tg_kinetics                   | main     |        11037 |                  11037 | True               |
| pathology                     | note_entities_llm_pathology                     | v2_stage |        11037 |                  11037 | True               |
| pathology                     | note_entities_llm_pathology                     | main     |        11037 |                  11037 | True               |
| synoptic_pathology_enrichment | note_entities_llm_synoptic_pathology_enrichment | v2_stage |        11037 |                  11037 | True               |
| synoptic_pathology_enrichment | note_entities_llm_synoptic_pathology_enrichment | main     |        11037 |                  11037 | True               |
| rai_detailed                  | note_entities_llm_rai_detailed                  | v2_stage |        11037 |                  11037 | True               |
| rai_detailed                  | note_entities_llm_rai_detailed                  | main     |        11037 |                  11037 | True               |
| rad_treatment                 | note_entities_llm_rad_treatment                 | v2_stage |        11037 |                  11037 | True               |
| rad_treatment                 | note_entities_llm_rad_treatment                 | main     |        11037 |                  11037 | True               |
| parathyroid_detail            | note_entities_llm_parathyroid_detail            | v2_stage |        11037 |                  11037 | True               |
| parathyroid_detail            | note_entities_llm_parathyroid_detail            | main     |        11037 |                  11037 | True               |
| recurrence                    | note_entities_llm_recurrence                    | v2_stage |        11037 |                  11037 | True               |
| recurrence                    | note_entities_llm_recurrence                    | main     |        11037 |                  11037 | True               |
| survival_followup             | note_entities_llm_survival_followup             | v2_stage |        11037 |                  11037 | True               |
| survival_followup             | note_entities_llm_survival_followup             | main     |        11037 |                  11037 | True               |
| cervical_ln_detail            | note_entities_llm_cervical_ln_detail            | v2_stage |        11037 |                  11037 | True               |
| cervical_ln_detail            | note_entities_llm_cervical_ln_detail            | main     |        11037 |                  11037 | True               |
| functional_outcomes           | note_entities_llm_functional_outcomes           | v2_stage |        11037 |                  11037 | True               |
| functional_outcomes           | note_entities_llm_functional_outcomes           | main     |        11037 |                  11037 | True               |
| past_medical_hx               | note_entities_llm_past_medical_hx               | v2_stage |        11037 |                  11037 | True               |
| past_medical_hx               | note_entities_llm_past_medical_hx               | main     |        11037 |                  11037 | True               |
| past_surgical_hx              | note_entities_llm_past_surgical_hx              | v2_stage |        11037 |                  11037 | True               |
| past_surgical_hx              | note_entities_llm_past_surgical_hx              | main     |        11037 |                  11037 | True               |
| presenting_symptoms           | note_entities_llm_presenting_symptoms           | v2_stage |        11037 |                  11037 | True               |
| presenting_symptoms           | note_entities_llm_presenting_symptoms           | main     |        11037 |                  11037 | True               |
| physical_exam                 | note_entities_llm_physical_exam                 | v2_stage |        11037 |                  11037 | True               |
| physical_exam                 | note_entities_llm_physical_exam                 | main     |        11037 |                  11037 | True               |
| vascular_invasion             | note_entities_llm_vascular_invasion             | v2_stage |        11037 |                  11037 | True               |
| vascular_invasion             | note_entities_llm_vascular_invasion             | main     |        11037 |                  11037 | True               |
| airway_invasion               | note_entities_llm_airway_invasion               | v2_stage |        11037 |                  11037 | True               |
| airway_invasion               | note_entities_llm_airway_invasion               | main     |        11037 |                  11037 | True               |
| frozen_section_detail         | note_entities_llm_frozen_section_detail         | v2_stage |        11037 |                  11037 | True               |
| frozen_section_detail         | note_entities_llm_frozen_section_detail         | main     |        11037 |                  11037 | True               |
| dynamic_risk_response         | note_entities_llm_dynamic_risk_response         | v2_stage |        11037 |                  11037 | True               |
| dynamic_risk_response         | note_entities_llm_dynamic_risk_response         | main     |        11037 |                  11037 | True               |
| patient_decision_adherence    | note_entities_llm_patient_decision_adherence    | v2_stage |        11037 |                  11037 | True               |
| patient_decision_adherence    | note_entities_llm_patient_decision_adherence    | main     |        11037 |                  11037 | True               |

## Interpretation

For every v2 stem on **v2_stage** and **main**, `COUNT(*)` equals `COUNT(DISTINCT note_row_id)` → **one row per note** across domains (11,037 notes in current RC). This is not entity-level duplication inside those tables.
