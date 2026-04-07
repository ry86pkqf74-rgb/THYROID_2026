# MotherDuck Formalization Release Validation Report

**Generated:** 2026-04-07T05:08:38.948774+00:00
**Mode:** Release Validation
**Total checks:** 20
**Passed:** 19  |  **Warned:** 0  |  **Failed:** 1

**VERDICT: BLOCKED** — 1 check(s) failed.

---

## Check Results

| Check | Status | Detail |
|-------|--------|--------|
| MD attachment | PASS | 6 databases attached |
| Row count parity | PASS | 23 domains checked, all match |
| Canonical canonical_extracted_fact_long_v2 | PASS | local=123,577  md=123,577 |
| Canonical canonical_fact_quarantine_v2 | PASS | local=199  md=199 |
| Canonical note_extraction_runs | PASS | local=3  md=3 |
| Schema completeness | PASS | Wide note-level v2 contract on 23 promoted table(s); entity_type/entity_value_* in main.canonical_extracted_fact_long_v2 (see docs/domain_mapping_rules.md). Example stems: note_entities_llm_imaging, note_entities_llm_tirads_granular, note_entities_llm_us_nodule_dynamics… |
| Canonical dist (canonical_extracted_fact_long_v2) | PASS | 594 domains, 123,577 total rows |
| Canonical dist (canonical_fact_quarantine_v2) | PASS | 69 domains, 199 total rows |
| Review queue | FAIL | 16,866 total, 11,244 reviewed, 5,622 PENDING — must be resolved before release |
| QA view promotion_scorecard_summary_v | PASS | 6 rows |
| QA view domain_validation_summary_v | PASS | 6 rows |
| QA view date_provenance_completeness_v | PASS | 23 rows |
| QA view manual_review_queue_summary_v | PASS | 48 rows |
| Load inventory | PASS | 180 entries, all match |
| Release schemas | PASS | 5 found: release_20260406, release_20260407, release_20260407_final, release_20260408, release_20260409 |
| Release manifest | PASS | 5 release(s); latest: 20260409 (2026-04-07 02:05:07.189573) |
| Canonical extraction_run_id | PASS | 123,577 rows; 0 blank (contract §3) |
| Presentation master_fact_long_verified_v1 | PASS | 123,577 rows; core traceability non-null (reviewer_status may be NULL) |
| Presentation master_source_lineage_v1 | PASS | 123,577 rows; core traceability non-null (reviewer_status may be NULL) |
| Presentation master_patient_rollup_verified_v1 | PASS | 5,574 patient rows; research_id + release_tag + review metrics present |

---

## Row Count Parity (v2 domains)

| Domain | Stem | Local | v2_stage | main | Stage Match | Main Match |
|--------|------|------:|--------:|-----:|:-----------:|:----------:|
| imaging | note_entities_llm_imaging | 11,037 | 11,037 | 11,037 | Y | Y |
| tirads_granular | note_entities_llm_tirads_granular | 11,037 | 11,037 | 11,037 | Y | Y |
| us_nodule_dynamics | note_entities_llm_us_nodule_dynamics | 11,037 | 11,037 | 11,037 | Y | Y |
| labs | note_entities_llm_labs | 11,037 | 11,037 | 11,037 | Y | Y |
| tg_kinetics | note_entities_llm_tg_kinetics | 11,037 | 11,037 | 11,037 | Y | Y |
| pathology | note_entities_llm_pathology | 11,037 | 11,037 | 11,037 | Y | Y |
| synoptic_pathology_enrichment | note_entities_llm_synoptic_pathology_enrichment | 11,037 | 11,037 | 11,037 | Y | Y |
| rai_detailed | note_entities_llm_rai_detailed | 11,037 | 11,037 | 11,037 | Y | Y |
| rad_treatment | note_entities_llm_rad_treatment | 11,037 | 11,037 | 11,037 | Y | Y |
| parathyroid_detail | note_entities_llm_parathyroid_detail | 11,037 | 11,037 | 11,037 | Y | Y |
| recurrence | note_entities_llm_recurrence | 11,037 | 11,037 | 11,037 | Y | Y |
| survival_followup | note_entities_llm_survival_followup | 11,037 | 11,037 | 11,037 | Y | Y |
| cervical_ln_detail | note_entities_llm_cervical_ln_detail | 11,037 | 11,037 | 11,037 | Y | Y |
| functional_outcomes | note_entities_llm_functional_outcomes | 11,037 | 11,037 | 11,037 | Y | Y |
| past_medical_hx | note_entities_llm_past_medical_hx | 11,037 | 11,037 | 11,037 | Y | Y |
| past_surgical_hx | note_entities_llm_past_surgical_hx | 11,037 | 11,037 | 11,037 | Y | Y |
| presenting_symptoms | note_entities_llm_presenting_symptoms | 11,037 | 11,037 | 11,037 | Y | Y |
| physical_exam | note_entities_llm_physical_exam | 11,037 | 11,037 | 11,037 | Y | Y |
| vascular_invasion | note_entities_llm_vascular_invasion | 11,037 | 11,037 | 11,037 | Y | Y |
| airway_invasion | note_entities_llm_airway_invasion | 11,037 | 11,037 | 11,037 | Y | Y |
| frozen_section_detail | note_entities_llm_frozen_section_detail | 11,037 | 11,037 | 11,037 | Y | Y |
| dynamic_risk_response | note_entities_llm_dynamic_risk_response | 11,037 | 11,037 | 11,037 | Y | Y |
| patient_decision_adherence | note_entities_llm_patient_decision_adherence | 11,037 | 11,037 | 11,037 | Y | Y |

---

## Canonical Fact Distribution

| Table | Domain | Rows | Patients |
|-------|--------|-----:|--------:|
| canonical_extracted_fact_long_v2 | procedure | 21,945 | 4,723 |
| canonical_extracted_fact_long_v2 | problem | 11,579 | 4,037 |
| canonical_extracted_fact_long_v2 | complication | 9,359 | 2,840 |
| canonical_extracted_fact_long_v2 | medication | 7,502 | 2,070 |
| canonical_extracted_fact_long_v2 | nerve_monitoring | 5,557 | 3,510 |
| canonical_extracted_fact_long_v2 | T_stage | 3,385 | 1,507 |
| canonical_extracted_fact_long_v2 | ultrasound_thyroid | 3,259 | 1,587 |
| canonical_extracted_fact_long_v2 | vital_status | 2,531 | 2,426 |
| canonical_extracted_fact_long_v2 | fna_cytology | 2,464 | 1,700 |
| canonical_extracted_fact_long_v2 | last_followup_date | 2,421 | 2,229 |
| canonical_extracted_fact_long_v2 | nodule_size | 2,080 | 1,081 |
| canonical_extracted_fact_long_v2 | surgical_pathology | 2,040 | 1,366 |
| canonical_extracted_fact_long_v2 | voice_quality | 1,924 | 1,812 |
| canonical_extracted_fact_long_v2 | gene | 1,738 | 605 |
| canonical_extracted_fact_long_v2 | ebl | 1,683 | 1,613 |
| canonical_extracted_fact_long_v2 | vocal_cord_mobility | 1,384 | 1,206 |
| canonical_extracted_fact_long_v2 | swallowing_function | 1,378 | 1,338 |
| canonical_extracted_fact_long_v2 | rln_finding | 1,307 | 832 |
| canonical_extracted_fact_long_v2 | bethesda_class | 1,197 | 985 |
| canonical_extracted_fact_long_v2 | parathyroid_management | 1,178 | 928 |
| canonical_extracted_fact_long_v2 | other_surgery | 1,168 | 543 |
| canonical_extracted_fact_long_v2 | tumor_size | 1,050 | 788 |
| canonical_extracted_fact_long_v2 | prior_thyroidectomy | 1,021 | 863 |
| canonical_extracted_fact_long_v2 | nodule_location | 984 | 623 |
| canonical_extracted_fact_long_v2 | prior_fna | 938 | 783 |
| canonical_extracted_fact_long_v2 | molecular_testing | 926 | 721 |
| canonical_extracted_fact_long_v2 | berry_ligament | 726 | 501 |
| canonical_extracted_fact_long_v2 | treatment_episode_number | 700 | 621 |
| canonical_extracted_fact_long_v2 | tsh | 695 | 567 |
| canonical_extracted_fact_long_v2 | ete_on_imaging | 688 | 640 |
| canonical_extracted_fact_long_v2 | swallowing_function_detail | 682 | 638 |
| canonical_extracted_fact_long_v2 | soft_tissue_invasion | 668 | 633 |
| canonical_extracted_fact_long_v2 | vascular_invasion | 656 | 628 |
| canonical_extracted_fact_long_v2 | tracheal_deviation | 655 | 553 |
| canonical_extracted_fact_long_v2 | rai_dose_mci | 632 | 573 |
| canonical_extracted_fact_long_v2 | capsular_invasion | 629 | 608 |
| canonical_extracted_fact_long_v2 | ct_neck | 589 | 478 |
| canonical_extracted_fact_long_v2 | voice_recovery | 585 | 552 |
| canonical_extracted_fact_long_v2 | ultrasound_lymph_node | 572 | 405 |
| canonical_extracted_fact_long_v2 | post_treatment_wbs_findings | 559 | 528 |
| canonical_extracted_fact_long_v2 | lymph_node_pathology | 544 | 437 |
| canonical_extracted_fact_long_v2 | calcium_quality_of_life | 534 | 521 |
| canonical_extracted_fact_long_v2 | strap_muscle | 506 | 451 |
| canonical_extracted_fact_long_v2 | drain_placement | 482 | 397 |
| canonical_extracted_fact_long_v2 | mass_effect | 463 | 422 |
| canonical_extracted_fact_long_v2 | extrathyroidal_extension | 457 | 417 |
| canonical_extracted_fact_long_v2 | neck_exam | 450 | 415 |
| canonical_extracted_fact_long_v2 | substernal_extension | 448 | 384 |
| canonical_extracted_fact_long_v2 | benign_pathology | 437 | 351 |
| canonical_extracted_fact_long_v2 | preparation_method | 434 | 421 |
| canonical_extracted_fact_long_v2 | tirads_score | 423 | 331 |
| canonical_extracted_fact_long_v2 | ptnm_stage | 413 | 400 |
| canonical_extracted_fact_long_v2 | lymphovascular_invasion | 386 | 351 |
| canonical_extracted_fact_long_v2 | multifocality | 352 | 325 |
| canonical_extracted_fact_long_v2 | margin_status | 349 | 315 |
| canonical_extracted_fact_long_v2 | thyroid_palpation | 347 | 322 |
| canonical_extracted_fact_long_v2 | specimen_detail | 342 | 323 |
| canonical_extracted_fact_long_v2 | pre_rai_tsh | 338 | 336 |
| canonical_extracted_fact_long_v2 | discharge_disposition | 321 | 311 |
| canonical_extracted_fact_long_v2 | prior_rai | 310 | 276 |
| canonical_extracted_fact_long_v2 | frozen_section_result | 302 | 279 |
| canonical_extracted_fact_long_v2 | follow_up_duration | 301 | 299 |
| canonical_extracted_fact_long_v2 | tumor_variant | 294 | 269 |
| canonical_extracted_fact_long_v2 | UNKNOWN | 293 | 65 |
| canonical_extracted_fact_long_v2 | side_effects | 287 | 287 |
| canonical_extracted_fact_long_v2 | rai_indication | 285 | 256 |
| canonical_extracted_fact_long_v2 | overall_stage | 276 | 203 |
| canonical_extracted_fact_long_v2 | isolation_days | 273 | 273 |
| canonical_extracted_fact_long_v2 | perineural_invasion_detailed | 260 | 251 |
| canonical_extracted_fact_long_v2 | synoptic_report | 259 | 256 |
| canonical_extracted_fact_long_v2 | thyroglobulin | 258 | 146 |
| canonical_extracted_fact_long_v2 | vessel_count | 251 | 249 |
| canonical_extracted_fact_long_v2 | prior_cancer | 243 | 164 |
| canonical_extracted_fact_long_v2 | lymph_node_palpation | 241 | 222 |
| canonical_extracted_fact_long_v2 | return_to_work | 240 | 237 |
| canonical_extracted_fact_long_v2 | nuclear_med | 236 | 159 |
| canonical_extracted_fact_long_v2 | dedifferentiation | 234 | 233 |
| canonical_extracted_fact_long_v2 | vascular_encasement | 231 | 208 |
| canonical_extracted_fact_long_v2 | free_t4 | 211 | 183 |
| canonical_extracted_fact_long_v2 | necrosis | 210 | 208 |
| canonical_extracted_fact_long_v2 | treatment_declined | 209 | 185 |
| canonical_extracted_fact_long_v2 | prior_neck_surgery | 199 | 153 |
| canonical_extracted_fact_long_v2 | surveillance_adherence | 197 | 150 |
| canonical_extracted_fact_long_v2 | mitotic_rate | 197 | 196 |
| canonical_extracted_fact_long_v2 | ki67_index | 193 | 192 |
| canonical_extracted_fact_long_v2 | tall_cell_percentage | 192 | 192 |
| canonical_extracted_fact_long_v2 | rai_ablation | 187 | 155 |
| canonical_extracted_fact_long_v2 | tracheal_narrowing | 184 | 171 |
| canonical_extracted_fact_long_v2 | parathyroid_autograft | 178 | 133 |
| canonical_extracted_fact_long_v2 | calcium | 177 | 145 |
| canonical_extracted_fact_long_v2 | airway_exam | 171 | 159 |
| canonical_extracted_fact_long_v2 | laryngoscopy_findings | 163 | 158 |
| canonical_extracted_fact_long_v2 | thyroglobulin_stimulated | 157 | 84 |
| canonical_extracted_fact_long_v2 | laryngoscopy_date | 151 | 147 |
| canonical_extracted_fact_long_v2 | structural_recurrence | 149 | 100 |
| canonical_extracted_fact_long_v2 | prior_neck_dissection | 144 | 120 |
| canonical_extracted_fact_long_v2 | frozen_section | 144 | 135 |
| canonical_extracted_fact_long_v2 | gland_location | 139 | 102 |
| canonical_extracted_fact_long_v2 | reoperative_field | 132 | 109 |
| canonical_extracted_fact_long_v2 | tg_value | 125 | 57 |
| canonical_extracted_fact_long_v2 | vocal_cord_imaging | 124 | 118 |
| canonical_extracted_fact_long_v2 | pre_rai_tg | 122 | 113 |
| canonical_extracted_fact_long_v2 | perineural_invasion | 119 | 113 |
| canonical_extracted_fact_long_v2 | lymph_node_level | 119 | 76 |
| canonical_extracted_fact_long_v2 | vitamin_d | 113 | 106 |
| canonical_extracted_fact_long_v2 | N_stage | 110 | 82 |
| canonical_extracted_fact_long_v2 | post_treatment_scan | 105 | 75 |
| canonical_extracted_fact_long_v2 | airway_compromise_grade | 105 | 100 |
| canonical_extracted_fact_long_v2 | neck_mass | 102 | 83 |
| canonical_extracted_fact_long_v2 | wound_exam | 101 | 89 |
| canonical_extracted_fact_long_v2 | ln_level | 99 | 48 |
| canonical_extracted_fact_long_v2 | patient_preference | 98 | 95 |
| canonical_extracted_fact_long_v2 | free_t3 | 97 | 86 |
| canonical_extracted_fact_long_v2 | autoimmune_thyroid | 97 | 83 |
| canonical_extracted_fact_long_v2 | pth | 96 | 85 |
| canonical_extracted_fact_long_v2 | shared_decision | 94 | 91 |
| canonical_extracted_fact_long_v2 | laryngeal_invasion | 89 | 85 |
| canonical_extracted_fact_long_v2 | voice_assessment | 88 | 88 |
| canonical_extracted_fact_long_v2 | thyroid_hormone_suppression | 87 | 69 |
| canonical_extracted_fact_long_v2 | dysphagia | 85 | 79 |
| canonical_extracted_fact_long_v2 | hypertension | 82 | 80 |
| canonical_extracted_fact_long_v2 | anti_thyroglobulin | 77 | 55 |
| canonical_extracted_fact_long_v2 | quality_of_life_score | 75 | 74 |
| canonical_extracted_fact_long_v2 | esophageal_compression | 74 | 72 |
| canonical_extracted_fact_long_v2 | vascular_invasion_type | 71 | 68 |
| canonical_extracted_fact_long_v2 | calcium_symptom_chronicity | 65 | 62 |
| canonical_extracted_fact_long_v2 | pet_ct | 63 | 49 |
| canonical_extracted_fact_long_v2 | scar_satisfaction | 63 | 62 |
| canonical_extracted_fact_long_v2 | diabetes | 62 | 61 |
| canonical_extracted_fact_long_v2 | cardiovascular | 62 | 38 |
| canonical_extracted_fact_long_v2 | obesity | 62 | 55 |
| canonical_extracted_fact_long_v2 | prior_parathyroidectomy | 61 | 57 |
| canonical_extracted_fact_long_v2 | final_pathology_concordance | 60 | 60 |
| canonical_extracted_fact_long_v2 | family_hx_thyroid | 60 | 53 |
| canonical_extracted_fact_long_v2 | prior_core_biopsy | 56 | 51 |
| canonical_extracted_fact_long_v2 | incidental_finding | 55 | 48 |
| canonical_extracted_fact_long_v2 | scar_assessment | 54 | 54 |
| canonical_extracted_fact_long_v2 | thyroid_nodule | 54 | 30 |
| canonical_extracted_fact_long_v2 | dysphonia | 53 | 51 |
| canonical_extracted_fact_long_v2 | disease_free | 51 | 42 |
| canonical_extracted_fact_long_v2 | radiation_exposure | 51 | 44 |
| canonical_extracted_fact_long_v2 | total_t4 | 48 | 46 |
| canonical_extracted_fact_long_v2 | cranial_nerve_exam | 48 | 48 |
| canonical_extracted_fact_long_v2 | chvostek_sign | 42 | 42 |
| canonical_extracted_fact_long_v2 | thyrogen_stimulation | 41 | 25 |
| canonical_extracted_fact_long_v2 | M_stage | 36 | 32 |
| canonical_extracted_fact_long_v2 | xerostomia | 35 | 35 |
| canonical_extracted_fact_long_v2 | tsi | 34 | 33 |
| canonical_extracted_fact_long_v2 | injection_laryngoplasty | 33 | 32 |
| canonical_extracted_fact_long_v2 | nodule_volume | 33 | 18 |
| canonical_extracted_fact_long_v2 | tirads_category | 33 | 30 |
| canonical_extracted_fact_long_v2 | rai_preparation | 32 | 30 |
| canonical_extracted_fact_long_v2 | stunning_concern | 31 | 31 |
| canonical_extracted_fact_long_v2 | speech_therapy_referral | 31 | 30 |
| canonical_extracted_fact_long_v2 | smoking_status | 31 | 28 |
| canonical_extracted_fact_long_v2 | rai_date_administered | 30 | 28 |
| canonical_extracted_fact_long_v2 | osteoporosis | 30 | 22 |
| canonical_extracted_fact_long_v2 | calcitonin | 28 | 24 |
| canonical_extracted_fact_long_v2 | intraop_complication | 27 | 23 |
| canonical_extracted_fact_long_v2 | trousseau_sign | 27 | 27 |
| canonical_extracted_fact_long_v2 | hoarseness | 26 | 26 |
| canonical_extracted_fact_long_v2 | mri_neck | 26 | 24 |
| canonical_extracted_fact_long_v2 | diagnostic_i123_scan | 25 | 24 |
| canonical_extracted_fact_long_v2 | anti_tg_value | 25 | 14 |
| canonical_extracted_fact_long_v2 | tirads_composition | 24 | 23 |
| canonical_extracted_fact_long_v2 | rai_dose | 23 | 21 |
| canonical_extracted_fact_long_v2 | biochemical_persistence | 23 | 16 |
| canonical_extracted_fact_long_v2 | dyspnea | 23 | 20 |
| canonical_extracted_fact_long_v2 | tpo_antibody | 23 | 23 |
| canonical_extracted_fact_long_v2 | family_hx_cancer | 22 | 17 |
| canonical_extracted_fact_long_v2 | cumulative_rai_dose | 22 | 22 |
| canonical_extracted_fact_long_v2 | nodule_dimensions | 21 | 17 |
| canonical_extracted_fact_long_v2 | removal_intent | 21 | 20 |
| canonical_extracted_fact_long_v2 | biochemical_recurrence | 21 | 16 |
| canonical_extracted_fact_long_v2 | distant_recurrence | 21 | 17 |
| canonical_extracted_fact_long_v2 | gland_count_total | 21 | 21 |
| canonical_extracted_fact_long_v2 | rai_treatment | 20 | 20 |
| canonical_extracted_fact_long_v2 | weight_change | 20 | 20 |
| canonical_extracted_fact_long_v2 | ata_risk_category | 19 | 18 |
| canonical_extracted_fact_long_v2 | pulmonary_disease | 18 | 14 |
| canonical_extracted_fact_long_v2 | followup_gap | 18 | 18 |
| canonical_extracted_fact_long_v2 | gland_cellularity | 18 | 16 |
| canonical_extracted_fact_long_v2 | ebrt | 17 | 14 |
| canonical_extracted_fact_long_v2 | gross_invasion | 16 | 13 |
| canonical_extracted_fact_long_v2 | tracheal_involvement | 16 | 9 |
| canonical_extracted_fact_long_v2 | voice_handicap_index | 16 | 16 |
| canonical_extracted_fact_long_v2 | tirads_echogenicity | 15 | 15 |
| canonical_extracted_fact_long_v2 | coagulopathy | 15 | 14 |
| canonical_extracted_fact_long_v2 | cea | 14 | 12 |
| canonical_extracted_fact_long_v2 | nodule_identifier | 13 | 11 |
| canonical_extracted_fact_long_v2 | surveillance_interval | 13 | 11 |
| canonical_extracted_fact_long_v2 | trab | 13 | 13 |
| canonical_extracted_fact_long_v2 | neck_examination | 13 | 13 |
| canonical_extracted_fact_long_v2 | t3 | 13 | 11 |
| canonical_extracted_fact_long_v2 | physical_exam | 12 | 1 |
| canonical_extracted_fact_long_v2 | psychiatric | 12 | 11 |
| canonical_extracted_fact_long_v2 | gland_count_preserved | 12 | 12 |
| canonical_extracted_fact_long_v2 | tirads_shape | 12 | 12 |
| canonical_extracted_fact_long_v2 | autotransplant | 11 | 10 |
| canonical_extracted_fact_long_v2 | fatigue | 11 | 11 |
| canonical_extracted_fact_long_v2 | renal_disease | 11 | 11 |
| canonical_extracted_fact_long_v2 | voice | 11 | 11 |
| canonical_extracted_fact_long_v2 | revision_surgery | 11 | 11 |
| canonical_extracted_fact_long_v2 | tirads_margin | 11 | 11 |
| canonical_extracted_fact_long_v2 | chest_xray | 10 | 10 |
| canonical_extracted_fact_long_v2 | tirads_echogenic_foci | 10 | 10 |
| canonical_extracted_fact_long_v2 | tirads | 10 | 6 |
| canonical_extracted_fact_long_v2 | trachea | 9 | 9 |
| canonical_extracted_fact_long_v2 | procedure_performed | 9 | 4 |
| canonical_extracted_fact_long_v2 | paired_tsh | 9 | 6 |
| canonical_extracted_fact_long_v2 | surveillance_impression | 9 | 8 |
| canonical_extracted_fact_long_v2 | anxiety_tremor | 9 | 9 |
| canonical_extracted_fact_long_v2 | symptom_duration | 9 | 8 |
| canonical_extracted_fact_long_v2 | clinical_trial | 8 | 8 |
| canonical_extracted_fact_long_v2 | gland_weight | 8 | 6 |
| canonical_extracted_fact_long_v2 | procedures_performed | 8 | 3 |
| canonical_extracted_fact_long_v2 | tirads_component_composition | 8 | 6 |
| canonical_extracted_fact_long_v2 | diagnosis | 8 | 3 |
| canonical_extracted_fact_long_v2 | tirads_total_points | 8 | 8 |
| canonical_extracted_fact_long_v2 | creatinine | 7 | 7 |
| canonical_extracted_fact_long_v2 | thyroid | 7 | 6 |
| canonical_extracted_fact_long_v2 | surgical_procedure | 7 | 6 |
| canonical_extracted_fact_long_v2 | tumor_stage | 7 | 7 |
| canonical_extracted_fact_long_v2 | frozen_section_target | 7 | 6 |
| canonical_extracted_fact_long_v2 | disease_status | 7 | 7 |
| canonical_extracted_fact_long_v2 | voice_changes | 7 | 7 |
| canonical_extracted_fact_long_v2 | hypercellularity_grade | 7 | 7 |
| canonical_extracted_fact_long_v2 | rai_refractory | 7 | 7 |
| canonical_extracted_fact_long_v2 | neck | 6 | 6 |
| canonical_extracted_fact_long_v2 | heat_cold_intolerance | 6 | 6 |
| canonical_extracted_fact_long_v2 | ata_response_category | 6 | 6 |
| canonical_extracted_fact_long_v2 | lymph_node_involvement | 6 | 5 |
| canonical_extracted_fact_long_v2 | surgical_approach | 6 | 6 |
| canonical_extracted_fact_long_v2 | palpitations | 6 | 6 |
| canonical_extracted_fact_long_v2 | gland_size | 6 | 4 |
| canonical_extracted_fact_long_v2 | men_syndrome | 6 | 6 |
| canonical_extracted_fact_long_v2 | t4 | 6 | 4 |
| canonical_extracted_fact_long_v2 | liver_disease | 6 | 6 |
| canonical_extracted_fact_long_v2 | tirads_recommendation | 6 | 6 |
| canonical_extracted_fact_long_v2 | treatment_decision | 6 | 6 |
| canonical_extracted_fact_long_v2 | nodule_stability | 6 | 6 |
| canonical_extracted_fact_long_v2 | tracheal_compression | 6 | 4 |
| canonical_extracted_fact_long_v2 | thyroid_function | 6 | 6 |
| canonical_extracted_fact_long_v2 | alkaline_phosphatase | 5 | 5 |
| canonical_extracted_fact_long_v2 | second_opinion | 5 | 4 |
| canonical_extracted_fact_long_v2 | rai_administration | 5 | 5 |
| canonical_extracted_fact_long_v2 | lymphatic_invasion | 5 | 5 |
| canonical_extracted_fact_long_v2 | neurologic_exam | 5 | 5 |
| canonical_extracted_fact_long_v2 | decline_reason | 5 | 4 |
| canonical_extracted_fact_long_v2 | total_t3 | 5 | 5 |
| canonical_extracted_fact_long_v2 | postoperative_diagnosis | 5 | 5 |
| canonical_extracted_fact_long_v2 | pT_stage | 5 | 5 |
| canonical_extracted_fact_long_v2 | tumor_multifocality | 5 | 5 |
| canonical_extracted_fact_long_v2 | TSH | 5 | 3 |
| canonical_extracted_fact_long_v2 | pathologic_stage | 5 | 5 |
| canonical_extracted_fact_long_v2 | tki_dose_reduction | 5 | 5 |
| canonical_extracted_fact_long_v2 | ct_chest | 5 | 5 |
| canonical_extracted_fact_long_v2 | cause_of_death | 4 | 4 |
| canonical_extracted_fact_long_v2 | thyroid_examination | 4 | 4 |
| canonical_extracted_fact_long_v2 | intraop_decision_impact | 4 | 4 |
| canonical_extracted_fact_long_v2 | anesthesia | 4 | 4 |
| canonical_extracted_fact_long_v2 | Neck | 4 | 4 |
| canonical_extracted_fact_long_v2 | preoperative_diagnosis | 4 | 4 |
| canonical_extracted_fact_long_v2 | tirads_component_echogenicity | 4 | 3 |
| canonical_extracted_fact_long_v2 | odynophagia | 4 | 4 |
| canonical_extracted_fact_long_v2 | extranodal_extension | 4 | 4 |
| canonical_extracted_fact_long_v2 | etr_on_imaging | 4 | 3 |
| canonical_extracted_fact_long_v2 | nodule | 4 | 3 |
| canonical_extracted_fact_long_v2 | radiation_exposure_history | 4 | 4 |
| canonical_extracted_fact_long_v2 | thyroid_exam | 4 | 3 |
| canonical_extracted_fact_long_v2 | stimulated_thyroglobulin | 4 | 2 |
| canonical_extracted_fact_long_v2 | calcium_level | 4 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_nodule_size | 4 | 3 |
| canonical_extracted_fact_long_v2 | tg_context | 4 | 2 |
| canonical_extracted_fact_long_v2 | thyromegaly | 4 | 3 |
| canonical_extracted_fact_long_v2 | nodule_growth_rate | 4 | 4 |
| canonical_extracted_fact_long_v2 | entity_date | 4 | 4 |
| canonical_extracted_fact_long_v2 | lymphadenopathy | 4 | 4 |
| canonical_extracted_fact_long_v2 | thyroid_biopsy | 4 | 4 |
| canonical_extracted_fact_long_v2 | pN_stage | 4 | 4 |
| canonical_extracted_fact_long_v2 | trachea_exam | 3 | 3 |
| canonical_extracted_fact_long_v2 | low_iodine_diet | 3 | 3 |
| canonical_extracted_fact_long_v2 | neck_supple | 3 | 3 |
| canonical_extracted_fact_long_v2 | previous_fna | 3 | 3 |
| canonical_extracted_fact_long_v2 | benign_lymph_nodes | 3 | 3 |
| canonical_extracted_fact_long_v2 | margin_distance | 3 | 3 |
| canonical_extracted_fact_long_v2 | cardiovascular_exam | 3 | 3 |
| canonical_extracted_fact_long_v2 | lymph_node_metastasis | 3 | 3 |
| canonical_extracted_fact_long_v2 | fna | 3 | 3 |
| canonical_extracted_fact_long_v2 | potassium | 3 | 3 |
| canonical_extracted_fact_long_v2 | pathology | 3 | 3 |
| canonical_extracted_fact_long_v2 | tki_therapy | 3 | 2 |
| canonical_extracted_fact_long_v2 | neurological_exam | 3 | 2 |
| canonical_extracted_fact_long_v2 | lymph_node | 3 | 3 |
| canonical_extracted_fact_long_v2 | specimen | 3 | 3 |
| canonical_extracted_fact_long_v2 | specimen_type | 3 | 2 |
| canonical_extracted_fact_long_v2 | parathyroid_frozen_section | 3 | 3 |
| canonical_extracted_fact_long_v2 | crainial_nerve_exam | 3 | 3 |
| canonical_extracted_fact_long_v2 | imaging | 3 | 2 |
| canonical_extracted_fact_long_v2 | blood_pressure | 3 | 3 |
| canonical_extracted_fact_long_v2 | dysphonia_dysphagia_dyspnea | 3 | 3 |
| canonical_extracted_fact_long_v2 | thyroid_cancer | 3 | 3 |
| canonical_extracted_fact_long_v2 | abdominal_exam | 3 | 3 |
| canonical_extracted_fact_long_v2 | esophageal_involvement | 2 | 2 |
| canonical_extracted_fact_long_v2 | angioinvasion_count | 2 | 2 |
| canonical_extracted_fact_long_v2 | lymph_nodes | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_enlargement | 2 | 2 |
| canonical_extracted_fact_long_v2 | metastatic_disease | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_function_test | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_size | 2 | 1 |
| canonical_extracted_fact_long_v2 | phosphorus | 2 | 2 |
| canonical_extracted_fact_long_v2 | specimens | 2 | 1 |
| canonical_extracted_fact_long_v2 | voice_change | 2 | 2 |
| canonical_extracted_fact_long_v2 | ln_number_per_level | 2 | 2 |
| canonical_extracted_fact_long_v2 | ligation_of_vessels | 2 | 1 |
| canonical_extracted_fact_long_v2 | isthmus_thickness | 2 | 2 |
| canonical_extracted_fact_long_v2 | alt | 2 | 2 |
| canonical_extracted_fact_long_v2 | lungs_exam | 2 | 1 |
| canonical_extracted_fact_long_v2 | positive_lymph_nodes | 2 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_condition | 2 | 2 |
| canonical_extracted_fact_long_v2 | date | 2 | 2 |
| canonical_extracted_fact_long_v2 | us_visit_number | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_findings | 2 | 2 |
| canonical_extracted_fact_long_v2 | tki_toxicity | 2 | 2 |
| canonical_extracted_fact_long_v2 | hypocalcemia | 2 | 2 |
| canonical_extracted_fact_long_v2 | calcitonin_level | 2 | 2 |
| canonical_extracted_fact_long_v2 | isthmus_size | 2 | 1 |
| canonical_extracted_fact_long_v2 | chromogranin_a | 2 | 1 |
| canonical_extracted_fact_long_v2 | hypoparathyroidism | 2 | 2 |
| canonical_extracted_fact_long_v2 | t3_uptake | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_palp | 2 | 2 |
| canonical_extracted_fact_long_v2 | hyperlipidemia | 2 | 2 |
| canonical_extracted_fact_long_v2 | margin_location | 2 | 2 |
| canonical_extracted_fact_long_v2 | margins | 2 | 2 |
| canonical_extracted_fact_long_v2 | tg_assay_method | 2 | 2 |
| canonical_extracted_fact_long_v2 | anesthesia_history | 2 | 2 |
| canonical_extracted_fact_long_v2 | ast | 2 | 2 |
| canonical_extracted_fact_long_v2 | tirads_component_shape | 2 | 1 |
| canonical_extracted_fact_long_v2 | neuro_exam | 2 | 2 |
| canonical_extracted_fact_long_v2 | symptoms | 2 | 2 |
| canonical_extracted_fact_long_v2 | intraop_nerve_monitoring | 2 | 2 |
| canonical_extracted_fact_long_v2 | reimplantation_detail | 2 | 2 |
| canonical_extracted_fact_long_v2 | surgical_plan | 2 | 2 |
| canonical_extracted_fact_long_v2 | surgery | 2 | 2 |
| canonical_extracted_fact_long_v2 | respiratory_exam | 2 | 2 |
| canonical_extracted_fact_long_v2 | complications | 2 | 2 |
| canonical_extracted_fact_long_v2 | hemoglobin | 2 | 2 |
| canonical_extracted_fact_long_v2 | preoperative_medication | 2 | 1 |
| canonical_extracted_fact_long_v2 | tirads_vascularity | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroidectomy | 2 | 2 |
| canonical_extracted_fact_long_v2 | allergies | 2 | 2 |
| canonical_extracted_fact_long_v2 | operation | 1 | 1 |
| canonical_extracted_fact_long_v2 | Cervical lymph nodes | 1 | 1 |
| canonical_extracted_fact_long_v2 | GFR | 1 | 1 |
| canonical_extracted_fact_long_v2 | 24 hour urine calcium | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroglobulin_antibody | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_cancer_risk | 1 | 1 |
| canonical_extracted_fact_long_v2 | family_history | 1 | 1 |
| canonical_extracted_fact_long_v2 | previous_thyroid_nodules | 1 | 1 |
| canonical_extracted_fact_long_v2 | tobacco_use | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_surgery | 1 | 1 |
| canonical_extracted_fact_long_v2 | weight_loss | 1 | 1 |
| canonical_extracted_fact_long_v2 | patient_consent | 1 | 1 |
| canonical_extracted_fact_long_v2 | General | 1 | 1 |
| canonical_extracted_fact_long_v2 | flexible_laryngoscopy | 1 | 1 |
| canonical_extracted_fact_long_v2 | musculoskeletal_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | ablation_rx | 1 | 1 |
| canonical_extracted_fact_long_v2 | lymph_node_status | 1 | 1 |
| canonical_extracted_fact_long_v2 | benign_thyroid_background | 1 | 1 |
| canonical_extracted_fact_long_v2 | difficulty_swallowing | 1 | 1 |
| canonical_extracted_fact_long_v2 | wound_exam, | 1 | 1 |
| canonical_extracted_fact_long_v2 | neoplasm | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_notch_palpable | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical | 1 | 1 |
| canonical_extracted_fact_long_v2 | hgb | 1 | 1 |
| canonical_extracted_fact_long_v2 | Ca | 1 | 1 |
| canonical_extracted_fact_long_v2 | chromogranin | 1 | 1 |
| canonical_extracted_fact_long_v2 | PTH | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurologic_examination | 1 | 1 |
| canonical_extracted_fact_long_v2 | hypocalcemia_risk | 1 | 1 |
| canonical_extracted_fact_long_v2 | integumentary_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_subsequent_rai_treatment | 1 | 1 |
| canonical_extracted_fact_long_v2 | recurrence | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_mass | 1 | 1 |
| canonical_extracted_fact_long_v2 | SpO2 | 1 | 1 |
| canonical_extracted_fact_long_v2 | musculoskeletal | 1 | 1 |
| canonical_extracted_fact_long_v2 | aphonia | 1 | 1 |
| canonical_extracted_fact_long_v2 | respiratory_function | 1 | 1 |
| canonical_extracted_fact_long_v2 | stage | 1 | 1 |
| canonical_extracted_fact_long_v2 | lymph_node_ratio | 1 | 1 |
| canonical_extracted_fact_long_v2 | CV | 1 | 1 |
| canonical_extracted_fact_long_v2 | benign | 1 | 1 |
| canonical_extracted_fact_long_v2 | 24_hour_urine_metanephrine | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_dissection | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_cancer_diagnosis | 1 | 1 |
| canonical_extracted_fact_long_v2 | acanthosis_nigricans | 1 | 1 |
| canonical_extracted_fact_long_v2 | abdomen soft NTND | 1 | 1 |
| canonical_extracted_fact_long_v2 | ct | 1 | 1 |
| canonical_extracted_fact_long_v2 | Physical Exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | intraoperative_monitoring | 1 | 1 |
| canonical_extracted_fact_long_v2 | physical_exam_findings | 1 | 1 |
| canonical_extracted_fact_long_v2 | procedure_plan | 1 | 1 |
| canonical_extracted_fact_long_v2 | ct_neck_findings | 1 | 1 |
| canonical_extracted_fact_long_v2 | comorbidity | 1 | 1 |
| canonical_extracted_fact_long_v2 | pulmonary_chest | 1 | 1 |
| canonical_extracted_fact_long_v2 | anesthesia_plan | 1 | 1 |
| canonical_extracted_fact_long_v2 | inguinal_hernia | 1 | 1 |
| canonical_extracted_fact_long_v2 | findings | 1 | 1 |
| canonical_extracted_fact_long_v2 | w | 1 | 1 |
| canonical_extracted_fact_long_v2 | substerneal_extension | 1 | 1 |
| canonical_extracted_fact_long_v2 | Neurologic | 1 | 1 |
| canonical_extracted_fact_long_v2 | lymph_node_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_ultrasound | 1 | 1 |
| canonical_extracted_fact_long_v2 | airway_assessment | 1 | 1 |
| canonical_extracted_fact_long_v2 | treatment | 1 | 1 |
| canonical_extracted_fact_long_v2 | calc3 | 1 | 1 |
| canonical_extracted_fact_long_v2 | symptom | 1 | 1 |
| canonical_extracted_fact_long_v2 | benign_background | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroglossal duct cyst | 1 | 1 |
| canonical_extracted_fact_long_v2 | capsular_invasion_type | 1 | 1 |
| canonical_extracted_fact_long_v2 | skin_warm_dry | 1 | 1 |
| canonical_extracted_fact_long_v2 | chronic_kidney_disease | 1 | 1 |
| canonical_extracted_fact_long_v2 | tuberculosis,  | 1 | 1 |
| canonical_extracted_fact_long_v2 | indications_for_procedure | 1 | 1 |
| canonical_extracted_fact_long_v2 | BMI | 1 | 1 |
| canonical_extracted_fact_long_v2 | tumor_type | 1 | 1 |
| canonical_extracted_fact_long_v2 | Date of Service | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid_hormone | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroxine_free | 1 | 1 |
| canonical_extracted_fact_long_v2 | adenomatoid_nodules | 1 | 1 |
| canonical_extracted_fact_long_v2 | regional_lymph_nodes | 1 | 1 |
| canonical_extracted_fact_long_v2 | total_thyroidectomy | 1 | 1 |
| canonical_extracted_fact_long_v2 | isthmus_thickening | 1 | 1 |
| canonical_extracted_fact_long_v2 | ct_scan | 1 | 1 |
| canonical_extracted_fact_long_v2 | laryngeal_mass | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_evidence_of_malignancy | 1 | 1 |
| canonical_extracted_fact_long_v2 | skin | 1 | 1 |
| canonical_extracted_fact_long_v2 | general_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 |  at line 1, column 1, near  | 1 | 1 |
| canonical_extracted_fact_long_v2 | white_blood_count | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurologic | 1 | 1 |
| canonical_extracted_fact_long_v2 | eyes_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | voice_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | lymph node assessment | 1 | 1 |
| canonical_extracted_fact_long_v2 | lost_to_followup | 1 | 1 |
| canonical_extracted_fact_long_v2 | tumor_margin | 1 | 1 |
| canonical_extracted_fact_long_v2 | pulmonary_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | airway_obstruction | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_surgery_wanted | 1 | 1 |
| canonical_extracted_fact_long_v2 | no lower extremity edema | 1 | 1 |
| canonical_extracted_fact_long_v2 | skin_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_exam, thyroid_palpation, lymph_node_palpation, voice_assessment, airway_exam, wound_exam, scar_assessment, chvostek_sign, trousseau_sign, cranial_nerve_exam) {entity_type}: [findings], [findings], ... | 1 | 1 |
| canonical_extracted_fact_long_v2 | abdominal_examination | 1 | 1 |
| canonical_extracted_fact_long_v2 | pet_scanning | 1 | 1 |
| canonical_extracted_fact_long_v2 | nuclear_medicine_scan | 1 | 1 |
| canonical_extracted_fact_long_v2 | vocal_cord_assessment | 1 | 1 |
| canonical_extracted_fact_long_v2 | Vitals | 1 | 1 |
| canonical_extracted_fact_long_v2 | angioinvasion | 1 | 1 |
| canonical_extracted_fact_long_v2 | high_risk_features | 1 | 1 |
| canonical_extracted_fact_long_v2 | radioactive_iodine | 1 | 1 |
| canonical_extracted_fact_long_v2 | a1c | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyrogland | 1 | 1 |
| canonical_extracted_fact_long_v2 | Calcitonin | 1 | 1 |
| canonical_extracted_fact_long_v2 | covid_test | 1 | 1 |
| canonical_extracted_fact_long_v2 | }]}, but the assistant's response is cut off. Let me complete the JSON structure properly. Here's the corrected version of the JSON output based on the provided information and the specified format:```json{ | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgeon | 1 | 1 |
| canonical_extracted_fact_long_v2 | elongated_soft_tissue | 1 | 1 |
| canonical_extracted_fact_long_v2 | entity_type | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid_preservation | 1 | 1 |
| canonical_extracted_fact_long_v2 | focal_location | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_surgery | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_equipment | 1 | 1 |
| canonical_extracted_fact_long_v2 | discordance_reason | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_stimulating_hormone | 1 | 1 |
| canonical_extracted_fact_long_v2 | HR | 1 | 1 |
| canonical_extracted_fact_long_v2 | tracheaumatisation | 1 | 1 |
| canonical_extracted_fact_long_v2 | ionized calcium | 1 | 1 |
| canonical_extracted_fact_long_v2 | 2023-07-20 | 1 | 1 |
| canonical_extracted_fact_long_v2 | Supraclavicular lymph nodes | 1 | 1 |
| canonical_extracted_fact_long_v2 | TPAb | 1 | 1 |
| canonical_extracted_fact_long_v2 | free_t4_index | 1 | 1 |
| canonical_extracted_fact_long_v2 | sodium | 1 | 1 |
| canonical_extracted_fact_long_v2 | tg_detection_limit | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_nodules | 1 | 1 |
| canonical_extracted_fact_long_v2 | jvd | 1 | 1 |
| canonical_extracted_fact_long_v2 | denial_of_symptoms | 1 | 1 |
| canonical_extracted_fact_long_v2 | chronic_lymphocytic_thyroiditis | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_planning | 1 | 1 |
| canonical_extracted_fact_long_v2 | afirma | 1 | 1 |
| canonical_extracted_fact_long_v2 | tg_trend | 1 | 1 |
| canonical_extracted_fact_long_v2 | respiratory | 1 | 1 |
| canonical_extracted_fact_long_v2 | calcium_level_total | 1 | 1 |
| canonical_extracted_fact_long_v2 | genitourinary_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | molecular_test | 1 | 1 |
| canonical_extracted_fact_long_v2 | dissection_of_thyroid | 1 | 1 |
| canonical_extracted_fact_long_v2 | estimated_blood_loss | 1 | 1 |
| canonical_extracted_fact_long_v2 | globus_sensation | 1 | 1 |
| canonical_extracted_fact_long_v2 | incision_status | 1 | 1 |
| canonical_extracted_fact_long_v2 | risk_assessment | 1 | 1 |
| canonical_extracted_fact_long_v2 | Note Received | 1 | 1 |
| canonical_extracted_fact_long_v2 | incision | 1 | 1 |
| canonical_extracted_fact_long_v2 | musculoskeletal_normal | 1 | 1 |
| canonical_extracted_fact_long_v2 | Pulmonary | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_cyst | 1 | 1 |
| canonical_extracted_fact_long_v2 | plan_surgery | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_nodule | 1 | 1 |
| canonical_extracted_fact_long_v2 | albumin | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroglossal_duct_cyst | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurological_examination | 1 | 1 |
| canonical_extracted_fact_long_v2 | tracheal_position | 1 | 1 |
| canonical_extracted_fact_long_v2 | HEENT | 1 | 1 |
| canonical_extracted_fact_long_v2 | trachea_deviation | 1 | 1 |
| canonical_extracted_fact_long_v2 | frozen_section_turnaround | 1 | 1 |
| canonical_extracted_fact_long_v2 | tsh_level | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_exam: stridor, tracheal deviation, airway compromise findings | 1 | 1 |
| canonical_extracted_fact_long_v2 | chvostek sign | 1 | 1 |
| canonical_extracted_fact_long_v2 | hypothyroidism | 1 | 1 |
| canonical_extracted_fact_long_v2 | no thyroid bed masses or thyroid tissue | 1 | 1 |
| canonical_extracted_fact_long_v2 | bilirubin | 1 | 1 |
| canonical_extracted_fact_long_v2 | anesthesia_agent | 1 | 1 |
| canonical_extracted_fact_long_v2 | vitamin_d_deficiency | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid_adenoma | 1 | 1 |
| canonical_extracted_fact_long_v2 | molecular_marker | 1 | 1 |
| canonical_extracted_fact_long_v2 | Extremities | 1 | 1 |
| canonical_extracted_fact_long_v2 | platelet_count | 1 | 1 |
| canonical_extracted_fact_long_v2 | carbon_dioxide | 1 | 1 |
| canonical_extracted_fact_long_v2 | present_or_negated | 1 | 1 |
| canonical_extracted_fact_long_v2 | tsh_goal | 1 | 1 |
| canonical_extracted_fact_long_v2 | Skin | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurological | 1 | 1 |
| canonical_extracted_fact_long_v2 | ectopic_parathyroid | 1 | 1 |
| canonical_extracted_fact_long_v2 | whole_body_scan | 1 | 1 |
| canonical_extracted_fact_long_v2 | Thyroid | 1 | 1 |
| canonical_extracted_fact_long_v2 | adenomatoid_nodule | 1 | 1 |
| canonical_extracted_fact_long_v2 | swallowing_difficulty | 1 | 1 |
| canonical_extracted_fact_long_v2 | trophostek sign | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_radiation_exposure | 1 | 1 |
| canonical_extracted_fact_long_v2 | ln_laterality | 1 | 1 |
| canonical_extracted_fact_long_v2 | fna_of_ln | 1 | 1 |
| canonical_extracted_fact_long_v2 | no cervical or supraclavicular lymphadenopathy | 1 | 1 |
| canonical_extracted_fact_long_v2 | benign_lesion | 1 | 1 |
| canonical_extracted_fact_long_v2 | phosphate | 1 | 1 |
| canonical_extracted_fact_long_v2 | coronary_artery_disease | 1 | 1 |
| canonical_extracted_fact_long_v2 | atrial_fibrillation | 1 | 1 |
| canonical_extracted_fact_long_v2 | diabetes_mellitus | 1 | 1 |
| canonical_extracted_fact_long_v2 | airway_clear | 1 | 1 |
| canonical_extracted_fact_long_v2 | subglottic_stenosis | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_thyroid_cancer_family_history | 1 | 1 |
| canonical_extracted_fact_long_v2 | chest clear to auscultation | 1 | 1 |
| canonical_extracted_fact_long_v2 | cricoid_not_palpable | 1 | 1 |
| canonical_extracted_fact_long_v2 | abdominal | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_cervical_lymphadenopathy | 1 | 1 |
| canonical_extracted_fact_long_v2 | ln_morphology | 1 | 1 |
| canonical_extracted_fact_long_v2 | assistant_surgeon | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_procedure | 1 | 1 |
| canonical_extracted_fact_long_v2 | lab_abnormality | 1 | 1 |
| canonical_extracted_fact_long_v2 | JVD | 1 | 1 |
| canonical_extracted_fact_long_v2 | postop PTH | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_masses_abdomen | 1 | 1 |
| canonical_extracted_fact_long_v2 | no_thyroid_enlargement | 1 | 1 |
| canonical_extracted_fact_long_v2 | hemoglobin_a1c | 1 | 1 |
| canonical_extracted_fact_long_v2 | tremors | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroidectomy_plan | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_incision | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid | 1 | 1 |
| canonical_extracted_fact_long_v2 | BP | 1 | 1 |
| canonical_extracted_fact_long_v2 | abdomen_soft | 1 | 1 |
| canonical_extracted_fact_long_v2 | allergy | 1 | 1 |
| canonical_extracted_fact_long_v2 | chloride | 1 | 1 |
| canonical_extracted_fact_long_v2 | tsh_receptor_antibody | 1 | 1 |
| canonical_extracted_fact_long_v2 | bethesda_category | 1 | 1 |
| canonical_extracted_fact_long_v2 | cancer_stage | 1 | 1 |
| canonical_extracted_fact_long_v2 | free_thyroxine_index | 1 | 1 |
| canonical_extracted_fact_long_v2 | heart_failure | 1 | 1 |
| canonical_extracted_fact_long_v2 | tirads_component_margin | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_nodularity | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_size_left_lobe | 1 | 1 |
| canonical_extracted_fact_long_v2 | plan_ct_neck | 1 | 1 |
| canonical_extracted_fact_long_v2 | closure | 1 | 1 |
| canonical_extracted_fact_long_v2 | cervical_back_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | heart_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | lymph-25, 2023-07-25. 2023-07-25 is the date of the procedure. The entity_date is 2023-07-25. The date_confidence is 1.0 because it's the procedure date. The date_source_keyword is  | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_soreness | 1 | 1 |
| canonical_extracted_fact_long_v2 | metastasis | 1 | 1 |
| canonical_extracted_fact_long_v2 | nerve_integrity_monitor | 1 | 1 |
| canonical_extracted_fact_long_v2 | follow_up | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurological_monitoring | 1 | 1 |
| canonical_extracted_fact_long_v2 | implant_site | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_imaging | 1 | 1 |
| canonical_extracted_fact_long_v2 | imaging_findings | 1 | 1 |
| canonical_extracted_fact_long_v2 | triiodothyronine | 1 | 1 |
| canonical_extracted_fact_long_v2 | laboratory | 1 | 1 |
| canonical_extracted_fact_long_v2 | chest_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_nodule_palpable | 1 | 1 |
| canonical_fact_quarantine_v2 | ete_on_imaging | 12 | 12 |
| canonical_fact_quarantine_v2 | vocal_cord_mobility | 12 | 12 |
| canonical_fact_quarantine_v2 | swallowing_function_detail | 11 | 10 |
| canonical_fact_quarantine_v2 | fna_cytology | 9 | 7 |
| canonical_fact_quarantine_v2 | disease_free | 8 | 6 |
| canonical_fact_quarantine_v2 | nodule_size | 8 | 6 |
| canonical_fact_quarantine_v2 | tracheal_deviation | 8 | 8 |
| canonical_fact_quarantine_v2 | ultrasound_thyroid | 7 | 6 |
| canonical_fact_quarantine_v2 | structural_recurrence | 7 | 6 |
| canonical_fact_quarantine_v2 | substernal_extension | 7 | 7 |
| canonical_fact_quarantine_v2 | tg_value | 6 | 4 |
| canonical_fact_quarantine_v2 | voice_recovery | 6 | 6 |
| canonical_fact_quarantine_v2 | mass_effect | 5 | 5 |
| canonical_fact_quarantine_v2 | tumor_size | 5 | 3 |
| canonical_fact_quarantine_v2 | frozen_section_result | 5 | 5 |
| canonical_fact_quarantine_v2 | tirads_score | 4 | 3 |
| canonical_fact_quarantine_v2 | nodule_location | 4 | 3 |
| canonical_fact_quarantine_v2 | surgical_pathology | 4 | 3 |
| canonical_fact_quarantine_v2 | bethesda_class | 3 | 3 |
| canonical_fact_quarantine_v2 | lymphovascular_invasion | 3 | 2 |
| canonical_fact_quarantine_v2 | neck_exam | 3 | 3 |
| canonical_fact_quarantine_v2 | tsh | 3 | 2 |
| canonical_fact_quarantine_v2 | biochemical_persistence | 2 | 2 |
| canonical_fact_quarantine_v2 | nodule_volume | 2 | 2 |
| canonical_fact_quarantine_v2 | margin_status | 2 | 2 |
| canonical_fact_quarantine_v2 | rai_dose_mci | 2 | 2 |
| canonical_fact_quarantine_v2 | laryngeal_invasion | 2 | 2 |
| canonical_fact_quarantine_v2 | ultrasound_lymph_node | 2 | 2 |
| canonical_fact_quarantine_v2 | vascular_encasement | 2 | 2 |
| canonical_fact_quarantine_v2 | dysphagia | 2 | 2 |
| canonical_fact_quarantine_v2 | free_t3 | 2 | 2 |
| canonical_fact_quarantine_v2 | dysphonia | 2 | 2 |
| canonical_fact_quarantine_v2 | biochemical_recurrence | 2 | 2 |
| canonical_fact_quarantine_v2 | distant_recurrence | 2 | 2 |
| canonical_fact_quarantine_v2 | calc3 | 1 | 1 |
| canonical_fact_quarantine_v2 | etr_on_imaging | 1 | 1 |
| canonical_fact_quarantine_v2 | benign_pathology | 1 | 1 |
| canonical_fact_quarantine_v2 | tracheal_narrowing | 1 | 1 |
| canonical_fact_quarantine_v2 | free_t4 | 1 | 1 |
| canonical_fact_quarantine_v2 | gland_location | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroglobulin | 1 | 1 |
| canonical_fact_quarantine_v2 | surveillance_impression | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroglobulin_stimulated | 1 | 1 |
| canonical_fact_quarantine_v2 | anti_thyroglobulin | 1 | 1 |
| canonical_fact_quarantine_v2 | gland_count_preserved | 1 | 1 |
| canonical_fact_quarantine_v2 | prior_thyroidectomy | 1 | 1 |
| canonical_fact_quarantine_v2 | surveillance_interval | 1 | 1 |
| canonical_fact_quarantine_v2 | nodule_identifier | 1 | 1 |
| canonical_fact_quarantine_v2 | airway_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | total_t4 | 1 | 1 |
| canonical_fact_quarantine_v2 | parathyroid_frozen_section | 1 | 1 |
| canonical_fact_quarantine_v2 | vital_status | 1 | 1 |
| canonical_fact_quarantine_v2 | gland_weight | 1 | 1 |
| canonical_fact_quarantine_v2 | gland_count_total | 1 | 1 |
| canonical_fact_quarantine_v2 | dyspnea | 1 | 1 |
| canonical_fact_quarantine_v2 | vocal_cord_imaging | 1 | 1 |
| canonical_fact_quarantine_v2 | tirads_composition | 1 | 1 |
| canonical_fact_quarantine_v2 | calcium | 1 | 1 |
| canonical_fact_quarantine_v2 | treatment_episode_number | 1 | 1 |
| canonical_fact_quarantine_v2 | gland_cellularity | 1 | 1 |
| canonical_fact_quarantine_v2 | discharge_disposition | 1 | 1 |
| canonical_fact_quarantine_v2 | airway_compromise_grade | 1 | 1 |
| canonical_fact_quarantine_v2 | chvostek_sign | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_nodule_size | 1 | 1 |
| canonical_fact_quarantine_v2 | return_to_work | 1 | 1 |
| canonical_fact_quarantine_v2 | removal_intent | 1 | 1 |
| canonical_fact_quarantine_v2 | shared_decision | 1 | 1 |
| canonical_fact_quarantine_v2 | nodule_dimensions | 1 | 1 |
| canonical_fact_quarantine_v2 | rai_preparation | 1 | 1 |
