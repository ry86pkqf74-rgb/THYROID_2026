# MotherDuck Formalization Release Validation Report

**Generated:** 2026-04-08T05:21:40.268243+00:00
**Mode:** Release Validation
**Total checks:** 39
**Passed:** 36  |  **Warned:** 3  |  **Failed:** 0

**VERDICT: PASS WITH WARNINGS** — 3 check(s) warned; no failures.

---

## Check Results

| Check | Status | Detail |
|-------|--------|--------|
| MD attachment | PASS | 11 databases attached |
| Row count parity | PASS | 23 domains checked, all match |
| Canonical canonical_extracted_fact_long_v2 | PASS | local=20,188  md=20,188 |
| Canonical canonical_fact_quarantine_v2 | PASS | local=103,588  md=103,588 |
| Canonical note_extraction_runs | PASS | local=3  md=3 |
| Schema completeness | PASS | Wide note-level v2 contract on 23 promoted table(s); entity_type/entity_value_* in main.canonical_extracted_fact_long_v2 (see docs/domain_mapping_rules.md). Example stems: note_entities_llm_imaging, note_entities_llm_tirads_granular, note_entities_llm_us_nodule_dynamics… |
| Canonical dist (canonical_extracted_fact_long_v2) | PASS | 237 domains, 20,188 total rows |
| Canonical dist (canonical_fact_quarantine_v2) | PASS | 549 domains, 103,588 total rows |
| Review queue | PASS | 5,622 total, 5,622 reviewed, 0 pending |
| Review queue (synthetic placeholder) | PASS | no synthetic-placeholder verification_status in qa.manual_review_queue |
| Promotion decision provenance | PASS | 4 row(s); all have non-empty decision_batch_id |
| QA view promotion_scorecard_summary_v | PASS | 7 rows |
| QA view domain_validation_summary_v | PASS | 7 rows |
| QA view date_provenance_completeness_v | PASS | 23 rows |
| QA view manual_review_queue_summary_v | PASS | 21 rows |
| Load inventory | PASS | 210 entries, all match |
| Release schemas | PASS | 11 found: release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260407_tier, release_20260408, release_20260408r2, release_20260408r3, release_20260409, release_20260410, release_20260411 |
| Release manifest | PASS | 11 release(s); latest: 20260411 (2026-04-07 19:15:39.106720) |
| Canonical extraction_run_id | PASS | 20,188 rows; 0 blank (contract §3) |
| Presentation master_fact_long_verified_v1 | PASS | 20,188 rows; core traceability non-null (reviewer_status may be NULL) |
| Presentation master_source_lineage_v1 | PASS | 20,188 rows; core traceability non-null (reviewer_status may be NULL) |
| Presentation master_patient_rollup_verified_v1 | PASS | 2,702 patient rows; research_id + release_tag + review metrics present |
| Molecular contract required columns | PASS | all 4 views expose required fields |
| Molecular results contract row parity | PASS | 10,862 live rows in contract view |
| Molecular result_id uniqueness | PASS | 10,862 rows; distinct molecular_result_id |
| Molecular contract non-empty | PASS | 10,862 contract rows |
| Molecular payload_checksum uniqueness | PASS | 10,862 non-null checksums; all distinct |
| Molecular provenance (results) | PASS | 10,862 rows; lineage_id + ingestion_ts present |
| Molecular allele_fraction bounds | PASS | 1,640 variant rows; AF in [0,1] or NULL |
| Molecular variant_class enum | PASS | all variant_class values in {SNV,INDEL,FUSION,CNV,OTHER} |
| Molecular assay/panel_version pairing | WARN | 10,597 rows with assay_name but empty panel_version |
| Molecular assay_name dictionary match | WARN | 1 distinct assay_name value(s) not in molecular_assay_dictionary (expected for non-afirma panels such as ThyroSeq) |
| Molecular episode upstream spine | PASS | main.molecular_testing present alongside molecular_test_episode_v2 |
| Specimen/FHIR tables present | PASS | 10 objects found |
| Specimen master fingerprint uniqueness | PASS | distinct fingerprints |
| qa.val_specimen_contract_v1 | PASS | no FAIL rows recorded |
| qa.val_specimen_genomic_binding_v1 | PASS | no FAIL rows recorded |
| Specimen/FHIR QA diagnostics (142 views + focus checks) | PASS | clean |
| Specimen-adjacent review burden (open/pending) | WARN | genomic_link_review open/pending=10705; specimen_merge_review open/pending=1 |

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
| canonical_extracted_fact_long_v2 | procedure | 5,036 | 1,791 |
| canonical_extracted_fact_long_v2 | medication | 1,516 | 540 |
| canonical_extracted_fact_long_v2 | nerve_monitoring | 1,341 | 772 |
| canonical_extracted_fact_long_v2 | problem | 1,328 | 740 |
| canonical_extracted_fact_long_v2 | last_followup_date | 679 | 630 |
| canonical_extracted_fact_long_v2 | prior_thyroidectomy | 678 | 599 |
| canonical_extracted_fact_long_v2 | vital_status | 608 | 576 |
| canonical_extracted_fact_long_v2 | rln_finding | 471 | 309 |
| canonical_extracted_fact_long_v2 | voice_quality | 464 | 436 |
| canonical_extracted_fact_long_v2 | complication | 418 | 167 |
| canonical_extracted_fact_long_v2 | ebl | 361 | 350 |
| canonical_extracted_fact_long_v2 | swallowing_function | 331 | 327 |
| canonical_extracted_fact_long_v2 | prior_fna | 319 | 289 |
| canonical_extracted_fact_long_v2 | tsh | 292 | 258 |
| canonical_extracted_fact_long_v2 | treatment_episode_number | 275 | 255 |
| canonical_extracted_fact_long_v2 | vocal_cord_mobility | 254 | 235 |
| canonical_extracted_fact_long_v2 | rai_dose_mci | 250 | 233 |
| canonical_extracted_fact_long_v2 | ete_on_imaging | 231 | 224 |
| canonical_extracted_fact_long_v2 | berry_ligament | 224 | 153 |
| canonical_extracted_fact_long_v2 | post_treatment_wbs_findings | 202 | 195 |
| canonical_extracted_fact_long_v2 | parathyroid_management | 192 | 139 |
| canonical_extracted_fact_long_v2 | rai_indication | 191 | 184 |
| canonical_extracted_fact_long_v2 | prior_rai | 187 | 177 |
| canonical_extracted_fact_long_v2 | strap_muscle | 162 | 137 |
| canonical_extracted_fact_long_v2 | discharge_disposition | 142 | 137 |
| canonical_extracted_fact_long_v2 | rai_ablation | 132 | 127 |
| canonical_extracted_fact_long_v2 | voice_recovery | 130 | 124 |
| canonical_extracted_fact_long_v2 | prior_cancer | 130 | 111 |
| canonical_extracted_fact_long_v2 | swallowing_function_detail | 127 | 119 |
| canonical_extracted_fact_long_v2 | tracheal_deviation | 124 | 121 |
| canonical_extracted_fact_long_v2 | other_surgery | 122 | 80 |
| canonical_extracted_fact_long_v2 | calcium_quality_of_life | 119 | 116 |
| canonical_extracted_fact_long_v2 | preparation_method | 117 | 112 |
| canonical_extracted_fact_long_v2 | free_t4 | 115 | 102 |
| canonical_extracted_fact_long_v2 | neck_exam | 114 | 108 |
| canonical_extracted_fact_long_v2 | drain_placement | 101 | 74 |
| canonical_extracted_fact_long_v2 | frozen_section_result | 95 | 93 |
| canonical_extracted_fact_long_v2 | mass_effect | 88 | 86 |
| canonical_extracted_fact_long_v2 | calcium | 88 | 76 |
| canonical_extracted_fact_long_v2 | gland_location | 86 | 62 |
| canonical_extracted_fact_long_v2 | prior_neck_dissection | 84 | 72 |
| canonical_extracted_fact_long_v2 | substernal_extension | 84 | 80 |
| canonical_extracted_fact_long_v2 | thyroid_palpation | 77 | 73 |
| canonical_extracted_fact_long_v2 | pre_rai_tg | 71 | 70 |
| canonical_extracted_fact_long_v2 | free_t3 | 67 | 62 |
| canonical_extracted_fact_long_v2 | post_treatment_scan | 66 | 60 |
| canonical_extracted_fact_long_v2 | vitamin_d | 64 | 61 |
| canonical_extracted_fact_long_v2 | specimen_detail | 61 | 55 |
| canonical_extracted_fact_long_v2 | thyroglobulin | 59 | 41 |
| canonical_extracted_fact_long_v2 | UNKNOWN | 57 | 9 |
| canonical_extracted_fact_long_v2 | vascular_encasement | 57 | 56 |
| canonical_extracted_fact_long_v2 | pth | 53 | 49 |
| canonical_extracted_fact_long_v2 | treatment_declined | 52 | 51 |
| canonical_extracted_fact_long_v2 | pre_rai_tsh | 49 | 48 |
| canonical_extracted_fact_long_v2 | lymph_node_palpation | 47 | 46 |
| canonical_extracted_fact_long_v2 | airway_exam | 42 | 39 |
| canonical_extracted_fact_long_v2 | shared_decision | 40 | 38 |
| canonical_extracted_fact_long_v2 | wound_exam | 39 | 29 |
| canonical_extracted_fact_long_v2 | laryngoscopy_date | 38 | 35 |
| canonical_extracted_fact_long_v2 | parathyroid_autograft | 38 | 32 |
| canonical_extracted_fact_long_v2 | prior_neck_surgery | 37 | 32 |
| canonical_extracted_fact_long_v2 | prior_parathyroidectomy | 35 | 33 |
| canonical_extracted_fact_long_v2 | follow_up_duration | 32 | 32 |
| canonical_extracted_fact_long_v2 | anti_thyroglobulin | 32 | 26 |
| canonical_extracted_fact_long_v2 | autoimmune_thyroid | 30 | 29 |
| canonical_extracted_fact_long_v2 | structural_recurrence | 29 | 18 |
| canonical_extracted_fact_long_v2 | laryngoscopy_findings | 29 | 27 |
| canonical_extracted_fact_long_v2 | calcium_symptom_chronicity | 28 | 26 |
| canonical_extracted_fact_long_v2 | laryngeal_invasion | 27 | 25 |
| canonical_extracted_fact_long_v2 | reoperative_field | 27 | 22 |
| canonical_extracted_fact_long_v2 | return_to_work | 27 | 26 |
| canonical_extracted_fact_long_v2 | scar_satisfaction | 26 | 25 |
| canonical_extracted_fact_long_v2 | final_pathology_concordance | 26 | 26 |
| canonical_extracted_fact_long_v2 | total_t4 | 26 | 26 |
| canonical_extracted_fact_long_v2 | rai_preparation | 24 | 24 |
| canonical_extracted_fact_long_v2 | tsi | 22 | 22 |
| canonical_extracted_fact_long_v2 | vocal_cord_imaging | 21 | 21 |
| canonical_extracted_fact_long_v2 | thyroglobulin_stimulated | 21 | 18 |
| canonical_extracted_fact_long_v2 | patient_preference | 20 | 20 |
| canonical_extracted_fact_long_v2 | radiation_exposure | 20 | 20 |
| canonical_extracted_fact_long_v2 | tracheal_narrowing | 19 | 19 |
| canonical_extracted_fact_long_v2 | rai_dose | 19 | 19 |
| canonical_extracted_fact_long_v2 | surveillance_adherence | 18 | 15 |
| canonical_extracted_fact_long_v2 | thyroid_hormone_suppression | 18 | 15 |
| canonical_extracted_fact_long_v2 | neck_mass | 18 | 15 |
| canonical_extracted_fact_long_v2 | tg_value | 17 | 13 |
| canonical_extracted_fact_long_v2 | rai_date_administered | 17 | 17 |
| canonical_extracted_fact_long_v2 | voice_assessment | 16 | 16 |
| canonical_extracted_fact_long_v2 | cumulative_rai_dose | 14 | 14 |
| canonical_extracted_fact_long_v2 | gland_cellularity | 13 | 12 |
| canonical_extracted_fact_long_v2 | incidental_finding | 13 | 12 |
| canonical_extracted_fact_long_v2 | tpo_antibody | 13 | 13 |
| canonical_extracted_fact_long_v2 | gland_count_total | 12 | 12 |
| canonical_extracted_fact_long_v2 | removal_intent | 12 | 12 |
| canonical_extracted_fact_long_v2 | airway_compromise_grade | 12 | 12 |
| canonical_extracted_fact_long_v2 | hypertension | 12 | 12 |
| canonical_extracted_fact_long_v2 | obesity | 11 | 11 |
| canonical_extracted_fact_long_v2 | injection_laryngoplasty | 11 | 11 |
| canonical_extracted_fact_long_v2 | side_effects | 10 | 10 |
| canonical_extracted_fact_long_v2 | ata_risk_category | 10 | 9 |
| canonical_extracted_fact_long_v2 | prior_core_biopsy | 10 | 10 |
| canonical_extracted_fact_long_v2 | t3 | 10 | 8 |
| canonical_extracted_fact_long_v2 | trab | 9 | 9 |
| canonical_extracted_fact_long_v2 | calcitonin | 9 | 9 |
| canonical_extracted_fact_long_v2 | esophageal_compression | 9 | 9 |
| canonical_extracted_fact_long_v2 | scar_assessment | 9 | 9 |
| canonical_extracted_fact_long_v2 | cranial_nerve_exam | 9 | 9 |
| canonical_extracted_fact_long_v2 | intraop_complication | 9 | 8 |
| canonical_extracted_fact_long_v2 | quality_of_life_score | 9 | 9 |
| canonical_extracted_fact_long_v2 | rai_treatment | 8 | 8 |
| canonical_extracted_fact_long_v2 | gland_count_preserved | 8 | 8 |
| canonical_extracted_fact_long_v2 | dysphagia | 7 | 7 |
| canonical_extracted_fact_long_v2 | family_hx_thyroid | 7 | 7 |
| canonical_extracted_fact_long_v2 | anti_tg_value | 7 | 5 |
| canonical_extracted_fact_long_v2 | cardiovascular | 7 | 6 |
| canonical_extracted_fact_long_v2 | biochemical_persistence | 7 | 5 |
| canonical_extracted_fact_long_v2 | cea | 6 | 6 |
| canonical_extracted_fact_long_v2 | speech_therapy_referral | 6 | 5 |
| canonical_extracted_fact_long_v2 | osteoporosis | 6 | 5 |
| canonical_extracted_fact_long_v2 | disease_free | 6 | 6 |
| canonical_extracted_fact_long_v2 | diabetes | 6 | 6 |
| canonical_extracted_fact_long_v2 | stunning_concern | 6 | 6 |
| canonical_extracted_fact_long_v2 | t4 | 5 | 3 |
| canonical_extracted_fact_long_v2 | thyroid_nodule | 5 | 4 |
| canonical_extracted_fact_long_v2 | xerostomia | 5 | 5 |
| canonical_extracted_fact_long_v2 | family_hx_cancer | 5 | 5 |
| canonical_extracted_fact_long_v2 | pulmonary_disease | 5 | 4 |
| canonical_extracted_fact_long_v2 | gland_weight | 5 | 3 |
| canonical_extracted_fact_long_v2 | alkaline_phosphatase | 4 | 4 |
| canonical_extracted_fact_long_v2 | dysphonia | 4 | 4 |
| canonical_extracted_fact_long_v2 | diagnostic_i123_scan | 4 | 4 |
| canonical_extracted_fact_long_v2 | anesthesia | 4 | 4 |
| canonical_extracted_fact_long_v2 | autotransplant | 4 | 4 |
| canonical_extracted_fact_long_v2 | chvostek_sign | 4 | 4 |
| canonical_extracted_fact_long_v2 | hypercellularity_grade | 4 | 4 |
| canonical_extracted_fact_long_v2 | hoarseness | 4 | 4 |
| canonical_extracted_fact_long_v2 | smoking_status | 4 | 3 |
| canonical_extracted_fact_long_v2 | revision_surgery | 3 | 3 |
| canonical_extracted_fact_long_v2 | treatment_decision | 3 | 3 |
| canonical_extracted_fact_long_v2 | potassium | 3 | 3 |
| canonical_extracted_fact_long_v2 | parathyroid_frozen_section | 3 | 3 |
| canonical_extracted_fact_long_v2 | voice_handicap_index | 3 | 3 |
| canonical_extracted_fact_long_v2 | psychiatric | 3 | 3 |
| canonical_extracted_fact_long_v2 | creatinine | 3 | 3 |
| canonical_extracted_fact_long_v2 | clinical_trial | 3 | 3 |
| canonical_extracted_fact_long_v2 | biochemical_recurrence | 3 | 2 |
| canonical_extracted_fact_long_v2 | distant_recurrence | 3 | 3 |
| canonical_extracted_fact_long_v2 | etr_on_imaging | 3 | 2 |
| canonical_extracted_fact_long_v2 | ebrt | 3 | 3 |
| canonical_extracted_fact_long_v2 | preoperative_medication | 2 | 1 |
| canonical_extracted_fact_long_v2 | symptom_duration | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyroid_function | 2 | 2 |
| canonical_extracted_fact_long_v2 | heat_cold_intolerance | 2 | 2 |
| canonical_extracted_fact_long_v2 | followup_gap | 2 | 2 |
| canonical_extracted_fact_long_v2 | vascular_invasion | 2 | 2 |
| canonical_extracted_fact_long_v2 | lymphovascular_invasion | 2 | 2 |
| canonical_extracted_fact_long_v2 | paired_tsh | 2 | 1 |
| canonical_extracted_fact_long_v2 | total_t3 | 2 | 2 |
| canonical_extracted_fact_long_v2 | hemoglobin | 2 | 2 |
| canonical_extracted_fact_long_v2 | disease_status | 2 | 2 |
| canonical_extracted_fact_long_v2 | preoperative_diagnosis | 2 | 2 |
| canonical_extracted_fact_long_v2 | ast | 2 | 2 |
| canonical_extracted_fact_long_v2 | t3_uptake | 2 | 2 |
| canonical_extracted_fact_long_v2 | surveillance_interval | 2 | 2 |
| canonical_extracted_fact_long_v2 | postoperative_diagnosis | 2 | 2 |
| canonical_extracted_fact_long_v2 | thyrogen_stimulation | 2 | 2 |
| canonical_extracted_fact_long_v2 | surgical_procedure | 2 | 2 |
| canonical_extracted_fact_long_v2 | alt | 2 | 2 |
| canonical_extracted_fact_long_v2 | hypoparathyroidism | 1 | 1 |
| canonical_extracted_fact_long_v2 | triiodothyronine | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid_preservation | 1 | 1 |
| canonical_extracted_fact_long_v2 | weight_change | 1 | 1 |
| canonical_extracted_fact_long_v2 | fatigue | 1 | 1 |
| canonical_extracted_fact_long_v2 | anxiety_tremor | 1 | 1 |
| canonical_extracted_fact_long_v2 | albumin | 1 | 1 |
| canonical_extracted_fact_long_v2 | low_iodine_diet | 1 | 1 |
| canonical_extracted_fact_long_v2 | parathyroid_hormone | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroxine_free | 1 | 1 |
| canonical_extracted_fact_long_v2 | decline_reason | 1 | 1 |
| canonical_extracted_fact_long_v2 | trousseau_sign | 1 | 1 |
| canonical_extracted_fact_long_v2 | cardiovascular_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | covid_test | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_surgery | 1 | 1 |
| canonical_extracted_fact_long_v2 | present_or_negated | 1 | 1 |
| canonical_extracted_fact_long_v2 | anesthesia_agent | 1 | 1 |
| canonical_extracted_fact_long_v2 | bilirubin | 1 | 1 |
| canonical_extracted_fact_long_v2 | chromogranin_a | 1 | 1 |
| canonical_extracted_fact_long_v2 | platelet_count | 1 | 1 |
| canonical_extracted_fact_long_v2 | carbon_dioxide | 1 | 1 |
| canonical_extracted_fact_long_v2 | neck_exam, thyroid_palpation, lymph_node_palpation, voice_assessment, airway_exam, wound_exam, scar_assessment, chvostek_sign, trousseau_sign, cranial_nerve_exam) {entity_type}: [findings], [findings], ... | 1 | 1 |
| canonical_extracted_fact_long_v2 | pulmonary_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgeon | 1 | 1 |
| canonical_extracted_fact_long_v2 | isolation_days | 1 | 1 |
| canonical_extracted_fact_long_v2 | reimplantation_detail | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_incision | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroglobulin_antibody | 1 | 1 |
| canonical_extracted_fact_long_v2 | extrathyroidal_extension | 1 | 1 |
| canonical_extracted_fact_long_v2 | integumentary_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_surgery | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_cancer_risk | 1 | 1 |
| canonical_extracted_fact_long_v2 | surveillance_impression | 1 | 1 |
| canonical_extracted_fact_long_v2 | cause_of_death | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_approach | 1 | 1 |
| canonical_extracted_fact_long_v2 | dyspnea | 1 | 1 |
| canonical_extracted_fact_long_v2 | crainial_nerve_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_equipment | 1 | 1 |
| canonical_extracted_fact_long_v2 | sodium | 1 | 1 |
| canonical_extracted_fact_long_v2 | renal_disease | 1 | 1 |
| canonical_extracted_fact_long_v2 | intraop_nerve_monitoring | 1 | 1 |
| canonical_extracted_fact_long_v2 | calcium_level_total | 1 | 1 |
| canonical_extracted_fact_long_v2 | molecular_test | 1 | 1 |
| canonical_extracted_fact_long_v2 | palpitations | 1 | 1 |
| canonical_extracted_fact_long_v2 | radiation_exposure_history | 1 | 1 |
| canonical_extracted_fact_long_v2 | voice_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | gland_size | 1 | 1 |
| canonical_extracted_fact_long_v2 | eyes_exam | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_procedure | 1 | 1 |
| canonical_extracted_fact_long_v2 | assistant_surgeon | 1 | 1 |
| canonical_extracted_fact_long_v2 | stimulated_thyroglobulin | 1 | 1 |
| canonical_extracted_fact_long_v2 | hemoglobin_a1c | 1 | 1 |
| canonical_extracted_fact_long_v2 | phosphorus | 1 | 1 |
| canonical_extracted_fact_long_v2 | men_syndrome | 1 | 1 |
| canonical_extracted_fact_long_v2 | chloride | 1 | 1 |
| canonical_extracted_fact_long_v2 | tsh_receptor_antibody | 1 | 1 |
| canonical_extracted_fact_long_v2 | free_thyroxine_index | 1 | 1 |
| canonical_extracted_fact_long_v2 | white_blood_count | 1 | 1 |
| canonical_extracted_fact_long_v2 | rai_administration | 1 | 1 |
| canonical_extracted_fact_long_v2 | rai_refractory | 1 | 1 |
| canonical_extracted_fact_long_v2 | wound_exam, | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_palp | 1 | 1 |
| canonical_extracted_fact_long_v2 | frozen_section_target | 1 | 1 |
| canonical_extracted_fact_long_v2 | thyroid_stimulating_hormone | 1 | 1 |
| canonical_extracted_fact_long_v2 | surgical_plan | 1 | 1 |
| canonical_extracted_fact_long_v2 | neurological_monitoring | 1 | 1 |
| canonical_extracted_fact_long_v2 | follow_up | 1 | 1 |
| canonical_extracted_fact_long_v2 | complications | 1 | 1 |
| canonical_fact_quarantine_v2 | procedure | 16,909 | 4,151 |
| canonical_fact_quarantine_v2 | problem | 10,251 | 3,775 |
| canonical_fact_quarantine_v2 | complication | 8,941 | 2,741 |
| canonical_fact_quarantine_v2 | medication | 5,986 | 1,885 |
| canonical_fact_quarantine_v2 | nerve_monitoring | 4,216 | 2,757 |
| canonical_fact_quarantine_v2 | T_stage | 3,385 | 1,507 |
| canonical_fact_quarantine_v2 | ultrasound_thyroid | 3,266 | 1,591 |
| canonical_fact_quarantine_v2 | fna_cytology | 2,473 | 1,703 |
| canonical_fact_quarantine_v2 | nodule_size | 2,088 | 1,085 |
| canonical_fact_quarantine_v2 | surgical_pathology | 2,044 | 1,368 |
| canonical_fact_quarantine_v2 | vital_status | 1,924 | 1,887 |
| canonical_fact_quarantine_v2 | last_followup_date | 1,742 | 1,689 |
| canonical_fact_quarantine_v2 | gene | 1,738 | 605 |
| canonical_fact_quarantine_v2 | voice_quality | 1,460 | 1,414 |
| canonical_fact_quarantine_v2 | ebl | 1,322 | 1,270 |
| canonical_fact_quarantine_v2 | bethesda_class | 1,200 | 986 |
| canonical_fact_quarantine_v2 | vocal_cord_mobility | 1,142 | 1,004 |
| canonical_fact_quarantine_v2 | tumor_size | 1,055 | 790 |
| canonical_fact_quarantine_v2 | swallowing_function | 1,047 | 1,025 |
| canonical_fact_quarantine_v2 | other_surgery | 1,046 | 499 |
| canonical_fact_quarantine_v2 | nodule_location | 988 | 625 |
| canonical_fact_quarantine_v2 | parathyroid_management | 986 | 793 |
| canonical_fact_quarantine_v2 | molecular_testing | 926 | 721 |
| canonical_fact_quarantine_v2 | rln_finding | 836 | 527 |
| canonical_fact_quarantine_v2 | soft_tissue_invasion | 668 | 633 |
| canonical_fact_quarantine_v2 | vascular_invasion | 654 | 627 |
| canonical_fact_quarantine_v2 | capsular_invasion | 629 | 608 |
| canonical_fact_quarantine_v2 | prior_fna | 619 | 538 |
| canonical_fact_quarantine_v2 | ct_neck | 589 | 478 |
| canonical_fact_quarantine_v2 | ultrasound_lymph_node | 574 | 406 |
| canonical_fact_quarantine_v2 | swallowing_function_detail | 566 | 544 |
| canonical_fact_quarantine_v2 | lymph_node_pathology | 544 | 437 |
| canonical_fact_quarantine_v2 | tracheal_deviation | 539 | 469 |
| canonical_fact_quarantine_v2 | berry_ligament | 502 | 349 |
| canonical_fact_quarantine_v2 | ete_on_imaging | 469 | 436 |
| canonical_fact_quarantine_v2 | voice_recovery | 461 | 442 |
| canonical_fact_quarantine_v2 | extrathyroidal_extension | 456 | 417 |
| canonical_fact_quarantine_v2 | benign_pathology | 438 | 352 |
| canonical_fact_quarantine_v2 | tirads_score | 427 | 334 |
| canonical_fact_quarantine_v2 | treatment_episode_number | 426 | 397 |
| canonical_fact_quarantine_v2 | calcium_quality_of_life | 415 | 410 |
| canonical_fact_quarantine_v2 | ptnm_stage | 413 | 400 |
| canonical_fact_quarantine_v2 | tsh | 406 | 336 |
| canonical_fact_quarantine_v2 | lymphovascular_invasion | 387 | 353 |
| canonical_fact_quarantine_v2 | rai_dose_mci | 384 | 368 |
| canonical_fact_quarantine_v2 | drain_placement | 381 | 323 |
| canonical_fact_quarantine_v2 | mass_effect | 380 | 354 |
| canonical_fact_quarantine_v2 | substernal_extension | 371 | 322 |
| canonical_fact_quarantine_v2 | post_treatment_wbs_findings | 357 | 350 |
| canonical_fact_quarantine_v2 | multifocality | 352 | 325 |
| canonical_fact_quarantine_v2 | margin_status | 351 | 317 |
| canonical_fact_quarantine_v2 | prior_thyroidectomy | 344 | 299 |
| canonical_fact_quarantine_v2 | strap_muscle | 344 | 314 |
| canonical_fact_quarantine_v2 | neck_exam | 339 | 313 |
| canonical_fact_quarantine_v2 | preparation_method | 317 | 317 |
| canonical_fact_quarantine_v2 | tumor_variant | 294 | 269 |
| canonical_fact_quarantine_v2 | pre_rai_tsh | 289 | 289 |
| canonical_fact_quarantine_v2 | specimen_detail | 281 | 269 |
| canonical_fact_quarantine_v2 | side_effects | 277 | 277 |
| canonical_fact_quarantine_v2 | overall_stage | 276 | 203 |
| canonical_fact_quarantine_v2 | isolation_days | 272 | 272 |
| canonical_fact_quarantine_v2 | thyroid_palpation | 270 | 251 |
| canonical_fact_quarantine_v2 | follow_up_duration | 269 | 268 |
| canonical_fact_quarantine_v2 | perineural_invasion_detailed | 260 | 251 |
| canonical_fact_quarantine_v2 | synoptic_report | 259 | 256 |
| canonical_fact_quarantine_v2 | vessel_count | 251 | 249 |
| canonical_fact_quarantine_v2 | nuclear_med | 236 | 159 |
| canonical_fact_quarantine_v2 | UNKNOWN | 236 | 56 |
| canonical_fact_quarantine_v2 | dedifferentiation | 234 | 233 |
| canonical_fact_quarantine_v2 | return_to_work | 214 | 213 |
| canonical_fact_quarantine_v2 | frozen_section_result | 212 | 197 |
| canonical_fact_quarantine_v2 | necrosis | 210 | 208 |
| canonical_fact_quarantine_v2 | thyroglobulin | 200 | 120 |
| canonical_fact_quarantine_v2 | mitotic_rate | 197 | 196 |
| canonical_fact_quarantine_v2 | lymph_node_palpation | 194 | 177 |
| canonical_fact_quarantine_v2 | ki67_index | 193 | 192 |
| canonical_fact_quarantine_v2 | tall_cell_percentage | 192 | 192 |
| canonical_fact_quarantine_v2 | discharge_disposition | 180 | 178 |
| canonical_fact_quarantine_v2 | surveillance_adherence | 179 | 137 |
| canonical_fact_quarantine_v2 | vascular_encasement | 176 | 158 |
| canonical_fact_quarantine_v2 | tracheal_narrowing | 166 | 155 |
| canonical_fact_quarantine_v2 | prior_neck_surgery | 162 | 128 |
| canonical_fact_quarantine_v2 | treatment_declined | 157 | 140 |
| canonical_fact_quarantine_v2 | frozen_section | 144 | 135 |
| canonical_fact_quarantine_v2 | parathyroid_autograft | 140 | 102 |
| canonical_fact_quarantine_v2 | thyroglobulin_stimulated | 137 | 75 |
| canonical_fact_quarantine_v2 | laryngoscopy_findings | 134 | 132 |
| canonical_fact_quarantine_v2 | airway_exam | 130 | 121 |
| canonical_fact_quarantine_v2 | structural_recurrence | 127 | 86 |
| canonical_fact_quarantine_v2 | prior_rai | 123 | 118 |
| canonical_fact_quarantine_v2 | perineural_invasion | 119 | 113 |
| canonical_fact_quarantine_v2 | lymph_node_level | 119 | 76 |
| canonical_fact_quarantine_v2 | tg_value | 114 | 54 |
| canonical_fact_quarantine_v2 | prior_cancer | 113 | 80 |
| canonical_fact_quarantine_v2 | laryngoscopy_date | 113 | 112 |
| canonical_fact_quarantine_v2 | N_stage | 110 | 82 |
| canonical_fact_quarantine_v2 | reoperative_field | 105 | 87 |
| canonical_fact_quarantine_v2 | vocal_cord_imaging | 104 | 101 |
| canonical_fact_quarantine_v2 | ln_level | 99 | 48 |
| canonical_fact_quarantine_v2 | free_t4 | 97 | 85 |
| canonical_fact_quarantine_v2 | airway_compromise_grade | 94 | 90 |
| canonical_fact_quarantine_v2 | rai_indication | 94 | 87 |
| canonical_fact_quarantine_v2 | calcium | 90 | 73 |
| canonical_fact_quarantine_v2 | neck_mass | 84 | 69 |
| canonical_fact_quarantine_v2 | dysphagia | 80 | 74 |
| canonical_fact_quarantine_v2 | patient_preference | 78 | 76 |
| canonical_fact_quarantine_v2 | voice_assessment | 72 | 72 |
| canonical_fact_quarantine_v2 | vascular_invasion_type | 71 | 68 |
| canonical_fact_quarantine_v2 | hypertension | 70 | 68 |
| canonical_fact_quarantine_v2 | thyroid_hormone_suppression | 69 | 54 |
| canonical_fact_quarantine_v2 | autoimmune_thyroid | 67 | 57 |
| canonical_fact_quarantine_v2 | quality_of_life_score | 66 | 65 |
| canonical_fact_quarantine_v2 | esophageal_compression | 65 | 64 |
| canonical_fact_quarantine_v2 | laryngeal_invasion | 64 | 63 |
| canonical_fact_quarantine_v2 | pet_ct | 63 | 49 |
| canonical_fact_quarantine_v2 | wound_exam | 62 | 60 |
| canonical_fact_quarantine_v2 | prior_neck_dissection | 60 | 53 |
| canonical_fact_quarantine_v2 | diabetes | 56 | 55 |
| canonical_fact_quarantine_v2 | shared_decision | 55 | 55 |
| canonical_fact_quarantine_v2 | cardiovascular | 55 | 34 |
| canonical_fact_quarantine_v2 | rai_ablation | 55 | 45 |
| canonical_fact_quarantine_v2 | gland_location | 54 | 42 |
| canonical_fact_quarantine_v2 | disease_free | 53 | 42 |
| canonical_fact_quarantine_v2 | family_hx_thyroid | 53 | 46 |
| canonical_fact_quarantine_v2 | pre_rai_tg | 51 | 47 |
| canonical_fact_quarantine_v2 | dysphonia | 51 | 49 |
| canonical_fact_quarantine_v2 | obesity | 51 | 44 |
| canonical_fact_quarantine_v2 | thyroid_nodule | 49 | 26 |
| canonical_fact_quarantine_v2 | vitamin_d | 49 | 47 |
| canonical_fact_quarantine_v2 | anti_thyroglobulin | 46 | 33 |
| canonical_fact_quarantine_v2 | prior_core_biopsy | 46 | 41 |
| canonical_fact_quarantine_v2 | scar_assessment | 45 | 45 |
| canonical_fact_quarantine_v2 | pth | 43 | 39 |
| canonical_fact_quarantine_v2 | incidental_finding | 42 | 36 |
| canonical_fact_quarantine_v2 | cranial_nerve_exam | 39 | 39 |
| canonical_fact_quarantine_v2 | chvostek_sign | 39 | 39 |
| canonical_fact_quarantine_v2 | post_treatment_scan | 39 | 34 |
| canonical_fact_quarantine_v2 | thyrogen_stimulation | 39 | 24 |
| canonical_fact_quarantine_v2 | scar_satisfaction | 37 | 37 |
| canonical_fact_quarantine_v2 | calcium_symptom_chronicity | 37 | 37 |
| canonical_fact_quarantine_v2 | M_stage | 36 | 32 |
| canonical_fact_quarantine_v2 | nodule_volume | 35 | 19 |
| canonical_fact_quarantine_v2 | final_pathology_concordance | 34 | 34 |
| canonical_fact_quarantine_v2 | tirads_category | 33 | 30 |
| canonical_fact_quarantine_v2 | free_t3 | 32 | 30 |
| canonical_fact_quarantine_v2 | radiation_exposure | 31 | 26 |
| canonical_fact_quarantine_v2 | xerostomia | 30 | 30 |
| canonical_fact_quarantine_v2 | smoking_status | 27 | 25 |
| canonical_fact_quarantine_v2 | mri_neck | 26 | 24 |
| canonical_fact_quarantine_v2 | prior_parathyroidectomy | 26 | 24 |
| canonical_fact_quarantine_v2 | trousseau_sign | 26 | 26 |
| canonical_fact_quarantine_v2 | tirads_composition | 25 | 24 |
| canonical_fact_quarantine_v2 | speech_therapy_referral | 25 | 25 |
| canonical_fact_quarantine_v2 | stunning_concern | 25 | 25 |
| canonical_fact_quarantine_v2 | osteoporosis | 24 | 19 |
| canonical_fact_quarantine_v2 | dyspnea | 23 | 20 |
| canonical_fact_quarantine_v2 | total_t4 | 23 | 22 |
| canonical_fact_quarantine_v2 | injection_laryngoplasty | 22 | 22 |
| canonical_fact_quarantine_v2 | hoarseness | 22 | 22 |
| canonical_fact_quarantine_v2 | nodule_dimensions | 22 | 17 |
| canonical_fact_quarantine_v2 | diagnostic_i123_scan | 21 | 20 |
| canonical_fact_quarantine_v2 | biochemical_recurrence | 20 | 15 |
| canonical_fact_quarantine_v2 | distant_recurrence | 20 | 16 |
| canonical_fact_quarantine_v2 | calcitonin | 19 | 17 |
| canonical_fact_quarantine_v2 | weight_change | 19 | 19 |
| canonical_fact_quarantine_v2 | intraop_complication | 18 | 15 |
| canonical_fact_quarantine_v2 | anti_tg_value | 18 | 10 |
| canonical_fact_quarantine_v2 | biochemical_persistence | 18 | 14 |
| canonical_fact_quarantine_v2 | family_hx_cancer | 17 | 13 |
| canonical_fact_quarantine_v2 | followup_gap | 16 | 16 |
| canonical_fact_quarantine_v2 | tracheal_involvement | 16 | 9 |
| canonical_fact_quarantine_v2 | gross_invasion | 16 | 13 |
| canonical_fact_quarantine_v2 | coagulopathy | 15 | 14 |
| canonical_fact_quarantine_v2 | tirads_echogenicity | 15 | 15 |
| canonical_fact_quarantine_v2 | ebrt | 14 | 12 |
| canonical_fact_quarantine_v2 | nodule_identifier | 14 | 12 |
| canonical_fact_quarantine_v2 | pulmonary_disease | 13 | 11 |
| canonical_fact_quarantine_v2 | rai_date_administered | 13 | 12 |
| canonical_fact_quarantine_v2 | voice_handicap_index | 13 | 13 |
| canonical_fact_quarantine_v2 | neck_examination | 13 | 13 |
| canonical_fact_quarantine_v2 | tirads_shape | 12 | 12 |
| canonical_fact_quarantine_v2 | tsi | 12 | 11 |
| canonical_fact_quarantine_v2 | rai_treatment | 12 | 12 |
| canonical_fact_quarantine_v2 | physical_exam | 12 | 1 |
| canonical_fact_quarantine_v2 | surveillance_interval | 12 | 10 |
| canonical_fact_quarantine_v2 | tirads_margin | 11 | 11 |
| canonical_fact_quarantine_v2 | voice | 11 | 11 |
| canonical_fact_quarantine_v2 | tirads_echogenic_foci | 10 | 10 |
| canonical_fact_quarantine_v2 | gland_count_total | 10 | 10 |
| canonical_fact_quarantine_v2 | tirads | 10 | 6 |
| canonical_fact_quarantine_v2 | removal_intent | 10 | 9 |
| canonical_fact_quarantine_v2 | chest_xray | 10 | 10 |
| canonical_fact_quarantine_v2 | tpo_antibody | 10 | 10 |
| canonical_fact_quarantine_v2 | renal_disease | 10 | 10 |
| canonical_fact_quarantine_v2 | fatigue | 10 | 10 |
| canonical_fact_quarantine_v2 | procedure_performed | 9 | 4 |
| canonical_fact_quarantine_v2 | rai_preparation | 9 | 8 |
| canonical_fact_quarantine_v2 | trachea | 9 | 9 |
| canonical_fact_quarantine_v2 | ata_risk_category | 9 | 9 |
| canonical_fact_quarantine_v2 | surveillance_impression | 9 | 8 |
| canonical_fact_quarantine_v2 | psychiatric | 9 | 8 |
| canonical_fact_quarantine_v2 | cea | 8 | 7 |
| canonical_fact_quarantine_v2 | revision_surgery | 8 | 8 |
| canonical_fact_quarantine_v2 | cumulative_rai_dose | 8 | 8 |
| canonical_fact_quarantine_v2 | tirads_component_composition | 8 | 6 |
| canonical_fact_quarantine_v2 | tirads_total_points | 8 | 8 |
| canonical_fact_quarantine_v2 | anxiety_tremor | 8 | 8 |
| canonical_fact_quarantine_v2 | procedures_performed | 8 | 3 |
| canonical_fact_quarantine_v2 | diagnosis | 8 | 3 |
| canonical_fact_quarantine_v2 | autotransplant | 7 | 6 |
| canonical_fact_quarantine_v2 | voice_changes | 7 | 7 |
| canonical_fact_quarantine_v2 | thyroid | 7 | 6 |
| canonical_fact_quarantine_v2 | paired_tsh | 7 | 5 |
| canonical_fact_quarantine_v2 | symptom_duration | 7 | 6 |
| canonical_fact_quarantine_v2 | tumor_stage | 7 | 7 |
| canonical_fact_quarantine_v2 | ata_response_category | 6 | 6 |
| canonical_fact_quarantine_v2 | liver_disease | 6 | 6 |
| canonical_fact_quarantine_v2 | frozen_section_target | 6 | 5 |
| canonical_fact_quarantine_v2 | gland_cellularity | 6 | 5 |
| canonical_fact_quarantine_v2 | tracheal_compression | 6 | 4 |
| canonical_fact_quarantine_v2 | lymph_node_involvement | 6 | 5 |
| canonical_fact_quarantine_v2 | neck | 6 | 6 |
| canonical_fact_quarantine_v2 | rai_refractory | 6 | 6 |
| canonical_fact_quarantine_v2 | nodule_stability | 6 | 6 |
| canonical_fact_quarantine_v2 | tirads_recommendation | 6 | 6 |
| canonical_fact_quarantine_v2 | thyroid_nodule_size | 5 | 3 |
| canonical_fact_quarantine_v2 | gland_count_preserved | 5 | 5 |
| canonical_fact_quarantine_v2 | disease_status | 5 | 5 |
| canonical_fact_quarantine_v2 | neurologic_exam | 5 | 5 |
| canonical_fact_quarantine_v2 | tumor_multifocality | 5 | 5 |
| canonical_fact_quarantine_v2 | palpitations | 5 | 5 |
| canonical_fact_quarantine_v2 | surgical_approach | 5 | 5 |
| canonical_fact_quarantine_v2 | men_syndrome | 5 | 5 |
| canonical_fact_quarantine_v2 | tki_dose_reduction | 5 | 5 |
| canonical_fact_quarantine_v2 | ct_chest | 5 | 5 |
| canonical_fact_quarantine_v2 | lymphatic_invasion | 5 | 5 |
| canonical_fact_quarantine_v2 | TSH | 5 | 3 |
| canonical_fact_quarantine_v2 | surgical_procedure | 5 | 4 |
| canonical_fact_quarantine_v2 | gland_size | 5 | 3 |
| canonical_fact_quarantine_v2 | clinical_trial | 5 | 5 |
| canonical_fact_quarantine_v2 | second_opinion | 5 | 4 |
| canonical_fact_quarantine_v2 | pT_stage | 5 | 5 |
| canonical_fact_quarantine_v2 | pathologic_stage | 5 | 5 |
| canonical_fact_quarantine_v2 | creatinine | 4 | 4 |
| canonical_fact_quarantine_v2 | pN_stage | 4 | 4 |
| canonical_fact_quarantine_v2 | rai_dose | 4 | 4 |
| canonical_fact_quarantine_v2 | entity_date | 4 | 4 |
| canonical_fact_quarantine_v2 | tirads_component_echogenicity | 4 | 3 |
| canonical_fact_quarantine_v2 | calcium_level | 4 | 2 |
| canonical_fact_quarantine_v2 | heat_cold_intolerance | 4 | 4 |
| canonical_fact_quarantine_v2 | Neck | 4 | 4 |
| canonical_fact_quarantine_v2 | nodule_growth_rate | 4 | 4 |
| canonical_fact_quarantine_v2 | thyroid_function | 4 | 4 |
| canonical_fact_quarantine_v2 | thyroid_examination | 4 | 4 |
| canonical_fact_quarantine_v2 | decline_reason | 4 | 3 |
| canonical_fact_quarantine_v2 | trab | 4 | 4 |
| canonical_fact_quarantine_v2 | intraop_decision_impact | 4 | 4 |
| canonical_fact_quarantine_v2 | extranodal_extension | 4 | 4 |
| canonical_fact_quarantine_v2 | lymphadenopathy | 4 | 4 |
| canonical_fact_quarantine_v2 | rai_administration | 4 | 4 |
| canonical_fact_quarantine_v2 | thyroid_biopsy | 4 | 4 |
| canonical_fact_quarantine_v2 | thyromegaly | 4 | 3 |
| canonical_fact_quarantine_v2 | thyroid_exam | 4 | 3 |
| canonical_fact_quarantine_v2 | gland_weight | 4 | 4 |
| canonical_fact_quarantine_v2 | tg_context | 4 | 2 |
| canonical_fact_quarantine_v2 | odynophagia | 4 | 4 |
| canonical_fact_quarantine_v2 | nodule | 4 | 3 |
| canonical_fact_quarantine_v2 | neurological_exam | 3 | 2 |
| canonical_fact_quarantine_v2 | trachea_exam | 3 | 3 |
| canonical_fact_quarantine_v2 | margin_distance | 3 | 3 |
| canonical_fact_quarantine_v2 | dysphonia_dysphagia_dyspnea | 3 | 3 |
| canonical_fact_quarantine_v2 | abdominal_exam | 3 | 3 |
| canonical_fact_quarantine_v2 | specimen_type | 3 | 2 |
| canonical_fact_quarantine_v2 | lymph_node_metastasis | 3 | 3 |
| canonical_fact_quarantine_v2 | hypercellularity_grade | 3 | 3 |
| canonical_fact_quarantine_v2 | tki_therapy | 3 | 2 |
| canonical_fact_quarantine_v2 | specimen | 3 | 3 |
| canonical_fact_quarantine_v2 | pathology | 3 | 3 |
| canonical_fact_quarantine_v2 | radiation_exposure_history | 3 | 3 |
| canonical_fact_quarantine_v2 | treatment_decision | 3 | 3 |
| canonical_fact_quarantine_v2 | lymph_node | 3 | 3 |
| canonical_fact_quarantine_v2 | postoperative_diagnosis | 3 | 3 |
| canonical_fact_quarantine_v2 | blood_pressure | 3 | 3 |
| canonical_fact_quarantine_v2 | total_t3 | 3 | 3 |
| canonical_fact_quarantine_v2 | previous_fna | 3 | 3 |
| canonical_fact_quarantine_v2 | neck_supple | 3 | 3 |
| canonical_fact_quarantine_v2 | thyroid_cancer | 3 | 3 |
| canonical_fact_quarantine_v2 | t3 | 3 | 3 |
| canonical_fact_quarantine_v2 | cause_of_death | 3 | 3 |
| canonical_fact_quarantine_v2 | benign_lymph_nodes | 3 | 3 |
| canonical_fact_quarantine_v2 | imaging | 3 | 2 |
| canonical_fact_quarantine_v2 | stimulated_thyroglobulin | 3 | 2 |
| canonical_fact_quarantine_v2 | fna | 3 | 3 |
| canonical_fact_quarantine_v2 | calc3 | 2 | 1 |
| canonical_fact_quarantine_v2 | etr_on_imaging | 2 | 2 |
| canonical_fact_quarantine_v2 | date | 2 | 2 |
| canonical_fact_quarantine_v2 | specimens | 2 | 1 |
| canonical_fact_quarantine_v2 | thyroid_enlargement | 2 | 2 |
| canonical_fact_quarantine_v2 | allergies | 2 | 2 |
| canonical_fact_quarantine_v2 | lymph_nodes | 2 | 2 |
| canonical_fact_quarantine_v2 | thyroid_size | 2 | 1 |
| canonical_fact_quarantine_v2 | us_visit_number | 2 | 2 |
| canonical_fact_quarantine_v2 | thyroid_condition | 2 | 2 |
| canonical_fact_quarantine_v2 | positive_lymph_nodes | 2 | 1 |
| canonical_fact_quarantine_v2 | tirads_component_shape | 2 | 1 |
| canonical_fact_quarantine_v2 | low_iodine_diet | 2 | 2 |
| canonical_fact_quarantine_v2 | esophageal_involvement | 2 | 2 |
| canonical_fact_quarantine_v2 | lungs_exam | 2 | 1 |
| canonical_fact_quarantine_v2 | respiratory_exam | 2 | 2 |
| canonical_fact_quarantine_v2 | surgery | 2 | 2 |
| canonical_fact_quarantine_v2 | metastatic_disease | 2 | 2 |
| canonical_fact_quarantine_v2 | crainial_nerve_exam | 2 | 2 |
| canonical_fact_quarantine_v2 | thyroidectomy | 2 | 2 |
| canonical_fact_quarantine_v2 | angioinvasion_count | 2 | 2 |
| canonical_fact_quarantine_v2 | margin_location | 2 | 2 |
| canonical_fact_quarantine_v2 | margins | 2 | 2 |
| canonical_fact_quarantine_v2 | tg_assay_method | 2 | 2 |
| canonical_fact_quarantine_v2 | tirads_vascularity | 2 | 2 |
| canonical_fact_quarantine_v2 | symptoms | 2 | 2 |
| canonical_fact_quarantine_v2 | cardiovascular_exam | 2 | 2 |
| canonical_fact_quarantine_v2 | neuro_exam | 2 | 2 |
| canonical_fact_quarantine_v2 | anesthesia_history | 2 | 2 |
| canonical_fact_quarantine_v2 | ligation_of_vessels | 2 | 1 |
| canonical_fact_quarantine_v2 | hypocalcemia | 2 | 2 |
| canonical_fact_quarantine_v2 | isthmus_thickness | 2 | 2 |
| canonical_fact_quarantine_v2 | thyroid_findings | 2 | 2 |
| canonical_fact_quarantine_v2 | tki_toxicity | 2 | 2 |
| canonical_fact_quarantine_v2 | calcitonin_level | 2 | 2 |
| canonical_fact_quarantine_v2 | isthmus_size | 2 | 1 |
| canonical_fact_quarantine_v2 | thyroid_function_test | 2 | 2 |
| canonical_fact_quarantine_v2 | voice_change | 2 | 2 |
| canonical_fact_quarantine_v2 | ln_number_per_level | 2 | 2 |
| canonical_fact_quarantine_v2 | preoperative_diagnosis | 2 | 2 |
| canonical_fact_quarantine_v2 | hyperlipidemia | 2 | 2 |
| canonical_fact_quarantine_v2 | procedure_plan | 1 | 1 |
| canonical_fact_quarantine_v2 | laboratory | 1 | 1 |
| canonical_fact_quarantine_v2 | weight_loss | 1 | 1 |
| canonical_fact_quarantine_v2 | aphonia | 1 | 1 |
| canonical_fact_quarantine_v2 | neurologic_examination | 1 | 1 |
| canonical_fact_quarantine_v2 | hgb | 1 | 1 |
| canonical_fact_quarantine_v2 | Ca | 1 | 1 |
| canonical_fact_quarantine_v2 | chromogranin | 1 | 1 |
| canonical_fact_quarantine_v2 | PTH | 1 | 1 |
| canonical_fact_quarantine_v2 | benign_thyroid_background | 1 | 1 |
| canonical_fact_quarantine_v2 | lymph_node_status | 1 | 1 |
| canonical_fact_quarantine_v2 | flexible_laryngoscopy | 1 | 1 |
| canonical_fact_quarantine_v2 | neoplasm | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_notch_palpable | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_palp | 1 | 1 |
| canonical_fact_quarantine_v2 | musculoskeletal_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | difficulty_swallowing | 1 | 1 |
| canonical_fact_quarantine_v2 | ablation_rx | 1 | 1 |
| canonical_fact_quarantine_v2 | tuberculosis,  | 1 | 1 |
| canonical_fact_quarantine_v2 | General | 1 | 1 |
| canonical_fact_quarantine_v2 | previous_thyroid_nodules | 1 | 1 |
| canonical_fact_quarantine_v2 | tobacco_use | 1 | 1 |
| canonical_fact_quarantine_v2 | GFR | 1 | 1 |
| canonical_fact_quarantine_v2 | 24 hour urine calcium | 1 | 1 |
| canonical_fact_quarantine_v2 | hypocalcemia_risk | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_mass | 1 | 1 |
| canonical_fact_quarantine_v2 | recurrence | 1 | 1 |
| canonical_fact_quarantine_v2 | family_history | 1 | 1 |
| canonical_fact_quarantine_v2 | no_subsequent_rai_treatment | 1 | 1 |
| canonical_fact_quarantine_v2 | SpO2 | 1 | 1 |
| canonical_fact_quarantine_v2 | musculoskeletal | 1 | 1 |
| canonical_fact_quarantine_v2 | indications_for_procedure | 1 | 1 |
| canonical_fact_quarantine_v2 | ct_neck_findings | 1 | 1 |
| canonical_fact_quarantine_v2 | chronic_kidney_disease | 1 | 1 |
| canonical_fact_quarantine_v2 | acanthosis_nigricans | 1 | 1 |
| canonical_fact_quarantine_v2 | physical_exam_findings | 1 | 1 |
| canonical_fact_quarantine_v2 | skin_warm_dry | 1 | 1 |
| canonical_fact_quarantine_v2 | hypoparathyroidism | 1 | 1 |
| canonical_fact_quarantine_v2 | comorbidity | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroglossal duct cyst | 1 | 1 |
| canonical_fact_quarantine_v2 | alkaline_phosphatase | 1 | 1 |
| canonical_fact_quarantine_v2 | airway_assessment | 1 | 1 |
| canonical_fact_quarantine_v2 | findings | 1 | 1 |
| canonical_fact_quarantine_v2 | substerneal_extension | 1 | 1 |
| canonical_fact_quarantine_v2 | t4 | 1 | 1 |
| canonical_fact_quarantine_v2 | treatment | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_ultrasound | 1 | 1 |
| canonical_fact_quarantine_v2 | pulmonary_chest | 1 | 1 |
| canonical_fact_quarantine_v2 | inguinal_hernia | 1 | 1 |
| canonical_fact_quarantine_v2 | w | 1 | 1 |
| canonical_fact_quarantine_v2 | Neurologic | 1 | 1 |
| canonical_fact_quarantine_v2 | lymph_node_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | anesthesia_plan | 1 | 1 |
| canonical_fact_quarantine_v2 | symptom | 1 | 1 |
| canonical_fact_quarantine_v2 | benign_background | 1 | 1 |
| canonical_fact_quarantine_v2 | BMI | 1 | 1 |
| canonical_fact_quarantine_v2 | capsular_invasion_type | 1 | 1 |
| canonical_fact_quarantine_v2 | respiratory_function | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_cancer_diagnosis | 1 | 1 |
| canonical_fact_quarantine_v2 | Physical Exam | 1 | 1 |
| canonical_fact_quarantine_v2 | ct | 1 | 1 |
| canonical_fact_quarantine_v2 | 24_hour_urine_metanephrine | 1 | 1 |
| canonical_fact_quarantine_v2 | stage | 1 | 1 |
| canonical_fact_quarantine_v2 | operation | 1 | 1 |
| canonical_fact_quarantine_v2 | benign | 1 | 1 |
| canonical_fact_quarantine_v2 | lymph_node_ratio | 1 | 1 |
| canonical_fact_quarantine_v2 | abdomen soft NTND | 1 | 1 |
| canonical_fact_quarantine_v2 | general_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | skin | 1 | 1 |
| canonical_fact_quarantine_v2 | no_evidence_of_malignancy | 1 | 1 |
| canonical_fact_quarantine_v2 | regional_lymph_nodes | 1 | 1 |
| canonical_fact_quarantine_v2 | adenomatoid_nodules | 1 | 1 |
| canonical_fact_quarantine_v2 | lymph node assessment | 1 | 1 |
| canonical_fact_quarantine_v2 | lost_to_followup | 1 | 1 |
| canonical_fact_quarantine_v2 | total_thyroidectomy | 1 | 1 |
| canonical_fact_quarantine_v2 | laryngeal_mass | 1 | 1 |
| canonical_fact_quarantine_v2 |  at line 1, column 1, near  | 1 | 1 |
| canonical_fact_quarantine_v2 | neurologic | 1 | 1 |
| canonical_fact_quarantine_v2 | isthmus_thickening | 1 | 1 |
| canonical_fact_quarantine_v2 | ct_scan | 1 | 1 |
| canonical_fact_quarantine_v2 | CV | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_dissection | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroidectomy_plan | 1 | 1 |
| canonical_fact_quarantine_v2 | pet_scanning | 1 | 1 |
| canonical_fact_quarantine_v2 | }]}, but the assistant's response is cut off. Let me complete the JSON structure properly. Here's the corrected version of the JSON output based on the provided information and the specified format:```json{ | 1 | 1 |
| canonical_fact_quarantine_v2 | entity_type | 1 | 1 |
| canonical_fact_quarantine_v2 | high_risk_features | 1 | 1 |
| canonical_fact_quarantine_v2 | radioactive_iodine | 1 | 1 |
| canonical_fact_quarantine_v2 | a1c | 1 | 1 |
| canonical_fact_quarantine_v2 | thyrogland | 1 | 1 |
| canonical_fact_quarantine_v2 | Calcitonin | 1 | 1 |
| canonical_fact_quarantine_v2 | elongated_soft_tissue | 1 | 1 |
| canonical_fact_quarantine_v2 | jvd | 1 | 1 |
| canonical_fact_quarantine_v2 | surgical_plan | 1 | 1 |
| canonical_fact_quarantine_v2 | reimplantation_detail | 1 | 1 |
| canonical_fact_quarantine_v2 | focal_location | 1 | 1 |
| canonical_fact_quarantine_v2 | complications | 1 | 1 |
| canonical_fact_quarantine_v2 | no_thyroid_cancer_family_history | 1 | 1 |
| canonical_fact_quarantine_v2 | parathyroid | 1 | 1 |
| canonical_fact_quarantine_v2 | phosphate | 1 | 1 |
| canonical_fact_quarantine_v2 | coronary_artery_disease | 1 | 1 |
| canonical_fact_quarantine_v2 | atrial_fibrillation | 1 | 1 |
| canonical_fact_quarantine_v2 | diabetes_mellitus | 1 | 1 |
| canonical_fact_quarantine_v2 | no thyroid bed masses or thyroid tissue | 1 | 1 |
| canonical_fact_quarantine_v2 | chvostek sign | 1 | 1 |
| canonical_fact_quarantine_v2 | neurological_examination | 1 | 1 |
| canonical_fact_quarantine_v2 | parathyroid_adenoma | 1 | 1 |
| canonical_fact_quarantine_v2 | vitamin_d_deficiency | 1 | 1 |
| canonical_fact_quarantine_v2 | patient_consent | 1 | 1 |
| canonical_fact_quarantine_v2 | tsh_level | 1 | 1 |
| canonical_fact_quarantine_v2 | Extremities | 1 | 1 |
| canonical_fact_quarantine_v2 | Skin | 1 | 1 |
| canonical_fact_quarantine_v2 | tsh_goal | 1 | 1 |
| canonical_fact_quarantine_v2 | chromogranin_a | 1 | 1 |
| canonical_fact_quarantine_v2 | HEENT | 1 | 1 |
| canonical_fact_quarantine_v2 | trachea_deviation | 1 | 1 |
| canonical_fact_quarantine_v2 | hypothyroidism | 1 | 1 |
| canonical_fact_quarantine_v2 | neck_exam: stridor, tracheal deviation, airway compromise findings | 1 | 1 |
| canonical_fact_quarantine_v2 | tracheal_position | 1 | 1 |
| canonical_fact_quarantine_v2 | frozen_section_turnaround | 1 | 1 |
| canonical_fact_quarantine_v2 | chest clear to auscultation | 1 | 1 |
| canonical_fact_quarantine_v2 | tremors | 1 | 1 |
| canonical_fact_quarantine_v2 | benign_lesion | 1 | 1 |
| canonical_fact_quarantine_v2 | subglottic_stenosis | 1 | 1 |
| canonical_fact_quarantine_v2 | airway_clear | 1 | 1 |
| canonical_fact_quarantine_v2 | denial_of_symptoms | 1 | 1 |
| canonical_fact_quarantine_v2 | no_radiation_exposure | 1 | 1 |
| canonical_fact_quarantine_v2 | ln_laterality | 1 | 1 |
| canonical_fact_quarantine_v2 | fna_of_ln | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroglossal_duct_cyst | 1 | 1 |
| canonical_fact_quarantine_v2 | incision | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_cyst | 1 | 1 |
| canonical_fact_quarantine_v2 | plan_surgery | 1 | 1 |
| canonical_fact_quarantine_v2 | risk_assessment | 1 | 1 |
| canonical_fact_quarantine_v2 | Note Received | 1 | 1 |
| canonical_fact_quarantine_v2 | neck_nodule | 1 | 1 |
| canonical_fact_quarantine_v2 | whole_body_scan | 1 | 1 |
| canonical_fact_quarantine_v2 | musculoskeletal_normal | 1 | 1 |
| canonical_fact_quarantine_v2 | Pulmonary | 1 | 1 |
| canonical_fact_quarantine_v2 | abdominal | 1 | 1 |
| canonical_fact_quarantine_v2 | no_thyroid_enlargement | 1 | 1 |
| canonical_fact_quarantine_v2 | lab_abnormality | 1 | 1 |
| canonical_fact_quarantine_v2 | JVD | 1 | 1 |
| canonical_fact_quarantine_v2 | postop PTH | 1 | 1 |
| canonical_fact_quarantine_v2 | no_masses_abdomen | 1 | 1 |
| canonical_fact_quarantine_v2 | no_cervical_lymphadenopathy | 1 | 1 |
| canonical_fact_quarantine_v2 | phosphorus | 1 | 1 |
| canonical_fact_quarantine_v2 | cricoid_not_palpable | 1 | 1 |
| canonical_fact_quarantine_v2 | ln_morphology | 1 | 1 |
| canonical_fact_quarantine_v2 | vocal_cord_assessment | 1 | 1 |
| canonical_fact_quarantine_v2 | ectopic_parathyroid | 1 | 1 |
| canonical_fact_quarantine_v2 | Thyroid | 1 | 1 |
| canonical_fact_quarantine_v2 | swallowing_difficulty | 1 | 1 |
| canonical_fact_quarantine_v2 | neurological | 1 | 1 |
| canonical_fact_quarantine_v2 | trophostek sign | 1 | 1 |
| canonical_fact_quarantine_v2 | adenomatoid_nodule | 1 | 1 |
| canonical_fact_quarantine_v2 | no_surgery_wanted | 1 | 1 |
| canonical_fact_quarantine_v2 | airway_obstruction | 1 | 1 |
| canonical_fact_quarantine_v2 | nuclear_medicine_scan | 1 | 1 |
| canonical_fact_quarantine_v2 | tumor_type | 1 | 1 |
| canonical_fact_quarantine_v2 | Date of Service | 1 | 1 |
| canonical_fact_quarantine_v2 | abdominal_examination | 1 | 1 |
| canonical_fact_quarantine_v2 | intraoperative_monitoring | 1 | 1 |
| canonical_fact_quarantine_v2 | Vitals | 1 | 1 |
| canonical_fact_quarantine_v2 | tumor_margin | 1 | 1 |
| canonical_fact_quarantine_v2 | angioinvasion | 1 | 1 |
| canonical_fact_quarantine_v2 | skin_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | no lower extremity edema | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_nodularity | 1 | 1 |
| canonical_fact_quarantine_v2 | globus_sensation | 1 | 1 |
| canonical_fact_quarantine_v2 | intraop_nerve_monitoring | 1 | 1 |
| canonical_fact_quarantine_v2 | genitourinary_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | dissection_of_thyroid | 1 | 1 |
| canonical_fact_quarantine_v2 | estimated_blood_loss | 1 | 1 |
| canonical_fact_quarantine_v2 | discordance_reason | 1 | 1 |
| canonical_fact_quarantine_v2 | TPAb | 1 | 1 |
| canonical_fact_quarantine_v2 | free_t4_index | 1 | 1 |
| canonical_fact_quarantine_v2 | tracheaumatisation | 1 | 1 |
| canonical_fact_quarantine_v2 | tg_detection_limit | 1 | 1 |
| canonical_fact_quarantine_v2 | Supraclavicular lymph nodes | 1 | 1 |
| canonical_fact_quarantine_v2 | HR | 1 | 1 |
| canonical_fact_quarantine_v2 | ionized calcium | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_nodule_palpable | 1 | 1 |
| canonical_fact_quarantine_v2 | 2023-07-20 | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_nodules | 1 | 1 |
| canonical_fact_quarantine_v2 | incision_status | 1 | 1 |
| canonical_fact_quarantine_v2 | chronic_lymphocytic_thyroiditis | 1 | 1 |
| canonical_fact_quarantine_v2 | surgical_planning | 1 | 1 |
| canonical_fact_quarantine_v2 | afirma | 1 | 1 |
| canonical_fact_quarantine_v2 | tg_trend | 1 | 1 |
| canonical_fact_quarantine_v2 | respiratory | 1 | 1 |
| canonical_fact_quarantine_v2 | cervical_back_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | heart_failure | 1 | 1 |
| canonical_fact_quarantine_v2 | tirads_component_margin | 1 | 1 |
| canonical_fact_quarantine_v2 | closure | 1 | 1 |
| canonical_fact_quarantine_v2 | plan_ct_neck | 1 | 1 |
| canonical_fact_quarantine_v2 | cancer_stage | 1 | 1 |
| canonical_fact_quarantine_v2 | no cervical or supraclavicular lymphadenopathy | 1 | 1 |
| canonical_fact_quarantine_v2 | allergy | 1 | 1 |
| canonical_fact_quarantine_v2 | heart_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | BP | 1 | 1 |
| canonical_fact_quarantine_v2 | bethesda_category | 1 | 1 |
| canonical_fact_quarantine_v2 | abdomen_soft | 1 | 1 |
| canonical_fact_quarantine_v2 | lymph-25, 2023-07-25. 2023-07-25 is the date of the procedure. The entity_date is 2023-07-25. The date_confidence is 1.0 because it's the procedure date. The date_source_keyword is  | 1 | 1 |
| canonical_fact_quarantine_v2 | parathyroid_frozen_section | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_size_left_lobe | 1 | 1 |
| canonical_fact_quarantine_v2 | thyroid_imaging | 1 | 1 |
| canonical_fact_quarantine_v2 | implant_site | 1 | 1 |
| canonical_fact_quarantine_v2 | nerve_integrity_monitor | 1 | 1 |
| canonical_fact_quarantine_v2 | neck_soreness | 1 | 1 |
| canonical_fact_quarantine_v2 | Cervical lymph nodes | 1 | 1 |
| canonical_fact_quarantine_v2 | chest_exam | 1 | 1 |
| canonical_fact_quarantine_v2 | metastasis | 1 | 1 |
| canonical_fact_quarantine_v2 | imaging_findings | 1 | 1 |
| canonical_fact_quarantine_v2 | molecular_marker | 1 | 1 |
