# THYROID_2026 — Data Dictionary
## canonical_patient_master_v1
- **Rows:** 10,871 (one per patient)
- **Columns:** 1374
- **Database:** thyroid_ete_fix_20260413

| Coverage tier | Count |
|---------------|-------|
| 100% coverage | 149 |
| >75% coverage | 198 |
| >50% coverage | 210 |
| <10% coverage | 629 |


### Source: clinical_note_ln_patient_rollup_v1 (36 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| cnln_total_entities | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_n_modalities | INTEGER | 100.0 | Clinical note lymph node integration |
| cnln_any_positive_any_modality | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_ene_any_modality | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_modalities_present | VARCHAR | 13.2 | Clinical note lymph node integration |
| cnln_earliest_date | VARCHAR | 13.2 | Clinical note lymph node integration |
| cnln_latest_date | VARCHAR | 13.2 | Clinical note lymph node integration |
| cnln_surg_n_entities | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_surg_n_notes | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_surg_any_positive | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_surg_max_positive_count | DOUBLE | 4.7 | Clinical note lymph node integration |
| cnln_surg_max_total_examined | DOUBLE | 1.5 | Clinical note lymph node integration |
| cnln_surg_ene_any | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_surg_bilateral | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_surg_levels_mentioned | VARCHAR | 10.2 | Clinical note lymph node integration |
| cnln_surg_first_date | VARCHAR | 11.4 | Clinical note lymph node integration |
| cnln_surg_last_date | VARCHAR | 11.4 | Clinical note lymph node integration |
| cnln_surg_source_note_types | VARCHAR | 11.4 | Clinical note lymph node integration |
| cnln_surg_avg_confidence | DOUBLE | 11.4 | Clinical note lymph node integration |
| cnln_img_n_entities | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_img_any_suspicious | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_img_max_size_cm | DOUBLE | 0.5 | Clinical note lymph node integration |
| cnln_img_laterality | VARCHAR | 2.5 | Clinical note lymph node integration |
| cnln_img_levels_mentioned | VARCHAR | 2.2 | Clinical note lymph node integration |
| cnln_img_first_date | VARCHAR | 3.0 | Clinical note lymph node integration |
| cnln_img_last_date | VARCHAR | 3.0 | Clinical note lymph node integration |
| cnln_img_avg_confidence | DOUBLE | 3.0 | Clinical note lymph node integration |
| cnln_path_n_entities | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_path_any_positive | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_path_max_positive_count | DOUBLE | 0.8 | Clinical note lymph node integration |
| cnln_path_ene_any | BOOLEAN | 0.3 | Clinical note lymph node integration |
| cnln_clin_n_entities | BIGINT | 100.0 | Clinical note lymph node integration |
| cnln_clin_any_positive | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_clin_avg_confidence | DOUBLE | 1.3 | Clinical note lymph node integration |
| cnln_novel_positive_flag | BOOLEAN | 100.0 | Clinical note lymph node integration |
| cnln_source_table | VARCHAR | 100.0 | Clinical note lymph node integration |

### Source: clinical_notes_long (NLP PMH) (64 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| pmhx_llm_extraction_method | VARCHAR | 2.7 | Past medical history |
| pmhx_llm_mean_confidence | DOUBLE | 2.7 | Past medical history |
| pmhx_llm_min_confidence | DOUBLE | 2.7 | Past medical history |
| pmhx_llm_n_source_notes | BIGINT | 2.7 | Past medical history |
| pmhx_llm_note_types | VARCHAR | 2.7 | Past medical history |
| pmhx_nlp_afib | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_afib_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_asthma | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_asthma_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_autoimmune_thyroid_hx | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_autoimmune_thyroid_hx_n_mentions | BIGINT | 2.7 | Past medical history |
| pmhx_nlp_breast_cancer | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_breast_cancer_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_cad | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_cad_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_ckd | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_ckd_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_coagulopathy | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_comorbidity_list | VARCHAR | 35.8 | Past medical history |
| pmhx_nlp_copd | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_copd_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_depression | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_depression_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_diabetes | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_diabetes_first_date | DATE | 4.8 | Past medical history |
| pmhx_nlp_diabetes_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_extraction_method | VARCHAR | 35.8 | Past medical history |
| pmhx_nlp_family_hx_cancer | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_family_hx_thyroid | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_family_hx_thyroid_n_mentions | BIGINT | 2.7 | Past medical history |
| pmhx_nlp_gerd | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_gerd_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_hypertension | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_hypertension_first_date | DATE | 6.3 | Past medical history |
| pmhx_nlp_hypertension_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_hyperthyroidism | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_hyperthyroidism_first_date | DATE | 6.1 | Past medical history |
| pmhx_nlp_hyperthyroidism_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_hypothyroidism | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_hypothyroidism_first_date | DATE | 9.8 | Past medical history |
| pmhx_nlp_hypothyroidism_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_lung_cancer | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_lung_cancer_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_men_syndrome | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_n_comorbidities | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_n_source_notes | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_note_types | VARCHAR | 35.8 | Past medical history |
| pmhx_nlp_obesity | BOOLEAN | 35.8 | Past medical history |
| pmhx_nlp_obesity_first_date | DATE | 2.1 | Past medical history |
| pmhx_nlp_obesity_n_mentions | BIGINT | 35.8 | Past medical history |
| pmhx_nlp_osteoporosis | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_prior_cancer_hx | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_prior_cancer_hx_n_mentions | BIGINT | 2.7 | Past medical history |
| pmhx_nlp_radiation_exposure | BOOLEAN | 2.7 | Past medical history |
| pmhx_nlp_radiation_exposure_confidence | DOUBLE | 0.3 | Past medical history |
| pmhx_nlp_radiation_exposure_date | DATE | 0.3 | Past medical history |
| pmhx_nlp_radiation_exposure_n_mentions | BIGINT | 2.7 | Past medical history |
| pmhx_nlp_smoking_status | VARCHAR | 0.2 | Past medical history |
| pmhx_nlp_diabetes_first_days_from_surg | INTEGER | 3.6 | Days from first surgery date (negative = before surgery) |
| pmhx_nlp_hypertension_first_days_from_surg | INTEGER | 3.4 | Days from first surgery date (negative = before surgery) |
| pmhx_nlp_hyperthyroidism_first_days_from_surg | INTEGER | 2.3 | Days from first surgery date (negative = before surgery) |
| pmhx_nlp_hypothyroidism_first_days_from_surg | INTEGER | 6.2 | Days from first surgery date (negative = before surgery) |
| pmhx_nlp_obesity_first_days_from_surg | INTEGER | 1.4 | Days from first surgery date (negative = before surgery) |
| pmhx_nlp_radiation_exposure_days_from_surg | INTEGER | 0.3 | Days from first surgery date (negative = before surgery) |

### Source: clinical_notes_long (NLP PSH) (20 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| pshx_llm_extraction_method | VARCHAR | 17.1 | Past surgical history |
| pshx_llm_mean_confidence | DOUBLE | 17.1 | Past surgical history |
| pshx_llm_min_confidence | DOUBLE | 17.1 | Past surgical history |
| pshx_llm_n_source_notes | BIGINT | 17.1 | Past surgical history |
| pshx_llm_note_types | VARCHAR | 17.1 | Past surgical history |
| pshx_nlp_n_prior_procedures | BIGINT | 17.1 | Past surgical history |
| pshx_nlp_prior_fna | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_fna_n_mentions | BIGINT | 17.1 | Past surgical history |
| pshx_nlp_prior_neck_dissection | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_neck_surgery | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_neck_surgery_n_mentions | BIGINT | 17.1 | Past surgical history |
| pshx_nlp_prior_parathyroidectomy | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_rai | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_rai_date | DATE | 2.2 | Past surgical history |
| pshx_nlp_prior_rai_n_mentions | BIGINT | 17.1 | Past surgical history |
| pshx_nlp_prior_thyroidectomy | BOOLEAN | 17.1 | Past surgical history |
| pshx_nlp_prior_thyroidectomy_date | DATE | 7.2 | Past surgical history |
| pshx_nlp_prior_thyroidectomy_n_mentions | BIGINT | 17.1 | Past surgical history |
| pshx_nlp_prior_rai_days_from_surg | INTEGER | 2.1 | Days from first surgery date (negative = before surgery) |
| pshx_nlp_prior_thyroidectomy_days_from_surg | INTEGER | 6.7 | Days from first surgery date (negative = before surgery) |

### Source: clinical_notes_long (NLP medications) (15 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| med_nlp_calcitriol | BOOLEAN | 18.5 | NLP-extracted medication data |
| med_nlp_calcitriol_date | DATE | 0.7 | NLP-extracted medication data |
| med_nlp_calcitriol_n_mentions | BIGINT | 18.5 | NLP-extracted medication data |
| med_nlp_calcium_supplement | BOOLEAN | 18.5 | NLP-extracted medication data |
| med_nlp_calcium_supplement_date | DATE | 1.1 | NLP-extracted medication data |
| med_nlp_calcium_supplement_n_mentions | BIGINT | 18.5 | NLP-extracted medication data |
| med_nlp_extraction_method | VARCHAR | 18.5 | NLP-extracted medication data |
| med_nlp_levothyroxine | BOOLEAN | 18.5 | NLP-extracted medication data |
| med_nlp_levothyroxine_date | DATE | 8.4 | NLP-extracted medication data |
| med_nlp_levothyroxine_n_mentions | BIGINT | 18.5 | NLP-extracted medication data |
| med_nlp_n_source_notes | BIGINT | 18.5 | NLP-extracted medication data |
| med_nlp_note_types | VARCHAR | 18.5 | NLP-extracted medication data |
| med_nlp_calcitriol_days_from_surg | INTEGER | 0.7 | Days from first surgery date (negative = before surgery) |
| med_nlp_calcium_supplement_days_from_surg | INTEGER | 1.0 | Days from first surgery date (negative = before surgery) |
| med_nlp_levothyroxine_days_from_surg | INTEGER | 6.5 | Days from first surgery date (negative = before surgery) |

### Source: clinical_notes_long (NLP operative) (44 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| op_nlp_berry_ligament_date | DATE | 4.1 | NLP-extracted from operative notes |
| op_nlp_berry_ligament_dissected | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_berry_ligament_mentioned | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_berry_ligament_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_drain_date | DATE | 2.5 | NLP-extracted from operative notes |
| op_nlp_drain_placed | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_drain_placed_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_ebl_date | DATE | 7.9 | NLP-extracted from operative notes |
| op_nlp_ebl_ml | DOUBLE | 14.8 | NLP-extracted from operative notes |
| op_nlp_ebl_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_esophageal_involvement | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_esophageal_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_extraction_method | VARCHAR | 37.1 | NLP-extracted from operative notes |
| op_nlp_gross_invasion | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_intraop_complication | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_intraop_complication_date | DATE | 0.1 | NLP-extracted from operative notes |
| op_nlp_intraop_complication_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_n_source_notes | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_nerve_monitoring_date | DATE | 24.0 | NLP-extracted from operative notes |
| op_nlp_nerve_monitoring_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_nerve_monitoring_type | VARCHAR | 32.1 | NLP-extracted from operative notes |
| op_nlp_nerve_monitoring_used | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_note_types | VARCHAR | 37.1 | NLP-extracted from operative notes |
| op_nlp_parathyroid_autograft | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_parathyroid_autograft_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_parathyroid_date | DATE | 7.1 | NLP-extracted from operative notes |
| op_nlp_parathyroid_managed | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_parathyroid_managed_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_reoperative_field | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_reoperative_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_rln_finding | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_rln_finding_date | DATE | 6.9 | NLP-extracted from operative notes |
| op_nlp_rln_finding_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_strap_muscle_involved | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_strap_muscle_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_tracheal_involvement | BOOLEAN | 37.1 | NLP-extracted from operative notes |
| op_nlp_tracheal_n_mentions | BIGINT | 37.1 | NLP-extracted from operative notes |
| op_nlp_berry_ligament_days_from_surg | INTEGER | 1.5 | Days from first surgery date (negative = before surgery) |
| op_nlp_drain_days_from_surg | INTEGER | 0.8 | Days from first surgery date (negative = before surgery) |
| op_nlp_ebl_days_from_surg | INTEGER | 4.0 | Days from first surgery date (negative = before surgery) |
| op_nlp_intraop_complication_days_from_surg | INTEGER | 0.1 | Days from first surgery date (negative = before surgery) |
| op_nlp_nerve_monitoring_days_from_surg | INTEGER | 7.9 | Days from first surgery date (negative = before surgery) |
| op_nlp_parathyroid_days_from_surg | INTEGER | 1.8 | Days from first surgery date (negative = before surgery) |
| op_nlp_rln_finding_days_from_surg | INTEGER | 2.9 | Days from first surgery date (negative = before surgery) |

### Source: complication_phenotype_v1 (74 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| comp_chyle_leak_confirmed | BOOLEAN | 14.6 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_days_postop | BIGINT | 14.2 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_evidence_tier | INTEGER | 0.2 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_permanent | BOOLEAN | 14.6 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_suspected | BOOLEAN | 14.6 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_timing_window | VARCHAR | 14.6 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_transient | BOOLEAN | 14.6 | Complication status from complication_phenotype_v1 |
| comp_chyle_leak_treatment_req | BOOLEAN | 14.6 | Complication status from complication_phenotype_v1 |
| comp_hematoma_confirmed | BOOLEAN | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hematoma_days_postop | BIGINT | 0.5 | Complication status from complication_phenotype_v1 |
| comp_hematoma_evidence_tier | INTEGER | 0.5 | Complication status from complication_phenotype_v1 |
| comp_hematoma_permanent | BOOLEAN | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hematoma_suspected | BOOLEAN | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hematoma_timing_window | VARCHAR | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hematoma_transient | BOOLEAN | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hematoma_treatment_req | BOOLEAN | 2.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_confirmed | BOOLEAN | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_days_postop | BIGINT | 6.2 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_evidence_tier | INTEGER | 0.8 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_permanent | BOOLEAN | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_suspected | BOOLEAN | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_timing_window | VARCHAR | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_transient | BOOLEAN | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypocalcemia_treatment_req | BOOLEAN | 17.3 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_confirmed | BOOLEAN | 4.0 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_days_postop | BIGINT | 0.6 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_evidence_tier | INTEGER | 0.6 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_permanent | BOOLEAN | 4.0 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_suspected | BOOLEAN | 4.0 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_timing_window | VARCHAR | 4.0 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_transient | BOOLEAN | 4.0 | Complication status from complication_phenotype_v1 |
| comp_hypoparathyroidism_treatment_req | BOOLEAN | 4.0 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_confirmed | BOOLEAN | 6.7 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_days_postop | BIGINT | 0.9 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_evidence_tier | INTEGER | 0.8 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_permanent | BOOLEAN | 6.7 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_suspected | BOOLEAN | 6.7 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_timing_window | VARCHAR | 6.7 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_transient | BOOLEAN | 6.7 | Complication status from complication_phenotype_v1 |
| comp_rln_injury_treatment_req | BOOLEAN | 6.7 | Complication status from complication_phenotype_v1 |
| comp_seroma_confirmed | BOOLEAN | 8.0 | Complication status from complication_phenotype_v1 |
| comp_seroma_days_postop | BIGINT | 5.5 | Complication status from complication_phenotype_v1 |
| comp_seroma_evidence_tier | INTEGER | 0.3 | Complication status from complication_phenotype_v1 |
| comp_seroma_permanent | BOOLEAN | 8.0 | Complication status from complication_phenotype_v1 |
| comp_seroma_suspected | BOOLEAN | 8.0 | Complication status from complication_phenotype_v1 |
| comp_seroma_timing_window | VARCHAR | 8.0 | Complication status from complication_phenotype_v1 |
| comp_seroma_transient | BOOLEAN | 8.0 | Complication status from complication_phenotype_v1 |
| comp_seroma_treatment_req | BOOLEAN | 8.0 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_confirmed | BOOLEAN | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_days_postop | BIGINT | 0.2 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_evidence_tier | INTEGER | 0.0 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_permanent | BOOLEAN | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_suspected | BOOLEAN | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_timing_window | VARCHAR | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_transient | BOOLEAN | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paralysis_treatment_req | BOOLEAN | 0.8 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_confirmed | BOOLEAN | 0.7 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_days_postop | BIGINT | 0.2 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_evidence_tier | INTEGER | 0.0 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_permanent | BOOLEAN | 0.7 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_suspected | BOOLEAN | 0.7 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_timing_window | VARCHAR | 0.7 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_transient | BOOLEAN | 0.7 | Complication status from complication_phenotype_v1 |
| comp_vc_paresis_treatment_req | BOOLEAN | 0.7 | Complication status from complication_phenotype_v1 |
| comp_voice_permanence_noted | BOOLEAN | 26.6 | Complication status from complication_phenotype_v1 |
| comp_voice_resolution_noted | BOOLEAN | 26.6 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_confirmed | BOOLEAN | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_days_postop | BIGINT | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_evidence_tier | INTEGER | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_permanent | BOOLEAN | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_suspected | BOOLEAN | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_timing_window | VARCHAR | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_transient | BOOLEAN | 0.1 | Complication status from complication_phenotype_v1 |
| comp_wound_infection_treatment_req | BOOLEAN | 0.1 | Complication status from complication_phenotype_v1 |

### Source: ct_imaging (29 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| ct_goiter_present_any | BOOLEAN | 21.8 | CT imaging data |
| ct_largest_ln_short_axis_mm | DOUBLE | 8.5 | CT imaging data |
| ct_ln_enlarged_any | BOOLEAN | 28.3 | CT imaging data |
| ct_ln_suspicious_any | BOOLEAN | 28.3 | CT imaging data |
| ct_n_exams | BIGINT | 28.4 | CT imaging data |
| ct_substernal_extension_any | BOOLEAN | 16.6 | CT imaging data |
| ct_tracheal_deviation_any | BOOLEAN | 28.4 | CT imaging data |
| ct_tracheal_narrowing_any | BOOLEAN | 28.4 | CT imaging data |
| ct_indication_first | VARCHAR | 27.9 | CT imaging data |
| ct_indication_last | VARCHAR | 27.9 | CT imaging data |
| ct_first_date | DATE | 27.8 | CT imaging data |
| ct_last_date | DATE | 27.8 | CT imaging data |
| ct_exam_type_first | VARCHAR | 27.9 | CT imaging data |
| ct_contrast_first | VARCHAR | 27.9 | CT imaging data |
| ct_thyroid_details_last | VARCHAR | 20.7 | CT imaging data |
| ct_ln_details_last | VARCHAR | 20.9 | CT imaging data |
| ct_ln_locations_last | VARCHAR | 8.3 | CT imaging data |
| ct_airway_compromise_any | BOOLEAN | 27.9 | CT imaging data |
| ct_airway_comment_last | VARCHAR | 9.1 | CT imaging data |
| ct_thyroid_postsurgical_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_not_visualized_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_heterogeneous_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_other_abnormality_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_normal_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_nodule_any | BOOLEAN | 27.9 | CT imaging data |
| ct_thyroid_enlarged_any | BOOLEAN | 27.9 | CT imaging data |
| ct_pathologic_ln_any | BOOLEAN | 25.9 | CT imaging data |
| ct_first_days_from_surg | INTEGER | 16.3 | Days from first surgery date (negative = before surgery) |
| ct_last_days_from_surg | INTEGER | 16.3 | Days from first surgery date (negative = before surgery) |

### Source: ct_imaging (PET subset) (24 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| pet_distant_met_sites | VARCHAR | 1.2 | PET/CT imaging data |
| pet_distant_mets_ever | BOOLEAN | 2.6 | PET/CT imaging data |
| pet_fdg_avid_cervical_ln_ever | BOOLEAN | 2.6 | PET/CT imaging data |
| pet_fdg_avid_thyroid_bed_ever | BOOLEAN | 2.4 | PET/CT imaging data |
| pet_first_date | VARCHAR | 2.7 | PET/CT imaging data |
| pet_has_data | BOOLEAN | 3.1 | PET/CT imaging data |
| pet_impression_last | VARCHAR | 2.6 | PET/CT imaging data |
| pet_indication_first | VARCHAR | 2.7 | PET/CT imaging data |
| pet_last_date | VARCHAR | 2.7 | PET/CT imaging data |
| pet_n_exams | BIGINT | 2.7 | PET/CT imaging data |
| pet_overall_worst | VARCHAR | 2.7 | PET/CT imaging data |
| pet_radiotracer_primary | VARCHAR | 2.6 | PET/CT imaging data |
| pet_suv_max_cervical_ln | DOUBLE | 0.9 | PET/CT imaging data |
| pet_suv_max_thyroid_bed | DOUBLE | 1.6 | PET/CT imaging data |
| pet_other_n_exams | INTEGER | 0.7 | PET/CT imaging data |
| pet_other_first_date | DATE | 0.6 | PET/CT imaging data |
| pet_other_last_date | DATE | 0.6 | PET/CT imaging data |
| pet_other_indication_first | VARCHAR | 0.7 | PET/CT imaging data |
| pet_other_mentions_metastasis | BOOLEAN | 0.7 | PET/CT imaging data |
| pet_other_ned_statement | BOOLEAN | 0.7 | PET/CT imaging data |
| pet_other_exam_type | VARCHAR | 0.7 | PET/CT imaging data |
| pet_other_extraction_method | VARCHAR | 0.7 | PET/CT imaging data |
| pet_other_first_days_from_surg | INTEGER | 0.3 | Days from first surgery date (negative = before surgery) |
| pet_other_last_days_from_surg | INTEGER | 0.3 | Days from first surgery date (negative = before surgery) |

### Source: extracted_ete_subgraded_v1 (10 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| ete_grade | VARCHAR | 37.5 | From extracted_ete_subgraded_v1 |
| ete_grade_source | VARCHAR | 100.0 | From extracted_ete_subgraded_v1 |
| ete_op_note_confidence | VARCHAR | 1.7 | From extracted_ete_subgraded_v1 |
| ete_op_note_grade | VARCHAR | 1.7 | From extracted_ete_subgraded_v1 |
| ete_original_grade | VARCHAR | 32.7 | From extracted_ete_subgraded_v1 |
| ete_original_source | VARCHAR | 32.7 | From extracted_ete_subgraded_v1 |
| ete_refined_grade | VARCHAR | 32.7 | From extracted_ete_subgraded_v1 |
| ete_subgrade_method | VARCHAR | 32.7 | From extracted_ete_subgraded_v1 |
| ete_subgrade_note | VARCHAR | 1.7 | From extracted_ete_subgraded_v1 |
| ete_grade_final | VARCHAR | 37.5 | From extracted_ete_subgraded_v1 |

### Source: extracted_tirads_validated_v1 / tirads_llm_extracted_v2 (15 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| tirads_best_category_v12 | VARCHAR | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_best_combined | INTEGER | 31.6 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_best_score_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_concordant_count_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_has_acr_recalc_v12 | BOOLEAN | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_mismatch_count_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_n_nodule_records_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_n_sources_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_nodule_size_max_mm_v12 | DOUBLE | 31.6 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_nodules_scored_combined | BIGINT | 31.6 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_reliability_v12 | DOUBLE | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_source_v12 | VARCHAR | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_worst_category_v12 | VARCHAR | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_worst_combined | INTEGER | 31.6 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |
| tirads_worst_score_v12 | BIGINT | 32.0 | From extracted_tirads_validated_v1 / tirads_llm_extracted_v2 |

### Source: fna_cytology (9 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| bethesda_2010 | BIGINT | 47.9 | From fna_cytology |
| bethesda_2015 | BIGINT | 47.9 | From fna_cytology |
| bethesda_2023 | BIGINT | 47.9 | From fna_cytology |
| bethesda_category | VARCHAR | 48.3 | From fna_cytology |
| bethesda_confidence | DOUBLE | 48.3 | From fna_cytology |
| bethesda_final | BIGINT | 48.3 | From fna_cytology |
| bethesda_final_name | VARCHAR | 48.3 | From fna_cytology |
| bethesda_num | DOUBLE | 48.3 | From fna_cytology |
| bethesda_source | VARCHAR | 48.3 | From fna_cytology |

### Source: gold_master_patient_facts_v1 (554 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| age_at_surgery | BIGINT | 100.0 | BIGINT field |
| ages_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ages_score | DOUBLE | 100.0 | DOUBLE field |
| aggressive_variant_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ajcc8_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ajcc8_m_stage | VARCHAR | 100.0 | VARCHAR field |
| ajcc8_missing_components | VARCHAR | 62.5 | VARCHAR field |
| ajcc8_n_stage | VARCHAR | 48.5 | VARCHAR field |
| ajcc8_stage_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ajcc8_stage_group | VARCHAR | 37.6 | VARCHAR field |
| ajcc8_t_stage | VARCHAR | 37.6 | VARCHAR field |
| ajcc8_t_stage_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| alk_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| ames_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ames_risk | VARCHAR | 100.0 | VARCHAR field |
| ames_risk_group | VARCHAR | 100.0 | VARCHAR field |
| analysis_eligible_flag | BOOLEAN | 100.0 | BOOLEAN field |
| anti_tg_nadir | DOUBLE | 12.7 | DOUBLE field |
| anti_tg_rising_flag | BOOLEAN | 12.7 | BOOLEAN field |
| any_analysis_eligible_complication | BOOLEAN | 26.6 | BOOLEAN field |
| any_confirmed_complication | BOOLEAN | 100.0 | BOOLEAN field |
| any_confirmed_complication_flag | BOOLEAN | 26.6 | BOOLEAN field |
| any_fusion_positive | BOOLEAN | 92.2 | BOOLEAN field |
| any_recurrence_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ata_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ata_initial_risk | VARCHAR | 28.9 | VARCHAR field |
| ata_response_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ata_response_category | VARCHAR | 0.3 | VARCHAR field |
| ata_response_is_provisional | BOOLEAN | 100.0 | BOOLEAN field |
| ata_risk_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| ata_risk_category | VARCHAR | 28.9 | VARCHAR field |
| best_ene_grade | VARCHAR | 14.7 | VARCHAR field |
| bilateral_disease_flag | BOOLEAN | 31.6 | BOOLEAN field |
| biochemical_recurrence_flag | BOOLEAN | 17.9 | BOOLEAN field |
| biochemical_tg_at_recurrence | DOUBLE | 6.1 | DOUBLE field |
| biochemical_tg_nadir_after_surgery | DOUBLE | 6.1 | DOUBLE field |
| braf_detection_method | VARCHAR | 3.5 | VARCHAR field |
| braf_detection_method_v11 | VARCHAR | 3.5 | VARCHAR field |
| braf_positive | BOOLEAN | 100.0 | BOOLEAN field |
| braf_positive_final | BOOLEAN | 100.0 | BOOLEAN field |
| braf_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| braf_recovered_status_v11 | VARCHAR | 3.5 | VARCHAR field |
| braf_recovered_variant_v11 | VARCHAR | 2.5 | VARCHAR field |
| braf_source | VARCHAR | 92.2 | VARCHAR field |
| braf_status_v7 | VARCHAR | 92.2 | VARCHAR field |
| braf_variant | VARCHAR | 1.6 | VARCHAR field |
| calcium_nadir | DOUBLE | 0.0 | DOUBLE field |
| calcium_nadir_30d | DOUBLE | 0.2 | DOUBLE field |
| calcium_nadir_days_postop | INTEGER | 0.3 | INTEGER field |
| calcium_supplement_required | BOOLEAN | 100.0 | BOOLEAN field |
| capsular_invasion_refined | VARCHAR | 11.0 | VARCHAR field |
| capsular_invasion_v6 | VARCHAR | 11.3 | VARCHAR field |
| chyle_leak_status | VARCHAR | 100.0 | VARCHAR field |
| closest_margin_mm | DOUBLE | 8.1 | DOUBLE field |
| completion_braf_positive | BOOLEAN | 5.6 | BOOLEAN field |
| completion_histology_type | VARCHAR | 1.7 | VARCHAR field |
| completion_prior_histology | VARCHAR | 3.5 | VARCHAR field |
| completion_reason | VARCHAR | 6.3 | VARCHAR field |
| completion_reason_confidence | DOUBLE | 6.3 | DOUBLE field |
| completion_t_stage | VARCHAR | 1.0 | VARCHAR field |
| completion_tert_positive | BOOLEAN | 5.6 | BOOLEAN field |
| confirmed_rai_episodes | BIGINT | 7.9 | BIGINT field |
| cross_fna_concordance | VARCHAR | 48.3 | VARCHAR field |
| date_traceability_status | VARCHAR | 100.0 | VARCHAR field |
| days_first_to_last_tg | BIGINT | 23.2 | BIGINT field |
| days_to_first_laryngoscopy | BIGINT | 0.2 | BIGINT field |
| days_to_last_laryngoscopy | BIGINT | 0.2 | BIGINT field |
| demo_confidence | INTEGER | 100.0 | INTEGER field |
| demo_source | VARCHAR | 100.0 | VARCHAR field |
| diagnosis_confidence | VARCHAR | 100.0 | VARCHAR field |
| diagnosis_full | VARCHAR | 100.0 | VARCHAR field |
| diagnosis_primary | VARCHAR | 100.0 | VARCHAR field |
| diagnosis_variant | VARCHAR | 13.9 | VARCHAR field |
| distant_mets_proxy | BOOLEAN | 100.0 | BOOLEAN field |
| dominant_nodule_size_cm | DOUBLE | 31.6 | DOUBLE field |
| earliest_complication_days | BIGINT | 0.6 | BIGINT field |
| eif1ax_positive | BOOLEAN | 92.2 | BOOLEAN field |
| ene_ct | VARCHAR | 3.4 | VARCHAR field |
| ene_deposit_cm | DOUBLE | 0.1 | DOUBLE field |
| ene_grade_v9 | VARCHAR | 11.6 | VARCHAR field |
| ene_levels_v9 | VARCHAR | 0.4 | VARCHAR field |
| ene_n_sources | BIGINT | 14.7 | BIGINT field |
| ene_op_intraop | VARCHAR | 0.1 | VARCHAR field |
| ene_path_ct_concordance | VARCHAR | 14.7 | VARCHAR field |
| ene_path_levels | VARCHAR | 0.2 | VARCHAR field |
| ene_path_nlp | VARCHAR | 0.2 | VARCHAR field |
| ene_path_synoptic | VARCHAR | 11.6 | VARCHAR field |
| ene_pet | VARCHAR | 0.6 | VARCHAR field |
| ene_positive | BOOLEAN | 11.6 | BOOLEAN field |
| ene_rai_scan | VARCHAR | 1.8 | VARCHAR field |
| ene_record_count_v9 | BIGINT | 11.6 | BIGINT field |
| ene_us | VARCHAR | 4.3 | VARCHAR field |
| first_recurrence_date | TIMESTAMP | 0.5 | TIMESTAMP field |
| first_surgery_date | TIMESTAMP | 80.3 | TIMESTAMP field |
| first_tg_date | DATE | 23.2 | DATE field |
| fna_bethesda_confidence | DOUBLE | 48.3 | DOUBLE field |
| fna_bethesda_source | VARCHAR | 48.3 | VARCHAR field |
| fna_confidence | DOUBLE | 48.3 | DOUBLE field |
| fna_path_concordance_category | VARCHAR | 48.3 | VARCHAR field |
| fna_path_concordant | BOOLEAN | 48.3 | BOOLEAN field |
| fna_path_outcome | VARCHAR | 100.0 | VARCHAR field |
| followup_category | VARCHAR | 100.0 | VARCHAR field |
| followup_completeness_score | INTEGER | 100.0 | INTEGER field |
| followup_days | BIGINT | 100.0 | BIGINT field |
| followup_years | DOUBLE | 100.0 | DOUBLE field |
| gm_lab_completeness_score | INTEGER | 100.0 | From gold_master_patient_facts_v1 |
| gm_macis_calculable_flag | BOOLEAN | 100.0 | From gold_master_patient_facts_v1 |
| gm_path_ene_raw | VARCHAR | 11.3 | From gold_master_patient_facts_v1 |
| gm_path_ete_raw | VARCHAR | 37.5 | From gold_master_patient_facts_v1 |
| gm_path_lvi_raw | VARCHAR | 31.0 | From gold_master_patient_facts_v1 |
| gm_path_m_stage_raw | VARCHAR | 36.8 | From gold_master_patient_facts_v1 |
| gm_path_pni_raw | VARCHAR | 13.2 | From gold_master_patient_facts_v1 |
| gm_path_stage_raw | VARCHAR | 0.0 | From gold_master_patient_facts_v1 |
| gm_path_vascular_inv_raw | VARCHAR | 33.9 | From gold_master_patient_facts_v1 |
| gm_provenance_confidence | INTEGER | 100.0 | From gold_master_patient_facts_v1 |
| gm_rai_date_confidence | VARCHAR | 5.3 | From gold_master_patient_facts_v1 |
| gm_rai_date_source | VARCHAR | 5.3 | From gold_master_patient_facts_v1 |
| gm_recurrence_date_source | VARCHAR | 1.7 | From gold_master_patient_facts_v1 |
| gm_recurrence_site_primary | VARCHAR | 0.0 | From gold_master_patient_facts_v1 |
| gm_recurrence_source | VARCHAR | 17.9 | From gold_master_patient_facts_v1 |
| gm_recurrence_type_primary | VARCHAR | 17.9 | From gold_master_patient_facts_v1 |
| gm_tg_below_threshold_ever | BOOLEAN | 23.6 | From gold_master_patient_facts_v1 |
| gross_ete_flag | BOOLEAN | 100.0 | BOOLEAN field |
| has_low_calcium_flag | BOOLEAN | 26.6 | BOOLEAN field |
| has_low_pth_flag | BOOLEAN | 26.6 | BOOLEAN field |
| has_suspicious_candidate | BOOLEAN | 56.4 | BOOLEAN field |
| has_voice_data | BOOLEAN | 100.0 | BOOLEAN field |
| hematoma_status | VARCHAR | 100.0 | VARCHAR field |
| high_risk_molecular_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| histology_final | VARCHAR | 38.1 | VARCHAR field |
| histology_source | VARCHAR | 100.0 | VARCHAR field |
| hras_positive_v11 | BOOLEAN | 3.2 | BOOLEAN field |
| hypocalcemia_status | VARCHAR | 100.0 | VARCHAR field |
| hypoparathyroidism_status | VARCHAR | 100.0 | VARCHAR field |
| ihc_braf_confidence_v13 | DOUBLE | 0.0 | DOUBLE field |
| ihc_braf_note_type_v13 | VARCHAR | 0.0 | VARCHAR field |
| ihc_braf_result_v13 | VARCHAR | 0.0 | VARCHAR field |
| imaging_ln_abnormal | BOOLEAN | 100.0 | BOOLEAN field |
| imaging_n_nodule_records | BIGINT | 32.0 | BIGINT field |
| imaging_nodule_size_cm_v11 | DOUBLE | 28.1 | DOUBLE field |
| imaging_suspicious_unconfirmed | BOOLEAN | 100.0 | BOOLEAN field |
| imaging_tirads_source | VARCHAR | 32.0 | VARCHAR field |
| is_malignant | BOOLEAN | 100.0 | BOOLEAN field |
| kras_positive_v11 | BOOLEAN | 3.2 | BOOLEAN field |
| last_contact_date | TIMESTAMP | 100.0 | TIMESTAMP field |
| last_contact_source | VARCHAR | 100.0 | VARCHAR field |
| last_tg_date | DATE | 23.2 | DATE field |
| lateral_detection_method | VARCHAR | 1.1 | VARCHAR field |
| lateral_levels_v10 | VARCHAR | 0.8 | VARCHAR field |
| lateral_neck_dissected_v10 | BOOLEAN | 100.0 | BOOLEAN field |
| lateral_side_v10 | VARCHAR | 0.9 | VARCHAR field |
| lateral_source_v10 | VARCHAR | 1.1 | VARCHAR field |
| laterality | VARCHAR | 95.1 | VARCHAR field |
| ln_burden_band | VARCHAR | 35.1 | VARCHAR field |
| ln_ene_status | VARCHAR | 11.3 | VARCHAR field |
| ln_lateral_dissected | BOOLEAN | 100.0 | BOOLEAN field |
| ln_level_i_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_i_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_ii_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_ii_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_iii_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_iii_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_iv_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_iv_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_v_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_v_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_vi_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_vi_positive | BIGINT | 36.7 | BIGINT field |
| ln_level_vii_examined | BIGINT | 36.7 | BIGINT field |
| ln_level_vii_positive | BIGINT | 36.7 | BIGINT field |
| ln_mets_atc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_ene_count | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_ftc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_hurthle | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_micrometastasis | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_mtc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_pdtc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_mets_ptc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_positive_flag | INTEGER | 37.3 | INTEGER field |
| ln_ratio | DOUBLE | 35.1 | DOUBLE field |
| ln_rollup_any_positive | BOOLEAN | 34.6 | BOOLEAN field |
| ln_rollup_bilateral_lateral_examined | BIGINT | 36.7 | BIGINT field |
| ln_rollup_bilateral_lateral_positive | BIGINT | 36.7 | BIGINT field |
| ln_rollup_central_examined | BIGINT | 36.7 | BIGINT field |
| ln_rollup_central_positive | BIGINT | 36.7 | BIGINT field |
| ln_rollup_crossval_status | VARCHAR | 36.7 | VARCHAR field |
| ln_rollup_ene | BIGINT | 10.9 | BIGINT field |
| ln_rollup_has_per_level_data | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_internal_consistency | VARCHAR | 36.7 | VARCHAR field |
| ln_rollup_largest_deposit_cm | DOUBLE | 7.0 | DOUBLE field |
| ln_rollup_lateral_left_examined | BIGINT | 36.7 | BIGINT field |
| ln_rollup_lateral_left_positive | BIGINT | 36.7 | BIGINT field |
| ln_rollup_lateral_right_examined | BIGINT | 36.7 | BIGINT field |
| ln_rollup_lateral_right_positive | BIGINT | 36.7 | BIGINT field |
| ln_rollup_mets_atc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_cystic | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_ene | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_ftc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_hurthle | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_micrometastasis | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_mtc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_pdtc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_ptc | BOOLEAN | 36.7 | BOOLEAN field |
| ln_rollup_mets_ptc_variant | VARCHAR | 0.4 | VARCHAR field |
| ln_rollup_other_examined | BIGINT | 36.7 | BIGINT field |
| ln_rollup_other_positive | BIGINT | 36.7 | BIGINT field |
| ln_rollup_ratio | DOUBLE | 34.4 | DOUBLE field |
| ln_rollup_source | VARCHAR | 36.7 | VARCHAR field |
| ln_rollup_total_examined | BIGINT | 36.3 | BIGINT field |
| ln_rollup_total_levels_involved | BIGINT | 36.7 | BIGINT field |
| ln_rollup_total_positive | BIGINT | 34.6 | BIGINT field |
| ln_total_examined | INTEGER | 71.1 | INTEGER field |
| ln_total_positive | INTEGER | 33.1 | INTEGER field |
| longitudinal_assessment_available | BOOLEAN | 56.4 | BOOLEAN field |
| lvi_grade | VARCHAR | 31.0 | VARCHAR field |
| lvi_grade_final_v13 | VARCHAR | 34.5 | VARCHAR field |
| macis_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| macis_missing_components | VARCHAR | 62.5 | VARCHAR field |
| macis_risk_group | VARCHAR | 37.5 | VARCHAR field |
| macis_score | DOUBLE | 37.5 | DOUBLE field |
| margin_r_class | VARCHAR | 100.0 | VARCHAR field |
| margin_r_class_v10 | VARCHAR | 36.5 | VARCHAR field |
| margin_r_classification | VARCHAR | 36.4 | VARCHAR field |
| margin_status | VARCHAR | 36.4 | VARCHAR field |
| max_stimulated_tg | DOUBLE | 0.0 | DOUBLE field |
| max_tirads_ever | BIGINT | 31.6 | BIGINT field |
| max_tumor_size_cm_v10 | DOUBLE | 12.4 | DOUBLE field |
| mol_first_test_date | TIMESTAMP | 6.5 | Molecular testing data |
| mol_genes_list | VARCHAR | 2.9 | Molecular testing data |
| mol_has_afirma | BOOLEAN | 8.9 | Molecular testing data |
| mol_has_dicer1 | BOOLEAN | 2.9 | Molecular testing data |
| mol_has_fusion | BOOLEAN | 6.5 | Molecular testing data |
| mol_has_pik3ca | BOOLEAN | 2.9 | Molecular testing data |
| mol_has_snv | BOOLEAN | 6.5 | Molecular testing data |
| mol_has_thyroseq | BOOLEAN | 8.9 | Molecular testing data |
| mol_has_tshr | BOOLEAN | 2.9 | Molecular testing data |
| mol_n_distinct_genes | BIGINT | 6.5 | Molecular testing data |
| mol_n_fusions | BIGINT | 6.5 | Molecular testing data |
| mol_n_snvs | BIGINT | 6.5 | Molecular testing data |
| mol_n_tests | BIGINT | 92.2 | Molecular testing data |
| mol_n_variants_total | BIGINT | 6.5 | Molecular testing data |
| mol_platform | VARCHAR | 11.8 | Molecular testing data |
| mol_test_count | BIGINT | 11.8 | Molecular testing data |
| mol_test_date | TIMESTAMP | 7.4 | Molecular testing data |
| mol_variant_classes | VARCHAR | 6.5 | Molecular testing data |
| molecular_data_confidence | VARCHAR | 100.0 | VARCHAR field |
| molecular_eligible_flag | BOOLEAN | 92.2 | BOOLEAN field |
| molecular_platforms_v7 | VARCHAR | 92.2 | VARCHAR field |
| molecular_risk_calculable_flag | BOOLEAN | 100.0 | BOOLEAN field |
| molecular_risk_tier | VARCHAR | 39.3 | VARCHAR field |
| molecular_tested_confirmed | BOOLEAN | 100.0 | BOOLEAN field |
| molecular_tested_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| multifocal_flag | BOOLEAN | 0.0 | BOOLEAN field |
| n_confirmed_complications | BIGINT | 26.6 | BIGINT field |
| n_fna_cytology_records | BIGINT | 48.2 | BIGINT field |
| n_fna_episodes | BIGINT | 48.3 | BIGINT field |
| n_molecular_tests_v7 | BIGINT | 92.2 | BIGINT field |
| n_rai_episodes | BIGINT | 7.9 | BIGINT field |
| n_tg_measurements_structured | BIGINT | 23.2 | BIGINT field |
| n_tgab_measurements | BIGINT | 23.9 | BIGINT field |
| n_tumors | INTEGER | 38.1 | INTEGER field |
| n_tumors_v10 | INTEGER | 12.4 | INTEGER field |
| n_us_exams | BIGINT | 56.4 | BIGINT field |
| n_us_nodules_total | BIGINT | 56.4 | BIGINT field |
| n_us_with_ln_assessment | BIGINT | 37.5 | BIGINT field |
| nras_positive_v11 | BOOLEAN | 3.2 | BOOLEAN field |
| ntrk_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| op_drain_placed_any | BOOLEAN | 1.6 | BOOLEAN field |
| op_esophageal_inv_any | BOOLEAN | 0.0 | BOOLEAN field |
| op_findings_summary | VARCHAR | 5.4 | VARCHAR field |
| op_intraop_gross_ete_any | BOOLEAN | 0.2 | BOOLEAN field |
| op_local_invasion_any | BOOLEAN | 0.2 | BOOLEAN field |
| op_n_surgeries_with_findings | INTEGER | 80.3 | INTEGER field |
| op_parathyroid_autograft_any | BOOLEAN | 0.4 | BOOLEAN field |
| op_reoperative_any | BOOLEAN | 0.4 | BOOLEAN field |
| op_rln_monitoring_any | BOOLEAN | 15.6 | BOOLEAN field |
| op_strap_muscle_any | BOOLEAN | 1.7 | BOOLEAN field |
| op_tracheal_inv_any | BOOLEAN | 0.1 | BOOLEAN field |
| path_gross_ete_flag | BIGINT | 9.0 | BIGINT field |
| path_histology_raw | VARCHAR | 38.1 | VARCHAR field |
| path_histology_variant_raw | VARCHAR | 30.5 | VARCHAR field |
| path_margin_raw | VARCHAR | 35.7 | VARCHAR field |
| path_n_stage_raw | VARCHAR | 36.9 | VARCHAR field |
| path_t_stage_raw | VARCHAR | 36.9 | VARCHAR field |
| pax8_pparg_positive | BOOLEAN | 92.2 | BOOLEAN field |
| perineural_invasion | VARCHAR | 13.2 | VARCHAR field |
| pni_positive | BOOLEAN | 13.7 | BOOLEAN field |
| pni_refined_v6 | VARCHAR | 13.7 | VARCHAR field |
| post_rai_tg_count | BIGINT | 2.7 | BIGINT field |
| post_rai_tg_last | DOUBLE | 2.7 | DOUBLE field |
| post_rai_tg_nadir | DOUBLE | 2.7 | DOUBLE field |
| postop_calcium_min_days_postop | INTEGER | 0.7 | INTEGER field |
| postop_calcium_min_value | DOUBLE | 5.1 | DOUBLE field |
| postop_calcium_n_measurements | BIGINT | 9.7 | BIGINT field |
| postop_calcium_source_reliability | VARCHAR | 5.1 | VARCHAR field |
| postop_ionized_cal_min_value | DOUBLE | 0.0 | DOUBLE field |
| postop_labs_has_data | BOOLEAN | 9.7 | BOOLEAN field |
| postop_pth_min_days_postop | INTEGER | 1.1 | INTEGER field |
| postop_pth_min_value | DOUBLE | 6.2 | DOUBLE field |
| postop_pth_n_measurements | BIGINT | 9.7 | BIGINT field |
| postop_pth_source_reliability | VARCHAR | 6.2 | VARCHAR field |
| preop_imaging_size_cm | DOUBLE | 31.6 | DOUBLE field |
| preop_sweep_genes_found_v11 | BIGINT | 100.0 | BIGINT field |
| preop_tirads_best | BIGINT | 32.0 | BIGINT field |
| preop_tirads_category | VARCHAR | 32.0 | VARCHAR field |
| preop_tirads_worst | BIGINT | 32.0 | BIGINT field |
| proc_nlp_extraction_method | VARCHAR | 43.3 | NLP-extracted procedure data |
| proc_nlp_laryngoscopy | BOOLEAN | 43.3 | NLP-extracted procedure data |
| proc_nlp_laryngoscopy_date | DATE | 1.4 | NLP-extracted procedure data |
| proc_nlp_laryngoscopy_n_mentions | BIGINT | 43.3 | NLP-extracted procedure data |
| proc_nlp_lateral_neck_dissection | BOOLEAN | 43.3 | NLP-extracted procedure data |
| proc_nlp_mrnd | BOOLEAN | 43.3 | NLP-extracted procedure data |
| proc_nlp_mrnd_n_mentions | BIGINT | 43.3 | NLP-extracted procedure data |
| proc_nlp_n_source_notes | BIGINT | 43.3 | NLP-extracted procedure data |
| proc_nlp_note_types | VARCHAR | 43.3 | NLP-extracted procedure data |
| proc_nlp_parathyroid_autotransplant | BOOLEAN | 43.3 | NLP-extracted procedure data |
| proc_nlp_tracheostomy | BOOLEAN | 43.3 | NLP-extracted procedure data |
| proc_nlp_tracheostomy_date | DATE | 0.7 | NLP-extracted procedure data |
| proc_nlp_tracheostomy_n_mentions | BIGINT | 43.3 | NLP-extracted procedure data |
| pth_nadir | DOUBLE | 6.2 | DOUBLE field |
| pth_nadir_30d | DOUBLE | 0.8 | DOUBLE field |
| pth_nadir_days_postop | INTEGER | 0.9 | INTEGER field |
| race | VARCHAR | 99.9 | VARCHAR field |
| radtx_llm_extraction_method | VARCHAR | 1.9 | VARCHAR field |
| radtx_llm_mean_confidence | DOUBLE | 1.9 | DOUBLE field |
| radtx_llm_n_source_notes | BIGINT | 1.9 | BIGINT field |
| radtx_nlp_external_beam_radiation | BOOLEAN | 1.9 | BOOLEAN field |
| radtx_nlp_has_data | BOOLEAN | 1.9 | BOOLEAN field |
| radtx_nlp_hormone_withdrawal | BOOLEAN | 1.9 | BOOLEAN field |
| radtx_nlp_post_tx_scan_negative | BOOLEAN | 1.9 | BOOLEAN field |
| radtx_nlp_rai_ablation | BOOLEAN | 1.9 | BOOLEAN field |
| radtx_nlp_rai_ablation_n_mentions | BIGINT | 1.9 | BIGINT field |
| radtx_nlp_thyrogen_prep | BOOLEAN | 1.9 | BOOLEAN field |
| rai_avid_flag | BOOLEAN | 7.9 | BOOLEAN field |
| rai_avidity | BOOLEAN | 7.9 | BOOLEAN field |
| rai_dose_confidence_worst | VARCHAR | 2.3 | VARCHAR field |
| rai_dose_linkage | VARCHAR | 2.5 | VARCHAR field |
| rai_dose_source | VARCHAR | 2.5 | VARCHAR field |
| rai_dose_v9 | DOUBLE | 2.5 | DOUBLE field |
| rai_eligible_flag | BOOLEAN | 100.0 | BOOLEAN field |
| rai_episode_date_span_days | BIGINT | 5.3 | BIGINT field |
| rai_first_date | TIMESTAMP | 5.3 | TIMESTAMP field |
| rai_first_episode_date | DATE | 5.3 | DATE field |
| rai_has_adjudication | BOOLEAN | 7.9 | BOOLEAN field |
| rai_has_completion_status | BOOLEAN | 7.9 | BOOLEAN field |
| rai_intent_list | VARCHAR | 7.9 | VARCHAR field |
| rai_intent_v9 | VARCHAR | 2.5 | VARCHAR field |
| rai_last_episode_date | DATE | 5.3 | DATE field |
| rai_max_dose_mci | DOUBLE | 100.0 | DOUBLE field |
| rai_min_dose_mci | DOUBLE | 2.3 | DOUBLE field |
| rai_n_distinct_intents | BIGINT | 7.9 | BIGINT field |
| rai_n_episodes_with_dose | BIGINT | 7.9 | BIGINT field |
| rai_received_flag | BOOLEAN | 100.0 | BOOLEAN field |
| rai_scan_findings_v9 | INTEGER | 0.0 | INTEGER field |
| rai_stimulated_tg | DOUBLE | 2.5 | DOUBLE field |
| rai_stimulated_tsh | DOUBLE | 0.6 | DOUBLE field |
| rai_total_cumulative_dose_mci | DOUBLE | 2.3 | DOUBLE field |
| rai_validation_tier | VARCHAR | 7.9 | VARCHAR field |
| ras_allele_freq_v11 | DOUBLE | 0.2 | DOUBLE field |
| ras_positive | BOOLEAN | 100.0 | BOOLEAN field |
| ras_positive_v11 | BOOLEAN | 3.2 | BOOLEAN field |
| ras_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| ras_primary_subtype_v11 | VARCHAR | 2.9 | VARCHAR field |
| ras_protein_change_v11 | VARCHAR | 1.0 | VARCHAR field |
| ras_resolution_confidence_v13 | DOUBLE | 0.3 | DOUBLE field |
| ras_resolution_source_v13 | VARCHAR | 0.3 | VARCHAR field |
| ras_resolved_af_v13 | DOUBLE | 0.1 | DOUBLE field |
| ras_resolved_gene_v13 | VARCHAR | 0.3 | VARCHAR field |
| ras_resolved_variant_v13 | VARCHAR | 0.2 | VARCHAR field |
| ras_subtype | VARCHAR | 1.6 | VARCHAR field |
| rec_event_rank | INTEGER | 17.9 | Recurrence data |
| rec_source_priority | INTEGER | 17.9 | Recurrence data |
| rec_source_table | VARCHAR | 17.9 | Recurrence data |
| rec_structural_flag | BOOLEAN | 17.9 | Recurrence data |
| recurrence_confirmed | BOOLEAN | 100.0 | BOOLEAN field |
| recurrence_data_confidence | VARCHAR | 100.0 | VARCHAR field |
| recurrence_date | TIMESTAMP | 7.4 | TIMESTAMP field |
| recurrence_definition | VARCHAR | 100.0 | VARCHAR field |
| recurrence_evidence_source | VARCHAR | 7.6 | VARCHAR field |
| recurrence_flag_scoring | BOOLEAN | 100.0 | BOOLEAN field |
| recurrence_histology | INTEGER | 0.0 | INTEGER field |
| recurrence_site | VARCHAR | 1.4 | VARCHAR field |
| recurrence_type | VARCHAR | 100.0 | VARCHAR field |
| research_id | VARCHAR | 100.0 | VARCHAR field |
| ret_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| rln_classification | VARCHAR | 0.8 | VARCHAR field |
| rln_injury_days_postop | INTEGER | 0.8 | INTEGER field |
| rln_injury_detection_date | DATE | 0.8 | DATE field |
| rln_injury_evidence | VARCHAR | 0.8 | VARCHAR field |
| rln_injury_is_confirmed | BOOLEAN | 0.8 | BOOLEAN field |
| rln_injury_tier | INTEGER | 0.8 | INTEGER field |
| rln_injury_type | VARCHAR | 0.8 | VARCHAR field |
| rln_laterality | VARCHAR | 0.1 | VARCHAR field |
| rln_permanent_flag | BOOLEAN | 100.0 | BOOLEAN field |
| rln_status | VARCHAR | 100.0 | VARCHAR field |
| rln_temporality | VARCHAR | 0.8 | VARCHAR field |
| rln_transient_flag | BOOLEAN | 100.0 | BOOLEAN field |
| seroma_status | VARCHAR | 100.0 | VARCHAR field |
| sex | VARCHAR | 100.0 | VARCHAR field |
| surg_hemithyroidectomy | BOOLEAN | 80.3 | BOOLEAN field |
| surg_n_procedures | BIGINT | 80.3 | BIGINT field |
| surg_procedure_type | VARCHAR | 80.3 | VARCHAR field |
| surg_total_thyroidectomy | BOOLEAN | 80.3 | BOOLEAN field |
| surv_max_time_days | BIGINT | 96.7 | BIGINT field |
| surv_max_time_days_capped | BIGINT | 96.7 | BIGINT field |
| surv_n_events | BIGINT | 96.7 | BIGINT field |
| surv_recurrence_risk_band | VARCHAR | 34.6 | VARCHAR field |
| surv_tg_annual_log_slope | DOUBLE | 16.2 | DOUBLE field |
| survival_eligible_flag | BOOLEAN | 100.0 | BOOLEAN field |
| sx_llm_extraction_method | VARCHAR | 1.1 | VARCHAR field |
| sx_llm_mean_confidence | DOUBLE | 1.1 | DOUBLE field |
| sx_llm_n_source_notes | BIGINT | 1.1 | BIGINT field |
| sx_nlp_any_symptom_data | BOOLEAN | 1.1 | BOOLEAN field |
| sx_nlp_dysphagia | BOOLEAN | 1.1 | BOOLEAN field |
| sx_nlp_dyspnea | BOOLEAN | 1.1 | BOOLEAN field |
| sx_nlp_hoarseness | BOOLEAN | 1.1 | BOOLEAN field |
| sx_nlp_neck_mass | BOOLEAN | 1.1 | BOOLEAN field |
| tert_platforms_v9 | VARCHAR | 0.7 | VARCHAR field |
| tert_positive | BOOLEAN | 100.0 | BOOLEAN field |
| tert_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| tert_status_v7 | VARCHAR | 92.2 | VARCHAR field |
| tert_test_count_v9 | BIGINT | 0.7 | BIGINT field |
| tert_tested | BOOLEAN | 94.8 | BOOLEAN field |
| tert_variant_v9 | VARCHAR | 0.7 | VARCHAR field |
| tgab_interference_flag | BOOLEAN | 25.0 | BOOLEAN field |
| tgab_last_value | DOUBLE | 23.9 | DOUBLE field |
| tgab_nadir | DOUBLE | 23.9 | DOUBLE field |
| tgab_peak | DOUBLE | 23.9 | DOUBLE field |
| time_to_recurrence_days | DOUBLE | 1.6 | DOUBLE field |
| total_ln_positive_v10 | INTEGER | 12.4 | INTEGER field |
| tp53_positive_v7 | BOOLEAN | 92.2 | BOOLEAN field |
| tp_central_examined | BIGINT | 36.7 | BIGINT field |
| tp_central_positive_total | BIGINT | 36.7 | BIGINT field |
| tp_ln_central_positive | BIGINT | 21.2 | BIGINT field |
| tp_ln_ene | BIGINT | 11.1 | BIGINT field |
| tp_ln_examined | BIGINT | 36.3 | BIGINT field |
| tp_ln_largest_deposit_cm | DOUBLE | 7.1 | DOUBLE field |
| tp_ln_lateral_positive | BIGINT | 21.2 | BIGINT field |
| tp_ln_levels_involved | BIGINT | 36.7 | BIGINT field |
| tp_ln_positive | BIGINT | 34.6 | BIGINT field |
| tumor_size_cm | DOUBLE | 38.0 | DOUBLE field |
| us_first_exam_date | DATE | 37.5 | DATE field |
| us_isthmus_thickness_mm | VARCHAR | 37.5 | VARCHAR field |
| us_last_exam_date | DATE | 37.5 | DATE field |
| us_left_lobe_volume_ml | VARCHAR | 37.5 | VARCHAR field |
| us_most_recent_date | DATE | 37.5 | DATE field |
| us_n_reports | BIGINT | 37.5 | BIGINT field |
| us_right_lobe_volume_ml | VARCHAR | 37.5 | VARCHAR field |
| us_total_volume_ml | VARCHAR | 37.5 | VARCHAR field |
| vasc_confidence_final_v13 | DOUBLE | 34.5 | DOUBLE field |
| vasc_grade | VARCHAR | 100.0 | VARCHAR field |
| vasc_grade_final_v13 | VARCHAR | 34.5 | VARCHAR field |
| vasc_source_final_v13 | VARCHAR | 34.5 | VARCHAR field |
| vasc_vessel_count_v13 | DOUBLE | 0.4 | DOUBLE field |
| vascular_invasion_grade | VARCHAR | 34.5 | VARCHAR field |
| vascular_who_2022_grade | VARCHAR | 3.7 | VARCHAR field |
| vessel_count | DOUBLE | 0.4 | DOUBLE field |
| voice_data_confidence | DOUBLE | 100.0 | DOUBLE field |
| voice_followup_completeness | VARCHAR | 100.0 | VARCHAR field |
| voice_outcome_category | VARCHAR | 100.0 | VARCHAR field |
| weight_kg_note | DOUBLE | 5.3 | DOUBLE field |
| worst_bethesda_num | BIGINT | 48.3 | BIGINT field |
| worst_ete_v10 | VARCHAR | 11.0 | VARCHAR field |
| worst_tirads_category | VARCHAR | 31.6 | VARCHAR field |
| wound_infection_status | VARCHAR | 100.0 | VARCHAR field |
| recurrence_site_raw | VARCHAR | 0.7 | VARCHAR field |
| recurrence_laterality | VARCHAR | 0.5 | VARCHAR field |
| recurrence_site_source | VARCHAR | 0.7 | VARCHAR field |
| followup_n_contact_sources | INTEGER | 99.9 | INTEGER field |
| followup_all_sources | VARCHAR | 99.9 | VARCHAR field |
| followup_recovery_method | VARCHAR | 99.9 | VARCHAR field |
| death_date | DATE | 1.8 | DATE field |
| vital_status | VARCHAR | 100.0 | VARCHAR field |
| death_occurred | BOOLEAN | 100.0 | BOOLEAN field |
| death_source | VARCHAR | 1.8 | VARCHAR field |
| overall_survival_days | BIGINT | 100.0 | BIGINT field |
| overall_survival_years | DOUBLE | 100.0 | DOUBLE field |
| survival_event | BOOLEAN | 100.0 | BOOLEAN field |
| followup_or_death_date | DATE | 100.0 | DATE field |
| followup_or_death_years | DOUBLE | 100.0 | DOUBLE field |
| death_integration_script | VARCHAR | 100.0 | VARCHAR field |
| braf_variant_raw | VARCHAR | 1.6 | VARCHAR field |
| fna_bethesda_final | BIGINT | 48.3 | BIGINT field |
| imaging_nodule_size_cm | DOUBLE | 31.6 | DOUBLE field |
| imaging_tirads_best | BIGINT | 32.0 | BIGINT field |
| imaging_tirads_category | VARCHAR | 32.0 | VARCHAR field |
| imaging_tirads_worst | BIGINT | 32.0 | BIGINT field |
| lateral_neck_dissected | BOOLEAN | 100.0 | BOOLEAN field |
| ln_positive_final | INTEGER | 37.3 | INTEGER field |
| margin_status_final | VARCHAR | 36.4 | VARCHAR field |
| mol_test_date_source | VARCHAR | 7.4 | Molecular testing data |
| path_ene_raw | VARCHAR | 11.3 | VARCHAR field |
| path_ete_raw | VARCHAR | 37.5 | VARCHAR field |
| path_laterality | VARCHAR | 95.1 | VARCHAR field |
| path_ln_examined_raw | INTEGER | 71.1 | INTEGER field |
| path_ln_positive_raw | INTEGER | 33.1 | INTEGER field |
| path_lvi_raw | VARCHAR | 31.0 | VARCHAR field |
| path_m_stage_raw | VARCHAR | 36.8 | VARCHAR field |
| path_multifocal_flag | BOOLEAN | 0.0 | BOOLEAN field |
| path_n_tumors | INTEGER | 0.0 | INTEGER field |
| path_pni_raw | VARCHAR | 13.2 | VARCHAR field |
| path_stage_raw | INTEGER | 0.0 | INTEGER field |
| path_tumor_size_cm | DOUBLE | 38.0 | DOUBLE field |
| path_vascular_invasion_raw | VARCHAR | 33.9 | VARCHAR field |
| postop_low_calcium_flag | BOOLEAN | 1.2 | BOOLEAN field |
| postop_low_pth_flag | BOOLEAN | 1.2 | BOOLEAN field |
| provenance_confidence | INTEGER | 100.0 | INTEGER field |
| provenance_note | VARCHAR | 100.0 | VARCHAR field |
| rai_assertion_statuses | VARCHAR | 0.3 | VARCHAR field |
| rai_date_confidence | DOUBLE | 5.3 | DOUBLE field |
| rai_date_source | VARCHAR | 5.3 | VARCHAR field |
| ras_positive_final | BOOLEAN | 100.0 | BOOLEAN field |
| ras_subtype_raw | VARCHAR | 1.6 | VARCHAR field |
| recurrence_date_source | VARCHAR | 1.7 | VARCHAR field |
| recurrence_site_primary | INTEGER | 0.0 | INTEGER field |
| recurrence_source | VARCHAR | 17.9 | VARCHAR field |
| recurrence_type_primary | VARCHAR | 17.9 | VARCHAR field |
| resolved_at | TIMESTAMP WITH TIME ZONE | 100.0 | TIMESTAMP WITH TIME ZONE field |
| resolved_layer_version | VARCHAR | 100.0 | VARCHAR field |
| scoring_ajcc8_flag | BOOLEAN | 100.0 | BOOLEAN field |
| scoring_ata_flag | BOOLEAN | 100.0 | BOOLEAN field |
| scoring_macis_flag | BOOLEAN | 100.0 | BOOLEAN field |
| source_script | VARCHAR | 100.0 | VARCHAR field |
| source_table | VARCHAR | 100.0 | VARCHAR field |
| structural_recurrence_flag | BOOLEAN | 17.9 | BOOLEAN field |
| surg_first_date | TIMESTAMP | 80.3 | TIMESTAMP field |
| tert_positive_final | BOOLEAN | 100.0 | BOOLEAN field |
| tsh_suppressed_ever | BOOLEAN | 0.0 | BOOLEAN field |
| vascular_invasion_final | VARCHAR | 34.5 | VARCHAR field |
| vascular_vessel_count | DOUBLE | 0.4 | DOUBLE field |
| first_recurrence_days_from_surg | INTEGER | 0.4 | Days from first surgery date (negative = before surgery) |
| first_tg_days_from_surg | INTEGER | 18.9 | Days from first surgery date (negative = before surgery) |
| last_contact_days_from_surg | INTEGER | 80.3 | Days from first surgery date (negative = before surgery) |
| last_tg_days_from_surg | INTEGER | 18.9 | Days from first surgery date (negative = before surgery) |
| mol_first_test_days_from_surg | INTEGER | 3.5 | Days from first surgery date (negative = before surgery) |
| mol_test_days_from_surg | INTEGER | 2.9 | Days from first surgery date (negative = before surgery) |
| proc_nlp_laryngoscopy_days_from_surg | INTEGER | 0.7 | Days from first surgery date (negative = before surgery) |
| proc_nlp_tracheostomy_days_from_surg | INTEGER | 0.1 | Days from first surgery date (negative = before surgery) |
| rai_first_days_from_surg | INTEGER | 4.4 | Days from first surgery date (negative = before surgery) |
| rai_first_episode_days_from_surg | INTEGER | 4.4 | Days from first surgery date (negative = before surgery) |
| rai_last_episode_days_from_surg | INTEGER | 4.4 | Days from first surgery date (negative = before surgery) |
| recurrence_days_from_surg | INTEGER | 7.0 | Days from first surgery date (negative = before surgery) |
| rln_injury_detection_days_from_surg | INTEGER | 0.6 | Days from first surgery date (negative = before surgery) |
| us_first_exam_days_from_surg | INTEGER | 23.4 | Days from first surgery date (negative = before surgery) |
| us_last_exam_days_from_surg | INTEGER | 23.4 | Days from first surgery date (negative = before surgery) |
| us_most_recent_days_from_surg | INTEGER | 23.3 | Days from first surgery date (negative = before surgery) |
| death_days_from_surg | INTEGER | 1.8 | Days from first surgery date (negative = before surgery) |
| followup_or_death_days_from_surg | INTEGER | 80.3 | Days from first surgery date (negative = before surgery) |
| resolved_days_from_surg | INTEGER | 80.3 | Days from first surgery date (negative = before surgery) |
| surg_first_days_from_surg | INTEGER | 80.3 | Days from first surgery date (negative = before surgery) |
| n_surgeries | INTEGER | 80.3 | INTEGER field |
| second_surgery_date | DATE | 0.0 | DATE field |
| third_surgery_date | DATE | 0.0 | DATE field |
| days_between_first_second_surgery | INTEGER | 0.0 | INTEGER field |

### Source: longitudinal_lab_canonical_v1 (53 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| lab_tsh_n_measurements | BIGINT | 100.0 | Laboratory value |
| lab_tsh_min | DOUBLE | 3.8 | Laboratory value |
| lab_tsh_max | DOUBLE | 3.8 | Laboratory value |
| lab_tsh_first_date | DATE | 3.8 | Laboratory value |
| lab_tsh_last_date | DATE | 3.8 | Laboratory value |
| lab_tsh_most_recent | DOUBLE | 3.8 | Laboratory value |
| lab_tsh_most_recent_date | DATE | 3.8 | Laboratory value |
| lab_tsh_unit | VARCHAR | 100.0 | Laboratory value |
| lab_tsh_n_censored | BIGINT | 100.0 | Laboratory value |
| lab_tsh_n_parsed_from_raw | BIGINT | 100.0 | Laboratory value |
| lab_vitd_n_measurements | BIGINT | 100.0 | Laboratory value |
| lab_vitd_min | DOUBLE | 0.7 | Laboratory value |
| lab_vitd_max | DOUBLE | 0.7 | Laboratory value |
| lab_vitd_first_date | DATE | 0.7 | Laboratory value |
| lab_vitd_last_date | DATE | 0.7 | Laboratory value |
| lab_vitd_most_recent | DOUBLE | 0.7 | Laboratory value |
| lab_vitd_most_recent_date | DATE | 0.7 | Laboratory value |
| lab_vitd_unit | VARCHAR | 100.0 | Laboratory value |
| lab_vitd_n_censored | BIGINT | 100.0 | Laboratory value |
| lab_vitd_n_parsed_from_raw | BIGINT | 100.0 | Laboratory value |
| lab_pth_n_measurements | INTEGER | 100.0 | Laboratory value |
| lab_pth_min | DOUBLE | 1.7 | Laboratory value |
| lab_pth_max | DOUBLE | 1.7 | Laboratory value |
| lab_pth_first_date | DATE | 1.7 | Laboratory value |
| lab_pth_last_date | DATE | 1.7 | Laboratory value |
| lab_pth_most_recent | DOUBLE | 1.7 | Laboratory value |
| lab_pth_most_recent_date | DATE | 1.7 | Laboratory value |
| lab_pth_unit | VARCHAR | 100.0 | Laboratory value |
| lab_pth_n_censored | BIGINT | 100.0 | Laboratory value |
| lab_pth_n_parsed_from_raw | BIGINT | 100.0 | Laboratory value |
| lab_calcium_n_measurements | INTEGER | 100.0 | Laboratory value |
| lab_calcium_min | DOUBLE | 1.5 | Laboratory value |
| lab_calcium_max | DOUBLE | 1.5 | Laboratory value |
| lab_calcium_first_date | DATE | 1.5 | Laboratory value |
| lab_calcium_last_date | DATE | 1.5 | Laboratory value |
| lab_calcium_most_recent | DOUBLE | 1.5 | Laboratory value |
| lab_calcium_most_recent_date | DATE | 1.5 | Laboratory value |
| lab_calcium_unit | VARCHAR | 100.0 | Laboratory value |
| lab_calcium_n_censored | BIGINT | 100.0 | Laboratory value |
| lab_calcium_n_parsed_from_raw | BIGINT | 100.0 | Laboratory value |
| lab_completeness_score | INTEGER | 100.0 | Laboratory value |
| lab_tsh_first_days_from_surg | INTEGER | 3.0 | Days from first surgery date (negative = before surgery) |
| lab_tsh_last_days_from_surg | INTEGER | 3.0 | Days from first surgery date (negative = before surgery) |
| lab_tsh_most_recent_days_from_surg | INTEGER | 3.0 | Days from first surgery date (negative = before surgery) |
| lab_vitd_first_days_from_surg | INTEGER | 0.5 | Days from first surgery date (negative = before surgery) |
| lab_vitd_last_days_from_surg | INTEGER | 0.5 | Days from first surgery date (negative = before surgery) |
| lab_vitd_most_recent_days_from_surg | INTEGER | 0.5 | Days from first surgery date (negative = before surgery) |
| lab_pth_first_days_from_surg | INTEGER | 1.1 | Days from first surgery date (negative = before surgery) |
| lab_pth_last_days_from_surg | INTEGER | 1.1 | Days from first surgery date (negative = before surgery) |
| lab_pth_most_recent_days_from_surg | INTEGER | 1.1 | Days from first surgery date (negative = before surgery) |
| lab_calcium_first_days_from_surg | INTEGER | 1.2 | Days from first surgery date (negative = before surgery) |
| lab_calcium_last_days_from_surg | INTEGER | 1.2 | Days from first surgery date (negative = before surgery) |
| lab_calcium_most_recent_days_from_surg | INTEGER | 1.2 | Days from first surgery date (negative = before surgery) |

### Source: mri_imaging (25 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| mri_contrast_used_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_exam_type_first | VARCHAR | 3.8 | MRI imaging data |
| mri_first_date | DATE | 3.7 | MRI imaging data |
| mri_has_data | BOOLEAN | 4.2 | MRI imaging data |
| mri_has_dimensions | BOOLEAN | 4.2 | MRI imaging data |
| mri_has_dominant_nodule | BOOLEAN | 4.2 | MRI imaging data |
| mri_impression_first | VARCHAR | 4.2 | MRI imaging data |
| mri_impression_last | VARCHAR | 4.2 | MRI imaging data |
| mri_indication_first | VARCHAR | 3.7 | MRI imaging data |
| mri_key_findings_last | VARCHAR | 4.2 | MRI imaging data |
| mri_last_date | DATE | 3.7 | MRI imaging data |
| mri_ln_mentioned_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_mass_effect_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_n_exams | BIGINT | 4.2 | MRI imaging data |
| mri_pathologic_ln_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_recommendation_last | VARCHAR | 1.5 | MRI imaging data |
| mri_substernal_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_substernal_extension_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_thyroid_assessment_worst | VARCHAR | 3.9 | MRI imaging data |
| mri_thyroid_enlarged_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_thyroid_nodule_any | BOOLEAN | 4.2 | MRI imaging data |
| mri_vocal_cords_described | BOOLEAN | 4.2 | MRI imaging data |
| mri_vocal_cords_normal | BOOLEAN | 4.2 | MRI imaging data |
| mri_first_days_from_surg | INTEGER | 2.1 | Days from first surgery date (negative = before surgery) |
| mri_last_days_from_surg | INTEGER | 2.1 | Days from first surgery date (negative = before surgery) |

### Source: note_entities_llm_* (fleet NLP) (117 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| nlp_airway_has_data | BOOLEAN | 10.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_airway_key_finding | VARCHAR | 10.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_airway_n_entities | BIGINT | 10.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_airway_n_notes | BIGINT | 10.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_dynrisk_has_data | BOOLEAN | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_dynrisk_key_finding | VARCHAR | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_dynrisk_n_entities | BIGINT | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_dynrisk_n_notes | BIGINT | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_frozensec_has_data | BOOLEAN | 1.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_frozensec_key_finding | VARCHAR | 1.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_frozensec_n_entities | BIGINT | 1.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_frozensec_n_notes | BIGINT | 1.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_funcoutcome_has_data | BOOLEAN | 14.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_funcoutcome_key_finding | VARCHAR | 14.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_funcoutcome_n_entities | BIGINT | 14.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_funcoutcome_n_notes | BIGINT | 14.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_imaging_has_data | BOOLEAN | 15.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_imaging_key_finding | VARCHAR | 15.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_imaging_n_entities | BIGINT | 15.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_imaging_n_notes | BIGINT | 15.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_labs_has_data | BOOLEAN | 7.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_labs_key_finding | VARCHAR | 7.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_labs_n_entities | BIGINT | 7.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_labs_n_notes | BIGINT | 7.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ln_has_data | BOOLEAN | 8.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ln_levels_mentioned | VARCHAR | 7.6 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ln_n_entities | BIGINT | 8.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ln_n_notes | BIGINT | 8.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ln_positive_mentioned | BOOLEAN | 8.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_complications_has_data | BOOLEAN | 26.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_complications_n_rows | BIGINT | 26.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_genetics_has_data | BOOLEAN | 5.6 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_genetics_n_rows | BIGINT | 5.6 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_medications_has_data | BOOLEAN | 19.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_medications_n_rows | BIGINT | 19.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_operative_has_data | BOOLEAN | 37.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_operative_n_rows | BIGINT | 37.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_problemlist_has_data | BOOLEAN | 37.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_problemlist_n_rows | BIGINT | 37.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_procedures_has_data | BOOLEAN | 43.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_procedures_n_rows | BIGINT | 43.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_staging_has_data | BOOLEAN | 15.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ne_staging_n_rows | BIGINT | 15.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_parathyroid_has_data | BOOLEAN | 1.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_parathyroid_key_finding | VARCHAR | 1.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_parathyroid_n_entities | BIGINT | 1.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_parathyroid_n_notes | BIGINT | 1.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_ete_mentioned | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_has_data | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_histology_mentioned | VARCHAR | 18.3 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_ln_positive_mentioned | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_margin_mentioned | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_multifocal_mentioned | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_n_entities | BIGINT | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_n_notes | BIGINT | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_path_vasc_inv_mentioned | BOOLEAN | 25.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_physexam_has_data | BOOLEAN | 4.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_physexam_key_finding | VARCHAR | 4.6 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_physexam_n_entities | BIGINT | 4.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_physexam_n_notes | BIGINT | 4.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pmhx_has_data | BOOLEAN | 2.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pmhx_key_finding | VARCHAR | 2.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pmhx_n_entities | BIGINT | 2.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pmhx_n_notes | BIGINT | 2.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pshx_has_data | BOOLEAN | 17.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pshx_key_finding | VARCHAR | 17.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pshx_n_entities | BIGINT | 17.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_pshx_n_notes | BIGINT | 17.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ptdecision_has_data | BOOLEAN | 3.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ptdecision_key_finding | VARCHAR | 3.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ptdecision_n_entities | BIGINT | 3.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_ptdecision_n_notes | BIGINT | 3.4 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_radtx_has_data | BOOLEAN | 1.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_radtx_key_finding | VARCHAR | 1.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_radtx_n_entities | BIGINT | 1.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_radtx_n_notes | BIGINT | 1.9 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_raidetail_has_data | BOOLEAN | 5.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_raidetail_key_finding | VARCHAR | 5.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_raidetail_n_entities | BIGINT | 5.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_raidetail_n_notes | BIGINT | 5.7 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_any_mentioned | BOOLEAN | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_confidence_tier | VARCHAR | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_disease_free_mentioned | BOOLEAN | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_earliest_date | DATE | 0.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_has_data | BOOLEAN | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_n_entities | BIGINT | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_type_worst | VARCHAR | 1.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_survfu_has_data | BOOLEAN | 26.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_survfu_key_finding | VARCHAR | 26.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_survfu_n_entities | BIGINT | 26.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_survfu_n_notes | BIGINT | 26.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_symptoms_has_data | BOOLEAN | 1.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_symptoms_key_finding | VARCHAR | 1.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_symptoms_n_entities | BIGINT | 1.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_symptoms_n_notes | BIGINT | 1.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_synoptic_has_data | BOOLEAN | 0.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_synoptic_key_finding | VARCHAR | 0.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_synoptic_n_entities | BIGINT | 0.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_synoptic_n_notes | BIGINT | 0.1 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tg_has_data | BOOLEAN | 0.5 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tg_n_entities | BIGINT | 0.5 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tg_rising_mentioned | BOOLEAN | 0.5 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tg_undetectable_mentioned | BOOLEAN | 0.5 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tirads_has_component_detail | BOOLEAN | 15.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tirads_has_data | BOOLEAN | 15.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tirads_max_category | VARCHAR | 15.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tirads_n_entities | BIGINT | 15.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_tirads_n_notes | BIGINT | 15.8 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_usnodule_has_data | BOOLEAN | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_usnodule_key_finding | VARCHAR | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_usnodule_n_entities | BIGINT | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_usnodule_n_notes | BIGINT | 0.2 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_vasc_confidence_tier | VARCHAR | 6.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_vasc_has_data | BOOLEAN | 6.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_vasc_n_entities | BIGINT | 6.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_vasc_positive_mentioned | BOOLEAN | 6.0 | NLP-extracted from clinical notes (qwen3:32b fleet) |
| nlp_rec_earliest_days_from_surg | INTEGER | 0.5 | Days from first surgery date (negative = before surgery) |

### Source: nsqip_data (100 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| nsqip_age_at_surgery | DOUBLE | 13.0 | NSQIP perioperative quality data |
| nsqip_asa_class | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_bmi | DOUBLE | 13.0 | NSQIP perioperative quality data |
| nsqip_diabetes | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_functional_status | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_height_in | DOUBLE | 13.0 | NSQIP perioperative quality data |
| nsqip_hypertension | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_length_of_stay_days | BIGINT | 13.0 | NSQIP perioperative quality data |
| nsqip_sex | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_smoker | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_source | VARCHAR | 13.0 | NSQIP perioperative quality data |
| nsqip_weight_lbs | DOUBLE | 13.0 | NSQIP perioperative quality data |
| nsqip_operation_date | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_match_method | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_readmission_count | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_unplanned_readmission_count | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_related_readmission_count | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_first_readmission_date | VARCHAR | 0.3 | NSQIP perioperative quality data |
| nsqip_hypocalcemia | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_predischarge | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_postdischarge | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_event | VARCHAR | 6.3 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_event_type | VARCHAR | 0.3 | NSQIP perioperative quality data |
| nsqip_iv_calcium | VARCHAR | 2.4 | NSQIP perioperative quality data |
| nsqip_calcium_checked | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_pth_checked | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_calcium_vitd_replacement | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_calcium_vitd_last_check | VARCHAR | 2.4 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_last_check | VARCHAR | 2.4 | NSQIP perioperative quality data |
| nsqip_rln_injury | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_neck_hematoma | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_hospital_los_days | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_surgical_los_days | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_discharge_destination | VARCHAR | 11.5 | NSQIP perioperative quality data |
| nsqip_death_30d | VARCHAR | 11.5 | NSQIP perioperative quality data |
| nsqip_unplanned_return_or | DOUBLE | 0.1 | NSQIP perioperative quality data |
| nsqip_operative_duration_min | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_inpatient_outpatient | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_cpt_code | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_cpt_description | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_primary_indication | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_operative_approach | VARCHAR | 4.5 | NSQIP perioperative quality data |
| nsqip_central_neck_dissection | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_lateral_neck_dissection | VARCHAR | 2.4 | NSQIP perioperative quality data |
| nsqip_vessel_sealant | VARCHAR | 6.3 | NSQIP perioperative quality data |
| nsqip_rln_monitoring | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_drain_usage | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_tobacco_use | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_heart_failure | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_copd | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_bleeding_disorder | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_disseminated_cancer | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_sodium | DOUBLE | 9.9 | NSQIP perioperative quality data |
| nsqip_bun | DOUBLE | 10.0 | NSQIP perioperative quality data |
| nsqip_creatinine | DOUBLE | 10.0 | NSQIP perioperative quality data |
| nsqip_albumin | DOUBLE | 6.5 | NSQIP perioperative quality data |
| nsqip_total_bilirubin | DOUBLE | 6.3 | NSQIP perioperative quality data |
| nsqip_ast | DOUBLE | 6.3 | NSQIP perioperative quality data |
| nsqip_alk_phos | DOUBLE | 6.3 | NSQIP perioperative quality data |
| nsqip_wbc | DOUBLE | 9.5 | NSQIP perioperative quality data |
| nsqip_hemoglobin | DOUBLE | 2.0 | NSQIP perioperative quality data |
| nsqip_hematocrit | DOUBLE | 9.5 | NSQIP perioperative quality data |
| nsqip_platelet_count | DOUBLE | 9.5 | NSQIP perioperative quality data |
| nsqip_hba1c | DOUBLE | 0.9 | NSQIP perioperative quality data |
| nsqip_inr | DOUBLE | 3.4 | NSQIP perioperative quality data |
| nsqip_ptt | DOUBLE | 3.1 | NSQIP perioperative quality data |
| nsqip_admission_date | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_discharge_date | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_surgery_start_time | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_surgery_finish_time | VARCHAR | 11.6 | NSQIP perioperative quality data |
| nsqip_superficial_ssi | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_deep_ssi | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_organ_space_ssi | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_dvt | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_pe | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_transfusion | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_sepsis | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_pneumonia | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_unplanned_intubation | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_final_pathology | VARCHAR | 2.4 | NSQIP perioperative quality data |
| nsqip_t_classification | VARCHAR | 3.4 | NSQIP perioperative quality data |
| nsqip_multifocal | VARCHAR | 3.4 | NSQIP perioperative quality data |
| nsqip_n_classification | VARCHAR | 3.4 | NSQIP perioperative quality data |
| nsqip_nodes_removed | DOUBLE | 7.6 | NSQIP perioperative quality data |
| nsqip_nodes_positive | DOUBLE | 0.7 | NSQIP perioperative quality data |
| nsqip_m_classification | VARCHAR | 3.0 | NSQIP perioperative quality data |
| nsqip_neoplasm | VARCHAR | 6.4 | NSQIP perioperative quality data |
| nsqip_neoplasm_type | VARCHAR | 4.1 | NSQIP perioperative quality data |
| nsqip_prior_neck_surgery | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_preop_biopsy_result | VARCHAR | 7.9 | NSQIP perioperative quality data |
| nsqip_molecular_testing | VARCHAR | 2.3 | NSQIP perioperative quality data |
| nsqip_molecular_result | VARCHAR | 0.5 | NSQIP perioperative quality data |
| nsqip_same_day_discharge_flag | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_hypocalcemia_flag | DOUBLE | 8.6 | NSQIP perioperative quality data |
| nsqip_readmission_30d_flag | BIGINT | 11.6 | NSQIP perioperative quality data |
| nsqip_rln_injury_flag | DOUBLE | 8.6 | NSQIP perioperative quality data |
| nsqip_hematoma_flag | DOUBLE | 8.6 | NSQIP perioperative quality data |
| nsqip_calcium_vitd_category | VARCHAR | 8.7 | NSQIP perioperative quality data |
| nsqip_thyroidectomy_has_data | BOOLEAN | 100.0 | NSQIP perioperative quality data |
| nsqip_thyroidectomy_source_script | VARCHAR | 100.0 | NSQIP perioperative quality data |

### Source: nsqip_data / op_sheet_data / clinical_notes_long (4 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| bmi_combined | DOUBLE | 19.2 | BMI from nsqip_data / op_sheet_data / clinical_notes_long |
| bmi_note_extracted | DOUBLE | 5.9 | BMI from nsqip_data / op_sheet_data / clinical_notes_long |
| bmi_note_source | VARCHAR | 5.9 | BMI from nsqip_data / op_sheet_data / clinical_notes_long |
| bmi_source | VARCHAR | 19.2 | BMI from nsqip_data / op_sheet_data / clinical_notes_long |

### Source: nuclear_med (26 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| nucmed_has_rai_scan | BOOLEAN | 10.6 | Nuclear medicine data |
| nucmed_n_scans | BIGINT | 10.6 | Nuclear medicine data |
| nucmed_scan_types | VARCHAR | 10.3 | Nuclear medicine data |
| nucmed_uptake_24hr_max | DOUBLE | 5.5 | Nuclear medicine data |
| nucmed_tg_max | DOUBLE | 2.5 | Nuclear medicine data |
| nucmed_tg_min | DOUBLE | 2.5 | Nuclear medicine data |
| nucmed_n_tsh_values | INTEGER | 4.1 | Nuclear medicine data |
| nucmed_n_tg_values | INTEGER | 4.1 | Nuclear medicine data |
| nucmed_n_tgab_values | INTEGER | 4.1 | Nuclear medicine data |
| nucmed_first_scan_with_labs | VARCHAR | 4.1 | Nuclear medicine data |
| nucmed_last_scan_with_labs | VARCHAR | 4.1 | Nuclear medicine data |
| nucmed_lab_source | VARCHAR | 4.1 | Nuclear medicine data |
| nucmed_indication_first | VARCHAR | 10.3 | Nuclear medicine data |
| nucmed_indication_last | VARCHAR | 10.3 | Nuclear medicine data |
| nucmed_impression_last | VARCHAR | 7.7 | Nuclear medicine data |
| nucmed_findings_last | VARCHAR | 8.2 | Nuclear medicine data |
| nucmed_tsh_max | DOUBLE | 4.0 | Nuclear medicine data |
| nucmed_tsh_is_stimulated | BOOLEAN | 4.0 | Nuclear medicine data |
| nucmed_tgab_max | DOUBLE | 0.0 | Nuclear medicine data |
| nucmed_uptake_pct_max | DOUBLE | 5.5 | Nuclear medicine data |
| nucmed_dose_max_parsed | DOUBLE | 2.8 | Nuclear medicine data |
| nucmed_cumulative_therapeutic_dose | DOUBLE | 2.6 | Nuclear medicine data |
| nucmed_n_doses_parsed | INTEGER | 10.6 | Nuclear medicine data |
| nucmed_n_with_indication | INTEGER | 10.6 | Nuclear medicine data |
| nucmed_n_with_impression | INTEGER | 10.6 | Nuclear medicine data |
| nucmed_overall_assessment | VARCHAR | 10.6 | Nuclear medicine data |

### Source: op_sheet_data (48 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| ops_anticoagulation_meds | VARCHAR | 0.1 | From operative sheet data |
| ops_bmi | DOUBLE | 1.1 | From operative sheet data |
| ops_cervical_ln_us_performed | VARCHAR | 1.1 | From operative sheet data |
| ops_difficult_airway | VARCHAR | 1.3 | From operative sheet data |
| ops_dominant_nodule_bethesda | VARCHAR | 0.9 | From operative sheet data |
| ops_dominant_nodule_location | VARCHAR | 0.0 | From operative sheet data |
| ops_dominant_nodule_size_us | VARCHAR | 0.0 | From operative sheet data |
| ops_ebl_ml | VARCHAR | 1.1 | From operative sheet data |
| ops_family_hx_thyroid_ca | VARCHAR | 1.3 | From operative sheet data |
| ops_head_neck_us_findings | VARCHAR | 0.0 | From operative sheet data |
| ops_intraop_appearance | VARCHAR | 3.1 | From operative sheet data |
| ops_intraop_nodule_count | VARCHAR | 6.0 | From operative sheet data |
| ops_io_tumor_appearance | VARCHAR | 18.6 | From operative sheet data |
| ops_ll_ag | DOUBLE | 2.0 | From operative sheet data |
| ops_ll_para_visualized | VARCHAR | 13.5 | From operative sheet data |
| ops_ll_resection | DOUBLE | 0.1 | From operative sheet data |
| ops_lu_ag | DOUBLE | 1.4 | From operative sheet data |
| ops_lu_para_visualized | VARCHAR | 13.4 | From operative sheet data |
| ops_lu_resection | DOUBLE | 0.0 | From operative sheet data |
| ops_max_diameter_cm | VARCHAR | 17.2 | From operative sheet data |
| ops_nerve_stim_final | VARCHAR | 1.0 | From operative sheet data |
| ops_nerve_stim_left | VARCHAR | 0.9 | From operative sheet data |
| ops_nerve_stim_right | VARCHAR | 0.7 | From operative sheet data |
| ops_other_ag | VARCHAR | 0.0 | From operative sheet data |
| ops_palpable_lesion | VARCHAR | 0.0 | From operative sheet data |
| ops_para_ag_performed | VARCHAR | 19.2 | From operative sheet data |
| ops_parathyroid_ag_notes | VARCHAR | 4.2 | From operative sheet data |
| ops_parathyroidectomy | VARCHAR | 0.3 | From operative sheet data |
| ops_periop_complications | VARCHAR | 12.9 | From operative sheet data |
| ops_preop_diagnosis | VARCHAR | 19.2 | From operative sheet data |
| ops_preop_imaging_performed | VARCHAR | 1.3 | From operative sheet data |
| ops_preop_laryngoscopy | VARCHAR | 1.1 | From operative sheet data |
| ops_preop_nodules_count_size | VARCHAR | 0.4 | From operative sheet data |
| ops_preop_symptoms | VARCHAR | 1.5 | From operative sheet data |
| ops_prior_neck_irradiation | VARCHAR | 1.4 | From operative sheet data |
| ops_prior_neck_operation | VARCHAR | 4.2 | From operative sheet data |
| ops_rl_ag | VARCHAR | 2.4 | From operative sheet data |
| ops_rl_para_visualized | DOUBLE | 13.1 | From operative sheet data |
| ops_rl_resection | DOUBLE | 0.1 | From operative sheet data |
| ops_ru_ag | DOUBLE | 1.2 | From operative sheet data |
| ops_ru_para_visualized | VARCHAR | 13.1 | From operative sheet data |
| ops_ru_resection | DOUBLE | 0.0 | From operative sheet data |
| ops_skin_to_skin_min | DOUBLE | 1.1 | From operative sheet data |
| ops_supranumerary_para | VARCHAR | 0.1 | From operative sheet data |
| ops_surg_date | VARCHAR | 80.3 | From operative sheet data |
| ops_surgeon | VARCHAR | 79.0 | From operative sheet data |
| ops_thyroid_scintigraphy | VARCHAR | 0.0 | From operative sheet data |
| ops_tumor_side | VARCHAR | 16.9 | From operative sheet data |

### Source: parathyroid_notes_intent_v1 (13 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| para_specimen_included | BOOLEAN | 13.7 | From parathyroid_notes_intent_v1 |
| para_removal_intent | VARCHAR | 33.7 | From parathyroid_notes_intent_v1 |
| para_incidental_status_refined | VARCHAR | 33.7 | From parathyroid_notes_intent_v1 |
| para_has_pathologic_glands | BOOLEAN | 33.7 | From parathyroid_notes_intent_v1 |
| para_abnormality_type | VARCHAR | 18.0 | From parathyroid_notes_intent_v1 |
| para_n_glands_identified | BIGINT | 33.7 | From parathyroid_notes_intent_v1 |
| para_n_glands_biopsied | BIGINT | 33.7 | From parathyroid_notes_intent_v1 |
| para_n_glands_excised | BIGINT | 33.7 | From parathyroid_notes_intent_v1 |
| para_max_cellularity_pct | DOUBLE | 3.5 | From parathyroid_notes_intent_v1 |
| para_min_cellularity_pct | DOUBLE | 3.5 | From parathyroid_notes_intent_v1 |
| para_max_gland_weight_g | DOUBLE | 4.0 | From parathyroid_notes_intent_v1 |
| para_source_workbook | VARCHAR | 33.7 | From parathyroid_notes_intent_v1 |
| para_source_script | VARCHAR | 33.7 | From parathyroid_notes_intent_v1 |

### Source: path_synoptics (40 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| syn_adenomatoid_nodules | BOOLEAN | 10.8 | From synoptic pathology reports |
| syn_architecture | VARCHAR | 100.0 | From synoptic pathology reports |
| syn_bilateral_neck_dissection | BOOLEAN | 0.1 | From synoptic pathology reports |
| syn_c_cell_hyperplasia | BOOLEAN | 0.5 | From synoptic pathology reports |
| syn_capsular_invasion_clean | VARCHAR | 10.4 | From synoptic pathology reports |
| syn_carcinoma_on_frozen | BOOLEAN | 32.2 | From synoptic pathology reports |
| syn_central_dissection | BOOLEAN | 6.0 | From synoptic pathology reports |
| syn_chronic_thyroiditis | BOOLEAN | 10.1 | From synoptic pathology reports |
| syn_colloid_nodule | BOOLEAN | 3.8 | From synoptic pathology reports |
| syn_follicular_adenoma | BOOLEAN | 8.5 | From synoptic pathology reports |
| syn_frozen_section | BOOLEAN | 79.2 | From synoptic pathology reports |
| syn_frozen_section_result | VARCHAR | 37.4 | From synoptic pathology reports |
| syn_graves | BOOLEAN | 5.3 | From synoptic pathology reports |
| syn_has_second_tumor | BOOLEAN | 100.0 | From synoptic pathology reports |
| syn_has_third_plus_tumor | BOOLEAN | 100.0 | From synoptic pathology reports |
| syn_hashimoto | BOOLEAN | 2.3 | From synoptic pathology reports |
| syn_histologic_grade | BIGINT | 3.3 | From synoptic pathology reports |
| syn_hurthle_cell_change | BOOLEAN | 5.9 | From synoptic pathology reports |
| syn_hyperplastic_nodules | BOOLEAN | 4.5 | From synoptic pathology reports |
| syn_io_rln_monitoring | BOOLEAN | 1.1 | From synoptic pathology reports |
| syn_isthmus_size_cm | VARCHAR | 36.6 | From synoptic pathology reports |
| syn_isthmus_weight_g | DOUBLE | 1.1 | From synoptic pathology reports |
| syn_ki67_index | VARCHAR | 0.2 | From synoptic pathology reports |
| syn_left_lobe_size_cm | VARCHAR | 66.3 | From synoptic pathology reports |
| syn_left_lobe_weight_g | DOUBLE | 38.7 | From synoptic pathology reports |
| syn_lymphatic_invasion_clean | VARCHAR | 31.6 | From synoptic pathology reports |
| syn_margin_distance_mm | VARCHAR | 100.0 | From synoptic pathology reports |
| syn_margin_status_synoptic | VARCHAR | 36.4 | From synoptic pathology reports |
| syn_mitotic_rate_numeric | DOUBLE | 6.4 | From synoptic pathology reports |
| syn_mitotic_rate_qualifier | VARCHAR | 3.7 | From synoptic pathology reports |
| syn_multinodular_goiter | BOOLEAN | 55.9 | From synoptic pathology reports |
| syn_n_parathyroid_identified | BIGINT | 100.0 | From synoptic pathology reports |
| syn_n_tumors_in_synoptic | BIGINT | 100.0 | From synoptic pathology reports |
| syn_necrosis_clean | VARCHAR | 6.7 | From synoptic pathology reports |
| syn_parathyroid_in_specimen | BOOLEAN | 53.9 | From synoptic pathology reports |
| syn_right_lobe_size_cm | VARCHAR | 64.9 | From synoptic pathology reports |
| syn_right_lobe_weight_g | DOUBLE | 37.4 | From synoptic pathology reports |
| syn_total_weight_g | DOUBLE | 35.3 | From synoptic pathology reports |
| syn_tumor2_histologic_type | VARCHAR | 12.4 | From synoptic pathology reports |
| syn_tumor2_size_cm | VARCHAR | 12.0 | From synoptic pathology reports |

### Source: patient_refined_master_clinical_v12 (26 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| prm_ete_imaging_path_concordance | VARCHAR | 37.1 | From patient_refined_master_clinical_v12 |
| prm_ete_path_confirmed | BOOLEAN | 100.0 | From patient_refined_master_clinical_v12 |
| prm_ete_rule_applied | VARCHAR | 32.6 | From patient_refined_master_clinical_v12 |
| prm_first_fna_date | DATE | 46.7 | From patient_refined_master_clinical_v12 |
| prm_fna_n_sources | INTEGER | 48.3 | From patient_refined_master_clinical_v12 |
| prm_fna_source_tables | VARCHAR | 48.3 | From patient_refined_master_clinical_v12 |
| prm_followup_clinical_events | INTEGER | 100.0 | From patient_refined_master_clinical_v12 |
| prm_followup_has_complications | BOOLEAN | 100.0 | From patient_refined_master_clinical_v12 |
| prm_followup_tg_labs | INTEGER | 100.0 | From patient_refined_master_clinical_v12 |
| prm_high_risk_marker_any | BOOLEAN | 94.8 | From patient_refined_master_clinical_v12 |
| prm_hypocalcemia_lab_flag | BOOLEAN | 9.4 | From patient_refined_master_clinical_v12 |
| prm_hypoparathyroidism_lab_flag | BOOLEAN | 9.4 | From patient_refined_master_clinical_v12 |
| prm_imaging_data_completeness | VARCHAR | 37.1 | From patient_refined_master_clinical_v12 |
| prm_last_fna_date | DATE | 46.7 | From patient_refined_master_clinical_v12 |
| prm_margin_confidence | VARCHAR | 36.4 | From patient_refined_master_clinical_v12 |
| prm_margin_source | VARCHAR | 36.4 | From patient_refined_master_clinical_v12 |
| prm_margin_with_gross_ete | VARCHAR | 36.4 | From patient_refined_master_clinical_v12 |
| prm_molecular_risk_category | VARCHAR | 92.2 | From patient_refined_master_clinical_v12 |
| prm_n_recurrence_sources | INTEGER | 100.0 | From patient_refined_master_clinical_v12 |
| prm_recurrence_detection_category | VARCHAR | 100.0 | From patient_refined_master_clinical_v12 |
| prm_rln_worst_grade | VARCHAR | 100.0 | From patient_refined_master_clinical_v12 |
| prm_size_concordance | VARCHAR | 37.1 | From patient_refined_master_clinical_v12 |
| prm_structural_disease_flag | BOOLEAN | 7.9 | From patient_refined_master_clinical_v12 |
| prm_tg_adequate_followup | BOOLEAN | 100.0 | From patient_refined_master_clinical_v12 |
| prm_first_fna_days_from_surg | INTEGER | 35.2 | Days from first surgery date (negative = before surgery) |
| prm_last_fna_days_from_surg | INTEGER | 35.2 | Days from first surgery date (negative = before surgery) |

### Source: tg_timeline_patient_summary_v1 (9 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| tg_last_censored | BOOLEAN | 23.2 | From tg_timeline_patient_summary_v1 |
| tg_last_value | DOUBLE | 23.3 | From tg_timeline_patient_summary_v1 |
| tg_mean | DOUBLE | 23.2 | From tg_timeline_patient_summary_v1 |
| tg_n_measurements | BIGINT | 23.6 | From tg_timeline_patient_summary_v1 |
| tg_nadir | DOUBLE | 23.5 | From tg_timeline_patient_summary_v1 |
| tg_peak | DOUBLE | 23.5 | From tg_timeline_patient_summary_v1 |
| tg_rising_flag | BOOLEAN | 25.3 | From tg_timeline_patient_summary_v1 |
| tg_trajectory_class | VARCHAR | 25.0 | From tg_timeline_patient_summary_v1 |
| tg_below_threshold_ever | BOOLEAN | 23.6 | From tg_timeline_patient_summary_v1 |

### Source: thyroid_weight_data (7 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| gland_weight_combined_g | DOUBLE | 83.6 | From thyroid_weight_data |
| gland_weight_final_g | DOUBLE | 84.0 | From thyroid_weight_data |
| gland_weight_isthmus_g | DOUBLE | 0.9 | From thyroid_weight_data |
| gland_weight_left_lobe_g | DOUBLE | 31.5 | From thyroid_weight_data |
| gland_weight_right_lobe_g | DOUBLE | 31.0 | From thyroid_weight_data |
| gland_weight_source | VARCHAR | 86.6 | From thyroid_weight_data |
| gland_weight_total_reported_g | DOUBLE | 49.4 | From thyroid_weight_data |

### Source: ultrasound_reports (LN US subset) (12 columns)

| Column | Type | Coverage% | Description |
|--------|------|-----------|-------------|
| lnus_has_dedicated_exam | BOOLEAN | 0.6 | Dedicated lymph node ultrasound data |
| lnus_n_exams | INTEGER | 0.6 | Dedicated lymph node ultrasound data |
| lnus_first_date | DATE | 0.1 | Dedicated lymph node ultrasound data |
| lnus_last_date | DATE | 0.1 | Dedicated lymph node ultrasound data |
| lnus_indication_first | VARCHAR | 0.6 | Dedicated lymph node ultrasound data |
| lnus_impression_last | VARCHAR | 0.5 | Dedicated lymph node ultrasound data |
| lnus_abnormal_ln_any | BOOLEAN | 0.6 | Dedicated lymph node ultrasound data |
| lnus_normal_ln_any | BOOLEAN | 0.6 | Dedicated lymph node ultrasound data |
| lnus_has_size_measurement | BOOLEAN | 0.6 | Dedicated lymph node ultrasound data |
| lnus_source | VARCHAR | 0.6 | Dedicated lymph node ultrasound data |
| lnus_first_days_from_surg | INTEGER | 0.1 | Days from first surgery date (negative = before surgery) |
| lnus_last_days_from_surg | INTEGER | 0.1 | Days from first surgery date (negative = before surgery) |
