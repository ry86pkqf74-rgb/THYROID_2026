SELECT CAST(research_id AS VARCHAR) AS research_id,
  race, bmi_combined,
  multifocal_flag_path, bilateral_disease_flag, aggressive_variant_flag,
  margin_involved_any, closest_margin_mm, capsular_invasion_v6, capsular_ordinal_worst,
  syn_hashimoto, syn_graves, syn_chronic_thyroiditis,
  pmhx_nlp_diabetes, pmhx_nlp_hypertension, pmhx_nlp_hypothyroidism, pmhx_nlp_hyperthyroidism,
  pmhx_nlp_obesity, pmhx_nlp_smoking_status,
  pmhx_nlp_family_hx_thyroid, pmhx_nlp_family_hx_cancer,
  pmhx_nlp_radiation_exposure, pmhx_nlp_men_syndrome,
  pmhx_nlp_prior_cancer_hx, pmhx_nlp_breast_cancer,
  pmhx_nlp_cad, pmhx_nlp_ckd, pmhx_nlp_copd, pmhx_nlp_depression,
  pmhx_nlp_autoimmune_thyroid_hx,
  braf_positive_final, tert_positive_final, ras_positive_final, ret_positive_unified,
  molecular_tested_confirmed,
  surg_total_thyroidectomy, ages_score, ages_calculable_flag,
  rai_received_flag, rai_max_dose_mci, rai_dose_v9, rai_intent_v9, rai_first_episode_days_from_surg,
  ata_initial_risk, ata_risk_category, ata_response_category,
  ops_prior_neck_irradiation, ops_prior_neck_operation,
  first_surgery_date AS first_surgery_date_cpm
FROM main.canonical_patient_master
WHERE research_id IN (SELECT research_id FROM manuscript_workspace.cohort_m044_ajcc_ete_v1)
ORDER BY research_id
