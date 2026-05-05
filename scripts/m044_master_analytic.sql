WITH cohort AS (
  SELECT
    c.*,
    CASE
      WHEN c.ete_grade_final IN ('no_negative','false','absent','none') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'        THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'              THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'   THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group,
    CASE
      WHEN c.lvi_grade ILIKE 'extensiv%'            THEN 'extensive'
      WHEN c.lvi_grade IN ('present','preesent')    THEN 'present'
      WHEN c.lvi_grade = 'focal'                    THEN 'focal'
      WHEN c.lvi_grade IS NULL                      THEN 'missing'
      WHEN c.lvi_grade IN ('indeterminate','indetermiante','indeeterminate','indeterminent','suspicious','x','c/a','no','n/s')
                                                    THEN 'indeterminate'
      ELSE 'indeterminate'
    END AS lvi_clean,
    COALESCE(c.vascular_invasion_final, 'missing')  AS vasc_clean,
    CASE
      WHEN c.histology_final IN ('PTC','differentiated thyroid carcinoma','high-grade PTC with thymic-like features',
                                 'infiltrating carcinoma with thymus-like differentiation','poorly differentiated PTC')
                                                                  THEN 'PTC'
      WHEN c.histology_final = 'follicular carcinoma'             THEN 'FTC'
      WHEN c.histology_final ILIKE 'metastatic PTC%'
        OR c.histology_final ILIKE 'recurrent/metastatic PTC%'
        OR c.histology_final ILIKE 'metastatic thyroid carcinoma%'
        OR c.histology_final ILIKE 'metastatic follicular carcinoma%'
        OR c.histology_final = 'recurrent/metastatic follicular carcinoma'
                                                                  THEN 'Metastatic-PTC'
      WHEN c.histology_final ILIKE '%poorly differentiated%'
        OR c.histology_final ILIKE '%poorly differntiated%'       THEN 'Poorly-differentiated DTC'
      WHEN c.histology_final ILIKE '%high grade%'
        OR c.histology_final ILIKE '%high-grade%'                 THEN 'High-grade DTC'
      WHEN c.histology_final IN ('MTC','metastatic MTC','recurrent MTC','MTC/PTC mixed composite')
                                                                  THEN 'NON-DTC: MTC'
      WHEN c.histology_final IN ('anaplastic carcinoma','metastatic anaplastic carcinoma','metastatic PTC/anaplastic carcinoma')
                                                                  THEN 'NON-DTC: Anaplastic'
      WHEN c.histology_final IN ('NIFTP','FTUMP','follicular adenoma','atypical follicular adenoma','Atypical hurthle cell neoplasm')
                                                                  THEN 'NON-DTC: NIFTP/borderline'
      WHEN c.histology_final IN ('NUT carcinoma','adenoid cystic carcinoma','high grade carcinoma with focal squamous features')
                                                                  THEN 'NON-DTC: Other rare'
      ELSE 'Unclassified'
    END AS histology_dtc_5level,
    CASE
      WHEN c.histology_final IN ('MTC','metastatic MTC','recurrent MTC','MTC/PTC mixed composite',
                                 'anaplastic carcinoma','metastatic anaplastic carcinoma','metastatic PTC/anaplastic carcinoma',
                                 'NIFTP','FTUMP','follicular adenoma','atypical follicular adenoma','Atypical hurthle cell neoplasm',
                                 'NUT carcinoma','adenoid cystic carcinoma') THEN 0
      ELSE 1
    END AS strict_dtc_include
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
),
ln AS (
  SELECT research_id,
    MAX(ln_total_examined) AS ln_examined, MAX(ln_total_positive) AS ln_positive,
    MAX(ln_central_examined) AS ln_central_examined, MAX(ln_central_positive) AS ln_central_positive,
    MAX(ln_lateral_left_positive) AS ln_lateral_left_positive,
    MAX(ln_lateral_right_positive) AS ln_lateral_right_positive,
    MAX(ln_bilateral_lateral_positive) AS ln_bilateral_lateral_positive,
    MAX(ln_level_vi_positive) AS ln_level_vi_positive,
    MAX(ln_level_vii_positive) AS ln_level_vii_positive,
    MAX(ln_extranodal_extension) AS ln_ene
  FROM manuscript_workspace.ln_master_rollup_v1 GROUP BY research_id
),
reop AS (
  SELECT research_id,
    MAX(n_surgeries) AS n_surgeries, MAX(second_surgery_date) AS second_surgery_date,
    MAX(days_between_first_second_surgery) AS days_to_2nd,
    MAX(completion_reason) AS completion_reason,
    MAX(completion_reason_confidence) AS completion_reason_confidence,
    MAX(completion_histology_type) AS completion_histology_type,
    MAX(op_reoperative_any) AS op_reoperative_any
  FROM manuscript_workspace.cohort_m040_reoperative_v1 GROUP BY research_id
),
rec AS (
  SELECT research_id, recurrence_path_proven, recurrence_path_proven_date, recurrence_path_proven_source,
         recurrence_imaging_suspicious, recurrence_imaging_suspicious_date, recurrence_imaging_then_path_confirmed,
         recurrence_status_final, days_to_path_proven, days_to_imaging_suspicious, is_implausible_date_quarantine
  FROM main.canonical_recurrence_resolved_v1
),
cpm AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         race, bmi_combined,
         multifocal_flag_path, bilateral_disease_flag, aggressive_variant_flag,
         margin_involved_any, closest_margin_mm,
         capsular_invasion_v6, capsular_ordinal_worst,
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
         rai_max_dose_mci, rai_dose_v9, rai_intent_v9, rai_first_episode_days_from_surg,
         ata_initial_risk, ata_response_category,
         ops_prior_neck_irradiation, ops_prior_neck_operation
  FROM main.canonical_patient_master
)
SELECT
  c.research_id, c.ete_group, c.ete_grade_final, c.ete_grade, c.ete_grade_source,
  c.gross_ete_flag, c.path_gross_ete_flag, c.ete_op_note_grade, c.ete_original_grade,
  c.strict_dtc_include, c.histology_dtc_5level, c.histology_final,
  c.age_at_surgery, c.sex, c.tumor_size_cm,
  c.ajcc8_t_stage, c.ajcc8_n_stage, c.ajcc8_m_stage, c.ajcc8_stage_group,
  c.ata_risk_category, c.surg_procedure_type, c.surg_first_date,
  c.followup_years, c.overall_survival_years, c.death_occurred,
  c.lvi_clean, c.vasc_clean, c.lvi_grade, c.vascular_invasion_final,
  ln.ln_examined, ln.ln_positive, ln.ln_central_examined, ln.ln_central_positive,
  ln.ln_lateral_left_positive, ln.ln_lateral_right_positive, ln.ln_bilateral_lateral_positive,
  ln.ln_level_vi_positive, ln.ln_level_vii_positive, ln.ln_ene,
  CASE WHEN ln.ln_central_positive > 0 THEN 1 ELSE 0 END AS central_pos_flag,
  CASE WHEN COALESCE(ln.ln_lateral_left_positive,0) > 0
         OR COALESCE(ln.ln_lateral_right_positive,0) > 0
         OR COALESCE(ln.ln_bilateral_lateral_positive,0) > 0 THEN 1 ELSE 0 END AS lateral_pos_flag,
  c.ln_positive_flag, c.ln_total_positive AS ln_total_positive_view,
  c.rai_received_flag,
  rec.recurrence_path_proven, rec.recurrence_path_proven_date, rec.recurrence_path_proven_source,
  rec.recurrence_imaging_suspicious, rec.recurrence_imaging_suspicious_date,
  rec.recurrence_imaging_then_path_confirmed, rec.recurrence_status_final,
  rec.days_to_path_proven, rec.days_to_imaging_suspicious, rec.is_implausible_date_quarantine,
  CASE WHEN rec.recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN TRUE ELSE FALSE END AS recurrence_composite,
  c.any_recurrence_flag, c.structural_recurrence_flag,
  reop.n_surgeries, reop.days_to_2nd, reop.second_surgery_date,
  reop.completion_reason, reop.completion_reason_confidence, reop.completion_histology_type, reop.op_reoperative_any,
  cpm.race, cpm.bmi_combined,
  cpm.multifocal_flag_path, cpm.bilateral_disease_flag, cpm.aggressive_variant_flag,
  cpm.margin_involved_any, cpm.closest_margin_mm,
  cpm.capsular_invasion_v6, cpm.capsular_ordinal_worst,
  cpm.syn_hashimoto, cpm.syn_graves, cpm.syn_chronic_thyroiditis,
  cpm.pmhx_nlp_diabetes, cpm.pmhx_nlp_hypertension, cpm.pmhx_nlp_hypothyroidism, cpm.pmhx_nlp_hyperthyroidism,
  cpm.pmhx_nlp_obesity, cpm.pmhx_nlp_smoking_status,
  cpm.pmhx_nlp_family_hx_thyroid, cpm.pmhx_nlp_family_hx_cancer,
  cpm.pmhx_nlp_radiation_exposure, cpm.pmhx_nlp_men_syndrome,
  cpm.pmhx_nlp_prior_cancer_hx, cpm.pmhx_nlp_breast_cancer,
  cpm.pmhx_nlp_cad, cpm.pmhx_nlp_ckd, cpm.pmhx_nlp_copd, cpm.pmhx_nlp_depression,
  cpm.pmhx_nlp_autoimmune_thyroid_hx,
  cpm.braf_positive_final, cpm.tert_positive_final, cpm.ras_positive_final, cpm.ret_positive_unified,
  cpm.molecular_tested_confirmed,
  cpm.surg_total_thyroidectomy, cpm.ages_score, cpm.ages_calculable_flag,
  cpm.rai_max_dose_mci, cpm.rai_dose_v9, cpm.rai_intent_v9, cpm.rai_first_episode_days_from_surg,
  cpm.ata_initial_risk, cpm.ata_response_category,
  cpm.ops_prior_neck_irradiation, cpm.ops_prior_neck_operation
FROM cohort c
LEFT JOIN ln USING (research_id)
LEFT JOIN reop USING (research_id)
LEFT JOIN rec USING (research_id)
LEFT JOIN cpm USING (research_id)
ORDER BY c.research_id
