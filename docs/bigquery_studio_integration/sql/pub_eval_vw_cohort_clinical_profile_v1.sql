-- pub_eval.vw_cohort_clinical_profile_v1
-- Patient-grain clinical profile over canonical_patient_master + the US lymph-node
-- rollup. Gives the pub_eval evaluation layer real clinical depth (lymph-node
-- pathology, lymph-node imaging, histopathologic / operative findings, diagnosis,
-- histologic variants) beyond the modality-coverage workup census.
-- Full canonical dataset, one row per patient (10,871 rows).
-- Built 2026-05-15, migration mig_cw_clinical_profile_qc_20260515.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_eval.vw_cohort_clinical_profile_v1`
OPTIONS (
  description = "Patient-grain clinical profile over canonical_patient_master + US LN rollup: lymph-node pathology, lymph-node imaging, histopathologic/operative findings, diagnosis, and histologic variants. Built 2026-05-15 (mig_cw_clinical_profile_qc_20260515) to give the pub_eval evaluation layer real clinical depth beyond the 16-metric workup-census dashboard. Full canonical dataset, one row per patient."
) AS
SELECT
  m.research_id,
  -- Diagnosis / pathology classification
  m.is_malignant,
  m.diagnosis_primary,
  m.histology_final,
  m.histologic_variants_all,
  m.aggressive_variant_flag,
  m.bethesda_final,
  m.ata_risk_category,
  m.ajcc8_stage_group_resolved,
  -- Lymph nodes: pathology
  (m.ln_positive_final = 1)                              AS ln_path_positive,
  (m.ln_positive_final IS NOT NULL)                      AS ln_path_status_known,
  m.path_ln_examined_raw                                 AS ln_examined_n,
  m.tp_ln_central_positive                               AS ln_central_positive_n,
  m.tp_ln_lateral_positive                               AS ln_lateral_positive_n,
  (m.tp_ln_ene = 1)                                      AS ln_ene_present,  -- NOTE: source only holds 1/NULL; cannot represent "evaluated, absent"
  m.ln_burden_band,
  -- Lymph nodes: imaging
  (us.has_us_ln_findings IS NOT NULL)                    AS us_ln_evaluated,
  COALESCE(us.any_us_ln_suspicious, FALSE)               AS us_ln_suspicious,
  us.n_us_ln_suspicious,
  us.max_us_ln_size_cm,
  COALESCE(m.ct_ln_suspicious_any, FALSE)                AS ct_ln_suspicious,
  COALESCE(m.ct_pathologic_ln_any, FALSE)                AS ct_pathologic_ln,
  COALESCE(m.imaging_ln_abnormal, FALSE)                 AS imaging_ln_abnormal_any,
  -- Histopathologic / operative findings
  COALESCE(m.ete_any_present_path, FALSE)                AS ete_present,
  m.ete_grade_final_v2,
  COALESCE(m.lvi_any_present_path, FALSE)                AS lvi_present,
  COALESCE(m.margin_involved_any, FALSE)                 AS margin_involved,
  COALESCE(m.multifocality_flag_v2, FALSE)               AS multifocal,
  COALESCE(m.bilateral_disease_flag, FALSE)              AS bilateral_disease,
  m.tumor_size_cm_dominant
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` m
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_lymph_node_patient_rollup_v2` us
  USING (research_id);
