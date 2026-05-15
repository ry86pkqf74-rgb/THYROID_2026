-- pub_eval.vw_patient_workup_census_v1
-- Patient-level workup census. One row per patient (canonical_patient_master spine, 10,871).
-- For US/CT/MRI/FNA/nuclear-medicine: preop/postop performed flags + day intervals,
-- plus reoperation signals and prior-thyroid-procedure pathology-gap review flags.
-- Built 2026-05-14, BigQuery Studio Integration Plan.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_eval.vw_patient_workup_census_v1`
OPTIONS (
  description = 'Patient-level workup census. One row per patient (canonical_patient_master spine, 10,871). For US/CT/MRI/FNA/nuclear-medicine: was the modality done before surgery, how long before, was it done after surgery, how long after. Plus reoperation signals and a prior-thyroid-procedure pathology-gap review flag. Surgery anchor = canonical_patient_master.first_surgery_date (SURG01 QC rule documents that first_surgery_date / surg_first_date / first_surgery_date_v2 can disagree; surgery_anchor_date is exposed for transparency). US/CT/MRI pre/post are derived from the canonical *_first_date / *_last_date columns, so n_exams is a patient total and the postop reference date is the last exam; FNA and nuclear medicine are event-level so their pre/post counts and first dates are exact. Nuclear-medicine dates come from pub_eval.vw_nuclear_med_dated_v1. prior_procedure_path_gap_flag is a chart-review trigger, not a definitive determination. Built 2026-05-14, BigQuery Studio Integration Plan.'
) AS
WITH
anchor AS (
  SELECT
    research_id,
    first_surgery_date AS surgery_anchor_date,
    ct_first_date, ct_last_date, ct_n_exams,
    mri_first_date, mri_last_date, mri_n_exams,
    us_first_exam_date, us_last_exam_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
),
fna AS (
  SELECT
    e.research_id,
    COUNTIF(e.fna_date_resolved <= a.surgery_anchor_date) AS fna_preop_n,
    COUNTIF(e.fna_date_resolved >  a.surgery_anchor_date) AS fna_postop_n,
    MIN(IF(e.fna_date_resolved <= a.surgery_anchor_date, e.fna_date_resolved, NULL)) AS fna_preop_first_date,
    MIN(IF(e.fna_date_resolved >  a.surgery_anchor_date, e.fna_date_resolved, NULL)) AS fna_postop_first_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_fna_events_v1` e
  JOIN anchor a USING (research_id)
  WHERE e.fna_date_resolved IS NOT NULL
  GROUP BY e.research_id
),
nm AS (
  SELECT
    e.research_id,
    COUNT(*) AS nucmed_n_total,
    COUNTIF(e.scandate_resolved IS NOT NULL) AS nucmed_n_dated,
    COUNTIF(e.scandate_resolved IS NOT NULL AND e.scandate_resolved <= a.surgery_anchor_date) AS nucmed_preop_n,
    COUNTIF(e.scandate_resolved IS NOT NULL AND e.scandate_resolved >  a.surgery_anchor_date) AS nucmed_postop_n,
    MIN(IF(e.scandate_resolved <= a.surgery_anchor_date, e.scandate_resolved, NULL)) AS nucmed_preop_first_date,
    MIN(IF(e.scandate_resolved >  a.surgery_anchor_date, e.scandate_resolved, NULL)) AS nucmed_postop_first_date
  FROM `thyroid-canonical-pub-2026.pub_eval.vw_nuclear_med_dated_v1` e
  JOIN anchor a USING (research_id)
  GROUP BY e.research_id
),
op AS (
  SELECT
    research_id, n_surgeries, n_total_thyroidectomies, n_completion_thyroidectomies,
    n_central_neck_dissections, n_lateral_neck_dissections, any_reoperative_field,
    earliest_surgery_date, latest_surgery_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1_1`
),
psh AS (
  SELECT
    research_id,
    psh_prior_thyroidectomy_any_evidence, psh_prior_neck_surgery_any_evidence,
    psh_prior_parathyroidectomy_any_evidence, psh_prior_neck_dissection_any_evidence,
    psh_prior_fna_any_evidence, psh_prior_rai_any_evidence
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_psh_patient_rollup_v1`
),
path_gland AS (
  SELECT research_id, TRUE AS has_path_gland_row
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_gland_patient_rollup_v1`
  GROUP BY research_id
),
completion AS (
  SELECT
    research_id,
    lobectomy_first_flag,
    LOWER(path_completion_definite_flag) = 'true' AS completion_path_definite_flag
  FROM `thyroid-canonical-pub-2026.pub_canonical.patient_completion_oed_path_linkage_v1`
)
SELECT
  a.research_id,
  a.surgery_anchor_date,

  -- ===== ULTRASOUND (patient-level first/last from canonical_patient_master) =====
  (a.us_first_exam_date IS NOT NULL AND a.us_first_exam_date <= a.surgery_anchor_date) AS us_preop_performed,
  IF(a.us_first_exam_date <= a.surgery_anchor_date, a.us_first_exam_date, NULL) AS us_preop_first_date,
  IF(a.us_first_exam_date <= a.surgery_anchor_date, DATE_DIFF(a.surgery_anchor_date, a.us_first_exam_date, DAY), NULL) AS us_preop_interval_days,
  (a.us_last_exam_date IS NOT NULL AND a.us_last_exam_date > a.surgery_anchor_date) AS us_postop_performed,
  IF(a.us_last_exam_date > a.surgery_anchor_date, a.us_last_exam_date, NULL) AS us_postop_last_date,
  IF(a.us_last_exam_date > a.surgery_anchor_date, DATE_DIFF(a.us_last_exam_date, a.surgery_anchor_date, DAY), NULL) AS us_postop_interval_days,

  -- ===== CT =====
  (a.ct_first_date IS NOT NULL AND a.ct_first_date <= a.surgery_anchor_date) AS ct_preop_performed,
  IF(a.ct_first_date <= a.surgery_anchor_date, a.ct_first_date, NULL) AS ct_preop_first_date,
  IF(a.ct_first_date <= a.surgery_anchor_date, DATE_DIFF(a.surgery_anchor_date, a.ct_first_date, DAY), NULL) AS ct_preop_interval_days,
  (a.ct_last_date IS NOT NULL AND a.ct_last_date > a.surgery_anchor_date) AS ct_postop_performed,
  IF(a.ct_last_date > a.surgery_anchor_date, a.ct_last_date, NULL) AS ct_postop_last_date,
  IF(a.ct_last_date > a.surgery_anchor_date, DATE_DIFF(a.ct_last_date, a.surgery_anchor_date, DAY), NULL) AS ct_postop_interval_days,
  a.ct_n_exams,

  -- ===== MRI =====
  (a.mri_first_date IS NOT NULL AND a.mri_first_date <= a.surgery_anchor_date) AS mri_preop_performed,
  IF(a.mri_first_date <= a.surgery_anchor_date, a.mri_first_date, NULL) AS mri_preop_first_date,
  IF(a.mri_first_date <= a.surgery_anchor_date, DATE_DIFF(a.surgery_anchor_date, a.mri_first_date, DAY), NULL) AS mri_preop_interval_days,
  (a.mri_last_date IS NOT NULL AND a.mri_last_date > a.surgery_anchor_date) AS mri_postop_performed,
  IF(a.mri_last_date > a.surgery_anchor_date, a.mri_last_date, NULL) AS mri_postop_last_date,
  IF(a.mri_last_date > a.surgery_anchor_date, DATE_DIFF(a.mri_last_date, a.surgery_anchor_date, DAY), NULL) AS mri_postop_interval_days,
  a.mri_n_exams,

  -- ===== FNA (event-level, exact pre/post) =====
  (COALESCE(fna.fna_preop_n, 0) > 0) AS fna_preop_performed,
  fna.fna_preop_first_date,
  IF(fna.fna_preop_first_date IS NOT NULL, DATE_DIFF(a.surgery_anchor_date, fna.fna_preop_first_date, DAY), NULL) AS fna_preop_interval_days,
  COALESCE(fna.fna_preop_n, 0) AS fna_preop_n,
  (COALESCE(fna.fna_postop_n, 0) > 0) AS fna_postop_performed,
  fna.fna_postop_first_date,
  IF(fna.fna_postop_first_date IS NOT NULL, DATE_DIFF(fna.fna_postop_first_date, a.surgery_anchor_date, DAY), NULL) AS fna_postop_interval_days,
  COALESCE(fna.fna_postop_n, 0) AS fna_postop_n,

  -- ===== NUCLEAR MEDICINE (event-level, dates parsed in vw_nuclear_med_dated_v1) =====
  (COALESCE(nm.nucmed_n_total, 0) > 0) AS nucmed_any_on_file,
  COALESCE(nm.nucmed_n_total, 0) AS nucmed_n_total,
  (COALESCE(nm.nucmed_n_dated, 0) > 0) AS nucmed_date_resolved,
  (COALESCE(nm.nucmed_preop_n, 0) > 0) AS nucmed_preop_performed,
  nm.nucmed_preop_first_date,
  IF(nm.nucmed_preop_first_date IS NOT NULL, DATE_DIFF(a.surgery_anchor_date, nm.nucmed_preop_first_date, DAY), NULL) AS nucmed_preop_interval_days,
  COALESCE(nm.nucmed_preop_n, 0) AS nucmed_preop_n,
  (COALESCE(nm.nucmed_postop_n, 0) > 0) AS nucmed_postop_performed,
  nm.nucmed_postop_first_date,
  IF(nm.nucmed_postop_first_date IS NOT NULL, DATE_DIFF(nm.nucmed_postop_first_date, a.surgery_anchor_date, DAY), NULL) AS nucmed_postop_interval_days,
  COALESCE(nm.nucmed_postop_n, 0) AS nucmed_postop_n,

  -- ===== PREOP WORKUP COMPLETENESS (US, CT, MRI, FNA, nuclear medicine) =====
  (
    CAST((a.us_first_exam_date IS NOT NULL AND a.us_first_exam_date <= a.surgery_anchor_date) AS INT64)
  + CAST((a.ct_first_date     IS NOT NULL AND a.ct_first_date     <= a.surgery_anchor_date) AS INT64)
  + CAST((a.mri_first_date    IS NOT NULL AND a.mri_first_date    <= a.surgery_anchor_date) AS INT64)
  + CAST((COALESCE(fna.fna_preop_n, 0) > 0) AS INT64)
  + CAST((COALESCE(nm.nucmed_preop_n, 0) > 0) AS INT64)
  ) AS n_preop_modalities,
  ROUND((
    CAST((a.us_first_exam_date IS NOT NULL AND a.us_first_exam_date <= a.surgery_anchor_date) AS INT64)
  + CAST((COALESCE(fna.fna_preop_n, 0) > 0) AS INT64)
  ) / 2.0, 2) AS preop_core_workup_score,
  CASE
    WHEN (a.us_first_exam_date IS NOT NULL AND a.us_first_exam_date <= a.surgery_anchor_date)
         AND COALESCE(fna.fna_preop_n, 0) > 0 THEN 'core_complete'
    WHEN (a.us_first_exam_date IS NOT NULL AND a.us_first_exam_date <= a.surgery_anchor_date)
         OR  COALESCE(fna.fna_preop_n, 0) > 0 THEN 'partial'
    ELSE 'sparse'
  END AS preop_workup_tier,

  -- ===== SURGERY / REOPERATION =====
  op.n_surgeries,
  op.n_total_thyroidectomies,
  op.n_completion_thyroidectomies,
  op.n_central_neck_dissections,
  op.n_lateral_neck_dissections,
  op.earliest_surgery_date,
  op.latest_surgery_date,
  COALESCE(op.any_reoperative_field, FALSE) AS any_reoperative_field,
  (
    COALESCE(op.n_surgeries, 0) > 1
    OR COALESCE(op.any_reoperative_field, FALSE)
    OR COALESCE(op.n_completion_thyroidectomies, 0) > 0
  ) AS reoperation_flag,

  -- ===== PRIOR THYROID PROCEDURE / PATHOLOGY GAP (review flags) =====
  COALESCE(psh.psh_prior_thyroidectomy_any_evidence, FALSE)     AS prior_thyroidectomy_documented,
  COALESCE(psh.psh_prior_neck_surgery_any_evidence, FALSE)      AS prior_neck_surgery_documented,
  COALESCE(psh.psh_prior_parathyroidectomy_any_evidence, FALSE) AS prior_parathyroidectomy_documented,
  COALESCE(psh.psh_prior_fna_any_evidence, FALSE)               AS prior_fna_documented,
  (
    COALESCE(psh.psh_prior_thyroidectomy_any_evidence, FALSE)
    OR COALESCE(psh.psh_prior_neck_surgery_any_evidence, FALSE)
    OR COALESCE(psh.psh_prior_parathyroidectomy_any_evidence, FALSE)
  ) AS prior_thyroid_procedure_documented,
  COALESCE(pg.has_path_gland_row, FALSE) AS has_surgical_pathology_on_file,
  (
    (
      COALESCE(psh.psh_prior_thyroidectomy_any_evidence, FALSE)
      OR COALESCE(psh.psh_prior_neck_surgery_any_evidence, FALSE)
      OR COALESCE(psh.psh_prior_parathyroidectomy_any_evidence, FALSE)
    )
    AND NOT COALESCE(pg.has_path_gland_row, FALSE)
  ) AS prior_procedure_path_gap_flag,
  COALESCE(comp.lobectomy_first_flag, FALSE) AS lobectomy_first_flag,
  COALESCE(comp.completion_path_definite_flag, FALSE) AS completion_path_definite_flag,
  (
    COALESCE(comp.lobectomy_first_flag, FALSE)
    AND NOT COALESCE(comp.completion_path_definite_flag, FALSE)
  ) AS lobectomy_first_no_completion_path_flag,

  CURRENT_TIMESTAMP() AS view_evaluated_at
FROM anchor a
LEFT JOIN fna         ON fna.research_id  = a.research_id
LEFT JOIN nm          ON nm.research_id   = a.research_id
LEFT JOIN op          ON op.research_id   = a.research_id
LEFT JOIN psh         ON psh.research_id  = a.research_id
LEFT JOIN path_gland pg   ON pg.research_id   = a.research_id
LEFT JOIN completion comp ON comp.research_id = a.research_id;
