-- pub_canonical.canonical_patient_workup_census_v1
-- Materialized patient-level workup census, promoted into pub_canonical to sit alongside
-- the other canonical_*_patient_rollup tables. One row per patient (10,871), 65 columns,
-- clustered on research_id. Sourced from pub_eval.vw_patient_workup_census_v1.
-- Migration: mig_cw_workup_census_canonical_20260514. Built 2026-05-14.

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_workup_census_v1`
CLUSTER BY research_id
OPTIONS (
  description = 'Canonical patient-level workup census. One row per patient (10,871). Materialized from pub_eval.vw_patient_workup_census_v1 on 2026-05-14 and promoted into pub_canonical to sit alongside the other canonical_*_patient_rollup tables. For US/CT/MRI/FNA/nuclear-medicine: preop/postop performed flags, first/last dates, and day intervals; plus reoperation signals and prior-thyroid-procedure pathology-gap review flags. CAVEATS (carried per-column): US/CT/MRI pre/post are derived from canonical_patient_master *_first_date / *_last_date (patient-level, so postop reference = last exam, n_exams = patient total); FNA and nuclear medicine are event-level and exact. Surgery anchor = first_surgery_date (SURG01 QC rule documents 3 surgery-date columns can disagree). prior_procedure_path_gap_flag is a chart-review trigger, not a determination. Built by Cowork / BigQuery Studio Integration Plan.'
) AS
SELECT
  * EXCEPT(view_evaluated_at),
  CURRENT_TIMESTAMP() AS build_ts,
  'cowork_bigquery_studio_integration_v1' AS build_script
FROM `thyroid-canonical-pub-2026.pub_eval.vw_patient_workup_census_v1`;
