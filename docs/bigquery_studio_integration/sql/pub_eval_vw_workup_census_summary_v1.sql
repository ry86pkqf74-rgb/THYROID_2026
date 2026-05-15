-- pub_eval.vw_workup_census_summary_v1
-- Long-format aggregate roll-up of vw_patient_workup_census_v1 (one row per metric),
-- built to drive the Looker Studio dashboard.
-- Built 2026-05-14, BigQuery Studio Integration Plan.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_eval.vw_workup_census_summary_v1`
OPTIONS (
  description = 'Aggregate roll-up of vw_patient_workup_census_v1, long format (one row per metric) for dashboards. metric_group = imaging_modality rows give preop/postop coverage and interval medians per modality. metric_group = cohort_flag rows give patient-level flag prevalences. Built 2026-05-14, BigQuery Studio Integration Plan, global evaluation layer.'
) AS
WITH c AS (
  SELECT * FROM `thyroid-canonical-pub-2026.pub_eval.vw_patient_workup_census_v1`
),
total AS (SELECT COUNT(*) AS n_patients FROM c),
modality AS (
  SELECT 'ultrasound' AS metric, us_preop_performed AS preop, us_preop_interval_days AS preop_iv, us_postop_performed AS postop, us_postop_interval_days AS postop_iv FROM c
  UNION ALL SELECT 'ct', ct_preop_performed, ct_preop_interval_days, ct_postop_performed, ct_postop_interval_days FROM c
  UNION ALL SELECT 'mri', mri_preop_performed, mri_preop_interval_days, mri_postop_performed, mri_postop_interval_days FROM c
  UNION ALL SELECT 'fna', fna_preop_performed, fna_preop_interval_days, fna_postop_performed, fna_postop_interval_days FROM c
  UNION ALL SELECT 'nuclear_medicine', nucmed_preop_performed, nucmed_preop_interval_days, nucmed_postop_performed, nucmed_postop_interval_days FROM c
),
modality_summary AS (
  SELECT
    'imaging_modality' AS metric_group,
    metric,
    (SELECT n_patients FROM total) AS n_patients,
    COUNTIF(preop) AS n_preop_performed,
    ROUND(100 * COUNTIF(preop) / (SELECT n_patients FROM total), 1) AS pct_preop_performed,
    APPROX_QUANTILES(IF(preop, preop_iv, NULL), 100)[OFFSET(25)] AS preop_interval_days_p25,
    APPROX_QUANTILES(IF(preop, preop_iv, NULL), 100)[OFFSET(50)] AS preop_interval_days_median,
    APPROX_QUANTILES(IF(preop, preop_iv, NULL), 100)[OFFSET(75)] AS preop_interval_days_p75,
    COUNTIF(postop) AS n_postop_performed,
    ROUND(100 * COUNTIF(postop) / (SELECT n_patients FROM total), 1) AS pct_postop_performed,
    APPROX_QUANTILES(IF(postop, postop_iv, NULL), 100)[OFFSET(50)] AS postop_interval_days_median
  FROM modality
  GROUP BY metric
),
flags AS (
  SELECT 'cohort_flag' AS metric_group, 'reoperation' AS metric,
    (SELECT n_patients FROM total) AS n_patients,
    COUNTIF(reoperation_flag) AS n_flagged,
    ROUND(100 * COUNTIF(reoperation_flag) / (SELECT n_patients FROM total), 1) AS pct_flagged
  FROM c
  UNION ALL SELECT 'cohort_flag', 'completion_thyroidectomy', (SELECT n_patients FROM total),
    COUNTIF(COALESCE(n_completion_thyroidectomies,0) > 0),
    ROUND(100 * COUNTIF(COALESCE(n_completion_thyroidectomies,0) > 0) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'prior_thyroid_procedure_documented', (SELECT n_patients FROM total),
    COUNTIF(prior_thyroid_procedure_documented),
    ROUND(100 * COUNTIF(prior_thyroid_procedure_documented) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'prior_procedure_path_gap', (SELECT n_patients FROM total),
    COUNTIF(prior_procedure_path_gap_flag),
    ROUND(100 * COUNTIF(prior_procedure_path_gap_flag) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'lobectomy_first_no_completion_path', (SELECT n_patients FROM total),
    COUNTIF(lobectomy_first_no_completion_path_flag),
    ROUND(100 * COUNTIF(lobectomy_first_no_completion_path_flag) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'no_surgical_pathology_on_file', (SELECT n_patients FROM total),
    COUNTIF(NOT has_surgical_pathology_on_file),
    ROUND(100 * COUNTIF(NOT has_surgical_pathology_on_file) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'nucmed_scans_on_file', (SELECT n_patients FROM total),
    COUNTIF(nucmed_any_on_file),
    ROUND(100 * COUNTIF(nucmed_any_on_file) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'nucmed_scans_undateable', (SELECT n_patients FROM total),
    COUNTIF(nucmed_any_on_file AND NOT nucmed_date_resolved),
    ROUND(100 * COUNTIF(nucmed_any_on_file AND NOT nucmed_date_resolved) / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'preop_workup_core_complete', (SELECT n_patients FROM total),
    COUNTIF(preop_workup_tier = 'core_complete'),
    ROUND(100 * COUNTIF(preop_workup_tier = 'core_complete') / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'preop_workup_partial', (SELECT n_patients FROM total),
    COUNTIF(preop_workup_tier = 'partial'),
    ROUND(100 * COUNTIF(preop_workup_tier = 'partial') / (SELECT n_patients FROM total), 1) FROM c
  UNION ALL SELECT 'cohort_flag', 'preop_workup_sparse', (SELECT n_patients FROM total),
    COUNTIF(preop_workup_tier = 'sparse'),
    ROUND(100 * COUNTIF(preop_workup_tier = 'sparse') / (SELECT n_patients FROM total), 1) FROM c
)
SELECT
  metric_group, metric, n_patients,
  n_preop_performed, pct_preop_performed,
  preop_interval_days_p25, preop_interval_days_median, preop_interval_days_p75,
  n_postop_performed, pct_postop_performed, postop_interval_days_median,
  CAST(NULL AS INT64) AS n_flagged, CAST(NULL AS FLOAT64) AS pct_flagged
FROM modality_summary
UNION ALL
SELECT
  metric_group, metric, n_patients,
  CAST(NULL AS INT64), CAST(NULL AS FLOAT64),
  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS INT64),
  CAST(NULL AS INT64), CAST(NULL AS FLOAT64), CAST(NULL AS INT64),
  n_flagged, pct_flagged
FROM flags
ORDER BY metric_group, metric;
