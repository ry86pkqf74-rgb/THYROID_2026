-- READY FOR LOGAN MANUSCRIPT REFINEMENT
-- mig_195 — Table 1 analytic cohort characteristics (starter SQL)
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com> (Cursor-authored template)
-- Posture: READ / COPY. Execute on MotherDuck after mig_188b → mig_186b → mig_185b → mig_187 apply.
--
-- Analytic cohort: CPM malignant spine intersect patients retaining ≥1 malignant path event
-- post mig_186b NIFTP/UMP exclusion (still listed in canonical_path_malignant_events_v1).
--
-- Exports: COPY (final SELECT) TO 'table_1_cohort_characteristics.csv' (HEADER, DELIMITER ',');
--   or run each UNION branch and paste into manuscript Table 1.

USE thyroid_canonical_publication_v1_0;

WITH malignant_pts AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_malignant_events_v1
),
base AS (
  SELECT
    c.*,
    CAST(c.research_id AS VARCHAR) AS rid_v,
    COALESCE(s.vital_status_current, CAST(c.vital_status AS VARCHAR)) AS vital_for_table1,
    /* Tumor size: path-primary; see CF tumor_size_cm_max multi-surgery under-report if needed */
    CAST(COALESCE(c.path_tumor_size_cm, c.tumor_size_cm_max) AS DOUBLE) AS tumor_size_cm_analysis,
    /* Histology bucketing for Table 1 */
    CASE
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%anaplastic%'
        OR LOWER(c.histology_final) LIKE '%atc%'
      ) THEN 'ATC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%medullary%'
        OR LOWER(c.histology_final) LIKE '%mtc%'
      ) THEN 'MTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%follicular%'
        AND LOWER(c.histology_final) NOT LIKE '%papillary%'
      ) THEN 'FTC'
      WHEN c.histology_final IS NOT NULL AND (
        LOWER(c.histology_final) LIKE '%papillary%'
        OR LOWER(c.histology_final) LIKE '%ptc%'
        OR LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%ptc%'
      ) THEN 'PTC'
      WHEN c.histology_final IS NOT NULL AND LOWER(c.histology_final) LIKE '%differentiated%'
        THEN 'DTC_nonspecific'
      WHEN c.histology_final IS NOT NULL AND TRIM(CAST(c.histology_final AS VARCHAR)) <> ''
        THEN 'other_or_mixed'
      WHEN LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%papillary%'
        THEN 'PTC'
      WHEN LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%follicular%'
        THEN 'FTC'
      WHEN LOWER(COALESCE(c.histologic_types_all, '')) LIKE '%medullary%'
        THEN 'MTC'
      ELSE 'unknown'
    END AS histology_bucket,
    /* ETE three-way display */
    CASE
      WHEN LOWER(COALESCE(c.ete_grade_final_v2, '')) IN ('gross') THEN 'gross'
      WHEN LOWER(COALESCE(c.ete_grade_final_v2, '')) IN ('microscopic', 'present_ungraded', 'minimal')
        OR LOWER(COALESCE(c.ete_grade_final_v2, '')) LIKE '%micro%'
        THEN 'minimal_or_ungraded_present'
      WHEN c.ete_grade_final_v2 IS NULL
        OR LOWER(COALESCE(c.ete_grade_final_v2, '')) IN ('none', 'absent', '')
        THEN 'absent_or_unknown'
      ELSE 'other'
    END AS ete_display_bucket,
    /* Surgery type */
    CASE
      WHEN c.surg_total_thyroidectomy IS TRUE THEN 'total_thyroidectomy'
      WHEN c.surg_hemithyroidectomy IS TRUE THEN 'hemithyroidectomy'
      WHEN (
        LOWER(COALESCE(c.surg_procedure_type, '')) LIKE '%neck%'
        OR LOWER(COALESCE(c.surg_procedure_type, '')) LIKE '%dissection%'
      )
        AND COALESCE(c.surg_total_thyroidectomy, FALSE) IS NOT TRUE
        AND COALESCE(c.surg_hemithyroidectomy, FALSE) IS NOT TRUE
        THEN 'neck_dissection_or_other_non_ttf_hemi'
      ELSE COALESCE(NULLIF(TRIM(CAST(c.surg_procedure_type AS VARCHAR)), ''), 'unknown')
    END AS surgery_group
  FROM main.canonical_patient_master AS c
  INNER JOIN malignant_pts AS mp ON CAST(c.research_id AS VARCHAR) = mp.research_id
  LEFT JOIN main.canonical_survival_followup_v1 AS s
    ON CAST(c.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
  WHERE c.is_malignant IS TRUE
),
denom AS (
  SELECT COUNT(*)::BIGINT AS n FROM base
)

/* Long-format rows: Logan copies to manuscript; extend with additional UNION arms as needed */
SELECT 1 AS sort_key, 'Cohort' AS characteristic, 'Analytic (malignant path event post-exclusion)' AS level,
       d.n::VARCHAR AS n, NULL::VARCHAR AS pct, 'N' AS statistic
FROM denom d

UNION ALL
SELECT 10, 'Age at first surgery (years)', 'Mean ± SD',
       CAST(ROUND(AVG(age_at_surgery), 2) AS VARCHAR) || ' ± ' || CAST(ROUND(STDDEV_POP(age_at_surgery), 2) AS VARCHAR),
       NULL, 'continuous'
FROM base CROSS JOIN denom

UNION ALL
SELECT 11, 'Age at first surgery (years)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(age_at_surgery, 0.5), 2) AS VARCHAR)
         || ' (' || CAST(ROUND(quantile_cont(age_at_surgery, 0.25), 2) AS VARCHAR)
         || '–' || CAST(ROUND(quantile_cont(age_at_surgery, 0.75), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base CROSS JOIN denom

UNION ALL
SELECT 20, 'Sex', 'Female',
       CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('female', 'f')) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('female', 'f')) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base

UNION ALL
SELECT 21, 'Sex', 'Male',
       CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('male', 'm')) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('male', 'm')) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base

UNION ALL
SELECT 25, 'Race / ethnicity', COALESCE(NULLIF(TRIM(CAST(race AS VARCHAR)), ''), 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(NULLIF(TRIM(CAST(race AS VARCHAR)), ''), 'unknown')

UNION ALL
SELECT 30, 'Histology (bucket)', histology_bucket,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY histology_bucket

UNION ALL
SELECT 40, 'Tumor size primary (cm)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.5), 2) AS VARCHAR)
         || ' (' || CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.25), 2) AS VARCHAR)
         || '–' || CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.75), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base CROSS JOIN denom
WHERE tumor_size_cm_analysis IS NOT NULL

UNION ALL
SELECT 41, 'Tumor size — <1 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis < 1) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis < 1) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 42, 'Tumor size — 1–2 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 1 AND tumor_size_cm_analysis < 2) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 1 AND tumor_size_cm_analysis < 2) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 43, 'Tumor size — 2–4 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 2 AND tumor_size_cm_analysis <= 4) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 2 AND tumor_size_cm_analysis <= 4) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 44, 'Tumor size — >4 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis > 4) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis > 4) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 50, 'AJCC8 stage group (resolved)', COALESCE(ajcc8_stage_group_resolved, 'unstaged_or_unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(ajcc8_stage_group_resolved, 'unstaged_or_unknown')

UNION ALL
SELECT 60, 'T stage (resolved)', COALESCE(ajcc8_t_stage_resolved, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(ajcc8_t_stage_resolved, 'unknown')

UNION ALL
SELECT 61, 'N stage (resolved)', COALESCE(ajcc8_n_stage_resolved, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(ajcc8_n_stage_resolved, 'unknown')

UNION ALL
SELECT 62, 'M stage (resolved)', COALESCE(ajcc8_m_stage_resolved, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(ajcc8_m_stage_resolved, 'unknown')

UNION ALL
SELECT 70, 'Multifocality (path)', '≥2 tumors',
       CAST(COUNT(*) FILTER (WHERE COALESCE(multifocal_flag_path, FALSE) IS TRUE OR COALESCE(n_tumors_path, 1) > 1) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(multifocal_flag_path, FALSE) IS TRUE OR COALESCE(n_tumors_path, 1) > 1) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 80, 'Extrathyroidal extension', ete_display_bucket,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY ete_display_bucket

UNION ALL
SELECT 90, 'Lymphovascular invasion (any)', 'Present',
       CAST(COUNT(*) FILTER (WHERE COALESCE(lvi_any_present_path, FALSE) IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(lvi_any_present_path, FALSE) IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 100, 'Margin (R-class)', COALESCE(r_class_true, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(r_class_true, 'unknown')

UNION ALL
SELECT 110, 'LN positive (any)', 'Yes',
       CAST(COUNT(*) FILTER (WHERE COALESCE(ln_positive_flag, FALSE) IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(ln_positive_flag, FALSE) IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 111, 'M1 at presentation (resolved)', 'Yes',
       CAST(COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc8_m_stage_resolved, '')) = 'M1') AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc8_m_stage_resolved, '')) = 'M1') / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 120, 'Surgery group', surgery_group,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY surgery_group

UNION ALL
SELECT 130, 'RAI ever', 'Yes (rai_first_date present)',
       CAST(COUNT(*) FILTER (WHERE rai_first_date IS NOT NULL) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE rai_first_date IS NOT NULL) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 140, 'Recurrence ever', 'Yes',
       CAST(COUNT(*) FILTER (WHERE COALESCE(any_recurrence_flag, FALSE) IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(any_recurrence_flag, FALSE) IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base CROSS JOIN denom
GROUP BY d.n

UNION ALL
SELECT 150, 'Follow-up (years from first surgery to last contact)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(
         DATE_DIFF('day', CAST(first_surgery_date AS DATE), CAST(last_contact_date AS DATE)) / 365.25, 0.5
       ), 2) AS VARCHAR) || ' (' ||
       CAST(ROUND(quantile_cont(
         DATE_DIFF('day', CAST(first_surgery_date AS DATE), CAST(last_contact_date AS DATE)) / 365.25, 0.25
       ), 2) AS VARCHAR) || '–' ||
       CAST(ROUND(quantile_cont(
         DATE_DIFF('day', CAST(first_surgery_date AS DATE), CAST(last_contact_date AS DATE)) / 365.25, 0.75
       ), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base CROSS JOIN denom
WHERE first_surgery_date IS NOT NULL AND last_contact_date IS NOT NULL

UNION ALL
SELECT 160, 'Vital status (last contact)', COALESCE(vital_for_table1, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base
GROUP BY COALESCE(vital_for_table1, 'unknown')

ORDER BY sort_key, characteristic, level;
