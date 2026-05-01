-- Lane M mig_234 — Table 1 cohort demographics (v15 refresh)
-- DB: thyroid_canonical_publication_v1_0
-- Primary spine: semantic_publication.vw_patient_master_safe_VIEW_v1
-- Malignant denominator: DISTINCT patients present on vw_path_malignant_tumor_safe_VIEW_v1
-- Supplemental columns not projected by mig_223 patient safe view: main.canonical_patient_master (explicit join;
--   documented in Methods_thyroid_canonical_pub_v1_0_20260501.md §Cohort definition).
-- LN positivity manuscript SSOT: manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1 (preferred over CPM ln flag).

USE thyroid_canonical_publication_v1_0;

WITH malignant_pts AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1
),
path_patient_rollups AS (
  SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    BOOL_OR(
      CASE
        WHEN lymphatic_invasion IS NULL THEN FALSE
        WHEN TRY_CAST(lymphatic_invasion AS BOOLEAN) IS TRUE THEN TRUE
        WHEN regexp_matches(
          LOWER(CAST(lymphatic_invasion AS VARCHAR)),
          'positive|present|\\bx\\b|yes|focal|extensive'
        )
          THEN TRUE
        ELSE FALSE
      END
    ) AS lvi_any_from_path_tumors,
    MAX(COALESCE(number_of_tumors, 1)) AS max_number_of_tumors_path,
    BOOL_OR(
      CASE
        WHEN multifocality_flag IS NULL THEN FALSE
        WHEN TRY_CAST(multifocality_flag AS BOOLEAN) IS TRUE THEN TRUE
        WHEN regexp_matches(LOWER(CAST(multifocality_flag AS VARCHAR)), '^(true|t|1|yes|x)$')
          THEN TRUE
        ELSE FALSE
      END
    ) AS multifocal_flag_from_path
  FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1
  GROUP BY 1
),
base AS (
  SELECT
    pm.*,
    CAST(pm.research_id AS VARCHAR) AS rid_v,
    c.path_tumor_size_cm,
    c.first_surgery_date,
    c.last_contact_date,
    c.ln_positive_flag AS cpm_ln_positive_flag,
    c.multifocal_flag_path,
    c.n_tumors_path,
    c.lvi_any_present_path,
    c.surg_total_thyroidectomy,
    c.surg_hemithyroidectomy,
    c.surg_procedure_type,
    c.rai_first_date,
    c.ajcc8_n_stage_resolved,
    c.ajcc8_m_stage_resolved,
    COALESCE(s.vital_status_current, CAST(c.vital_status AS VARCHAR)) AS vital_for_table1,
    CAST(COALESCE(c.path_tumor_size_cm, c.tumor_size_cm_max) AS DOUBLE)
      AS tumor_size_cm_analysis,
    CASE
      WHEN ln.research_id IS NOT NULL AND COALESCE(ln.ln_total_positive_safe, 0) > 0 THEN TRUE
      WHEN COALESCE(c.ln_positive_flag, FALSE) IS TRUE THEN TRUE
      ELSE FALSE
    END AS ln_positive_any,
    CASE
      WHEN COALESCE(pr.lvi_any_from_path_tumors, FALSE) IS TRUE THEN TRUE
      WHEN COALESCE(c.lvi_any_present_path, FALSE) IS TRUE THEN TRUE
      ELSE FALSE
    END AS lvi_any_row,
    CASE
      WHEN COALESCE(pr.multifocal_flag_from_path, FALSE) IS TRUE
        OR COALESCE(pr.max_number_of_tumors_path, 1) > 1 THEN TRUE
      WHEN COALESCE(c.multifocal_flag_path, FALSE) IS TRUE
        OR COALESCE(c.n_tumors_path, 1) > 1 THEN TRUE
      ELSE FALSE
    END AS multifocal_any,
    CASE
      WHEN COALESCE(c.ajcc8_n_stage_resolved, pm.ajcc8_n_stage) IS NOT NULL
        THEN COALESCE(c.ajcc8_n_stage_resolved, CAST(pm.ajcc8_n_stage AS VARCHAR))
      ELSE 'unknown'
    END AS n_stage_disp,
    CASE
      WHEN COALESCE(c.ajcc8_m_stage_resolved, pm.ajcc8_m_stage) IS NOT NULL
        THEN COALESCE(c.ajcc8_m_stage_resolved, CAST(pm.ajcc8_m_stage AS VARCHAR))
      ELSE 'unknown'
    END AS m_stage_disp,
    CASE
      WHEN COALESCE(s.last_known_alive_date, CAST(c.last_contact_date AS DATE)) IS NOT NULL
           AND COALESCE(s.first_surgery_date, CAST(c.first_surgery_date AS DATE)) IS NOT NULL
        THEN DATE_DIFF(
          'day',
          CAST(COALESCE(s.first_surgery_date, c.first_surgery_date) AS DATE),
          CAST(COALESCE(s.last_known_alive_date, c.last_contact_date) AS DATE)
        ) / 365.25
      ELSE NULL
    END AS followup_years_from_surgery
  FROM semantic_publication.vw_patient_master_safe_VIEW_v1 AS pm
  INNER JOIN malignant_pts AS mp ON CAST(pm.research_id AS VARCHAR) = mp.research_id
  INNER JOIN main.canonical_patient_master AS c ON CAST(c.research_id AS VARCHAR) = mp.research_id
  LEFT JOIN main.canonical_survival_followup_v1 AS s
    ON CAST(c.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
  LEFT JOIN manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1 AS ln
    ON CAST(ln.research_id AS VARCHAR) = mp.research_id
  LEFT JOIN path_patient_rollups AS pr ON pr.research_id = mp.research_id
  WHERE COALESCE(pm.is_malignant, FALSE) IS TRUE
),
base_enriched AS (
  SELECT
    b.*,
    CASE
      WHEN b.histology_final IS NOT NULL AND (
        LOWER(b.histology_final) LIKE '%anaplastic%' OR LOWER(b.histology_final) LIKE '%atc%'
      ) THEN 'ATC'
      WHEN b.histology_final IS NOT NULL AND (
        LOWER(b.histology_final) LIKE '%medullary%' OR LOWER(b.histology_final) LIKE '%mtc%'
      ) THEN 'MTC'
      WHEN b.histology_final IS NOT NULL AND (
        LOWER(b.histology_final) LIKE '%follicular%' AND LOWER(b.histology_final) NOT LIKE '%papillary%'
      ) THEN 'FTC'
      WHEN b.histology_final IS NOT NULL AND (
        LOWER(b.histology_final) LIKE '%papillary%' OR LOWER(b.histology_final) LIKE '%ptc%'
        OR LOWER(COALESCE(b.histologic_types_all, '')) LIKE '%ptc%'
      ) THEN 'PTC'
      WHEN b.histology_final IS NOT NULL AND LOWER(b.histology_final) LIKE '%differentiated%'
        THEN 'DTC_nonspecific'
      WHEN b.histology_final IS NOT NULL AND TRIM(CAST(b.histology_final AS VARCHAR)) <> ''
        THEN 'other_or_mixed'
      WHEN LOWER(COALESCE(b.histologic_types_all, '')) LIKE '%papillary%' THEN 'PTC'
      WHEN LOWER(COALESCE(b.histologic_types_all, '')) LIKE '%follicular%' THEN 'FTC'
      WHEN LOWER(COALESCE(b.histologic_types_all, '')) LIKE '%medullary%' THEN 'MTC'
      ELSE 'unknown'
    END AS histology_bucket,
    CASE
      WHEN LOWER(COALESCE(b.ete_grade_final, '')) IN ('gross') THEN 'gross'
      WHEN LOWER(COALESCE(b.ete_grade_final, '')) IN ('microscopic', 'present_ungraded', 'minimal')
        OR LOWER(COALESCE(b.ete_grade_final, '')) LIKE '%micro%'
        THEN 'minimal_or_ungraded_present'
      WHEN b.ete_grade_final IS NULL OR LOWER(COALESCE(b.ete_grade_final, '')) IN ('none', 'absent', '')
        THEN 'absent_or_unknown'
      ELSE 'other'
    END AS ete_display_bucket,
    CASE
      WHEN COALESCE(b.surg_total_thyroidectomy, FALSE) IS TRUE THEN 'total_thyroidectomy'
      WHEN COALESCE(b.surg_hemithyroidectomy, FALSE) IS TRUE THEN 'hemithyroidectomy'
      WHEN (
        LOWER(COALESCE(b.surg_procedure_type, '')) LIKE '%neck%'
        OR LOWER(COALESCE(b.surg_procedure_type, '')) LIKE '%dissection%'
      )
        AND COALESCE(b.surg_total_thyroidectomy, FALSE) IS NOT TRUE
        AND COALESCE(b.surg_hemithyroidectomy, FALSE) IS NOT TRUE
        THEN 'neck_dissection_or_other_non_ttf_hemi'
      ELSE COALESCE(NULLIF(TRIM(CAST(b.surg_procedure_type AS VARCHAR)), ''), 'unknown')
    END AS surgery_group
  FROM base AS b
),
denom AS (
  SELECT COUNT(*)::BIGINT AS n FROM base_enriched
)

SELECT 1 AS sort_key, 'Cohort' AS characteristic,
       'Analytic (malignant CPM ∩ ≥1 semantic vw_path_malignant_tumor_safe row)' AS level,
       d.n::VARCHAR AS n, NULL::VARCHAR AS pct, 'N' AS statistic
FROM denom d

UNION ALL
SELECT 10, 'Age at first surgery (years)', 'Mean ± SD',
       CAST(ROUND(AVG(age_at_surgery), 2) AS VARCHAR) || ' ± ' || CAST(ROUND(STDDEV_POP(age_at_surgery), 2) AS VARCHAR),
       NULL, 'continuous'
FROM base_enriched CROSS JOIN denom

UNION ALL
SELECT 11, 'Age at first surgery (years)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(age_at_surgery, 0.5), 2) AS VARCHAR)
         || ' (' || CAST(ROUND(quantile_cont(age_at_surgery, 0.25), 2) AS VARCHAR)
         || '–' || CAST(ROUND(quantile_cont(age_at_surgery, 0.75), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base_enriched CROSS JOIN denom

UNION ALL
SELECT 20, 'Sex', 'Female',
       CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('female', 'f')) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('female', 'f')) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched

UNION ALL
SELECT 21, 'Sex', 'Male',
       CAST(COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('male', 'm')) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(sex, '')) IN ('male', 'm')) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched

UNION ALL
SELECT 25, 'Race / ethnicity', COALESCE(NULLIF(TRIM(CAST(race AS VARCHAR)), ''), 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY COALESCE(NULLIF(TRIM(CAST(race AS VARCHAR)), ''), 'unknown')

UNION ALL
SELECT 30, 'Histology (bucket)', histology_bucket,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY histology_bucket

UNION ALL
SELECT 40, 'Tumor size primary (cm)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.5), 2) AS VARCHAR)
         || ' (' || CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.25), 2) AS VARCHAR)
         || '–' || CAST(ROUND(quantile_cont(tumor_size_cm_analysis, 0.75), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base_enriched CROSS JOIN denom AS d
WHERE tumor_size_cm_analysis IS NOT NULL

UNION ALL
SELECT 41, 'Tumor size — <1 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis < 1) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis < 1) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 42, 'Tumor size — 1–2 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 1 AND tumor_size_cm_analysis < 2) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 1 AND tumor_size_cm_analysis < 2) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 43, 'Tumor size — 2–4 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 2 AND tumor_size_cm_analysis <= 4) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis >= 2 AND tumor_size_cm_analysis <= 4) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 44, 'Tumor size — >4 cm', NULL,
       CAST(COUNT(*) FILTER (WHERE tumor_size_cm_analysis > 4) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE tumor_size_cm_analysis > 4) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'bucket'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 50, 'AJCC8 stage group (resolved)', COALESCE(ajcc8_stage_group_resolved, 'unstaged_or_unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY COALESCE(ajcc8_stage_group_resolved, 'unstaged_or_unknown')

UNION ALL
SELECT 60, 'T stage (resolved)', COALESCE(ajcc8_t_stage_resolved, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY COALESCE(ajcc8_t_stage_resolved, 'unknown')

UNION ALL
SELECT 61, 'N stage (resolved / fallback)', n_stage_disp,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY n_stage_disp

UNION ALL
SELECT 62, 'M stage (resolved / fallback)', m_stage_disp,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY m_stage_disp

UNION ALL
SELECT 70, 'Multifocality (path)', '≥2 tumors',
       CAST(COUNT(*) FILTER (WHERE multifocal_any IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE multifocal_any IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 80, 'Extrathyroidal extension', ete_display_bucket,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY ete_display_bucket

UNION ALL
SELECT 90, 'Lymphovascular invasion (any)', 'Present',
       CAST(COUNT(*) FILTER (WHERE lvi_any_row IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE lvi_any_row IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 100, 'Margin (R-class)', COALESCE(margin_r_class, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY COALESCE(margin_r_class, 'unknown')

UNION ALL
SELECT 110, 'LN positive (LN safe view ∪ CPM flag)', 'Yes',
       CAST(COUNT(*) FILTER (WHERE ln_positive_any IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE ln_positive_any IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 111, 'M1 at presentation (resolved / fallback)', 'Yes',
       CAST(COUNT(*) FILTER (WHERE UPPER(COALESCE(m_stage_disp, '')) = 'M1') AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(m_stage_disp, '')) = 'M1') / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 120, 'Surgery group', surgery_group,
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY surgery_group

UNION ALL
SELECT 130, 'RAI ever', 'Yes (rai_first_date present)',
       CAST(COUNT(*) FILTER (WHERE rai_first_date IS NOT NULL) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE rai_first_date IS NOT NULL) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 140, 'Recurrence ever', 'Yes',
       CAST(COUNT(*) FILTER (WHERE COALESCE(any_recurrence_flag, FALSE) IS TRUE) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE COALESCE(any_recurrence_flag, FALSE) IS TRUE) / NULLIF(d.n, 0), 1) AS VARCHAR),
       'binary'
FROM base_enriched CROSS JOIN denom AS d
GROUP BY d.n

UNION ALL
SELECT 150, 'Follow-up (years surgery→last contact)', 'Median (IQR)',
       CAST(ROUND(quantile_cont(followup_years_from_surgery, 0.5), 2) AS VARCHAR) || ' (' ||
       CAST(ROUND(quantile_cont(followup_years_from_surgery, 0.25), 2) AS VARCHAR) || '–' ||
       CAST(ROUND(quantile_cont(followup_years_from_surgery, 0.75), 2) AS VARCHAR) || ')',
       NULL, 'continuous'
FROM base_enriched CROSS JOIN denom
WHERE followup_years_from_surgery IS NOT NULL

UNION ALL
SELECT 160, 'Vital status (last contact)', COALESCE(vital_for_table1, 'unknown'),
       CAST(COUNT(*) AS VARCHAR),
       CAST(ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM denom), 0), 1) AS VARCHAR),
       'categorical'
FROM base_enriched
GROUP BY COALESCE(vital_for_table1, 'unknown')

ORDER BY sort_key, characteristic, level;
