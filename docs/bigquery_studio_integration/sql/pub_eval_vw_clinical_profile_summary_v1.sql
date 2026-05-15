-- pub_eval.vw_clinical_profile_summary_v1
-- Long-format roll-up of vw_cohort_clinical_profile_v1 (one row per
-- metric_group / metric / category) built to drive the Looker Studio dashboard
-- with clinical depth (LN pathology, LN imaging, histopath findings, diagnoses,
-- histologic variants) beyond the modality-coverage workup census.
-- Built 2026-05-15, migration mig_cw_clinical_profile_qc_20260515.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_eval.vw_clinical_profile_summary_v1`
OPTIONS (
  description = "Long-format roll-up of vw_cohort_clinical_profile_v1 for the Looker dashboard: one row per (metric_group, metric, category) with patient count and percent of the full canonical cohort. Built 2026-05-15 (mig_cw_clinical_profile_qc_20260515) so the pub_eval dashboard has clinical depth (LN path/imaging, histopath findings, diagnoses, variants) beyond modality coverage."
) AS
WITH base AS (
  SELECT * FROM `thyroid-canonical-pub-2026.pub_eval.vw_cohort_clinical_profile_v1`
),
total AS (SELECT COUNT(*) AS n_total FROM base),
rolled AS (
  -- Diagnosis classification
  SELECT 'diagnosis' AS metric_group, 'is_malignant' AS metric,
         CASE WHEN is_malignant THEN 'malignant' WHEN is_malignant IS FALSE THEN 'non-malignant' ELSE '(unknown)' END AS category,
         COUNT(*) AS n FROM base GROUP BY 3
  UNION ALL
  SELECT 'diagnosis', 'diagnosis_primary', COALESCE(diagnosis_primary,'(null)'), COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'diagnosis', 'bethesda_final', COALESCE(CAST(bethesda_final AS STRING),'(null)'), COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'diagnosis', 'ata_risk_category', COALESCE(ata_risk_category,'(null)'), COUNT(*) FROM base GROUP BY 3
  UNION ALL
  -- Histologic variants (malignant only)
  SELECT 'variants', 'histologic_variants_all', COALESCE(NULLIF(TRIM(histologic_variants_all),''),'(none recorded)'), COUNT(*)
  FROM base WHERE is_malignant GROUP BY 3
  UNION ALL
  SELECT 'variants', 'aggressive_variant_flag', CASE WHEN aggressive_variant_flag THEN 'aggressive variant' ELSE 'not flagged' END, COUNT(*)
  FROM base WHERE is_malignant GROUP BY 3
  UNION ALL
  -- Lymph nodes: pathology
  SELECT 'ln_pathology', 'ln_path_positive', CASE WHEN NOT ln_path_status_known THEN '(status unknown)' WHEN ln_path_positive THEN 'node-positive' ELSE 'node-negative' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_pathology', 'ln_burden_band', COALESCE(ln_burden_band,'(null)'), COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_pathology', 'ln_central_positive', CASE WHEN ln_central_positive_n > 0 THEN 'central positive' ELSE 'no central positive / NA' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_pathology', 'ln_lateral_positive', CASE WHEN ln_lateral_positive_n > 0 THEN 'lateral positive' ELSE 'no lateral positive / NA' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_pathology', 'ln_ene_present', CASE WHEN ln_ene_present THEN 'ENE documented present' ELSE 'ENE not documented (absent or not evaluated)' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  -- Lymph nodes: imaging
  SELECT 'ln_imaging', 'us_ln_suspicious', CASE WHEN us_ln_suspicious THEN 'US suspicious node(s)' ELSE 'no US suspicious node' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_imaging', 'ct_ln_suspicious', CASE WHEN ct_ln_suspicious THEN 'CT suspicious node(s)' ELSE 'no CT suspicious node' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_imaging', 'ct_pathologic_ln', CASE WHEN ct_pathologic_ln THEN 'CT pathologic node(s)' ELSE 'no CT pathologic node' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'ln_imaging', 'imaging_ln_abnormal_any', CASE WHEN imaging_ln_abnormal_any THEN 'any abnormal nodal imaging' ELSE 'no abnormal nodal imaging' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  -- Histopathologic / operative findings
  SELECT 'histopath', 'ete_present', CASE WHEN ete_present THEN 'ETE present' ELSE 'no ETE' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'histopath', 'ete_grade_final_v2', COALESCE(ete_grade_final_v2,'(null)'), COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'histopath', 'lvi_present', CASE WHEN lvi_present THEN 'LVI present' ELSE 'no LVI' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'histopath', 'margin_involved', CASE WHEN margin_involved THEN 'margin involved' ELSE 'margin not involved' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'histopath', 'multifocal', CASE WHEN multifocal THEN 'multifocal' ELSE 'unifocal / NA' END, COUNT(*) FROM base GROUP BY 3
  UNION ALL
  SELECT 'histopath', 'bilateral_disease', CASE WHEN bilateral_disease THEN 'bilateral' ELSE 'not bilateral / NA' END, COUNT(*) FROM base GROUP BY 3
)
SELECT
  r.metric_group, r.metric, r.category, r.n,
  ROUND(100 * r.n / t.n_total, 1) AS pct_of_cohort,
  t.n_total AS cohort_n,
  CURRENT_DATE() AS as_of_date
FROM rolled r CROSS JOIN total t
ORDER BY metric_group, metric, n DESC;
