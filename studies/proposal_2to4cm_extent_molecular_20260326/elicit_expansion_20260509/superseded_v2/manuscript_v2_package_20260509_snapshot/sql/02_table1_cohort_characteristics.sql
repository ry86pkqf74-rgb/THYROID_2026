-- 02_table1_cohort_characteristics.sql
-- Patient-level summary statistics across analytic strata.
-- Cohort: surg_first_date 1999-2025 AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy').
-- Strata: overall, lobe, total, preop_lt2cm, preop_2to4cm, preop_gt4cm, pre_2015, 2015_plus.

WITH base AS (
  SELECT
    research_id, age_at_surgery,
    LOWER(IFNULL(sex,'')) AS sex_lc,
    fna_bethesda_final AS bethesda,
    mol_platform,
    imaging_nodule_size_cm AS preop_size_cm,
    surg_procedure_type,
    surg_total_thyroidectomy, surg_hemithyroidectomy,
    EXTRACT(YEAR FROM surg_first_date) AS surgery_year,
    histology_final,
    imaging_tirads_best, imaging_tirads_worst
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
strata AS (
  SELECT 'overall' AS stratum, * FROM base
  UNION ALL SELECT 'lobe', * FROM base WHERE surg_procedure_type='hemithyroidectomy'
  UNION ALL SELECT 'total', * FROM base WHERE surg_procedure_type='total_thyroidectomy'
  UNION ALL SELECT 'preop_2to4cm', * FROM base WHERE preop_size_cm BETWEEN 2.0 AND 4.0
  UNION ALL SELECT 'preop_lt2cm', * FROM base WHERE preop_size_cm < 2.0
  UNION ALL SELECT 'preop_gt4cm', * FROM base WHERE preop_size_cm > 4.0
  UNION ALL SELECT 'pre_2015', * FROM base WHERE surgery_year < 2015
  UNION ALL SELECT '2015_plus', * FROM base WHERE surgery_year >= 2015
)
SELECT
  stratum,
  COUNT(*) AS n,
  COUNTIF(sex_lc='female') AS n_female,
  COUNTIF(sex_lc='male') AS n_male,
  ROUND(AVG(age_at_surgery),1) AS mean_age,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(50)] AS median_age,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(25)] AS p25_age,
  APPROX_QUANTILES(age_at_surgery,100)[OFFSET(75)] AS p75_age,
  COUNTIF(preop_size_cm IS NOT NULL) AS n_with_preop_size,
  ROUND(AVG(preop_size_cm),2) AS mean_preop_size_cm,
  APPROX_QUANTILES(preop_size_cm,100)[OFFSET(50)] AS median_preop_size_cm,
  COUNTIF(bethesda IS NOT NULL) AS n_with_bethesda,
  COUNTIF(bethesda=1) AS n_b1,
  COUNTIF(bethesda=2) AS n_b2,
  COUNTIF(bethesda=3) AS n_b3,
  COUNTIF(bethesda=4) AS n_b4,
  COUNTIF(bethesda=5) AS n_b5,
  COUNTIF(bethesda=6) AS n_b6,
  COUNTIF(mol_platform='Afirma') AS n_afirma,
  COUNTIF(mol_platform='ThyroSeq') AS n_thyroseq,
  COUNTIF(mol_platform IN ('Afirma','ThyroSeq')) AS n_named_platform,
  COUNTIF(imaging_tirads_worst IS NOT NULL) AS n_with_tirads,
  COUNTIF(imaging_tirads_worst >= 4) AS n_tirads_ge4,
  COUNTIF(surgery_year < 2015) AS n_pre_2015,
  COUNTIF(surgery_year >= 2015) AS n_2015_plus,
  COUNTIF(surg_total_thyroidectomy) AS n_total_thyroid,
  COUNTIF(surg_hemithyroidectomy) AS n_lobectomy,
  COUNTIF(histology_final IS NOT NULL) AS n_malignant_histology
FROM strata
GROUP BY stratum
ORDER BY CASE stratum
  WHEN 'overall' THEN 1
  WHEN 'lobe' THEN 2
  WHEN 'total' THEN 3
  WHEN 'preop_lt2cm' THEN 4
  WHEN 'preop_2to4cm' THEN 5
  WHEN 'preop_gt4cm' THEN 6
  WHEN 'pre_2015' THEN 7
  WHEN '2015_plus' THEN 8
END;
