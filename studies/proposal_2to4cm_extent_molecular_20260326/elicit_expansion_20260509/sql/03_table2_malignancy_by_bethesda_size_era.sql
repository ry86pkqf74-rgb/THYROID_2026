-- 03_table2_malignancy_by_bethesda_size_era.sql
-- Malignancy rate stratified by Bethesda × size_band × era.
-- Histology classifier: see data_dictionary.md §"Histology classification rules".

WITH base AS (
  SELECT
    research_id,
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS preop_size_cm,
    EXTRACT(YEAR FROM surg_first_date) AS surgery_year,
    histology_final,
    LOWER(TRIM(IFNULL(histology_final,''))) AS histo_lower,
    surg_procedure_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
classified AS (
  SELECT
    research_id, bethesda, preop_size_cm, surgery_year, histology_final,
    CASE
      WHEN preop_size_cm IS NULL THEN 'unknown'
      WHEN preop_size_cm < 2.0 THEN 'lt2cm'
      WHEN preop_size_cm BETWEEN 2.0 AND 4.0 THEN '2to4cm'
      WHEN preop_size_cm > 4.0 THEN 'gt4cm'
    END AS size_band,
    CASE WHEN surgery_year < 2015 THEN 'pre_2015' ELSE '2015_plus' END AS era,
    CASE
      WHEN histo_lower IS NULL OR histo_lower = '' THEN NULL
      WHEN histo_lower LIKE '%ptc%' OR histo_lower LIKE '%papillary%'
        OR histo_lower LIKE '%mtc%' OR histo_lower LIKE '%medullary%'
        OR histo_lower LIKE '%follicular carcinoma%'
        OR histo_lower LIKE '%anaplastic%'
        OR histo_lower LIKE '%poorly differentiated%' OR histo_lower LIKE '%pooly differentiated%' OR histo_lower LIKE '%poorly differentied%'
        OR histo_lower LIKE '%differentiated high grade%' OR histo_lower LIKE '%differentiated thyroid carcinoma%'
        OR histo_lower LIKE '%nut carcinoma%' OR histo_lower LIKE '%adenoid cystic%'
        OR histo_lower LIKE '%angiosarcoma%' OR histo_lower LIKE '%high grade carcinoma%'
        OR histo_lower LIKE '%infiltrating carcinoma%' OR histo_lower LIKE '%metastatic%'
        THEN 'malignant'
      WHEN histo_lower LIKE '%niftp%' OR histo_lower LIKE '%nifcp%' OR histo_lower LIKE '%nifp%' OR histo_lower LIKE '%nifpt%' THEN 'niftp'
      WHEN histo_lower LIKE '%ftump%' OR histo_lower LIKE '%hyalinizing trabecular tumor%' THEN 'borderline'
      WHEN histo_lower LIKE '%adenoma%' AND histo_lower NOT LIKE '%adenoid%' THEN 'benign_adenoma'
      ELSE 'benign_other'
    END AS malignancy_class
  FROM base
)
SELECT
  bethesda, size_band, era,
  COUNT(*) AS n_total,
  COUNTIF(malignancy_class IS NOT NULL) AS n_with_histology,
  COUNTIF(malignancy_class = 'malignant') AS n_malignant_strict,
  COUNTIF(malignancy_class IN ('malignant','niftp','borderline')) AS n_malignant_inclusive,
  COUNTIF(malignancy_class = 'niftp') AS n_niftp,
  COUNTIF(malignancy_class = 'borderline') AS n_borderline
FROM classified
WHERE bethesda IS NOT NULL
GROUP BY bethesda, size_band, era
ORDER BY bethesda, size_band, era;
