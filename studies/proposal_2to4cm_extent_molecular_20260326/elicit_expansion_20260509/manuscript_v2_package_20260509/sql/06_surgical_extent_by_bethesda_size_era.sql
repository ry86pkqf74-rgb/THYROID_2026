-- 06_surgical_extent_by_bethesda_size_era.sql
-- Total thyroidectomy vs lobectomy rates stratified by Bethesda × size × era.
-- Used for Table 2b.

WITH base AS (
  SELECT
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS preop_size_cm,
    EXTRACT(YEAR FROM surg_first_date) AS surgery_year,
    surg_total_thyroidectomy, surg_hemithyroidectomy
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
)
SELECT
  bethesda,
  CASE
    WHEN preop_size_cm IS NULL THEN 'unknown'
    WHEN preop_size_cm < 2.0 THEN 'lt2cm'
    WHEN preop_size_cm BETWEEN 2.0 AND 4.0 THEN '2to4cm'
    WHEN preop_size_cm > 4.0 THEN 'gt4cm'
  END AS size_band,
  CASE WHEN surgery_year < 2015 THEN 'pre_2015' ELSE '2015_plus' END AS era,
  COUNT(*) AS n_total,
  COUNTIF(surg_total_thyroidectomy) AS n_total_thyroid,
  COUNTIF(surg_hemithyroidectomy) AS n_lobectomy
FROM base
WHERE bethesda IS NOT NULL
GROUP BY bethesda, size_band, era
ORDER BY bethesda, size_band, era;
