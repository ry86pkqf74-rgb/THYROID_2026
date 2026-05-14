-- 04b_table3_v2_actual_reported_call.sql
-- CORRECTED Table 3 — uses the actual reported call from canonical_molecular_genetics_v2
-- (not the derived call from manuscript_cohort_v1's molecular_risk_tier + mutation flags
-- which underlies sql/04_table3_diagnostic_performance.sql).
--
-- Reported-call rules:
--   Afirma   test-positive   = overall_result_class IN ('suspicious','positive')
--   Afirma   test-negative   = overall_result_class = 'negative'
--   Afirma   not classifiable = NULL or 'other' or 'non_diagnostic'
--   ThyroSeq test-positive   = rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH')
--                              OR overall_result_class = 'positive'
--   ThyroSeq test-negative   = rom_descriptor IN ('LOW','INTERMEDIATE-LOW')
--                              OR overall_result_class = 'negative'
--   ThyroSeq INTERMEDIATE    = rom_descriptor = 'INTERMEDIATE'  (REPORTED AS THIRD CATEGORY)
--   ThyroSeq not classifiable = otherwise
--
-- Patient grain: when a patient has multiple molecular tests, the latest preoperative
-- test (test_date <= surg_first_date) is used; falls back to the most recent test if no
-- preop test exists.

WITH surgical_b34 AS (
  SELECT
    CAST(research_id AS STRING) AS rid_s,
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS preop_size_cm,
    surg_first_date,
    histology_final,
    LOWER(TRIM(IFNULL(histology_final,''))) AS histo_lower
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE fna_bethesda_final IN (3,4)
    AND surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
mol AS (
  SELECT
    research_id AS rid_s,
    platform,
    overall_result_class,
    rom_descriptor,
    rom_percent_point,
    rom_percent_low,
    rom_percent_high,
    resolved_test_date,
    CASE
      WHEN platform = 'Afirma' AND overall_result_class IN ('suspicious','positive') THEN 'positive'
      WHEN platform = 'Afirma' AND overall_result_class = 'negative' THEN 'negative'
      WHEN platform = 'ThyroSeq' AND rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH') THEN 'positive'
      WHEN platform = 'ThyroSeq' AND rom_descriptor IN ('LOW','INTERMEDIATE-LOW') THEN 'negative'
      WHEN platform = 'ThyroSeq' AND rom_descriptor = 'INTERMEDIATE' THEN 'intermediate'
      WHEN platform = 'ThyroSeq' AND overall_result_class = 'positive' THEN 'positive'
      WHEN platform = 'ThyroSeq' AND overall_result_class = 'negative' THEN 'negative'
      ELSE 'unknown_or_excluded'
    END AS reported_call
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
  WHERE platform IN ('Afirma','ThyroSeq')
    -- mig_325 (2026-05-13): exclude fabricated ThyroSeq superseded / cancelled tests
    AND NOT (
      platform = 'ThyroSeq'
      AND (
        overall_result_class = 'superseded'
        OR IFNULL(platform_reclass_status, '') = 'superseded_by_afirma_row'
      )
    )
    AND IFNULL(platform_reclass_status, '') != 'non_diagnostic_cancelled'
),
joined AS (
  SELECT
    s.*, m.platform, m.overall_result_class, m.rom_descriptor,
    m.rom_percent_point, m.resolved_test_date, m.reported_call,
    ROW_NUMBER() OVER (
      PARTITION BY s.rid_s
      ORDER BY
        CASE WHEN m.resolved_test_date IS NOT NULL
              AND m.resolved_test_date <= DATE(s.surg_first_date) THEN 0 ELSE 1 END,
        m.resolved_test_date DESC
    ) AS rn
  FROM surgical_b34 s
  JOIN mol m USING (rid_s)
)
SELECT
  bethesda,
  platform,
  CASE
    WHEN preop_size_cm BETWEEN 2.0 AND 4.0 THEN '2to4cm'
    WHEN preop_size_cm < 2.0 THEN 'lt2cm'
    WHEN preop_size_cm > 4.0 THEN 'gt4cm'
    ELSE 'unknown_size'
  END AS size_band,
  reported_call,
  CASE
    WHEN histo_lower IS NULL OR histo_lower = '' THEN 'benign'
    WHEN histo_lower LIKE '%niftp%' OR histo_lower LIKE '%nifcp%' OR histo_lower LIKE '%nifp%' OR histo_lower LIKE '%nifpt%' THEN 'niftp'
    WHEN histo_lower LIKE '%ftump%' OR histo_lower LIKE '%hyalinizing trabecular tumor%' THEN 'borderline'
    WHEN histo_lower LIKE '%adenoma%' AND histo_lower NOT LIKE '%adenoid%' THEN 'benign_adenoma'
    ELSE 'malignant'
  END AS histo_class,
  COUNT(*) AS n,
  ROUND(AVG(rom_percent_point),1) AS mean_rom_pct,
  APPROX_QUANTILES(rom_percent_point, 100)[OFFSET(50)] AS median_rom_pct,
  APPROX_QUANTILES(rom_percent_point, 100)[OFFSET(25)] AS p25_rom_pct,
  APPROX_QUANTILES(rom_percent_point, 100)[OFFSET(75)] AS p75_rom_pct,
  COUNTIF(rom_percent_point IS NOT NULL) AS n_with_rom_pct
FROM joined
WHERE rn = 1
GROUP BY bethesda, platform, size_band, reported_call, histo_class
ORDER BY platform, bethesda, size_band, reported_call, histo_class;
