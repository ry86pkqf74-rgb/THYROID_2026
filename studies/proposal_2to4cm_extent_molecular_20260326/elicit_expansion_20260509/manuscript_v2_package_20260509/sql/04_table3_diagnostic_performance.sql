-- 04_table3_diagnostic_performance.sql
-- 2x2 cell counts for ThyroSeq vs Afirma vs final histology in Bethesda III/IV.
-- Wilson 95% CIs are computed in build_elicit_expansion.py — this query just
-- emits the (bethesda, mol_platform, size_band, mol_call, histo_class, n) cells.
-- Histology is binary "benign vs malignant"; benign = surgical patient with
-- histology_final IS NULL. NIFTP & borderline split out for the strict/inclusive
-- mirror tables.

WITH base AS (
  SELECT
    research_id,
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS preop_size_cm,
    EXTRACT(YEAR FROM surg_first_date) AS surgery_year,
    histology_final,
    LOWER(TRIM(IFNULL(histology_final,''))) AS histo_lower,
    mol_platform,
    molecular_risk_tier,
    braf_positive_final, ras_positive_final, tert_positive_final,
    surg_procedure_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE fna_bethesda_final IN (3,4)
    AND mol_platform IN ('Afirma','ThyroSeq')
    AND surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
classified AS (
  SELECT
    research_id, bethesda, preop_size_cm, mol_platform,
    CASE
      WHEN braf_positive_final OR ras_positive_final OR tert_positive_final THEN 'positive'
      WHEN molecular_risk_tier IN ('high','intermediate','low_intermediate') THEN 'positive'
      ELSE 'negative'
    END AS mol_call,
    CASE
      WHEN histo_lower IS NULL OR histo_lower = '' THEN 'benign'
      WHEN histo_lower LIKE '%niftp%' OR histo_lower LIKE '%nifcp%' OR histo_lower LIKE '%nifp%' OR histo_lower LIKE '%nifpt%' THEN 'niftp'
      WHEN histo_lower LIKE '%ftump%' OR histo_lower LIKE '%hyalinizing trabecular tumor%' THEN 'borderline'
      WHEN histo_lower LIKE '%adenoma%' AND histo_lower NOT LIKE '%adenoid%' THEN 'benign_adenoma'
      ELSE 'malignant'
    END AS histo_class,
    CASE
      WHEN preop_size_cm BETWEEN 2.0 AND 4.0 THEN '2to4cm'
      WHEN preop_size_cm < 2.0 THEN 'lt2cm'
      WHEN preop_size_cm > 4.0 THEN 'gt4cm'
      ELSE 'unknown_size'
    END AS size_band,
    braf_positive_final, ras_positive_final, tert_positive_final
  FROM base
)
SELECT
  bethesda, mol_platform, size_band, mol_call, histo_class,
  COUNT(*) AS n,
  COUNTIF(braf_positive_final) AS n_braf,
  COUNTIF(ras_positive_final) AS n_ras,
  COUNTIF(tert_positive_final) AS n_tert
FROM classified
GROUP BY bethesda, mol_platform, size_band, mol_call, histo_class
ORDER BY mol_platform, bethesda, size_band, mol_call, histo_class;
