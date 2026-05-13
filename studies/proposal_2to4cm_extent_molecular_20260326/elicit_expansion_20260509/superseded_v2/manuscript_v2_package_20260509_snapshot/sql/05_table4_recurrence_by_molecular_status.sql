-- 05_table4_recurrence_by_molecular_status.sql
-- Path-proven (biopsy/op-path) recurrence by molecular group + mutation class.
-- Per user direction: structural and any-recurrence reported only as cross-checks;
-- the headline metric is recurrence_path_proven from canonical_recurrence_resolved_v1.

WITH base AS (
  SELECT
    CAST(pc.research_id AS STRING) AS research_id_s,
    pc.fna_bethesda_final AS bethesda,
    pc.imaging_nodule_size_cm AS preop_size_cm,
    pc.mol_platform,
    pc.molecular_risk_tier,
    pc.braf_positive_final, pc.ras_positive_final, pc.tert_positive_final,
    pc.histology_final,
    pc.any_recurrence_flag, pc.structural_recurrence_flag
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` pc
  WHERE pc.surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM pc.surg_first_date) BETWEEN 1999 AND 2025
    AND pc.surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
    AND pc.histology_final IS NOT NULL  -- malignant cases only
),
joined AS (
  SELECT
    b.*,
    rr.recurrence_path_proven,
    rr.recurrence_path_proven_source,
    rr.days_to_path_proven
  FROM base b
  LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_resolved_v1` rr
    ON b.research_id_s = rr.research_id
)
SELECT
  CASE
    WHEN mol_platform IN ('Afirma','ThyroSeq') THEN mol_platform
    WHEN mol_platform = 'Other' THEN 'Other / historical / in-house'
    ELSE 'Untested'
  END AS molecular_group,
  CASE
    WHEN braf_positive_final OR tert_positive_final THEN 'high_risk_mutation'
    WHEN ras_positive_final THEN 'ras_only'
    WHEN molecular_risk_tier = 'wild_type' THEN 'wild_type'
    WHEN molecular_risk_tier IS NULL THEN 'no_result'
    ELSE 'other_intermediate'
  END AS mutation_class,
  COUNT(*) AS n_malignant,
  COUNTIF(recurrence_path_proven) AS n_path_proven_recurrence,
  COUNTIF(structural_recurrence_flag) AS n_structural_recurrence,
  COUNTIF(any_recurrence_flag) AS n_any_recurrence
FROM joined
GROUP BY molecular_group, mutation_class
ORDER BY molecular_group, mutation_class;
