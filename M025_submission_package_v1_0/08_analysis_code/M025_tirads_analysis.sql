-- M025 — reproducibility excerpts (MotherDuck: thyroid_canonical_publication_v1_0)
-- Cohort spine + tirads_resolved (post mig_288) + gold-standard malignancy flag.

USE thyroid_canonical_publication_v1_0;

-- §1 Row count gate (expects ~3,375 post mig_280 footprint)
SELECT 'cohort_m025_tirads_performance_v1' AS object_name, COUNT(*) AS n_rows
FROM manuscript_workspace.cohort_m025_tirads_performance_v1;

-- §2 TIRADS enum join + malignant counts by resolved category
SELECT
  pm.tirads_resolved,
  COUNT(*) AS n_patients,
  COUNT_IF(pm.is_malignant IS TRUE) AS n_malignant,
  ROUND(100.0 * COUNT_IF(pm.is_malignant IS TRUE) / NULLIF(COUNT(*), 0), 2) AS rom_pct
FROM manuscript_workspace.cohort_m025_tirads_performance_v1 c
LEFT JOIN main.canonical_patient_master pm
  ON CAST(c.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
GROUP BY pm.tirads_resolved
ORDER BY pm.tirads_resolved;

-- §3 Fallback rank from cohort scores when tirads_resolved NULL (audit only)
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(pm.tirads_resolved IS NOT NULL AND TRIM(CAST(pm.tirads_resolved AS VARCHAR)) <> '') AS n_tirads_resolved_nonnull,
  COUNT_IF(pm.tirads_resolved IS NULL OR TRIM(CAST(pm.tirads_resolved AS VARCHAR)) = '') AS n_tirads_resolved_null,
  COUNT_IF(c.tirads_worst_score_v12 IS NOT NULL OR c.tirads_best_score_v12 IS NOT NULL) AS n_has_numeric_score_fallback
FROM manuscript_workspace.cohort_m025_tirads_performance_v1 c
LEFT JOIN main.canonical_patient_master pm
  ON CAST(c.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR);
