-- M025 v2.0 — reproducibility excerpts (MotherDuck: thyroid_canonical_publication_v1_0)
-- Nodule-level spine (mig_306) + patient-level comparator (v1.0).

USE thyroid_canonical_publication_v1_0;

-- §1 Row counts
SELECT 'cohort_m025_nodule_level_v1' AS object_name, COUNT(*) AS n_rows
FROM manuscript_workspace.cohort_m025_nodule_level_v1;

SELECT 'cohort_m025_nodule_level_v1_strict' AS slice, COUNT(*) AS n_rows
FROM manuscript_workspace.cohort_m025_nodule_level_v1
WHERE analytic_eligible_strict_acr_pernodule IS TRUE;

SELECT 'cohort_m025_tirads_performance_v1_patient' AS object_name, COUNT(*) AS n_rows
FROM manuscript_workspace.cohort_m025_tirads_performance_v1;

-- §2 Nodule-level ROM by ACR2017 category (strict)
SELECT
  acr2017_tirads_category,
  COUNT(*) AS n_nodules,
  COUNT_IF(nodule_path_proven_malignant) AS n_malignant,
  ROUND(100.0 * COUNT_IF(nodule_path_proven_malignant) / NULLIF(COUNT(*), 0), 2) AS rom_pct
FROM manuscript_workspace.cohort_m025_nodule_level_v1
WHERE analytic_eligible_strict_acr_pernodule IS TRUE
GROUP BY acr2017_tirads_category
ORDER BY acr2017_tirads_category;

-- §3 Bethesda × TI-RADS (strict, counts)
-- Use whichever Bethesda numeric column exists on the live view (often bethesda_final_num).
SELECT
  CAST(COALESCE(bethesda_2023_num, bethesda_final_num) AS VARCHAR) AS bethesda_key,
  acr2017_tirads_category,
  COUNT(*) AS n
FROM manuscript_workspace.cohort_m025_nodule_level_v1
WHERE analytic_eligible_strict_acr_pernodule IS TRUE
GROUP BY 1, 2
ORDER BY 1, 2;
