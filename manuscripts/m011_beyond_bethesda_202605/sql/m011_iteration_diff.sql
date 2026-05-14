-- =====================================================================
-- M011 — Iteration diff template
-- Run AFTER rebuilding the cohort in a new iteration (v2, v3 …), comparing the
-- fresh pub_workspace.m011_* tables against the most recent pub_archive baseline.
-- Catches the two silent-drift failure modes:
--   (1) "builder didn't retrigger" — a feeder table changed, numbers moved, no rebuild
--   (2) "legacy carry-over broke"  — a column/filter changed meaning between versions
--
-- BEFORE running: set @prev to the previous baseline date suffix.
-- =====================================================================
DECLARE prev_suffix STRING DEFAULT 'v1_baseline_20260514';  -- <-- update each iteration

-- (1) PATIENT-LEVEL DIFF — who entered / left the primary cohort
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.m011_iter_diff_patients` AS
WITH cur AS (SELECT research_id, any_malignancy, bethesda_highest, acr_imputed_max, molecular_tested
             FROM `thyroid-canonical-pub-2026.pub_workspace.m011_frame_b` WHERE in_primary_cohort),
prev AS (SELECT research_id, any_malignancy, bethesda_highest, acr_imputed_max, molecular_tested
         FROM `thyroid-canonical-pub-2026.pub_archive.m011_frame_b_v1_baseline_20260514` WHERE in_primary_cohort)
SELECT
  COALESCE(c.research_id, p.research_id) AS research_id,
  CASE WHEN p.research_id IS NULL THEN 'ADDED'
       WHEN c.research_id IS NULL THEN 'DROPPED'
       WHEN c.any_malignancy IS DISTINCT FROM p.any_malignancy
         OR c.bethesda_highest IS DISTINCT FROM p.bethesda_highest
         OR c.acr_imputed_max IS DISTINCT FROM p.acr_imputed_max
         OR c.molecular_tested IS DISTINCT FROM p.molecular_tested THEN 'CHANGED'
       ELSE 'STABLE' END AS diff_status,
  p.any_malignancy AS prev_malig, c.any_malignancy AS cur_malig,
  p.bethesda_highest AS prev_beth, c.bethesda_highest AS cur_beth,
  p.acr_imputed_max AS prev_acr,  c.acr_imputed_max AS cur_acr,
  p.molecular_tested AS prev_mol, c.molecular_tested AS cur_mol
FROM cur c FULL OUTER JOIN prev p USING(research_id)
WHERE p.research_id IS NULL OR c.research_id IS NULL
   OR c.any_malignancy IS DISTINCT FROM p.any_malignancy
   OR c.bethesda_highest IS DISTINCT FROM p.bethesda_highest
   OR c.acr_imputed_max IS DISTINCT FROM p.acr_imputed_max
   OR c.molecular_tested IS DISTINCT FROM p.molecular_tested;

-- Summary: count by diff_status (expect all-zero except STABLE if nothing changed)
SELECT diff_status, COUNT(*) n FROM `thyroid-canonical-pub-2026.pub_workspace.m011_iter_diff_patients`
GROUP BY 1 ORDER BY 1;

-- (2) LOCKED-NUMBER DIFF — which model AUCs / cohort-audit counts moved
SELECT 'model_metrics' AS table_name, c.model AS key, 'auc' AS metric,
  p.auc AS prev_value, c.auc AS cur_value, ROUND(c.auc - p.auc, 4) AS delta
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_model_metrics` c
JOIN `thyroid-canonical-pub-2026.pub_archive.m011_model_metrics_v1_baseline_20260514` p USING(model)
WHERE ABS(c.auc - p.auc) > 0.0005
UNION ALL
SELECT 'cohort_audit', c.metric, 'n',
  CAST(p.n AS FLOAT64), CAST(c.n AS FLOAT64), CAST(c.n - p.n AS FLOAT64)
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_cohort_audit` c
JOIN `thyroid-canonical-pub-2026.pub_archive.m011_cohort_audit_v1_baseline_20260514` p USING(metric)
WHERE c.n <> p.n
ORDER BY table_name, ABS(delta) DESC;

-- (3) SOURCE-DRIFT CHECK — did any feeder table change since the provenance manifest?
SELECT pm.source_table, pm.source_last_modified AS manifest_last_modified,
  TIMESTAMP_MILLIS(t.last_modified_time) AS current_last_modified,
  TIMESTAMP_MILLIS(t.last_modified_time) > pm.source_last_modified AS source_changed_since_build
FROM `thyroid-canonical-pub-2026.pub_workspace.m011_provenance_manifest` pm
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.__TABLES__` t
  ON t.table_id = pm.source_table
ORDER BY source_changed_since_build DESC, pm.source_table;

-- INTERPRETATION:
--  * diff (1) non-empty  -> cohort membership moved; explain every ADDED/DROPPED/CHANGED row.
--  * diff (2) non-empty  -> a locked manuscript number changed; trace to a feeder rebuild,
--                           a QC fix, or a cohort-filter change before telling a co-author.
--  * diff (3) TRUE rows  -> a source table was rebuilt after this build; the cohort MAY have
--                           drifted even if you didn't change the M011 SQL — rebuild + re-diff.
