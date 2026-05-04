-- mig_288: M025 TIRADS CPM cleanup (mig_282 retry)
-- Applied: 2026-05-04  by cursor_composer_mig288_retry_of_282
-- Closes: CF-M025-CPM-TIRADS-COL-DIRTY
-- Context: mig_282 was authored 2026-05-03 but NOT applied (no signoff row).
--          This migration adds tirads_resolved as a clean TR1-TR5 VARCHAR enum
--          on canonical_patient_master, sourced from the cohort_m025 view.

USE thyroid_canonical_publication_v1_0;

-- §1 — Pre-snapshot (already executed; kept for reference)
-- CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_tirads_pre_mig288_20260504 AS
-- SELECT research_id, nlp_tirads_max_category, nlp_tirads_has_data
-- FROM main.canonical_patient_master;

-- §2a — Add column
ALTER TABLE main.canonical_patient_master ADD COLUMN tirads_resolved VARCHAR;

-- §2b — Populate from cohort_m025 view (worst/highest TIRADS score wins)
UPDATE main.canonical_patient_master pm
SET tirads_resolved = (
    SELECT COALESCE(cm.tirads_worst_category_v12, cm.preop_tirads_category)
    FROM manuscript_workspace.cohort_m025_tirads_performance_v1 cm
    WHERE CAST(cm.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
)
WHERE pm.tirads_resolved IS NULL;
-- Result: 3,375 rows populated from cohort_m025_tirads_performance_v1

-- §2c — Regex fallback: extract clean TR1-TR5 from nlp_tirads_max_category
UPDATE main.canonical_patient_master
SET tirads_resolved = NULLIF(regexp_extract(nlp_tirads_max_category, '^TR[1-5]$'), '')
WHERE tirads_resolved IS NULL
  AND nlp_tirads_max_category IS NOT NULL;
-- Result: +7 rows from NLP fallback; total 3,382 non-NULL

-- §2d — Stamp cpm_built_at
UPDATE main.canonical_patient_master
SET cpm_built_at = CURRENT_TIMESTAMP
WHERE tirads_resolved IS NOT NULL;

-- §2e — Registry signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_288', CURRENT_TIMESTAMP, 'cursor_composer_mig288_retry_of_282',
 'mig_288: Added canonical_patient_master.tirads_resolved (VARCHAR TR1-TR5+NULL). 3,375 rows from cohort_m025_tirads_performance_v1 (worst_category COALESCE preop_category) + 7 NLP regex fallback = 3,382 total. Archive: cpm_tirads_pre_mig288_20260504. mig_282 retry. Closes CF-M025-CPM-TIRADS-COL-DIRTY.');

-- §3 — Verification query (informational)
-- SELECT tirads_resolved, COUNT(*) n,
--        COUNT_IF(is_malignant) n_malig,
--        ROUND(100.0*COUNT_IF(is_malignant)/COUNT(*), 1) pct_malig
-- FROM main.canonical_patient_master
-- GROUP BY 1 ORDER BY 1;
-- Expected results (applied 2026-05-04):
--   TR1:  340 pts, 96 malignant (28.2%)
--   TR2:  299 pts, 96 malignant (32.1%)
--   TR3:  845 pts, 233 malignant (27.6%)
--   TR4:  496 pts, 236 malignant (47.6%)
--   TR5: 1402 pts, 823 malignant (58.7%)
--   NULL: 7489 pts (no TIRADS data)
