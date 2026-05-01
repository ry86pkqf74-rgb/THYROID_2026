-- mig_271: Post-mig_264b cascade — clear AJCC8 stage on NIFTP / follicular adenoma
-- patients now IS_MALIGNANT=FALSE (NIFTP excluded from AJCC staging per AJCC 8).
-- Archive: "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig271_20260502
-- Closes CF-mig264b-DOWNSTREAM-CASCADE.

-- Probe 1a (expect 0 after apply): NIFTP/FA non-malignant with stage populated
/*
SELECT research_id, histology_final, ajcc8_stage_group
FROM main.canonical_patient_master
WHERE is_malignant = FALSE
  AND (
    histology_final = 'NIFTP'
    OR LOWER(TRIM(histology_final)) IN ('follicular adenoma', 'atypical follicular adenoma')
    OR histology_final ILIKE '%follicular adenoma%'
  )
  AND ajcc8_stage_group IS NOT NULL;
*/

-- 2a. Pre-snapshot
-- CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig271_20260502 AS
-- SELECT research_id, ajcc8_stage_group, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, histology_final
-- FROM main.canonical_patient_master
-- WHERE is_malignant = FALSE AND ajcc8_stage_group IS NOT NULL
--   AND (
--     histology_final = 'NIFTP'
--     OR LOWER(TRIM(histology_final)) IN ('follicular adenoma', 'atypical follicular adenoma')
--     OR histology_final ILIKE '%follicular adenoma%'
--   );

-- 2b. NULL stage for non-malignant NIFTP/FA
-- UPDATE main.canonical_patient_master
-- SET
--   ajcc8_stage_group = NULL,
--   ajcc8_t_stage = NULL,
--   ajcc8_n_stage = NULL,
--   ajcc8_m_stage = NULL,
--   cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
-- WHERE is_malignant = FALSE
--   AND ajcc8_stage_group IS NOT NULL
--   AND (
--     histology_final = 'NIFTP'
--     OR LOWER(TRIM(histology_final)) IN ('follicular adenoma', 'atypical follicular adenoma')
--     OR histology_final ILIKE '%follicular adenoma%'
--   );

-- 2c. Signoff (via Python runner)
-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) ...
