-- mig_287: Smoking taxonomy normalization (mig_281 follow-up)
-- Date: 2026-05-04
-- Closes: CF-mig281-SMOKING-TAXONOMY-DIRTY
-- Purpose: Normalize pmhx_nlp_smoking_status on CPM and canonical_pmh_events_v1
--          to a clean 3-level enum: current / former / never (+ NULL for unknown).
-- Root cause: AI_CLASSIFY Snowflake Cortex output has 6+ variants for what should
--             be a 3-level factor. Breaks C(smoking_combined) factor encoding in
--             M044 and M032/M037 regressions.

-- §2a: Pre-snapshot (run once, before UPDATEs)
-- CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_smoking_pre_mig287_20260504 AS
--   SELECT research_id, pmhx_nlp_smoking_status FROM main.canonical_patient_master;
-- CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.pmh_events_smoking_pre_mig287_20260504 AS
--   SELECT * FROM main.canonical_pmh_events_v1
--   WHERE LOWER(COALESCE(finding_value_norm,'')) LIKE '%smok%'
--      OR LOWER(COALESCE(finding_value,'')) LIKE '%smok%'
--      OR LOWER(COALESCE(finding_value,'')) LIKE '%tobacco%';

-- §2b: Normalize CPM pmhx_nlp_smoking_status
UPDATE main.canonical_patient_master
SET pmhx_nlp_smoking_status = CASE
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('current','current_smoker','current smoker') THEN 'current'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('former','former_smoker','former smoker','quit smoking','quit_smoking','ex-smoker','ex_smoker') THEN 'former'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('never','never_smoker','never smoker','non-smoker','non_smoker','nonsmoker') THEN 'never'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('unknown_or_not_mentioned','unknown','not mentioned','nan') THEN NULL
  -- free-text present-tense smoking → current
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('tobacco smoking') THEN 'current'
  WHEN pmhx_nlp_smoking_status ILIKE '%now%cigarette%' THEN 'current'
  ELSE pmhx_nlp_smoking_status
END
WHERE pmhx_nlp_smoking_status IS NOT NULL;

-- §2c: Normalize canonical_pmh_events_v1 finding_value_norm for smoking rows
UPDATE main.canonical_pmh_events_v1
SET finding_value_norm = CASE
  WHEN LOWER(COALESCE(finding_value_norm, finding_value,'')) IN ('smoking_current','current','current_smoker','current smoker') THEN 'smoking_current'
  WHEN LOWER(COALESCE(finding_value_norm, finding_value,'')) IN ('smoking_former','former','former_smoker','former smoker','quit smoking','quit_smoking','ex-smoker','ex_smoker') THEN 'smoking_former'
  WHEN LOWER(COALESCE(finding_value_norm, finding_value,'')) IN ('smoking_never','never','never_smoker','never smoker','non-smoker','non_smoker','nonsmoker') THEN 'smoking_never'
  WHEN LOWER(COALESCE(finding_value_norm, finding_value,'')) IN ('tobacco smoking','smoking_status') THEN 'smoking_status_unresolved'
  ELSE finding_value_norm
END
WHERE LOWER(COALESCE(finding_value_norm,'')) LIKE '%smok%'
   OR LOWER(COALESCE(finding_value,'')) LIKE '%smok%'
   OR LOWER(COALESCE(finding_value,'')) LIKE '%tobacco%';

-- §2d: Stamp cpm_built_at
UPDATE main.canonical_patient_master
SET cpm_built_at = CURRENT_TIMESTAMP
WHERE pmhx_nlp_smoking_status IS NOT NULL;

-- §2e: Registry signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_287', CURRENT_TIMESTAMP, 'cursor_composer_mig287',
 'mig_287: Smoking taxonomy normalization. UPDATE pmhx_nlp_smoking_status on CPM to clean 3-level enum (current/former/never + NULL). UPDATE canonical_pmh_events_v1 finding_value_norm for smoking rows. 2 residual free-text values (tobacco smoking, History of tobacco smoking) → current. Final: never=2303, former=504, current=215. Closes CF-mig281-SMOKING-TAXONOMY-DIRTY.');
