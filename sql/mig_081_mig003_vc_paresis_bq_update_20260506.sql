-- MIG-003 / mig_081 — Repopulate comp_vc_paresis_* from Snowflake AI_CLASSIFY evidence (2026-05-06)
-- Prerequisite (Snowflake, not replayed here): AI_CLASSIFY on contrast-language notes in
-- CLINICAL_NOTES_SEARCH_V1 for 12 patients without CPM paralysis yielded 1 note labeled
-- `clinical_paresis_distinct_from_paralysis` (research_id 8616). Full methods in
-- _scripts/mig003_paresis_revalidation_summary.md
--
-- Dry-run (2026-05-06): ~46,095,307 bytes upper bound
--
-- DML:
UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS t
SET comp_vc_paresis_confirmed = TRUE,
    comp_vc_paresis_evidence_tier = 2
WHERE research_id = '8616'
  AND comp_vc_paralysis_confirmed IS NOT TRUE;

-- Rollback:
-- UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
-- SET comp_vc_paresis_confirmed = FALSE, comp_vc_paresis_evidence_tier = NULL
-- WHERE research_id = '8616';
