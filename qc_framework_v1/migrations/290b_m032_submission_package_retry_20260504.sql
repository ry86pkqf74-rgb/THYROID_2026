-- mig_290b: M032 25-yr Descriptive submission package — retry of mig_290
-- Generated: 2026-05-04 | Cursor Composer (mig_290b dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Purpose: Record mig_290b completion (explicit retry lane; mig_290 row may already exist).
-- Filesystem deliverable: M032_submission_package_v1_0/ (see cursor_prompts/CURSOR_PROMPT_MIG_290B_*)
--
-- Carry-forwards: same as mig_290 (CF-M032-READY-FOR-WRITING)

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_290b', CURRENT_TIMESTAMP, 'cursor_composer_mig290b_retry_of_290',
 'mig_290b: M032 25-yr Descriptive submission package v1.0 built (mig_290 retry). See mig_290 prompt for full scope. Closes M032 ready-for-writing gate.');

SELECT mig_id, signed_off_at, by_actor, summary
FROM main.signoff_migration
WHERE mig_id = 'mig_290b';
