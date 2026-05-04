-- mig_290: M032 25-yr Descriptive Submission Package Scaffold
-- Generated: 2026-05-04 | Cursor Composer (mig_290 dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Purpose: Record mig_290 completion and close the M032 ready-for-writing gate.
--
-- This migration is documentation-only (no schema changes):
-- - M032_submission_package_v1_0/ built on local filesystem
-- - All analysis code and figures generated and materialized
-- - Validation: 9/9 QA metrics PASS within tolerance
-- - n_malig live = 4,019 (Cowork lock 4,018; +1 patient from mig_285 edge case)
--
-- Carry-forwards closed: CF-M032-READY-FOR-WRITING
-- Carry-forwards remaining: CF-M032-LOGREG-MULTIVARIABLE, CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE

USE thyroid_canonical_publication_v1_0;

-- ── Signoff ────────────────────────────────────────────────────────────────
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_290', CURRENT_TIMESTAMP, 'cursor_composer_mig290',
 'mig_290: M032 25-yr Descriptive submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp S1-S2 + 4 figures (300 DPI). SQL reproducibility package + 3 Python build scripts + validation report. 9/9 QA metrics PASS. n_malig live=4019 (lock=4018, +1 within tolerance). Closes CF-M032-READY-FOR-WRITING. Open: CF-M032-LOGREG-MULTIVARIABLE.');

-- ── Verify signoff recorded ────────────────────────────────────────────────
SELECT mig_id, signed_off_at, by_actor, summary
FROM main.signoff_migration
WHERE mig_id = 'mig_290';
