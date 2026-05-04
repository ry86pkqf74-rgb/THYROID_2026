-- mig_291: M037 LN Predictors Submission Package v1.0
-- Generated: 2026-05-04 | Cursor Composer (mig_291 dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Documentation-only signoff: package built on repo filesystem under
-- M037_submission_package_v1_0/ (tables, figures, SQL excerpts, Python builders).
--
-- Carry-forwards: closes M037 ready-for-scaffold Manuscript packaging gate.
-- Prerequisites: manuscript_workspace.cohort_m037_ln_metastasis_v1 (mig_280/286),
--                  main.canonical_patient_master.ln_status_source (mig_259).

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_291',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig291',
  'mig_291: M037 LN Predictors submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp S1-S2 + 4 figures (300 DPI). SQL reproducibility (M037_ln_predictors_analysis.sql) + build_m037_tables/figures/manuscript_md. Headline: family-hx aOR ~1.05 (null); male sex/age/tumor size significant in primary 4-variable model. Repro: run Python builders with MotherDuck RW token.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_291');

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 120) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_291';
