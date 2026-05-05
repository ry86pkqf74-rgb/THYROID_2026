-- mig_307: M025 submission package v2.0 (nodule-level pivot)
-- Generated: 2026-05-04 | Cursor (mig_307 dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Documentation signoff: package under M025_submission_package_v2_0/
-- (tables, figures, reproducibility SQL, Python builders).
-- Prerequisites: manuscript_workspace.cohort_m025_nodule_level_v1 (mig_306).
-- Sister package: M025_submission_package_v1_0/ (patient-level, frozen).

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_307',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig307',
  'mig_307: M025 submission package v2.0 scaffold — nodule-level TI-RADS performance driven by cohort_m025_nodule_level_v1. Python builders: per-nodule sens/spec/PPV/NPV at TR>=TR3/4/5; patient-vs-nodule ROM table; Bethesda×TIRADS cross-strat; ROC (nodule + patient comparator). Title from 00_README. v1_0 package remains frozen patient-level companion.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_307');

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 160) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_307';
