-- mig_292: M025 TI-RADS Performance submission package v1.0
-- Generated: 2026-05-04 | Cursor Composer (mig_292 dispatch)
-- DB: thyroid_canonical_publication_v1_0
--
-- Documentation-only signoff: package built under M025_submission_package_v1_0/
-- (tables, figures, reproducibility SQL, Python builders).
-- Prerequisites: cohort_m025_tirads_performance_v1 (mig_280), tirads_resolved on CPM (mig_288).

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_292',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig292',
  'mig_292: M025 TI-RADS Performance submission package v1.0. Mirrors M037/M044 structure. Tables 1-4 + Supp ROC + Bethesda cross-stratification + operative-cohort caveat. Figures 1-4 + CSV sidecars from build_m025_figures.py. tirads_resolved from mig_288 + cohort view join. Apply after local QA PASS.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_292');

SELECT mig_id,
       signed_off_at,
       by_actor,
       substring(summary, 1, 140) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_292';
