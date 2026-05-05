-- mig_316 — M037 cohort naming reconciliation (CF-M037-COHORT-MISSING)
--
-- Context: Handoff docs referenced manuscript_workspace.cohort_m037_ln_predictors_v1
-- which never existed. M037 v1 submission (frozen) uses cohort_m037_ln_metastasis_v1 —
-- malignant patients with LN examined > 0 OR ln_positive_flag = TRUE (same predicate as
-- mig_280 on canonical_patient_master). M043 = all malignant (cohort_m043_ln_predictors_v1).
--
-- Resolution: MATERIALIZE cohort_m037_ln_predictors_v1 AS the M037-aligned subset of
-- cohort_m043_ln_predictors_v1 using the IDENTICAL eligibility filter as mig_280 /
-- cohort_m037_ln_metastasis_v1 (not LN-positive-only; outcome modeling uses N1+ within cohort).
--
-- Prerequisites: thyroid_canonical_publication_v1_0; cohort_m043_ln_predictors_v1 unchanged.
-- Apply: run via MotherDuck RW (scripts one-liner or CI) after reviewing counts.

CREATE OR REPLACE TABLE manuscript_workspace.cohort_m037_ln_predictors_v1 AS
SELECT *
FROM manuscript_workspace.cohort_m043_ln_predictors_v1 AS m
WHERE (m.ln_total_examined > 0)
   OR (m.ln_positive_flag = CAST('t' AS BOOLEAN));

-- Post-check (expect n_m037_metastasis = n_m037_predictors ≈ 2,234; small drift acceptable after CPM passes)
-- SELECT COUNT(*) AS n FROM manuscript_workspace.cohort_m037_ln_predictors_v1;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_316',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig316',
  'mig_316: M037 cohort naming reconciliation. Investigation: M037 v1 submission uses '
  || 'cohort_m037_ln_metastasis_v1 (malignant; LN examined > 0 OR ln_positive_flag TRUE), '
  || 'n≈2234 — not LN-positive-only. Handoff referenced non-existent cohort_m037_ln_predictors_v1. '
  || 'Resolution: TABLE manuscript_workspace.cohort_m037_ln_predictors_v1 = subset of '
  || 'cohort_m043_ln_predictors_v1 with same mig_280 predicate (no changes to M043). '
  || 'Closes CF-M037-COHORT-MISSING.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_316');
