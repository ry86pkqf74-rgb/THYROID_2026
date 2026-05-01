-- =============================================================================
-- mig_259 — CF-mig258: `ln_status_source` on CPM (Rule C: N-stage vs LN count)
-- =============================================================================
-- Date:   2026-05-01
-- DB:     thyroid_canonical_publication_v1_0.main
-- Lane:   Snowflake/Cowork dispatch mig_258 / CF-mig258-NSTAGE-LNCOUNT-RECONCILE
-- File:   Numbered 259 to avoid collision with mig_258 M044 surgery-date migration.
--
-- Decision: Rule C (see scripts/output/mig_258_ln_reconcile_decision_20260501.md).
--   Do NOT overwrite `ajcc8_n_stage` or `ln_total_positive`; add explicit source
--   domain so manuscripts (M037 LN predictors, M044 ETE) choose staging vs count.
--
-- Values:
--   both     — N1a/N1b with ln_total_positive > 0
--   staging  — N1a/N1b but LN positive count NULL or 0 (structured count missing)
--   count    — ln_total_positive > 0 without N1a/N1b (none in current cohort; reserved)
--
-- Invariants after apply:
--   canonical_patient_master row count = 10871; no UPDATE to research_id keys.
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig259_ln_status_source_20260501 AS
SELECT
  research_id,
  is_malignant,
  ajcc8_n_stage,
  ln_total_positive,
  ln_total_examined,
  ln_positive_flag
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

ALTER TABLE thyroid_canonical_publication_v1_0.main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS ln_status_source VARCHAR;

UPDATE thyroid_canonical_publication_v1_0.main.canonical_patient_master
SET ln_status_source = CASE
  WHEN is_malignant IS NOT TRUE THEN NULL
  WHEN ajcc8_n_stage IN ('N1a', 'N1b') AND COALESCE(ln_total_positive, 0) > 0 THEN 'both'
  WHEN ajcc8_n_stage IN ('N1a', 'N1b') THEN 'staging'
  WHEN COALESCE(ln_total_positive, 0) > 0 THEN 'count'
  ELSE NULL
END;

UPDATE thyroid_canonical_publication_v1_0.main.canonical_patient_master
SET cpm_built_at = CURRENT_TIMESTAMP;

INSERT INTO main.canonical_column_verification_registry_v1
      (schema_name, table_name, column_name, data_type, ordinal_position,
       category, upstream_source, verification_status, verified_by, verified_ts,
       verification_method, batch_id, notes, registered_ts, na_rationale)
SELECT
  'main',
  'canonical_patient_master',
  'ln_status_source',
  'VARCHAR',
  1 + COALESCE((
    SELECT MAX(ordinal_position)
    FROM main.canonical_column_verification_registry_v1
    WHERE schema_name = 'main' AND table_name = 'canonical_patient_master'
  ), 0),
  'adjudicated',
  NULL,
  'verified',
  'logan',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'rule_c_staging_vs_ln_count_reconcile_mig259',
  'mig_259_ln_status_source_cf_mig258_20260501',
  ' | CF-mig258: adds ln_status_source (both/staging/count). N-stage = mig_132/266b; ln_total_positive = mig_133. No overwrite of source columns.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  NULL
WHERE NOT EXISTS (
  SELECT 1
  FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name = 'main'
    AND r.table_name = 'canonical_patient_master'
    AND r.column_name = 'ln_status_source'
);

INSERT INTO thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES (
  'mig_259_ln_status_source_cf_mig258_20260501',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP,
  'mig_259_ln_status_source_rule_c',
  '0',
  '0',
  '1509',
  '0'
);

-- -----------------------------------------------------------------------------
-- Post-apply verification (manual)
-- -----------------------------------------------------------------------------
-- SELECT ln_status_source, COUNT(*) FROM main.canonical_patient_master GROUP BY 1;
-- expect: both=1126, staging=1509, NULL remainder
-- SELECT COUNT(*) FROM main.canonical_patient_master WHERE cpm_built_at IS NULL;  -- expect 0
