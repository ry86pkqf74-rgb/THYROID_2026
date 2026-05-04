-- =============================================================================
-- mig_279 — Registry audit backfill signoff for mig_253 / mig_256 / mig_258 / mig_259
-- =============================================================================
-- Date:    2026-05-03
-- DB:      thyroid_canonical_publication_v1_0
-- Purpose: SQL files existed without main.signoff_migration rows. Dry probes on
--          20260503 confirmed all four migs already reached expected end-state;
--          this file records retro signoffs only (no DDL/DML on clinical tables).
--
-- Probes executed (MotherDuck, publication DB):
--   mig_253: canonical_patient_master all-three-NULL surgical flags = 2
--            (dry-run expectation 2138 → 2); non-null surg_procedure_type = 10869
--   mig_256: cohort_m032_descriptive_25yr_v1 exposes comp_hypocalcemia_timing_window,
--            comp_hypoparathyroidism_timing_window (timing passthrough per 256 DDL)
--   mig_258: cohort_m044_ajcc_ete_v1 has surg_first_date_lineage_note + surg_date_* flags
--   mig_259: ln_status_source counts both=1126, staging=1509, NULL=8236 (Rule C expectation)
--
-- Closes: CF-mig253-REGISTRY-GAP + CF-mig256-REGISTRY-GAP + CF-mig258-REGISTRY-GAP +
--         CF-mig259-REGISTRY-GAP
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

BEGIN;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_253',
  CURRENT_TIMESTAMP::TIMESTAMP,
  'cursor_composer_mig279_backfill',
  'mig_279 retro signoff: mig_253 CPM surgical procedure-type NULL-fill already applied.'
  || ' Probe 20260503 — n_rows=10871; all_three_NULL(surg_procedure_type,surg_total_thyroidectomy,surg_hemithyroidectomy)=2;'
  || ' non-null surg_procedure_type=10869. Matches signed dry-run 2138→2 residual.'
);

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_256',
  CURRENT_TIMESTAMP::TIMESTAMP,
  'cursor_composer_mig279_backfill',
  'mig_279 retro signoff: mig_256 complication temporality passthrough cohort views live.'
  || ' Probe 20260503 — manuscript_workspace.cohort_m032_descriptive_25yr_v1 has columns'
  || ' comp_hypocalcemia_timing_window, comp_hypoparathyroidism_timing_window (CREATE OR REPLACE VIEW 256 DDL).'
);

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_258',
  CURRENT_TIMESTAMP::TIMESTAMP,
  'cursor_composer_mig279_backfill',
  'mig_279 retro signoff: mig_258 M044 surgery-date lineage + study-window booleans applied.'
  || ' Probe 20260503 — cohort_m044_ajcc_ete_v1 includes surg_first_date_lineage_note, surg_date_missing,'
  || ' surg_date_pre_1999, surg_date_1999_2024, surg_date_post_2024, surg_date_after_2024_06_04.'
);

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_259',
  CURRENT_TIMESTAMP::TIMESTAMP,
  'cursor_composer_mig279_backfill',
  'mig_279 retro signoff: mig_259 ln_status_source Rule C on CPM verified.'
  || ' Probe 20260503 — both=1126, staging=1509, ln_status_source IS NULL remainder=8236 (matches mig_259 commentary).'
);

COMMIT;

-- =============================================================================
-- End mig_279
-- =============================================================================
