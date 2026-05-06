-- =============================================================================
-- BQ mig_096 — manuscript_workspace.cohort_m038_massive_goiter_v2
--             BigQuery counterpart of MotherDuck mig_333.
--             Adds composite massive-goiter flag columns to the M038 cohort view.
--
-- Date:       2026-05-07
-- Project:    thyroid-canonical-pub-2026
-- Dataset:    manuscript_workspace
-- Lane:       audit-gap closure — post_mig_086_manuscript_reconciliation_20260507.md §2.11
-- MD peer:    qc_framework_v1/migrations/333_cohort_m038_v2_composite_flag_20260507.sql
--
-- NOTE ON ECMO: The manuscript_inventory.md entry for M038 mistakenly
-- labels it "ECMO Support for Massive Goiter Surgery".  M038 is the massive
-- goiter descriptive cohort paper with NO ECMO content.  The task that
-- requested "cohort_m038_massive_goiter_ecmo_v1" was based on the incorrect
-- inventory title.  The correct view is *_v2 with the composite flag.
--
-- VERIFY (post-apply):
--   SELECT
--     COUNT(*) AS n_total,
--     COUNTIF(is_massive_composite) AS n_massive,
--     COUNTIF(gland_weight_known)   AS n_wt_known,
--     COUNTIF(is_massive_w)         AS n_w,
--     COUNTIF(is_massive_s)         AS n_s,
--     COUNTIF(is_massive_a)         AS n_a
--   FROM `thyroid-canonical-pub-2026.manuscript_workspace.cohort_m038_massive_goiter_v2`;
--   -- n_total=10871, n_massive=2501, n_wt_known=9130, n_w=1429, n_s=1047, n_a=1440
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.manuscript_workspace.cohort_m038_massive_goiter_v2` AS
SELECT
  v.*,
  -- W-axis: weight >= 100 g
  COALESCE(v.gland_weight_final_g >= 100, FALSE)                                        AS is_massive_w,
  -- S-axis: substernal on CT or MRI
  COALESCE(v.ct_substernal_extension_any OR v.mri_substernal_any, FALSE)                AS is_massive_s,
  -- A-axis: airway compromise/deviation/narrowing on CT
  COALESCE(
      v.ct_tracheal_deviation_any
      OR v.ct_tracheal_narrowing_any
      OR v.ct_airway_compromise_any,
      FALSE
  )                                                                                      AS is_massive_a,
  -- Composite (primary exposure): W OR S OR A
  COALESCE(v.gland_weight_final_g >= 100, FALSE)
      OR COALESCE(v.ct_substernal_extension_any OR v.mri_substernal_any, FALSE)
      OR COALESCE(
             v.ct_tracheal_deviation_any
             OR v.ct_tracheal_narrowing_any
             OR v.ct_airway_compromise_any,
             FALSE)                                                                      AS is_massive_composite,
  -- Weight-known flag (denominator for weight-sensitivity analyses)
  (v.gland_weight_final_g IS NOT NULL)                                                  AS gland_weight_known
FROM `thyroid-canonical-pub-2026.manuscript_workspace.cohort_m038_massive_goiter_v1` AS v;

-- =============================================================================
-- bq_migration_log_v1 row (append after apply):
-- =============================================================================
-- INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.bq_migration_log_v1`
-- VALUES (
--   'mig_096',
--   TIMESTAMP('2026-05-07'),
--   'manuscript_workspace.cohort_m038_massive_goiter_v2',
--   'CREATE OR REPLACE VIEW',
--   'Adds is_massive_composite, is_massive_w/s/a, gland_weight_known flags. '
--   || 'Audit gap closure (CANNOT_VERIFY → MATCH): n_massive=2501 vs locked 2501.',
--   'Cowork'
-- );
-- =============================================================================
-- End mig_096
-- =============================================================================
