-- mig_289: Sync manuscript_workspace cohort views to Snowflake (convenience mirror)
-- Applied: 2026-05-04
-- Lane: Export 5 MD cohort views as SF tables so SF-native scripts can query directly
--       without MD roundtrip. Read-only mirror; refreshed per MD→SF export cycle.
--
-- Cohort views synced:
--   manuscript_workspace.cohort_m044_ajcc_ete_v1         -> COHORT_M044_AJCC_ETE_V1
--   manuscript_workspace.cohort_m037_ln_metastasis_v1    -> COHORT_M037_LN_METASTASIS_V1
--   manuscript_workspace.cohort_m025_tirads_performance_v1 -> COHORT_M025_TIRADS_PERFORMANCE_V1
--   manuscript_workspace.cohort_m032_descriptive_25yr_v1 -> COHORT_M032_DESCRIPTIVE_25YR_V1
--   main.cohort_m038_massive_goiter_v1                   -> COHORT_M038_MASSIVE_GOITER_V1
--
-- Scripts modified:
--   snowflake_trial/scripts/01_export_md_to_parquet.py  (COHORT_VIEWS block)
--   snowflake_trial/scripts/04_build_flat_views.py      (COHORT_VIEW_TABLES passthrough)
-- Script 02 picks up new parquet files automatically (iterates PARQUET_DIR/*.parquet).

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
(
  'mig_289',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig289',
  'mig_289: Added 5 manuscript_workspace cohort views (m044/m037/m025/m032/m038) to SF refresh pipeline. SF-native scripts no longer need MD roundtrip. Cohort views refresh per cycle. Verified row counts match MD post-load.'
);
