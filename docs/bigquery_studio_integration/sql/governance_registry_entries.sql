-- Governance registry entries for mig_cw_workup_census_canonical_20260514.
-- These were applied 2026-05-14 as part of the workup-census canonical integration.
-- Recorded here for audit / reproducibility.

-- 1. Snapshot taken before the integration (the base table was NOT mutated):
--    pub_archive.canonical_patient_master_pre_workup_census_merge_20260514
--    CREATE TABLE ... AS SELECT * FROM pub_canonical.canonical_patient_master;  (10,871 rows x 2,314 cols)

-- 2. Migration log entry
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
(migration_id, applied_at, applied_by, description, affected_dataset, affected_table, pre_snapshot_table, rows_before, rows_after, rollback_sql, notes)
VALUES (
  'mig_cw_workup_census_canonical_20260514',
  CURRENT_TIMESTAMP(),
  'cowork-claude-2026-05-14',
  'Promoted the workup census into pub_canonical. Created canonical_patient_workup_census_v1 (materialized table, one row per patient) and canonical_patient_master_v1_9 (view that left-joins the census columns onto canonical_patient_master on research_id). The base canonical_patient_master table was NOT altered.',
  'pub_canonical',
  'canonical_patient_workup_census_v1,canonical_patient_master_v1_9',
  'pub_archive.canonical_patient_master_pre_workup_census_merge_20260514',
  10871,
  10871,
  'DROP VIEW `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master_v1_9`; DROP TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_workup_census_v1`;',
  'BigQuery Studio Integration Plan. Census logic sourced from pub_eval.vw_patient_workup_census_v1. Related Linear: THY-86, THY-87, THY-88. Caveats: US/CT/MRI pre/post derived from patient-level first/last dates; prior_procedure_path_gap_flag is a chart-review trigger.'
);

-- 3. Signoff registry entries
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
(schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na, table_status, signed_off_ts, signoff_migration, priority_tier, notes, registered_ts)
VALUES
(
  'pub_canonical', 'canonical_patient_workup_census_v1',
  65, 0, 65, 0, 0,
  'Active', NULL, 'mig_cw_workup_census_canonical_20260514', 'tier_2',
  'Materialized workup census promoted into pub_canonical by the BigQuery Studio Integration Plan. One row per patient (10,871). Columns are NOT yet individually verified in canonical_column_verification_registry_v1 - n_not_started reflects this. Caveats documented in the table description.',
  CURRENT_TIMESTAMP()
),
(
  'pub_canonical', 'canonical_patient_master_v1_9',
  2375, 0, 2375, 0, 0,
  'Active', NULL, 'mig_cw_workup_census_canonical_20260514', 'tier_1',
  'View: canonical_patient_master left-joined with canonical_patient_workup_census_v1 on research_id. Inherits column verification status from canonical_patient_master (2,314 cols) plus 61 new census columns. Base canonical_patient_master table unchanged.',
  CURRENT_TIMESTAMP()
);
