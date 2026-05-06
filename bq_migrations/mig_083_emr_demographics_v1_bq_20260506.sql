-- migration_id: mig_083_emr_demographics_v1_bq_20260506
-- DFL: DFL-20260506-EMRDEMO
-- Linear: THY-1
-- Authority: parquet from scripts/emr_demographics_v1_pipeline.py (PHI-safe;
--   bootstrap from BigQuery canonical_patient_master + path_synoptics by default).
--
-- Loads/replaces `pub_workspace.emr_demographics_v1` via:
--   bq load --source_format=PARQUET --schema=schemas/emr_demographics_v1_bigquery.json --replace
--   OR google.cloud.bigquery LoadJobConfig with the same schema (avoids all-NULL
--   STRING columns being autodetected as INTEGER).
--
-- Governance row (idempotent: skip if already logged).
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1` (
  migration_id,
  applied_at,
  applied_by,
  description,
  affected_dataset,
  affected_table,
  pre_snapshot_table,
  rows_before,
  rows_after,
  rollback_sql,
  notes
)
SELECT
  'mig_083_emr_demographics_v1_bq_20260506',
  CURRENT_TIMESTAMP(),
  'cursor_agent_thy1_bq',
  'THY-1: PHI-safe emr_demographics_v1 in pub_workspace (race/sex/dob_year only; TGDC verification vs BQ).',
  'pub_workspace',
  'emr_demographics_v1',
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.emr_demographics_v1`),
  'Re-load from git-tracked parquet via scripts/emr_demographics_v1_pipeline.py --bq-load (or bq load with schemas/emr_demographics_v1_bigquery.json).',
  FORMAT(
    'DFL=DFL-20260506-EMRDEMO; rows=%d; explicit schema prevents all-NULL ethnicity INTEGER autodetect; MotherDuck superseded for ongoing ops.',
    (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.emr_demographics_v1`)
  )
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1
  FROM `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = 'mig_083_emr_demographics_v1_bq_20260506'
);
