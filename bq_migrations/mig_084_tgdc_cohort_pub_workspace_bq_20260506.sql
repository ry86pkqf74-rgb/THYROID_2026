-- migration_id: mig_084_tgdc_cohort_pub_workspace_bq_20260506
-- DFL: DFL-20260506-TGDC-BQ
-- Linear: THY-2
--
-- Canonical BigQuery mirror for MotherDuck ``pub_workspace`` TGDC tables:
--   * pub_workspace.tgdc_manual_addons_v1
--   * pub_workspace.cohort_tgdc_primary_v1
--
-- Load mechanism: Parquet WRITE_TRUNCATE via
--   studies/tgdc_reconciliation/build_cohort.py --apply --bq-load
--   (or --bq-load alone to re-sync from existing MotherDuck tables).
--
-- Hard gate: COUNT(DISTINCT research_id) = 227 on cohort_tgdc_primary_v1 (enforced in script).
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
  'mig_084_tgdc_cohort_pub_workspace_bq_20260506',
  CURRENT_TIMESTAMP(),
  'cursor_agent_tgdc_bq',
  'THY-2: TGDC cohort tables in pub_workspace (addons + primary union); BQ mirror from build_cohort.py --bq-load.',
  'pub_workspace',
  'cohort_tgdc_primary_v1',
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_tgdc_primary_v1`),
  'Re-load from MotherDuck via build_cohort.py --bq-load; companion table tgdc_manual_addons_v1 loaded in same run.',
  FORMAT(
    'DFL=DFL-20260506-TGDC-BQ; cohort rows=%d; distinct_ids must be 227 (script gate).',
    (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_tgdc_primary_v1`)
  )
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1
  FROM `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = 'mig_084_tgdc_cohort_pub_workspace_bq_20260506'
);
