-- =============================================================================
-- mig_325_register_ctc_signoff — THY-18: register CTC in BQ table signoff registry
-- =============================================================================
-- Target:      thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1
-- Prerequisites:
--   * pub_canonical.canonical_tumor_characteristics_v1 loaded (mig_324_load_ctc_bq.py)
--
-- Notes: BQ schema mirrors MotherDuck governance columns only. Extended audit fields
-- requested by THY-18 are packed into `notes`:
--   * source_table_md
--   * row_count (BQ)
--   * parity_check_sql
-- =============================================================================

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1` (
  schema_name,
  table_name,
  n_columns_total,
  n_verified,
  n_not_started,
  n_failed,
  n_na,
  table_status,
  signed_off_ts,
  signoff_migration,
  priority_tier,
  notes,
  registered_ts
)
SELECT
  'pub_canonical' AS schema_name,
  'canonical_tumor_characteristics_v1' AS table_name,
  col_counts.n_columns_total,
  0 AS n_verified,
  col_counts.n_columns_total AS n_not_started,
  0 AS n_failed,
  0 AS n_na,
  'live' AS table_status,
  CURRENT_TIMESTAMP() AS signed_off_ts,
  'qc_framework_v1/migrations/325_register_ctc_signoff.sql' AS signoff_migration,
  'tier1_canonical_base' AS priority_tier,
  FORMAT(
    'THY-18 CTC MD→BQ. source_table_md=%s; row_count_bq=%d; '
    'parity_check_sql=%s',
    'thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1',
    row_counts.n_rows,
    'SELECT COUNT(*) AS md_rows FROM thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1; '
    'SELECT COUNT(*) AS bq_rows FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_tumor_characteristics_v1`; '
    'Expect ABS(md_rows - bq_rows) <= CEIL(0.001 * md_rows).'
  ) AS notes,
  CURRENT_TIMESTAMP() AS registered_ts
FROM (
  SELECT COUNT(*) AS n_columns_total
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_schema = 'pub_canonical'
    AND table_name = 'canonical_tumor_characteristics_v1'
) AS col_counts
CROSS JOIN (
  SELECT COUNT(*) AS n_rows
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_tumor_characteristics_v1`
) AS row_counts
WHERE NOT EXISTS (
  SELECT 1
  FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1` AS z
  WHERE z.schema_name = 'pub_canonical'
    AND z.table_name = 'canonical_tumor_characteristics_v1'
);
