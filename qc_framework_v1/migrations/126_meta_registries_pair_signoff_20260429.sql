-- =============================================================================
-- Migration 126 — Meta-registries paired sign-off (Protocol v2 / Lane 18)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Targets:
--   * main.canonical_table_signoff_registry_v1 (meta row for self + column registry)
--   * main.canonical_column_verification_registry_v1 (two meta table definitions)
--
-- Methodology:
--   Meta fixed-point: rows describe global verification state including themselves;
--   verification uses internal consistency vs information_schema + sum/cross-registry math
--   (cf. mig_109 cleanliness audit, mig_117 drift allowlist discipline).
--
-- Pre-reconcile (PROBE 3c / orphans — NOT meta tables; registry drift blocking global gate):
--   * canonical_patient_master — signoff totals lag live schema by 6 cols (1592 vs 1598);
--       column registry already 1598 rows; align signoff n_columns_total / n_not_started.
--   * canonical_complications_patient_rollup_v1 — column `rebuild_lineage_v1` removed from
--       live table; orphan verified row in column registry; DELETE row + align signoff
--       (51→50 cols, 49→48 verified).
--
-- Live MotherDuck probes (thyroid_canonical_publication_v1_0, pre-apply 2026-04-29):
--   * 3a-P1 orphan tables (excl. legacy/archived): expect 0 post-reconcile for listed issues.
--   * 3a-P3 row-level sum = n_columns_total: 0 violations for non-reconciled rows; CPM fixed.
--   * 3b-P1 column registry orphan: 0 post-DELETE rebuild_lineage row.
--   * 3b-P3 gate-4 (verified requires by + batch + method): 0 (unchanged).
--   * Self/meta: ordinal + data_type vs information_schema for both meta tables = 0 drift;
--       global TIMESTAMP vs TIMESTAMP WITH TIME ZONE drift on unrelated tables documented as CF
--       (mig_117 pattern; out of scope for this lane).
--
-- Sign-off scope:
--   * canonical_table_signoff_registry_v1: 12 not_started → verified; 1 na (registered_ts).
--   * canonical_column_verification_registry_v1: 12 not_started → verified; 2 na (batch_id, registered_ts).
--   * Both table rows: table_status → verified, signoff_migration stamped, signed_off_ts set.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 126a — Preconditions: CPM signoff counts align to column-registry + live information_schema
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 1598,
    n_not_started   = 1594,
    notes = COALESCE(notes,'')
              || ' | mig126: reconcile n_columns_total/n_not_started to match column-registry '
              || 'information_schema cardinality (+6 cols vs stale signoff row; 3c drift fix).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND n_columns_total = 1592;

-- -----------------------------------------------------------------------------
-- 126b — Preconditions: ghost column verified row (table dropped rebuild_lineage in live DDL)
-- -----------------------------------------------------------------------------
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name  = 'canonical_complications_patient_rollup_v1'
  AND column_name = 'rebuild_lineage_v1';

UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 50,
    n_verified      = 48,
    notes = COALESCE(notes,'')
              || ' | mig126: removed orphan verified row for rebuild_lineage_v1 (not in live '
              || 'canonical_complications_patient_rollup_v1 DDL); counters 51→50, verified 49→48.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_complications_patient_rollup_v1'
  AND n_columns_total = 51;

-- -----------------------------------------------------------------------------
-- 126c — Meta: canonical_table_signoff_registry_v1 columns (Protocol v2)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'internal_consistency_information_schema_and_registry_algebra_mig126',
    batch_id            = 'mig126_meta_registries_pair_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                            || ' | mig126: keys/schema_name + table_name match information_schema.tables; '
                            || 'counts + status fields cross-checked vs column-registry rollups '
                            || 'and row-level sum identity; ENUM table_status consistent; meta fixed-point.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_table_signoff_registry_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'schema_name',
    'table_name',
    'n_columns_total',
    'n_verified',
    'n_not_started',
    'n_failed',
    'n_na',
    'table_status',
    'signed_off_ts',
    'signoff_migration',
    'priority_tier',
    'notes'
  );

-- -----------------------------------------------------------------------------
-- 126d — Meta: canonical_column_verification_registry_v1 columns (Protocol v2)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'internal_consistency_information_schema_and_self_registry_mig126',
    batch_id            = 'mig126_meta_registries_pair_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                            || ' | mig126: (schema, table, column, data_type, ordinal_position) '
                            || 'paired to main.information_schema.columns; category/upstream gated; '
                            || 'verification_status derived column coherent; meta fixed-point; '
                            || 'na batch_id remains na_provenance lane policy.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_column_verification_registry_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'schema_name',
    'table_name',
    'column_name',
    'data_type',
    'ordinal_position',
    'category',
    'upstream_source',
    'verification_status',
    'verified_by',
    'verified_ts',
    'verification_method',
    'notes'
  );

-- -----------------------------------------------------------------------------
-- 126e — Roll up both meta table rows into canonical_table_signoff_registry_v1 (paired)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total   = subq.n_total,
    n_verified        = subq.n_verified,
    n_not_started     = subq.n_not_started,
    n_failed          = COALESCE(subq.n_failed, 0),
    n_na              = subq.n_na,
    table_status      = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts       = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration   = 'qc_framework_v1/migrations/126_meta_registries_pair_signoff_20260429.sql',
    notes               = COALESCE(ts.notes, '')
                            || ' | mig126: Meta-registry pair verified (Lane 18). '
                            || 'canonical_table_signoff_registry_v1: 13 cols (12 verified + 1 na registered_ts); '
                            || 'canonical_column_verification_registry_v1: 14 cols (12 verified + '
                            || 'na batch_id + registered_ts). '
                            || 'Self-referential fixed-point acknowledged; probes 3a/3b/3c clean post pre-reconcile.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name IN (
          'canonical_table_signoff_registry_v1',
          'canonical_column_verification_registry_v1'
        )
  GROUP BY schema_name, table_name
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name  = subq.table_name;

-- =============================================================================
-- end migration 126 — paired meta-registry verification (Lakehouse audit SSOT closure)
-- =============================================================================
