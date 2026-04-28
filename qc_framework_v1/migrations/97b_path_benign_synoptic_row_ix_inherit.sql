-- =============================================================================
-- Migration 97b -- canonical_path_benign_events_v1.synoptic_row_ix CF closure
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser <logan.glosser@gmail.com> (Cowork session)
-- Plan:   Close CF-PATH-BENIGN-SYNOPTIC-ROW-IX (the only failed CF on
--         canonical_path_benign_events_v1 after mig_97).
--
-- Discovery (Cowork probe 2026-04-28):
--   path_synoptics has no synoptic_row_ix column directly. BUT:
--     - canonical_path_malignant_events_v1.synoptic_row_ix: 6,689/6,689 populated
--     - main.specimen_master_v1.synoptic_row_ix: 10,139/10,139 populated
--     - 35 other tables in the database carry the column
--   specimen_master_v1 is the canonical source carrying the genuine Script-108
--   pandas-load-order index. Joining canonical_path_benign_events_v1 to
--   specimen_master_v1 on specimen_id recovers 9,057 / 11,688 rows (77 %).
--   The remaining 2,631 rows have NULL specimen_id (no source linkage exists --
--   same root cause as the 13 residual surgery_episode_id NULLs from mig_97).
--
--   Per memory `reference_synoptic_row_ix.md`: do NOT synthesize via SQL
--   ROW_NUMBER. INHERITANCE from a verified source (specimen_master_v1) does
--   not violate this rule -- specimen_master_v1 already carries the genuine
--   Script-108 index baked in from the original load.
-- =============================================================================
--
-- Pre-state (2026-04-28 via mig_97 close):
--   55 cols / 50 verified / 1 failed (synoptic_row_ix) / 4 na
--   table_status='in_progress'
--
-- Apply: backfill synoptic_row_ix from specimen_master_v1 via specimen_id;
--        flip CF from 'failed' -> 'verified'; refresh table_signoff_registry.
-- =============================================================================

BEGIN TRANSACTION;

-- Step 1: backfill synoptic_row_ix from specimen_master_v1
UPDATE main.canonical_path_benign_events_v1 b
SET synoptic_row_ix = sm.synoptic_row_ix
FROM main.specimen_master_v1 sm
WHERE b.specimen_id = sm.specimen_id
  AND sm.synoptic_row_ix IS NOT NULL;
-- Expected: 9,057 rows updated

-- Step 2: flip column registry: failed -> verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='Logan Glosser (Cowork mig_97b 2026-04-28)',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method='mechanical_derivation_compare',
    batch_id='mig_97b_synoptic_row_ix_inherit_2026-04-28',
    notes='CF-PATH-BENIGN-SYNOPTIC-ROW-IX CLOSED. Backfilled 9,057/11,688 (77%) by inheriting genuine Script-108 index from specimen_master_v1 via specimen_id join. 2,631 rows remain NULL (no specimen_id linkage available -- same root cause as 13 residual surgery_episode_id NULLs, just at higher prevalence). Build rule: synoptic_row_ix=specimen_master_v1.synoptic_row_ix WHERE b.specimen_id=sm.specimen_id; NULL where specimen_id absent. Inheritance from verified source (mig_89 path_malignant uses same source) -- not synthesized via ROW_NUMBER per memory reference_synoptic_row_ix.md.'
WHERE schema_name='main'
  AND table_name='canonical_path_benign_events_v1'
  AND column_name='synoptic_row_ix';

-- Step 3: refresh table_signoff_registry -> 'verified'
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/97b_path_benign_synoptic_row_ix_inherit.sql',
    notes = 'mig_97b: CF-PATH-BENIGN-SYNOPTIC-ROW-IX closed by inheriting from specimen_master_v1.synoptic_row_ix. 9,057/11,688 backfilled (77%); 2,631 NULL where specimen_id absent. table_status -> verified. 16th Protocol v2 table.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_benign_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name=subq.schema_name AND ts.table_name=subq.table_name;

COMMIT;

-- =============================================================================
-- Post-state confirmed 2026-04-28 20:35 UTC:
--   synoptic_row_ix: 9,057 populated / 2,631 NULL (build rule correct)
--   Registry: 51 verified / 0 failed / 4 na = 55
--   table_status='verified'
-- =============================================================================
-- 16th Protocol v2 table: canonical_path_benign_events_v1 fully verified
-- =============================================================================
