-- =============================================================================
-- Migration 72 -- specimen_location verified
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Protocol-v2 mechanical_source_compare for specimen_location.
-- Scope:  main.canonical_fna_events_v1.specimen_location
--
-- Result: 7,993 trim-match + 61 both-NULL + 0 diverge across 8,054 rows.
--
-- Net effect:
--   * 0 row-level value changes
--   * 1 row in canonical_column_verification_registry_v1 flipped to verified
--   * 1 row in canonical_table_signoff_registry_v1 recomputed
--     (n_verified: 17 -> 18 of 39)
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id            = 'mig_72_specimen_location',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_72: 100% match against source workbook '
                          || '(7,993 trim-match + 61 both-null + 0 diverge of 8,054 rows). '
                          || 'specimen_site_raw mirror was dropped in mig_69; '
                          || 'specimen_location is the authoritative specimen column.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'specimen_location';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 72
-- =============================================================================
