-- =============================================================================
-- Migration 70 -- canonical_fna_events_v1 derivation recompute
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Protocol-v2 mechanical_derivation_compare for 6 derived columns,
--         re-derived from the now-clean fna_date_raw (MM/DD/YYYY).
-- Scope:  main.canonical_fna_events_v1
--
-- Pre-state: fna_date_resolved was severely stale (e.g. research_id 6529's
-- 11 rows ALL showed 2013-02-10 or 2015-09-11 regardless of fna_date_raw).
-- The mig_67/69 deletes also broke fna_total_n_for_patient (6529 still said 12
-- after idx 7 was removed; should be 11).
--
-- Derivation rules applied (all chronological by fna_date_resolved with
-- fna_event_id tiebreaker for same-date ties):
--
--   fna_date_resolved        := CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE)
--   fna_seq_n                := ROW_NUMBER() OVER (PARTITION BY research_id
--                                                  ORDER BY fna_date_resolved,
--                                                           fna_event_id)
--   fna_total_n_for_patient  := COUNT(*) OVER (PARTITION BY research_id)
--   is_first_fna             := fna_seq_n = 1
--   is_last_fna              := fna_seq_n = fna_total_n_for_patient
--   days_from_first_fna      := DATE_DIFF('day', MIN(fna_date_resolved) OVER
--                                                  (PARTITION BY research_id),
--                                                fna_date_resolved)
--
-- Columns NOT recomputed in this migration:
--   fna_index            -- left as source-workbook column position (1..12);
--                           NOT chronological. mig_64 documented this as
--                           DENSE_RANK by date but the data has always been
--                           the source-workbook position. Doc-data conflict
--                           resolved in favor of data; semantics noted here.
--   is_index_fna         -- per-cohort index FNA selection rule TBD; deferred.
--   days_to_surgery      -- cross-table to surgery date; deferred to op-events
--                           verification round.
--
-- Net effect:
--   8,054 rows updated (every row in canonical_fna_events_v1)
--   6 rows in canonical_column_verification_registry_v1 flipped to verified
--     (verification_method=mechanical_derivation_compare, batch_id=mig_70_fna_derivations)
--   1 row in canonical_table_signoff_registry_v1 recomputed
--     (n_verified: 1 -> 7, n_not_started: 38 -> 32, table_status=in_progress)
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_fna_events_v1 db
SET
  fna_date_resolved        = sub.dt,
  fna_seq_n                = sub.seqn,
  fna_total_n_for_patient  = sub.tot,
  is_first_fna             = sub.is_first,
  is_last_fna              = sub.is_last,
  days_from_first_fna      = sub.days_from_first
FROM (
  SELECT
    fna_event_id,
    CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE) AS dt,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE), fna_event_id
    ) AS seqn,
    COUNT(*) OVER (PARTITION BY research_id) AS tot,
    (ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE), fna_event_id
    ) = 1) AS is_first,
    (ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE), fna_event_id
    ) = COUNT(*) OVER (PARTITION BY research_id)) AS is_last,
    DATE_DIFF('day',
      MIN(CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE)) OVER (PARTITION BY research_id),
      CAST(strptime(fna_date_raw, '%m/%d/%Y') AS DATE)
    ) AS days_from_first
  FROM main.canonical_fna_events_v1
) sub
WHERE db.fna_event_id = sub.fna_event_id;

-- Registry flips for the 6 verified derived columns
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_70_fna_derivations',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | recomputed in mig_70 from cleaned fna_date_raw '
                          || '(MM/DD/YYYY); rule: ROW_NUMBER chronological with '
                          || 'fna_event_id tiebreaker for same-date ties.'
WHERE schema_name='main'
  AND table_name='canonical_fna_events_v1'
  AND column_name IN (
    'fna_date_resolved',
    'fna_seq_n',
    'fna_total_n_for_patient',
    'is_first_fna',
    'is_last_fna',
    'days_from_first_fna'
  );

-- Recompute table-level signoff counts
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
-- end of migration 70
-- =============================================================================
