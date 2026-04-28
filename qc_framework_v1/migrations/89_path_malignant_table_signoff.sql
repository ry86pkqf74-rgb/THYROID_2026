-- =============================================================================
-- Migration 89 -- canonical_path_malignant_events_v1 SIGN-OFF (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Final sign-off of canonical_path_malignant_events_v1 under Protocol v2
--         Step D. Third table closed under v2 (after FNA pilot mig_78 and
--         airway invasion mig_83).
--
-- Net session arc (mig_84 -> mig_89, all 2026-04-28):
--   mig_84 : structural opener (drop 4 deprecated cols + 11 dependent
--            fingerprint views; reclass 3 cols to na_provenance)
--   mig_85 : surgery_date (mechanical_source_compare, 6,689/6,689 MATCH
--            against path_synoptics.surg_date)
--   mig_86 : tumor_ordinal (mechanical_derivation_compare, two-path rule:
--            Script 108 SLOT_MAP + archived TEM v2 text-extraction; 6,689/6,689 MATCH)
--   mig_87 : 36 inherited cols batch-verified via CTC pre361 mass equivalence
--            (faithful-copy check; 6,695/6,695 MATCH for 35 cols, gross_ete
--            6,689/6,695 with 6 join-duplicate cosmetic artifact)
--   mig_88 : 6 post-361-UPDATE cols verified by re-running Script 361 UPDATE
--            rules (Path A TEM v2 archive: 4 cols 6,693/6,693; Path B STF v1
--            LIVE: 2 cols 6,689/6,689)
--   mig_89 : this file -- Step D batch flip of 12 auto_no_source_counterpart
--            cols + table sign-off.
--
-- Step D batch flip (12 cols):
--   9 already-na (set 'na' at registry seed time):
--     research_id, surgery_episode_id, path_surgery_id, specimen_id,
--     specimen_focus_id, source_tables, build_script, build_ts, consolidation_source
--   3 reclassed in mig_84d to na_provenance + auto_no_source_counterpart but
--   left at status='not_started' for explicit Step D flip:
--     synoptic_row_ix, histology_source, resolution_rule
--
-- Final state of canonical_path_malignant_events_v1 (post-mig_89):
--   Rows     : 6,689
--   Patients : 4,137
--   Cols     : 56 (started 60, dropped 4 deprecated in mig_84)
--   Verified : 56 / 56 = 100 %
--   Carry-forwards (all deferred, not blocking):
--     CF-86-1 : 64 Path-B tumor_ordinal rows (text-extraction via archived
--               TEM v2). Verifiable against archive_pub_v1_0 if future
--               restore-and-reverify is run.
--     CF-87-AJCC : AJCC7/8 staging values verified as faithful copies of CTC
--                  pre361. Findings-vs-staging derivation correctness (Logan
--                  airway-invasion rule extended to ETE/multifocality/nodal)
--                  is upstream of canonical (CTC build, scripts 251/266).
--                  Defer to a future round.
--     CF-87-GROSS-ETE : 6 of 6,695 join-duplicate rows show inconsistent
--                  gross_ete between paired archive rows. Each canonical row
--                  matches at least one archive row. Cosmetic. Defer.
--
-- Architectural innovations established this session (carry forward to
-- subsequent tables in queue):
--   - **CTC-equivalence verification pattern**: when a canonical was built
--     by Script-361-style SELECT * + filter + UPDATEs, the archived
--     pre-script snapshot is the value-source-of-truth and a single
--     mass-equivalence query verifies dozens of inherited cols at once.
--     Read-only verification reference; canonical is never sourced from
--     archive (consistent with feedback_no_cross_db_canonical_sourcing.md).
--   - **Script-rule re-run verification** for post-build UPDATE-derived
--     cols: re-run the original UPDATE logic as a SELECT and compare row
--     by row against canonical's stored value. Works for both archived
--     upstreams (TEM v2 pre361) and live ones (specimen_tumor_focus_v1).
--
-- This is the THIRD table closed under Protocol v2.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 89a: Step D batch flip of 12 auto_no_source_counterpart cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_89_path_malignant_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_89: auto-verified at table sign-off (Step D); '
                          || 'pure provenance/pipeline-trace, no source counterpart. '
                          || 'Joined the 9 already-na cols + 3 reclassed-in-mig_84d for the '
                          || '12-col batch flip per FNA pilot precedent (mig_78c).'
WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1'
  AND (verification_status = 'na'
       OR (verification_status = 'not_started'
           AND verification_method = 'auto_no_source_counterpart'));

-- 89b: recompute table_signoff_registry counts and sign off
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
    END,
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/89_path_malignant_table_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 89 -- canonical_path_malignant_events_v1 closed
-- THIRD table verified under Protocol v2.
-- =============================================================================
