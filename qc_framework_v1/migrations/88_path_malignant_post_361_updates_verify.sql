-- =============================================================================
-- Migration 88 -- canonical_path_malignant_events_v1: 6 post-361-UPDATE cols VERIFIED
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Verify the 6 columns added by Script 361 UPDATEs after the initial
--         CTC SELECT * copy (those are out-of-scope for mig_87 since they have
--         no counterpart in CTC pre361).
--
-- Method: mechanical_derivation_compare (re-run the Script 361 UPDATE rule
--         as a SELECT and compare to canonical's stored values).
--
-- Cols verified (6):
--   Path A (TEM v2 pre361 archive, 4 cols, 6,693/6,693 = 100 %):
--     discordance_histology_flag    = TEM.histology_discordance_flag
--     discordance_t_stage_flag      = TEM.t_stage_discordance_flag
--     discordance_notes             = CASE
--                                       WHEN TEM.consult_precedence_flag IS TRUE
--                                         OR TEM.consult_diagnosis IS NOT NULL
--                                       THEN COALESCE(NULLIF(TRIM(TEM.consult_diagnosis),''),
--                                                     'consult_precedence_flag=TRUE')
--                                       ELSE NULL
--                                     END
--     discordance_laterality_flag   = NULL (sentinel; no upstream source per
--                                     Script 361 design comment)
--     Source: "Thyroid 2026 UPdated".archive_pub_v1_0.tumor_episode_master_v2_pre361_20260422_002245
--     Join: TRY_CAST(rid AS BIGINT) = TRY_CAST(rid AS BIGINT)
--           AND surgery_episode_id = . AND tumor_ordinal = .
--
--   Path B (STF v1 LIVE in main, 2 cols, 6,689/6,689 = 100 %):
--     specimen_focus_id       = STF.specimen_focus_id (top-rank pick)
--     linkage_confidence_tier = STF.linkage_confidence_tier
--     linkage_score           = STF.linkage_score
--     Top-rank pick: ROW_NUMBER() OVER (PARTITION BY rid, sid, ordinal
--                       ORDER BY linkage_score DESC NULLS LAST,
--                                specimen_focus_id) = 1
--     Source: main.specimen_tumor_focus_v1 (LIVE; not archived)
--     Note: specimen_focus_id is already na_provenance (will flip at Step D);
--           verified as a copy-faithful audit but registry status not changed
--           by this migration. The 2 cols flipped here are the analytic-payload
--           fields linkage_confidence_tier (Bucket D) and linkage_score (Bucket C).
--
-- Probe results (this session):
--   discordance_histology_flag    : 6,693 / 6,693 MATCH
--   discordance_t_stage_flag      : 6,693 / 6,693 MATCH
--   discordance_notes             : 6,693 / 6,693 MATCH
--   discordance_laterality_flag   : 6,693 / 6,693 NULL (sentinel rule confirmed)
--   linkage_confidence_tier       : 6,689 / 6,689 MATCH
--   linkage_score                 : 6,689 / 6,689 MATCH
--
-- Post-mig_88 table progress:
--   Verified : 44 / 47 not-na cols (38 prev + 6 this batch)
--   Remaining: 0 non-na not_started (all 47 not-na cols verified)
--   Plus 12 na_provenance cols pending Step D batch flip (mig_89).
--
-- No row-level data writes; no canonical_logan_review_log_v1 entries.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 88a: flip 4 TEM-derived cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = '"Thyroid 2026 UPdated".archive_pub_v1_0.tumor_episode_master_v2_pre361_20260422_002245 (Script 361 UPDATE rule)',
    batch_id            = 'mig_88_path_malignant_post_361_updates',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_88: re-ran Script 361 UPDATE rule against archived TEM v2 pre361. '
                          || 'discordance_histology_flag = TEM.histology_discordance_flag (6,693/6,693). '
                          || 'discordance_t_stage_flag = TEM.t_stage_discordance_flag (6,693/6,693). '
                          || 'discordance_notes = CASE on TEM.consult_precedence_flag/consult_diagnosis (6,693/6,693). '
                          || 'discordance_laterality_flag = NULL sentinel rule confirmed (6,693/6,693 NULL).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name IN (
    'discordance_histology_flag', 'discordance_t_stage_flag',
    'discordance_laterality_flag', 'discordance_notes'
  );

-- 88b: flip 2 STF-derived cols (specimen_focus_id is already na, deferred to Step D)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = 'main.specimen_tumor_focus_v1 (LIVE; Script 361 top-rank pick rule: ROW_NUMBER OVER partition by (rid, sid, ordinal) ORDER BY linkage_score DESC NULLS LAST, specimen_focus_id)',
    batch_id            = 'mig_88_path_malignant_post_361_updates',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_88: re-ran Script 361 STF UPDATE rule against LIVE specimen_tumor_focus_v1. '
                          || 'Match: 6,689/6,689. Top-rank pick rule: highest linkage_score per (rid, sid, ordinal) '
                          || 'with specimen_focus_id ascending tiebreak.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name IN ('linkage_confidence_tier', 'linkage_score');

-- 88c: recompute signoff
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified,
    n_not_started = subq.n_not_started, n_failed = COALESCE(subq.n_failed, 0), n_na = subq.n_na,
    table_status = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1' GROUP BY 1,2
) subq WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 88 -- 6 post-361-UPDATE cols verified
-- Table progress: 44 verified / 3 not_started (deferred Step D) / 9 na = 56 total
-- =============================================================================
