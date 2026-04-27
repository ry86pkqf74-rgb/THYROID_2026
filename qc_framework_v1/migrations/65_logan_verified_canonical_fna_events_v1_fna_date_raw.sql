-- =============================================================================
-- Migration 65 -- Logan-verified column: canonical_fna_events_v1.fna_date_raw
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   qc_framework_v1/MASTER_VERIFICATION_PLAN.md (Protocol v2 Step D)
-- Scope:  main.canonical_fna_events_v1.fna_date_raw  (column 1 of 40 in pilot)
--
-- Source canary CSV:
--   verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv
--
-- Outcome of Logan's review:
--   - 8,042 MATCH rows accepted as-is, no audit entry needed.
--   - 72 AMBIGUOUS rows accepted as-is. Every one had db_value == source_value
--     (raw cell text agrees on both sides). The 2-digit-year ambiguity is a
--     downstream concern (resolved by fna_date_resolved derivation, where
--     Logan ratified rule "2-digit YY -> 20YY: 00=2000, 25=2025, etc."). For
--     fna_date_raw verification, raw-cell agreement is sufficient -> MATCH.
--     One in-bucket outlier (research_id=6024, fna_index=1) where DB has the
--     normalized string '2016-04-25 00:00:00' and source has the Excel serial
--     '42485' is also accepted as-is -- DB build-script normalization is
--     correct; source preserved the raw numeric.
--   - 8 edge-case rows individually adjudicated by Logan with notes captured
--     below (no value changes; documented in canonical_logan_review_log_v1):
--
--       NO_SOURCE_MATCH (DB has phantom row, no source row):
--         research_id=10637, fna_index=3 -- "no FNA 3"
--         research_id=1640,  fna_index=1 -- "incidental data; no FNA's performed"
--
--       SOURCE_NO_DB_MATCH (Excel has data, no DB row):
--         research_id=10637, fna_index=4 -- "only 2 FNA's performed."
--         research_id=1640,  fna_index=2 -- "incidental data; no FNA's performed"
--         research_id=9904,  fna_index=3 -- "no FNA 3"
--
--       DB_NULL_SOURCE_HAS (DB null, source = literal 'n/a'):
--         research_id=1701, fna_index=1  -- "No FNA's performed"
--         research_id=1964, fna_index=1  -- "No FNA's performed"
--         research_id=2904, fna_index=1  -- "No FNA's performed"
--
--   - your_correction was blank for all 8 rows -> 0 UPDATEs to
--     main.canonical_fna_events_v1. fna_date_raw values remain unchanged.
--
-- Net effect of this migration:
--   * 8 INSERTs into manuscript_workspace.canonical_logan_review_log_v1
--   * 1 UPDATE in canonical_column_verification_registry_v1
--       (fna_date_raw -> verified, verification_method=mechanical_source_compare,
--        verified_by=logan, batch_id=mig_65_fna_date_raw, verified_ts=now)
--   * 1 UPDATE in canonical_table_signoff_registry_v1
--       (recompute n_verified=1, n_not_started=39 for canonical_fna_events_v1)
--
-- Executed via Cowork mode's mcp__motherduck__query_rw tool 2026-04-27.
-- =============================================================================

-- (a) Audit-log entries for the 8 individually-adjudicated edge-case rows.
INSERT INTO manuscript_workspace.canonical_logan_review_log_v1
  (log_id, research_id, schema_name, table_name, column_name,
   old_value, new_value, batch_id, csv_path, logan_note)
VALUES
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '10637','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=3 | match_flag=NO_SOURCE_MATCH | no FNA 3 | accepted as-is (no value change)'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '1640','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=1 | match_flag=NO_SOURCE_MATCH | incidental data; no FNAs performed | accepted as-is (no value change)'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '10637','main','canonical_fna_events_v1','fna_date_raw',
   NULL, '2023-04-14 00:00:00 (in source only)', 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=4 | match_flag=SOURCE_NO_DB_MATCH | only 2 FNAs performed (source idx 4 ignored)'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '1640','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=2 | match_flag=SOURCE_NO_DB_MATCH | incidental data; no FNAs performed | accepted as-is'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '9904','main','canonical_fna_events_v1','fna_date_raw',
   NULL, 'e (in source only)', 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=3 | match_flag=SOURCE_NO_DB_MATCH | no FNA 3 (source typo ''e'' ignored)'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '1701','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=1 | match_flag=DB_NULL_SOURCE_HAS | source=''n/a'' | No FNAs performed | DB NULL accepted'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '1964','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=1 | match_flag=DB_NULL_SOURCE_HAS | source=''n/a'' | No FNAs performed | DB NULL accepted'),
  (nextval('manuscript_workspace.seq_logan_review_log_id'),
   '2904','main','canonical_fna_events_v1','fna_date_raw',
   NULL, NULL, 'mig_65_fna_date_raw',
   'verification_csvs/canonical_fna_events_v1/fna_date_raw__mig_65_logan_step_b.csv',
   'fna_index=1 | match_flag=DB_NULL_SOURCE_HAS | source=''n/a'' | No FNAs performed | DB NULL accepted');

-- (b) Flip fna_date_raw to verified in the column registry.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id            = 'mig_65_fna_date_raw',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes, '')
                          || ' | verified by mechanical_source_compare against '
                          || 'FNAs 12_5_2025.xlsx > FNA Bethesda; '
                          || '8042 MATCH, 72 AMBIGUOUS (raw-cell agreement, 2-digit year), '
                          || '8 edge-case dispositions logged in canonical_logan_review_log_v1 '
                          || '(2 NO_SOURCE_MATCH, 3 SOURCE_NO_DB_MATCH, 3 DB_NULL_SOURCE_HAS).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_fna_events_v1'
  AND column_name = 'fna_date_raw';

-- (c) Recompute table-level counts in the sign-off registry.
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
-- end of migration 65
-- =============================================================================
