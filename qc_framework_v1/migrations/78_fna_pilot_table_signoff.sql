-- =============================================================================
-- Migration 78 -- FNA pilot table sign-off (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Final sign-off of the FNA pilot table (canonical_fna_events_v1)
--         under Protocol v2 Step D.
--
-- mig_78a: 2 final typo/abbreviation fixes Logan caught
--   10069/2: 'Right cervical level II' (Roman at end-of-string, my rule
--            missed) -> fna_site=lymph_node_right_level_2
--   10180/1: 'THYROID NODULE, R. SUPERIOR' (R. abbreviation, my rule
--            missed) -> fna_site=thyroid_right_lobe, laterality=right
--
-- mig_78b: drop is_index_fna column (Logan: "drop index column")
--   No views referenced it. Registry row also removed.
--
-- mig_78c: flip 14 auto_no_source_counterpart provenance columns to verified
--   Per Protocol v2 Step D: deferred during per-column work, batch-flipped
--   at table sign-off. These are pure pipeline-trace / build-metadata cols
--   with no source counterpart:
--     fna_event_id, research_id, fna_date_status, fna_date_confidence,
--     bethesda_confidence, bethesda_derivation_method, bethesda_rules_category,
--     bethesda_rules_confidence, bethesda_provider, bethesda_evidence_present,
--     path_text_length, source_tables_represented, ingest_script_version,
--     ingested_at_utc
--
-- Plus fna_index (semantics ratified: source-workbook column position 1..12,
-- NOT chronological; chronological order is fna_seq_n).
--
-- days_to_surgery deferred as carry-forward (Logan: "no need, we can
-- derive later if we want it"). Marked verification_status='failed' as
-- a deferred-blocked sentinel; sign-off rule allows up to 1 such carry-forward.
--
-- Final state:
--   canonical_fna_events_v1: 8,050 rows / 38 cols / 37 verified / 1 deferred
--   table_status = 'verified'
--   signed_off_ts = 2026-04-28
--   signoff_migration = qc_framework_v1/migrations/78_fna_pilot_table_signoff.sql
--
-- This is the FIRST pilot table closed under Protocol v2.
--
-- Net session arc (mig_65 -> mig_78):
--   - Verified all source columns against FNAs 12_5_2025.xlsx > FNA Bethesda
--   - Verified bethesda category against FNAs_Rescored_Long_Format.xlsx
--   - Re-derived all date/index/sequence/flag rollups from cleaned data
--   - Added fna_site as new structured anatomic-site column
--   - Renamed pathology_diagnosis -> fna_history, pathology_extended ->
--     fna_pathology_report
--   - Dropped specimen_site_raw (mig_69), subtype (mig_74), is_index_fna (mig_78)
--   - Net rowcount: 8,119 -> 8,050 (removed 69 phantom rows)
--   - Net columns: 40 -> 38
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 78a: typo/abbreviation fixes
UPDATE main.canonical_fna_events_v1
SET fna_site = 'lymph_node_right_level_2'
WHERE research_id = '10069' AND fna_index = 2;

UPDATE main.canonical_fna_events_v1
SET fna_site = 'thyroid_right_lobe', laterality = 'right'
WHERE research_id = '10180' AND fna_index = 1;

-- 78b: drop is_index_fna
ALTER TABLE main.canonical_fna_events_v1 DROP COLUMN is_index_fna;
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_fna_events_v1' AND column_name='is_index_fna';

-- 78c: flip 14 provenance + fna_index to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_78_fna_pilot_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_78: auto-verified at FNA pilot table sign-off (Step D); '
                          || 'pure provenance/pipeline-trace, no source counterpart.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name IN (
    'fna_event_id', 'research_id',
    'fna_date_status', 'fna_date_confidence',
    'bethesda_confidence', 'bethesda_derivation_method',
    'bethesda_rules_category', 'bethesda_rules_confidence',
    'bethesda_provider', 'bethesda_evidence_present',
    'path_text_length', 'source_tables_represented',
    'ingest_script_version', 'ingested_at_utc'
  );

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_78_fna_pilot_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_78: source-workbook column position 1..12 (NOT chronological); '
                          || 'chronological order is in fna_seq_n. Stable identifier preserved.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'fna_index';

-- days_to_surgery: deferred carry-forward
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'failed',
    notes               = COALESCE(notes,'')
                          || ' | mig_78: DEFERRED carry-forward. Cross-table derivation '
                          || '(fna_date_resolved + canonical_operative_events_v1.surgery_date). '
                          || 'Logan: "no need, we can derive later if we want it." '
                          || 'Will be revisited if/when needed.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'days_to_surgery';

-- Recompute table signoff and sign off
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started = 0 AND COALESCE(subq.n_failed,0) <= 1 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/78_fna_pilot_table_signoff.sql'
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
-- end of migration 78 -- FNA pilot table closed
-- =============================================================================
