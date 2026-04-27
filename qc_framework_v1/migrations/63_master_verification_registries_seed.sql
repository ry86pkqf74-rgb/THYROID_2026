-- ============================================================================
-- Migration 63 — Master verification registries seed
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Author:        Logan Glosser (executed via Claude / Cowork)
-- Date:          2026-04-27
-- Issue ID:      MASTER_VERIFICATION_REGISTRIES_SEED
-- ----------------------------------------------------------------------------
-- Purpose:
--   Stand up the two registry tables that drive the global database
--   verification effort described in qc_framework_v1/MASTER_VERIFICATION_PLAN.md.
--
--   1. main.canonical_column_verification_registry_v1
--      One row per column across main + manuscript_workspace base tables
--      (excluding _archived, _legacy, _pre_cleanup, _pre_* snapshots).
--      Auto-classifies each column into one of four categories:
--        - na_provenance     (build/audit/identifier — auto-verified)
--        - derived           (computed from other columns — verify rule)
--        - source            (raw value loaded from Excel/notes — sample-verify)
--        - adjudicated       (clinical judgement — Logan CSV review)
--
--   2. main.canonical_table_signoff_registry_v1
--      One row per table with rolled-up column counts and a priority tier.
--
--   3. manuscript_workspace.canonical_logan_review_log_v1
--      Per-cell audit log for every Logan-driven correction.
--
-- This migration captures the EXACT SQL used to seed the registries on
-- 2026-04-27 so the state is reproducible from a clean DB.
-- ============================================================================

-- 1. Column verification registry --------------------------------------------

CREATE TABLE IF NOT EXISTS main.canonical_column_verification_registry_v1 (
  schema_name           VARCHAR,
  table_name            VARCHAR,
  column_name           VARCHAR,
  data_type             VARCHAR,
  ordinal_position      INTEGER,
  category              VARCHAR,    -- source | derived | adjudicated | na_provenance
  upstream_source       VARCHAR,
  verification_status   VARCHAR,    -- not_started | in_review | verified | failed | na
  verified_by           VARCHAR,    -- 'auto' | 'logan' | 'claude_inline'
  verified_ts           TIMESTAMP,
  verification_method   VARCHAR,
  batch_id              VARCHAR,
  notes                 VARCHAR,
  registered_ts         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Table sign-off registry -------------------------------------------------

CREATE TABLE IF NOT EXISTS main.canonical_table_signoff_registry_v1 (
  schema_name           VARCHAR,
  table_name            VARCHAR,
  n_columns_total       INTEGER,
  n_verified            INTEGER,
  n_not_started         INTEGER,
  n_failed              INTEGER,
  n_na                  INTEGER,
  table_status          VARCHAR,
  signed_off_ts         TIMESTAMP,
  signoff_migration     VARCHAR,
  priority_tier         VARCHAR,
  notes                 VARCHAR,
  registered_ts         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Logan correction audit log ----------------------------------------------

CREATE TABLE IF NOT EXISTS manuscript_workspace.canonical_logan_review_log_v1 (
  log_id          BIGINT,
  research_id     VARCHAR,
  schema_name     VARCHAR,
  table_name      VARCHAR,
  column_name     VARCHAR,
  old_value       VARCHAR,
  new_value       VARCHAR,
  batch_id        VARCHAR,
  change_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  csv_path        VARCHAR,
  logan_note      VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS manuscript_workspace.seq_logan_review_log_id START 1;

-- 4. Auto-classification populate of column registry -------------------------

DELETE FROM main.canonical_column_verification_registry_v1;

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position,
   category, verification_status, verified_by, verification_method, notes)
SELECT
  c.table_schema, c.table_name, c.column_name, c.data_type, c.ordinal_position,
  CASE
    WHEN c.column_name IN (
      'build_script','build_ts','last_updated_ts','registered_ts','ingested_at_utc',
      'ingest_script_version','ingest_sheet_spec',
      'extracted_at','llm_build_ts','llm_model','llm_provider','llm_sdk','llm_sdk_version',
      'llm_base_url','provider_returned_model','provider_system_fingerprint',
      'raw_llm_response','parsed_json','result_json','elapsed_s',
      'source_workbook','source_sheet','source_column','source_table','source_tables',
      'source_priority','source_tables_represented','source_row_id','source_line',
      'preprocess_batch_id','preprocessed_at_utc','preprocess_script_version',
      'consolidation_source','cleaned_at','queued_at','queued_by_script','flagged_by','flagged_at',
      'op_enrichment_source','episode_source_mix','status','note',
      'note_row_id','note_index','note_type','note_date','linkage_date','entity_domain','domain'
    ) THEN 'na_provenance'
    WHEN c.column_name = 'research_id' OR c.column_name LIKE '%_id'
         OR c.column_name LIKE '%_uid' OR c.column_name = 'specimen_focus_id'
      THEN 'na_provenance'
    WHEN c.column_name LIKE '%_clean'
      OR c.column_name LIKE '%_clean\_%' ESCAPE '\'
      OR c.column_name LIKE '%_normalized'
      OR c.column_name LIKE '%_normalized_%'
      OR c.column_name LIKE '%_derived'
      OR c.column_name LIKE '%_resolved'
      OR c.column_name LIKE '%_resolved_%'
      OR c.column_name LIKE '%_recomputed'
      OR c.column_name LIKE '%_calculated'
      OR c.column_name LIKE '%_grouped'
      OR c.column_name LIKE '%_final'
      OR c.column_name LIKE '%_final\_%' ESCAPE '\'
      OR c.column_name LIKE '%_v2'
      OR c.column_name LIKE '%_v3'
      OR c.column_name LIKE '%_v4'
      OR c.column_name LIKE '%_v5'
      OR c.column_name LIKE '%_rebound'
      OR c.column_name LIKE '%_rebind'
      OR c.column_name LIKE 'days_to_%'
      OR c.column_name LIKE '%_days'
      OR c.column_name LIKE 'is_%'
      OR c.column_name LIKE 'has_%'
      OR c.column_name LIKE 'any_%'
      OR c.column_name LIKE 'n_%'
      OR c.column_name LIKE '%_score'
      OR c.column_name LIKE '%_summary'
      OR c.column_name LIKE '%_status'
      OR c.column_name LIKE '%_count'
      OR c.column_name LIKE 'worst_%'
      THEN 'derived'
    WHEN c.column_name LIKE '%_raw'
      OR c.column_name LIKE '%_text'
      OR c.column_name LIKE '%_native'
      OR c.column_name LIKE '%_native\_%' ESCAPE '\'
      OR c.column_name LIKE '%_quote'
      OR c.column_name LIKE 'evidence_%'
      OR c.column_name = 'reasoning'
      OR c.column_name LIKE '%_findings'
      OR c.column_name LIKE 'original_%'
      THEN 'source'
    WHEN c.table_name IN ('path_synoptics','ct_imaging','mri_imaging','nuclear_med',
                           'manuscript_cohort_v1','clinical_notes_long','data_dictionary_v279',
                           'nsqip_enrichment','nsqip_patient_summary','rai_treatment_episode_v2')
      OR c.table_name LIKE 'note_entities_%'
      THEN 'source'
    ELSE 'adjudicated'
  END AS category,
  CASE
    WHEN c.column_name IN (
      'build_script','build_ts','last_updated_ts','registered_ts','ingested_at_utc',
      'ingest_script_version','ingest_sheet_spec','extracted_at','llm_build_ts','llm_model',
      'llm_provider','llm_sdk','llm_sdk_version','llm_base_url','provider_returned_model',
      'provider_system_fingerprint','raw_llm_response','parsed_json','result_json','elapsed_s',
      'source_workbook','source_sheet','source_column','source_table','source_tables',
      'source_priority','source_tables_represented','source_row_id','source_line',
      'preprocess_batch_id','preprocessed_at_utc','preprocess_script_version',
      'consolidation_source','cleaned_at','queued_at','queued_by_script','flagged_by','flagged_at',
      'op_enrichment_source','episode_source_mix','status','note','note_row_id','note_index',
      'note_type','note_date','linkage_date','entity_domain','domain'
    ) THEN 'na'
    WHEN c.column_name = 'research_id' OR c.column_name LIKE '%_id' OR c.column_name LIKE '%_uid'
      THEN 'na'
    ELSE 'not_started'
  END AS verification_status,
  CASE
    WHEN c.column_name IN ('build_script','build_ts','last_updated_ts','registered_ts','ingested_at_utc',
      'ingest_script_version','ingest_sheet_spec','extracted_at','llm_build_ts','llm_model','llm_provider',
      'llm_sdk','llm_sdk_version','llm_base_url','provider_returned_model','provider_system_fingerprint',
      'raw_llm_response','parsed_json','result_json','elapsed_s','source_workbook','source_sheet',
      'source_column','source_table','source_tables','source_priority','source_tables_represented',
      'source_row_id','source_line','preprocess_batch_id','preprocessed_at_utc','preprocess_script_version',
      'consolidation_source','cleaned_at','queued_at','queued_by_script','flagged_by','flagged_at',
      'op_enrichment_source','episode_source_mix','status','note','note_row_id','note_index','note_type',
      'note_date','linkage_date','entity_domain','domain')
      THEN 'auto'
    WHEN c.column_name = 'research_id' OR c.column_name LIKE '%_id' OR c.column_name LIKE '%_uid'
      THEN 'auto'
    ELSE NULL
  END AS verified_by,
  CASE
    WHEN c.column_name IN ('build_script','build_ts','last_updated_ts','registered_ts','ingested_at_utc',
      'ingest_script_version','ingest_sheet_spec','extracted_at','llm_build_ts','llm_model','llm_provider',
      'llm_sdk','llm_sdk_version','llm_base_url','provider_returned_model','provider_system_fingerprint',
      'raw_llm_response','parsed_json','result_json','elapsed_s','source_workbook','source_sheet',
      'source_column','source_table','source_tables','source_priority','source_tables_represented',
      'source_row_id','source_line','preprocess_batch_id','preprocessed_at_utc','preprocess_script_version',
      'consolidation_source','cleaned_at','queued_at','queued_by_script','flagged_by','flagged_at',
      'op_enrichment_source','episode_source_mix','status','note','note_row_id','note_index','note_type',
      'note_date','linkage_date','entity_domain','domain')
      THEN 'auto_provenance_skip'
    WHEN c.column_name = 'research_id' OR c.column_name LIKE '%_id' OR c.column_name LIKE '%_uid'
      THEN 'auto_identifier_skip'
    ELSE NULL
  END AS verification_method,
  NULL AS notes
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE c.table_schema IN ('main','manuscript_workspace')
  AND t.table_type = 'BASE TABLE'
  AND c.table_name NOT LIKE '%_archived_%'
  AND c.table_name NOT LIKE '%_pre_cleanup_%'
  AND c.table_name NOT LIKE '%_pre_%'
  AND c.table_name NOT LIKE '%_legacy%';

-- 5. Populate the table sign-off registry from the column registry -----------

DELETE FROM main.canonical_table_signoff_registry_v1;

INSERT INTO main.canonical_table_signoff_registry_v1
  (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
   table_status, priority_tier)
SELECT
  schema_name, table_name,
  COUNT(*),
  SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END),
  SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END),
  SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END),
  SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END),
  CASE
    WHEN SUM(CASE WHEN verification_status NOT IN ('verified','na') THEN 1 ELSE 0 END) = 0 THEN 'verified'
    WHEN SUM(CASE WHEN verification_status='in_review' THEN 1 ELSE 0 END) > 0 THEN 'in_progress'
    ELSE 'not_started'
  END AS table_status,
  CASE
    WHEN table_name = 'canonical_fna_events_v1' THEN 'pilot'
    WHEN table_name = 'canonical_patient_master' THEN 'tier1_anchor'
    WHEN table_name LIKE 'canonical_%_events_v1' THEN 'tier1_events'
    WHEN table_name LIKE 'canonical_%_patient_rollup_v1' THEN 'tier2_rollups'
    WHEN table_name IN ('path_synoptics','clinical_notes_long','ct_imaging','mri_imaging','nuclear_med',
                         'manuscript_cohort_v1','rai_treatment_episode_v2',
                         'canonical_us_lymph_node_v2','canonical_us_nodule_v2','canonical_us_thyroid_gland_v2',
                         'nsqip_enrichment','nsqip_patient_summary')
      THEN 'tier1_source'
    WHEN table_name LIKE 'canonical_%' THEN 'tier2_canonical'
    WHEN table_name LIKE 'note_entities_%' THEN 'tier3_extraction'
    ELSE 'tier3_helper'
  END AS priority_tier
FROM main.canonical_column_verification_registry_v1
GROUP BY 1,2;

-- 6. Acceptance probes -------------------------------------------------------

-- Total counts:
-- SELECT category, COUNT(*) FROM main.canonical_column_verification_registry_v1 GROUP BY 1;
--   adjudicated   2834
--   derived       1150
--   na_provenance  767
--   source         745

-- Tier counts:
-- SELECT priority_tier, COUNT(*) FROM main.canonical_table_signoff_registry_v1 GROUP BY 1;
--   pilot               1
--   tier1_anchor        1
--   tier1_events       18
--   tier1_source       12
--   tier2_canonical    16
--   tier2_rollups      19
--   tier3_extraction   17
--   tier3_helper       91
