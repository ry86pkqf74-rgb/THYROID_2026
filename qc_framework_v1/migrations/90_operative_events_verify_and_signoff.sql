-- =============================================================================
-- Migration 90 -- canonical_operative_events_v1 VERIFY + SIGN-OFF (combined)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Apply the CTC-equivalence verification pattern (established in
--         mig_87) to canonical_operative_events_v1. Single combined migration
--         since the table is clean (no structural opener needed; no per-col
--         investigation required).
--
-- This is the FOURTH table closed under Protocol v2 (after FNA pilot mig_78,
-- airway invasion mig_83, path malignant mig_89).
--
-- Build pattern (scripts/362_operative_consolidation.py):
--   Step 1a: CREATE OR REPLACE TABLE main.canonical_operative_events_v1 AS
--            SELECT [TRY_CAST(research_id AS BIGINT) + all other cols]
--            FROM main.operative_episode_detail_v2
--   Step 1b: UPDATE 6 op_detail_*_n enrichment cols from
--            main.note_entities_operative_detail (per-rid count of present-or-
--            negated entities by entity_type)
--   Step 1c: ADD COLUMN provenance fields (build_script, build_ts,
--            consolidation_source) + UPDATE with constants
--   (Script 363 later stripped 4 cols: gross_ete_flag, local_invasion_flag,
--    tracheal_involvement_flag, esophageal_involvement_flag — out of scope)
--
-- Verification (Probe results, this session):
--   Path A (CTC-equivalence vs operative_episode_detail_v2_pre362_20260422_005646):
--     43 inherited cols × 11,773/11,773 MATCH (100 %).
--     resolved_surgery_date raw bytes differ (archive ISO 'YYYY-MM-DD' vs
--     canonical US 'MM/DD/YYYY') but parse to equal dates: 11,773/11,773
--     MATCH under TRY_STRPTIME.
--
--   Path B (Step 1b UPDATE rule re-run vs note_entities_operative_detail):
--     6 enrichment cols × 11,773/11,773 MATCH:
--       op_detail_nerve_monitoring_n   = COUNT(*) FILTER (entity_type='nerve_monitoring')
--       op_detail_ebl_n                = COUNT(*) FILTER (entity_type='ebl')
--       op_detail_parathyroid_mgmt_n   = COUNT(*) FILTER (entity_type='parathyroid_management')
--       op_detail_intraop_complication_n = COUNT(*) FILTER (entity_type='intraop_complication')
--       op_detail_reoperative_field_n  = COUNT(*) FILTER (entity_type='reoperative_field')
--       op_detail_total_mentions       = COUNT(*) FILTER (any entity_type)
--     All filters AND COALESCE(present_or_negated, 'present') = 'present'.
--
--   Step D batch flip (10 cols already na_provenance):
--     research_id, surgery_episode_id (auto_identifier_skip)
--     source_tables, op_enrichment_source, episode_source_mix
--     linked_pathology_episode_id, linked_fna_episode_id (auto_identifier_skip)
--     build_script, build_ts, consolidation_source (auto_provenance_skip)
--
-- Final state (post-mig_90):
--   Rows     : 11,773
--   Patients : 10,871
--   Cols     : 54 / 54 verified (100 %)
--   table_status = verified
--
-- Carry-forwards:
--   CF-90-DATE-FORMAT: resolved_surgery_date is stored as 'MM/DD/YYYY' in canonical
--     vs 'YYYY-MM-DD' in the pre362 archive. Date values are identical; the format
--     was reformatted by a downstream normalization pass (NOT Script 362 itself —
--     Script 362 was a literal SELECT *). The reformat may have happened in
--     normalize_dates_v1_0_pass1.py or a related cleanup script. Defer:
--     downstream consumers parse the string anyway, so format isn't blocking.
--
-- This sign-off ALSO unblocks the FNA pilot's deferred carry-forward
-- canonical_fna_events_v1.days_to_surgery (cross-table derivation against
-- canonical_operative_events_v1.resolved_surgery_date / surgery_date_native).
-- That CF can now be re-opened and verified if desired.
--
-- No row-level data writes; no canonical_logan_review_log_v1 entries.
-- Read-only verification reference: archive table not modified.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 90a: flip 43 inherited cols to verified via mass-equivalence vs pre362 archive
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = '"Thyroid 2026 UPdated".archive_pub_v1_0.operative_episode_detail_v2_pre362_20260422_005646 (immediate upstream that produced canonical via Script 362 Step 1a SELECT * + research_id BIGINT cast)',
    batch_id            = 'mig_90_operative_events_verify_and_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_90: 11,773/11,773 MATCH against operative_episode_detail_v2_pre362 archive '
                          || '(faithful-copy equivalence via Script 362 Step 1a SELECT * + research_id BIGINT cast). '
                          || 'resolved_surgery_date 100% under date-parsing (raw bytes differ -- US vs ISO format -- '
                          || 'but parsed dates match). CF-90-DATE-FORMAT.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_operative_events_v1'
  AND column_name IN (
    'surgery_date_native', 'resolved_surgery_date', 'date_status',
    'procedure_raw', 'procedure_normalized', 'laterality',
    'central_neck_dissection_flag', 'lateral_neck_dissection_flag',
    'rln_monitoring_flag', 'rln_finding_raw',
    'parathyroid_autograft_flag', 'parathyroid_autograft_count', 'parathyroid_autograft_site',
    'parathyroid_resection_flag', 'strap_muscle_involvement_flag', 'reoperative_field_flag',
    'ebl_ml', 'drain_flag', 'operative_findings_raw',
    'op_confidence',
    'note_date_resolved', 'note_date_source', 'note_date_confidence',
    'parathyroid_identified_count',
    'frozen_section_flag', 'berry_ligament_flag',
    'ebl_ml_nlp',
    'path_link_score_v3', 'fna_link_score_v3',
    'episode_rank', 'n_dates_in_cluster',
    'n_entity_notes_in_episode',
    'episode_has_cpm_anchor', 'episode_cpm_ordinal',
    'frozen_section_n',
    'frozen_section_any_malignant_flag', 'frozen_section_any_deferred_flag',
    'frozen_section_any_suspected_malignant_flag'
  );

-- 90b: flip 6 op_detail enrichment cols to verified via Script 362 Step 1b UPDATE re-run
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = 'main.note_entities_operative_detail (LIVE; Script 362 Step 1b UPDATE rule: COUNT FILTER per entity_type per research_id)',
    batch_id            = 'mig_90_operative_events_verify_and_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_90: 11,773/11,773 MATCH via re-run of Script 362 Step 1b UPDATE rule. '
                          || 'COUNT(*) FILTER (entity_type=<X> AND COALESCE(present_or_negated,present)=present) '
                          || 'aggregated by research_id; entity_types: nerve_monitoring, ebl, parathyroid_management, '
                          || 'intraop_complication, reoperative_field; total = COUNT(*) FILTER (any entity_type, present).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_operative_events_v1'
  AND column_name IN (
    'op_detail_nerve_monitoring_n', 'op_detail_ebl_n',
    'op_detail_parathyroid_mgmt_n', 'op_detail_intraop_complication_n',
    'op_detail_reoperative_field_n', 'op_detail_total_mentions'
  );

-- 90c: Step D batch flip of 10 auto_no_source_counterpart cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_90_operative_events_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_90: auto-verified at table sign-off (Step D); '
                          || 'pure provenance/identifier/pipeline-trace, no source counterpart.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_operative_events_v1'
  AND verification_status = 'na';

-- 90d: recompute table_signoff_registry counts and sign off
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
    signoff_migration = 'qc_framework_v1/migrations/90_operative_events_verify_and_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_operative_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 90 -- canonical_operative_events_v1 closed
-- FOURTH table verified under Protocol v2.
-- Pattern productivity: 54 cols verified in single migration vs 6-migration arc
-- for path_malignant. CTC-equivalence pattern is paying off.
-- =============================================================================
