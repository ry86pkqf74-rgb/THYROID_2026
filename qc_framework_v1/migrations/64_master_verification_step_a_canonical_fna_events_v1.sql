-- =============================================================================
-- Migration 64 — master verification, Step A re-tier, canonical_fna_events_v1
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   qc_framework_v1/MASTER_VERIFICATION_PLAN.md (Protocol v2)
-- Scope:  main.canonical_fna_events_v1 (40 columns, pilot table)
--
-- This migration captures the Step-A re-tier of the FNA pilot table under
-- Protocol v2 ("full-row mechanical compare"). It does NOT verify any data
-- column — that happens in subsequent batches when each source/derived/
-- adjudicated column is reviewed by Logan and signed off.
--
-- Net effect when applied:
--   * 40 column rows in canonical_column_verification_registry_v1 updated:
--       category, upstream_source, batch_id, notes set
--       verification_status reset to 'not_started' for the 5 cols previously
--         carrying status='na' (under v1's auto-skip semantics, deprecated)
--   * 1 row in canonical_table_signoff_registry_v1 recomputed:
--       n_total=40, n_verified=0, n_not_started=40, n_na=0,
--       table_status='not_started'
--
-- Tier breakdown after this migration:
--   na_provenance: 14 (auto-verified at Step D table sign-off)
--   source:        7  (mechanical_source_compare → FNAs 12_5_2025.xlsx)
--   derived:       16 (mechanical_derivation_compare)
--   adjudicated:   3  (manual_source_review against upstream raw text)
--
-- Executed via Cowork mode's mcp__motherduck__query_rw tool 2026-04-27.
-- This file is the canonical record of the change for replay/audit.
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET
  category = CASE column_name
    WHEN 'fna_event_id'                  THEN 'na_provenance'
    WHEN 'research_id'                   THEN 'na_provenance'
    WHEN 'fna_date_status'               THEN 'na_provenance'
    WHEN 'fna_date_confidence'           THEN 'na_provenance'
    WHEN 'bethesda_confidence'           THEN 'na_provenance'
    WHEN 'bethesda_derivation_method'    THEN 'na_provenance'
    WHEN 'bethesda_rules_category'       THEN 'na_provenance'
    WHEN 'bethesda_rules_confidence'     THEN 'na_provenance'
    WHEN 'bethesda_provider'             THEN 'na_provenance'
    WHEN 'bethesda_evidence_present'     THEN 'na_provenance'
    WHEN 'path_text_length'              THEN 'na_provenance'
    WHEN 'source_tables_represented'     THEN 'na_provenance'
    WHEN 'ingest_script_version'         THEN 'na_provenance'
    WHEN 'ingested_at_utc'               THEN 'na_provenance'
    WHEN 'fna_date_raw'                  THEN 'source'
    WHEN 'specimen_location'             THEN 'source'
    WHEN 'specimen_site_raw'             THEN 'source'
    WHEN 'bethesda_original_text'        THEN 'source'
    WHEN 'bethesda_reasoning'            THEN 'source'
    WHEN 'pathology_diagnosis'           THEN 'source'
    WHEN 'pathology_extended'            THEN 'source'
    WHEN 'fna_index'                     THEN 'derived'
    WHEN 'fna_seq_n'                     THEN 'derived'
    WHEN 'fna_total_n_for_patient'       THEN 'derived'
    WHEN 'is_first_fna'                  THEN 'derived'
    WHEN 'is_last_fna'                   THEN 'derived'
    WHEN 'is_index_fna'                  THEN 'derived'
    WHEN 'fna_date_resolved'             THEN 'derived'
    WHEN 'days_from_first_fna'           THEN 'derived'
    WHEN 'days_to_surgery'               THEN 'derived'
    WHEN 'bethesda_2010_num'             THEN 'derived'
    WHEN 'bethesda_2010_name'            THEN 'derived'
    WHEN 'bethesda_2015_num'             THEN 'derived'
    WHEN 'bethesda_2015_name'            THEN 'derived'
    WHEN 'bethesda_2023_num'             THEN 'derived'
    WHEN 'bethesda_2023_name'            THEN 'derived'
    WHEN 'bethesda_final_num'            THEN 'derived'
    WHEN 'laterality'                    THEN 'adjudicated'
    WHEN 'bethesda_calculated_num'       THEN 'adjudicated'
    WHEN 'subtype'                       THEN 'adjudicated'
    ELSE category
  END,
  upstream_source = CASE column_name
    WHEN 'fna_index'                     THEN 'research_id, fna_date_resolved'
    WHEN 'fna_seq_n'                     THEN 'research_id, fna_date_resolved'
    WHEN 'fna_total_n_for_patient'       THEN 'research_id'
    WHEN 'is_first_fna'                  THEN 'fna_seq_n'
    WHEN 'is_last_fna'                   THEN 'fna_seq_n, fna_total_n_for_patient'
    WHEN 'is_index_fna'                  THEN 'fna_seq_n, days_to_surgery'
    WHEN 'fna_date_resolved'             THEN 'fna_date_raw'
    WHEN 'days_from_first_fna'           THEN 'fna_date_resolved (per-patient first FNA)'
    WHEN 'days_to_surgery'               THEN 'fna_date_resolved + surgery date (cross-table)'
    WHEN 'bethesda_2010_num'             THEN 'bethesda_calculated_num'
    WHEN 'bethesda_2010_name'            THEN 'bethesda_calculated_num'
    WHEN 'bethesda_2015_num'             THEN 'bethesda_calculated_num'
    WHEN 'bethesda_2015_name'            THEN 'bethesda_calculated_num'
    WHEN 'bethesda_2023_num'             THEN 'bethesda_calculated_num'
    WHEN 'bethesda_2023_name'            THEN 'bethesda_calculated_num'
    WHEN 'bethesda_final_num'            THEN 'bethesda_calculated_num'
    WHEN 'laterality'                    THEN 'specimen_site_raw, specimen_location'
    WHEN 'bethesda_calculated_num'       THEN 'bethesda_original_text'
    WHEN 'subtype'                       THEN 'pathology_extended'
    ELSE upstream_source
  END,
  verification_status = CASE
    WHEN column_name IN ('fna_event_id','research_id','source_tables_represented','ingest_script_version','ingested_at_utc')
      THEN 'not_started'
    ELSE verification_status
  END,
  verified_by = CASE
    WHEN column_name IN ('fna_event_id','research_id','source_tables_represented','ingest_script_version','ingested_at_utc')
      THEN NULL
    ELSE verified_by
  END,
  verified_ts = CASE
    WHEN column_name IN ('fna_event_id','research_id','source_tables_represented','ingest_script_version','ingested_at_utc')
      THEN NULL
    ELSE verified_ts
  END,
  verification_method = CASE
    WHEN column_name IN ('fna_event_id','research_id','source_tables_represented','ingest_script_version','ingested_at_utc')
      THEN NULL
    ELSE verification_method
  END,
  batch_id = 'mig_64_fna_pilot_step_a',
  notes = CASE
    WHEN column_name = 'fna_event_id'                THEN 'pure provenance: content-derived hash; no source counterpart; auto-verify at Step D table sign-off'
    WHEN column_name = 'research_id'                 THEN 'pure provenance: patient identifier; no source counterpart; auto-verify at Step D table sign-off'
    WHEN column_name = 'fna_date_status'             THEN 'pipeline trace; superseded by manual verification of fna_date_resolved; auto-verify at Step D'
    WHEN column_name = 'fna_date_confidence'         THEN 'pipeline trace (100/0); superseded by manual verification of fna_date_resolved; auto-verify at Step D'
    WHEN column_name = 'bethesda_confidence'         THEN 'pipeline confidence; not a clinical truth; auto-verify at Step D'
    WHEN column_name = 'bethesda_derivation_method'  THEN 'pipeline trace (rules/llm/rules+llm/none); auto-verify at Step D'
    WHEN column_name = 'bethesda_rules_category'     THEN 'intermediate rules-engine output; auto-verify at Step D'
    WHEN column_name = 'bethesda_rules_confidence'   THEN 'intermediate rules-engine confidence; auto-verify at Step D'
    WHEN column_name = 'bethesda_provider'           THEN 'LLM provider name; auto-verify at Step D'
    WHEN column_name = 'bethesda_evidence_present'   THEN 'extraction-pipeline flag; not derivable cleanly from any DB column; auto-verify at Step D'
    WHEN column_name = 'path_text_length'            THEN 'upstream extraction trace; does not equal LENGTH(pathology_extended) for most rows; auto-verify at Step D'
    WHEN column_name = 'source_tables_represented'   THEN 'pure provenance: build script output; auto-verify at Step D'
    WHEN column_name = 'ingest_script_version'       THEN 'pure provenance: build script version; auto-verify at Step D'
    WHEN column_name = 'ingested_at_utc'             THEN 'pure provenance: ingest timestamp; auto-verify at Step D'
    WHEN column_name = 'fna_date_raw'                THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > Date cell (per FNA index 1..12)'
    WHEN column_name = 'specimen_location'           THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > Specimen received cell'
    WHEN column_name = 'specimen_site_raw'           THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > Specimen received cell (mirror of specimen_location, distinction TBD)'
    WHEN column_name = 'bethesda_original_text'      THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > Bethesda cell'
    WHEN column_name = 'bethesda_reasoning'          THEN 'LLM reasoning text; sourced from rescoring pipeline output, treated as source-text and reviewed inline alongside bethesda_original_text'
    WHEN column_name = 'pathology_diagnosis'         THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > History cell'
    WHEN column_name = 'pathology_extended'          THEN 'mechanical_source_compare → FNAs 12_5_2025.xlsx > FNA Bethesda > Path extended cell'
    WHEN column_name = 'fna_date_resolved'           THEN 'mechanical_derivation_compare; rule: parse fna_date_raw to DATE'
    WHEN column_name = 'days_to_surgery'             THEN 'mechanical_derivation_compare; rule: DATE_DIFF(surgery_date, fna_date_resolved)'
    WHEN column_name = 'days_from_first_fna'         THEN 'mechanical_derivation_compare; rule: DATE_DIFF(fna_date_resolved, MIN(fna_date_resolved) OVER patient)'
    WHEN column_name = 'fna_seq_n'                   THEN 'mechanical_derivation_compare; rule: ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved)'
    WHEN column_name = 'fna_index'                   THEN 'mechanical_derivation_compare; rule: DENSE_RANK() OVER (PARTITION BY research_id ORDER BY fna_date_resolved)'
    WHEN column_name = 'fna_total_n_for_patient'     THEN 'mechanical_derivation_compare; rule: COUNT(*) OVER (PARTITION BY research_id)'
    WHEN column_name = 'is_first_fna'                THEN 'mechanical_derivation_compare; rule: fna_seq_n = 1'
    WHEN column_name = 'is_last_fna'                 THEN 'mechanical_derivation_compare; rule: fna_seq_n = fna_total_n_for_patient'
    WHEN column_name = 'is_index_fna'                THEN 'mechanical_derivation_compare; rule: per-cohort index FNA selection (TBD documented)'
    WHEN column_name LIKE 'bethesda_201%_num'        THEN 'mechanical_derivation_compare; rule: identical to bethesda_calculated_num'
    WHEN column_name LIKE 'bethesda_201%_name'       THEN 'mechanical_derivation_compare; rule: num→name lookup keyed on bethesda_calculated_num'
    WHEN column_name LIKE 'bethesda_2023%_num'       THEN 'mechanical_derivation_compare; rule: identical to bethesda_calculated_num'
    WHEN column_name LIKE 'bethesda_2023%_name'      THEN 'mechanical_derivation_compare; rule: num→name lookup keyed on bethesda_calculated_num'
    WHEN column_name = 'bethesda_final_num'          THEN 'mechanical_derivation_compare; rule: bethesda_calculated_num (canonical final)'
    WHEN column_name = 'laterality'                  THEN 'manual_source_review; upstream raw text: specimen_site_raw / specimen_location'
    WHEN column_name = 'bethesda_calculated_num'     THEN 'manual_source_review; upstream raw text: bethesda_original_text'
    WHEN column_name = 'subtype'                     THEN 'manual_source_review; upstream raw text: pathology_extended'
    ELSE notes
  END
WHERE schema_name = 'main' AND table_name = 'canonical_fna_events_v1';

-- Recompute table-level counts.
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
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 64
-- =============================================================================
