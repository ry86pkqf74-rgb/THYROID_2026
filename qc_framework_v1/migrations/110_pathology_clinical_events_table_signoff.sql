-- =============================================================================
-- Migration 110 -- canonical_pathology_clinical_events_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Verify LLM-extracted pathology_clinical events canonical via
--         extraction-faithfulness pattern (mig_102 sibling), adapted for
--         UNNEST-style multi-entity-per-note JSON extraction.
--
-- Methodology: Extraction-faithfulness vs upstream JSON (UNNEST variant)
--   The build (Script 369, build_script tag in events) UNNESTs
--   `note_entities_llm_pathology.result_json.entities` into one row per entity,
--   then DISTINCT ON (research_id, note_row_id, entity_type, entity_value,
--   source_line). Verification re-derives every col from the same upstream and
--   compares per-row.
--
-- Verification probe results (run 2026-04-29):
--   - Canonical: 13,358 rows / 3,382 patients
--   - Upstream note_entities_llm_pathology: 10,084 rows (each carries entities[])
--   - After UNNEST + DISTINCT: 13,358 fresh rows
--   - Joined on natural key (research_id, note_row_id, entity_type, entity_value,
--     source_line): 13,358 / 13,358 ✓
--   - 8/8 not_started cols: 0 drift on IS-DISTINCT-FROM (entity_type,
--     entity_value, present_or_negated, confidence, evidence_text, entity_date,
--     date_confidence, date_source_keyword)
--
-- Sign-off scope:
--   8 not_started cols flipped to verified via
--     verification_method='extraction_faithfulness_vs_upstream_json_unnest'
--   7 already-na cols carry over: research_id, note_row_id, source_column,
--     note_type, note_date, source_line, build_ts (auto provenance/identifier)
--
-- Notable: note_date is VARCHAR but always empty string ('' in 13,358/13,358
-- rows). It's a legacy provenance placeholder, NOT a clinical date column.
-- The actual clinical date is `entity_date` which is already DATE type. So
-- there is no date-type violation here — note_date stays na, no retype needed.
--
-- Final state of canonical_pathology_clinical_events_v1 (post-mig_110):
--   Rows     : 13,358 (entity grain — multiple entities per note)
--   Patients : 3,382
--   Cols     : 15
--   Verified : 8 / 15 + 7 na = 15 / 15 closed
--
-- Vocab confirmed clean:
--   entity_type: 14 values (fna_cytology, benign_pathology, surgical_pathology,
--     tumor_size, molecular_testing, bethesda_class, extrathyroidal_extension,
--     lymph_node_pathology, frozen_section, margin_status, multifocality,
--     lymphovascular_invasion, tumor_variant, perineural_invasion)
--   present_or_negated: 2 values (present 12,141, negated 1,217)
--   note_type: 7 values (HP, OPNOTE, OTHER_HISTORY, ENDOCRINE_FM, DC_SUM,
--     OTHER_NOTES, ED_NOTE)
--
-- 24th canonical table closed under Protocol v2 (post-cleanliness audit mig_109).
-- Executed via Cowork query_rw 2026-04-29.
-- =============================================================================

-- 110a: flip 8 not_started cols via extraction-faithfulness (UNNEST variant)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_upstream_json_unnest',
    batch_id            = 'mig_110_pathology_clinical_events_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_110: extraction-faithfulness vs '
                          || 'main.note_entities_llm_pathology (Script 369 '
                          || 'UNNEST + DISTINCT ON natural key). 13,358/13,358 '
                          || 'rows match fresh re-derivation under IS-DISTINCT-'
                          || 'FROM compare. UNNEST variant of mig_102 pattern.'
WHERE schema_name='main'
  AND table_name='canonical_pathology_clinical_events_v1'
  AND verification_status='not_started';

-- 110b: recompute table_signoff_registry counts and sign off
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
    signed_off_ts     = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql',
    notes             = 'Extraction-faithfulness vs upstream '
                        || 'note_entities_llm_pathology (Script 369 UNNEST + '
                        || 'DISTINCT). 8/8 not_started cols 0 drift on 13,358 '
                        || 'rows. UNNEST variant of mig_102 extraction-'
                        || 'faithfulness pattern. note_date is VARCHAR but '
                        || 'always empty string (legacy placeholder, not a '
                        || 'clinical date — actual date is entity_date DATE).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_pathology_clinical_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 110 -- canonical_pathology_clinical_events_v1 closed
-- 24th canonical table verified.
-- =============================================================================
