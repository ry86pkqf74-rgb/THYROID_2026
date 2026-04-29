-- =============================================================================
-- Migration 111 -- canonical_cervical_ln_clinical_events_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Sibling sign-off to mig_110 pathology_clinical. Same UNNEST entity-grain
--         shape, same extraction-faithfulness pattern.
--
-- Methodology: Extraction-faithfulness vs upstream JSON, UNNEST variant.
--   Build (Script 382) is structurally identical to Script 369 — UNNESTs
--   `note_entities_llm_cervical_ln_detail.result_json.entities[]` then DISTINCT ON
--   (research_id, note_row_id, entity_type, entity_value, source_line).
--
-- Verification probe (run 2026-04-29):
--   - Canonical: 4,493 rows / 1,643 patients
--   - Upstream note_entities_llm_cervical_ln_detail: 10,084 rows
--   - After UNNEST + DISTINCT: 4,493 fresh rows
--   - 4,493/4,493 joined on natural key
--   - 8/8 not_started cols: 0 drift (entity_type, entity_value, present_or_negated,
--     confidence, evidence_text, entity_date, date_confidence, date_source_keyword)
--
-- Sign-off scope:
--   8 not_started cols flipped via extraction_faithfulness_vs_upstream_json_unnest
--   7 already-na cols carry over (research_id + 5 provenance + source_line)
--
-- Vocab confirmed clean:
--   entity_type: 12 values (ln_level, ln_size, fna_of_ln, ln_morphology, etc.)
--   present_or_negated: present (3,391) + negated (1,102)
--
-- Final state: 8 verified + 7 na = 15/15 closed.
-- 25th canonical table verified.
-- =============================================================================

-- 111a: flip 8 not_started cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_upstream_json_unnest',
    batch_id            = 'mig_111_cervical_ln_clinical_events_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_111: extraction-faithfulness vs '
                          || 'main.note_entities_llm_cervical_ln_detail '
                          || '(Script 382 UNNEST + DISTINCT, sibling of Script '
                          || '369 pathology). 4,493/4,493 rows match fresh '
                          || 're-derivation under IS-DISTINCT-FROM compare. '
                          || 'UNNEST variant of mig_102 pattern.'
WHERE schema_name='main'
  AND table_name='canonical_cervical_ln_clinical_events_v1'
  AND verification_status='not_started';

-- 111b: sign off
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified, n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed,0), n_na = subq.n_na,
    table_status = CASE WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified' WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/111_cervical_ln_clinical_events_table_signoff.sql',
    notes = 'Extraction-faithfulness vs note_entities_llm_cervical_ln_detail (Script 382 UNNEST + DISTINCT). 8/8 not_started cols 0 drift on 4,493 rows. UNNEST variant of mig_102 pattern.'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_cervical_ln_clinical_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
