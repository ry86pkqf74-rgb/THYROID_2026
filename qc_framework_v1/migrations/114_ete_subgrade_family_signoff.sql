-- =============================================================================
-- Migration 114 -- canonical_ete_subgrade_events_v1 + _patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Close the ETE subgrade family in one migration. Both tables are LLM-
--         extracted (mig_54 build, openai/gpt-oss-120b) via Migration 55 SQL.
--         Events: 287 rows / 151 patients. Rollup: 151 rows / 151 patients.
--
-- Methodology:
--   Events: extraction-faithfulness vs upstream JSON (mig_102 pattern), with
--     CASE NORMALIZATION on ete_grade ('unable_to_determine' -> 'unknown')
--   Rollup: derivation re-derivation against verified events for 13 internal
--     cols + cross-table LEFT JOIN to canonical_t4b_invasion_patient_rollup_v1
--     for 2 crosswalk cols (any_pT4b_from_t4b_invasion +
--     pT4b_ete_vs_t4b_invasion_discordant)
--
-- Verification probes (run 2026-04-29 via Cowork query_rw):
--   - Events: 287/287 rows, 5/5 not_started cols 0 drift on IS-DISTINCT-FROM
--     vs fresh json_extract from note_entities_llm_ete_subgrade_v1 (error=0)
--   - Rollup: 151/151 patients, 13/13 internal-derived cols 0 drift, 2/2
--     cross-table cols 0 drift vs LEFT JOIN to verified t4b_invasion rollup
--
-- Sign-off scope:
--   Events: 5 not_started → verified (4 via extraction_faithfulness_vs_upstream_json,
--           1 via extraction_faithfulness_with_case_normalization for ete_grade)
--           + 12 already-na (provenance/identifier auto skips)
--   Rollup: 15 not_started → verified (13 via derivation_re_derivation_against_verified_events,
--           2 via derivation_re_derivation_cross_table_t4b_rollup)
--           + 3 already-na (research_id, build_script, build_ts)
--
-- Final state:
--   canonical_ete_subgrade_events_v1: 5 verified + 12 na = 17/17 closed
--   canonical_ete_subgrade_patient_rollup_v1: 15 verified + 3 na = 18/18 closed
--
-- Vocab confirmed clean (events table):
--   ete_grade: gross (142), unknown (92), microscopic (40), absent (13) — per CASE
--   ajcc8_implication: pT3b (89), pT4a (51), pT3a_size_only (6), pT4b (3), null (138)
--   confidence: high (217), medium (65), low (5)
--
-- 28th + 29th canonical tables verified under Protocol v2.
-- ete_subgrade family complete.
-- Executed via Cowork query_rw 2026-04-29.
-- =============================================================================

-- 114a: events — flip 4 cols via standard extraction-faithfulness
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'extraction_faithfulness_vs_upstream_json',
    batch_id = 'ete_subgrade_events_signoff_20260429',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_114: extraction-faithfulness vs '
            || 'main.note_entities_llm_ete_subgrade_v1 (error=0). 287/287 rows '
            || 'match fresh json_extract_string re-derivation under IS-DISTINCT-'
            || 'FROM compare. Build: Migration 55 (mig_54_ete_subgrade_20260424).'
WHERE schema_name='main'
  AND table_name='canonical_ete_subgrade_events_v1'
  AND verification_status='not_started'
  AND column_name <> 'ete_grade';

-- 114b: events — flip ete_grade with CASE-normalization note
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'extraction_faithfulness_with_case_normalization',
    batch_id = 'ete_subgrade_events_signoff_20260429',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_114: 287/287 rows match fresh re-derivation. '
            || 'CASE normalization applied at build: gross/microscopic/absent '
            || 'pass through; unable_to_determine -> unknown; ELSE -> unknown. '
            || 'Drift only on rows where source had values outside enum (none '
            || 'observed in 287 rows).'
WHERE schema_name='main'
  AND table_name='canonical_ete_subgrade_events_v1'
  AND column_name='ete_grade'
  AND verification_status='not_started';

-- 114c: events — table signoff
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified, n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed,0), n_na = subq.n_na,
    table_status = CASE WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified' WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql',
    notes = 'Extraction-faithfulness vs note_entities_llm_ete_subgrade_v1 (Migration 55 build, mig_54 tag). 287 rows / 151 patients. 4/5 cols pure json_extract; ete_grade has CASE normalization (unable_to_determine -> unknown).'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_ete_subgrade_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- 114d: rollup — flip 13 internal-derived cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'derivation_re_derivation_against_verified_events',
    batch_id = 'ete_subgrade_rollup_signoff_20260429',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_114: per-pt aggregate + worst-mention re-derivation '
            || 'against verified canonical_ete_subgrade_events_v1. Worst-mention '
            || 'logic: ROW_NUMBER OVER PARTITION BY research_id ORDER BY '
            || 'grade_rank DESC, ajcc_rank DESC, conf_rank DESC. 151/151 patients '
            || '0 drift.'
WHERE schema_name='main'
  AND table_name='canonical_ete_subgrade_patient_rollup_v1'
  AND verification_status='not_started'
  AND column_name NOT IN ('any_pT4b_from_t4b_invasion','pT4b_ete_vs_t4b_invasion_discordant');

-- 114e: rollup — flip 2 cross-table crosswalk cols
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'derivation_re_derivation_cross_table_t4b_rollup',
    batch_id = 'ete_subgrade_rollup_signoff_20260429',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_114: cross-table LEFT JOIN to verified '
            || 'canonical_t4b_invasion_patient_rollup_v1.any_pT4b_final '
            || '(t4b family verified mig_92 + mig_95). Discordant flag has '
            || 'NULL handling per Migration 56 build SQL. 151/151 patients '
            || '0 drift.'
WHERE schema_name='main'
  AND table_name='canonical_ete_subgrade_patient_rollup_v1'
  AND column_name IN ('any_pT4b_from_t4b_invasion','pT4b_ete_vs_t4b_invasion_discordant')
  AND verification_status='not_started';

-- 114f: rollup — table signoff
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified, n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed,0), n_na = subq.n_na,
    table_status = CASE WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified' WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql',
    notes = 'Derivation re-derivation against verified canonical_ete_subgrade_events_v1 (worst-mention logic + counts) + cross-table LEFT JOIN to verified canonical_t4b_invasion_patient_rollup_v1 (2 crosswalk cols). 151 patients / 151 rows / 0 drift on 15 derivable cols. ete_subgrade family complete.'
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
    SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
    SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
    SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
    SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_ete_subgrade_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 114 -- ete_subgrade family closed (28th + 29th canonical)
-- =============================================================================
