-- =============================================================================
-- Migration 109 -- Cleanliness audit pass over all 37 verified tables
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Pre-forge-ahead cleanliness audit before continuing into next batch
--         of Tier 1 events (pathology_clinical, cervical_ln_clinical). Verifies
--         all 37 currently-verified tables meet Protocol v2 quality standards.
--
-- Audit gates (re-run 2026-04-29 via Cowork query_rw):
--   1. verified_tables_total: 37 (27 canonical + 10 raw note_entities_llm_*)
--   2. tables_missing_signoff: 0 (after this fix; was 10 pre-fix)
--   3. tables_count_mismatch: 0 (n_verified + n_na = n_columns_total, n_not_started=0,
--                               n_failed=0 on every verified table)
--   4. verified_cols_missing_metadata: 0 (verified_by, batch_id, verification_method
--                                          all populated on every verified col)
--   5. date_violations_on_verified: 0 (no clinical-date col is VARCHAR or
--                                       TIMESTAMP — Cursor 1 cleanup held + new
--                                       verifications didn't reintroduce violations)
--
-- Minor findings documented as known CFs (not blockers, not requiring fix):
--   - canonical_medications_events_v1.med_status missing 'historical' value (0
--     of 6,473 rows). STRUCTURAL per Script 365 CHANGE H comment: source
--     `note_entities_medications` extracts only the bare med name; no
--     past-tense context window means MED_HISTORICAL_MARKERS never fire.
--     Tier-1 CF inherited from build, already documented.
--   - canonical_parathyroid_events_v1: 17 rows have confidence=NULL with ALL
--     extraction cols also NULL (empty LLM responses). Inherited from mig_58
--     build; canonical extraction is faithful to upstream LLM output. Already
--     covered by CF-58 family in mig_102 close-out.
--
-- Fix applied: backfilled signoff_migration on the 10 raw note_entities_llm_*
-- tier3_extraction tables that were registry-seeded as 'verified' without an
-- explicit signoff_migration. These are raw LLM-output mirrors (not subject to
-- per-col verification at this layer; their content is QC-checked at LLM
-- extraction time via the extractor's `error` column).
-- =============================================================================

-- 109a: backfill signoff_migration on the 10 tier3_extraction raw mirror tables
UPDATE main.canonical_table_signoff_registry_v1
SET signoff_migration = 'registry_seed_raw_llm_mirror_exempt',
    signed_off_ts     = COALESCE(signed_off_ts, registered_ts),
    notes             = COALESCE(notes,'')
                        || 'Raw LLM-output mirror table; exempt from per-col '
                        || 'verification (no canonical adjudication required at '
                        || 'this layer). Content is already QC-checked at LLM '
                        || 'extraction time via the extractor''s error column '
                        || 'and downstream canonicalization into canonical_'
                        || '<domain>_events_v1 tables. Audited 2026-04-29 in '
                        || 'cleanliness pass.'
WHERE table_status='verified'
  AND signoff_migration IS NULL
  AND priority_tier='tier3_extraction';

-- =============================================================================
-- end of migration 109 -- cleanliness audit + raw-mirror tagging complete
-- All 37 verified tables now pass all 5 audit gates. Lakehouse is clean.
-- =============================================================================
