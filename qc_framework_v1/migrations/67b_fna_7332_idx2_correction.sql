-- =============================================================================
-- Migration 67b -- canonical_fna_events_v1 research_id 7332 idx 2 correction
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   follow-up to mig_67 to resolve the conflict flagged on 7332
-- Scope:  main.canonical_fna_events_v1.fna_date_raw, single row
--
-- mig_67 applied two literal CSV-cell values for research_id 7332:
--   idx 1 -> 04/16/2019
--   idx 2 -> 05/07/2019  (sourced from Logan's note "date for both FNA 1 and
--                         FNA 2 should be 5/7/2019"; treated as the idx-2 cell)
-- Logan clarified: BOTH dates should be 04/16/2019. Adjust idx 2 to match.
--
-- Net effect: 1 UPDATE.
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_fna_events_v1
SET fna_date_raw = '04/16/2019'
WHERE research_id = '7332' AND fna_index = 2;

-- =============================================================================
-- end of migration 67b
-- =============================================================================
