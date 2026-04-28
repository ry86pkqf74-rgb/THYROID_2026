-- =============================================================================
-- Migration 94b -- canonical_vascular_invasion_events_v1 quantification follow-up
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Two follow-up tweaks after mig_94 sign-off, both per Logan
--         clinical direction:
--
--   1. vascular_invasion_extent backfill: 284 vi='present' rows had NULL
--      extent. Probe of evidence_quote showed the SOURCE REPORTS themselves
--      did not grade extent (e.g. "Vascular Invasion: Present" with no
--      "focal/extensive" qualifier; some explicitly say "extent not
--      specified"). Rather than leave NULL (which is ambiguous), introduce
--      sentinel value 'unspecified' so analytics can distinguish "source
--      said present without grading" from "extent not applicable".
--
--      Final vocab: focal | extensive | minimal | widely_invasive | unspecified
--      NULL is now reserved for vi != 'present' rows (extent not applicable).
--
--   2. vessel_count analytics warning. Numeric vessel_count is captured in
--      only ~5% of vi='present' rows because CAP synoptic typically uses
--      categorical ranges ("less than 4 vessels" / "4 or more vessels") not
--      exact integers. The categorical info is in vascular_invasion_extent.
--      Add a column comment + registry note so downstream consumers don't
--      use vessel_count for analytics.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 94b-row-1: backfill 284 NULL extent rows on vi='present'
UPDATE main.canonical_vascular_invasion_events_v1
SET vascular_invasion_extent = 'unspecified'
WHERE vascular_invasion = 'present'
  AND vascular_invasion_extent IS NULL;

-- 94b-comment: column comments
COMMENT ON COLUMN main.canonical_vascular_invasion_events_v1.vessel_count IS
  'DO NOT USE FOR ANALYTICS. Numeric vessel count is captured in only ~5% of '
  'vascular_invasion=present rows because CAP synoptic typically uses categorical '
  'ranges ("less than 4 vessels" / "4 or more vessels") rather than exact integers. '
  'Prefer vascular_invasion_extent for tier analysis. mig_94 / 2026-04-28.';

COMMENT ON COLUMN main.canonical_vascular_invasion_events_v1.vascular_invasion_extent IS
  'Categorical extent grading. Vocab: focal | extensive | minimal | widely_invasive '
  '| unspecified. unspecified = source explicitly said "Present" without grading '
  'extent (mig_94 backfill 2026-04-28; 284 rows). NULL = extent not applicable '
  '(vascular_invasion != present).';

-- 94b-registry: append analytics-warning + backfill notes to column verification registry
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
         || ' | mig_94 follow-up 2026-04-28: ANALYTICS WARNING -- vessel_count '
         || 'captured in only ~5% of vi=present rows because CAP synoptic uses '
         || 'categorical ranges, not exact integers. Use vascular_invasion_extent '
         || 'for tier analysis. Column comment added.'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND column_name = 'vessel_count';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
         || ' | mig_94 follow-up 2026-04-28: backfilled 284 NULL extent rows '
         || 'for vi=present to "unspecified" sentinel (source said Present but '
         || 'did not grade). Final vocab: focal | extensive | minimal | '
         || 'widely_invasive | unspecified. NULL only when vi != present.'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND column_name = 'vascular_invasion_extent';

-- =============================================================================
-- end of mig_94b -- canonical_vascular_invasion_events_v1 quantification stable
-- Final extent distribution on vi=present (739 rows):
--   focal: 209 / extensive: 175 / minimal: 57 / widely_invasive: 14 / unspecified: 284
-- =============================================================================
