-- ============================================================================
-- Migration 40 — GEN06 resolved_test_date tiered fallback + GEN07 ROM out-of-range
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   GEN06 — canonical_molecular_genetics_v2.resolved_test_date NULL on 903 rows
--           (test_date_native ALSO null on all 903 — tier-2 fallback inside
--            canonical yields nothing; specimen_genomic_assay_v1_relinked
--            recovers 378, 525 remain unresolvable)
--   GEN07 — rom_percent_point out of [0,100]: observed 2 rows (registry 6 —
--           stale). Both are raw "395%" / "599%" which read like OCR/typo.
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Tiered date fallback:
--   Tier A: resolved_test_date (if non-null and non-empty)           → parse as DATE
--   Tier B: test_date_native   (if non-null)                          → cast to DATE
--   Tier C: specimen_genomic_assay_v1_relinked.test_date_native      → MIN per molecular_episode_id
--   Tier D: unresolved                                                → queue
--
-- Linkage key for Tier C: sga.legacy_molecular_episode_id = canonical.molecular_episode_id
--
-- ROM range check: gen07_rom_out_of_range_flag surfaces any row where
--   rom_percent_point, rom_percent_low, or rom_percent_high falls outside
--   [0,100]. Queued as-is — fix requires re-OCR of raw text, which is human work.
--
-- Output:
--   manuscript_workspace.canonical_molecular_genetics_v2_date_clean (VIEW)
--     + resolved_test_date_final       DATE
--     + resolved_test_date_source      VARCHAR ∈ {resolved, native, sga, unresolved}
--     + gen06_date_unresolved_flag     BOOLEAN
--     + gen07_rom_out_of_range_flag    BOOLEAN
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_molecular_genetics_v2_date_clean AS
WITH sga_fallback AS (
  SELECT legacy_molecular_episode_id AS molecular_episode_id,
         MIN(test_date_native) AS sga_test_date_native
  FROM manuscript_workspace.specimen_genomic_assay_v1_relinked
  WHERE test_date_native IS NOT NULL
  GROUP BY legacy_molecular_episode_id
)
SELECT
  g.*,
  CASE
    WHEN g.resolved_test_date IS NOT NULL AND LENGTH(TRIM(g.resolved_test_date))>0
         AND TRY_CAST(g.resolved_test_date AS DATE) IS NOT NULL
      THEN TRY_CAST(g.resolved_test_date AS DATE)
    WHEN g.test_date_native IS NOT NULL
      THEN CAST(g.test_date_native AS DATE)
    WHEN s.sga_test_date_native IS NOT NULL
      THEN CAST(s.sga_test_date_native AS DATE)
    ELSE NULL
  END AS resolved_test_date_final,
  CASE
    WHEN g.resolved_test_date IS NOT NULL AND LENGTH(TRIM(g.resolved_test_date))>0
         AND TRY_CAST(g.resolved_test_date AS DATE) IS NOT NULL THEN 'resolved'
    WHEN g.test_date_native IS NOT NULL THEN 'native'
    WHEN s.sga_test_date_native IS NOT NULL THEN 'sga'
    ELSE 'unresolved'
  END AS resolved_test_date_source,
  ((g.resolved_test_date IS NULL OR LENGTH(TRIM(g.resolved_test_date))=0
    OR TRY_CAST(g.resolved_test_date AS DATE) IS NULL)
   AND g.test_date_native IS NULL
   AND s.sga_test_date_native IS NULL) AS gen06_date_unresolved_flag,
  ((g.rom_percent_point IS NOT NULL AND (g.rom_percent_point < 0 OR g.rom_percent_point > 100))
   OR (g.rom_percent_low IS NOT NULL AND (g.rom_percent_low < 0 OR g.rom_percent_low > 100))
   OR (g.rom_percent_high IS NOT NULL AND (g.rom_percent_high < 0 OR g.rom_percent_high > 100))
  ) AS gen07_rom_out_of_range_flag
FROM main.canonical_molecular_genetics_v2 g
LEFT JOIN sga_fallback s ON s.molecular_episode_id = g.molecular_episode_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('GEN06','GEN07');

-- GEN06: rows with no date in any tier
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN06',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    platform := platform,
    resolved_test_date := resolved_test_date,
    test_date_native := test_date_native,
    parser := parser,
    parse_status := parse_status
  )),
  'GEN06 resolved_test_date unresolvable — canonical has no native date and SGA fallback yields no match',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_date_clean
WHERE gen06_date_unresolved_flag;

-- GEN07: ROM out of [0,100]
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN07',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    platform := platform,
    rom_percent_raw := rom_percent_raw,
    rom_percent_point := rom_percent_point,
    rom_percent_low := rom_percent_low,
    rom_percent_high := rom_percent_high
  )),
  'GEN07 ROM value outside [0,100] — raw text likely OCR/typo (e.g., 395% / 599%); needs hand fix',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_date_clean
WHERE gen07_rom_out_of_range_flag;

COMMENT ON TABLE main.canonical_molecular_genetics_v2 IS
'Molecular genetics master (1,384 rows). Two clean views: ..._platform_clean (mig 39) and ..._date_clean (mig 40) — latter surfaces resolved_test_date_final + resolved_test_date_source (resolved/native/sga/unresolved), gen06_date_unresolved_flag (525 rows), gen07_rom_out_of_range_flag (2 rows). 378/903 date-NULL recovered via SGA fallback. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_39';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_molecular_genetics_v2','table',
   'manuscript_workspace.canonical_molecular_genetics_v2_date_clean',
   'GEN06,GEN07','prompt_39','column_only',DATE '2026-04-23',
   'GEN06: 903 rows had resolved_test_date NULL + test_date_native NULL in canonical; SGA relinked recovered 378, 525 unresolvable. GEN07: 2 rows with rom_percent values >100 (registry 6 — stale). Tiered view exposes final date and source tag; ROM rows queued for hand correction.',
   NULL,
   'Tier order: resolved_test_date → test_date_native → specimen_genomic_assay_v1_relinked.test_date_native (MIN per molecular_episode_id) → unresolved. GEN06=525 / GEN07=2 queued.');
