-- ============================================================================
-- Migration 42 — FNA02: fna_date_raw present but fna_date_resolved NULL
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      FNA02 — 1,516 of 8,119 canonical_fna_events_v1 rows have raw
--                date text present but fna_date_resolved NULL.
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   Pre-state FNA table:
--     8,119 total rows; 6,537 with fna_date_resolved; 1,582 NULL-resolved;
--     of those 1,582 NULL, 1,516 have fna_date_raw present and 67 are fully
--     NULL-source.
--
--   Recovery chain over the 1,516 (raw trimmed + tab/CR/LF → space),
--   regex-guarded to defeat DuckDB's lenient %Y (which accepts 2-digit yrs
--   as literal sub-100 years):
--     Tier A — raw_clean ~ '\d{4}$'  + STRPTIME('%m/%d/%Y'):         1,431
--     Tier B — raw_clean ~ '\d{2}$'  + STRPTIME('%m/%d/%y'):            33
--     Tier C — raw_extracted ~ '\d{4}$' + STRPTIME('%m/%d/%Y'):         16
--     Tier D — raw_extracted ~ '\d{2}$' + STRPTIME('%m/%d/%y'):          1
--     Tier E — unresolved (n/s + narrative/typo):                      35
--
-- Design:
--   View layer only — canonical table untouched. Expose:
--     fna_date_resolved_final (DATE)
--     fna_date_resolved_source ∈ {existing, reparsed_mdy_4d, reparsed_mdy_2d,
--                                  reparsed_regex_extract, unresolved, null_raw}
--     fna02_date_unresolved_flag (BOOLEAN)  — raw present, all tiers failed.
--
--   Queue: 34 unresolved rows to qc_manual_review_queue_v1 under FNA02.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_fna_events_v1_date_clean AS
WITH cleaned AS (
  SELECT
    e.*,
    TRIM(REPLACE(REPLACE(REPLACE(COALESCE(e.fna_date_raw,''),
                                 CHR(9), ' '), CHR(10), ' '), CHR(13), ' ')) AS raw_clean,
    REGEXP_EXTRACT(REPLACE(REPLACE(REPLACE(COALESCE(e.fna_date_raw,''),
                                 CHR(9), ' '), CHR(10), ' '), CHR(13), ' '),
                   '\d{1,2}/\d{1,2}/\d{2,4}') AS raw_extracted
  FROM main.canonical_fna_events_v1 e
)
-- NB: DuckDB's '%Y' token is lenient and will happily parse '5/22/14' as year 14.
-- Guard tier A with a regex that requires a 4-digit trailing year.
SELECT
  c.*,
  CASE
    WHEN c.fna_date_resolved IS NOT NULL THEN c.fna_date_resolved
    WHEN LENGTH(c.raw_clean)=0 THEN NULL
    WHEN c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{4}$'
         AND TRY_STRPTIME(c.raw_clean, '%m/%d/%Y') IS NOT NULL
      THEN CAST(TRY_STRPTIME(c.raw_clean, '%m/%d/%Y') AS DATE)
    WHEN c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{2}$'
         AND TRY_STRPTIME(c.raw_clean, '%m/%d/%y') IS NOT NULL
      THEN CAST(TRY_STRPTIME(c.raw_clean, '%m/%d/%y') AS DATE)
    WHEN c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{4}$'
         AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%Y') IS NOT NULL
      THEN CAST(TRY_STRPTIME(c.raw_extracted, '%m/%d/%Y') AS DATE)
    WHEN c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{2}$'
         AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%y') IS NOT NULL
      THEN CAST(TRY_STRPTIME(c.raw_extracted, '%m/%d/%y') AS DATE)
    ELSE NULL
  END AS fna_date_resolved_final,
  CASE
    WHEN c.fna_date_resolved IS NOT NULL THEN 'existing'
    WHEN LENGTH(c.raw_clean)=0 THEN 'null_raw'
    WHEN c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{4}$'
         AND TRY_STRPTIME(c.raw_clean, '%m/%d/%Y') IS NOT NULL THEN 'reparsed_mdy_4d'
    WHEN c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{2}$'
         AND TRY_STRPTIME(c.raw_clean, '%m/%d/%y') IS NOT NULL THEN 'reparsed_mdy_2d'
    WHEN c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{4}$'
         AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%Y') IS NOT NULL THEN 'reparsed_regex_extract_4d'
    WHEN c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{2}$'
         AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%y') IS NOT NULL THEN 'reparsed_regex_extract_2d'
    ELSE 'unresolved'
  END AS fna_date_resolved_source,
  (c.fna_date_resolved IS NULL
   AND LENGTH(c.raw_clean) > 0
   AND NOT (c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{4}$' AND TRY_STRPTIME(c.raw_clean, '%m/%d/%Y') IS NOT NULL)
   AND NOT (c.raw_clean ~ '\d{1,2}/\d{1,2}/\d{2}$' AND TRY_STRPTIME(c.raw_clean, '%m/%d/%y') IS NOT NULL)
   AND NOT (c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{4}$' AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%Y') IS NOT NULL)
   AND NOT (c.raw_extracted ~ '\d{1,2}/\d{1,2}/\d{2}$' AND TRY_STRPTIME(c.raw_extracted, '%m/%d/%y') IS NOT NULL)
  ) AS fna02_date_unresolved_flag
FROM cleaned c;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA02';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'FNA02',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_fna_events_v1',
  CAST(fna_event_id AS VARCHAR),
  TO_JSON(struct_pack(
    fna_date_raw := fna_date_raw,
    fna_date_resolved := fna_date_resolved,
    fna_date_resolved_source := fna_date_resolved_source,
    specimen_location := specimen_location,
    laterality := laterality,
    bethesda_final_num := bethesda_final_num
  )),
  'FNA02 fna_date_raw present but unparseable after 3-tier regex/strptime chain — narrative / typo / OSH-not-specified',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_fna_events_v1_date_clean
WHERE fna02_date_unresolved_flag;

COMMENT ON TABLE main.canonical_fna_events_v1 IS
'FNA event table (8,119 rows). Clean view manuscript_workspace.canonical_fna_events_v1_date_clean surfaces fna_date_resolved_final + fna_date_resolved_source via 4-tier regex-guarded re-parse (mdy_4d/mdy_2d/regex_extract_4d/regex_extract_2d). 1,481 of 1,516 FNA02 rows recovered; 35 residual queued. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_41';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_fna_events_v1.fna_date_resolved','column',
   'manuscript_workspace.canonical_fna_events_v1_date_clean.fna_date_resolved_final',
   'FNA02','prompt_41','column_only',DATE '2026-04-24',
   'FNA02: 1,516 rows had fna_date_raw present but fna_date_resolved NULL. Regex-guarded 4-tier re-parse recovers 1,481 (1,431 %m/%d/%Y + 33 %m/%d/%y + 16 regex-extract-4d + 1 regex-extract-2d); 35 residual (n/s + narrative/typo) queued.',
   NULL,
   'View also carries fna_date_resolved_source ∈ {existing, reparsed_mdy_4d, reparsed_mdy_2d, reparsed_regex_extract, unresolved, null_raw}. Downstream temporal joins should use the _final column; the original fna_date_resolved is left in place for audit.');
