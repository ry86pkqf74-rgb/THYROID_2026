-- ============================================================================
-- Migration 36 — NM01: nuclear_med.scandate unparseable
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      NM01 — 1,364 of 2,220 rows have un-ISO-parseable scandate
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.nuclear_med.scandate distribution (probed 2026-04-23):
--   ISO-parseable YYYY-MM-DD: 855 rows   (TRY_CAST succeeds)
--   MM/DD/YY (2-digit year):  1,363 rows (full coverage on unparseable)
--   NULL:                     1 row
--   Other patterns:           0 rows
--
-- Two-format tiered parser suffices. No LLM pass needed (original registry
-- spec called for one; data is cleaner than registry feared).
--
-- DuckDB %y parses 2-digit years 00-68 as 2000-2068, 69-99 as 1969-1999
-- (standard strptime behavior). Nuclear med data runs 2016-2025 so this
-- mapping is correct for all observed values.
--
-- Output:
--   manuscript_workspace.nuclear_med_clean (VIEW)
--     + scandate_parsed        DATE    (resolved date)
--     + scandate_parse_source  VARCHAR ∈ {iso, mm_dd_yy, unresolved, null_source}
--     + nm01_scandate_unresolved_flag BOOLEAN
--
-- `main.nuclear_med.scandate` is NOT renamed — preserving raw. Downstream
-- code should migrate to nuclear_med_clean.scandate_parsed.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.nuclear_med_clean AS
SELECT
  n.*,
  CASE
    WHEN n.scandate IS NULL THEN NULL
    WHEN TRY_CAST(n.scandate AS DATE) IS NOT NULL THEN TRY_CAST(n.scandate AS DATE)
    WHEN REGEXP_MATCHES(n.scandate, '^\d{1,2}/\d{1,2}/\d{2}$')
      THEN CAST(TRY_STRPTIME(n.scandate, '%m/%d/%y') AS DATE)
    ELSE NULL
  END AS scandate_parsed,
  CASE
    WHEN n.scandate IS NULL THEN 'null_source'
    WHEN TRY_CAST(n.scandate AS DATE) IS NOT NULL THEN 'iso'
    WHEN REGEXP_MATCHES(n.scandate, '^\d{1,2}/\d{1,2}/\d{2}$')
         AND TRY_STRPTIME(n.scandate, '%m/%d/%y') IS NOT NULL THEN 'mm_dd_yy'
    ELSE 'unresolved'
  END AS scandate_parse_source,
  (n.scandate IS NOT NULL
   AND TRY_CAST(n.scandate AS DATE) IS NULL
   AND NOT (REGEXP_MATCHES(n.scandate, '^\d{1,2}/\d{1,2}/\d{2}$')
            AND TRY_STRPTIME(n.scandate, '%m/%d/%y') IS NOT NULL))
    AS nm01_scandate_unresolved_flag
FROM main.nuclear_med n;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='NM01';

-- NM01: queue rows that remain unparseable after the tiered parser
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'NM01',
  TRY_CAST(research_id AS INTEGER),
  'main.nuclear_med',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(scandate,''), '|',
         COALESCE(scantype,'')),
  TO_JSON(struct_pack(
    scandate_raw := scandate,
    scantype := scantype,
    radiotracer := radiotracer
  )),
  'NM01 scandate still unparseable after ISO + MM/DD/YY tiered parser — new format needs handler',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.nuclear_med_clean
WHERE nm01_scandate_unresolved_flag;

COMMENT ON TABLE main.nuclear_med IS
'Nuclear med row-per-exam (2,220 rows). scandate is VARCHAR with mixed formats; use manuscript_workspace.nuclear_med_clean.scandate_parsed for analytic joins. 2-format tiered parser: 855 ISO + 1,363 MM/DD/YY + 1 NULL + 0 unresolved. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_35';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.nuclear_med.scandate','column',
   'manuscript_workspace.nuclear_med_clean.scandate_parsed',
   'NM01','prompt_35','column_only',DATE '2026-04-23',
   '1,364 of 2,220 rows had MM/DD/YY 2-digit-year format that TRY_CAST(AS DATE) cannot parse. Tiered parser resolves 100% of non-null rows; 0 rows remain unparseable.',
   NULL,
   'scandate_parsed on nuclear_med_clean. scandate_parse_source ∈ {iso, mm_dd_yy, null_source, unresolved}. 0 rows currently queued under NM01 — all non-null dates resolved.');
