-- ============================================================================
-- Migration 38 — NM03 + NM04: nuclear_med narrative & scantype/radiotracer
--                              coverage flags
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   NM03 — 522 rows with BOTH findings_text and impression_text empty
--          (both_empty = 522 / findings_empty = 736 / impression_empty = 691)
--   NM04 — 64 rows NULL scantype + 110 rows NULL radiotracer
--          (overlapping: "Unspecified Thyroid Study" with NULL radiotracer = 66
--           is the dominant pattern — scantype present but uninformative)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- scan_present (the ~1,911-char narrative from mig 37) contains the source
-- material from which scantype + radiotracer could be LLM-re-extracted for
-- the missing rows. That is a Tier-2 LLM pass (parallel to GEN03 flagged in
-- prior batches). For THIS migration we:
--   1. Surface boolean flags on the clean view.
--   2. Queue NM03 (522) and NM04 (64 scantype-null — the registry issue).
--   3. Log radiotracer-null as advisory (110 rows) — registry didn't number it
--      but it's visible from the probe.
--
-- Tier-2 LLM pass is deferred to LLM_TODO.md append (Logan queues separately).
--
-- Extends manuscript_workspace.nuclear_med_clean (mig 36 + 37) — we replace
-- the view again to add three new boolean flags on top of the scandate /
-- scan_present_bool stack already there.
--
-- Output columns added this migration:
--   nm03_narrative_empty_flag         BOOLEAN (findings AND impression empty)
--   nm04_scantype_null_flag           BOOLEAN
--   nm04_radiotracer_null_flag        BOOLEAN  (advisory)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.nuclear_med_clean AS
SELECT
  n.*,
  -- NM01 / mig 36
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
    AS nm01_scandate_unresolved_flag,
  -- NM02 / mig 37
  CAST(LENGTH(n.scan_present) AS INTEGER) AS scan_report_text_length,
  (CASE
    WHEN n.scandate IS NULL THEN NULL
    WHEN TRY_CAST(n.scandate AS DATE) IS NOT NULL THEN TRY_CAST(n.scandate AS DATE)
    WHEN REGEXP_MATCHES(n.scandate, '^\d{1,2}/\d{1,2}/\d{2}$')
      THEN CAST(TRY_STRPTIME(n.scandate, '%m/%d/%y') AS DATE)
    ELSE NULL
  END) IS NOT NULL AS scan_present_bool,
  -- NM03 (new) — narrative absent in both findings + impression
  ((n.findings_text IS NULL OR LENGTH(TRIM(n.findings_text))=0)
   AND (n.impression_text IS NULL OR LENGTH(TRIM(n.impression_text))=0))
    AS nm03_narrative_empty_flag,
  -- NM04 (new)
  (n.scantype IS NULL OR LENGTH(TRIM(n.scantype))=0) AS nm04_scantype_null_flag,
  (n.radiotracer IS NULL OR LENGTH(TRIM(n.radiotracer))=0) AS nm04_radiotracer_null_flag
FROM main.nuclear_med n;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('NM03','NM04');

-- NM03: both findings and impression empty (522 rows) — queued as candidates
-- for LLM re-parse from scan_present narrative (Tier-2, deferred)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'NM03',
  TRY_CAST(research_id AS INTEGER),
  'main.nuclear_med',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(scandate,''), '|',
         COALESCE(CAST(scan_index AS VARCHAR),'')),
  TO_JSON(struct_pack(
    scandate := scandate,
    scantype := scantype,
    radiotracer := radiotracer,
    scan_report_text_length := scan_report_text_length
  )),
  'NM03 findings_text + impression_text both empty — LLM re-parse from scan_present narrative is a candidate Tier-2 pass',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.nuclear_med_clean
WHERE nm03_narrative_empty_flag;

-- NM04: NULL scantype (64 rows) — the registry-numbered fact. radiotracer-null
-- is advisory (flagged on view, not queued) pending LLM pass.
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'NM04',
  TRY_CAST(research_id AS INTEGER),
  'main.nuclear_med',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(scandate,''), '|',
         COALESCE(CAST(scan_index AS VARCHAR),'')),
  TO_JSON(struct_pack(
    scandate := scandate,
    scantype := scantype,
    radiotracer := radiotracer,
    scan_report_text_length := scan_report_text_length
  )),
  'NM04 scantype NULL — LLM re-parse from scan_present narrative candidate',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.nuclear_med_clean
WHERE nm04_scantype_null_flag;

COMMENT ON TABLE main.nuclear_med IS
'Nuclear med row-per-exam (2,220 rows). Clean view manuscript_workspace.nuclear_med_clean exposes: scandate_parsed + parse_source + nm01_scandate_unresolved_flag (mig 36); scan_present_bool + scan_report_text_length (mig 37 — scan_present is narrative text, not yes/no); nm03_narrative_empty_flag=522 (findings+impression both empty), nm04_scantype_null_flag=64, nm04_radiotracer_null_flag=110 (advisory). LLM re-parse from scan_present narrative is the recovery path for NM03/NM04 and tracked in LLM_TODO.md. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_37';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.nuclear_med','table',
   'manuscript_workspace.nuclear_med_clean',
   'NM03,NM04','prompt_37','column_only',DATE '2026-04-23',
   'NM03: 522 rows have both findings_text and impression_text empty. NM04: 64 scantype-NULL + 110 radiotracer-NULL (radiotracer advisory — registry numbered only scantype). Recovery path is LLM re-parse from scan_present narrative (see LLM_TODO.md).',
   NULL,
   'Flags on nuclear_med_clean. NM03 (522) + NM04 (64) queued as LLM-pass candidates. Radiotracer-null (110) flagged on view but not queued (advisory).');
