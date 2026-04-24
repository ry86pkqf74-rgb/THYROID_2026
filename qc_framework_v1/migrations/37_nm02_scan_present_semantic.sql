-- ============================================================================
-- Migration 37 — NM02: nuclear_med.scan_present semantic clarification
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      NM02 — 100% "non-standard" scan_present values (registry: "not
--                really yes/no — needs inspection first")
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Probe (2026-04-23): main.nuclear_med.scan_present is NOT a yes/no indicator.
-- All 2,220 rows are populated (min 251 chars, max 11,985, avg ~1,911 chars);
-- content is the full radiology report narrative. Sample:
--   '10/12/21 INTERPRETATION EXAM: NM Consult for Eval/Manage w/Chg Tele ...'
--   '7/17/2024 NM thyroid ablation Anatomical Region Laterality Modality ...'
--
-- Resolution is *semantic* rather than row-level. No queue emission needed.
-- Surface two derived fields on the clean view:
--   scan_report_text_length (INT)     — LENGTH(scan_present) for analytics
--   scan_present_bool       (BOOLEAN) — derived: scandate_parsed IS NOT NULL
--     (a row with a parseable scan date is a scan that actually happened;
--      2,218 of 2,220 rows qualify — the 2 NULLs are the mig-36 NULL source
--      and the mig-36 unresolved typo "2/18/216".)
--
-- This migration CREATE OR REPLACEs nuclear_med_clean (from mig 36) to add
-- the two new columns alongside the existing scandate_parsed / parse_source /
-- unresolved_flag.
--
-- Output:
--   manuscript_workspace.nuclear_med_clean (VIEW) — replaces mig 36 version
--     + (from mig 36)  scandate_parsed, scandate_parse_source, nm01_scandate_unresolved_flag
--     + (new)          scan_report_text_length
--     + (new)          scan_present_bool
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.nuclear_med_clean AS
SELECT
  n.*,
  -- NM01 (inherited from mig 36)
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
  -- NM02 (new in mig 37): scan_present is report-narrative text, not a bool.
  CAST(LENGTH(n.scan_present) AS INTEGER) AS scan_report_text_length,
  (CASE
    WHEN n.scandate IS NULL THEN NULL
    WHEN TRY_CAST(n.scandate AS DATE) IS NOT NULL THEN TRY_CAST(n.scandate AS DATE)
    WHEN REGEXP_MATCHES(n.scandate, '^\d{1,2}/\d{1,2}/\d{2}$')
      THEN CAST(TRY_STRPTIME(n.scandate, '%m/%d/%y') AS DATE)
    ELSE NULL
  END) IS NOT NULL AS scan_present_bool
FROM main.nuclear_med n;

-- NM02: no queue emission. Semantic clarification only.
DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='NM02';

COMMENT ON TABLE main.nuclear_med IS
'Nuclear med row-per-exam (2,220 rows). scandate VARCHAR with mixed formats — use manuscript_workspace.nuclear_med_clean.scandate_parsed. scan_present column is MISLEADING (avg ~1,911 / max 11,985 char report narrative, not yes/no) — use scan_present_bool (derived from scandate_parsed IS NOT NULL) and scan_report_text_length instead. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_36';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.nuclear_med.scan_present','column',
   'manuscript_workspace.nuclear_med_clean.scan_present_bool',
   'NM02','prompt_36','column_only',DATE '2026-04-23',
   'Column mislabeled — registry flagged as "not really yes/no". Probe confirmed all 2,220 rows contain full report narrative text (min 251 / max 11,985 / avg ~1,911 chars), not boolean values. Semantic rename at view layer; raw column preserved.',
   NULL,
   'scan_present_bool = (scandate_parsed IS NOT NULL); 2,218 TRUE + 1 NULL (null source) + 1 FALSE (unresolved typo). scan_report_text_length exposes narrative length for analytics. No queue rows — fix is purely semantic.');
