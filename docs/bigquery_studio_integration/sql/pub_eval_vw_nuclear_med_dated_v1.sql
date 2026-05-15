-- pub_eval.vw_nuclear_med_dated_v1
-- Nuclear-medicine scans with scan dates recovered from the raw scandate string.
-- pub_workspace.nuclear_med_clean.scandate_parsed was NULL for every row (the parser
-- never ran); the raw scandate is a clean MM/DD/YYYY value in 2,218 of 2,220 rows.
-- Linear THY-86 tracks the recommended upstream backfill.
-- Built 2026-05-14, BigQuery Studio Integration Plan.

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_eval.vw_nuclear_med_dated_v1`
OPTIONS (
  description = 'Nuclear medicine scans with resolved scan dates. pub_workspace.nuclear_med_clean.scandate_parsed was NULL for every row (the parser never ran), but the raw scandate string is a clean MM/DD/YYYY value in 2,218 of 2,220 rows. This view parses it. scandate_quality: ok = resolved; blank / unparseable_format / implausible_year (pre-1990) / future_date = needs attention. Recommended fix is to backfill scandate_parsed in the upstream nuclear_med_clean table - tracked in Linear THY-86. Built 2026-05-14, BigQuery Studio Integration Plan, global evaluation layer.'
) AS
SELECT
  research_id,
  scan_index,
  scantype,
  radiotracer,
  indication_text,
  scandate AS scandate_raw,
  CASE
    WHEN scandate IS NULL OR TRIM(scandate) = '' THEN NULL
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate)) IS NULL THEN NULL
    WHEN EXTRACT(YEAR FROM SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate))) < 1990 THEN NULL
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate)) > CURRENT_DATE() THEN NULL
    ELSE SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate))
  END AS scandate_resolved,
  CASE
    WHEN scandate IS NULL OR TRIM(scandate) = '' THEN 'blank'
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate)) IS NULL THEN 'unparseable_format'
    WHEN EXTRACT(YEAR FROM SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate))) < 1990 THEN 'implausible_year'
    WHEN SAFE.PARSE_DATE('%m/%d/%Y', TRIM(scandate)) > CURRENT_DATE() THEN 'future_date'
    ELSE 'ok'
  END AS scandate_quality
FROM `thyroid-canonical-pub-2026.pub_workspace.nuclear_med_clean`;
