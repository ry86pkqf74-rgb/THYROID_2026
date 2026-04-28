-- =============================================================================
-- Migration 69b -- fix mig_68's broken 20YY rule
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   final-cleaning of date columns; corrects mig_68's silent failure
--         to apply the 2-digit-year -> 20YY rule for many rows.
--
-- Bug: mig_68 used SQL like
--   try_strptime("col", '%m/%d/%y') + INTERVAL (2000 - DATE_PART('year', dt)) YEAR
-- which DuckDB doesn't accept as a dynamic INTERVAL argument; the offset never
-- applied, leaving year as parsed (year 11 stored as "0011" instead of "2011").
--
-- Affected (rows with format MM/DD/00YY):
--   nuclear_med.scandate                       2,218 rows
--   canonical_fna_events_v1.fna_date_raw          71 rows
--   thyroid_sizes.surg_date                        1 row
--
-- Plus 5 ambiguous typo cases in canonical_fna_events_v1.fna_date_raw:
--   5866/1, 5866/2: '12/22/0106' -> '12/22/2006'
--   9506/1:        '04/18/0202' -> '04/18/2002'
--   3867/2:        '04/06/1017' -> '04/06/2017'
--   11915/3:       '11/17/2029' -> '11/17/2019'
--
-- Net effect:
--   2,290 rows fixed via bulk substring replace + 5 explicit overrides.
--   Pre-state count of fna_date_raw rows w/ year < 1995: 75 ; post-state: 0.
--
-- Note: nuclear_med has 1 additional anomalous row (year between 100-1900) not
-- yet handled. To investigate when nuclear_med is verified later in the queue.
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_fna_events_v1
SET fna_date_raw = SUBSTR(fna_date_raw, 1, 6) || '20' || SUBSTR(fna_date_raw, 9, 2)
WHERE fna_date_raw ~ '^\d{2}/\d{2}/00\d{2}$';

UPDATE main.nuclear_med
SET scandate = SUBSTR(scandate, 1, 6) || '20' || SUBSTR(scandate, 9, 2)
WHERE scandate ~ '^\d{2}/\d{2}/00\d{2}$';

UPDATE main.thyroid_sizes
SET surg_date = SUBSTR(surg_date, 1, 6) || '20' || SUBSTR(surg_date, 9, 2)
WHERE surg_date ~ '^\d{2}/\d{2}/00\d{2}$';

UPDATE main.canonical_fna_events_v1
SET fna_date_raw = CASE
  WHEN research_id='5866'  AND fna_index=1 THEN '12/22/2006'
  WHEN research_id='5866'  AND fna_index=2 THEN '12/22/2006'
  WHEN research_id='9506'  AND fna_index=1 THEN '04/18/2002'
  WHEN research_id='3867'  AND fna_index=2 THEN '04/06/2017'
  WHEN research_id='11915' AND fna_index=3 THEN '11/17/2019'
END
WHERE (research_id, fna_index) IN (
  ('5866', 1), ('5866', 2), ('9506', 1), ('3867', 2), ('11915', 3)
);

-- =============================================================================
-- end of migration 69b
-- =============================================================================
