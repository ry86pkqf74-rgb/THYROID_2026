-- =============================================================================
-- Migration 69 -- FNA round 3 cleanup + drop specimen_site_raw
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   final-cleaning of canonical_fna_events_v1 (62 NULL-date rows
--         resolved + corrupted mirror column dropped)
-- Source: verification_csvs/canonical_fna_events_v1/fna_null_date_review_round3.csv
--         (Logan-filled, uploaded 2026-04-27)
--         + fna_null_date_phantoms_round3.txt (21 unconditional phantom deletes)
--
-- Step 1: 9 date UPDATEs from round-3 review
--   research_id, fna_index -> new fna_date_raw value (MM/DD/YYYY)
--     11345/1, 11345/2 -> 12/09/2023  (Logan filled)
--     1629/1           -> 05/04/2013  (Logan filled)
--     1517/4           -> 09/11/2023  (from Logan's concern note in column O;
--                                       date alignment confirmed via direct
--                                       DB-vs-source comparison)
--     3916/2           -> 09/17/2019  (Logan filled)
--     528/1            -> 10/16/2004  (Logan filled)
--     5310/1, 5310/2   -> 02/17/2016  (Logan filled)
--     9871/2           -> 08/01/2023  (Logan filled)
--
-- Step 2: 53 DELETEs
--   32 from round-3 review CSV (notes "Delete, no Nth FNA performed",
--   "Blank, no FNA performed", or empty rows that turned out to be phantoms):
--     idx=2 batch (28 rows): 10042, 10081, 10414, 10498, 10552, 10592, 10807,
--       10818, 10897, 11017, 11148, 11266, 11277, 11483, 11484, 11581, 11670,
--       11708, 11713, 11716, 11720, 11820, 9861
--     idx=4 batch (8 rows): 11254, 11739, 11895, 11896, 11902, 11958, 3772
--     idx=1: 8330
--     idx=7: 6529 ('NO FNA 7, I skipped a this on acciden')
--   21 phantoms (NULL date + no FNA content at all):
--     2275, 2278, 2305, 2355, 2360, 2398, 2401, 2406, 2429, 2455, 2483, 2500,
--     2575, 2578, 2601, 2605, 2612, 2613, 2662, 2676, 92  (all fna_index=1)
--
-- Step 3: DROP COLUMN specimen_site_raw
--   Per mig_64's notes specimen_site_raw was supposed to be a mirror of
--   specimen_location (both pulled from the same Excel "Specimen received"
--   cell). Cross-check against manuscript_workspace.fna_source_long_v1_step_b
--   showed:
--     6,560 rows: mirror correct
--     1,546 rows: specimen_location correct, specimen_site_raw drifted
--     1 row:     specimen_site_raw correct, specimen_location drifted
--   specimen_location is the authoritative column. Dropping the redundant
--   buggy mirror per Logan directive ("Drop column entirely").
--
--   No views in main or manuscript_workspace reference specimen_site_raw.
--   Registry row also removed and table_signoff counts recomputed.
--
-- Net effect:
--   Pre-state:  8,107 rows / 40 cols / 62 NULL fna_date_raw
--   Post-state: 8,054 rows / 39 cols / 0 NULL fna_date_raw / 100% MM/DD/YYYY
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

-- ---- Step 1: date UPDATEs ----
UPDATE main.canonical_fna_events_v1
SET fna_date_raw = CASE
  WHEN research_id = '11345' AND fna_index = 1 THEN '12/09/2023'
  WHEN research_id = '11345' AND fna_index = 2 THEN '12/09/2023'
  WHEN research_id = '1629'  AND fna_index = 1 THEN '05/04/2013'
  WHEN research_id = '1517'  AND fna_index = 4 THEN '09/11/2023'
  WHEN research_id = '3916'  AND fna_index = 2 THEN '09/17/2019'
  WHEN research_id = '528'   AND fna_index = 1 THEN '10/16/2004'
  WHEN research_id = '5310'  AND fna_index = 1 THEN '02/17/2016'
  WHEN research_id = '5310'  AND fna_index = 2 THEN '02/17/2016'
  WHEN research_id = '9871'  AND fna_index = 2 THEN '08/01/2023'
END
WHERE (research_id, fna_index) IN (
  ('11345', 1), ('11345', 2), ('1629', 1), ('1517', 4),
  ('3916', 2),  ('528', 1),   ('5310', 1), ('5310', 2),
  ('9871', 2)
);

-- ---- Step 2: 53 DELETEs ----
DELETE FROM main.canonical_fna_events_v1
WHERE (research_id, fna_index) IN (
  -- Round-3 review (32 rows):
  ('10042', 2), ('10081', 2), ('10414', 2), ('10498', 2), ('10552', 2),
  ('10592', 2), ('10807', 2), ('10818', 2), ('10897', 2), ('11017', 2),
  ('11148', 2), ('11254', 4), ('11266', 2), ('11277', 2), ('11483', 2),
  ('11484', 2), ('11581', 2), ('11670', 2), ('11708', 2), ('11713', 2),
  ('11716', 2), ('11720', 2), ('11739', 4), ('11820', 2), ('11895', 4),
  ('11896', 4), ('11902', 4), ('11958', 4),
  ('9861', 2),  ('8330', 1),  ('3772', 4),  ('6529', 7),
  -- 21 phantoms:
  ('2275', 1),  ('2278', 1),  ('2305', 1),  ('2355', 1),  ('2360', 1),
  ('2398', 1),  ('2401', 1),  ('2406', 1),  ('2429', 1),  ('2455', 1),
  ('2483', 1),  ('2500', 1),  ('2575', 1),  ('2578', 1),  ('2601', 1),
  ('2605', 1),  ('2612', 1),  ('2613', 1),  ('2662', 1),  ('2676', 1),
  ('92', 1)
);

-- ---- Step 3: DROP COLUMN specimen_site_raw + registry housekeeping ----
ALTER TABLE main.canonical_fna_events_v1 DROP COLUMN specimen_site_raw;

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main'
  AND table_name='canonical_fna_events_v1'
  AND column_name='specimen_site_raw';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 69
-- =============================================================================
