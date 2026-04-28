-- =============================================================================
-- Migration 67 -- FNA fna_date_raw cleanup round 2 (Logan-curated)
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Source: verification_csvs/canonical_fna_events_v1/fna_date_raw__cleanup_round2.csv
--         (Logan-filled; uploaded as fna_date_raw__cleanup_round2.xlsx)
-- Scope:  main.canonical_fna_events_v1.fna_date_raw -- the 53 non-parseable
--         rows surfaced after mig_68 global date normalization left them as-is.
--
-- Net effect:
--   * 46 UPDATEs setting fna_date_raw to a Logan-supplied MM/DD/YYYY value
--   * 7 DELETEs of rows tagged "All Null n FNA performed" in his notes:
--       research_ids 1422, 2113, 2210, 2212, 2610, 8803, 9201 (all fna_index=1)
--
-- Row count before: 8,114 ; after: 8,107
-- fna_date_raw distribution after: 8,045 MM/DD/YYYY + 62 NULL + 0 anomalies.
--
-- Outstanding clarification (flagged but not blocking):
--   research_id 7332 idx 2 carries Logan's note "date for both FNA 1 and FNA 2
--   should be 5/7/2019", but his idx-1 decision cell holds 04/16/2019. Both
--   decisions were applied as written. Resolve in a follow-up if needed.
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_fna_events_v1
SET fna_date_raw = CASE
  WHEN research_id = '6171'  AND fna_index = 2 THEN '07/14/2017'
  WHEN research_id = '5594'  AND fna_index = 2 THEN '05/21/2014'
  WHEN research_id = '6103'  AND fna_index = 2 THEN '05/22/2017'
  WHEN research_id = '7332'  AND fna_index = 2 THEN '05/07/2019'
  WHEN research_id = '8745'  AND fna_index = 2 THEN '03/01/2021'
  WHEN research_id = '9003'  AND fna_index = 1 THEN '02/22/2018'
  WHEN research_id = '9852'  AND fna_index = 2 THEN '12/13/2022'
  WHEN research_id = '1121'  AND fna_index = 1 THEN '10/06/2006'
  WHEN research_id = '1614'  AND fna_index = 1 THEN '12/04/2011'
  WHEN research_id = '1614'  AND fna_index = 2 THEN '12/04/2011'
  WHEN research_id = '1614'  AND fna_index = 3 THEN '12/04/2011'
  WHEN research_id = '1959'  AND fna_index = 1 THEN '09/01/2010'
  WHEN research_id = '2799'  AND fna_index = 1 THEN '08/01/2006'
  WHEN research_id = '3221'  AND fna_index = 1 THEN '10/11/2009'
  WHEN research_id = '4064'  AND fna_index = 1 THEN '06/25/2013'
  WHEN research_id = '4064'  AND fna_index = 2 THEN '06/25/2013'
  WHEN research_id = '4064'  AND fna_index = 3 THEN '06/25/2013'
  WHEN research_id = '11032' AND fna_index = 1 THEN '01/08/2024'
  WHEN research_id = '10377' AND fna_index = 1 THEN '07/05/2022'
  WHEN research_id = '10377' AND fna_index = 2 THEN '07/05/2022'
  WHEN research_id = '10756' AND fna_index = 1 THEN '01/02/2023'
  WHEN research_id = '1169'  AND fna_index = 1 THEN '08/22/2006'
  WHEN research_id = '1817'  AND fna_index = 1 THEN '01/20/2010'
  WHEN research_id = '1980'  AND fna_index = 2 THEN '04/05/2012'
  WHEN research_id = '2004'  AND fna_index = 4 THEN '10/24/2012'
  WHEN research_id = '2221'  AND fna_index = 1 THEN '10/04/2019'
  WHEN research_id = '234'   AND fna_index = 3 THEN '09/23/2005'
  WHEN research_id = '2723'  AND fna_index = 2 THEN '09/14/2015'
  WHEN research_id = '3061'  AND fna_index = 1 THEN '09/29/2008'
  WHEN research_id = '3593'  AND fna_index = 1 THEN '09/07/2011'
  WHEN research_id = '3730'  AND fna_index = 3 THEN '03/27/2012'
  WHEN research_id = '3891'  AND fna_index = 2 THEN '03/30/2018'
  WHEN research_id = '4418'  AND fna_index = 1 THEN '05/30/2014'
  WHEN research_id = '4418'  AND fna_index = 2 THEN '05/30/2014'
  WHEN research_id = '4460'  AND fna_index = 1 THEN '09/18/2015'
  WHEN research_id = '4709'  AND fna_index = 1 THEN '11/17/2014'
  WHEN research_id = '4952'  AND fna_index = 2 THEN '10/06/2011'
  WHEN research_id = '5536'  AND fna_index = 2 THEN '01/29/2016'
  WHEN research_id = '5777'  AND fna_index = 2 THEN '03/08/2016'
  WHEN research_id = '5804'  AND fna_index = 2 THEN '08/10/2016'
  WHEN research_id = '5907'  AND fna_index = 1 THEN '03/14/2017'
  WHEN research_id = '5937'  AND fna_index = 2 THEN '08/08/2016'
  WHEN research_id = '6077'  AND fna_index = 3 THEN '03/29/2016'
  WHEN research_id = '6865'  AND fna_index = 4 THEN '05/01/2012'
  WHEN research_id = '7332'  AND fna_index = 1 THEN '04/16/2019'
  WHEN research_id = '9049'  AND fna_index = 1 THEN '07/13/2021'
END
WHERE (research_id, fna_index) IN (
  ('6171', 2),  ('5594', 2),  ('6103', 2),  ('7332', 2),  ('8745', 2),
  ('9003', 1),  ('9852', 2),  ('1121', 1),  ('1614', 1),  ('1614', 2),
  ('1614', 3),  ('1959', 1),  ('2799', 1),  ('3221', 1),  ('4064', 1),
  ('4064', 2),  ('4064', 3),  ('11032', 1), ('10377', 1), ('10377', 2),
  ('10756', 1), ('1169', 1),  ('1817', 1),  ('1980', 2),  ('2004', 4),
  ('2221', 1),  ('234', 3),   ('2723', 2),  ('3061', 1),  ('3593', 1),
  ('3730', 3),  ('3891', 2),  ('4418', 1),  ('4418', 2),  ('4460', 1),
  ('4709', 1),  ('4952', 2),  ('5536', 2),  ('5777', 2),  ('5804', 2),
  ('5907', 1),  ('5937', 2),  ('6077', 3),  ('6865', 4),  ('7332', 1),
  ('9049', 1)
);

DELETE FROM main.canonical_fna_events_v1
WHERE (research_id, fna_index) IN (
  ('1422', 1), ('2113', 1), ('2210', 1),
  ('2212', 1), ('2610', 1), ('8803', 1), ('9201', 1)
);

-- =============================================================================
-- end of migration 67
-- =============================================================================
