-- =============================================================================
-- Migration 74 -- drop subtype + re-derive laterality
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Logan directives:
--           - subtype: "Not needed, if we want to look later we can find this
--             from the reports directly." -> drop the column.
--           - laterality: "Rederive, give me CSV to review for any that have
--             ?" -> re-derive from specimen_location; bulk-apply unambiguous
--             cases; surface ambiguous (?) rows for Logan review.
--
-- Pre-investigation:
--   subtype had 287 distinct values for 2,357 non-null rows -- mostly case
--   variants and synonyms (Colloid Nodule x3 case variants for 765 rows;
--   AUS/FLUS/AUS-FLUS/FLUS-AUS for 165 rows; etc.). Logan elected to drop
--   the column entirely; future analyses can pull from fna_pathology_report.
--
--   For laterality, derived rule (priority order):
--     1. specimen_location contains 'isthmus' -> 'isthmus'
--     2. specimen_location contains left-keyword AND right-keyword -> AMBIG
--     3. left-keyword only -> 'left'
--     4. right-keyword only -> 'right'
--     5. otherwise -> NULL/AMBIG (no laterality info)
--   Left keywords:  '%left%', '%ll fna%', '%ll nodule%', 'll %', '%-ll-%'
--   Right keywords: '%right%', '%rl fna%', '%rl nodule%', 'rl %', '%-rl-%'
--
-- Re-derivation results across 8,054 rows:
--   match                       4,233  (no action)
--   fill_null_with_derived      2,600  (UPDATE applied below)
--   both_unclear                  619  (current NULL + rule NULL; leave as NULL)
--   disagree                      530  (Logan reviews via CSV)
--   current_set_derived_ambig      72  (Logan reviews via CSV)
--
-- Net effect:
--   * 1 column dropped (subtype)
--   * 1 registry row deleted (subtype)
--   * 2,600 rows: laterality filled from rule
--   * Ambiguous 602 rows surfaced in
--     verification_csvs/canonical_fna_events_v1/laterality_review_round1.csv
--     for Logan review (subsequent migration will apply his decisions).
--   * canonical_fna_events_v1 columns: 39 -> 38
--
-- No dependent views referenced subtype (verified pre-execution).
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

ALTER TABLE main.canonical_fna_events_v1 DROP COLUMN subtype;

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_fna_events_v1' AND column_name='subtype';

-- Bulk-fill laterality where current is NULL but derivation rule is unambiguous
WITH derived AS (
  SELECT
    fna_event_id,
    laterality AS current_lat,
    CASE
      WHEN LOWER(COALESCE(specimen_location, '')) LIKE '%isthmus%' THEN 'isthmus'
      WHEN (LOWER(specimen_location) LIKE '%left%'
         OR LOWER(specimen_location) LIKE '%ll fna%'
         OR LOWER(specimen_location) LIKE '%ll nodule%'
         OR LOWER(specimen_location) LIKE 'll %'
         OR LOWER(specimen_location) LIKE '%-ll-%')
       AND (LOWER(specimen_location) LIKE '%right%'
         OR LOWER(specimen_location) LIKE '%rl fna%'
         OR LOWER(specimen_location) LIKE '%rl nodule%'
         OR LOWER(specimen_location) LIKE 'rl %'
         OR LOWER(specimen_location) LIKE '%-rl-%')
        THEN NULL
      WHEN LOWER(specimen_location) LIKE '%left%'
        OR LOWER(specimen_location) LIKE '%ll fna%'
        OR LOWER(specimen_location) LIKE '%ll nodule%'
        OR LOWER(specimen_location) LIKE 'll %'
        OR LOWER(specimen_location) LIKE '%-ll-%'
        THEN 'left'
      WHEN LOWER(specimen_location) LIKE '%right%'
        OR LOWER(specimen_location) LIKE '%rl fna%'
        OR LOWER(specimen_location) LIKE '%rl nodule%'
        OR LOWER(specimen_location) LIKE 'rl %'
        OR LOWER(specimen_location) LIKE '%-rl-%'
        THEN 'right'
      ELSE NULL
    END AS new_lat
  FROM main.canonical_fna_events_v1
)
UPDATE main.canonical_fna_events_v1 db
SET laterality = derived.new_lat
FROM derived
WHERE db.fna_event_id = derived.fna_event_id
  AND derived.current_lat IS NULL
  AND derived.new_lat IS NOT NULL;

-- Recompute table signoff (column count drops from 39 to 38)
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
-- end of migration 74
-- =============================================================================
