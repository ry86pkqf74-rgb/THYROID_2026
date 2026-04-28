-- =============================================================================
-- Migration 75 -- add fna_site column to canonical_fna_events_v1
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Logan directive: "FNA site - left lobe nodule, or left lobe, etc.
--         Lymph nodes and level. etc" -- add a structured anatomic-site
--         column derived from specimen_location + fna_pathology_report +
--         laterality.
--
-- New column: main.canonical_fna_events_v1.fna_site VARCHAR
--
-- Vocabulary:
--   thyroid_left_lobe                    3,293
--   thyroid_right_lobe                   3,225
--   thyroid_unspecified                    374
--   thyroid_isthmus                        350
--   thyroid_left_lobe_isthmus               41
--   thyroid_bilateral                       37
--   thyroid_right_lobe_isthmus              35
--   lymph_node_right_neck                  180
--   lymph_node_left_neck                   162
--   lymph_node_unspecified_neck             62
--   lymph_node_<left|right|unspecified|bilateral>_<level_1..7|paratracheal|
--     supraclavicular|submandibular|mediastinal|central|neck>  (~430 total)
--   parathyroid_left/right/unspecified      42 total
--   unclear                                 13  (Logan reviews via CSV)
--   NULL                                     4  (no specimen/path data; possible phantoms)
--
-- Total: 8,054 rows.
--
-- Derivation rule (priority order):
--   1. Lymph node: any of (lymph, neck node, neck mass, neck ln, cervical ln,
--      cervical lymph, ' ln ', 'ln ', 'ln fna', neck+level keyword) in
--      specimen_location OR fna_pathology_report.
--      Sub-region: paratracheal | supraclavicular | submandibular | mediastinal |
--                  central | level_<1-7> (Arabic+Roman normalized) | neck (default)
--      Laterality: left/right/bilateral/unspecified
--   2. Parathyroid: 'parathyroid' in specimen or path report.
--   3. Bilateral: explicit 'bilateral' OR (left-keyword AND right-keyword detected).
--   4. Thyroid + isthmus + laterality combinations.
--   5. Thyroid laterality only.
--   6. Thyroid (unspecified) if 'thyroid'/'thyroud' (typo) appears anywhere.
--   7. Fall back to laterality column when specimen/path lack keywords but
--      laterality was previously adjudicated.
--   8. Otherwise: 'unclear' (Logan reviews) or NULL (no data).
--
-- Whitespace-tolerant: applies REGEXP_REPLACE for leading/trailing whitespace
-- (incl. \\n, \\t, \\r) so patterns match strings like "\\nLL FNA\\n".
--
-- Roman numeral level normalization: level i/ii/iii/iv/v/vi/vii -> 1/2/3/4/5/6/7.
--
-- Net effect:
--   * 1 ALTER TABLE ADD COLUMN
--   * 1 UPDATE filling all 8,054 rows
--   * 1 INSERT into canonical_column_verification_registry_v1 (fna_site)
--   * canonical_fna_events_v1 columns: 38 -> 39
--   * fna_site marked verification_status='not_started' pending Logan review
--     of 17 rows (13 unclear + 4 truly-empty) in
--     verification_csvs/canonical_fna_events_v1/fna_site_unclear_review.csv
--
-- Executed via Cowork query_rw 2026-04-27. The full UPDATE rule is preserved
-- in this file for replay/audit.
-- =============================================================================

ALTER TABLE main.canonical_fna_events_v1 ADD COLUMN fna_site VARCHAR;

WITH derived AS (
  SELECT
    fna_event_id, laterality,
    LOWER(REGEXP_REPLACE(COALESCE(specimen_location, ''),     '(^[\s]+|[\s]+$)', '', 'g')) AS s,
    LOWER(REGEXP_REPLACE(COALESCE(fna_pathology_report, ''), '(^[\s]+|[\s]+$)', '', 'g')) AS p,
    specimen_location, fna_pathology_report
  FROM main.canonical_fna_events_v1
),
classified AS (
  SELECT *,
    (s LIKE '%lymph%' OR p LIKE '%lymph node%'
     OR s LIKE '%neck node%' OR s LIKE '%neck mass%' OR s LIKE '%neck ln%'
     OR s LIKE '%cervical ln%' OR s LIKE '%cervical lymph%'
     OR s LIKE '% ln %' OR s LIKE 'ln %' OR s LIKE '%ln fna%' OR s LIKE '%fna ln%'
     OR (s LIKE '%neck%' AND s LIKE '%level%')) AS is_ln,
    (s LIKE '%parathyroid%' OR p LIKE '%parathyroid%') AS is_parathyroid,
    CASE
      WHEN s LIKE '%level 1%' OR s LIKE '%level i %' OR s LIKE '%level i,%' OR s LIKE '%level i:%' THEN '1'
      WHEN s LIKE '%level 2%' OR s LIKE '%level ii %' OR s LIKE '%level ii,%' OR s LIKE '%level ii:%' THEN '2'
      WHEN s LIKE '%level 3%' OR s LIKE '%level iii%' THEN '3'
      WHEN s LIKE '%level 4%' OR s LIKE '%level iv%' THEN '4'
      WHEN s LIKE '%level 5%' OR s LIKE '%level v %' OR s LIKE '%level v,%' OR s LIKE '%level v:%' THEN '5'
      WHEN s LIKE '%level 6%' OR s LIKE '%level vi%' THEN '6'
      WHEN s LIKE '%level 7%' OR s LIKE '%level vii%' THEN '7'
      ELSE NULL
    END AS lvl,
    (s LIKE '%paratrach%')      AS is_paratracheal,
    (s LIKE '%supraclavic%')    AS is_supraclavicular,
    (s LIKE '%submandibular%')  AS is_submandibular,
    (s LIKE '%mediastin%' OR s LIKE '%subcarinal%') AS is_mediastinal,
    (s LIKE '%central%')        AS is_central,
    (s LIKE '%isthmus%' OR s LIKE '%isthums%' OR s LIKE '%ishtmus%' OR s LIKE '%isthmic%') AS is_isthmus,
    (s LIKE '%left%' OR s LIKE 'll%' OR s LIKE '% ll%' OR s LIKE '%-ll%'
     OR s LIKE '%(ll%' OR s LIKE '%/ll%' OR s LIKE '%fna ll%' OR s LIKE '%"ll%'
     OR s LIKE 'lt %' OR s LIKE '% lt %' OR s LIKE '%lt neck%'
     OR p LIKE '%fna ll%' OR p LIKE '%ll fna%' OR p LIKE '%ll &%' OR p LIKE '%ll nodule%') AS is_left,
    (s LIKE '%right%' OR s LIKE 'rl%' OR s LIKE '% rl%' OR s LIKE '%-rl%'
     OR s LIKE '%(rl%' OR s LIKE '%/rl%' OR s LIKE '%fna rl%' OR s LIKE '%"rl%'
     OR s LIKE 'rt %' OR s LIKE '% rt %' OR s LIKE '%rt neck%'
     OR p LIKE '%fna rl%' OR p LIKE '%rl fna%' OR p LIKE '%& rl%' OR p LIKE '%rl nodule%') AS is_right,
    (s LIKE '%bilateral%' OR (s LIKE '%ll%' AND s LIKE '%rl%')
     OR (p LIKE '%ll & rl%' OR p LIKE '%ll and rl%' OR p LIKE '%rl & ll%')) AS is_explicit_bilateral
  FROM derived
)
UPDATE main.canonical_fna_events_v1 db
SET fna_site = (
  CASE
    WHEN c.is_ln OR c.is_submandibular OR c.is_mediastinal OR c.is_paratracheal OR c.is_supraclavicular THEN
      'lymph_node_' ||
      CASE WHEN c.is_explicit_bilateral OR (c.is_left AND c.is_right) THEN 'bilateral'
           WHEN c.is_left  THEN 'left'
           WHEN c.is_right THEN 'right'
           ELSE 'unspecified' END ||
      CASE
        WHEN c.is_paratracheal     THEN '_paratracheal'
        WHEN c.is_supraclavicular  THEN '_supraclavicular'
        WHEN c.is_submandibular    THEN '_submandibular'
        WHEN c.is_mediastinal      THEN '_mediastinal'
        WHEN c.is_central          THEN '_central'
        WHEN c.lvl IS NOT NULL     THEN '_level_' || c.lvl
        ELSE '_neck'
      END
    WHEN c.is_parathyroid THEN
      'parathyroid_' || CASE WHEN c.is_left THEN 'left' WHEN c.is_right THEN 'right' ELSE 'unspecified' END
    WHEN c.is_explicit_bilateral OR (c.is_left AND c.is_right) THEN 'thyroid_bilateral'
    WHEN c.is_isthmus AND c.is_left  THEN 'thyroid_left_lobe_isthmus'
    WHEN c.is_isthmus AND c.is_right THEN 'thyroid_right_lobe_isthmus'
    WHEN c.is_isthmus              THEN 'thyroid_isthmus'
    WHEN c.is_left                 THEN 'thyroid_left_lobe'
    WHEN c.is_right                THEN 'thyroid_right_lobe'
    WHEN c.s = '' AND c.p = '' THEN NULL
    WHEN c.laterality = 'left'    THEN 'thyroid_left_lobe'
    WHEN c.laterality = 'right'   THEN 'thyroid_right_lobe'
    WHEN c.laterality = 'isthmus' THEN 'thyroid_isthmus'
    WHEN c.s LIKE '%thyroid%' OR c.p LIKE '%thyroid%' OR c.s LIKE '%thyroud%' THEN 'thyroid_unspecified'
    WHEN c.specimen_location IS NULL AND c.fna_pathology_report IS NULL THEN NULL
    ELSE 'unclear'
  END
)
FROM classified c
WHERE db.fna_event_id = c.fna_event_id;

INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, ordinal_position,
   category, upstream_source, verification_status, verified_by,
   verification_method, batch_id, notes, registered_ts)
VALUES (
  'main', 'canonical_fna_events_v1', 'fna_site', 'VARCHAR',
  (SELECT MAX(ordinal_position) + 1 FROM main.canonical_column_verification_registry_v1
   WHERE schema_name='main' AND table_name='canonical_fna_events_v1'),
  'derived', 'specimen_location + fna_pathology_report (rule-based extraction)',
  'not_started', NULL, NULL, NULL,
  'mig_75: NEW column for FNA anatomic site. Pending Logan review of 17 rows '
  '(13 unclear + 4 truly-empty) in fna_site_unclear_review.csv.',
  CURRENT_TIMESTAMP
);

-- =============================================================================
-- end of migration 75
-- =============================================================================
