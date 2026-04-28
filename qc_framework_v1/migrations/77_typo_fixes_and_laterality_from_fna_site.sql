-- =============================================================================
-- Migration 77 -- typo fixes + re-derive laterality from fna_site
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Logan flagged research_id 10006 fna_index 2 as wrong: specimen
--         clearly says "Rigt thyroid" (typo for Right) but laterality was
--         set to 'left'. Investigation found 5 such typo rows and a much
--         broader pattern of laterality-vs-specimen disagreement (hundreds
--         of rows). Cleanest fix: re-derive laterality from the
--         Logan-audited fna_site column.
--
-- mig_77a: 5 explicit typo fixes
--   10006/2: 'Rigt thyroid'              -> right / thyroid_right_lobe
--   10205/2: 'Righ neck Level 4'         -> right / lymph_node_right_level_4
--   10591/1: 'Righ Mid Lower Thyroid'    -> right / thyroid_right_lobe
--   9191/1:  'Righ thyroid'              -> right / thyroid_right_lobe
--   8899/1:  'rigth level iii'           -> right / lymph_node_right_level_3
--
-- mig_77b: re-derive laterality from fna_site for all 8,050 rows
--   Mapping (fna_site -> laterality):
--     thyroid_left_*, lymph_node_left_*, parathyroid_left, neck_mass_left   -> left
--     thyroid_right_*, lymph_node_right_*, parathyroid_right, neck_mass_right -> right
--     thyroid_isthmus                                                        -> isthmus
--     all other (unspecified / bilateral / cyst / midline)                   -> NULL
--
-- mig_77c: flip laterality column to verified
--   verification_method = 'mechanical_derivation_compare'
--   batch_id = 'mig_77_laterality_from_fna_site'
--
-- Net effect:
--   Pre-state:  laterality had hundreds of rows where the value contradicted
--               specimen_location text (typos + earlier processing errors).
--   Post-state: 100% consistency between laterality and fna_site.
--               left=3,613 / right=3,584 / isthmus=351 / NULL=502 (= 8,050).
--   This supersedes the laterality_review_round1.csv -- the 530 disagree +
--   72 ambiguous rows are now resolved automatically via fna_site authority.
--
--   canonical_fna_events_v1 n_verified: 21 -> 22 of 39.
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

-- 77a: typo fixes
UPDATE main.canonical_fna_events_v1
SET laterality = 'right',
    fna_site = CASE
      WHEN research_id = '10006' AND fna_index = 2 THEN 'thyroid_right_lobe'
      WHEN research_id = '10205' AND fna_index = 2 THEN 'lymph_node_right_level_4'
      WHEN research_id = '10591' AND fna_index = 1 THEN 'thyroid_right_lobe'
      WHEN research_id = '9191'  AND fna_index = 1 THEN 'thyroid_right_lobe'
      WHEN research_id = '8899'  AND fna_index = 1 THEN 'lymph_node_right_level_3'
    END
WHERE (research_id, fna_index) IN (
  ('10006', 2), ('10205', 2), ('10591', 1), ('9191', 1), ('8899', 1)
);

-- 77b: re-derive laterality from fna_site for all rows
UPDATE main.canonical_fna_events_v1
SET laterality = CASE
  WHEN fna_site LIKE 'thyroid_left%'
    OR fna_site LIKE 'lymph_node_left%'
    OR fna_site = 'parathyroid_left'
    OR fna_site = 'neck_mass_left'                   THEN 'left'
  WHEN fna_site LIKE 'thyroid_right%'
    OR fna_site LIKE 'lymph_node_right%'
    OR fna_site = 'parathyroid_right'
    OR fna_site = 'neck_mass_right'                  THEN 'right'
  WHEN fna_site = 'thyroid_isthmus'                  THEN 'isthmus'
  ELSE NULL
END;

-- 77c: flip laterality to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_77_laterality_from_fna_site',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_77: re-derived from fna_site (Logan-audited authoritative source). '
                          || 'Distribution: left=3613, right=3584, isthmus=351, NULL=502 '
                          || '(NULL covers thyroid_unspecified / thyroid_bilateral / thyroid_cyst / '
                          || 'lymph_node_unspecified / parathyroid_unspecified / midline_cyst). '
                          || 'mig_77a fixed 5 typo rows (Rigt/Righ/rigth) before re-derivation.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1' AND column_name='laterality';

-- Recompute table signoff
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
-- end of migration 77
-- =============================================================================
