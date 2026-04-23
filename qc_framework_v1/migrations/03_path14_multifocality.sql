-- ============================================================================
-- Migration 03 — PATH14: rebuild multifocality (focality + episode_laterality)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH14
-- Author:        Logan Glosser
-- Date:          2026-04-22
-- ----------------------------------------------------------------------------
-- DESIGN REVISION (2026-04-22):
--   Original prompt asked for (number_of_tumors, multifocality_flag,
--   bilateral_flag). Two issues surfaced:
--     (1) `laterality` on path is free-text with 79 distinct values — the
--         strict `laterality IN ('left','right')` rule produced 0 bilateral
--         flags because the dominant bilateral label is the string
--         `'bilateral'` (3,500 rows), plus lobe-suffixed variants.
--     (2) Conceptually, "multifocality" and "bilaterality" are orthogonal
--         clinical axes. Folding unifocal-bilateral cases (single tumor
--         crossing midline) into a generic bilateral_flag was misleading.
--
--   New contract — two separate axes:
--     focality           ∈ {'unifocal','multifocal'}
--     episode_laterality ∈ {'left','right','bilateral','isthmus','other','unknown'}
--
--   This cleanly distinguishes:
--     - unifocal-bilateral (n=1,212) — one tumor crossing midline
--     - multifocal-bilateral (n=1,063) — truly bilateral disease
--     - unifocal-unilateral (625 right, 563 left) — standard cases
--     - multifocal-unilateral (307 right, 253 left)
--     - isthmus / other / unknown edge cases
--
--   The old boolean columns (multifocality_flag, bilateral_flag) are
--   DROPPED. Grep confirmed no downstream prompt or code references them.
--
-- Row-level laterality normalization:
--   lowercase; 'bilateral' string OR substring of both 'left' and 'right'
--   → 'bilateral'; else substring 'left'/'right'/'isthmus' → that side;
--   else 'other'; NULL/empty → NULL.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.path_episode_multifocality_v1 AS
WITH rows_norm AS (
    SELECT research_id,
           surgery_episode_uid,
           CASE
               WHEN laterality IS NULL OR TRIM(laterality) = ''       THEN NULL
               WHEN LOWER(TRIM(laterality)) = 'bilateral'             THEN 'bilateral'
               WHEN LOWER(laterality) LIKE '%left%'
                AND LOWER(laterality) LIKE '%right%'                  THEN 'bilateral'
               WHEN LOWER(laterality) LIKE '%left%'                   THEN 'left'
               WHEN LOWER(laterality) LIKE '%right%'                  THEN 'right'
               WHEN LOWER(laterality) LIKE '%isthmus%'                THEN 'isthmus'
               ELSE                                                        'other'
           END AS lat_norm
    FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed
)
SELECT
    research_id,
    surgery_episode_uid,
    COUNT(*)                                         AS number_of_tumors,
    CASE WHEN COUNT(*) = 1 THEN 'unifocal' ELSE 'multifocal' END AS focality,
    CASE
        WHEN SUM(CASE WHEN lat_norm = 'bilateral' THEN 1 ELSE 0 END) > 0 THEN 'bilateral'
        WHEN SUM(CASE WHEN lat_norm = 'left'  THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN lat_norm = 'right' THEN 1 ELSE 0 END) > 0     THEN 'bilateral'
        WHEN SUM(CASE WHEN lat_norm = 'left'  THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN lat_norm = 'right' THEN 1 ELSE 0 END) = 0     THEN 'left'
        WHEN SUM(CASE WHEN lat_norm = 'right' THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN lat_norm = 'left'  THEN 1 ELSE 0 END) = 0     THEN 'right'
        WHEN SUM(CASE WHEN lat_norm = 'isthmus' THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN lat_norm IN ('left','right') THEN 1 ELSE 0 END) = 0 THEN 'isthmus'
        WHEN SUM(CASE WHEN lat_norm IS NULL THEN 1 ELSE 0 END) = COUNT(*) THEN 'unknown'
        ELSE                                                                    'other'
    END AS episode_laterality
FROM rows_norm
GROUP BY research_id, surgery_episode_uid;
