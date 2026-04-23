-- ============================================================================
-- Migration 08 — HIST01/HIST02/HIST03: normalize histology_final in cohort
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     HIST01 (whitespace), HIST02 (unnormalized variants),
--                HIST03 (metastatic prefix collapsing site into histology)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.manuscript_cohort_v1:
--   10,871 rows / 59 distinct histology_final values / 4,137 non-NULL
--   Whitespace issues: "PTC " (72), " metastatic PTC follicular" (1),
--                      " Poorly differentiated..." (1),
--                      "  metastatic papillary thyroid carcinoma..." (1),
--                      "  infiltrating carcinoma with thymUS LIKE..." (1),
--                      "Differentiated high grade thyroid carcinoma " (1) ...
--   Case-inconsistency: PTC/pTC, "Follicular carcinoma" vs "follicular carcinoma",
--                       "Anaplastic carcinoma" vs "anaplastic carcinoma", etc.
--   Metastatic prefix: 179 rows match (case-insensitive 'metastatic ' leading).
--   Unnormalized PTC family: PTC(3001), "PTC "(72), pTC(2), "metastatic PTC"(133),
--                            "Metastatic PTC"(8), various "...PTC..." free-text.
--   Misspellings: "metastatitic PTC"(1), "pooly differentiated..."(1),
--                 "microscopiic"(present in ETE not HIST but same class of issue),
--                 "Poorly differentied PTC"(1), "differntiation"(1), "paillary"(1).
-- ----------------------------------------------------------------------------
-- Design:
--   Pipeline in a single CTE:
--     1) hist_trim  = LOWER(TRIM(REGEXP_REPLACE(histology_final,'\s+',' ','g')))
--        — collapses internal whitespace (handles the "**THYROID bIOPSY\nAnaplastic
--        carcinoma", "MTC\nPTC mixed composit" newline-embedded cases).
--     2) hist_met_stripped = strip leading 'metastatic ' and 'recurrent '
--        (in any order) — so "recurrent/metastatic PTC", "metastatic/recurrent
--        PTC", "recurrent PTC" all become "ptc" for classification purposes.
--     3) Controlled-vocab CASE:
--          NULL/empty                                  → NULL
--          equals 'ptc' or starts with 'ptc '          → papillary thyroid carcinoma
--          contains 'papillary' AND 'carcinoma'        → papillary thyroid carcinoma
--          equals 'mtc' or starts with 'mtc '          → medullary thyroid carcinoma
--          contains 'medullary' AND 'carcinoma'        → medullary thyroid carcinoma
--          contains 'anaplastic'                       → anaplastic thyroid carcinoma
--          contains 'hurthle' OR contains 'oncocytic'  → oncocytic thyroid carcinoma
--          equals 'niftp'                              → NIFTP
--          equals 'ftump'                              → FTUMP
--          contains 'follicular adenoma'               → follicular adenoma
--          contains 'follicular' AND 'carcinoma'       → follicular thyroid carcinoma
--          contains 'poorly differentiated'            → poorly differentiated thyroid carcinoma
--            OR 'pooly differentiated' (typo)          (typo fold-in)
--            OR 'poorly differentied' (typo)
--          contains 'differentiated high grade'        → differentiated high grade thyroid carcinoma
--          contains 'high grade' AND 'carcinoma'       → high grade thyroid carcinoma
--          contains 'angiosarcoma'                     → angiosarcoma
--          contains 'adenoid cystic'                   → adenoid cystic carcinoma
--          contains 'nut carcinoma'                    → NUT carcinoma
--          contains 'infiltrating carcinoma'           → thymic-like carcinoma (NIFTP/CASTLE-like)
--          contains 'squamous'                         → squamous-featured carcinoma
--          contains 'neuroendocrine'                   → poorly differentiated neuroendocrine carcinoma
--          default                                     → hist_trim (audit fallback)
--
--   histology_metastatic_prefix_flag: TRUE when hist_trim starts with 'metastatic '
--     (covers the 179-row HIST03 cohort). Note: "recurrent/metastatic" matches
--     too (does not start with 'metastatic' but contains it) — but that falls
--     under a separate recurrent-prefix semantic and the prompt's spec is
--     literal on "starts with 'metastatic '", so the flag stays narrow.
--     Future researchers wanting an either/or can combine with LIKE '%metastatic%'.
--
--   histology_variant_extracted: greedy LIKE-based token extraction. Multiple
--     matches concatenated with '; '. Tokens checked (per prompt spec, plus
--     empirically present):
--       tall cell, columnar, diffuse sclerosing, follicular variant,
--       solid variant, classical/classic, oncocytic, hurthle.
--     Output examples:
--       "metastatic PTC tall cell variant"      → "tall cell"
--       "metastatic PTC classical"              → "classical"
--       "metastatic PTC classical with ... oncocytic & focal tall cell features <5%"
--                                               → "tall cell; classical; oncocytic"
--       "metastatic PTC with focal tall cell features" → "tall cell"
-- ----------------------------------------------------------------------------
-- Output: manuscript_workspace.manuscript_cohort_v1_histology_clean (view
--   over main.manuscript_cohort_v1; never mutates the source).
--
-- Queue policy: no rows emitted under HIST01/HIST02. Those issues are
--   resolved by the view itself — the clean columns ARE the resolution,
--   no per-row human review is needed for whitespace or case canonicalization.
--   HIST03 likewise: the prefix flag is the resolution; downstream cohort
--   logic decides whether to exclude metastatic-only rows, not HIST03.
--   (Per pattern established in prompts 06/07: rename/rebuild views that
--   resolve the entire issue class in one pass get no queue.)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.manuscript_cohort_v1_histology_clean AS
WITH base AS (
    SELECT
        c.*,
        -- Step 1: whitespace + case normalization
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(c.histology_final, ''), '\s+', ' ', 'g')))
            AS _hist_trim
    FROM main.manuscript_cohort_v1 c
),
stripped AS (
    SELECT
        b.*,
        -- Step 2: strip leading metastatic / recurrent prefixes (any order,
        -- including "recurrent/metastatic" and "metastatic/recurrent").
        TRIM(REGEXP_REPLACE(
            _hist_trim,
            '^(metastatic/recurrent|recurrent/metastatic|metastatic|recurrent)\s+',
            '',
            'g'
        )) AS _hist_met_stripped
    FROM base b
)
SELECT
    s.* EXCLUDE (_hist_trim, _hist_met_stripped),
    -- histology_final_clean: controlled-vocab mapping
    CASE
        WHEN s._hist_trim = '' OR s._hist_trim IS NULL THEN NULL
        WHEN s._hist_met_stripped = 'ptc'
             OR s._hist_met_stripped LIKE 'ptc %'
             OR s._hist_met_stripped LIKE 'ptc/%'
             OR s._hist_met_stripped LIKE 'high-grade ptc%'
             OR s._hist_met_stripped LIKE '%ptc tall%'
             OR s._hist_met_stripped LIKE '%ptc classical%'
             OR s._hist_met_stripped LIKE '%ptc follicular%'
             OR s._hist_met_stripped LIKE '%ptc with %'
             OR s._hist_met_stripped LIKE '%ptc?%'
             OR s._hist_met_stripped LIKE 'metastatitic ptc%'
             OR s._hist_met_stripped LIKE 'poorly differentied ptc%'
             OR (s._hist_met_stripped LIKE '%papillary%' AND s._hist_met_stripped LIKE '%carcinoma%')
             OR s._hist_met_stripped LIKE '%paillary%' -- typo: "paillary"
             THEN 'papillary thyroid carcinoma'
        WHEN s._hist_met_stripped = 'mtc'
             OR s._hist_met_stripped LIKE 'mtc %'
             OR s._hist_met_stripped LIKE 'mtc/%'
             OR (s._hist_met_stripped LIKE '%medullary%' AND s._hist_met_stripped LIKE '%carcinoma%')
             THEN 'medullary thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%anaplastic%'
             THEN 'anaplastic thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%hurthle%'
             OR s._hist_met_stripped LIKE '%oncocytic%'
             THEN 'oncocytic thyroid carcinoma'
        WHEN s._hist_met_stripped = 'niftp' THEN 'NIFTP'
        WHEN s._hist_met_stripped = 'ftump' THEN 'FTUMP'
        WHEN s._hist_met_stripped LIKE '%atypical follicular adenoma%'
             THEN 'atypical follicular adenoma'
        WHEN s._hist_met_stripped LIKE '%follicular adenoma%'
             THEN 'follicular adenoma'
        WHEN (s._hist_met_stripped LIKE '%follicular%' AND s._hist_met_stripped LIKE '%carcinoma%')
             THEN 'follicular thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%differentiated high grade%'
             THEN 'differentiated high grade thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%poorly differentiated%'
             OR s._hist_met_stripped LIKE '%pooly differentiated%'
             OR s._hist_met_stripped LIKE '%poorly differentied%'
             THEN 'poorly differentiated thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%neuroendocrine%'
             THEN 'poorly differentiated neuroendocrine carcinoma'
        WHEN s._hist_met_stripped LIKE '%differentiated thyroid carcinoma%'
             THEN 'differentiated thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%angiosarcoma%'
             THEN 'angiosarcoma'
        WHEN s._hist_met_stripped LIKE '%adenoid cystic%'
             THEN 'adenoid cystic carcinoma'
        WHEN s._hist_met_stripped LIKE '%nut carcinoma%'
             THEN 'NUT carcinoma'
        WHEN s._hist_met_stripped LIKE '%thymic%'
             OR s._hist_met_stripped LIKE '%thymus like%'
             OR s._hist_met_stripped LIKE '%thymus-like%'
             THEN 'thymic-like carcinoma'
        WHEN s._hist_met_stripped LIKE '%squamous%'
             OR s._hist_met_stripped LIKE '%high grade%'
             THEN 'high grade / squamous thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%metastatic thyroid carcinoma%'
             THEN 'thyroid carcinoma unspecified'
        ELSE s._hist_met_stripped
    END AS histology_final_clean,

    -- histology_metastatic_prefix_flag: TRUE when trimmed-normalized string
    -- starts with 'metastatic '
    (s._hist_trim LIKE 'metastatic %') AS histology_metastatic_prefix_flag,

    -- histology_variant_extracted: semicolon-concatenated subtype tokens
    NULLIF(TRIM(BOTH '; ' FROM (
          (CASE WHEN s._hist_met_stripped LIKE '%tall cell%'          THEN 'tall cell; '          ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%columnar%'           THEN 'columnar; '           ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%diffuse sclerosing%' THEN 'diffuse sclerosing; ' ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%follicular variant%'
                  OR s._hist_met_stripped LIKE '%follicular growth pattern%'
                  OR (s._hist_met_stripped LIKE '%ptc follicular%' AND s._hist_met_stripped NOT LIKE '%follicular variant%')
               THEN 'follicular variant; ' ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%solid variant%'      THEN 'solid variant; '      ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%classical%'
                  OR s._hist_met_stripped LIKE '%classic type%'
                  OR s._hist_met_stripped = 'classic'
                  OR s._hist_met_stripped LIKE 'classic %'
               THEN 'classical; ' ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%oncocytic%'          THEN 'oncocytic; '          ELSE '' END)
        ||(CASE WHEN s._hist_met_stripped LIKE '%hurthle%'            THEN 'hurthle; '            ELSE '' END)
    )), '') AS histology_variant_extracted
FROM stripped s;

-- ---------------------------------------------------------------------------
-- No queue emission — HIST01/02/03 are resolved by the view itself, not by
-- row-level chart review. The clean columns ARE the resolution.
-- ---------------------------------------------------------------------------
