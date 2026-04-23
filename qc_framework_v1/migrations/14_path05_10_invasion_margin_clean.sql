-- ============================================================================
-- Migration 14 — PATH05-10: normalize 6 invasion/margin columns on path events
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH05 margin_status           (14 raws)
--                PATH06 capsular_invasion       (50 raws)
--                PATH07 extranodal_extension    (55 raws — mostly location trails)
--                PATH08 lymphatic_invasion      (18 raws)
--                PATH09 perineural_invasion     (6 raws)
--                PATH10 vascular_invasion       (20 raws)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Shared pattern: single path event row holds 6 semi-structured invasion fields,
-- each ~50-90% NULL or carrying 'x' (Emory convention = checked/negative).
-- None use a controlled vocab in raw form; all mix typos, multi-line entries,
-- spec abbreviations (c/a = close/abutting, n/s = not specified, x = negative).
--
-- Emory 'x' convention (verified by distribution — highest frequency non-NULL
-- token across PNI 93%, LVI 72%, vascular 81%, margin 79% of non-NULL rows):
--   On boolean-ish CAP synoptic checkboxes 'x' = marked "not present/uninvolved"
--   (i.e. negative). On capsular_invasion (28% x) the distribution is mixed
--   and 'x' still coded as negative — alternate reading "not assessed" is
--   already captured by 4,779 explicit NULLs.
-- ----------------------------------------------------------------------------
-- Controlled vocabs (per-column, most-specific-wins precedence):
--
-- margin_status_clean:          positive | close | negative | indeterminate | NULL
-- capsular_invasion_clean:      widely_invasive | minimally_invasive | present_nos
--                               | negative | indeterminate | NULL
-- extranodal_extension_clean:   present | negative | indeterminate | NULL
-- lymphatic_invasion_clean:     extensive | focal | present | negative
--                               | indeterminate | NULL
-- perineural_invasion_clean:    focal | present | negative | indeterminate | NULL
-- vascular_invasion_clean:      extensive | focal | present | negative
--                               | indeterminate | NULL
--
-- Precedence (all 6 columns share the pattern):
--   explicit-extensive > explicit-focal/minimal > generic-present
--   > close/abutting > negative-sentinel > indeterminate > NULL
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean
--     — all rows + 6 *_clean columns + 6 *_raw pass-throughs.
--
-- No queue: classification IS the resolution for all 6 issues.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean AS
WITH norm AS (
    SELECT e.*,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.margin_status       , ''), '\s+', ' ', 'g'))) AS _m,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.capsular_invasion   , ''), '\s+', ' ', 'g'))) AS _c,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.extranodal_extension, ''), '\s+', ' ', 'g'))) AS _e,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.lymphatic_invasion  , ''), '\s+', ' ', 'g'))) AS _l,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.perineural_invasion , ''), '\s+', ' ', 'g'))) AS _p,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.vascular_invasion   , ''), '\s+', ' ', 'g'))) AS _v
    FROM main.canonical_path_malignant_events_v1 e
)
SELECT
    n.* EXCLUDE (_m, _c, _e, _l, _p, _v),
    n.margin_status        AS margin_status_raw,
    n.capsular_invasion    AS capsular_invasion_raw,
    n.extranodal_extension AS extranodal_extension_raw,
    n.lymphatic_invasion   AS lymphatic_invasion_raw,
    n.perineural_invasion  AS perineural_invasion_raw,
    n.vascular_invasion    AS vascular_invasion_raw,

    -- ------------------------------------------------------------------ margin
    CASE
        WHEN n._m = '' OR n._m IS NULL                          THEN NULL
        WHEN n._m IN ('involved','involvd','present')           THEN 'positive'
        WHEN n._m IN ('c/a','<1','1','0.1')                     THEN 'close'
        WHEN n._m IN ('indeterminate','n/s')                    THEN 'indeterminate'
        WHEN n._m IN ('x','negative')                           THEN 'negative'
        ELSE 'other'
    END AS margin_status_clean,

    -- --------------------------------------------------------- capsular_invasion
    CASE
        WHEN n._c = '' OR n._c IS NULL                                       THEN NULL
        WHEN n._c LIKE '%widely invasive%' OR n._c LIKE '%widely invasivre%'
             OR n._c LIKE '%widely invasvie%' OR n._c LIKE '%invasive%'
             OR n._c LIKE '%infiltrative%' OR n._c LIKE '%multifocal%'
             OR n._c LIKE '%multiple foci%'
                                                                             THEN 'widely_invasive'
        WHEN n._c LIKE '%minimally invasive%' OR n._c LIKE '%miinimally%'
             OR n._c LIKE '%minimally invasvie%' OR n._c LIKE '%minimallyinvasive%'
             OR n._c = 'minimal' OR n._c LIKE 'minimal %' OR n._c LIKE '%(minimal%'
             OR n._c LIKE '%minimal;%' OR n._c LIKE '%minimal%)'
             OR n._c = 'focal' OR n._c LIKE '%(focal%' OR n._c = 'single focus'
             OR n._c = 'c/a' OR n._c = 'into but not through'
             OR n._c LIKE '%into but not through%'
             OR n._c = 'm'
                                                                             THEN 'minimally_invasive'
        WHEN n._c LIKE '%present%' OR n._c LIKE '%preesent%'
             OR n._c LIKE '%preseent%' OR n._c LIKE '%preent%'
             OR n._c LIKE '%prewent%'
             OR n._c = 'yes' OR n._c = 'yes;' OR n._c LIKE 'yes;%'
                                                                             THEN 'present_nos'
        WHEN n._c IN ('indeterminate','cannot be assessed','equivocal',
                      'n/s','n/s;','infiltrative?','none?')                  THEN 'indeterminate'
        WHEN n._c IN ('x','no','no;','none','n/a')                           THEN 'negative'
        ELSE 'other'
    END AS capsular_invasion_clean,

    -- ------------------------------------------------------ extranodal_extension
    CASE
        WHEN n._e = '' OR n._e IS NULL                                       THEN NULL
        WHEN n._e LIKE 'present%' OR n._e LIKE '%extensive%'
             OR n._e = 'yes' OR n._e = 'focal' OR n._e LIKE 'focal%'
             OR n._e = 'minimal' OR n._e = 'suspected'
                                                                             THEN 'present'
        WHEN n._e IN ('indeterminate','equivocal','n/s','ns','c/a')          THEN 'indeterminate'
        WHEN n._e IN ('x','no','none')                                       THEN 'negative'
        ELSE 'other'
    END AS extranodal_extension_clean,

    -- --------------------------------------------------------- lymphatic_invasion
    CASE
        WHEN n._l = '' OR n._l IS NULL                                       THEN NULL
        WHEN n._l LIKE '%extensive%' OR n._l LIKE '%extensivre%'
             OR n._l LIKE '%extensiver%'
                                                                             THEN 'extensive'
        WHEN n._l = 'focal' OR n._l = '1 focus'                              THEN 'focal'
        WHEN n._l LIKE '%present%' OR n._l LIKE '%preesent%'                 THEN 'present'
        WHEN n._l IN ('indeterminate','indeeterminate','indetermiante',
                      'indeterminent','n/s','c/a','suspicious')
             OR n._l LIKE 'cannot be determined%'                            THEN 'indeterminate'
        WHEN n._l IN ('x','no')                                              THEN 'negative'
        ELSE 'other'
    END AS lymphatic_invasion_clean,

    -- --------------------------------------------------------- perineural_invasion
    CASE
        WHEN n._p = '' OR n._p IS NULL                                       THEN NULL
        WHEN n._p = 'focal'                                                  THEN 'focal'
        WHEN n._p LIKE '%present%'                                           THEN 'present'
        WHEN n._p IN ('indeterminate','c/a')                                 THEN 'indeterminate'
        WHEN n._p = 'x'                                                      THEN 'negative'
        ELSE 'other'
    END AS perineural_invasion_clean,

    -- ----------------------------------------------------------- vascular_invasion
    CASE
        WHEN n._v = '' OR n._v IS NULL                                       THEN NULL
        WHEN n._v LIKE '%extensive%' OR n._v LIKE '%extrensive%'
             OR n._v LIKE '%estensive%' OR n._v = 'prominent'
             OR n._v = 'multifocal'
                                                                             THEN 'extensive'
        WHEN n._v = 'focal' OR n._v = 'foacl' OR n._v = 'minimal'
             OR n._v = 'limited' OR n._v = 's'
                                                                             THEN 'focal'
        WHEN n._v LIKE '%present%' OR n._v LIKE '%presnt%'
             OR n._v LIKE '%preent%' OR n._v = 'identified'
                                                                             THEN 'present'
        WHEN n._v IN ('indeterminate','suspicious','c/a')                    THEN 'indeterminate'
        WHEN n._v = 'x'                                                      THEN 'negative'
        ELSE 'other'
    END AS vascular_invasion_clean

FROM norm n;

-- ---------------------------------------------------------------------------
-- Cleanup pass — 6 COMMENT ON + 6 deprecation_log rows under prompt_13
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.margin_status IS
'RAW FIELD — 14 distinct raws, includes "x" (Emory=negative sentinel) and size stubs (0.1, <1, 1) for close margins. Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.margin_status_clean (4 buckets + NULL). PATH05 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.capsular_invasion IS
'RAW FIELD — 50 distinct raws, mixes minimally-invasive/widely-invasive semantics with bare yes/no/x. Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.capsular_invasion_clean (5 buckets + NULL). PATH06 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.extranodal_extension IS
'RAW FIELD — 55 distinct raws dominated by "present\n<LN location>" multi-line entries. Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.extranodal_extension_clean (3 buckets + NULL). PATH07 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.lymphatic_invasion IS
'RAW FIELD — 18 distinct raws including typos (preesent, extensivre, indetermiante). Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.lymphatic_invasion_clean (5 buckets + NULL). PATH08 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.perineural_invasion IS
'RAW FIELD — 6 distinct raws. Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.perineural_invasion_clean (4 buckets + NULL). PATH09 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.vascular_invasion IS
'RAW FIELD — 20 distinct raws w/ extensive vs focal gradation + typos. Use manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean.vascular_invasion_clean (5 buckets + NULL). PATH10 resolved 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_13';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.margin_status','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH05','prompt_13','column_only',DATE '2026-04-23',
   '14 distinct raws; "x" sentinel + size-stub close-margin entries.',NULL,
   '4-bucket clean column (positive/close/negative/indeterminate + NULL).'),
  ('main.canonical_path_malignant_events_v1.capsular_invasion','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH06','prompt_13','column_only',DATE '2026-04-23',
   '50 distinct raws; mixes minimal/wide/present-nos w/ yes/no sentinels.',NULL,
   '5-bucket clean column preserving widely_invasive vs minimally_invasive distinction clinically relevant for FVPTC.'),
  ('main.canonical_path_malignant_events_v1.extranodal_extension','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH07','prompt_13','column_only',DATE '2026-04-23',
   '55 distinct raws — most are "present\n<LN location>" multi-line entries; location stripped in clean column.',NULL,
   '3-bucket clean column. LN location suffixes preserved in raw pass-through for downstream use.'),
  ('main.canonical_path_malignant_events_v1.lymphatic_invasion','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH08','prompt_13','column_only',DATE '2026-04-23',
   '18 distinct raws incl. 4 indeterminate typos, 3 extensive typos.',NULL,
   '5-bucket clean column (extensive/focal/present/negative/indeterminate + NULL).'),
  ('main.canonical_path_malignant_events_v1.perineural_invasion','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH09','prompt_13','column_only',DATE '2026-04-23',
   '6 distinct raws — tightest vocab of the 6.',NULL,
   '4-bucket clean column.'),
  ('main.canonical_path_malignant_events_v1.vascular_invasion','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean',
   'PATH10','prompt_13','column_only',DATE '2026-04-23',
   '20 distinct raws; extensive vs focal gradation preserved, multiple typos collapsed.',NULL,
   '5-bucket clean column; DOUBLE column angioinvasion_quantify remains for numeric scoring.');
