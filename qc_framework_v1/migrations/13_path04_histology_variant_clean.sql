-- ============================================================================
-- Migration 13 — PATH04: normalize histology_variant on path events
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH04 (histology_variant is raw prose, no controlled vocab).
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.canonical_path_malignant_events_v1.histology_variant:
--   ~180 distinct raws over 6,689 rows. 1,167 NULL. Multi-line entries
--   ("classical\ntall cell features (10%)"), typos (microcarcioma, micorcarcinoma,
--   microcarinoma, tal cell, onocytic, cribiform-morular, tall cel), and
--   size-qualifiers (Microcarcinoma 2,369 + Microcarcinoma variants) mixed
--   into the variant column.
-- ----------------------------------------------------------------------------
-- Controlled vocab (per prompt 12 spec + 2 thyroid-specific additions):
--   classical
--   tall_cell
--   columnar_cell
--   hobnail
--   diffuse_sclerosing
--   follicular_variant_encapsulated     (covers bare 'follicular', 'minimally invasive')
--   follicular_variant_infiltrative     ('widely invasive', 'follicular sclerosing')
--   solid_variant
--   cribriform_morular                  (typos: cribiform-morular)
--   oncocytic                           (oncocytic, hurthle cell, onocytic typos)
--   warthin_like
--   microcarcinoma                      (+ typos: microcarcioma, micorcarcinoma,
--                                         microcarinoma, microcaarcinoma)   [addition]
--   insular                             (insular variant, insular growth)   [addition]
--   other
--   NULL (source NULL or unmappable)
--
--   Precedence (most-specific wins — same pattern as migration 10 ETE):
--     diffuse_sclerosing → cribriform_morular → hobnail → columnar_cell
--     → tall_cell → warthin_like → solid_variant
--     → follicular_variant_infiltrative → follicular_variant_encapsulated
--     → oncocytic → insular → microcarcinoma → classical → other.
--
-- Deviation from prompt spec:
--   +microcarcinoma (2,369 + typos of the same = ~2,500 rows, dominant raw)
--   +insular (clinically distinct entity with its own bucket in other parts of
--             the DB; grouping into 'other' would lose 20+ flagged rows).
--   Documented here so downstream users know these buckets exist beyond spec.
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.dim_histology_variant_v1
--     — distinct raw → clean mapping (self-refreshing: it re-evaluates CASE
--       over current DISTINCT raws, so new variants surface as 'other').
--   manuscript_workspace.canonical_path_malignant_events_v1_variant_clean
--     — all rows + histology_variant_clean column via same CASE.
--
-- No queue: variant normalization resolves PATH04 entirely (the classification
-- is the resolution — no row requires human review beyond new 'other' entries
-- that accumulate over time; those are a monitoring concern, not a QC issue).
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.dim_histology_variant_v1 AS
WITH raw AS (
    SELECT DISTINCT histology_variant AS variant_raw
    FROM main.canonical_path_malignant_events_v1
),
norm AS (
    SELECT
        variant_raw,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(variant_raw, ''), '\s+', ' ', 'g'))) AS _v
    FROM raw
)
SELECT
    variant_raw,
    CASE
        WHEN _v = '' OR _v IS NULL                                     THEN NULL
        WHEN _v LIKE '%diffuse sclerosing%'                            THEN 'diffuse_sclerosing'
        WHEN _v LIKE '%cribriform%morular%' OR _v LIKE '%cribiform%morular%'
             OR _v LIKE '%cribiform%morula%' OR _v LIKE '%cribriform%morula%'
                                                                       THEN 'cribriform_morular'
        WHEN _v LIKE '%hobnail%'                                       THEN 'hobnail'
        WHEN _v LIKE '%columnar cell%' OR _v LIKE '%columnar-cell%'
                                                                       THEN 'columnar_cell'
        WHEN _v LIKE '%tall cell%' OR _v LIKE '%tall-cell%'
             OR _v LIKE '%tal cell%' OR _v LIKE '%tall cel %'
             OR _v LIKE '%tall cel l%' OR _v LIKE '%tall cell feature%'
                                                                       THEN 'tall_cell'
        WHEN _v LIKE '%warthin%'                                       THEN 'warthin_like'
        WHEN _v LIKE '%solid variant%' OR _v = 'solid' OR _v LIKE 'solid %'
             OR _v LIKE '%solid & trabecular%' OR _v LIKE '%solid/trabecular%'
                                                                       THEN 'solid_variant'
        WHEN _v LIKE '%follicular sclerosing%' OR _v LIKE '%widely invasive%'
             OR _v LIKE '%infiltrative%'
                                                                       THEN 'follicular_variant_infiltrative'
        WHEN _v = 'follicular' OR _v LIKE 'follicular %'
             OR _v LIKE '%follicular variant%' OR _v LIKE '%minimally invasive%'
             OR _v LIKE '%follicular growth%' OR _v LIKE '%macrofollicular%'
                                                                       THEN 'follicular_variant_encapsulated'
        WHEN _v LIKE '%oncocytic%' OR _v LIKE '%onocytic%' OR _v LIKE '%hurthle%'
             OR _v LIKE '%hurthel%' OR _v = 'oxyphilic' OR _v LIKE 'oxyphilic %'
                                                                       THEN 'oncocytic'
        WHEN _v LIKE '%insular%'                                       THEN 'insular'
        WHEN _v LIKE '%microcarcinoma%' OR _v LIKE '%microcarcioma%'
             OR _v LIKE '%micorcarcinoma%' OR _v LIKE '%microcarinoma%'
             OR _v LIKE '%microcaarcinoma%' OR _v LIKE '%microcaricnoma%'
             OR _v LIKE '%microcarcinooma%'
                                                                       THEN 'microcarcinoma'
        WHEN _v LIKE '%folliucalr%' OR _v LIKE '%follicualr%'
                                                                       THEN 'follicular_variant_encapsulated'
        WHEN _v LIKE '%collumnar%'                                     THEN 'columnar_cell'
        WHEN _v LIKE '%classical%' OR _v LIKE '%classic %' OR _v = 'classic'
             OR _v LIKE '%classsical%' OR _v = 'ptc'
                                                                       THEN 'classical'
        ELSE 'other'
    END AS variant_clean
FROM norm;

-- Main view: all rows + histology_variant_clean
CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_variant_clean AS
WITH norm AS (
    SELECT e.*,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.histology_variant, ''), '\s+', ' ', 'g'))) AS _v
    FROM main.canonical_path_malignant_events_v1 e
)
SELECT
    n.* EXCLUDE (_v),
    n.histology_variant AS histology_variant_raw,
    CASE
        WHEN n._v = '' OR n._v IS NULL                                     THEN NULL
        WHEN n._v LIKE '%diffuse sclerosing%'                              THEN 'diffuse_sclerosing'
        WHEN n._v LIKE '%cribriform%morular%' OR n._v LIKE '%cribiform%morular%'
             OR n._v LIKE '%cribiform%morula%' OR n._v LIKE '%cribriform%morula%'
                                                                           THEN 'cribriform_morular'
        WHEN n._v LIKE '%hobnail%'                                         THEN 'hobnail'
        WHEN n._v LIKE '%columnar cell%' OR n._v LIKE '%columnar-cell%'    THEN 'columnar_cell'
        WHEN n._v LIKE '%tall cell%' OR n._v LIKE '%tall-cell%'
             OR n._v LIKE '%tal cell%' OR n._v LIKE '%tall cel %'
             OR n._v LIKE '%tall cel l%' OR n._v LIKE '%tall cell feature%'
                                                                           THEN 'tall_cell'
        WHEN n._v LIKE '%warthin%'                                         THEN 'warthin_like'
        WHEN n._v LIKE '%solid variant%' OR n._v = 'solid' OR n._v LIKE 'solid %'
             OR n._v LIKE '%solid & trabecular%' OR n._v LIKE '%solid/trabecular%'
                                                                           THEN 'solid_variant'
        WHEN n._v LIKE '%follicular sclerosing%' OR n._v LIKE '%widely invasive%'
             OR n._v LIKE '%infiltrative%'
                                                                           THEN 'follicular_variant_infiltrative'
        WHEN n._v = 'follicular' OR n._v LIKE 'follicular %'
             OR n._v LIKE '%follicular variant%' OR n._v LIKE '%minimally invasive%'
             OR n._v LIKE '%follicular growth%' OR n._v LIKE '%macrofollicular%'
                                                                           THEN 'follicular_variant_encapsulated'
        WHEN n._v LIKE '%oncocytic%' OR n._v LIKE '%onocytic%' OR n._v LIKE '%hurthle%'
             OR n._v LIKE '%hurthel%' OR n._v = 'oxyphilic' OR n._v LIKE 'oxyphilic %'
                                                                           THEN 'oncocytic'
        WHEN n._v LIKE '%insular%'                                         THEN 'insular'
        WHEN n._v LIKE '%microcarcinoma%' OR n._v LIKE '%microcarcioma%'
             OR n._v LIKE '%micorcarcinoma%' OR n._v LIKE '%microcarinoma%'
             OR n._v LIKE '%microcaarcinoma%' OR n._v LIKE '%microcaricnoma%'
             OR n._v LIKE '%microcarcinooma%'
                                                                           THEN 'microcarcinoma'
        WHEN n._v LIKE '%folliucalr%' OR n._v LIKE '%follicualr%'
                                                                           THEN 'follicular_variant_encapsulated'
        WHEN n._v LIKE '%collumnar%'                                       THEN 'columnar_cell'
        WHEN n._v LIKE '%classical%' OR n._v LIKE '%classic %' OR n._v = 'classic'
             OR n._v LIKE '%classsical%' OR n._v = 'ptc'
                                                                           THEN 'classical'
        ELSE 'other'
    END AS histology_variant_clean
FROM norm n;

-- ---------------------------------------------------------------------------
-- Cleanup pass
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.histology_variant IS
'RAW FIELD — free-text prose with ~180 distinct values, typos, multi-line entries, and size qualifiers (microcarcinoma). Use manuscript_workspace.canonical_path_malignant_events_v1_variant_clean.histology_variant_clean for analysis (15 controlled buckets). Dim in manuscript_workspace.dim_histology_variant_v1. PATH04 resolved 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_12';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.histology_variant','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_variant_clean',
   'PATH04','prompt_12','column_only',DATE '2026-04-23',
   '~180 distinct raw values: prose, multi-line, typos, size qualifiers mixed with variant labels.',
   NULL,
   'Clean view provides 15 controlled buckets + dim_histology_variant_v1. No queue — normalization IS the resolution. 2 buckets added beyond prompt spec (microcarcinoma, insular) for clinical fidelity.');
