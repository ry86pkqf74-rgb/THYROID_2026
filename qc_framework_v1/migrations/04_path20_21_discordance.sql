-- ============================================================================
-- Migration 04 — PATH20 / PATH21: rebuild discordance flags (T-stage + laterality)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH20 (broken discordance_t_stage_flag), PATH21 (dormant discordance_laterality_flag)
-- Author:        Logan Glosser
-- Date:          2026-04-22
-- ----------------------------------------------------------------------------
-- Pre-state on main.canonical_path_malignant_events_v1 (6,689 rows):
--   discordance_t_stage_flag   : 3,152 TRUE / 511 FALSE / 3,026 NULL   (47% TRUE — broken)
--   discordance_laterality_flag: 0 TRUE / 6,689 NULL                   (reserved, never fired)
--
-- Upstream context (Logan's scripts 390-399, 2026-04-22/23):
--   * Scripts 390-399 operated on CPM (patient-level) stage columns only.
--   * They did NOT touch main.canonical_path_malignant_events_v1 (per-event).
--   * The per-event t_stage_ajcc8 column is still the path-report's own
--     AJCC8 T-assignment — this is what PATH20 validates for self-
--     consistency against size + ETE on the same row.
--   * The per-event discordance_t_stage_flag is sourced from
--     tumor_episode_master_v2.t_stage_discordance_flag, which conflates
--     size, ETE, and multifocality — column comment acknowledges this.
--   * No AJCC7/8 migration issue: AJCC8 thyroid T-category rules are
--     unified across DTC / MTC / ATC (size + ETE); tumor-type routing
--     matters for stage_group (Scripts 395-399 scope) and for AJCC7-ATC-
--     always-T4 (not in scope here). PATH20 is AJCC8-only and
--     tumor-type-agnostic at the T-level.
--
-- AJCC8 thyroid T-stage rule set (applied uniformly to DTC/MTC/ATC):
--   T1a  : size ≤ 1.0 cm,  no gross ETE
--   T1b  : 1.0 < size ≤ 2.0 cm, no gross ETE
--   T2   : 2.0 < size ≤ 4.0 cm, no gross ETE
--   T3a  : size > 4.0 cm, no gross ETE
--   T3b  : any size + gross ETE into strap muscles ONLY
--   T4a  : any size + gross ETE into subcut / larynx / trachea / esophagus / RLN
--   T4b  : any size + gross ETE into prevertebral fascia OR carotid /
--          mediastinal vessel encasement
--   (TX / NULL if size AND ete both missing)
--
-- Limit: T3b vs T4a vs T4b requires narrative parsing of ETE target tissue
-- from op note / gross / microscopic. Current structured columns only give
-- us `gross_ete` (BIGINT, 1 or NULL) and `extrathyroidal_extension` (35
-- free-text tokens) — no structured tissue target. See
-- qc_framework_v1/LLM_TODO.md item #1. Until that LLM pass lands, gross-ETE
-- rows collapse to `indeterminate_t3b_t4a_t4b_requires_llm`.
--
-- Discordance contract:
--   discordance_t_stage_flag   : TRUE  if reported t_stage_ajcc8 conflicts with
--                                  derived_t_stage_ajcc8 (both non-NULL, derived
--                                  not 'indeterminate_*', not 'not_applicable').
--                                FALSE if they agree.
--                                NULL  if either side is missing OR derived is
--                                  indeterminate/not_applicable (honestly unknown).
--   discordance_laterality_flag: TRUE  when laterality ∈ {left,right} AND `site`
--                                  mentions the opposite side (classic
--                                  laterality↔site contradiction).
--                                FALSE when laterality and site agree or site
--                                  missing / unparseable.
--                                NULL  when laterality itself is NULL/empty.
--
-- Histology bucket (for future use + queue-triage reason; does NOT change
-- the AJCC8 T-category rule applied):
--   DTC  : PTC, FTC, HCC, PDTC, DHGTC, DTC_NOS, NIFTP, FTUMP-borderline
--   MTC  : medullary carcinoma
--   ATC  : anaplastic / sarcomatoid
--   non-staged : NIFTP/FTUMP/other-benign (T-staging conceptually NA)
--   other : NUT, angiosarcoma, adenoid cystic, insular, etc.
--
-- Downstream: PATH22/PATH23 will consume derived_t_stage_ajcc8 to rebuild
-- overall_stage_ajcc8 per tumor type (DTC age-split, MTC, ATC-all-IV),
-- paralleling Logan's CPM-level Scripts 395-399.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.path_event_discordance_v1 AS
WITH row_base AS (
    SELECT
        research_id,
        surgery_episode_uid,
        path_surgery_id,
        tumor_ordinal,
        synoptic_row_ix,
        specimen_id,
        primary_histology,
        laterality,
        site,
        size_greatest_dimension_cm,
        extrathyroidal_extension,
        gross_ete,
        t_stage_ajcc8   AS reported_t_stage_ajcc8,
        t_stage_ajcc7   AS reported_t_stage_ajcc7
    FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed
),

-- ------------------------------------------------------------------------
-- Normalize histology into coarse staging bucket
-- (DTC/MTC/ATC all share AJCC8 T-rules; NIFTP/FTUMP/other route to non-staged)
-- ------------------------------------------------------------------------
row_hist AS (
    SELECT b.*,
        CASE
            WHEN LOWER(TRIM(COALESCE(primary_histology,''))) IN ('mtc','medullary')
              OR LOWER(primary_histology) LIKE '%medullary%'                       THEN 'MTC'
            WHEN LOWER(primary_histology) LIKE '%anaplastic%'
              OR LOWER(primary_histology) LIKE '%sarcomatoid%'                     THEN 'ATC'
            WHEN LOWER(primary_histology) LIKE '%niftp%'                           THEN 'non_staged'
            WHEN LOWER(primary_histology) LIKE '%ftump%'
              OR LOWER(primary_histology) LIKE '%uncertain malignant potential%'   THEN 'non_staged'
            WHEN LOWER(primary_histology) LIKE '%follicular adenoma%'
              OR LOWER(primary_histology) LIKE '%atypical follicular adenoma%'
              OR LOWER(primary_histology) LIKE '%atypical hurthle cell neoplasm%'
              OR LOWER(primary_histology) LIKE '%hurthle cell adenoma%'            THEN 'non_staged'
            WHEN LOWER(primary_histology) LIKE '%ptc%'
              OR LOWER(primary_histology) LIKE '%papillary%'
              OR LOWER(primary_histology) LIKE '%follicular car%'
              OR LOWER(primary_histology) LIKE '%follicular caric%'
              OR LOWER(primary_histology) LIKE '%hurthle%'
              OR LOWER(primary_histology) LIKE '%hürthle%'
              OR LOWER(primary_histology) LIKE '%oncocytic%'
              OR LOWER(primary_histology) LIKE '%poorly differentiated%'
              OR LOWER(primary_histology) LIKE '%pdtc%'
              OR LOWER(primary_histology) LIKE '%differentiated high grade%'
              OR LOWER(primary_histology) LIKE '%dhgtc%'
              OR LOWER(primary_histology) LIKE '%differentiated thyroid%'          THEN 'DTC'
            WHEN primary_histology IS NULL OR TRIM(primary_histology)=''           THEN 'unknown_histology'
            ELSE                                                                        'other'
        END AS histology_bucket
    FROM row_base b
),

-- ------------------------------------------------------------------------
-- Normalize ETE free text (35 variants) into a compact vocabulary
-- Aligned with Script 390 (Rule A) / Script 392 (boolean-string normalization).
--   present  : any affirmative ETE mention (incl. 'minimal', 'microscopic',
--              'focal', 'extensive', 'present', 'yes', 'true')
--   absent   : explicit negatives ('x', 'false', 'n/a', 'no', 'none')
--              and the legacy sentinel 'x' (5,069 rows)
--   unknown  : NULL / empty / 'indeterminate'
--   other    : free-text fragments that don't pattern-match (routes to queue)
-- ------------------------------------------------------------------------
row_ete AS (
    SELECT rh.*,
        CASE
            WHEN extrathyroidal_extension IS NULL
              OR TRIM(extrathyroidal_extension) = ''                              THEN 'unknown'
            WHEN LOWER(TRIM(extrathyroidal_extension)) IN
                 ('x','`x','false','no','n/a','none','absent')                    THEN 'absent'
            WHEN LOWER(extrathyroidal_extension) LIKE '%indetermin%'              THEN 'unknown'
            WHEN LOWER(extrathyroidal_extension) LIKE '%present%'
              OR LOWER(extrathyroidal_extension) LIKE '%minimal%'
              OR LOWER(extrathyroidal_extension) LIKE '%microscop%'
              OR LOWER(extrathyroidal_extension) LIKE '%focal%'
              OR LOWER(extrathyroidal_extension) LIKE '%extensi%'
              OR LOWER(extrathyroidal_extension) LIKE '%extesive%'
              OR LOWER(extrathyroidal_extension) LIKE 'yes%'
              OR LOWER(extrathyroidal_extension) LIKE '%true%'
              OR LOWER(extrathyroidal_extension) LIKE '%extension%'
              OR LOWER(extrathyroidal_extension) LIKE '%perithyroidal%'           THEN 'present'
            ELSE                                                                       'other'
        END AS ete_norm
    FROM row_hist rh
),

-- ------------------------------------------------------------------------
-- Collapse to an "effective gross ETE present" boolean.
-- Trust either the `gross_ete=1` BIGINT flag (direct) OR ete_norm='present'.
-- Treat ete_norm='other' as unknown (don't force TRUE or FALSE).
-- ------------------------------------------------------------------------
row_ete_effective AS (
    SELECT re.*,
        CASE
            WHEN gross_ete = 1                    THEN TRUE
            WHEN ete_norm = 'present'             THEN TRUE
            WHEN ete_norm = 'absent'              THEN FALSE
            ELSE                                       NULL  -- unknown/other
        END AS gross_ete_effective
    FROM row_ete re
),

-- ------------------------------------------------------------------------
-- Derive T-stage (AJCC8, DTC/MTC/ATC-unified) from size + gross_ete_effective.
-- T3b vs T4a vs T4b cannot be distinguished from current columns
-- (no structured invasion target) — collapsed to `indeterminate_t3b_t4a_t4b_requires_llm`.
-- See qc_framework_v1/LLM_TODO.md item #1.
-- non_staged histology → derived = 'not_applicable' (staging conceptually NA).
-- other histology       → derived = 'unknown_histology_staging' (route to queue).
-- Missing size AND unknown ETE → derived = NULL (TX).
-- ------------------------------------------------------------------------
row_derived AS (
    SELECT ref.*,
        CASE
            WHEN histology_bucket = 'non_staged'                                  THEN 'not_applicable'
            WHEN histology_bucket = 'other'                                       THEN 'unknown_histology_staging'
            WHEN histology_bucket = 'unknown_histology'                           THEN NULL
            WHEN gross_ete_effective IS TRUE                                      THEN 'indeterminate_t3b_t4a_t4b_requires_llm'
            WHEN gross_ete_effective IS FALSE
             AND size_greatest_dimension_cm IS NOT NULL
             AND size_greatest_dimension_cm <= 1.0                                THEN 'T1a'
            WHEN gross_ete_effective IS FALSE
             AND size_greatest_dimension_cm > 1.0
             AND size_greatest_dimension_cm <= 2.0                                THEN 'T1b'
            WHEN gross_ete_effective IS FALSE
             AND size_greatest_dimension_cm > 2.0
             AND size_greatest_dimension_cm <= 4.0                                THEN 'T2'
            WHEN gross_ete_effective IS FALSE
             AND size_greatest_dimension_cm > 4.0                                 THEN 'T3a'
            -- unknown ETE with size → best-effort but flag indeterminate at T3a boundary
            WHEN gross_ete_effective IS NULL
             AND size_greatest_dimension_cm IS NOT NULL
             AND size_greatest_dimension_cm <= 1.0                                THEN 'T1a_assumes_no_ete'
            WHEN gross_ete_effective IS NULL
             AND size_greatest_dimension_cm IS NOT NULL
             AND size_greatest_dimension_cm > 1.0
             AND size_greatest_dimension_cm <= 2.0                                THEN 'T1b_assumes_no_ete'
            WHEN gross_ete_effective IS NULL
             AND size_greatest_dimension_cm IS NOT NULL
             AND size_greatest_dimension_cm > 2.0
             AND size_greatest_dimension_cm <= 4.0                                THEN 'T2_assumes_no_ete'
            WHEN gross_ete_effective IS NULL
             AND size_greatest_dimension_cm IS NOT NULL
             AND size_greatest_dimension_cm > 4.0                                 THEN 'T3a_assumes_no_ete'
            ELSE                                                                        NULL  -- TX
        END AS derived_t_stage_ajcc8
    FROM row_ete_effective ref
),

-- ------------------------------------------------------------------------
-- Laterality normalization (matches migration 03 pattern)
-- ------------------------------------------------------------------------
row_lat AS (
    SELECT rd.*,
        CASE
            WHEN laterality IS NULL OR TRIM(laterality) = ''       THEN NULL
            WHEN LOWER(TRIM(laterality)) = 'bilateral'             THEN 'bilateral'
            WHEN LOWER(laterality) LIKE '%left%'
             AND LOWER(laterality) LIKE '%right%'                  THEN 'bilateral'
            WHEN LOWER(laterality) LIKE '%left%'                   THEN 'left'
            WHEN LOWER(laterality) LIKE '%right%'                  THEN 'right'
            WHEN LOWER(laterality) LIKE '%isthmus%'                THEN 'isthmus'
            ELSE                                                        'other'
        END AS lat_norm,
        CASE
            WHEN site IS NULL OR TRIM(site) = ''                   THEN NULL
            WHEN LOWER(site) LIKE '%left%'
             AND LOWER(site) LIKE '%right%'                        THEN 'bilateral'
            WHEN LOWER(site) LIKE '%left%'                         THEN 'left'
            WHEN LOWER(site) LIKE '%right%'                        THEN 'right'
            WHEN LOWER(site) LIKE '%isthmus%'                      THEN 'isthmus'
            ELSE                                                        'other'
        END AS site_norm
    FROM row_derived rd
)

-- ========================================================================
-- Final projection + discordance flags
-- ========================================================================
SELECT
    research_id,
    surgery_episode_uid,
    path_surgery_id,
    tumor_ordinal,
    synoptic_row_ix,
    specimen_id,
    primary_histology,
    histology_bucket,
    size_greatest_dimension_cm,
    extrathyroidal_extension                                AS ete_raw,
    ete_norm,
    gross_ete                                               AS gross_ete_raw_flag,
    gross_ete_effective,
    laterality                                              AS laterality_raw,
    lat_norm,
    site                                                    AS site_raw,
    site_norm,
    reported_t_stage_ajcc7,
    reported_t_stage_ajcc8,
    derived_t_stage_ajcc8,

    -- ------------------------------------------------------------------
    -- discordance_t_stage_flag (AJCC8, rebuilt)
    -- TRUE only when both sides are decisive and disagree.
    -- NULL when derived is indeterminate, not_applicable, unknown_histology_staging,
    -- or when either side is missing / "_assumes_no_ete".
    -- ------------------------------------------------------------------
    CASE
        WHEN reported_t_stage_ajcc8 IS NULL
          OR derived_t_stage_ajcc8  IS NULL                              THEN NULL
        WHEN derived_t_stage_ajcc8 IN ('not_applicable',
                                       'unknown_histology_staging',
                                       'indeterminate_t3b_t4a_t4b_requires_llm',
                                       'T1a_assumes_no_ete',
                                       'T1b_assumes_no_ete',
                                       'T2_assumes_no_ete',
                                       'T3a_assumes_no_ete')             THEN NULL
        WHEN derived_t_stage_ajcc8 = reported_t_stage_ajcc8               THEN FALSE
        ELSE                                                                   TRUE
    END                                                     AS discordance_t_stage_flag,

    -- Textual reason for NULL/TRUE — aids triage
    CASE
        WHEN reported_t_stage_ajcc8 IS NULL                               THEN 'reported_t_stage_ajcc8_missing'
        WHEN derived_t_stage_ajcc8 IS NULL                                THEN 'size_and_ete_both_missing'
        WHEN derived_t_stage_ajcc8 = 'not_applicable'                     THEN 'histology_not_staged'
        WHEN derived_t_stage_ajcc8 = 'unknown_histology_staging'          THEN 'unknown_histology_bucket'
        WHEN derived_t_stage_ajcc8 = 'indeterminate_t3b_t4a_t4b_requires_llm'
                                                                          THEN 'gross_ete_present_t4b_requires_llm'
        WHEN derived_t_stage_ajcc8 LIKE '%_assumes_no_ete'                THEN 'ete_unknown_derived_with_assumption'
        WHEN derived_t_stage_ajcc8 = reported_t_stage_ajcc8               THEN 'concordant'
        ELSE                                                                   'reported_neq_derived'
    END                                                     AS t_stage_derivation_note,

    -- ------------------------------------------------------------------
    -- discordance_laterality_flag (PATH21, rebuilt)
    -- Fires when laterality is a definite side AND site names the opposite side.
    -- Bilateral laterality with either-side site is NOT a contradiction.
    -- Isthmus is not a side — treated as NULL for this check.
    -- ------------------------------------------------------------------
    CASE
        WHEN lat_norm IS NULL OR lat_norm = 'bilateral' OR lat_norm = 'isthmus' OR lat_norm = 'other'
                                                                          THEN NULL
        WHEN site_norm IS NULL OR site_norm IN ('isthmus','other')        THEN NULL
        WHEN lat_norm = 'left'  AND site_norm = 'right'                   THEN TRUE
        WHEN lat_norm = 'right' AND site_norm = 'left'                    THEN TRUE
        WHEN lat_norm = 'left'  AND site_norm = 'bilateral'               THEN TRUE
        WHEN lat_norm = 'right' AND site_norm = 'bilateral'               THEN TRUE
        ELSE                                                                   FALSE
    END                                                     AS discordance_laterality_flag
FROM row_lat;


-- ===========================================================================
-- Queue emission: rows where rebuilt flags reveal a clinical contradiction
-- Idempotent via NOT EXISTS on (issue_id, source_table, source_pk).
-- PATH20 queue: T-stage discordance (rebuilt TRUE).
-- PATH21 queue: laterality↔site contradiction (rebuilt TRUE).
-- ===========================================================================

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'PATH20'                                                     AS issue_id,
    d.research_id                                                AS research_id,
    'main.canonical_path_malignant_events_v1'                    AS source_table,
    CAST(d.path_surgery_id AS VARCHAR)                           AS source_pk,
    TO_JSON(struct_pack(
        tumor_ordinal              := d.tumor_ordinal,
        primary_histology          := d.primary_histology,
        histology_bucket           := d.histology_bucket,
        size_cm                    := d.size_greatest_dimension_cm,
        ete_raw                    := d.ete_raw,
        gross_ete_effective        := d.gross_ete_effective,
        reported_t_stage_ajcc8     := d.reported_t_stage_ajcc8,
        derived_t_stage_ajcc8      := d.derived_t_stage_ajcc8,
        note                       := d.t_stage_derivation_note
    ))                                                           AS context_json,
    'AJCC8 T-stage on path_malignant row disagrees with size + gross_ete derivation' AS reason
FROM manuscript_workspace.path_event_discordance_v1 d
WHERE d.discordance_t_stage_flag = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM manuscript_workspace.qc_manual_review_queue_v1 q
      WHERE q.issue_id     = 'PATH20'
        AND q.source_table = 'main.canonical_path_malignant_events_v1'
        AND q.source_pk    = CAST(d.path_surgery_id AS VARCHAR)
  );


INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'PATH21'                                                     AS issue_id,
    d.research_id                                                AS research_id,
    'main.canonical_path_malignant_events_v1'                    AS source_table,
    CAST(d.path_surgery_id AS VARCHAR)                           AS source_pk,
    TO_JSON(struct_pack(
        tumor_ordinal  := d.tumor_ordinal,
        laterality_raw := d.laterality_raw,
        lat_norm       := d.lat_norm,
        site_raw       := d.site_raw,
        site_norm      := d.site_norm
    ))                                                           AS context_json,
    'Laterality names one side but site text names the opposite side (or bilateral)' AS reason
FROM manuscript_workspace.path_event_discordance_v1 d
WHERE d.discordance_laterality_flag = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM manuscript_workspace.qc_manual_review_queue_v1 q
      WHERE q.issue_id     = 'PATH21'
        AND q.source_table = 'main.canonical_path_malignant_events_v1'
        AND q.source_pk    = CAST(d.path_surgery_id AS VARCHAR)
  );
