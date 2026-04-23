-- ============================================================================
-- Migration 12 — PATH02 / PATH19 / PATH03: normalize primary_histology on
-- canonical_path_malignant_events_v1 + queue non-malignant buckets.
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH02 (primary_histology non-normalized),
--                PATH19 (metastatic/recurrent prefix collapsed in with histology),
--                PATH03 (benign/borderline/UMP in malignant table).
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.canonical_path_malignant_events_v1:
--   6,689 rows / 0 NULL / 101 distinct primary_histology values.
--   Top raws: PTC 5,068, follicular carcinoma 545, MTC 230, NIFTP 212,
--             metastatic PTC 190, "PTC " 88, Follicular carcinoma 63,
--             FTUMP 49, poorly differentiated 30, anaplastic 20, ...
--   Typos: metastatitic PTC (1), pooly differentiated (1), pTC (3), *PTC (1),
--          "**THYROID bIOPSY\nAnaplastic carcinoma" (1), paillary (absent here
--          but carried into the CASE for parity with migration 08).
--   Metastatic/recurrent prefix (PATH19): ~260 rows once case-insensitive.
--
-- Pre-state PATH03 scope (LIKE adenoma / UMP / ftump / niftp):
--   NIFTP 214, FTUMP 50, UMP_strings 6, adenoma_strings 6 = 276 rows / 250 pts.
--   (Registry quoted 62 rows — stale / counted a narrower subset pre-NIFTP.)
-- ----------------------------------------------------------------------------
-- Design:
--   Reuse the migration-08 controlled-vocab CASE verbatim so path events and
--   cohort_v1 normalize under the same dictionary (call-site symmetry —
--   downstream joins on histology won't drift).
--
--   Three derived columns:
--     primary_histology_clean        — controlled vocab bucket.
--     histology_metastatic_flag      — raw starts with 'metastatic ' (case-ins).
--     histology_recurrent_flag       — raw starts with 'recurrent ' (case-ins).
--     primary_histology_raw          — alias of original (audit).
--
--   Queue policy (PATH03):
--     Rows whose clean bucket is {NIFTP, FTUMP, follicular adenoma,
--     atypical follicular adenoma, uncertain malignant potential strings}
--     are emitted. Bucket tagged in context_json so downstream cohort_v2
--     can drop per-bucket.
--
--   source_pk strategy (same as migration 10):
--     CONCAT(research_id, surgery_episode_id, path_surgery_id, specimen_id,
--     specimen_focus_id) with COALESCE 'NULL' — deterministic, NULL-safe,
--     idempotent under NOT EXISTS.
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_histology_clean
--   (VIEW). Queue: PATH03 inserts into qc_manual_review_queue_v1.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_histology_clean AS
WITH base AS (
    SELECT
        e.*,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.primary_histology, ''), '\s+', ' ', 'g')))
            AS _hist_trim
    FROM main.canonical_path_malignant_events_v1 e
),
stripped AS (
    SELECT
        b.*,
        TRIM(REGEXP_REPLACE(
            _hist_trim,
            '^(metastatic/recurrent|recurrent/metastatic|metastatic|recurrent)\s+',
            '', 'g'
        )) AS _hist_met_stripped
    FROM base b
)
SELECT
    s.* EXCLUDE (_hist_trim, _hist_met_stripped),
    s.primary_histology AS primary_histology_raw,
    CASE
        WHEN s._hist_trim = '' OR s._hist_trim IS NULL THEN NULL
        WHEN s._hist_met_stripped = 'ptc'
             OR s._hist_met_stripped LIKE 'ptc %'
             OR s._hist_met_stripped LIKE 'ptc/%'
             OR s._hist_met_stripped LIKE 'high-grade ptc%'
             OR s._hist_met_stripped LIKE '%ptc tall%'
             OR s._hist_met_stripped LIKE '%ptc classical%'
             OR s._hist_met_stripped LIKE '%ptc classic%'
             OR s._hist_met_stripped LIKE '%ptc follicular%'
             OR s._hist_met_stripped LIKE '%ptc with %'
             OR s._hist_met_stripped LIKE '%ptc?%'
             OR s._hist_met_stripped LIKE '%ptc high grade%'
             OR s._hist_met_stripped LIKE '%ptc microcarcinoma%'
             OR s._hist_met_stripped LIKE '%ptc nos%'
             OR s._hist_met_stripped LIKE '%ptc onocytic%'
             OR s._hist_met_stripped LIKE '%ptc calssical%' -- typo in source
             OR s._hist_met_stripped LIKE '*ptc%'
             OR s._hist_met_stripped LIKE 'metastatitic ptc%'
             OR s._hist_met_stripped LIKE 'poorly differentied ptc%'
             OR (s._hist_met_stripped LIKE '%papillary%' AND s._hist_met_stripped LIKE '%carcinoma%')
             OR s._hist_met_stripped LIKE '%paillary%'
             OR s._hist_met_stripped LIKE '**thyroid biopsy%ptc%'
             THEN 'papillary thyroid carcinoma'
        WHEN s._hist_met_stripped = 'mtc'
             OR s._hist_met_stripped LIKE 'mtc %'
             OR s._hist_met_stripped LIKE 'mtc/%'
             OR s._hist_met_stripped = 'medullary'
             OR (s._hist_met_stripped LIKE '%medullary%' AND s._hist_met_stripped LIKE '%carcinoma%')
             THEN 'medullary thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%anaplastic%'
             OR s._hist_met_stripped LIKE '**thyroid biopsy%anaplastic%'
             THEN 'anaplastic thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%hurthle cell carcinoma%'
             OR (s._hist_met_stripped LIKE '%hurthle%' AND s._hist_met_stripped LIKE '%carcinoma%')
             OR s._hist_met_stripped LIKE '%oncocytic%carcinoma%'
             THEN 'oncocytic thyroid carcinoma'
        WHEN s._hist_met_stripped = 'niftp' OR s._hist_met_stripped LIKE '%(niftp)%'
             OR s._hist_met_stripped LIKE '%niftp%'
             THEN 'NIFTP'
        WHEN s._hist_met_stripped = 'ftump' OR s._hist_met_stripped LIKE '%ft-ump%'
             OR s._hist_met_stripped LIKE '%ftump%'
             THEN 'FTUMP'
        WHEN s._hist_met_stripped LIKE '%atypical follicular%'
             OR s._hist_met_stripped LIKE '%atypical hurthle cell neoplasm%'
             THEN 'atypical follicular / hurthle neoplasm'
        WHEN s._hist_met_stripped LIKE '%follicular adenoma%'
             THEN 'follicular adenoma'
        WHEN s._hist_met_stripped LIKE '%uncertain malignant potential%'
             OR s._hist_met_stripped LIKE '%lesion of uncertain%'
             THEN 'uncertain malignant potential (non-FTUMP)'
        WHEN (s._hist_met_stripped LIKE '%follicular%'
              AND (s._hist_met_stripped LIKE '%carcinoma%'
                   OR s._hist_met_stripped LIKE '%caricinoma%')) -- 'caricinoma' typo
             THEN 'follicular thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%differentiated high grade%'
             THEN 'differentiated high grade thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%poorly differentiated%'
             OR s._hist_met_stripped LIKE '%pooly differentiated%'
             OR s._hist_met_stripped LIKE '%poorly differentied%'
             THEN 'poorly differentiated thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%neuroendocrine%'
             THEN 'poorly differentiated neuroendocrine carcinoma'
        WHEN s._hist_met_stripped LIKE '%insular carcinoma%'
             THEN 'insular carcinoma'
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
             OR s._hist_met_stripped LIKE '%high grade%carcinoma%'
             THEN 'high grade / squamous thyroid carcinoma'
        WHEN s._hist_met_stripped LIKE '%thyroid carcinoma%'
             OR s._hist_met_stripped LIKE '%carcinoma%'
             THEN 'thyroid carcinoma unspecified'
        ELSE s._hist_met_stripped
    END AS primary_histology_clean,
    (s._hist_trim LIKE 'metastatic %')                                 AS histology_metastatic_flag,
    (s._hist_trim LIKE 'recurrent %' OR s._hist_trim LIKE 'recurrent/%') AS histology_recurrent_flag
FROM stripped s;

-- ---------------------------------------------------------------------------
-- QC queue emission (idempotent) — PATH03 (non-malignant / borderline / UMP)
-- ---------------------------------------------------------------------------

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'PATH03',
    v.research_id,
    'canonical_path_malignant_events_v1',
    CONCAT(
        COALESCE(CAST(v.research_id       AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.surgery_episode_id AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.path_surgery_id   AS VARCHAR), 'NULL'), '|',
        COALESCE(v.specimen_id, 'NULL'),                        '|',
        COALESCE(v.specimen_focus_id, 'NULL')
    ),
    TO_JSON(struct_pack(
        primary_histology_raw   := v.primary_histology_raw,
        primary_histology_clean := v.primary_histology_clean,
        bucket                  := v.primary_histology_clean
    )),
    'PATH03: non-malignant / borderline / uncertain histology bucket present in the malignant events table (NIFTP / FTUMP / follicular adenoma / UMP). Reviewer decides whether to exclude from cohort_v2 or retain (e.g. NIFTP analyses)'
FROM manuscript_workspace.canonical_path_malignant_events_v1_histology_clean v
WHERE v.primary_histology_clean IN (
    'NIFTP','FTUMP','follicular adenoma',
    'atypical follicular / hurthle neoplasm',
    'uncertain malignant potential (non-FTUMP)'
)
AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
    WHERE q.issue_id = 'PATH03'
    AND q.source_table = 'canonical_path_malignant_events_v1'
    AND q.source_pk = CONCAT(
        COALESCE(CAST(v.research_id       AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.surgery_episode_id AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.path_surgery_id   AS VARCHAR), 'NULL'), '|',
        COALESCE(v.specimen_id, 'NULL'),                        '|',
        COALESCE(v.specimen_focus_id, 'NULL')
    )
);

-- ---------------------------------------------------------------------------
-- Cleanup pass (per the rhythm established in migration 09):
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.primary_histology IS
'RAW FIELD — not normalized. 101 distinct values (case, whitespace, typos, metastatic/recurrent prefixes, narrative). Use manuscript_workspace.canonical_path_malignant_events_v1_histology_clean for analysis: primary_histology_clean (controlled vocab), histology_metastatic_flag, histology_recurrent_flag. Non-malignant buckets (NIFTP/FTUMP/adenoma/UMP) queued as PATH03. PATH02/PATH19/PATH03 resolved 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1
WHERE closing_prompt = 'prompt_11';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.primary_histology',
   'column',
   'manuscript_workspace.canonical_path_malignant_events_v1_histology_clean',
   'PATH02/PATH19/PATH03',
   'prompt_11',
   'column_only',
   DATE '2026-04-23',
   '101 raw distinct values: case/whitespace/typos + metastatic/recurrent prefixes collapsed into the tumor-type field + 276 rows of non-malignant buckets (NIFTP/FTUMP/adenoma/UMP) mixed into the malignant table.',
   NULL,
   'Raw column retained for audit; use primary_histology_clean / histology_metastatic_flag / histology_recurrent_flag on the clean view. PATH03 queue emits the 276 non-malignant rows for chart-review decision on inclusion in cohort_v2.');
