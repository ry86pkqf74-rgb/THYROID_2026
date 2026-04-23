-- ============================================================================
-- Migration 19 — PATH17/18: normalize laterality, detect site↔laterality conflict
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH17 (laterality↔site contradictions)
--                PATH18 (laterality field contains site-prose)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Both laterality and site columns carry mixed structured + prose content:
--   laterality has 79 distinct values incl. "right lobe", "right neck soft tissue",
--     "Thyroglossal duct cyst excision", "2.6", "c/a", etc.
--   site has similar pollution across its 100+ distinct values.
--
-- Strategy: parse EACH field independently to controlled tokens:
--   {left, right, bilateral, isthmus, extra_thyroidal, nonlobular, NULL}
-- Then cross-check.
--
-- Token rules (ordered — most-specific wins):
--   bilateral  → text contains BOTH 'right' AND 'left' (multi-line variants)
--                OR text = 'bilateral'
--   right      → contains 'right' or 'rigth' (typo) but NOT 'left'
--   left       → contains 'left' or 'let' (typo) or 'lobectomy' but NOT 'right'
--   isthmus    → contains 'isthmus' or 'isthmsu' (typo) or 'isthus' typo only
--   extra_thyroidal → neck mass, soft tissue, paratracheal, mediastinal, spine,
--                     thyroglossal, suprasternal, pyramidal, thyroid bed, fossa
--   nonlobular → c/a, n/s, numeric, other non-interpretable
--   NULL       → NULL or empty
--
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean
--     + laterality_token, site_token, derived_laterality_final,
--       site_laterality_contradict_flag (PATH17),
--       laterality_has_site_prose_flag (PATH18)
--
-- Queue: rows where contradict_flag OR site_prose_flag → PATH17 or PATH18.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean AS
WITH norm AS (
    SELECT e.*,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.laterality, ''), '\s+', ' ', 'g'))) AS _lat,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(e.site, ''),       '\s+', ' ', 'g'))) AS _site
    FROM main.canonical_path_malignant_events_v1 e
),
toks AS (
    SELECT
        n.*,
        CASE
            WHEN n._lat = '' OR n._lat IS NULL                                   THEN NULL
            WHEN n._lat = 'bilateral'                                            THEN 'bilateral'
            WHEN (n._lat LIKE '%right%' OR n._lat LIKE '%rigth%')
             AND (n._lat LIKE '%left%' OR n._lat LIKE '%let lobe%')              THEN 'bilateral'
            WHEN n._lat LIKE '%right%' OR n._lat LIKE '%rigth%'                  THEN 'right'
            WHEN n._lat LIKE '%left%' OR n._lat LIKE '%let lobe%'
              OR n._lat LIKE '%lobectomy%'                                       THEN 'left'
            WHEN n._lat LIKE '%isthmus%' OR n._lat LIKE '%isthmsu%'
              OR n._lat LIKE '%isthus%'                                          THEN 'isthmus'
            WHEN n._lat LIKE '%neck%' OR n._lat LIKE '%soft tissue%'
              OR n._lat LIKE '%paratracheal%' OR n._lat LIKE '%mediastinal%'
              OR n._lat LIKE '%thyroglossal%' OR n._lat LIKE '%pyramidal%'
              OR n._lat LIKE '%suprasternal%' OR n._lat LIKE '%thyroid bed%'
              OR n._lat LIKE '%thyroid fossa%' OR n._lat LIKE '%spine%'
              OR n._lat LIKE '%retropharyngeal%' OR n._lat LIKE '%anterior neck%'
              OR n._lat LIKE '%pretracheal%' OR n._lat LIKE '%midline%'
              OR n._lat LIKE '%ectopic%' OR n._lat LIKE '%remnant%'
              OR n._lat LIKE '%mass%' OR n._lat LIKE '%trachea%'
              OR n._lat LIKE '%clavicular%' OR n._lat LIKE '%residual%'
              OR n._lat LIKE '%level %'                                          THEN 'extra_thyroidal'
            ELSE 'nonlobular'
        END AS laterality_token,

        CASE
            WHEN n._site = '' OR n._site IS NULL                                 THEN NULL
            WHEN n._site IN ('ll','l l')                                         THEN 'left'
            WHEN n._site IN ('rl','r l')                                         THEN 'right'
            WHEN (n._site LIKE '%right%' OR n._site LIKE '%rigth%')
             AND (n._site LIKE '%left%' OR n._site LIKE '%let lobe%')            THEN 'bilateral'
            WHEN n._site LIKE '%right%' OR n._site LIKE '%rigth%'                THEN 'right'
            WHEN n._site LIKE '%left%' OR n._site LIKE '%let lobe%'
              OR n._site LIKE '%leeft%' OR n._site LIKE 'lt lobe%'               THEN 'left'
            WHEN n._site LIKE '%isthmus%' OR n._site LIKE '%isthmsu%'
              OR n._site LIKE '%isthus%'                                         THEN 'isthmus'
            WHEN n._site LIKE '%neck%' OR n._site LIKE '%soft tissue%'
              OR n._site LIKE '%paratracheal%' OR n._site LIKE '%mediastinal%'
              OR n._site LIKE '%thyroglossal%' OR n._site LIKE '%pyramidal%'
              OR n._site LIKE '%suprasternal%' OR n._site LIKE '%thyroid bed%'
              OR n._site LIKE '%thyroid fossa%' OR n._site LIKE '%remnant%'
              OR n._site LIKE '%ectopic%' OR n._site LIKE '%mass%'
              OR n._site LIKE '%level %' OR n._site LIKE '%retropharyngeal%'
              OR n._site LIKE '%pretracheal%' OR n._site LIKE '%midline%'
              OR n._site LIKE '%spine%' OR n._site LIKE '%clavicular%'
              OR n._site LIKE '%trachea%' OR n._site LIKE '%anterior%'           THEN 'extra_thyroidal'
            ELSE 'nonlobular'
        END AS site_token
    FROM norm n
)
SELECT
    t.* EXCLUDE (_lat, _site),
    t.laterality AS laterality_raw,
    t.site       AS site_raw,
    -- final derived laterality: prefer site_token when lateralit_token is
    -- extra_thyroidal/nonlobular (i.e. laterality was hijacked by site prose)
    COALESCE(
        CASE
            WHEN t.laterality_token IN ('left','right','bilateral','isthmus')
                THEN t.laterality_token
            WHEN t.site_token IN ('left','right','bilateral','isthmus')
                THEN t.site_token
            ELSE COALESCE(t.laterality_token, t.site_token)
        END,
        NULL
    ) AS derived_laterality_final,

    -- PATH17: real contradiction = one side says left, the other says right.
    -- bilateral-vs-{right|left|isthmus} is NOT a contradiction (bilateral = patient
    -- has tumors on both sides; site refers to current tumor's specific side).
    -- {left|right}-vs-isthmus is also not a hard contradiction (could be adjacent).
    ((t.laterality_token = 'left'  AND t.site_token = 'right')
     OR (t.laterality_token = 'right' AND t.site_token = 'left')
    ) AS site_laterality_contradict_flag,

    -- PATH18: laterality field is site-prose (extra-thyroidal) rather than
    -- a laterality token
    (t.laterality_token IN ('extra_thyroidal','nonlobular')
     AND t.site_token IS NOT NULL)        AS laterality_has_site_prose_flag
FROM toks t;

-- Idempotent queue
DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('PATH17','PATH18');

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    CASE WHEN site_laterality_contradict_flag THEN 'PATH17' ELSE 'PATH18' END,
    CAST(research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|',
        CAST(research_id AS VARCHAR),
        CAST(surgery_date AS VARCHAR),
        CAST(tumor_ordinal AS VARCHAR),
        CAST(COALESCE(specimen_id, '') AS VARCHAR)
    ),
    TO_JSON(struct_pack(
        laterality_raw := laterality_raw,
        site_raw := site_raw,
        laterality_token := laterality_token,
        site_token := site_token,
        derived_laterality_final := derived_laterality_final
    )),
    CASE
        WHEN site_laterality_contradict_flag
            THEN CONCAT('Laterality token (', laterality_token, ') ≠ site token (', site_token, ')')
        ELSE CONCAT('Laterality field is site-prose (token=', laterality_token, '); real laterality in site (token=', COALESCE(site_token, 'NULL'), ')')
    END,
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean
WHERE site_laterality_contradict_flag OR laterality_has_site_prose_flag;

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.laterality IS
'RAW FIELD — 79 distinct values, polluted with site prose (neck mass, soft tissue, etc.). Use manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean.derived_laterality_final for normalized laterality. PATH17/18 resolved 2026-04-23.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.site IS
'RAW FIELD — 100+ distinct values, mixes lobe/isthmus with extra-thyroidal site prose. Use manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean.site_token. PATH17/18 resolved 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_18';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.laterality','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean',
   'PATH17,PATH18','prompt_18','column_only',DATE '2026-04-23',
   '79 distinct raws mixing structured laterality with site prose.',
   NULL,
   'laterality_token + derived_laterality_final in clean view; contradictions and site-prose hijacks queued under PATH17/18.'),
  ('main.canonical_path_malignant_events_v1.site','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean',
   'PATH17,PATH18','prompt_18','column_only',DATE '2026-04-23',
   '100+ distinct raws with lobe/isthmus/extra-thyroidal site prose mixed.',
   NULL,
   'site_token in clean view (left/right/bilateral/isthmus/extra_thyroidal/nonlobular).');
