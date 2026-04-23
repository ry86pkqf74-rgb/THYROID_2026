-- ============================================================================
-- Migration 10 — ETE01/ETE02: normalize extrathyroidal_extension + discordance
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     ETE01 (35 raw strings → controlled vocab),
--                ETE02 (gross_ete=1 paired with minimal/microscopic/present_unspecified)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.canonical_path_malignant_events_v1:
--   6,689 rows / 4,138 distinct patients.
--   extrathyroidal_extension: 35 distinct raw values. Top: 'x' 5069,
--     NULL 445, 'present' 365, 'minimal' 292, 'false' 201, 'microscopic' 120,
--     'c/a' 42, 'yes' 30, 'extensive' 25, 'focal' 23, 'indeterminate' 19,
--     'Yes;' 12, 'extesive' 8, 'true' 6, plus 22 low-count variants
--     including 'microscopiic', 'Yes;minimal;', 'yes (minimal)', 'yes (focal)',
--     'yes, extensive', 'minimal into fat', 'focal right side',
--     'x\n(single microscopic focus of extension)', '`x', '* (see margin comment)',
--     'present (microscopic perithyroidal soft tissue only with no clinical
--      or macroscopic evidence of invasion)',
--     'present (perithyroidal fibroadipose tissue involved)'.
--   gross_ete: BIGINT. 1,571 rows (900 pts) = 1; 5,118 rows (3,238 pts) NULL.
-- ----------------------------------------------------------------------------
-- Design:
--   Normalize pipeline:
--     _ete_norm = LOWER(TRIM(REGEXP_REPLACE(ete, '\s+',' ','g')))
--     — this collapses the "x\n(single microscopic focus of extension)"
--       newline into "x (single microscopic focus of extension)".
--
--   Controlled-vocab CASE (ordering matters — most-specific wins):
--     1. NULL/empty                                    → NULL
--     2. in {x, `x, c/a, n/a, indeterminate, * (see margin comment)}
--                                                      → NULL
--     3. contains 'extensive' OR 'extesive'            → extensive
--     4. contains 'gross'                              → gross
--     5. contains 'microscopic' OR 'microscopiic'      → microscopic
--        (catches "x (single microscopic focus of extension)" as microscopic,
--         overriding rule 2's bare 'x' → NULL; also reclassifies
--         "present (microscopic perithyroidal...)" to microscopic, which is
--         the clinically accurate grade)
--     6. contains 'minimal'                            → minimal
--     7. contains 'focal'                              → minimal
--        (focal == minimal per path convention — prompt literal)
--     8. in {false, no}                                → none
--     9. anything else starting with yes/present/true  → present_unspecified
--    10. otherwise                                     → NULL (audit fallback)
--
--   ete_grade_grouped: collapses minimal+microscopic → 'minimal_microscopic';
--     gross, extensive, present_unspecified, none, NULL passthrough.
--
--   ete_discordance_flag: TRUE when gross_ete=1 AND ete_grade IN
--     ('minimal','microscopic','present_unspecified'). This is the ETE02
--     contradiction the prompt targets.
--
-- Source-pk strategy for queue:
--   canonical_path_malignant_events_v1 has no single-column PK. Use
--   concatenated (research_id | surgery_episode_id | path_surgery_id |
--   specimen_id | specimen_focus_id) — deterministic, NULL-safe via COALESCE.
--   Ensures idempotent NOT EXISTS de-dup guard works across reruns.
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_ete_clean (view)
--   Queue inserts: ETE02 rows into manuscript_workspace.qc_manual_review_queue_v1
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_ete_clean AS
WITH norm AS (
    SELECT
        e.*,
        LOWER(TRIM(REGEXP_REPLACE(
            COALESCE(e.extrathyroidal_extension, ''), '\s+', ' ', 'g'
        ))) AS _ete_norm
    FROM main.canonical_path_malignant_events_v1 e
),
graded AS (
    SELECT
        n.*,
        CASE
            WHEN n._ete_norm = '' OR n._ete_norm IS NULL THEN NULL
            WHEN n._ete_norm LIKE '%extensive%' OR n._ete_norm LIKE '%extesive%'
                 THEN 'extensive'
            WHEN n._ete_norm LIKE '%gross%'
                 THEN 'gross'
            WHEN n._ete_norm LIKE '%microscopic%' OR n._ete_norm LIKE '%microscopiic%'
                 THEN 'microscopic'
            WHEN n._ete_norm LIKE '%minimal%'
                 THEN 'minimal'
            WHEN n._ete_norm LIKE '%focal%'
                 THEN 'minimal'
            WHEN n._ete_norm IN ('false','no')
                 THEN 'none'
            WHEN n._ete_norm IN ('x','`x','c/a','n/a','indeterminate','* (see margin comment)')
                 THEN NULL
            WHEN n._ete_norm LIKE 'yes%' OR n._ete_norm LIKE 'present%' OR n._ete_norm LIKE 'true%'
                 THEN 'present_unspecified'
            ELSE NULL
        END AS ete_grade
    FROM norm n
)
SELECT
    g.* EXCLUDE (_ete_norm),
    g.ete_grade,
    CASE
        WHEN g.ete_grade IN ('minimal','microscopic') THEN 'minimal_microscopic'
        ELSE g.ete_grade
    END AS ete_grade_grouped,
    (g.gross_ete = 1 AND g.ete_grade IN ('minimal','microscopic','present_unspecified'))
        AS ete_discordance_flag
FROM graded g;

-- ---------------------------------------------------------------------------
-- QC queue emission (idempotent) — ETE02
-- ---------------------------------------------------------------------------

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'ETE02' AS issue_id,
    v.research_id,
    'canonical_path_malignant_events_v1' AS source_table,
    CONCAT(
        COALESCE(CAST(v.research_id       AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.surgery_episode_id AS VARCHAR), 'NULL'), '|',
        COALESCE(CAST(v.path_surgery_id   AS VARCHAR), 'NULL'), '|',
        COALESCE(v.specimen_id, 'NULL'),                        '|',
        COALESCE(v.specimen_focus_id, 'NULL')
    ) AS source_pk,
    TO_JSON(struct_pack(
        extrathyroidal_extension_raw := v.extrathyroidal_extension,
        ete_grade                    := v.ete_grade,
        ete_grade_grouped            := v.ete_grade_grouped,
        gross_ete                    := v.gross_ete
    )) AS context_json,
    'ETE02: gross_ete=1 paired with ete_grade IN (minimal, microscopic, present_unspecified) — contradiction between structured gross flag and narrative grade' AS reason
FROM manuscript_workspace.canonical_path_malignant_events_v1_ete_clean v
WHERE v.ete_discordance_flag = TRUE
AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
    WHERE q.issue_id = 'ETE02'
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
--   Comment the raw column + log the deprecation. Queue is already visible.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.extrathyroidal_extension IS
'RAW FIELD — not normalized. 35 distinct values (case, whitespace, typos, narrative free-text). Use manuscript_workspace.canonical_path_malignant_events_v1_ete_clean for analysis: controlled ete_grade (none, minimal, microscopic, gross, extensive, present_unspecified, NULL), ete_grade_grouped (minimal_microscopic collapse), and ete_discordance_flag (ETE02). ETE01/02 resolved 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1
WHERE closing_prompt = 'prompt_09';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.extrathyroidal_extension',
   'column',
   'manuscript_workspace.canonical_path_malignant_events_v1_ete_clean',
   'ETE01/ETE02',
   'prompt_09',
   'column_only',
   DATE '2026-04-23',
   '35 distinct raw strings (case, whitespace, typos, narrative). No controlled vocab.',
   NULL,
   'Raw column retained for audit; use ete_grade / ete_grade_grouped / ete_discordance_flag from the clean view. ETE02 discordance rows queued in qc_manual_review_queue_v1.');
