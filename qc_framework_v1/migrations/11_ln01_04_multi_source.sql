-- ============================================================================
-- Migration 11 — LN01/02/03/04: multi-source LN architecture
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     LN01 (ln_positive_final > path_ln_examined_raw),
--                LN02 (positive > 0 with denominator 0/NULL),
--                LN03 (raw vs final LN disagreement),
--                LN04 (LN data missing on both cohort columns).
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state (measured on main.canonical_path_malignant_events_v1):
--   * 0 rows at row-grain violate ln_involved > ln_examined (LN01 row-level).
--   * 28 rows / 27 pts have ln_involved > 0 AND (ln_examined IS NULL OR 0)
--     (LN02 row-level).
--   * At patient-rollup grain (SUM per research_id):
--       LN01: 2 pts (SUM involved > SUM examined)
--       LN02: 19 pts (SUM involved > 0 AND SUM examined = 0)
--
-- Per-source coverage (distinct patients):
--   path_ln_any_nonnull = 3,999
--   us_ln_shell        = 4,077   (table is shell — USLN01; count rows only)
--   ct_pathologic      = 978
--   ct_suspicious      = 784
--   mri_pathologic (=1)=  73
--   mri_mentioned  (=1)= 185
--   clinical_any       = 1,643
--   clinical_pos_level = 974
-- ----------------------------------------------------------------------------
-- Architecture (per "Resolved decisions #2" — multi-source, never collapsed):
--   Create manuscript_workspace.ln_per_patient_multisource_v1 with per-source
--   columns AND availability flags — downstream analysts pick the source
--   appropriate to the analysis, and Table 1 reports N per source.
--
--   Column layout (grain = research_id):
--     ln_path_positive, ln_path_examined               (SUM on path events)
--     ln_us_suspicious_count                           (NULL — USLN01 shell)
--     ln_ct_suspicious_count, ln_ct_pathologic_count   (COUNT CT exams w/ flag)
--     ln_mri_suspicious_count                          (COUNT MRI w/ path LN)
--     ln_clinical_positive_flag                        (BOOL from rollup)
--     ln_data_available_{path,us,ct,mri,clinical}      (BOOL availability)
--     ln_path_ln01_rollup_flag                         (QC: pos > exam @ rollup)
--     ln_path_ln02_rollup_flag                         (QC: pos>0, exam=0 or NULL)
--
-- Type hygiene:
--   canonical_path_malignant_events_v1.research_id       INTEGER
--   canonical_us_lymph_node_v2.research_id                INTEGER
--   ct_imaging.research_id                                VARCHAR
--   mri_imaging.research_id                               VARCHAR
--   canonical_cervical_ln_clinical_patient_rollup_v1      VARCHAR
--   → cast INTEGER-keyed sources to VARCHAR at CTE edge for clean LEFT JOINs.
--
-- USLN01 treatment:
--   canonical_us_lymph_node_v2 is entirely shell (no LN measurements).
--   ln_us_suspicious_count is reserved at NULL until USLN01 rebuild;
--   ln_data_available_us = TRUE iff any us_ln row exists (tells downstream
--   "exam happened, measurement missing — not absent").
--
-- Queue emission (idempotent, NOT EXISTS guard, source_pk = research_id):
--   issue_id='LN01': 2 rows (rollup pos > exam)
--   issue_id='LN02': 19 rows (rollup pos > 0, exam = 0/NULL)
--   LN03 and LN04 are architectural resolutions — no queue needed (the
--   replacement view's per-source columns eliminate the "raw vs final"
--   conflict and the "both null" collapse).
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.ln_per_patient_multisource_v1 (view)
--   Queue inserts into manuscript_workspace.qc_manual_review_queue_v1
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.ln_per_patient_multisource_v1 AS
WITH all_pts AS (
    -- Union of every patient appearing in any LN-bearing source
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
    FROM main.canonical_path_malignant_events_v1
    UNION
    SELECT DISTINCT CAST(research_id AS VARCHAR) FROM main.canonical_us_lymph_node_v2
    UNION
    SELECT DISTINCT research_id FROM main.ct_imaging
    UNION
    SELECT DISTINCT research_id FROM main.mri_imaging
    UNION
    SELECT DISTINCT research_id FROM main.canonical_cervical_ln_clinical_patient_rollup_v1
),
path AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        SUM(ln_involved)  AS ln_path_positive,
        SUM(ln_examined)  AS ln_path_examined,
        COUNT(*)          AS n_path_rows,
        COUNT(ln_involved) + COUNT(ln_examined) AS n_path_ln_values_nonnull
    FROM main.canonical_path_malignant_events_v1
    GROUP BY CAST(research_id AS VARCHAR)
),
us AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        COUNT(*) AS n_us_ln_rows
    FROM main.canonical_us_lymph_node_v2
    GROUP BY CAST(research_id AS VARCHAR)
),
ct AS (
    SELECT
        research_id,
        SUM(CASE WHEN lymph_nodes_suspicious = TRUE THEN 1 ELSE 0 END) AS ln_ct_suspicious_count,
        SUM(CASE WHEN pathologic_lymph_nodes = TRUE THEN 1 ELSE 0 END) AS ln_ct_pathologic_count,
        COUNT(*) AS n_ct_exams
    FROM main.ct_imaging
    GROUP BY research_id
),
mri AS (
    SELECT
        research_id,
        SUM(CASE WHEN pathologic_lymph_nodes = 1 THEN 1 ELSE 0 END) AS ln_mri_suspicious_count,
        COUNT(*) AS n_mri_exams
    FROM main.mri_imaging
    GROUP BY research_id
),
clin AS (
    SELECT
        research_id,
        has_positive_ln_level AS ln_clinical_positive_flag
    FROM main.canonical_cervical_ln_clinical_patient_rollup_v1
)
SELECT
    a.research_id,
    -- Path stream
    p.ln_path_positive,
    p.ln_path_examined,
    -- US stream (shell until USLN01 rebuilt)
    CAST(NULL AS BIGINT) AS ln_us_suspicious_count,
    -- CT / MRI streams
    c.ln_ct_suspicious_count,
    c.ln_ct_pathologic_count,
    m.ln_mri_suspicious_count,
    -- Clinical stream
    cl.ln_clinical_positive_flag,
    -- Availability flags (LN04 resolution)
    (p.ln_path_positive IS NOT NULL OR p.ln_path_examined IS NOT NULL)
        AS ln_data_available_path,
    (u.n_us_ln_rows > 0)            AS ln_data_available_us,
    (c.n_ct_exams    > 0)           AS ln_data_available_ct,
    (m.n_mri_exams   > 0)           AS ln_data_available_mri,
    (cl.research_id IS NOT NULL)    AS ln_data_available_clinical,
    -- QC flags within the path stream
    (p.ln_path_positive > p.ln_path_examined)
        AS ln_path_ln01_rollup_flag,
    (p.ln_path_positive > 0 AND COALESCE(p.ln_path_examined, 0) = 0)
        AS ln_path_ln02_rollup_flag
FROM all_pts a
LEFT JOIN path p  ON p.research_id  = a.research_id
LEFT JOIN us   u  ON u.research_id  = a.research_id
LEFT JOIN ct   c  ON c.research_id  = a.research_id
LEFT JOIN mri  m  ON m.research_id  = a.research_id
LEFT JOIN clin cl ON cl.research_id = a.research_id;

-- ---------------------------------------------------------------------------
-- QC queue emission (idempotent) — LN01 and LN02 at patient-rollup grain
-- ---------------------------------------------------------------------------

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'LN01' AS issue_id,
    v.research_id,
    'ln_per_patient_multisource_v1' AS source_table,
    v.research_id AS source_pk,
    TO_JSON(struct_pack(
        ln_path_positive     := v.ln_path_positive,
        ln_path_examined     := v.ln_path_examined,
        ln_path_examined_is_null := (v.ln_path_examined IS NULL)
    )) AS context_json,
    'LN01: SUM(ln_involved) > SUM(ln_examined) at patient-rollup grain — path numerator exceeds denominator; requires chart review of constituent path events' AS reason
FROM manuscript_workspace.ln_per_patient_multisource_v1 v
WHERE v.ln_path_ln01_rollup_flag = TRUE
AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
    WHERE q.issue_id = 'LN01'
    AND q.source_table = 'ln_per_patient_multisource_v1'
    AND q.source_pk = v.research_id
);

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'LN02' AS issue_id,
    v.research_id,
    'ln_per_patient_multisource_v1' AS source_table,
    v.research_id AS source_pk,
    TO_JSON(struct_pack(
        ln_path_positive := v.ln_path_positive,
        ln_path_examined := v.ln_path_examined
    )) AS context_json,
    'LN02: SUM(ln_involved) > 0 with SUM(ln_examined) = 0 or NULL at patient-rollup grain — positive nodes reported without examined denominator; N1a/N1b staging uncomputable without chart review' AS reason
FROM manuscript_workspace.ln_per_patient_multisource_v1 v
WHERE v.ln_path_ln02_rollup_flag = TRUE
AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
    WHERE q.issue_id = 'LN02'
    AND q.source_table = 'ln_per_patient_multisource_v1'
    AND q.source_pk = v.research_id
);

-- ---------------------------------------------------------------------------
-- Cleanup pass (per migration 09/10 rhythm):
--   Comment the three deprecated cohort LN columns + log deprecation entry.
--   Note: ln_ratio and ln_burden_band derive from these and become stale —
--   mentioned in the comment text rather than separately logged.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.manuscript_cohort_v1.ln_positive_final IS
'DEPRECATED 2026-04-23 (LN01/LN02/LN03). Single-column collapse of multi-source LN counts — caused 10 patients with positive > examined, 28 with positive without denominator, 51 raw/final disagreements. Use manuscript_workspace.ln_per_patient_multisource_v1.ln_path_positive (SUM over canonical_path_malignant_events_v1.ln_involved) and the other per-source columns. ln_ratio / ln_burden_band derive from this column and are likewise stale — rebuild against the multisource view for cohort_v2.';

COMMENT ON COLUMN main.manuscript_cohort_v1.path_ln_examined_raw IS
'DEPRECATED 2026-04-23 (LN01/LN02/LN03). Use manuscript_workspace.ln_per_patient_multisource_v1.ln_path_examined (SUM over canonical_path_malignant_events_v1.ln_examined).';

COMMENT ON COLUMN main.manuscript_cohort_v1.path_ln_positive_raw IS
'DEPRECATED 2026-04-23 (LN01/LN02/LN03). Use manuscript_workspace.ln_per_patient_multisource_v1.ln_path_positive.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1
WHERE closing_prompt = 'prompt_10';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.manuscript_cohort_v1.ln_positive_final',
   'column',
   'manuscript_workspace.ln_per_patient_multisource_v1.ln_path_positive',
   'LN01/LN02/LN03',
   'prompt_10',
   'column_only',
   DATE '2026-04-23',
   'Single-column collapse of multi-source LN counts at patient grain; causes numerator>denominator and raw/final disagreement.',
   NULL,
   'Multi-source architecture: path, us, ct, mri, clinical each get their own column on ln_per_patient_multisource_v1 with availability flags. Downstream ln_ratio/ln_burden_band derivatives become stale and need rebuild against the new view.'),

  ('main.manuscript_cohort_v1.path_ln_examined_raw',
   'column',
   'manuscript_workspace.ln_per_patient_multisource_v1.ln_path_examined',
   'LN01/LN02/LN03',
   'prompt_10',
   'column_only',
   DATE '2026-04-23',
   'Part of the collapsed LN columns on cohort_v1; replaced by path-stream SUM on the multisource view.',
   NULL,
   NULL),

  ('main.manuscript_cohort_v1.path_ln_positive_raw',
   'column',
   'manuscript_workspace.ln_per_patient_multisource_v1.ln_path_positive',
   'LN01/LN02/LN03',
   'prompt_10',
   'column_only',
   DATE '2026-04-23',
   'Part of the collapsed LN columns on cohort_v1; replaced by path-stream SUM on the multisource view.',
   NULL,
   NULL);
