-- ============================================================================
-- Migration 16 — PATH12: size disagreement between greatest-dim and per-surgery
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH12
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Two size columns coexist on canonical_path_malignant_events_v1:
--   size_greatest_dimension_cm  — per-tumor greatest dimension (row-level)
--   tumor_size_cm_per_surgery   — per-surgery aggregate (constant across rows
--                                  sharing (research_id, surgery_date))
--
-- Raw row-level comparison is noisy: 1,208 rows (>5mm) / 866 rows (>1cm)
-- disagreement — most are explained by multi-tumor surgeries where the
-- per-surgery aggregate naturally differs from any single tumor's greatest dim.
--
-- Real invariants enforced at SURGERY grain:
--   A. single-tumor surgery → max(greatest) should equal per_surgery    ( 1 row)
--   B. multi-tumor surgery  → per_surgery >= max(greatest) (per-surgery is max)
--      violation = per_surgery < max(greatest) - 0.1cm                  (79 rows)
--   C. per_surgery > max(greatest) + 0.1cm
--      (possibly legitimate if per-surgery is SUM; flag for review)     (15 rows)
--
-- Queue: 95 surgery-grain rows under issue_id='PATH12'.
-- ----------------------------------------------------------------------------
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_size_flag
--     — all rows + boolean flags per-surgery (broadcast to rows):
--         size_single_tumor_mismatch_flag
--         size_per_surgery_understates_flag   (per_surg < max_greatest - 0.1)
--         size_per_surgery_overstates_flag    (per_surg > max_greatest + 0.1)
--         size_disagreement_any_flag
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_size_flag AS
WITH surg AS (
    SELECT
        research_id, surgery_date,
        COUNT(*) AS n_tumors,
        MAX(size_greatest_dimension_cm)   AS max_greatest_cm,
        ANY_VALUE(tumor_size_cm_per_surgery) AS per_surgery_cm
    FROM main.canonical_path_malignant_events_v1
    GROUP BY research_id, surgery_date
)
SELECT
    e.*,
    s.n_tumors,
    s.max_greatest_cm,
    (s.n_tumors = 1
       AND s.max_greatest_cm IS NOT NULL
       AND s.per_surgery_cm IS NOT NULL
       AND ABS(s.per_surgery_cm - s.max_greatest_cm) > 0.1)
        AS size_single_tumor_mismatch_flag,
    (s.per_surgery_cm < s.max_greatest_cm - 0.1)
        AS size_per_surgery_understates_flag,
    (s.per_surgery_cm > s.max_greatest_cm + 0.1)
        AS size_per_surgery_overstates_flag,
    (
        (s.n_tumors = 1 AND s.max_greatest_cm IS NOT NULL AND s.per_surgery_cm IS NOT NULL
            AND ABS(s.per_surgery_cm - s.max_greatest_cm) > 0.1)
        OR (s.per_surgery_cm < s.max_greatest_cm - 0.1)
        OR (s.per_surgery_cm > s.max_greatest_cm + 0.1)
    ) AS size_disagreement_any_flag
FROM main.canonical_path_malignant_events_v1 e
LEFT JOIN surg s
  ON e.research_id = s.research_id
 AND e.surgery_date = s.surgery_date;

-- Idempotent queue (surgery-grain) under PATH12
DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH12';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
WITH surg AS (
    SELECT
        research_id, surgery_date,
        COUNT(*) AS n_tumors,
        MAX(size_greatest_dimension_cm)       AS max_greatest_cm,
        MIN(size_greatest_dimension_cm)       AS min_greatest_cm,
        ANY_VALUE(tumor_size_cm_per_surgery)  AS per_surgery_cm
    FROM main.canonical_path_malignant_events_v1
    GROUP BY research_id, surgery_date
)
SELECT
    'PATH12',
    CAST(s.research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|', CAST(s.research_id AS VARCHAR), CAST(s.surgery_date AS VARCHAR)),
    TO_JSON(struct_pack(
        n_tumors := s.n_tumors,
        max_greatest_cm := s.max_greatest_cm,
        min_greatest_cm := s.min_greatest_cm,
        per_surgery_cm  := s.per_surgery_cm,
        delta_cm        := s.per_surgery_cm - s.max_greatest_cm
    )),
    CASE
        WHEN s.n_tumors = 1
             AND ABS(s.per_surgery_cm - s.max_greatest_cm) > 0.1
            THEN 'Single-tumor surgery: per_surgery differs from greatest by >0.1cm'
        WHEN s.per_surgery_cm < s.max_greatest_cm - 0.1
            THEN 'Multi-tumor: per_surgery UNDERSTATES max greatest dim by >0.1cm (per-surg should be max-of-tumors)'
        WHEN s.per_surgery_cm > s.max_greatest_cm + 0.1
            THEN 'per_surgery OVERSTATES max greatest dim by >0.1cm (possibly per-surg is sum, or extra tumor not in path events)'
    END,
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM surg s
WHERE
    (s.n_tumors = 1 AND s.max_greatest_cm IS NOT NULL AND s.per_surgery_cm IS NOT NULL
        AND ABS(s.per_surgery_cm - s.max_greatest_cm) > 0.1)
    OR (s.per_surgery_cm < s.max_greatest_cm - 0.1)
    OR (s.per_surgery_cm > s.max_greatest_cm + 0.1);

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.tumor_size_cm_per_surgery IS
'Per-surgery tumor-size aggregate (constant across rows sharing research_id+surgery_date). Invariant: per_surgery >= max(size_greatest_dimension_cm) across tumors. Violations flagged PATH12. See manuscript_workspace.canonical_path_malignant_events_v1_size_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_15';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.tumor_size_cm_per_surgery','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_size_flag',
   'PATH12','prompt_15','column_only',DATE '2026-04-23',
   '95 surgery-grain rows violate the size monotonicity invariant (79 understates, 15 overstates, 1 single-tumor mismatch).',
   NULL,
   '4-flag view + PATH12 queue at surgery grain. Column retained as-is — fix requires chart review to reconcile per-surgery vs per-tumor sources.');
