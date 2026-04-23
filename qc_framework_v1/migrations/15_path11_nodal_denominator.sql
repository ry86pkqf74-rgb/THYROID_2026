-- ============================================================================
-- Migration 15 — PATH11: nodal positive > 0 with denominator NULL or 0
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH11
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- The path event table carries TWO LN count pairs:
--   (ln_examined, ln_involved)                        — from synoptic form
--   (nodal_disease_total_count, nodal_disease_positive_count) — from structured
--                                                         LN detail summary
-- PATH11 flags rows with positive count > 0 but denominator NULL or 0:
--   Pair A (ln_*):            28 rows   (all denominator-NULL, none denominator-0)
--   Pair B (nodal_disease_*): 19 rows   (all denominator-NULL, none denominator-0)
-- Overlap between pairs is checked post-hoc in queue output.
--
-- 0 rows have positive > examined on either pair — so no cross-field contradiction
-- beyond the missing-denominator class.
--
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag
--     — all rows + two boolean flags:
--         ln_synoptic_denom_missing_flag
--         ln_detail_denom_missing_flag
--     plus a derived ln_denom_missing_any_flag.
--
-- Queue (qc_manual_review_queue_v1) emits one row per flagged event under
-- issue_id='PATH11' with both positive counts + both denominators in context_json.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag AS
SELECT
    e.*,
    (e.ln_involved > 0 AND e.ln_examined IS NULL)
        AS ln_synoptic_denom_missing_flag,
    (e.nodal_disease_positive_count > 0
        AND (e.nodal_disease_total_count IS NULL OR e.nodal_disease_total_count = 0))
        AS ln_detail_denom_missing_flag,
    ((e.ln_involved > 0 AND e.ln_examined IS NULL)
      OR (e.nodal_disease_positive_count > 0
          AND (e.nodal_disease_total_count IS NULL OR e.nodal_disease_total_count = 0)))
        AS ln_denom_missing_any_flag
FROM main.canonical_path_malignant_events_v1 e;

-- Idempotent queue emission (issue_id='PATH11')
DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH11';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'PATH11',
    CAST(e.research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|',
        CAST(e.research_id AS VARCHAR),
        CAST(e.surgery_date AS VARCHAR),
        CAST(COALESCE(e.specimen_id, '') AS VARCHAR),
        CAST(COALESCE(e.tumor_ordinal, 0) AS VARCHAR)
    ) AS source_pk,
    TO_JSON(struct_pack(
        ln_examined := e.ln_examined,
        ln_involved := e.ln_involved,
        nodal_disease_total_count    := e.nodal_disease_total_count,
        nodal_disease_positive_count := e.nodal_disease_positive_count,
        ln_synoptic_denom_missing    := (e.ln_involved > 0 AND e.ln_examined IS NULL),
        ln_detail_denom_missing      := (e.nodal_disease_positive_count > 0
                                         AND (e.nodal_disease_total_count IS NULL
                                              OR e.nodal_disease_total_count = 0))
    )) AS context_json,
    CASE
        WHEN (e.ln_involved > 0 AND e.ln_examined IS NULL)
         AND (e.nodal_disease_positive_count > 0
              AND (e.nodal_disease_total_count IS NULL OR e.nodal_disease_total_count = 0))
            THEN 'LN positive count > 0 on BOTH synoptic and detail pairs but both denominators missing'
        WHEN e.ln_involved > 0 AND e.ln_examined IS NULL
            THEN 'ln_involved > 0 but ln_examined is NULL (synoptic pair)'
        WHEN e.nodal_disease_positive_count > 0
         AND (e.nodal_disease_total_count IS NULL OR e.nodal_disease_total_count = 0)
            THEN 'nodal_disease_positive_count > 0 but nodal_disease_total_count is NULL or 0 (detail pair)'
    END AS reason,
    'open' AS status,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at
FROM main.canonical_path_malignant_events_v1 e
WHERE (e.ln_involved > 0 AND e.ln_examined IS NULL)
   OR (e.nodal_disease_positive_count > 0
       AND (e.nodal_disease_total_count IS NULL OR e.nodal_disease_total_count = 0));

-- ---------------------------------------------------------------------------
-- Cleanup pass
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.ln_involved IS
'Positive LN count from CAP synoptic form. Paired with ln_examined — when ln_involved>0 and ln_examined IS NULL, row is PATH11-flagged. See manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag.ln_synoptic_denom_missing_flag.';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.nodal_disease_positive_count IS
'Positive LN count from structured LN detail. Paired with nodal_disease_total_count — when positive>0 and total IS NULL/0, row is PATH11-flagged. See manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag.ln_detail_denom_missing_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_14';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.(ln_involved,ln_examined,nodal_disease_positive_count,nodal_disease_total_count)','column_group',
   'manuscript_workspace.canonical_path_malignant_events_v1_ln_denominator_flag',
   'PATH11','prompt_14','column_only',DATE '2026-04-23',
   '28 rows (synoptic pair) + 19 rows (detail pair) with positive count > 0 but denominator NULL or 0. Overlap computed in context_json.',
   NULL,
   '3 boolean flag columns surface violation; qc_manual_review_queue_v1 PATH11 rows require chart review to either recover denominator or downgrade positive count.');
