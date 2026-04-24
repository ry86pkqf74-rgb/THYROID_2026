-- ============================================================================
-- Migration 45 — FNA05: rollup bethesda_final recompute (stale registry)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      FNA05 — registry claimed "6 pts — rollup bethesda_final NULL
--                despite non-null preop event-level Bethesda".
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24) against canonical_fna_events_v1_dedup + operative rollup:
--   rollup.bethesda_final NULL pts:  240 / 5,266
--     → 38 truly have no event-level Bethesda
--     → 202 have ONLY post-surgery FNAs (legitimately excluded by rollup's
--        pre-surgery filter — not a bug)
--     → 0  with a preop Bethesda that rollup missed
--   Registry's "6 pts" is STALE.
--
--   Agreement audit:
--     4,965 agree (stored == recomputed preop max)
--        60 stored-non-null / recomputed-NULL (pts whose all FNAs are
--           on-or-after earliest_surgery_date BUT the original rollup used
--           the looser ≤-date window — ordering artifact)
--         1 true disagreement: research_id=462 (stored=6, recomputed=1)
--       240 both NULL
--
-- Design:
--   Build manuscript_workspace.canonical_fna_patient_rollup_v1_clean that
--   carries:
--     + bethesda_final_recomputed        INTEGER (max preop Bethesda from dedup)
--     + fna05_rollup_beth_disagree_flag  BOOLEAN (stored<>recomputed, both non-null)
--     + fna05_rollup_beth_looser_flag    BOOLEAN (stored non-null, recomputed NULL)
--   Queue the 1 true disagreement under FNA05.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_fna_patient_rollup_v1_clean AS
WITH preop AS (
  SELECT f.research_id,
         MAX(f.bethesda_final_num) AS preop_max_beth
  FROM manuscript_workspace.canonical_fna_events_v1_dedup f
  LEFT JOIN main.canonical_operative_patient_rollup_v1 op
         ON CAST(f.research_id AS BIGINT) = op.research_id
  WHERE f.fna_row_rank=1
    AND f.bethesda_final_num IS NOT NULL
    AND (op.earliest_surgery_date IS NULL OR f.fna_date_resolved < op.earliest_surgery_date)
  GROUP BY 1
)
SELECT
  r.*,
  p.preop_max_beth AS bethesda_final_recomputed,
  (r.bethesda_final IS NOT NULL
   AND p.preop_max_beth IS NOT NULL
   AND r.bethesda_final <> p.preop_max_beth) AS fna05_rollup_beth_disagree_flag,
  (r.bethesda_final IS NOT NULL
   AND p.preop_max_beth IS NULL) AS fna05_rollup_beth_looser_flag
FROM main.canonical_fna_patient_rollup_v1 r
LEFT JOIN preop p USING (research_id);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA05';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'FNA05',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_fna_patient_rollup_v1',
  CAST(research_id AS VARCHAR),
  TO_JSON(struct_pack(
    bethesda_final_stored := bethesda_final,
    bethesda_final_recomputed := bethesda_final_recomputed
  )),
  'FNA05 rollup bethesda_final disagrees with recomputed preop max — chart review',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_fna_patient_rollup_v1_clean
WHERE fna05_rollup_beth_disagree_flag;

COMMENT ON TABLE main.canonical_fna_patient_rollup_v1 IS
'FNA patient rollup (5,266 rows). Clean view manuscript_workspace.canonical_fna_patient_rollup_v1_clean surfaces bethesda_final_recomputed (preop max over _dedup events) and two audit flags. 1 true disagreement queued under FNA05 (research_id 462). 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_44';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_fna_patient_rollup_v1.bethesda_final','column',
   'manuscript_workspace.canonical_fna_patient_rollup_v1_clean.bethesda_final_recomputed',
   'FNA05','prompt_44','column_only',DATE '2026-04-24',
   'FNA05: registry claimed 6 pts with rollup bethesda_final NULL despite preop event Bethesda — probe shows 0 pts match that pattern (240 NULLs are all legitimate: 38 no-event-beth + 202 post-surgery-only). Effectively resolved-by-audit; registry count was stale.',
   NULL,
   'View carries bethesda_final_recomputed + fna05_rollup_beth_disagree_flag (1 pt queued) + fna05_rollup_beth_looser_flag (60 pts: stored preserved from looser ≤-date window; informational only).');
