-- ============================================================================
-- Migration 47 — OP03 + OP04: procedure code re-linkage
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   OP03 — 904 procedure-code rows with linkage_ambiguous_multi_episode=TRUE
--          (multiple candidate op episodes within 30d)
--   OP04 — 11,134 procedure-code rows with linked_surgery_episode_id=NULL
--          (no candidate op episode within 30d)
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   OP03 re-attribution via (procedure_normalized match DESC, gap_days ASC,
--   surgery_episode_id ASC):
--     - 290 / 904 rn=1 have procedure match (trusted)
--     - 614 / 904 rn=1 are date-nearest-only (lower confidence)
--     - Distribution of gap_days at rn=1:
--         0-1d: 216  |  2-7d: 217  |  8-30d: 471
--     All 904 reach a unique winner — zero remaining ties.
--
--   OP04: 11,134 NULL-linked rows. Empirically note_date gaps to nearest op
--   episode span years — most are retrospective progress notes referencing
--   prior surgeries; not recoverable via temporal linkage.
--
-- Output:
--   manuscript_workspace.canonical_operative_procedure_codes_v1_relinked (VIEW)
--     + linked_surgery_episode_id_relinked    BIGINT
--     + relink_method                         VARCHAR ∈
--         {same_day, temporal_30d, procedure_match_tightest, tightest_date_only,
--          orphan_unlinked}
--     + relink_gap_days                       INTEGER
--     + op03_relinked_flag                    BOOLEAN (newly resolved from ambiguous)
--     + op04_orphan_flag                      BOOLEAN (still unlinked)
--   Queue: 614 low-confidence OP03 relinks (tightest_date_only) +
--          all 11,134 OP04 orphan rows (issue_id='OP04').
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_operative_procedure_codes_v1_relinked AS
WITH op_events AS (
  SELECT research_id, surgery_episode_id, procedure_normalized,
         CAST(surgery_date_native AS DATE) AS surg_date
  FROM main.canonical_operative_events_v1
  WHERE surgery_date_native IS NOT NULL
),
rank_amb AS (
  SELECT p.procedure_mention_id,
         e.surgery_episode_id,
         COALESCE(p.procedure_normalized = e.procedure_normalized, FALSE) AS proc_match,
         ABS(CAST(e.surg_date - p.note_date AS INTEGER)) AS gap_days,
         ROW_NUMBER() OVER (PARTITION BY p.procedure_mention_id
                            ORDER BY COALESCE(p.procedure_normalized = e.procedure_normalized, FALSE) DESC,
                                     ABS(CAST(e.surg_date - p.note_date AS INTEGER)) ASC,
                                     e.surgery_episode_id ASC) AS rn
  FROM main.canonical_operative_procedure_codes_v1 p
  JOIN op_events e ON e.research_id = p.research_id
  WHERE p.linkage_ambiguous_multi_episode
),
amb_pick AS (
  SELECT procedure_mention_id, surgery_episode_id AS pick_episode, proc_match, gap_days
  FROM rank_amb WHERE rn=1
)
SELECT
  p.*,
  COALESCE(p.linked_surgery_episode_id, a.pick_episode) AS linked_surgery_episode_id_relinked,
  CASE
    WHEN p.linkage_method='same_day'               THEN 'same_day'
    WHEN p.linkage_method='temporal_30d'           THEN 'temporal_30d'
    WHEN p.linkage_method='temporal_30d_ambiguous' AND a.proc_match THEN 'procedure_match_tightest'
    WHEN p.linkage_method='temporal_30d_ambiguous' AND NOT a.proc_match THEN 'tightest_date_only'
    WHEN p.linkage_method='unlinked'               THEN 'orphan_unlinked'
    ELSE p.linkage_method
  END AS relink_method,
  a.gap_days AS relink_gap_days,
  (p.linkage_ambiguous_multi_episode AND a.pick_episode IS NOT NULL) AS op03_relinked_flag,
  (p.linkage_method='unlinked') AS op04_orphan_flag
FROM main.canonical_operative_procedure_codes_v1 p
LEFT JOIN amb_pick a USING (procedure_mention_id);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('OP03','OP04');

-- OP03: queue only the tightest_date_only relinks (lower confidence)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'OP03', research_id, 'main.canonical_operative_procedure_codes_v1',
  procedure_mention_id,
  TO_JSON(struct_pack(
    procedure_raw := procedure_raw,
    procedure_normalized := procedure_normalized,
    note_date := note_date,
    pick_episode := linked_surgery_episode_id_relinked,
    relink_method := relink_method,
    relink_gap_days := relink_gap_days
  )),
  'OP03 ambiguous-multi-episode relinked via tightest-date only (no procedure-name match) — audit',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_operative_procedure_codes_v1_relinked
WHERE relink_method='tightest_date_only';

-- OP04: queue all orphan_unlinked rows
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'OP04', research_id, 'main.canonical_operative_procedure_codes_v1',
  procedure_mention_id,
  TO_JSON(struct_pack(
    procedure_raw := procedure_raw,
    procedure_normalized := procedure_normalized,
    note_date := note_date,
    note_type := note_type
  )),
  'OP04 procedure-code row has no op episode within 30d — retrospective mention or orphan',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_operative_procedure_codes_v1_relinked
WHERE op04_orphan_flag;

COMMENT ON TABLE main.canonical_operative_procedure_codes_v1 IS
'Operative procedure codes (21,691 rows). Clean view manuscript_workspace.canonical_operative_procedure_codes_v1_relinked surfaces linked_surgery_episode_id_relinked + relink_method ∈ {same_day 9575, temporal_30d 78, procedure_match_tightest 290, tightest_date_only 614, orphan_unlinked 11134} + relink_gap_days. OP03 tightest_date_only + OP04 orphan rows queued. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_46';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_operative_procedure_codes_v1.linked_surgery_episode_id','column',
   'manuscript_workspace.canonical_operative_procedure_codes_v1_relinked.linked_surgery_episode_id_relinked',
   'OP03,OP04','prompt_46','column_only',DATE '2026-04-24',
   'OP03: 904 ambiguous-multi-episode rows re-attributed via (proc_match DESC, gap_days ASC, episode_id ASC) — 290 procedure_match_tightest (trusted), 614 tightest_date_only (queued). OP04: 11,134 truly unlinked rows tagged orphan_unlinked and queued.',
   NULL,
   'Relink writes to view only; original linked_surgery_episode_id preserved. Downstream queries should use linked_surgery_episode_id_relinked WHERE relink_method IN (''same_day'', ''temporal_30d'', ''procedure_match_tightest'') for high-confidence analysis; include ''tightest_date_only'' when analyst tolerance allows.');
