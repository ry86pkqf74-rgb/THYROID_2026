-- ============================================================================
-- Migration 48 — OP05: path_malignant surgery_episode_id rebind to global op namespace
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      OP05 — path_malignant.surgery_episode_id carries patient-local
--                ordinals (1,2,3,NULL); canonical_operative_events_v1 now uses
--                globally-unique episode IDs (11,773 total). 5,254 rows need
--                rebind to the global namespace.
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24):
--   Unique (research_id, surgery_date_native) pairs in op → 11,773 (1:1 with
--   surgery_episode_id). So (rid, date) is a deterministic key.
--
--   LEFT JOIN path × op on (rid, CAST(surgery_date AS DATE)):
--     already_match:  1 row (coincidentally aligned)
--     mismatch:      5,254 rows (path local ordinal ≠ op global — OP05 target)
--     path_null:     1,434 rows (path.surgery_episode_id NULL, date present)
--   All 6,689 path rows resolve to a unique op episode.
--
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_global_epi (VIEW)
--     + surgery_episode_id_global         BIGINT   (from op rebind)
--     + surgery_episode_uid_source        VARCHAR  ∈ {op_rebind, already_match, md5_fallback, no_date}
--     + op05_rebind_applied_flag          BOOLEAN
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_global_epi AS
WITH op AS (
  SELECT research_id, surgery_episode_id AS op_epi, CAST(surgery_date_native AS DATE) AS sd
  FROM main.canonical_operative_events_v1
  WHERE surgery_date_native IS NOT NULL
)
SELECT
  p.*,
  op.op_epi AS surgery_episode_id_global,
  CASE
    WHEN op.op_epi IS NOT NULL AND p.surgery_episode_id = op.op_epi THEN 'already_match'
    WHEN op.op_epi IS NOT NULL                                       THEN 'op_rebind'
    WHEN CAST(p.surgery_date AS DATE) IS NULL                        THEN 'no_date'
    ELSE 'md5_fallback'
  END AS surgery_episode_uid_source,
  (op.op_epi IS NOT NULL AND p.surgery_episode_id IS DISTINCT FROM op.op_epi)
    AS op05_rebind_applied_flag
FROM main.canonical_path_malignant_events_v1 p
LEFT JOIN op
  ON op.research_id = p.research_id
 AND op.sd          = CAST(p.surgery_date AS DATE);

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='OP05';

-- Queue: rows that did NOT rebind to an op episode (md5_fallback / no_date)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'OP05', research_id, 'main.canonical_path_malignant_events_v1',
  CAST(path_surgery_id AS VARCHAR),
  TO_JSON(struct_pack(
    surgery_episode_id_local := surgery_episode_id,
    surgery_date := surgery_date,
    surgery_episode_uid_source := surgery_episode_uid_source
  )),
  'OP05 path row has no op-events match (md5_fallback/no_date) — chart review',
  'open', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_path_malignant_events_v1_global_epi
WHERE surgery_episode_uid_source IN ('md5_fallback','no_date');

COMMENT ON TABLE main.canonical_path_malignant_events_v1 IS
'Path malignant events (6,689 rows). Clean view manuscript_workspace.canonical_path_malignant_events_v1_global_epi surfaces surgery_episode_id_global (rebound to op events via (rid, surgery_date_native)) + surgery_episode_uid_source ∈ {already_match 1, op_rebind 5,254+1,434, md5_fallback 0, no_date 0} + op05_rebind_applied_flag. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_47';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_path_malignant_events_v1.surgery_episode_id','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_global_epi.surgery_episode_id_global',
   'OP05','prompt_47','column_only',DATE '2026-04-24',
   'OP05: 6,688 of 6,689 path rows rebound to global op episode namespace via (research_id, surgery_date_native); 1 already matched; 0 orphans. (rid, surgery_date_native) is 1:1 with surgery_episode_id in canonical_operative_events_v1.',
   NULL,
   'Downstream joins between path and operative should use surgery_episode_id_global from the clean view, NOT the local surgery_episode_id. PATH01 is resolved via the same rebind (shares surgery_episode_id column).');
