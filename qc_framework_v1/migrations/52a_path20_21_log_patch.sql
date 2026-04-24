-- ============================================================================
-- Migration 52a — PATH20/PATH21 deprecation-log backfill patch
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH20 (T-stage AJCC7/8 discordance), PATH21 (laterality discordance)
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Context:
--   Prompt 04 (2026-04-23) built manuscript_workspace.path_event_discordance_v1
--   surfacing derived_t_stage_ajcc8 + discordance_t_stage_flag +
--   discordance_laterality_flag and queued 207 PATH20 + 219 PATH21 rows for
--   chart review. The view was created, but the corresponding row in
--   manuscript_workspace.canonical_deprecation_log_v1 was not inserted. Audit
--   on 2026-04-24 (prior to migration 52) caught the gap — this file records
--   the backfill so the log-state is reproducible from migrations alone.
--
-- Idempotent: DELETEs any prior prompt_04 row before INSERT.
-- ============================================================================

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_04';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_path_malignant_events_v1.reported_t_stage_ajcc8','column',
   'manuscript_workspace.path_event_discordance_v1.derived_t_stage_ajcc8',
   'PATH20,PATH21','prompt_04','column_only',DATE '2026-04-23',
   'PATH20 (T-stage discordance between AJCC7/AJCC8) + PATH21 (laterality discordance between reported and derived) surfaced via path_event_discordance_v1 with discordance_t_stage_flag + discordance_laterality_flag. 207+219 rows queued under PATH20/PATH21 for chart review. Log row was missing from prior migration; patched 2026-04-24.',
   NULL,
   'Downstream staging analyses should use derived_t_stage_ajcc8 + discordance_t_stage_flag from the discordance view. Laterality-trust: lat_norm + discordance_laterality_flag. Original reported_t_stage_ajcc7/8 preserved on main for audit.');
