-- =============================================================================
-- mig_246 — manuscript_workspace.manuscript_dashboard_VIEW_v1
-- Date:    2026-05-01
-- Author:  Cowork (post v17 + post-mig_245)
-- Lane:    Cowork-direct
-- Tip of origin/main at apply: 96e8ce3 (post-mig_245)
-- =============================================================================
--
-- BACKGROUND:
--   Logan reframed manuscript-prep priorities post-MD-migration-decision.
--   ChatGPT's audit recommended building a manuscript_prep schema + registry,
--   but discovery showed the registry essentially already exists:
--     - manuscript_workspace.manuscript_feasibility_v1 (83 manuscripts, 14 cols
--       including title/status/priority/feasibility_color/gating_issues/etc.)
--     - manuscript_workspace.manuscript_dive_map_v1 (63 manuscripts mapped to
--       31 Dives, with cohort_view_name).
--
--   What's missing is a single-pane JOIN view + an explicit "ready to draft"
--   readiness signal. mig_246 builds that.
--
-- DELIVERABLE:
--   manuscript_workspace.manuscript_dashboard_VIEW_v1 — single VIEW joining
--   feasibility_v1 + dive_map_v1 with computed:
--     - feas_age_days (how stale is the feasibility scoring)
--     - cohort_view_exists (existence check via information_schema)
--     - draft_readiness_signal (READY_TO_DRAFT / GREEN_BUT_IDEA_STAGE /
--       CAVEATS_BUT_ACTIVE / YELLOW_IDEA / DEFERRED / NOT_SCORED)
--
-- READINESS SIGNAL LOGIC (current):
--   READY_TO_DRAFT     — feasibility_color=GREEN  AND status IN (Ready to Submit, In Progress, Proposed)
--   GREEN_BUT_IDEA     — feasibility_color=GREEN  AND status = Idea
--   CAVEATS_BUT_ACTIVE — feasibility_color=YELLOW AND status IN (Ready to Submit, In Progress, Proposed)
--   YELLOW_IDEA        — feasibility_color=YELLOW AND status = Idea
--   DEFERRED           — feasibility_color=RED
--   NOT_SCORED         — manuscript in dive_map but not yet in feasibility_v1
--
-- POST-APPLY DISTRIBUTION (2026-05-01, against 63 dive-mapped manuscripts):
--   READY_TO_DRAFT     16  (M025, M029, M030, M031, M032, M033, M035, M036,
--                           M037, M038, M042, M043, M044, M045, M046, M047)
--   GREEN_BUT_IDEA     30  (M007, M009, M011 + 27 thematic-T1-mapped ideas)
--   CAVEATS_BUT_ACTIVE  3  (M028, M039, M040)
--   YELLOW_IDEA        14  (M001, M004, M006, M016, M017, M018, M019, M023,
--                           M072, M075, M078, M079, M080, M081)
--
-- COMPLEMENTARY UNFINISHED WORK (carry-forwards):
--   * Feasibility scoring is stale (scored_at = 2026-04-16, pre-v17, pre-
--     mig_245). Re-scoring against current canonical_patient_master schema is
--     mig_247 — recommended dispatch to Cursor Composer agent for full re-eval.
--   * Live cohort size table (manuscript_workspace.dive_cohort_size_v1)
--     attempted in mig_246 but blocked by SECOND silently-broken cohort view
--     surfaced post-mig_245: cohort_m031_nuclear_medicine_v1 references
--     `syn_isthmus_size_cm` which was renamed to `syn_isthmus_size_cm_legacy_raw`
--     (sibling: `syn_isthmus_size_parse_status`). Mig_245's regex scan only
--     covered _VIEW_v1/v2 renames in main; intra-table column renames (like
--     this one) were not in scope. Per-view scan + repair = mig_248.
--
-- =============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.manuscript_dashboard_VIEW_v1 AS
SELECT
  m.manuscript_id,
  COALESCE(f.title, m.manuscript_title) AS manuscript_title,
  f.status AS manuscript_status,
  f.priority,
  f.feasibility_color AS feas_color,
  f.candidate_n AS scored_at_candidate_n,
  m.cohort_view_name,
  m.dive_id,
  m.dive_title,
  m.dive_type,
  m.filter_type,
  f.project_leaders,
  f.gating_issues,
  f.recommended_next_step,
  f.v1_1_upgrade_prediction,
  f.scored_at AS feas_scored_at,
  CAST(EXTRACT(EPOCH FROM (CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - CAST(f.scored_at AS TIMESTAMP))) / 86400 AS INTEGER) AS feas_age_days,
  EXISTS (
    SELECT 1 FROM information_schema.views v
    WHERE v.table_schema = 'manuscript_workspace'
      AND v.table_name = m.cohort_view_name
  ) AS cohort_view_exists,
  m.canonical_version,
  m.notes AS dive_sprint_label,
  CASE
    WHEN f.feasibility_color = 'GREEN' AND f.status IN ('Ready to Submit', 'In Progress', 'Proposed') THEN 'READY_TO_DRAFT'
    WHEN f.feasibility_color = 'GREEN' AND f.status = 'Idea' THEN 'GREEN_BUT_IDEA_STAGE'
    WHEN f.feasibility_color = 'YELLOW' AND f.status IN ('Ready to Submit', 'In Progress', 'Proposed') THEN 'CAVEATS_BUT_ACTIVE'
    WHEN f.feasibility_color = 'YELLOW' THEN 'YELLOW_IDEA'
    WHEN f.feasibility_color = 'RED' THEN 'DEFERRED'
    WHEN f.feasibility_color IS NULL THEN 'NOT_SCORED'
    ELSE 'UNKNOWN'
  END AS draft_readiness_signal
FROM manuscript_workspace.manuscript_dive_map_v1 m
LEFT JOIN manuscript_workspace.manuscript_feasibility_v1 f ON f.manuscript_id = m.manuscript_id
ORDER BY m.manuscript_id;

-- =============================================================================
-- Verification queries (run post-apply):
-- =============================================================================
-- 1) Dashboard summary by readiness:
--    SELECT draft_readiness_signal, COUNT(*) AS n, STRING_AGG(CAST(manuscript_id AS VARCHAR), ',' ORDER BY manuscript_id) AS ids
--    FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
--    GROUP BY draft_readiness_signal;
--
-- 2) Priority queue for drafting (READY_TO_DRAFT, sorted by priority):
--    SELECT manuscript_id, manuscript_title, manuscript_status, priority, scored_at_candidate_n, dive_title
--    FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
--    WHERE draft_readiness_signal = 'READY_TO_DRAFT'
--    ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, manuscript_id;
--
-- 3) Manuscripts whose cohort_view doesn't exist (dive_map references a view
--    that was dropped or renamed since dive_map was populated):
--    SELECT manuscript_id, cohort_view_name, dive_title
--    FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
--    WHERE NOT cohort_view_exists;
--
-- 4) Stale-feasibility flag (feas_age_days > 14 means scoring is stale):
--    SELECT COUNT(*) FROM manuscript_workspace.manuscript_dashboard_VIEW_v1
--    WHERE feas_age_days > 14;
--    -- Expected: ALL rows (since scored_at = 2026-04-16, ~15 days ago at apply).

-- =============================================================================
-- End of mig_246
-- =============================================================================
