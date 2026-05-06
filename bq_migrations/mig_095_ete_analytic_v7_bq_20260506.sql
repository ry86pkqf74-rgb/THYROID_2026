-- mig_095: ete_manuscript_analytic_v7 (BQ)
-- THY-19 — v7 adds canonical recurrence resolved fields (final layer).
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: cascade v7 + full rebuild DONE)
-- Prerequisites: mig_089–mig_094
--
-- Translation notes:
--   - main.canonical_recurrence_resolved_v1 → pub_canonical.canonical_recurrence_resolved_v1
--   - CAST(rr.research_id AS VARCHAR) → CAST(rr.research_id AS STRING)
--
-- Expected rows: same as v1 (~6,469)
-- Smoke-test target: COUNT(*) ±5% of 6,469 = [6,145, 6,793]
-- cohort_m044_ajcc_ete_v1 guard: must still return 3,868 rows after deploy

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v7` AS
SELECT
    v6.*,
    rr.recurrence_path_proven,
    rr.recurrence_path_proven_date,
    rr.recurrence_path_proven_source,
    rr.days_to_path_proven,
    rr.recurrence_imaging_suspicious,
    rr.recurrence_imaging_suspicious_date,
    rr.recurrence_imaging_modality,
    rr.recurrence_imaging_modality_summary,
    rr.recurrence_imaging_finding_text,
    rr.recurrence_imaging_n_events,
    rr.days_to_imaging_suspicious,
    rr.recurrence_imaging_then_path_confirmed,
    rr.recurrence_status_final

FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v6` AS v6
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_resolved_v1` AS rr
    ON CAST(rr.research_id AS STRING) = CAST(v6.research_id AS STRING)
;

-- =============================================================================
-- FINAL SMOKE TEST (run after mig_095 completes)
-- All 6 analytic views should return non-zero rows.
-- v7 target: COUNT(*) between 6145 and 6793 (±5% of 6469).
-- cohort_m044_ajcc_ete_v1 guard: must still be 3868.
-- =============================================================================
-- SELECT 'ete_manuscript_analytic_v1' AS view_name, COUNT(*) AS n
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v1`
-- UNION ALL
-- SELECT 'ete_manuscript_analytic_v2', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v2`
-- UNION ALL
-- SELECT 'ete_manuscript_analytic_v3', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v3`
-- UNION ALL
-- SELECT 'ete_manuscript_analytic_v4', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v4`
-- UNION ALL
-- SELECT 'ete_manuscript_analytic_v6', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v6`
-- UNION ALL
-- SELECT 'ete_manuscript_analytic_v7', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v7`
-- UNION ALL
-- SELECT 'cohort_m044_ajcc_ete_v1 (GUARD — must stay 3868)', COUNT(*)
-- FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m044_ajcc_ete_v1`
-- ;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_095_ete_analytic_v7_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v7 FINAL view with recurrence resolved in BQ', 'pub_workspace', 'ete_manuscript_analytic_v7', NULL, 6469, 'DFL-20260506-ETEFAMILY; cascade-done milestone; depends on v6 (mig_094) + pub_canonical.canonical_recurrence_resolved_v1');
