-- mig_091: ete_manuscript_analytic_v2 (BQ)
-- THY-19 — v2 adds LLM ETE sub-grading layer on top of v1.
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: cascade v2)
-- Prerequisites: mig_089 (helpers), mig_090 (v1)
--
-- Translation notes:
--   - manuscript_workspace.ete_manuscript_analytic_v1 → pub_workspace.ete_manuscript_analytic_v1
--   - manuscript_workspace.ete_llm_grade_patient_v1   → pub_workspace.ete_llm_grade_patient_v1
--     (exists as BASE TABLE in pub_workspace, 2026-05-06 verification)
--
-- Expected rows: same as v1 (~6,469)

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v2` AS
SELECT
    a.*,
    g.ete_grade_llm,
    g.has_gross_llm,
    g.has_micro_llm,
    g.n_ete_mentions AS llm_ete_mention_count,

    -- Final ETE grade combining structured (v1) with LLM sub-grading
    CASE
        WHEN a.ete_norm = 'extensive'                             THEN 'gross'
        WHEN a.ete_norm IN ('microscopic', 'minimal')             THEN 'microscopic'
        WHEN a.ete_norm = 'present_unspecified' AND g.has_gross_llm THEN 'gross'
        WHEN a.ete_norm = 'present_unspecified' AND g.has_micro_llm THEN 'microscopic'
        WHEN a.ete_norm = 'present_unspecified'                   THEN 'unspec_remaining'
        WHEN a.ete_norm = 'none'                                  THEN 'none'
        ELSE NULL
    END AS ete_grade_final,

    CASE
        WHEN a.ete_norm = 'extensive'                             THEN 'structured'
        WHEN a.ete_norm IN ('microscopic', 'minimal')             THEN 'structured'
        WHEN a.ete_norm = 'present_unspecified' AND g.has_gross_llm THEN 'llm_subgrade'
        WHEN a.ete_norm = 'present_unspecified' AND g.has_micro_llm THEN 'llm_subgrade'
        WHEN a.ete_norm = 'present_unspecified'                   THEN 'unresolved'
        WHEN a.ete_norm = 'none'                                  THEN 'structured'
        ELSE NULL
    END AS ete_grade_source

FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v1` AS a
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.ete_llm_grade_patient_v1` AS g
    ON g.research_id = a.research_id
;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_091_ete_analytic_v2_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v2 LLM sub-grade layer in BQ', 'pub_workspace', 'ete_manuscript_analytic_v2', NULL, 6469, 'DFL-20260506-ETEFAMILY; depends on v1 (mig_090) + ete_llm_grade_patient_v1');
