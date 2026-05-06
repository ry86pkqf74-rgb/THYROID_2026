-- mig_092: ete_manuscript_analytic_v3 (BQ)
-- THY-19 — v3 adds "fresh LLM" re-extraction layer (second-pass LLM notes query).
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: cascade v3)
-- Prerequisites: mig_089, mig_090, mig_091
--
-- Translation notes:
--   - manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 → pub_workspace.*
--     (exists as BASE TABLE in pub_workspace, 2026-05-06 verification)
--   - CAST(f.research_id AS VARCHAR) → CAST(f.research_id AS STRING)
--
-- Expected rows: same as v1 (~6,469)

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v3` AS
SELECT
    v2.*,
    f.ete_grade_fresh_llm,
    f.has_gross_fresh,
    f.has_micro_fresh,
    f.has_absent_fresh,
    f.fresh_evidence_quotes,
    f.fresh_best_confidence,
    f.fresh_ajcc8_implications,
    f.n_fresh_mentions AS llm_fresh_mention_count,

    -- Final ETE grade v3: prefer v2 resolved grade, fill unspec_remaining with fresh LLM
    CASE
        WHEN v2.ete_grade_final IS NOT NULL
             AND v2.ete_grade_final != 'unspec_remaining'   THEN v2.ete_grade_final
        WHEN f.has_gross_fresh                              THEN 'gross'
        WHEN f.has_micro_fresh                             THEN 'microscopic'
        WHEN f.has_absent_fresh                            THEN 'none'
        WHEN v2.ete_grade_final = 'unspec_remaining'       THEN 'unspec_remaining'
        ELSE NULL
    END AS ete_grade_final_v3,

    CASE
        WHEN v2.ete_grade_final IS NOT NULL
             AND v2.ete_grade_final != 'unspec_remaining'       THEN v2.ete_grade_source
        WHEN f.has_gross_fresh OR f.has_micro_fresh              THEN 'llm_fresh_subgrade'
        WHEN f.has_absent_fresh                                  THEN 'llm_fresh_absent'
        WHEN f.ete_grade_fresh_llm = 'unable_to_determine'      THEN 'llm_unable'
        WHEN v2.ete_grade_final = 'unspec_remaining'            THEN 'unresolved'
        ELSE NULL
    END AS ete_grade_source_v3

FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v2` AS v2
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.ete_llm_fresh_subgrade_patient_v1` AS f
    ON CAST(f.research_id AS STRING) = CAST(v2.research_id AS STRING)
;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_092_ete_analytic_v3_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v3 fresh LLM sub-grade in BQ', 'pub_workspace', 'ete_manuscript_analytic_v3', NULL, 6469, 'DFL-20260506-ETEFAMILY; depends on v2 (mig_091) + ete_llm_fresh_subgrade_patient_v1');
