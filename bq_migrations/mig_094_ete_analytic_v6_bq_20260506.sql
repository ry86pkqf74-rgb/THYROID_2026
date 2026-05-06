-- mig_094: ete_manuscript_analytic_v6 (BQ)
-- THY-19 — v6 adds inline adjudication layer (there is no v5).
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: cascade v6)
-- Prerequisites: mig_089–mig_093 (helpers + v1–v4)
--
-- BUG FIX: DuckDB "~" LIKE operator → BQ LIKE
--   The original tombstone mig_067 failed because BQ does not support the DuckDB
--   ~~ (LIKE) operator. All ~~ patterns are translated to LIKE here.
--   Specifically: `ipt.inline_set ~~ 'queue_microscopic_no_invasion_signal%'`
--   becomes: `ipt.inline_set LIKE 'queue_microscopic_no_invasion_signal%'`
--
-- Translation notes:
--   - main.canonical_ete_inline_adjudication_v1 → pub_canonical.canonical_ete_inline_adjudication_v1
--   - CAST(x AS VARCHAR) → CAST(x AS STRING)
--   - ~~ (DuckDB LIKE) → LIKE (BQ)
--
-- Expected rows: same as v1 (~6,469)

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v6` AS

WITH inline_pt AS (
    -- Patient-level inline adjudications (no tumor_ordinal = patient-level override)
    SELECT
        research_id,
        ete_grade_resolved  AS inline_grade,
        resolution_set      AS inline_set,
        evidence_quote      AS inline_evidence,
        resolution_source   AS inline_source
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_ete_inline_adjudication_v1`
    WHERE tumor_ordinal IS NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY CASE
            WHEN resolution_set = 'queue_straggler'                    THEN 1
            WHEN resolution_set = 'pm_disagreement_gross_vs_micro'    THEN 2
            WHEN resolution_set = 'queue_boolean_string_upstream_bug'  THEN 3
            ELSE 4
        END
    ) = 1
),

inline_evt AS (
    -- Event-level inline adjudications (with tumor_ordinal = per-tumor override)
    SELECT
        research_id,
        tumor_ordinal,
        ete_grade_resolved  AS inline_evt_grade,
        evidence_quote      AS inline_evt_evidence,
        resolution_set      AS inline_evt_set
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_ete_inline_adjudication_v1`
    WHERE tumor_ordinal IS NOT NULL
)

SELECT
    v4.*,
    ipt.inline_grade,
    ipt.inline_set,
    ipt.inline_evidence,
    ipt.inline_source,
    ievt.inline_evt_grade,
    ievt.inline_evt_evidence,
    ievt.inline_evt_set,

    -- v6 final grade: event-level inline overrides > patient-level inline overrides > v4
    CASE
        WHEN ievt.inline_evt_grade IN ('gross', 'microscopic', 'none')        THEN ievt.inline_evt_grade
        WHEN ievt.inline_evt_grade = 'unable_to_determine'                    THEN 'unable_to_determine'
        WHEN ipt.inline_set = 'pm_disagreement_gross_vs_micro'               THEN ipt.inline_grade
        WHEN v4.ete_grade_final_v4 IS NULL
             AND ipt.inline_grade IN ('gross', 'microscopic', 'none')          THEN ipt.inline_grade
        WHEN v4.ete_grade_final_v4 IS NULL
             AND ipt.inline_grade = 'unable_to_determine'                      THEN 'unable_to_determine'
        ELSE v4.ete_grade_final_v4
    END AS ete_grade_final_v6,

    CASE
        WHEN ievt.inline_evt_grade IS NOT NULL                                THEN 'inline_adjudication_event'
        WHEN ipt.inline_set = 'pm_disagreement_gross_vs_micro'               THEN 'inline_adjudication_pm_disagreement'
        WHEN v4.ete_grade_final_v4 IS NULL AND ipt.inline_grade IS NOT NULL
             THEN CASE
                WHEN ipt.inline_set = 'queue_straggler'
                    THEN 'inline_adjudication_queue'
                -- BUG FIX: ~~ → LIKE (DuckDB LIKE operator → BQ LIKE)
                WHEN ipt.inline_set LIKE 'queue_microscopic_no_invasion_signal%'
                    THEN 'inline_admin_accept_script_390'
                WHEN ipt.inline_set = 'queue_boolean_string_upstream_bug'
                    THEN 'inline_admin_boolean_bug_fix'
                ELSE 'inline_adjudication_other'
             END
        ELSE v4.ete_grade_source_v4
    END AS ete_grade_source_v6

FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v4` AS v4
LEFT JOIN inline_pt AS ipt
    ON ipt.research_id = CAST(v4.research_id AS STRING)
LEFT JOIN inline_evt AS ievt
    ON  ievt.research_id = CAST(v4.research_id AS STRING)
    AND ievt.tumor_ordinal = v4.tumor_ordinal
;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_094_ete_analytic_v6_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v6 inline adjudication in BQ; ~~ operator fixed to LIKE', 'pub_workspace', 'ete_manuscript_analytic_v6', NULL, 6469, 'DFL-20260506-ETEFAMILY; BUG FIX: ~~ → LIKE; depends on v4 (mig_093) + pub_canonical.canonical_ete_inline_adjudication_v1');
