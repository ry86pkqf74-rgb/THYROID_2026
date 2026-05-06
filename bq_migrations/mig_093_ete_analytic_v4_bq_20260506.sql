-- mig_093: ete_manuscript_analytic_v4 (BQ)
-- THY-19 — v4 overlays patient-master ETE grade for patients with unresolved v3 grade.
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (milestone: cascade v4)
-- Prerequisites: mig_089, mig_090, mig_091, mig_092
--
-- Translation notes:
--   - main.canonical_patient_master → pub_canonical.canonical_patient_master
--   - manuscript_workspace.cpm_ete_self_contradiction_queue_v1 → pub_workspace.*
--   - CAST('f' AS BOOLEAN) → FALSE
--   - CAST(x AS VARCHAR) → CAST(x AS STRING)
--   - status = 'awaiting_manual_review' (DuckDB filter preserved exactly)
--
-- Expected rows: same as v1 (~6,469)

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v4` AS

WITH q_open AS (
    -- Patients with open ETE self-contradiction flags (awaiting manual review)
    SELECT DISTINCT CAST(research_id AS STRING) AS research_id
    FROM `thyroid-canonical-pub-2026.pub_workspace.cpm_ete_self_contradiction_queue_v1`
    WHERE status = 'awaiting_manual_review'
),

pm AS (
    -- Patient master ETE fields for fallback grading
    SELECT
        research_id,
        ete_grade_clean,
        ete_grade,
        ete_grade_source,
        ete_grade_adjudicated,
        ete_adjudicated_flag
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
)

SELECT
    v3.*,
    pm.ete_grade_clean          AS pm_ete_grade_clean,
    pm.ete_grade_source         AS pm_ete_grade_source,
    pm.ete_grade_adjudicated    AS pm_ete_grade_adjudicated,
    pm.ete_adjudicated_flag     AS pm_ete_adjudicated_flag,

    -- v4 final grade: prefer v3, fill unspec_remaining from patient master clean
    CASE
        WHEN v3.ete_grade_final_v3 IS NOT NULL
             AND v3.ete_grade_final_v3 != 'unspec_remaining'      THEN v3.ete_grade_final_v3
        WHEN pm.ete_grade_clean IN ('gross', 'microscopic', 'none') THEN pm.ete_grade_clean
        WHEN v3.ete_grade_final_v3 = 'unspec_remaining'           THEN 'unspec_remaining'
        WHEN pm.ete_grade_clean = 'indeterminate'                  THEN 'indeterminate'
        ELSE NULL
    END AS ete_grade_final_v4,

    CASE
        WHEN v3.ete_grade_final_v3 IS NOT NULL
             AND v3.ete_grade_final_v3 != 'unspec_remaining'      THEN v3.ete_grade_source_v3
        WHEN pm.ete_grade_clean IN ('gross', 'microscopic', 'none') THEN 'patient_master_clean'
        WHEN v3.ete_grade_final_v3 = 'unspec_remaining'           THEN 'unresolved'
        WHEN pm.ete_grade_clean = 'indeterminate'                  THEN 'patient_master_indeterminate'
        ELSE NULL
    END AS ete_grade_source_v4,

    -- Flag when patient master internal ETE fields disagree
    (
        pm.ete_grade IS NOT NULL
        AND pm.ete_grade_clean IS NOT NULL
        AND pm.ete_grade NOT IN ('true', 'false', 'absent', 'present_ungraded')
        AND pm.ete_grade_clean NOT IN ('indeterminate')
        AND pm.ete_grade != pm.ete_grade_clean
    ) AS ete_grade_pm_disagreement_flag,

    EXISTS(
        SELECT 1 FROM q_open AS q
        WHERE q.research_id = CAST(v3.research_id AS STRING)
    ) AS ete_self_contradiction_open_flag,

    -- v4 analytic eligibility (broader than v1 — uses COALESCE of v3 + PM grade)
    (
        COALESCE(
            CASE
                WHEN v3.ete_grade_final_v3 IS NOT NULL
                     AND v3.ete_grade_final_v3 != 'unspec_remaining' THEN v3.ete_grade_final_v3
                ELSE pm.ete_grade_clean
            END,
            v3.ete_norm
        ) IS NOT NULL
        AND v3.surgery_episode_id_global IS NOT NULL
        AND v3.size_greatest_dimension_cm_trusted IS NOT NULL
        AND v3.primary_histology_trusted IS NOT NULL
        AND v3.primary_histology_trusted NOT IN (
            'NIFTP', 'FTUMP', 'follicular adenoma',
            'atypical follicular / hurthle neoplasm',
            'uncertain malignant potential (non-FTUMP)'
        )
    ) AS analytic_eligible_v4

FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v3` AS v3
-- NOTE: canonical_patient_master.research_id is INT64 -- v3.research_id is STRING
LEFT JOIN pm ON CAST(pm.research_id AS STRING) = CAST(v3.research_id AS STRING)
;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_093_ete_analytic_v4_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: ete_manuscript_analytic_v4 patient-master fallback in BQ', 'pub_workspace', 'ete_manuscript_analytic_v4', NULL, 6469, 'DFL-20260506-ETEFAMILY; depends on v3 (mig_092) + pub_canonical.canonical_patient_master');
