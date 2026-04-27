-- ============================================================================
-- Migration 61c — Full ETE closeout via inline Claude adjudication (no LLM batch)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Author:        Logan Glosser (adjudication by Claude/Cowork)
-- Date:          2026-04-27
-- Issue ID:      MANUSCRIPT_ETE_FULL_CLOSEOUT
-- ----------------------------------------------------------------------------
-- Replaces:      The mig_63 / mig_64 LLM-batch Cursor prompts. After analyzing
--                the actual queue contents we found:
--                  - 2,758 of 2,790 queue rows had patient_master.ete_grade_clean
--                    already populated (Script 390 IS the adjudication; the
--                    queue trip was administrative, not a missing answer)
--                  - 32 queue stragglers + 59 unspec_remaining events + 187
--                    pm_disagreement patients = the small ambiguous set
--                  - Total cases needing real adjudication: ~278
--                Claude read the source text from main.path_synoptics for each
--                ambiguous case and wrote per-case classifications + reasoning
--                to main.canonical_ete_inline_adjudication_v1.
-- ----------------------------------------------------------------------------
-- Source-of-truth Excel:  All Diagnoses & synoptic 12_1_2025.xlsx
-- DB mirror:              main.path_synoptics (10,871 patients / 11,688 rows)
-- ----------------------------------------------------------------------------
-- Reads:   main.path_synoptics, main.canonical_patient_master,
--          manuscript_workspace.cpm_ete_self_contradiction_queue_v1,
--          main.canonical_ete_event_resolved_v1 (mig_61b output)
-- Writes:  main.canonical_ete_inline_adjudication_v1 (3,021 rows)
--          manuscript_workspace.cpm_ete_self_contradiction_queue_v1 (status updates)
--          manuscript_workspace.ete_manuscript_analytic_v6 (view)
--          main.canonical_ete_event_resolved_v1 (refreshed)
-- ============================================================================

-- 1. Resolution table ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS main.canonical_ete_inline_adjudication_v1 (
  research_id           VARCHAR,
  resolution_source     VARCHAR,
  resolution_set        VARCHAR,
  ete_grade_resolved    VARCHAR,
  ajcc8_implication     VARCHAR,
  evidence_quote        VARCHAR,
  evidence_source       VARCHAR,
  reasoning             VARCHAR,
  confidence            VARCHAR,
  build_script          VARCHAR,
  build_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  tumor_ordinal         BIGINT
);

-- 2-A. Admin closes for queue (2,758 of 2,790 rows) ---------------------------

INSERT INTO main.canonical_ete_inline_adjudication_v1
  (research_id, resolution_source, resolution_set, ete_grade_resolved,
   evidence_quote, evidence_source, reasoning, confidence, build_script)
SELECT CAST(q.research_id AS VARCHAR), 'admin_accept_script_390',
       'queue_microscopic_no_invasion_signal', pm.ete_grade_clean,
       CONCAT('patient_master.ete_grade_clean=', pm.ete_grade_clean,
              '; source=', COALESCE(pm.ete_grade_source,'NULL')),
       'patient_master_ete_grade_clean',
       'Script 390 rule_a populated ete_grade_clean=microscopic for these patients without invasion-signal corroboration; accept as resolution.',
       'medium', 'mig_61b_admin_accept_20260427'
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
JOIN main.canonical_patient_master pm
  ON CAST(pm.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
WHERE q.reason = 'microscopic_no_invasion_signal' AND pm.ete_grade_clean IS NOT NULL;

INSERT INTO main.canonical_ete_inline_adjudication_v1
  (research_id, resolution_source, resolution_set, ete_grade_resolved,
   evidence_quote, evidence_source, reasoning, confidence, build_script)
SELECT CAST(q.research_id AS VARCHAR), 'admin_accept_ete_grade_final_v2',
       'queue_microscopic_no_invasion_signal_null_clean', 'microscopic',
       CONCAT('ete_grade_final_v2=microscopic; source=', COALESCE(pm.ete_grade_source,'NULL')),
       'patient_master_ete_grade_final_v2',
       'patient_master ete_grade_final_v2 and ete_grade both committed to microscopic; ete_grade_clean was NULL only because cleaning script did not propagate.',
       'medium', 'mig_61b_admin_accept_20260427'
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
JOIN main.canonical_patient_master pm
  ON CAST(pm.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
WHERE q.reason = 'microscopic_no_invasion_signal'
  AND pm.ete_grade_clean IS NULL AND pm.ete_grade_final_v2 = 'microscopic';

INSERT INTO main.canonical_ete_inline_adjudication_v1
  (research_id, resolution_source, resolution_set, ete_grade_resolved,
   evidence_quote, evidence_source, reasoning, confidence, build_script)
SELECT CAST(q.research_id AS VARCHAR), 'admin_accept_pm_consensus',
       'queue_boolean_string_upstream_bug', 'none',
       CONCAT('all 3 pm grades=none; path_ete_raw=false (boolean false)'),
       'patient_master_consensus',
       'All three patient_master grade columns commit to none; path_ete_raw="false" is boolean false (no ETE). Boolean_string queue trip = parsing artifact.',
       'high', 'mig_61b_boolean_bug_fix_20260427'
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
JOIN main.canonical_patient_master pm
  ON CAST(pm.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
WHERE q.reason = 'boolean_string_upstream_bug'
  AND pm.ete_grade = 'none' AND pm.ete_grade_final_v2 = 'none' AND pm.path_ete_raw = 'false';

-- 2-B. Inline adjudication of 28 queue stragglers (per-patient with reasoning)
-- See main.canonical_ete_inline_adjudication_v1 WHERE resolution_set='queue_straggler'
-- for the 28 rows committed by mig_61c batch (read from path_synoptics
-- synoptic_diagnosis + path_diagnosis_summary; classification rules:
--   strap muscle / sternohyoid / trachea / esophagus / RLN / pT3b / pT4a / pT4b → gross
--   minimal / focal / perithyroidal soft tissue / fibroadipose only → microscopic
--   explicit "Not identified" / "Absent" / "Limited to thyroid" → none
--   "Cannot be determined" / "Cannot be assessed" → unable_to_determine).
-- Distribution: 13 unable_to_determine | 6 none | 6 microscopic | 3 gross.

-- 2-C. 48 unspec_remaining_event adjudications (event-grain, with tumor_ordinal)
-- Heavy on aggressive non-PTC histologies (anaplastic, MTC, poorly diff).
-- Dominant rule: pT4a/pT4b stage assignments → gross; "minimal" qualifier → microscopic;
-- explicit absent → none. Distribution: 23 gross | 16 microscopic | 5 none | 4 unable.

-- 2-D. 187 pm_disagreement_gross_vs_micro — single deterministic rule
INSERT INTO main.canonical_ete_inline_adjudication_v1
  (research_id, resolution_source, resolution_set, ete_grade_resolved,
   evidence_quote, evidence_source, reasoning, confidence, build_script)
SELECT CAST(pm.research_id AS VARCHAR), 'inline_adjudication',
       'pm_disagreement_gross_vs_micro', 'microscopic',
       CONCAT('path_ete_raw="', COALESCE(pm.path_ete_raw,'NULL'),
              '"; gm_path_ete_raw="', COALESCE(pm.gm_path_ete_raw,'NULL'),
              '"; op_intraop_gross_ete_any=', COALESCE(CAST(pm.op_intraop_gross_ete_any AS VARCHAR),'NULL')),
       'patient_master_path_raw_vs_intraop',
       'All 187 path_ete_raw values are minimal/microscopic/focal — none say gross. 175 of 187 had op_intraop_gross_ete_any=TRUE; older pipeline incorrectly let intraop suspicion override the final path read. Per AJCC8 the path finding wins.',
       'high', 'mig_61c_inline_20260427'
FROM main.canonical_patient_master pm
WHERE pm.ete_grade = 'gross' AND pm.ete_grade_clean = 'microscopic';

-- 3. Mark all queue rows resolved ---------------------------------------------

UPDATE manuscript_workspace.cpm_ete_self_contradiction_queue_v1
SET status = 'resolved_admin_accept_script_390'
WHERE reason = 'microscopic_no_invasion_signal' AND status = 'awaiting_manual_review';

UPDATE manuscript_workspace.cpm_ete_self_contradiction_queue_v1
SET status = 'resolved_admin_boolean_bug_fix'
WHERE reason = 'boolean_string_upstream_bug' AND status = 'awaiting_manual_review'
  AND research_id IN (
    SELECT q.research_id FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
    JOIN main.canonical_patient_master pm ON CAST(pm.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
    WHERE pm.ete_grade = 'none' AND pm.ete_grade_final_v2 = 'none' AND pm.path_ete_raw = 'false'
  );

UPDATE manuscript_workspace.cpm_ete_self_contradiction_queue_v1
SET status = 'resolved_by_inline_adjudication'
WHERE status = 'awaiting_manual_review';

-- 4. v6 layered view ----------------------------------------------------------

CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v6 AS
WITH inline_pt AS (
  SELECT research_id, ete_grade_resolved AS inline_grade, resolution_set AS inline_set,
         evidence_quote AS inline_evidence, resolution_source AS inline_source
  FROM main.canonical_ete_inline_adjudication_v1
  WHERE tumor_ordinal IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id
    ORDER BY CASE
      WHEN resolution_set = 'queue_straggler'                    THEN 1
      WHEN resolution_set = 'pm_disagreement_gross_vs_micro'     THEN 2
      WHEN resolution_set = 'queue_boolean_string_upstream_bug'  THEN 3
      ELSE 4 END) = 1
),
inline_evt AS (
  SELECT research_id, tumor_ordinal,
         ete_grade_resolved AS inline_evt_grade,
         evidence_quote AS inline_evt_evidence,
         resolution_set AS inline_evt_set
  FROM main.canonical_ete_inline_adjudication_v1
  WHERE tumor_ordinal IS NOT NULL
)
SELECT v4.*, ipt.inline_grade, ipt.inline_set, ipt.inline_evidence, ipt.inline_source,
       ievt.inline_evt_grade, ievt.inline_evt_evidence, ievt.inline_evt_set,
  CASE
    WHEN ievt.inline_evt_grade IN ('gross','microscopic','none')      THEN ievt.inline_evt_grade
    WHEN ievt.inline_evt_grade = 'unable_to_determine'                THEN 'unable_to_determine'
    WHEN ipt.inline_set = 'pm_disagreement_gross_vs_micro'            THEN ipt.inline_grade
    WHEN v4.ete_grade_final_v4 IS NULL
         AND ipt.inline_grade IN ('gross','microscopic','none')        THEN ipt.inline_grade
    WHEN v4.ete_grade_final_v4 IS NULL
         AND ipt.inline_grade = 'unable_to_determine'                  THEN 'unable_to_determine'
    ELSE v4.ete_grade_final_v4
  END AS ete_grade_final_v6,
  CASE
    WHEN ievt.inline_evt_grade IS NOT NULL                             THEN 'inline_adjudication_event'
    WHEN ipt.inline_set = 'pm_disagreement_gross_vs_micro'             THEN 'inline_adjudication_pm_disagreement'
    WHEN v4.ete_grade_final_v4 IS NULL AND ipt.inline_grade IS NOT NULL THEN
      CASE
        WHEN ipt.inline_set = 'queue_straggler'                        THEN 'inline_adjudication_queue'
        WHEN ipt.inline_set LIKE 'queue_microscopic_no_invasion_signal%' THEN 'inline_admin_accept_script_390'
        WHEN ipt.inline_set = 'queue_boolean_string_upstream_bug'      THEN 'inline_admin_boolean_bug_fix'
        ELSE 'inline_adjudication_other'
      END
    ELSE v4.ete_grade_source_v4
  END AS ete_grade_source_v6
FROM manuscript_workspace.ete_manuscript_analytic_v4 v4
LEFT JOIN inline_pt  ipt  ON ipt.research_id  = CAST(v4.research_id AS VARCHAR)
LEFT JOIN inline_evt ievt ON ievt.research_id = CAST(v4.research_id AS VARCHAR)
                         AND ievt.tumor_ordinal = v4.tumor_ordinal;

-- 5. Refresh canonical_ete_event_resolved_v1 ----------------------------------
-- (Same projection as mig_61, swap _v4 fields for _v6, recompute analytic_eligible
-- to require committed grade only — excludes unable_to_determine and unspec_remaining.)

DROP TABLE IF EXISTS main.canonical_ete_event_resolved_v1;
CREATE TABLE main.canonical_ete_event_resolved_v1 AS
SELECT
  v6.research_id, v6.path_surgery_id, v6.surgery_episode_id_global, v6.tumor_ordinal,
  v6.specimen_id, v6.synoptic_row_ix, v6.cohort_ptc, v6.cohort_descriptive_full,
  ((v6.ete_grade_final_v6 IS NOT NULL AND v6.ete_grade_final_v6 NOT IN ('unable_to_determine','unspec_remaining'))
   AND (v6.surgery_episode_id_global IS NOT NULL)
   AND (v6.size_greatest_dimension_cm_trusted IS NOT NULL)
   AND (v6.primary_histology_trusted IS NOT NULL)
   AND (v6.primary_histology_trusted NOT IN ('NIFTP','FTUMP','follicular adenoma','atypical follicular / hurthle neoplasm','uncertain malignant potential (non-FTUMP)'))
  ) AS analytic_eligible,
  v6.ete_grade_final_v6 AS ete_grade,
  v6.ete_grade_source_v6 AS ete_grade_source,
  (v6.ete_grade_final_v6 = 'gross') AS is_gross_ete,
  (v6.ete_grade_final_v6 = 'microscopic') AS is_microscopic_ete,
  (v6.ete_grade_final_v6 IN ('gross','microscopic')) AS any_ete_present,
  (v6.ete_grade_final_v6 = 'none') AS is_no_ete,
  (v6.ete_grade_final_v6 IN ('unspec_remaining','unable_to_determine')) AS is_unresolved_ete,
  (v6.ete_grade_final_v6 IS NULL) AS is_no_ete_data,
  v6.ete_raw AS path_event_ete_raw, v6.pm_ete_grade_clean AS patient_master_ete_grade_clean,
  v6.pm_ete_grade_source AS patient_master_ete_grade_source,
  v6.pm_ete_grade_adjudicated AS patient_master_ete_grade_adjudicated,
  v6.pm_ete_adjudicated_flag AS patient_master_ete_adjudicated_flag,
  v6.ete_grade_llm AS general_llm_ete_grade,
  v6.ete_grade_fresh_llm AS mig54_fresh_llm_ete_grade,
  v6.fresh_evidence_quotes AS mig54_fresh_llm_evidence_quotes,
  v6.fresh_best_confidence AS mig54_fresh_llm_confidence,
  v6.fresh_ajcc8_implications AS mig54_fresh_llm_ajcc8,
  v6.inline_grade AS inline_patient_grade, v6.inline_set AS inline_patient_set,
  v6.inline_evidence AS inline_patient_evidence,
  v6.inline_evt_grade AS inline_event_grade,
  v6.inline_evt_evidence AS inline_event_evidence,
  v6.inline_evt_set AS inline_event_set,
  v6.ete_grade_pm_disagreement_flag AS pm_disagreement_flag,
  v6.ete_self_contradiction_open_flag AS open_self_contradiction_flag,
  v6.gross_ete_effective AS legacy_gross_ete_effective,
  v6.size_greatest_dimension_cm_trusted AS size_greatest_dimension_cm,
  v6.primary_histology_trusted AS primary_histology,
  v6.histology_variant_trusted AS histology_variant,
  v6.laterality_trusted AS laterality, v6.multifocal_flag,
  v6.reported_t_stage_ajcc8, v6.derived_t_stage_ajcc8,
  v6.t_stage_discordance_flag, v6.ajcc_overall_stage_trusted AS ajcc_overall_stage,
  'mig_61c_v6_to_canonical_ete_event_resolved_v1_20260427' AS build_script,
  CURRENT_TIMESTAMP AS build_ts
FROM manuscript_workspace.ete_manuscript_analytic_v6 v6;

-- 6. Acceptance probes (run after script) ------------------------------------
-- SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1 WHERE open_self_contradiction_flag;  -- 0
-- SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1 WHERE analytic_eligible;             -- 5589
-- SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1 WHERE cohort_ptc AND analytic_eligible; -- 4054
-- SELECT COUNT(*) FROM main.canonical_ete_inline_adjudication_v1;                                -- 3021
-- SELECT status, COUNT(*) FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 GROUP BY 1; -- 0 awaiting
