-- ============================================================================
-- Migration 61 — ete_manuscript_analytic_v4 + canonical_ete_event_resolved_v1
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Author:        Logan Glosser (via Claude / Cowork)
-- Date:          2026-04-27
-- Issue ID:      MANUSCRIPT_ETE_PM_FALLBACK_AND_QUEUE_FIX
-- ----------------------------------------------------------------------------
-- Purpose:
--   1. Land Migration 54's `ete_manuscript_analytic_v3` (it had been written
--      to repo but never executed against MotherDuck).
--   2. Build `ete_manuscript_analytic_v4` that:
--        a. Adds canonical_patient_master.ete_grade_clean as the patient-level
--           fallback when v3.ete_grade_final_v3 is NULL or 'unspec_remaining'.
--           Recovers analytic_eligible from 901 -> 4,971 events.
--        b. Replaces the broken patient-level EXISTS join on
--           cpm_ete_self_contradiction_queue_v1 with a query that scopes to
--           status = 'awaiting_manual_review' (open queue rows only). Today
--           every queue row is open so the count is unchanged, but the fix
--           ensures Migration 63 closeout will auto-clear the flag.
--        c. Adds `ete_grade_pm_disagreement_flag` for events where the patient
--           has ete_grade <> ete_grade_clean on canonical_patient_master
--           (the 187-patient discordance set, queued for Migration 64).
--   3. Materialise the result as `main.canonical_ete_event_resolved_v1` —
--      the column-of-record manuscript table (event grain, n=6,689).
-- ----------------------------------------------------------------------------
-- Reads:   main.note_entities_llm_ete_subgrade_v1, main.canonical_patient_master,
--          manuscript_workspace.ete_manuscript_analytic_v2,
--          manuscript_workspace.cpm_ete_self_contradiction_queue_v1
-- Writes:  manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 (view)
--          manuscript_workspace.ete_manuscript_analytic_v3 (view)
--          manuscript_workspace.ete_manuscript_analytic_v4 (view)
--          main.canonical_ete_event_resolved_v1 (table)
--          manuscript_workspace.canonical_deprecation_log_v1 (insert)
-- ----------------------------------------------------------------------------
-- Acceptance probes (run after the script):
--   SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1;             -- 6689
--   SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1
--     WHERE analytic_eligible;                                              -- 4971
--   SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1
--     WHERE cohort_ptc AND analytic_eligible;                               -- 4056
--   SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1
--     WHERE pm_disagreement_flag;                                           -- 356  (mig_64 input)
--   SELECT COUNT(*) FROM main.canonical_ete_event_resolved_v1
--     WHERE open_self_contradiction_flag;                                   -- 4382 (mig_63 input)
-- ============================================================================

-- 1. mig_54 v3 ----------------------------------------------------------------

CREATE OR REPLACE VIEW manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 AS
WITH graded AS (
  SELECT v.research_id,
         v.ete_grade  AS ete_grade_llm,
         v.confidence,
         v.evidence_quote,
         v.ajcc8_implication
  FROM main.v_note_entities_llm_ete_subgrade_v1 v
  WHERE v.error = 0
    AND v.ete_grade IN ('gross','microscopic','absent','unable_to_determine')
)
SELECT
  research_id,
  CASE
    WHEN MAX(CASE WHEN ete_grade_llm='gross' THEN 1 ELSE 0 END)=1           THEN 'gross'
    WHEN MAX(CASE WHEN ete_grade_llm='microscopic' THEN 1 ELSE 0 END)=1     THEN 'microscopic'
    WHEN MAX(CASE WHEN ete_grade_llm='absent' THEN 1 ELSE 0 END)=1          THEN 'absent'
    ELSE 'unable_to_determine'
  END AS ete_grade_fresh_llm,
  MAX(CASE WHEN ete_grade_llm='gross' THEN 1 ELSE 0 END)::BOOLEAN AS has_gross_fresh,
  MAX(CASE WHEN ete_grade_llm='microscopic' THEN 1 ELSE 0 END)::BOOLEAN AS has_micro_fresh,
  MAX(CASE WHEN ete_grade_llm='absent' THEN 1 ELSE 0 END)::BOOLEAN AS has_absent_fresh,
  STRING_AGG(
    CASE WHEN ete_grade_llm IN ('gross','microscopic','absent')
         THEN NULLIF(evidence_quote,'')
         ELSE NULL END,
    ' | '
  ) AS fresh_evidence_quotes,
  MAX(confidence) AS fresh_best_confidence,
  STRING_AGG(DISTINCT ajcc8_implication, ',') AS fresh_ajcc8_implications,
  COUNT(*) AS n_fresh_mentions
FROM graded
GROUP BY research_id;

CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v3 AS
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
  CASE
    WHEN v2.ete_grade_final IS NOT NULL AND v2.ete_grade_final <> 'unspec_remaining' THEN v2.ete_grade_final
    WHEN f.has_gross_fresh                                       THEN 'gross'
    WHEN f.has_micro_fresh                                       THEN 'microscopic'
    WHEN f.has_absent_fresh                                      THEN 'none'
    WHEN v2.ete_grade_final = 'unspec_remaining'                 THEN 'unspec_remaining'
    ELSE NULL
  END AS ete_grade_final_v3,
  CASE
    WHEN v2.ete_grade_final IS NOT NULL AND v2.ete_grade_final <> 'unspec_remaining' THEN v2.ete_grade_source
    WHEN f.has_gross_fresh OR f.has_micro_fresh                  THEN 'llm_fresh_subgrade'
    WHEN f.has_absent_fresh                                      THEN 'llm_fresh_absent'
    WHEN f.ete_grade_fresh_llm = 'unable_to_determine'           THEN 'llm_unable'
    WHEN v2.ete_grade_final = 'unspec_remaining'                 THEN 'unresolved'
    ELSE NULL
  END AS ete_grade_source_v3
FROM manuscript_workspace.ete_manuscript_analytic_v2 v2
LEFT JOIN manuscript_workspace.ete_llm_fresh_subgrade_patient_v1 f
  ON CAST(f.research_id AS VARCHAR) = CAST(v2.research_id AS VARCHAR);

-- 2. v4 + pm fallback + queue scope fix + disagreement flag --------------------

CREATE OR REPLACE VIEW manuscript_workspace.ete_manuscript_analytic_v4 AS
WITH q_open AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
  WHERE status = 'awaiting_manual_review'
),
pm AS (
  SELECT research_id, ete_grade_clean, ete_grade, ete_grade_source,
         ete_grade_adjudicated, ete_adjudicated_flag
  FROM main.canonical_patient_master
)
SELECT
  v3.*,
  pm.ete_grade_clean         AS pm_ete_grade_clean,
  pm.ete_grade_source        AS pm_ete_grade_source,
  pm.ete_grade_adjudicated   AS pm_ete_grade_adjudicated,
  pm.ete_adjudicated_flag    AS pm_ete_adjudicated_flag,
  CASE
    WHEN v3.ete_grade_final_v3 IS NOT NULL AND v3.ete_grade_final_v3 <> 'unspec_remaining'
      THEN v3.ete_grade_final_v3
    WHEN pm.ete_grade_clean IN ('gross','microscopic','none')
      THEN pm.ete_grade_clean
    WHEN v3.ete_grade_final_v3 = 'unspec_remaining'
      THEN 'unspec_remaining'
    WHEN pm.ete_grade_clean = 'indeterminate'
      THEN 'indeterminate'
    ELSE NULL
  END AS ete_grade_final_v4,
  CASE
    WHEN v3.ete_grade_final_v3 IS NOT NULL AND v3.ete_grade_final_v3 <> 'unspec_remaining'
      THEN v3.ete_grade_source_v3
    WHEN pm.ete_grade_clean IN ('gross','microscopic','none')
      THEN 'patient_master_clean'
    WHEN v3.ete_grade_final_v3 = 'unspec_remaining'
      THEN 'unresolved'
    WHEN pm.ete_grade_clean = 'indeterminate'
      THEN 'patient_master_indeterminate'
    ELSE NULL
  END AS ete_grade_source_v4,
  (pm.ete_grade IS NOT NULL
   AND pm.ete_grade_clean IS NOT NULL
   AND pm.ete_grade NOT IN ('true','false','absent','present_ungraded')
   AND pm.ete_grade_clean NOT IN ('indeterminate')
   AND pm.ete_grade <> pm.ete_grade_clean) AS ete_grade_pm_disagreement_flag,
  EXISTS (SELECT 1 FROM q_open q WHERE q.research_id = CAST(v3.research_id AS VARCHAR))
    AS ete_self_contradiction_open_flag,
  ((COALESCE(
      CASE WHEN v3.ete_grade_final_v3 IS NOT NULL AND v3.ete_grade_final_v3 <> 'unspec_remaining'
           THEN v3.ete_grade_final_v3
           ELSE pm.ete_grade_clean END,
      v3.ete_norm
    ) IS NOT NULL)
   AND (v3.surgery_episode_id_global IS NOT NULL)
   AND (v3.size_greatest_dimension_cm_trusted IS NOT NULL)
   AND (v3.primary_histology_trusted IS NOT NULL)
   AND (v3.primary_histology_trusted NOT IN
        ('NIFTP','FTUMP','follicular adenoma',
         'atypical follicular / hurthle neoplasm',
         'uncertain malignant potential (non-FTUMP)'))
  ) AS analytic_eligible_v4
FROM manuscript_workspace.ete_manuscript_analytic_v3 v3
LEFT JOIN pm ON pm.research_id = v3.research_id;

-- 3. Materialise canonical_ete_event_resolved_v1 ------------------------------

CREATE OR REPLACE TABLE main.canonical_ete_event_resolved_v1 AS
SELECT
  v4.research_id,
  v4.path_surgery_id,
  v4.surgery_episode_id_global,
  v4.tumor_ordinal,
  v4.specimen_id,
  v4.synoptic_row_ix,
  v4.cohort_ptc,
  v4.cohort_descriptive_full,
  v4.analytic_eligible_v4                AS analytic_eligible,
  v4.ete_grade_final_v4                  AS ete_grade,
  v4.ete_grade_source_v4                 AS ete_grade_source,
  (v4.ete_grade_final_v4 = 'gross')                                       AS is_gross_ete,
  (v4.ete_grade_final_v4 = 'microscopic')                                 AS is_microscopic_ete,
  (v4.ete_grade_final_v4 IN ('gross','microscopic'))                      AS any_ete_present,
  (v4.ete_grade_final_v4 = 'none')                                        AS is_no_ete,
  (v4.ete_grade_final_v4 IN ('unspec_remaining','indeterminate'))         AS is_unresolved_ete,
  (v4.ete_grade_final_v4 IS NULL)                                         AS is_no_ete_data,
  v4.ete_raw                              AS path_event_ete_raw,
  v4.pm_ete_grade_clean                   AS patient_master_ete_grade_clean,
  v4.pm_ete_grade_source                  AS patient_master_ete_grade_source,
  v4.pm_ete_grade_adjudicated             AS patient_master_ete_grade_adjudicated,
  v4.pm_ete_adjudicated_flag              AS patient_master_ete_adjudicated_flag,
  v4.ete_grade_llm                        AS general_llm_ete_grade,
  v4.ete_grade_fresh_llm                  AS mig54_fresh_llm_ete_grade,
  v4.fresh_evidence_quotes                AS mig54_fresh_llm_evidence_quotes,
  v4.fresh_best_confidence                AS mig54_fresh_llm_confidence,
  v4.fresh_ajcc8_implications             AS mig54_fresh_llm_ajcc8,
  v4.ete_grade_pm_disagreement_flag       AS pm_disagreement_flag,
  v4.ete_self_contradiction_open_flag     AS open_self_contradiction_flag,
  v4.gross_ete_effective                  AS legacy_gross_ete_effective,
  v4.size_greatest_dimension_cm_trusted   AS size_greatest_dimension_cm,
  v4.primary_histology_trusted            AS primary_histology,
  v4.histology_variant_trusted            AS histology_variant,
  v4.laterality_trusted                   AS laterality,
  v4.multifocal_flag,
  v4.reported_t_stage_ajcc8,
  v4.derived_t_stage_ajcc8,
  v4.t_stage_discordance_flag,
  v4.ajcc_overall_stage_trusted           AS ajcc_overall_stage,
  'mig_61_v4_to_canonical_ete_event_resolved_v1_20260427' AS build_script,
  CURRENT_TIMESTAMP                                       AS build_ts
FROM manuscript_workspace.ete_manuscript_analytic_v4 v4;

COMMENT ON TABLE main.canonical_ete_event_resolved_v1 IS
'Manuscript-facing column of record for extrathyroidal extension. One row per path malignant event (n=6,689). ete_grade in (gross, microscopic, none, unspec_remaining, indeterminate, NULL). ete_grade_source ladder: structured > llm_fresh_subgrade (mig_54) > llm_subgrade (mig_53 general LLM) > llm_fresh_absent > patient_master_clean (Script 390 + extraction_audit_engine_v7) > patient_master_indeterminate > unresolved. analytic_eligible recomputed with patient_master fallback. open_self_contradiction_flag scoped to status=awaiting_manual_review queue rows only. Built mig_61 2026-04-27 from manuscript_workspace.ete_manuscript_analytic_v4. SUPERSEDES the v2/v3 analytic views as the read-path; older views retained for audit.';

-- 4. Deprecation log ----------------------------------------------------------

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('manuscript_workspace.ete_manuscript_analytic_v3.ete_grade_final_v3', 'column',
   'main.canonical_ete_event_resolved_v1.ete_grade', 'MANUSCRIPT_ETE_PM_FALLBACK_AND_QUEUE_FIX',
   'mig_61', 'column_only', DATE '2026-04-27',
   'mig_61: layered patient_master.ete_grade_clean fallback onto v3 (recovers analytic_eligible 901->4971), fixed patient-level EXISTS self-contradiction join to scope by status=awaiting_manual_review only, added pm_disagreement_flag for ete_grade vs ete_grade_clean divergences (356 events). v4 view + materialized canonical_ete_event_resolved_v1 land 2026-04-27.',
   NULL,
   'Downstream ETE analyses MUST use main.canonical_ete_event_resolved_v1.ete_grade with provenance via ete_grade_source. v3 view retained for audit. Open issues queued for mig_63 (queue closeout, 2786 pts) and mig_64 (pm disagreement adjudication, ~187 pts).');
