-- =============================================================================
-- mig_257 — M044 legacy recurrence flag QC view (vs canonical_recurrence_resolved_v1)
--
-- Date: 2026-05-01
-- Target: manuscript_workspace.m044_legacy_recurrence_flag_audit_v1
--
-- PURPOSE
--   Single-row summary for the M044 analytic cohort comparing **legacy** CPM flags
--   (`canonical_patient_master.any_recurrence_flag`, `structural_recurrence_flag`)
--   to `main.canonical_recurrence_resolved_v1.recurrence_status_final`.
--
--   Do **not** use legacy flags as manuscript endpoints; primary = path-proven /
--   dual-track fields on canonical_recurrence_resolved_v1 (see M044 methods).
--
-- §0 Rebind `cohort_m044_ajcc_ete_v1` (required after mig_160b PM clinical DATE retype).
--   Stale view catalog expected TIMESTAMP for `surg_first_date`; CPM column is DATE →
--   BinderException "types don't match ... TIMESTAMP ... DATE". Drop + recreate cohort,
--   then build the audit view.
--
-- POST-APPLY:
--   SELECT COUNT(*) FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;  -- expect 4128
--   SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;
--   (Live legacy_* counts drift with CPM / canonical_recurrence_resolved_v1; not frozen.)
--
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

DROP VIEW IF EXISTS manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;
DROP VIEW IF EXISTS manuscript_workspace.cohort_m044_ajcc_ete_v1;

CREATE VIEW manuscript_workspace.cohort_m044_ajcc_ete_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.histology_final,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.ete_grade_final,
  p.ete_grade,
  p.ete_grade_source,
  p.gross_ete_flag,
  p.path_gross_ete_flag,
  p.ete_op_note_grade,
  p.ete_original_grade,
  p.ajcc8_t_stage,
  p.ajcc8_n_stage,
  p.ajcc8_m_stage,
  p.ajcc8_stage_group,
  p.ln_positive_flag,
  p.ln_total_positive,
  p.lvi_grade,
  p.vascular_invasion_final,
  p.ata_risk_category,
  p.rai_received_reconciled AS rai_received_flag,
  p.any_recurrence_flag,
  p.structural_recurrence_flag,
  p.followup_years,
  p.overall_survival_years,
  p.death_occurred,
  p.surg_procedure_type,
  CAST(p.surg_first_date AS DATE) AS surg_first_date
FROM main.canonical_patient_master AS p
WHERE p.is_malignant IS TRUE
  AND p.ajcc8_stage_group IS NOT NULL;

CREATE VIEW manuscript_workspace.m044_legacy_recurrence_flag_audit_v1 AS
WITH base AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    p.any_recurrence_flag,
    p.structural_recurrence_flag,
    r.recurrence_status_final
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 AS c
  INNER JOIN main.canonical_patient_master AS p
    ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR)
  LEFT JOIN main.canonical_recurrence_resolved_v1 AS r
    ON CAST(c.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
)
SELECT
  COUNT(*) AS m044_cohort_n,
  SUM(CASE WHEN any_recurrence_flag IS TRUE THEN 1 ELSE 0 END) AS legacy_any_recurrence_true_n,
  SUM(
    CASE
      WHEN any_recurrence_flag IS TRUE AND recurrence_status_final = 'none'
      THEN 1 ELSE 0 END
  ) AS legacy_any_true_canonical_status_none_n,
  SUM(CASE WHEN structural_recurrence_flag IS TRUE THEN 1 ELSE 0 END) AS legacy_structural_recurrence_true_n,
  SUM(
    CASE
      WHEN structural_recurrence_flag IS TRUE AND recurrence_status_final = 'none'
      THEN 1 ELSE 0 END
  ) AS legacy_structural_true_canonical_status_none_n,
  SUM(CASE WHEN recurrence_status_final IS NULL THEN 1 ELSE 0 END) AS canonical_recurrence_row_missing_n
FROM base;
