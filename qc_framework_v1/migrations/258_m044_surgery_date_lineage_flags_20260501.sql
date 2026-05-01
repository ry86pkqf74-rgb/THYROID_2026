-- =============================================================================
-- mig_258 — M044 cohort surgery-date lineage + manuscript date-window flags
--
-- Date: 2026-05-01
-- DB:   thyroid_canonical_publication_v1_0.manuscript_workspace
--
-- CONTEXT
--   `surg_first_date` SSOT is `main.canonical_patient_master.surg_first_date` (DATE).
--   mig_254 backfilled NULL operative-spine gaps from `first_surgery_date_v2`
--   (operative / path_synoptics-aligned earliest surgery). The M044 analytic cohort
--   must expose that column directly plus explicit study-window booleans — do not re-
--   derive earliest surgery inside the cohort view from episode tables here.
--
--   Recurrence time-to-event in M044 joins `canonical_recurrence_resolved_v1` for
--   `days_to_path_proven`; surgery anchor for eligibility and logistic S2 mirrors
--   `CAST(surg_first_date AS DATE)` from this lineage.
--
-- POST-APPLY (expect after mig_254 + current CPM):
--   See `scripts/m044_validate_canonical_v1.sql` — QUERY: surgery_date_lineage
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
  CAST(p.surg_first_date AS DATE) AS surg_first_date,

  CAST(
    'main.canonical_patient_master.surg_first_date; mig_254 backfill surg_first=NULL from first_surgery_date_v2'
    AS VARCHAR
  ) AS surg_first_date_lineage_note,

  (CAST(p.surg_first_date AS DATE) IS NULL) AS surg_date_missing,
  (
    CAST(p.surg_first_date AS DATE) IS NOT NULL
    AND CAST(p.surg_first_date AS DATE) < DATE '1999-01-01'
  ) AS surg_date_pre_1999,
  (
    CAST(p.surg_first_date AS DATE) IS NOT NULL
    AND CAST(p.surg_first_date AS DATE) BETWEEN DATE '1999-01-01' AND DATE '2024-12-31'
  ) AS surg_date_1999_2024,
  (
    CAST(p.surg_first_date AS DATE) IS NOT NULL
    AND CAST(p.surg_first_date AS DATE) > DATE '2024-12-31'
  ) AS surg_date_post_2024,
  (
    CAST(p.surg_first_date AS DATE) IS NOT NULL
    AND CAST(p.surg_first_date AS DATE) > DATE '2024-06-04'
  ) AS surg_date_after_2024_06_04
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
