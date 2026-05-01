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
-- PREREQ: `manuscript_workspace.cohort_m044_ajcc_ete_v1` must compile. If MotherDuck
--   raises BinderException (TIMESTAMP vs DATE) when selecting the cohort view, rebuild
--   dependent manuscript_workspace views per clinical_date_retype / gate5 playbook before
--   applying this migration.
--
--   SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;
--   Example row observed 2026-05-01: legacy_any_recurrence_true_n = 503,
--   legacy_any_true_canonical_status_none_n = 318,
--   legacy_structural_recurrence_true_n = 1817,
--   legacy_structural_true_canonical_status_none_n = 1588.
--
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

DROP VIEW IF EXISTS manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;

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
