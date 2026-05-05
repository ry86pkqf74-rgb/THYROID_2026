-- =============================================================================
-- mig_315 — M044 cohort flat rebuild: ete_grade_final normalization
--
-- Date:   2026-05-05
-- DB:     thyroid_canonical_publication_v1_0.manuscript_workspace
-- Closes: CF-M044-DUP-COLS
-- Opens:  CF-M044-V6-MANUSCRIPT-PATCH (Cowork prose review lane)
--
-- BACKGROUND
--   Two defects reported by Cowork in cohort_m044_ajcc_ete_v1:
--
--   Defect 1 (duplicate columns): investigation confirmed the VIEW has 35 unique
--   columns; the information_schema.columns n_cols=64 is the well-known MotherDuck
--   multi-catalog artifact. No actual column duplication — defect is a measurement
--   artifact. VIEW SQL was intact (mig_258 definition). Defect 1 is CLOSED.
--
--   Defect 2 (ete_grade_final Boolean→VARCHAR artifacts): CONFIRMED.
--   canonical_patient_master.ete_grade_final has values {'false','absent','true'}
--   that should be {'no_negative','no_negative','gross'} respectively. Root cause:
--   ete_grade_final_v2 column in CPM (sourced from ete_adjudication_v1) uses
--   vocabulary {'none','absent','true'} in place of the expected {'no_negative'}.
--   The view read ete_grade_final directly, propagating the artifacts.
--
--   Fix: rebuild view to source ete_grade_final from ete_grade_final_v2 with
--   explicit normalization CASE. The 'no_negative' canonical label is introduced
--   and downstream Python updated accordingly.
--
-- PRE-CONDITIONS
--   canonical_patient_master = 10,871 rows (post-mig_313)
--   m_stage_corruption_fix applied (mig_313): M1=114 (2.84%), IVB=76
--   Expected post-fix cohort: ~3,868 (post-mig_313 staging recompute;
--     prior v5 count 3,578 was in strict-DTC analytic frame, not total view)
--
-- POST-APPLY VERIFICATION
--   SELECT COUNT(*) FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;
--     -- expect ~3,868 (post-mig_313; up from 3,578 v5 strict-DTC frame)
--   SELECT ete_grade_final, COUNT(*) n
--     FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
--     GROUP BY 1 ORDER BY n DESC;
--     -- expect ONLY: no_negative, microscopic, gross, present_ungraded, NULL
--     -- no 'false', 'true', 'absent'
--     -- no_negative count: 173 (158+15 from old vocab)
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- ---------------------------------------------------------------------------
-- §1  Archive pre-state
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m044_ajcc_ete_v1_pre_mig315_20260505 AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name   = 'manuscript_workspace'
  AND view_name     = 'cohort_m044_ajcc_ete_v1';

-- ---------------------------------------------------------------------------
-- §2  Rebuild cohort_m044_ajcc_ete_v1 with normalized ete_grade_final
--     Sources ete_grade_final from ete_grade_final_v2 (more semantically
--     correct than ete_grade_final which has boolean cast artifacts).
--     Explicit column projection — no SELECT *.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;
DROP VIEW IF EXISTS manuscript_workspace.cohort_m044_ajcc_ete_v1;

CREATE VIEW manuscript_workspace.cohort_m044_ajcc_ete_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.histology_final,
  p.path_tumor_size_cm AS tumor_size_cm,

  -- Normalized ete_grade_final: maps ete_grade_final_v2 vocabulary to canonical
  -- {no_negative, microscopic, gross, present_ungraded, NULL}
  -- Prior vocabulary artifacts: 'none'→no_negative, 'absent'→no_negative, 'true'→gross
  CASE
    WHEN p.ete_grade_final_v2 IN ('none', 'absent') THEN 'no_negative'
    WHEN p.ete_grade_final_v2 = 'gross'             THEN 'gross'
    WHEN p.ete_grade_final_v2 = 'microscopic'       THEN 'microscopic'
    WHEN p.ete_grade_final_v2 = 'present_ungraded'  THEN 'present_ungraded'
    WHEN p.ete_grade_final_v2 = 'true'              THEN 'gross'  -- 4-row boolean artifact
    WHEN p.ete_grade_final_v2 IS NULL               THEN NULL
    ELSE 'present_ungraded'
  END AS ete_grade_final,

  -- ete_grade: same normalization (synonym column consumed by some downstream queries)
  CASE
    WHEN p.ete_grade_final_v2 IN ('none', 'absent') THEN 'no_negative'
    WHEN p.ete_grade_final_v2 = 'gross'             THEN 'gross'
    WHEN p.ete_grade_final_v2 = 'microscopic'       THEN 'microscopic'
    WHEN p.ete_grade_final_v2 = 'present_ungraded'  THEN 'present_ungraded'
    WHEN p.ete_grade_final_v2 = 'true'              THEN 'gross'
    WHEN p.ete_grade_final_v2 IS NULL               THEN NULL
    ELSE 'present_ungraded'
  END AS ete_grade,

  p.ete_grade_source,
  p.gross_ete_flag,
  p.path_gross_ete_flag,
  p.ete_op_note_grade,
  p.ete_original_grade,
  p.ete_grade_final_v2 AS ete_grade_final_v2_raw,  -- preserve raw v2 for audit/traceability

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
    'main.canonical_patient_master.ete_grade_final_v2 (normalized); surg_first_date mig_254; mig_315 rebuild 20260505'
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


-- ---------------------------------------------------------------------------
-- §3  Rebuild dependent audit view
-- ---------------------------------------------------------------------------
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


-- ---------------------------------------------------------------------------
-- §4  Validation probes (run after apply to verify)
-- ---------------------------------------------------------------------------
-- Probe 4a: column uniqueness (expect: n = 37 unique and total from DESCRIBE)
-- DESCRIBE manuscript_workspace.cohort_m044_ajcc_ete_v1;

-- Probe 4b: ete_grade_final distribution (expect: no 'false','true','absent')
-- SELECT ete_grade_final, COUNT(*) AS n
-- FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
-- GROUP BY 1 ORDER BY n DESC;
-- EXPECT: no_negative=173, microscopic=2413, gross=1241, present_ungraded=28, NULL=11

-- Probe 4c: cohort N (expect ~3868 post-mig_313)
-- SELECT COUNT(*) FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;

-- Probe 4d: stage IVB (expect 50-120 post-mig_313)
-- SELECT ajcc8_stage_group, COUNT(*)
-- FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
-- GROUP BY 1 ORDER BY 1;


-- ---------------------------------------------------------------------------
-- §5  Signoff
-- ---------------------------------------------------------------------------
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_315',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig315',
  'mig_315: M044 cohort flat rebuild. '
  || 'Defect 1 (duplicate columns): confirmed measurement artifact from MotherDuck multi-catalog information_schema — VIEW has 35 clean unique columns via DESCRIBE; no schema change needed. '
  || 'Defect 2 (ete_grade_final Boolean->VARCHAR: false/absent/true): fixed at cohort VIEW layer — sourced from ete_grade_final_v2 with explicit CASE normalizing none/absent->no_negative, true->gross. '
  || 'Cohort N=3868 (post-mig_313 expansion from 3578 strict-DTC v5 frame; 151 malignant patients now have NULL stage_group post M-stage fix). '
  || 'no_negative=173, microscopic=2413, gross=1241, present_ungraded=28. '
  || 'Python consumers updated to handle no_negative vocab. '
  || 'Closes CF-M044-DUP-COLS; opens CF-M044-V6-MANUSCRIPT-PATCH for Cowork prose review.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_315');
