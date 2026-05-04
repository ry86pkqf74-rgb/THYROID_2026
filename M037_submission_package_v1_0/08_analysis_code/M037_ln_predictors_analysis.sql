-- =============================================================================
-- M037 — LN predictors analysis excerpts (MotherDuck)
-- Database: thyroid_canonical_publication_v1_0
-- Cohort: manuscript_workspace.cohort_m037_ln_metastasis_v1
-- =============================================================================
-- Run in a session with: USE thyroid_canonical_publication_v1_0;
-- =============================================================================

-- -----------------------------------------------------------------------------
-- A. Base cohort count (must match manuscript_workspace view definition)
-- -----------------------------------------------------------------------------
-- SELECT COUNT(*) AS n_m037 FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;

-- -----------------------------------------------------------------------------
-- B. Enriched spine (cohort + CPM fields not surfaced on the view)
--    Used for ln_status_source sensitivity (mig_259 Rule C) and AJCC T-stage.
-- -----------------------------------------------------------------------------
/*
CREATE OR REPLACE TEMP VIEW m037_analytic_spine AS
SELECT
  c.*,
  p.ln_status_source,
  p.ajcc8_t_stage,
  p.race,
  p.ajcc8_m_stage
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1 c
INNER JOIN main.canonical_patient_master p
  ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR);
*/

-- -----------------------------------------------------------------------------
-- C. N-stage bucket for Table 1 (LN stratum descriptors)
-- -----------------------------------------------------------------------------
/*
SELECT
  CASE
    WHEN UPPER(COALESCE(ajcc8_n_stage, '')) IN ('N0') THEN 'N0'
    WHEN UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'N1A%' THEN 'N1a'
    WHEN UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'N1B%' THEN 'N1b'
    WHEN UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'NX%' OR ajcc8_n_stage IS NULL THEN 'Nx'
    WHEN UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'N1%' THEN 'N1_other'
    ELSE 'Other'
  END AS n_bucket,
  COUNT(*) AS n
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1
GROUP BY 1
ORDER BY 1;
*/

-- -----------------------------------------------------------------------------
-- D. Outcome for logistic regression (N1 vs non-N1) — aligns with Cowork Table
-- -----------------------------------------------------------------------------
/*
SELECT
  COUNT(*) AS n,
  COUNT(*) FILTER (
    WHERE UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'N1%'
  ) AS n_ln_pos,
  ROUND(100.0 * COUNT(*) FILTER (WHERE UPPER(COALESCE(ajcc8_n_stage, '')) LIKE 'N1%') / COUNT(*), 1) AS pct_ln_pos
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;
*/

-- -----------------------------------------------------------------------------
-- E. Sensitivity cohort — exclude staging-only LN discordance (mig_258/259)
--    Matches snowflake_trial/scripts/25_m037_sensitivity_ln_both.py predicate.
-- -----------------------------------------------------------------------------
/*
SELECT COUNT(*) AS n_sens
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1 c
INNER JOIN main.canonical_patient_master p
  ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR)
WHERE p.ln_status_source IS NULL OR p.ln_status_source != 'staging';
*/

-- -----------------------------------------------------------------------------
-- F. Signoff (applied via qc_framework migration 291, not ad hoc)
-- -----------------------------------------------------------------------------
/*
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_291', CURRENT_TIMESTAMP, 'cursor_composer_mig291',
 'mig_291: M037 LN Predictors submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp + figures. SQL reproducibility + Python builders. Headline: family-hx aOR ~1.05 (null); male sex, age, tumor size significant.');
*/
