-- M088 cohort assembly
-- Plan-lock 2026-05-09 (analysis_plan_v1.md §3)
--
-- Cohort spine: canonical_diagnosis_unified_v1 (patient-level rollup).
-- Inclusion: diagnosis_primary in the 10 follicular-patterned entities, 1990-2025.
-- Exclusion: classical PTC, medullary, anaplastic w/o follicular, lymphoma,
--            metastatic to thyroid, consult-only specimens, recurrent/persistent disease.
--
-- This file is the canonical CTE used by all H1-H4 queries; the H1-H4 SQLs reproduce
-- the cohort CTE inline so each query is self-contained for audit and reproducibility.

WITH cohort_base AS (
  SELECT DISTINCT
    d.research_id,
    d.diagnosis_primary,
    d.diagnosis_variant,
    d.diagnosis_full,
    d.is_malignant,
    d.n_tumors,
    d.source_table
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1` d
  WHERE d.diagnosis_primary IN (
    'follicular_adenoma',
    'hurthle_cell_adenoma',
    'FTUMP',
    'atypical_follicular_adenoma',
    'NIFTP',
    'FTC',
    'HCC',
    'DHGTC',
    'PDTC',
    'hyalinizing_trabecular_tumor'
  )
),
-- Exclude recurrent/persistent disease per plan §3
recurrent AS (
  SELECT DISTINCT research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1` d
  -- canonical_histology_lookup_v1 is a vocabulary; recurrence flag is on the diagnosis_full mapping
  WHERE d.diagnosis_full IN (
    SELECT DISTINCT histology_normalized
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_histology_lookup_v1`
    WHERE COALESCE(is_recurrent, FALSE) = TRUE
       OR COALESCE(is_metastatic, FALSE) = TRUE
  )
),
op AS (
  SELECT
    research_id,
    earliest_surgery_date,
    latest_surgery_date,
    n_total_thyroidectomies,
    n_hemithyroidectomies,
    n_completion_thyroidectomies,
    n_central_neck_dissections,
    n_lateral_neck_dissections
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1_1`
),
cohort AS (
  SELECT
    cb.*,
    -- Era flag (5-year bins keyed on earliest_surgery_date)
    CASE
      WHEN op.earliest_surgery_date IS NULL THEN 'unknown'
      WHEN op.earliest_surgery_date <  '1995-01-01' THEN '1990-1994'
      WHEN op.earliest_surgery_date <  '2000-01-01' THEN '1995-1999'
      WHEN op.earliest_surgery_date <  '2005-01-01' THEN '2000-2004'
      WHEN op.earliest_surgery_date <  '2010-01-01' THEN '2005-2009'
      WHEN op.earliest_surgery_date <  '2015-01-01' THEN '2010-2014'
      WHEN op.earliest_surgery_date <  '2020-01-01' THEN '2015-2019'
      ELSE '2020-2025'
    END AS era_5yr,
    EXTRACT(YEAR FROM op.earliest_surgery_date) AS surgery_year,
    op.n_total_thyroidectomies,
    op.n_hemithyroidectomies,
    op.n_completion_thyroidectomies,
    -- Derived flags for downstream H1/H2/H3
    CASE
      WHEN cb.diagnosis_primary = 'hurthle_cell_adenoma' THEN TRUE
      WHEN cb.diagnosis_primary = 'HCC'                  THEN TRUE
      WHEN cb.diagnosis_primary = 'FTC' AND cb.diagnosis_variant = 'oncocytic_warthin' THEN TRUE
      ELSE FALSE
    END AS is_oncocytic_tier_a,
    -- Surgery binary at index
    CASE
      WHEN op.n_total_thyroidectomies > 0 THEN 'total'
      WHEN op.n_hemithyroidectomies   > 0 THEN 'hemi'
      ELSE 'unknown'
    END AS extent_of_resection,
    CASE
      WHEN op.n_completion_thyroidectomies > 0 THEN TRUE
      ELSE FALSE
    END AS had_completion_thyroidectomy
  FROM cohort_base cb
  LEFT JOIN op ON cb.research_id = op.research_id
  WHERE cb.research_id NOT IN (SELECT research_id FROM recurrent)
)
SELECT * FROM cohort;
