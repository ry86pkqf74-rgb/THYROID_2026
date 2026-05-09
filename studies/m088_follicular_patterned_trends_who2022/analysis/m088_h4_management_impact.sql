-- M088 H4: Index-encounter management impact under 2022 WHO reclassification
-- Plan-lock 2026-05-09 (analysis_plan_v1.md §4.4)
--
-- Endpoints (RAI dropped per NF-2026-05-09-rai-extraction-sparse-follicular-cohort):
--   1. extent_of_resection (binary: hemi vs total)
--   2. completion_thyroidectomy_at_index (binary)
--
-- Reclassified labels are produced by joining H1 + H2 + H3 outputs.
-- The Python analysis (m088_tables.py) is the authoritative joiner; this SQL
-- is the per-patient management spine.

WITH cohort AS (
  SELECT DISTINCT
    d.research_id, d.diagnosis_primary, d.diagnosis_variant
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1` d
  WHERE d.diagnosis_primary IN (
    'follicular_adenoma','hurthle_cell_adenoma','FTUMP','atypical_follicular_adenoma',
    'NIFTP','FTC','HCC','DHGTC','PDTC','hyalinizing_trabecular_tumor'
  )
),
op AS (
  SELECT research_id,
         earliest_surgery_date,
         n_total_thyroidectomies,
         n_hemithyroidectomies,
         n_completion_thyroidectomies,
         n_central_neck_dissections,
         n_lateral_neck_dissections,
         CASE
           WHEN earliest_surgery_date IS NULL THEN 'unknown'
           WHEN earliest_surgery_date <  '1995-01-01' THEN '1990-1994'
           WHEN earliest_surgery_date <  '2000-01-01' THEN '1995-1999'
           WHEN earliest_surgery_date <  '2005-01-01' THEN '2000-2004'
           WHEN earliest_surgery_date <  '2010-01-01' THEN '2005-2009'
           WHEN earliest_surgery_date <  '2015-01-01' THEN '2010-2014'
           WHEN earliest_surgery_date <  '2020-01-01' THEN '2015-2019'
           ELSE '2020-2025'
         END AS era_5yr
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1_1`
)
SELECT
  c.research_id,
  c.diagnosis_primary,
  c.diagnosis_variant,
  op.era_5yr,
  EXTRACT(YEAR FROM op.earliest_surgery_date) AS surgery_year,
  CASE
    WHEN op.n_total_thyroidectomies > 0 THEN 'total'
    WHEN op.n_hemithyroidectomies   > 0 THEN 'hemi'
    ELSE 'unknown'
  END AS extent_of_resection,
  CASE WHEN op.n_completion_thyroidectomies > 0 THEN 1 ELSE 0 END AS had_completion,
  -- Combined "definitive total thyroidectomy at any time" (initial + completion completed = total at horizon)
  CASE
    WHEN op.n_total_thyroidectomies > 0 OR op.n_completion_thyroidectomies > 0 THEN 1
    ELSE 0
  END AS definitive_total_thyroidectomy
FROM cohort c
LEFT JOIN op ON c.research_id = op.research_id;
