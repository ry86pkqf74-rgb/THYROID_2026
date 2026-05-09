-- M088 H1: Oncocytic family migration under 2022 WHO
-- Plan-lock 2026-05-09 (analysis_plan_v1.md §4.1, Tier A deterministic)
--
-- Reclassification rules (Tier A):
--   hurthle_cell_adenoma  -> Oncocytic Adenoma (OA)
--   HCC                   -> Oncocytic Carcinoma (OC)
--   FTC + oncocytic_warthin variant -> Oncocytic Carcinoma (OC)
--   FTUMP with 'oncocytic'/'hurthle' in tumor_N_histology_comment -> OTUMP
--   atypical_follicular_adenoma + path_synoptics.hurthle_cell_oncocytic_adenoma -> OA or OTUMP
--
-- Output: per-patient historical_label and 2022_who_label, era stratification,
--         and the family-migration counts.

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
  SELECT research_id, earliest_surgery_date,
         EXTRACT(YEAR FROM earliest_surgery_date) AS surgery_year
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1_1`
),
-- Pull oncocytic-morphology evidence from path_synoptics for FTUMP / atypical_FA edge cases
synoptic_onco AS (
  SELECT DISTINCT research_id, TRUE AS has_oncocytic_synoptic
  FROM (
    SELECT research_id, hurthle_cell_oncocytic_adenoma,
           tumor_1_histology_comment, tumor_2_histology_comment, tumor_3_histology_comment
    FROM `thyroid-canonical-pub-2026.pub_canonical.path_synoptics`
  )
  WHERE LOWER(COALESCE(hurthle_cell_oncocytic_adenoma, '')) IN ('yes','y','present','positive','true')
     OR REGEXP_CONTAINS(LOWER(COALESCE(tumor_1_histology_comment, '')), r'oncocytic|hurthle|h\xfcrthle|hurtle')
     OR REGEXP_CONTAINS(LOWER(COALESCE(tumor_2_histology_comment, '')), r'oncocytic|hurthle|h\xfcrthle|hurtle')
     OR REGEXP_CONTAINS(LOWER(COALESCE(tumor_3_histology_comment, '')), r'oncocytic|hurthle|h\xfcrthle|hurtle')
),
labeled AS (
  SELECT
    c.research_id,
    c.diagnosis_primary,
    c.diagnosis_variant,
    op.surgery_year,
    -- Era 5-yr
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
    COALESCE(s.has_oncocytic_synoptic, FALSE) AS has_oncocytic_synoptic,
    -- Historical label (legacy 2017 WHO 4th)
    CASE
      WHEN c.diagnosis_primary = 'hurthle_cell_adenoma' THEN 'Hurthle cell adenoma'
      WHEN c.diagnosis_primary = 'HCC'                  THEN 'Hurthle cell carcinoma'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'oncocytic_warthin'  THEN 'FTC, oncocytic variant'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'minimally_invasive' THEN 'FTC, minimally invasive'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'widely_invasive'    THEN 'FTC, widely invasive'
      WHEN c.diagnosis_primary = 'FTC'                                                THEN 'FTC, not otherwise specified'
      WHEN c.diagnosis_primary = 'FTUMP'                                              THEN 'FT-UMP'
      WHEN c.diagnosis_primary = 'atypical_follicular_adenoma'                        THEN 'Atypical follicular adenoma'
      WHEN c.diagnosis_primary = 'follicular_adenoma'                                 THEN 'Follicular adenoma'
      WHEN c.diagnosis_primary = 'NIFTP'                                              THEN 'NIFTP'
      WHEN c.diagnosis_primary = 'DHGTC'                                              THEN 'DHGTC'
      WHEN c.diagnosis_primary = 'PDTC'                                               THEN 'PDTC'
      WHEN c.diagnosis_primary = 'hyalinizing_trabecular_tumor'                       THEN 'Hyalinizing trabecular tumor'
    END AS historical_label,
    -- 2022 WHO label (Tier A deterministic)
    CASE
      -- Oncocytic family
      WHEN c.diagnosis_primary = 'hurthle_cell_adenoma'                                            THEN 'Oncocytic adenoma'
      WHEN c.diagnosis_primary = 'HCC'                                                             THEN 'Oncocytic carcinoma'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'oncocytic_warthin'               THEN 'Oncocytic carcinoma'
      WHEN c.diagnosis_primary = 'FTUMP' AND COALESCE(s.has_oncocytic_synoptic, FALSE)             THEN 'Oncocytic UMP'
      WHEN c.diagnosis_primary = 'atypical_follicular_adenoma' AND COALESCE(s.has_oncocytic_synoptic, FALSE) THEN 'Oncocytic UMP'
      -- Non-oncocytic follicular family (unchanged)
      WHEN c.diagnosis_primary = 'follicular_adenoma'                                              THEN 'Follicular adenoma'
      WHEN c.diagnosis_primary = 'FTUMP'                                                           THEN 'FT-UMP'
      WHEN c.diagnosis_primary = 'atypical_follicular_adenoma'                                     THEN 'FT-UMP (from atypical FA)'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'minimally_invasive'              THEN 'FTC, minimally invasive'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'widely_invasive'                 THEN 'FTC, widely invasive'
      WHEN c.diagnosis_primary = 'FTC'                                                             THEN 'FTC, NOS'
      WHEN c.diagnosis_primary = 'NIFTP'                                                           THEN 'NIFTP'
      WHEN c.diagnosis_primary = 'DHGTC'                                                           THEN 'DHGTC'
      WHEN c.diagnosis_primary = 'PDTC'                                                            THEN 'PDTC'
      WHEN c.diagnosis_primary = 'hyalinizing_trabecular_tumor'                                    THEN 'Hyalinizing trabecular tumor'
    END AS who2022_label,
    -- Family
    CASE
      WHEN c.diagnosis_primary = 'hurthle_cell_adenoma'                                            THEN 'Oncocytic'
      WHEN c.diagnosis_primary = 'HCC'                                                             THEN 'Oncocytic'
      WHEN c.diagnosis_primary = 'FTC' AND c.diagnosis_variant = 'oncocytic_warthin'               THEN 'Oncocytic'
      WHEN c.diagnosis_primary = 'FTUMP' AND COALESCE(s.has_oncocytic_synoptic, FALSE)             THEN 'Oncocytic'
      WHEN c.diagnosis_primary = 'atypical_follicular_adenoma' AND COALESCE(s.has_oncocytic_synoptic, FALSE) THEN 'Oncocytic'
      ELSE 'Conventional follicular'
    END AS who2022_family
  FROM cohort c
  LEFT JOIN op ON c.research_id = op.research_id
  LEFT JOIN synoptic_onco s ON c.research_id = s.research_id
)
SELECT * FROM labeled;
