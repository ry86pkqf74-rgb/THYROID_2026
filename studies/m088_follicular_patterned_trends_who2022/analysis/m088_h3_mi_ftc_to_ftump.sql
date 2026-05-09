-- M088 H3: Minimally invasive FTC -> FT-UMP migration under 2022 WHO
-- Plan-lock 2026-05-09 (analysis_plan_v1.md §4.3)
--
-- Reclassification candidate: borderline capsular invasion AND no documented vascular invasion.
-- Three locked sensitivity bounds:
--   STRICT  : finding_status = 'indeterminate' AND no vascular
--   MODERATE: STRICT  ∪ qualifier 'minimal'/'minimally invasive'
--   BROAD   : MODERATE ∪ qualifier 'focal'/'single focus'/'partial'

WITH cohort AS (
  SELECT DISTINCT research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1`
  WHERE diagnosis_primary = 'FTC' AND diagnosis_variant = 'minimally_invasive'
),
op AS (
  SELECT research_id,
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
),
capsular AS (
  SELECT
    research_id,
    MAX(CASE WHEN finding_status = 'indeterminate' THEN 1 ELSE 0 END) AS strict_capsular_eq,
    MAX(CASE
          WHEN finding_status = 'indeterminate' THEN 1
          WHEN LOWER(evidence_qualifier) IN ('minimal','minimally invasive','yes (minimal)') THEN 1
          ELSE 0
        END) AS moderate_capsular_eq,
    MAX(CASE
          WHEN finding_status = 'indeterminate' THEN 1
          WHEN LOWER(evidence_qualifier) IN (
            'minimal','minimally invasive','yes (minimal)',
            'focal','single focus','partial','yes (focal)'
          ) THEN 1
          ELSE 0
        END) AS broad_capsular_eq
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_invasion_events_v1`
  WHERE invasion_type = 'capsular'
  GROUP BY research_id
),
vascular AS (
  -- Any vascular invasion documented as present
  SELECT DISTINCT research_id, 1 AS any_vascular
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_vascular_invasion_events_v1`
  WHERE LOWER(COALESCE(vascular_invasion, '')) IN ('present','positive','yes')
     OR vascular_invasion_extent IS NOT NULL
),
quantify AS (
  -- canonical_path_indeterminate_events_v1.angioinvasion_quantify >= 1 = unequivocal vascular
  SELECT DISTINCT research_id, 1 AS quantified_vascular
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_indeterminate_events_v1`
  WHERE angioinvasion_quantify >= 1
)
SELECT
  c.research_id,
  op.era_5yr,
  COALESCE(cap.strict_capsular_eq,   0) AS strict_capsular_eq,
  COALESCE(cap.moderate_capsular_eq, 0) AS moderate_capsular_eq,
  COALESCE(cap.broad_capsular_eq,    0) AS broad_capsular_eq,
  COALESCE(v.any_vascular,           0) AS any_vascular_documented,
  COALESCE(q.quantified_vascular,    0) AS quantified_vascular,
  -- Reclassification under each bound (requires capsular eq AND no vascular)
  CASE WHEN COALESCE(cap.strict_capsular_eq,0)   = 1
        AND COALESCE(v.any_vascular,0)           = 0
        AND COALESCE(q.quantified_vascular,0)    = 0
       THEN 'FT-UMP (strict)'
       ELSE 'FTC, minimally invasive'
  END AS who2022_label_strict,
  CASE WHEN COALESCE(cap.moderate_capsular_eq,0) = 1
        AND COALESCE(v.any_vascular,0)           = 0
        AND COALESCE(q.quantified_vascular,0)    = 0
       THEN 'FT-UMP (moderate)'
       ELSE 'FTC, minimally invasive'
  END AS who2022_label_moderate,
  CASE WHEN COALESCE(cap.broad_capsular_eq,0)    = 1
        AND COALESCE(v.any_vascular,0)           = 0
        AND COALESCE(q.quantified_vascular,0)    = 0
       THEN 'FT-UMP (broad)'
       ELSE 'FTC, minimally invasive'
  END AS who2022_label_broad
FROM cohort c
LEFT JOIN op       ON c.research_id = op.research_id
LEFT JOIN capsular cap ON c.research_id = cap.research_id
LEFT JOIN vascular v   ON c.research_id = v.research_id
LEFT JOIN quantify q   ON c.research_id = q.research_id;
