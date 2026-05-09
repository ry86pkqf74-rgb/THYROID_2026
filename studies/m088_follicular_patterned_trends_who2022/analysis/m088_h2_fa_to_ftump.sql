-- M088 H2: Follicular adenoma -> FT-UMP migration under 2022 WHO
-- Plan-lock 2026-05-09 (analysis_plan_v1.md §4.2)
--
-- A historical FA reclassifies to FT-UMP if capsular invasion is judged equivocal.
-- Two locked sensitivity bounds:
--   STRICT: finding_status IN ('indeterminate','suspected') OR
--           evidence_qualifier IN ('equivocal','uncertain','possible','questionable',
--                                  'indeterminate','cannot be assessed','infiltrative?',
--                                  'focal suggestion of penetration','focal suspicious','focally')
--   BROAD : STRICT  UNION  evidence_qualifier IN ('focal','partial','single focus','minimal')

WITH cohort AS (
  SELECT DISTINCT d.research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1` d
  WHERE d.diagnosis_primary = 'follicular_adenoma'
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
    -- Strict equivocality
    MAX(CASE
          WHEN finding_status IN ('indeterminate','suspected') THEN 1
          WHEN LOWER(evidence_qualifier) IN (
            'equivocal','uncertain','possible','questionable','indeterminate',
            'cannot be assessed','infiltrative?',
            'focal suggestion of penetration','focal suspicious','focally'
          ) THEN 1
          ELSE 0
        END) AS strict_equivocal,
    -- Broad equivocality (strict ∪ minimal/focal/partial/single focus)
    MAX(CASE
          WHEN finding_status IN ('indeterminate','suspected') THEN 1
          WHEN LOWER(evidence_qualifier) IN (
            'equivocal','uncertain','possible','questionable','indeterminate',
            'cannot be assessed','infiltrative?',
            'focal suggestion of penetration','focal suspicious','focally',
            'focal','partial','single focus','minimal','minimally invasive',
            'yes (minimal)','yes (focal)'
          ) THEN 1
          ELSE 0
        END) AS broad_equivocal,
    -- Unequivocal present (in case we want a sanity check that some FAs have actual capsular invasion called present, which would be misclassification)
    MAX(CASE WHEN finding_status = 'present' AND
                  LOWER(COALESCE(evidence_qualifier,'')) NOT IN ('equivocal','uncertain','possible','questionable','indeterminate',
                                                                  'cannot be assessed','infiltrative?',
                                                                  'focal suggestion of penetration','focal suspicious','focally',
                                                                  'focal','partial','single focus','minimal','minimally invasive')
              THEN 1 ELSE 0 END) AS unequiv_present
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_invasion_events_v1`
  WHERE invasion_type = 'capsular'
  GROUP BY research_id
)
SELECT
  c.research_id,
  op.era_5yr,
  COALESCE(cap.strict_equivocal, 0) AS strict_equivocal,
  COALESCE(cap.broad_equivocal,  0) AS broad_equivocal,
  COALESCE(cap.unequiv_present,  0) AS unequiv_present,
  CASE WHEN COALESCE(cap.strict_equivocal,0) = 1 THEN 'FT-UMP (strict)'
       ELSE 'Follicular adenoma' END AS who2022_label_strict,
  CASE WHEN COALESCE(cap.broad_equivocal,0)  = 1 THEN 'FT-UMP (broad)'
       ELSE 'Follicular adenoma' END AS who2022_label_broad
FROM cohort c
LEFT JOIN op  ON c.research_id = op.research_id
LEFT JOIN capsular cap ON c.research_id = cap.research_id;
