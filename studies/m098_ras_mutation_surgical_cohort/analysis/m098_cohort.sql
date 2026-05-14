-- M098 cohort SQL (locked snapshot used in analysis)
-- Cohort N = 292
SELECT research_id
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
WHERE ras_positive_final IS TRUE
  AND surg_first_date IS NOT NULL
ORDER BY research_id;
