-- 04b_table3_v4_actual_reported_call.sql — EXT2-4 Table 3 v4 (actual reported-call pipeline)
--
-- PRIMARY COHORT DEFINITION (v4, Logan 2026-05-14 decision):
--   Patients in surgical denominator intersected with at least ONE preoperative
--   ultrasound lesion where canonical_us_nodule_v2.size_cm_max BETWEEN 2.0 AND 4.0
--   on an examination with exam_date <= DATE(first surgery).
--
-- Full cell-level SQL mirrors the derivation in Python:
--     ext2_4_v4_derive_tables.py
-- which emits tables/_v4_table3_cells.json then build_table3_v4_actual_call.py
-- aggregates into CSV with Wilson intervals.
--
-- Quick size check (standalone):
WITH surgical AS (
  SELECT CAST(research_id AS STRING) AS rid_s,
         fna_bethesda_final AS bethesda,
         imaging_nodule_size_cm AS imaging_nodule_size_cm_index,
         DATE(surg_first_date) AS d_surg
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy', 'hemithyroidectomy')
),
cohort_v4_pts AS (
  SELECT DISTINCT s.rid_s
  FROM surgical s
  INNER JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` n
    ON CAST(n.research_id AS STRING) = s.rid_s
   AND n.exam_date <= s.d_surg
  WHERE n.size_cm_max BETWEEN 2.0 AND 4.0
)
SELECT COUNT(*) AS n_v4_total FROM cohort_v4_pts;

-- Verified 2026-05-13: n_v4_total = 765
-- Verified 2026-05-13: v4_strict (strict nodal exclusion CTE chain as in Cursor handoff)
--   COUNT DISTINCT rid_s = 654
