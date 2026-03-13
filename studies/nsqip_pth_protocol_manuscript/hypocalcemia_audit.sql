-- =============================================================================
-- Hypocalcemia Rate Audit SQL — NSQIP/PTH Protocol Manuscript
-- Date: 2026-03-12
-- Source: studies/nsqip_linkage/nsqip_thyroid_linkage_final.csv loaded into DuckDB
-- =============================================================================

-- NOTE: These queries operate on the raw CSV loaded into a DuckDB temporary table.
-- Run via: .venv/bin/python hypocalcemia_audit_runner.py
-- Or paste into a DuckDB session after loading the CSV.

-- =============================================================================
-- QUERY 1: Load and filter raw linkage to total/completion CPTs
-- =============================================================================
CREATE OR REPLACE TEMP TABLE nsqip_tc AS
SELECT *,
  CASE WHEN "CPT Code" = 60260 THEN 'Completion' ELSE 'Total' END AS procedure_category,
  ROW_NUMBER() OVER (PARTITION BY "linked_research_id" ORDER BY "Operation Date") AS surgery_rank
FROM read_csv_auto('studies/nsqip_linkage/nsqip_thyroid_linkage_final.csv')
WHERE "match_status" = 'Perfect deterministic match'
  AND "CPT Code" IN (60240, 60252, 60254, 60260, 60270, 60271);

-- =============================================================================
-- QUERY 2: Reproduce manuscript claim — surgery-level (57/763)
-- =============================================================================
SELECT
  'surgery_level' AS analysis_level,
  COUNT(*) AS parent_cohort,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS module_available,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END) AS hypo_yes,
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END)
    / NULLIF(COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END), 0), 1) AS rate_pct
FROM nsqip_tc;

-- =============================================================================
-- QUERY 3: Correct patient-level analysis (57/755)
-- =============================================================================
SELECT
  'patient_level' AS analysis_level,
  COUNT(*) AS parent_cohort,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS module_available,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END) AS hypo_yes,
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END)
    / NULLIF(COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END), 0), 1) AS rate_pct
FROM nsqip_tc
WHERE surgery_rank = 1;

-- =============================================================================
-- QUERY 4: Multi-surgery patient audit
-- =============================================================================
SELECT
  "linked_research_id",
  COUNT(*) AS n_surgeries,
  LIST("CPT Code") AS cpt_codes,
  LIST("Operation Date") AS op_dates,
  LIST("Thyroidectomy Postoperative Hypocalcemia") AS hypo_values
FROM nsqip_tc
GROUP BY "linked_research_id"
HAVING COUNT(*) > 1
ORDER BY "linked_research_id";

-- =============================================================================
-- QUERY 5: Sensitivity analyses (patient-level)
-- =============================================================================
WITH pt AS (
  SELECT * FROM nsqip_tc WHERE surgery_rank = 1
),
denom AS (
  SELECT COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS n_module
  FROM pt
)
SELECT
  'A_any_nsqip_hypo' AS definition,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END) AS numerator,
  (SELECT n_module FROM denom) AS denominator,
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END)
    / (SELECT n_module FROM denom), 1) AS rate_pct
FROM pt
UNION ALL
SELECT
  'B_hypo_or_iv_ca',
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes'
    OR LOWER(CAST("Thyroidectomy IV Calcium" AS VARCHAR)) LIKE 'yes%' THEN 1 END),
  (SELECT n_module FROM denom),
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes'
    OR LOWER(CAST("Thyroidectomy IV Calcium" AS VARCHAR)) LIKE 'yes%' THEN 1 END)
    / (SELECT n_module FROM denom), 1)
FROM pt
UNION ALL
SELECT
  'C_hypo_event_any',
  COUNT(CASE WHEN "Thyroidectomy Postop Hypocalcemia-related Event" = 'Yes' THEN 1 END),
  COUNT(CASE WHEN "Thyroidectomy Postop Hypocalcemia-related Event" IN ('Yes','No') THEN 1 END),
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postop Hypocalcemia-related Event" = 'Yes' THEN 1 END)
    / NULLIF(COUNT(CASE WHEN "Thyroidectomy Postop Hypocalcemia-related Event" IN ('Yes','No') THEN 1 END), 0), 1)
FROM pt
UNION ALL
SELECT
  'D_hypo_and_event_or_iv',
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes'
    AND ("Thyroidectomy Postop Hypocalcemia-related Event" = 'Yes'
      OR LOWER(CAST("Thyroidectomy IV Calcium" AS VARCHAR)) LIKE 'yes%') THEN 1 END),
  (SELECT n_module FROM denom),
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes'
    AND ("Thyroidectomy Postop Hypocalcemia-related Event" = 'Yes'
      OR LOWER(CAST("Thyroidectomy IV Calcium" AS VARCHAR)) LIKE 'yes%') THEN 1 END)
    / (SELECT n_module FROM denom), 1)
FROM pt
UNION ALL
SELECT
  'E_iv_calcium_event',
  COUNT(CASE WHEN LOWER(CAST("Thyroidectomy Postop Hypocalcemia-related Event Type" AS VARCHAR))
    LIKE '%iv calcium%' THEN 1 END),
  (SELECT n_module FROM denom),
  ROUND(100.0 * COUNT(CASE WHEN LOWER(CAST("Thyroidectomy Postop Hypocalcemia-related Event Type" AS VARCHAR))
    LIKE '%iv calcium%' THEN 1 END)
    / (SELECT n_module FROM denom), 1)
FROM pt;

-- =============================================================================
-- QUERY 6: Missingness by year (patient-level)
-- =============================================================================
SELECT
  EXTRACT(YEAR FROM TRY_CAST("Operation Date" AS DATE)) AS surgery_year,
  COUNT(*) AS n_patients,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS module_available,
  COUNT(*) - COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS missing,
  ROUND(100.0 * (COUNT(*) - COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END))
    / COUNT(*), 1) AS missing_pct,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END) AS hypo_yes,
  CASE WHEN COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) > 0
    THEN ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END)
      / COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END), 1)
    ELSE NULL END AS hypo_rate_pct
FROM nsqip_tc
WHERE surgery_rank = 1
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- QUERY 7: Missingness by surgery type (patient-level)
-- =============================================================================
SELECT
  procedure_category,
  COUNT(*) AS n_patients,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS module_available,
  COUNT(*) - COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END) AS missing,
  COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END) AS hypo_yes,
  ROUND(100.0 * COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" = 'Yes' THEN 1 END)
    / NULLIF(COUNT(CASE WHEN "Thyroidectomy Postoperative Hypocalcemia" IN ('Yes','No') THEN 1 END), 0), 1) AS hypo_rate_pct
FROM nsqip_tc
WHERE surgery_rank = 1
GROUP BY 1;

-- =============================================================================
-- QUERY 8: Lobectomy exclusion verification
-- =============================================================================
SELECT
  'lobectomy_check' AS check_name,
  COUNT(CASE WHEN "CPT Code" IN (60220, 60225) THEN 1 END) AS lobectomy_in_tc_set,
  COUNT(DISTINCT "linked_research_id") AS unique_patients
FROM nsqip_tc;

-- =============================================================================
-- QUERY 9: MotherDuck institutional cross-check
-- =============================================================================
-- Run against MotherDuck:
-- SELECT COUNT(DISTINCT research_id) FROM complications WHERE hypocalcemia IS NOT NULL;
-- Expected: 0 (all NULL)

-- SELECT entity_tier, COUNT(*) FROM extracted_hypocalcemia_refined_v2 GROUP BY 1;
-- Expected: Tier 1: 18, Tier 2: 47, Tier 3: 17

-- SELECT COUNT(*) FROM vw_postop_lab_nadir WHERE lab_type ILIKE '%calcium%' AND nadir_value < 7.5;
-- Expected: 5 patients
