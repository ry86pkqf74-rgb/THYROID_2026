-- Live counts: Thyroid 2026.main.manuscript_cohort_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "surgery_date" IS NULL THEN 1 ELSE 0 END) AS null_surgery_date
FROM "Thyroid 2026"."main"."manuscript_cohort_v1";
