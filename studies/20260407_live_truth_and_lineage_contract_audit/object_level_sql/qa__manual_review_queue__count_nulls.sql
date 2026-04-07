-- Live counts: Thyroid 2026.qa.manual_review_queue
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id
FROM "Thyroid 2026"."qa"."manual_review_queue";
