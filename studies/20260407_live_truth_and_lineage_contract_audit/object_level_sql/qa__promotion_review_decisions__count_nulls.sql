-- Live counts: Thyroid 2026.qa.promotion_review_decisions
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_object_id" IS NULL THEN 1 ELSE 0 END) AS null_source_object_id,
  SUM(CASE WHEN "created_at" IS NULL THEN 1 ELSE 0 END) AS null_created_at
FROM "Thyroid 2026"."qa"."promotion_review_decisions";
