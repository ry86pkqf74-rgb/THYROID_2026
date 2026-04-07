-- Live counts: Thyroid 2026.main.extracted_braf_recovery_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "extracted_at" IS NULL THEN 1 ELSE 0 END) AS null_extracted_at
FROM "Thyroid 2026"."main"."extracted_braf_recovery_v1";
