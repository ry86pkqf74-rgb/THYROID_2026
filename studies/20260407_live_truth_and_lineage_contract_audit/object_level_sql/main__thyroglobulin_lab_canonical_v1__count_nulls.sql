-- Live counts: Thyroid 2026.main.thyroglobulin_lab_canonical_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "ingestion_script" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_script,
  SUM(CASE WHEN "surg_date" IS NULL THEN 1 ELSE 0 END) AS null_surg_date,
  SUM(CASE WHEN "ingestion_date" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_date
FROM "Thyroid 2026"."main"."thyroglobulin_lab_canonical_v1";
