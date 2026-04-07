-- Live counts: Thyroid 2026.release_20260409.master_fact_long_verified_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_domain" IS NULL THEN 1 ELSE 0 END) AS null_source_domain,
  SUM(CASE WHEN "entity_date" IS NULL THEN 1 ELSE 0 END) AS null_entity_date,
  SUM(CASE WHEN "extraction_run_id" IS NULL THEN 1 ELSE 0 END) AS null_extraction_run_id
FROM "Thyroid 2026"."release_20260409"."master_fact_long_verified_v1";
