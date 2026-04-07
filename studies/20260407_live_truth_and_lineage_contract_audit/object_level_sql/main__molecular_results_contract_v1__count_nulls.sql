-- Live counts: Thyroid 2026.main.molecular_results_contract_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_table" IS NULL THEN 1 ELSE 0 END) AS null_source_table,
  SUM(CASE WHEN "ingestion_ts" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_ts,
  SUM(CASE WHEN "molecular_result_id" IS NULL THEN 1 ELSE 0 END) AS null_molecular_result_id,
  SUM(CASE WHEN "ingestion_run_id" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_run_id
FROM "Thyroid 2026"."main"."molecular_results_contract_v1";
