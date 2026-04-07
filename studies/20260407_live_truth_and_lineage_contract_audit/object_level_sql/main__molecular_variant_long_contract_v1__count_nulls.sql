-- Live counts: Thyroid 2026.main.molecular_variant_long_contract_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "lineage_id" IS NULL THEN 1 ELSE 0 END) AS null_lineage_id,
  SUM(CASE WHEN "ingestion_ts" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_ts,
  SUM(CASE WHEN "molecular_result_id" IS NULL THEN 1 ELSE 0 END) AS null_molecular_result_id
FROM "Thyroid 2026"."main"."molecular_variant_long_contract_v1";
