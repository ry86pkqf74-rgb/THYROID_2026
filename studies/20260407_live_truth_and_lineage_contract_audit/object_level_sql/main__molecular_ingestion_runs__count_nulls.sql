-- Live counts: Thyroid 2026.main.molecular_ingestion_runs
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "started_at" IS NULL THEN 1 ELSE 0 END) AS null_started_at,
  SUM(CASE WHEN "ingestion_run_id" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_run_id
FROM "Thyroid 2026"."main"."molecular_ingestion_runs";
