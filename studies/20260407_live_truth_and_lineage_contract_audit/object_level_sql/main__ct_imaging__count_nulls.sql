-- Live counts: Thyroid 2026.main.ct_imaging
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_workbook" IS NULL THEN 1 ELSE 0 END) AS null_source_workbook,
  SUM(CASE WHEN "ingested_at_utc" IS NULL THEN 1 ELSE 0 END) AS null_ingested_at_utc,
  SUM(CASE WHEN "ingest_script_version" IS NULL THEN 1 ELSE 0 END) AS null_ingest_script_version
FROM "Thyroid 2026"."main"."ct_imaging";
