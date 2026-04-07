-- Live counts: Thyroid 2026.main.longitudinal_lab_canonical_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_table" IS NULL THEN 1 ELSE 0 END) AS null_source_table,
  SUM(CASE WHEN "lab_date" IS NULL THEN 1 ELSE 0 END) AS null_lab_date,
  SUM(CASE WHEN "ingestion_wave" IS NULL THEN 1 ELSE 0 END) AS null_ingestion_wave
FROM "Thyroid 2026"."main"."longitudinal_lab_canonical_v1";
