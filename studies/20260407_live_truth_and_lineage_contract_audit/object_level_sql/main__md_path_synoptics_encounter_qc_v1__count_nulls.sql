-- Live counts: Thyroid 2026.main.md_path_synoptics_encounter_qc_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_workbook" IS NULL THEN 1 ELSE 0 END) AS null_source_workbook,
  SUM(CASE WHEN "surg_date" IS NULL THEN 1 ELSE 0 END) AS null_surg_date,
  SUM(CASE WHEN "ingest_script_version" IS NULL THEN 1 ELSE 0 END) AS null_ingest_script_version
FROM "Thyroid 2026"."main"."md_path_synoptics_encounter_qc_v1";
