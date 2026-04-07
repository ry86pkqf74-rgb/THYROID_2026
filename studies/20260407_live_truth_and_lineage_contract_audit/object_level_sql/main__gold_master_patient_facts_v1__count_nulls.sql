-- Live counts: Thyroid 2026.main.gold_master_patient_facts_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_table" IS NULL THEN 1 ELSE 0 END) AS null_source_table
FROM "Thyroid 2026"."main"."gold_master_patient_facts_v1";
