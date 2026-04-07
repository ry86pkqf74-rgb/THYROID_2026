-- Live counts: Thyroid 2026.main.patient_refined_master_clinical_v12
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id
FROM "Thyroid 2026"."main"."patient_refined_master_clinical_v12";
