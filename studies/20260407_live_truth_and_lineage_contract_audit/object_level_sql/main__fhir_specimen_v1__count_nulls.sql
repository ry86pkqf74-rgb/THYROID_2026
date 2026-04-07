-- Live counts: Thyroid 2026.main.fhir_specimen_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "patient_fhir_id" IS NULL THEN 1 ELSE 0 END) AS null_patient_fhir_id,
  SUM(CASE WHEN "built_at" IS NULL THEN 1 ELSE 0 END) AS null_built_at,
  SUM(CASE WHEN "specimen_id" IS NULL THEN 1 ELSE 0 END) AS null_specimen_id
FROM "Thyroid 2026"."main"."fhir_specimen_v1";
