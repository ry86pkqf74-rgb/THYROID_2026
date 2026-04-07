-- Live counts: Thyroid 2026.main.fhir_episode_of_care_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "patient_fhir_id" IS NULL THEN 1 ELSE 0 END) AS null_patient_fhir_id,
  SUM(CASE WHEN "built_at" IS NULL THEN 1 ELSE 0 END) AS null_built_at,
  SUM(CASE WHEN "surgery_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id
FROM "Thyroid 2026"."main"."fhir_episode_of_care_v1";
