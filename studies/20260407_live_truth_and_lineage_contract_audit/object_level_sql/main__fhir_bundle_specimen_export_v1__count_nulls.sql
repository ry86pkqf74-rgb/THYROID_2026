-- Live counts: Thyroid 2026.main.fhir_bundle_specimen_export_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "specimen_id" IS NULL THEN 1 ELSE 0 END) AS null_specimen_id,
  SUM(CASE WHEN "built_at" IS NULL THEN 1 ELSE 0 END) AS null_built_at
FROM "Thyroid 2026"."main"."fhir_bundle_specimen_export_v1";
