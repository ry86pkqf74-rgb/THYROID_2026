-- Live counts: Thyroid 2026.main.specimen_source_xref_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "specimen_id" IS NULL THEN 1 ELSE 0 END) AS null_specimen_id,
  SUM(CASE WHEN "source_table" IS NULL THEN 1 ELSE 0 END) AS null_source_table,
  SUM(CASE WHEN "identity_build_run_id" IS NULL THEN 1 ELSE 0 END) AS null_identity_build_run_id
FROM "Thyroid 2026"."main"."specimen_source_xref_v1";
