-- Live counts: Thyroid 2026.main.val_imaging_fna_linkage_audit_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "built_at" IS NULL THEN 1 ELSE 0 END) AS null_built_at
FROM "Thyroid 2026"."main"."val_imaging_fna_linkage_audit_v1";
