-- Live counts: Thyroid 2026.main.specimen_master_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "specimen_fingerprint_sha256" IS NULL THEN 1 ELSE 0 END) AS null_specimen_fingerprint_sha256,
  SUM(CASE WHEN "procedure_date_day" IS NULL THEN 1 ELSE 0 END) AS null_procedure_date_day,
  SUM(CASE WHEN "surgery_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id,
  SUM(CASE WHEN "identity_build_run_id" IS NULL THEN 1 ELSE 0 END) AS null_identity_build_run_id
FROM "Thyroid 2026"."main"."specimen_master_v1";
