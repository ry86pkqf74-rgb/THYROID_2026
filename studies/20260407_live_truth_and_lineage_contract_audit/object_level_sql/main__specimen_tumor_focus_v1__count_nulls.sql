-- Live counts: Thyroid 2026.main.specimen_tumor_focus_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "surg_date" IS NULL THEN 1 ELSE 0 END) AS null_surg_date,
  SUM(CASE WHEN "surgery_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id,
  SUM(CASE WHEN "identity_build_run_id" IS NULL THEN 1 ELSE 0 END) AS null_identity_build_run_id
FROM "Thyroid 2026"."main"."specimen_tumor_focus_v1";
