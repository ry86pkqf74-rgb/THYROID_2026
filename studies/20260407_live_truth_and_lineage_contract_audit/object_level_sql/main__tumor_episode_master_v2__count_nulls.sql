-- Live counts: Thyroid 2026.main.tumor_episode_master_v2
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "surgery_date" IS NULL THEN 1 ELSE 0 END) AS null_surgery_date,
  SUM(CASE WHEN "surgery_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id
FROM "Thyroid 2026"."main"."tumor_episode_master_v2";
