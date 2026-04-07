-- Live counts: Thyroid 2026.main.imaging_fna_linkage_mm_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "built_at" IS NULL THEN 1 ELSE 0 END) AS null_built_at,
  SUM(CASE WHEN "fna_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_fna_episode_id
FROM "Thyroid 2026"."main"."imaging_fna_linkage_mm_v1";
