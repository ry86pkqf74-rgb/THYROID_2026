-- Live counts: Thyroid 2026.main.fna_molecular_linkage_v3
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "fna_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_fna_episode_id
FROM "Thyroid 2026"."main"."fna_molecular_linkage_v3";
