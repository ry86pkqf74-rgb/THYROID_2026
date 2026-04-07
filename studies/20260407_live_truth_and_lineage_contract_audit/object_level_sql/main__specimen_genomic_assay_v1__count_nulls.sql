-- Live counts: Thyroid 2026.main.specimen_genomic_assay_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "source_table" IS NULL THEN 1 ELSE 0 END) AS null_source_table,
  SUM(CASE WHEN "surgery_episode_id" IS NULL THEN 1 ELSE 0 END) AS null_surgery_episode_id,
  SUM(CASE WHEN "materialized_at" IS NULL THEN 1 ELSE 0 END) AS null_materialized_at
FROM "Thyroid 2026"."main"."specimen_genomic_assay_v1";
