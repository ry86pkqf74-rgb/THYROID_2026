-- Live counts: Thyroid 2026.main.mrn_crosswalk_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id
FROM "Thyroid 2026"."main"."mrn_crosswalk_v1";
