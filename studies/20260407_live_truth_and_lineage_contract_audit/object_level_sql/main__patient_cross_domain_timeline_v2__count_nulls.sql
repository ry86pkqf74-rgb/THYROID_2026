-- Live counts: Thyroid 2026.main.patient_cross_domain_timeline_v2
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "event_date" IS NULL THEN 1 ELSE 0 END) AS null_event_date
FROM "Thyroid 2026"."main"."patient_cross_domain_timeline_v2";
