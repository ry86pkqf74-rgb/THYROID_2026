-- Live counts: Thyroid 2026.main.tg_lab_review_queue_v1
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "specimen_collect_dt" IS NULL THEN 1 ELSE 0 END) AS null_specimen_collect_dt
FROM "Thyroid 2026"."main"."tg_lab_review_queue_v1";
