-- Live counts: Thyroid 2026.release_20260409.note_extraction_runs
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "run_id" IS NULL THEN 1 ELSE 0 END) AS null_run_id,
  SUM(CASE WHEN "started_at" IS NULL THEN 1 ELSE 0 END) AS null_started_at,
  SUM(CASE WHEN "release_tag" IS NULL THEN 1 ELSE 0 END) AS null_release_tag
FROM "Thyroid 2026"."release_20260409"."note_extraction_runs";
