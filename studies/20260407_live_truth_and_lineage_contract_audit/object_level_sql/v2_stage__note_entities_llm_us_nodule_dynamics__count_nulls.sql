-- Live counts: Thyroid 2026.v2_stage.note_entities_llm_us_nodule_dynamics
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "note_row_id" IS NULL THEN 1 ELSE 0 END) AS null_note_row_id,
  SUM(CASE WHEN "note_date" IS NULL THEN 1 ELSE 0 END) AS null_note_date,
  SUM(CASE WHEN "extraction_run_id" IS NULL THEN 1 ELSE 0 END) AS null_extraction_run_id
FROM "Thyroid 2026"."v2_stage"."note_entities_llm_us_nodule_dynamics";
