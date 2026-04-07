-- Live counts: Thyroid 2026.main.note_entities_llm_dynamic_risk_response
SELECT
  COUNT(*) AS row_count,
  SUM(CASE WHEN "research_id" IS NULL THEN 1 ELSE 0 END) AS null_research_id,
  SUM(CASE WHEN "note_row_id" IS NULL THEN 1 ELSE 0 END) AS null_note_row_id,
  SUM(CASE WHEN "note_date" IS NULL THEN 1 ELSE 0 END) AS null_note_date
FROM "Thyroid 2026"."main"."note_entities_llm_dynamic_risk_response";
