-- ============================================================================
-- Migration 29 — USGLAND01/USGLAND02: gland-exam shells + parenchymal rebuild
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     USGLAND01 (shell rows)               —  6,785 of 13,578 rows
--                USGLAND02 (parenchymal fields NULL)  — 13,578 of 13,578 (100%)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.canonical_us_thyroid_gland_v2:
--   total_rows:          13,578
--   shell rows:           6,785  (USGLAND01: all 10 measure cols NULL)
--   parenchymal all-NULL 13,578  (USGLAND02: parse layer never populated
--                                background_echogenicity, heterogeneity,
--                                hashimoto_pattern, vascularity_overall,
--                                calcifications_parenchymal, goiter_flag,
--                                pyramidal_present_flag, substernal_extension_flag)
--
-- Output:
--   manuscript_workspace.canonical_us_thyroid_gland_v2_shape
--     + gland_row_type ∈ {measured, shell}
--     + parenchymal_all_null_flag (USGLAND02 regression guard)
--
-- USGLAND02 remediation requires an LLM re-parse over gland exam free text.
-- That pass is out of scope for this prompt — tracked in
-- qc_framework_v1/NOTES/usgland_parenchymal_rebuild.md.
--
-- Queue: USGLAND01 emits one row per patient with only-shell gland exams.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_us_thyroid_gland_v2_shape AS
SELECT
  g.*,
  (g.rl_length_cm IS NULL AND g.rl_width_cm IS NULL AND g.rl_depth_cm IS NULL AND g.rl_volume_ml IS NULL
   AND g.ll_length_cm IS NULL AND g.ll_width_cm IS NULL AND g.ll_depth_cm IS NULL AND g.ll_volume_ml IS NULL
   AND g.isthmus_thickness_mm IS NULL AND g.total_thyroid_volume_ml IS NULL) AS shell_row_flag,
  (g.background_echogenicity IS NULL AND g.heterogeneity IS NULL AND g.hashimoto_pattern IS NULL
   AND g.vascularity_overall IS NULL AND g.calcifications_parenchymal IS NULL AND g.goiter_flag IS NULL
   AND g.pyramidal_present_flag IS NULL AND g.substernal_extension_flag IS NULL) AS parenchymal_all_null_flag,
  CASE
    WHEN (g.rl_length_cm IS NULL AND g.rl_width_cm IS NULL AND g.rl_depth_cm IS NULL AND g.rl_volume_ml IS NULL
          AND g.ll_length_cm IS NULL AND g.ll_width_cm IS NULL AND g.ll_depth_cm IS NULL AND g.ll_volume_ml IS NULL
          AND g.isthmus_thickness_mm IS NULL AND g.total_thyroid_volume_ml IS NULL)
      THEN 'shell'
    ELSE 'measured'
  END AS gland_row_type
FROM main.canonical_us_thyroid_gland_v2 g;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('USGLAND01','USGLAND02');

-- USGLAND01: one row per patient whose gland exams are ALL shells
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'USGLAND01',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_us_thyroid_gland_v2',
  CAST(research_id AS VARCHAR),
  TO_JSON(struct_pack(
    n_exams := n_exams,
    n_shell_exams := n_shell_exams,
    n_measured_exams := n_measured_exams
  )),
  CONCAT('Patient has ', CAST(n_exams AS VARCHAR), ' gland exams, all shells — no gland measurements'),
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM (
  SELECT
    research_id,
    COUNT(*) AS n_exams,
    SUM(shell_row_flag::INT) AS n_shell_exams,
    SUM((NOT shell_row_flag)::INT) AS n_measured_exams
  FROM manuscript_workspace.canonical_us_thyroid_gland_v2_shape
  GROUP BY research_id
) per_patient
WHERE n_measured_exams = 0;

COMMENT ON TABLE main.canonical_us_thyroid_gland_v2 IS
'Per-exam gland table. 6,785 of 13,578 rows are shells (USGLAND01); 100% of rows have all parenchymal-phenotype fields NULL (USGLAND02) — parse layer never populated them; LLM rebuild pending (see qc_framework_v1/NOTES/usgland_parenchymal_rebuild.md). Use manuscript_workspace.canonical_us_thyroid_gland_v2_shape.gland_row_type.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_28';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_thyroid_gland_v2','table',
   'manuscript_workspace.canonical_us_thyroid_gland_v2_shape',
   'USGLAND01,USGLAND02','prompt_28','column_only',DATE '2026-04-23',
   '6,785 shell rows out of 13,578 (USGLAND01). 100% of rows have all 8 parenchymal-phenotype fields NULL (USGLAND02) — requires LLM rebuild pass.',
   NULL,
   'gland_row_type ∈ {measured, shell} + shell_row_flag + parenchymal_all_null_flag. USGLAND01 rows queued at per-patient grain (only-shell patients). USGLAND02 tracked as TODO — rebuild out of scope for prompt 28.');
