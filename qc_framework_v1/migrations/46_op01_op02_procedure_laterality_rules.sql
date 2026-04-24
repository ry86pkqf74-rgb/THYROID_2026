-- ============================================================================
-- Migration 46 — OP01 + OP02: procedure × laterality rule violations
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   OP01 — procedure_normalized='total_thyroidectomy' with unilateral laterality
--          (33 rows / 33 pts)
--   OP02 — procedure_normalized='hemithyroidectomy' with laterality='bilateral'
--          (3 rows / 3 pts)
-- Author:        Logan Glosser
-- Date:          2026-04-24
-- ----------------------------------------------------------------------------
-- Probe (2026-04-24) — registry counts confirmed exactly.
--
-- Rule rationale:
--   Total thyroidectomy is by definition bilateral gland removal — a row
--   showing procedure_normalized='total_thyroidectomy' + laterality∈{left,right}
--   is internally inconsistent and typically means either (a) really a
--   completion thyroidectomy of the remaining lobe after a prior hemi, or
--   (b) really a hemithyroidectomy misclassified upstream. Requires chart
--   review — cannot be auto-fixed.
--
--   Hemithyroidectomy is unilateral — bilateral hemi is semantically the same
--   as total. Likely upstream classification bug; flag for review.
--
-- Output:
--   manuscript_workspace.canonical_operative_events_v1_rule_clean (VIEW)
--     + op01_tt_unilateral_flag       BOOLEAN
--     + op02_hemi_bilateral_flag      BOOLEAN
--     + procedure_normalized_trusted  VARCHAR — NULL if either flag set
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_operative_events_v1_rule_clean AS
SELECT
  e.*,
  (e.procedure_normalized='total_thyroidectomy' AND e.laterality IN ('left','right'))
    AS op01_tt_unilateral_flag,
  (e.procedure_normalized='hemithyroidectomy' AND e.laterality='bilateral')
    AS op02_hemi_bilateral_flag,
  CASE
    WHEN (e.procedure_normalized='total_thyroidectomy' AND e.laterality IN ('left','right'))
      OR (e.procedure_normalized='hemithyroidectomy' AND e.laterality='bilateral')
    THEN NULL
    ELSE e.procedure_normalized
  END AS procedure_normalized_trusted
FROM main.canonical_operative_events_v1 e;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('OP01','OP02');

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'OP01',
  research_id,
  'main.canonical_operative_events_v1',
  CAST(surgery_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    procedure_raw := procedure_raw,
    procedure_normalized := procedure_normalized,
    laterality := laterality
  )),
  'OP01 total_thyroidectomy + unilateral laterality — likely completion thyroidectomy mis-classified',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_operative_events_v1_rule_clean
WHERE op01_tt_unilateral_flag;

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'OP02',
  research_id,
  'main.canonical_operative_events_v1',
  CAST(surgery_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    procedure_raw := procedure_raw,
    procedure_normalized := procedure_normalized,
    laterality := laterality
  )),
  'OP02 hemithyroidectomy + bilateral laterality — likely really a total, mis-classified as hemi',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_operative_events_v1_rule_clean
WHERE op02_hemi_bilateral_flag;

COMMENT ON TABLE main.canonical_operative_events_v1 IS
'Operative events (11,773 rows). Clean view manuscript_workspace.canonical_operative_events_v1_rule_clean surfaces op01_tt_unilateral_flag (33), op02_hemi_bilateral_flag (3), and procedure_normalized_trusted (NULL where flagged). Downstream cohort queries should use procedure_normalized_trusted for rule-clean classification. 2026-04-24.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_45';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_operative_events_v1.procedure_normalized','column',
   'manuscript_workspace.canonical_operative_events_v1_rule_clean.procedure_normalized_trusted',
   'OP01,OP02','prompt_45','column_only',DATE '2026-04-24',
   'OP01 (33) + OP02 (3) = 36 rows with procedure × laterality internal inconsistency. Queued for chart review; procedure_normalized_trusted nulled until resolved.',
   NULL,
   'Rule: total_thyroidectomy must be bilateral; hemithyroidectomy must be unilateral. Rule-violating rows have procedure_normalized_trusted=NULL on the clean view. Original procedure_normalized preserved for audit.');
