-- ============================================================================
-- Migration 32 — MRI01/MRI02/MRI03: MRI imaging normalization
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   MRI01 — explicit API/parse errors              (45 rows — registry 45 exact)
--   MRI02 — LN mentioned but no location            (129 rows observed — registry 71)
--   MRI03 — thyroid_normal=1 + abnormality flags    (5 rows — registry 5 exact)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.mri_imaging (715 rows). Boolean-valued DOUBLE columns (0/1/NULL).
-- `error` column holds parse-error text on 45 rows:
--   'API/parse error after retries: Expecting value: line 1 column 1 (char 0)'
--
-- MRI02 count overshoot mirrors CT01 — parser likely tightened since registry
-- draft. Surfaces as an advisory flag (no queue) pending Logan's reconciliation.
--
-- MRI01 is queued (45 rows; these need source-report re-extraction or
-- hand-annotation). MRI03 is queued (5 rows; clinical contradiction to review).
--
-- Output:
--   manuscript_workspace.mri_imaging_clean (VIEW)
--     + mri_parse_error_flag                 (MRI01)
--     + mri_ln_underspecified_flag           (MRI02; advisory)
--     + mri_normal_contradiction_flag        (MRI03)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.mri_imaging_clean AS
SELECT
  m.*,
  -- MRI01: explicit API/parse error
  (m.error IS NOT NULL) AS mri_parse_error_flag,
  -- MRI02: LN mentioned but no location string (advisory — count overshoots registry)
  (COALESCE(m.lymph_nodes_mentioned,0)=1
     AND (m.lymph_node_locations IS NULL
          OR m.lymph_node_locations IN ('','null','not_mentioned','unknown','none')))
    AS mri_ln_underspecified_flag,
  -- MRI03: thyroid_normal=1 paired with any abnormality flag
  (COALESCE(m.thyroid_normal,0)=1
     AND (COALESCE(m.thyroid_nodule,0)=1
          OR COALESCE(m.thyroid_enlarged,0)=1
          OR COALESCE(m.thyroid_postsurgical,0)=1
          OR COALESCE(m.thyroid_mass_effect,0)=1
          OR COALESCE(m.substernal_goiter,0)=1
          OR COALESCE(m.substernal_extension,0)=1))
    AS mri_normal_contradiction_flag
FROM main.mri_imaging m;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('MRI01','MRI02','MRI03');

-- MRI01: parse errors — queue for re-extraction / hand-annotation
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'MRI01',
  TRY_CAST(research_id AS INTEGER),
  'main.mri_imaging',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(date_of_exam,''), '|',
         COALESCE(mri_label,'')),
  TO_JSON(struct_pack(
    date_of_exam := date_of_exam,
    mri_label := mri_label,
    exam_type_detail := exam_type_detail,
    error := error
  )),
  'MRI01 explicit API/parse error — source report needs hardened re-extraction',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.mri_imaging_clean
WHERE mri_parse_error_flag;

-- MRI03: thyroid_normal=1 with abnormality flags
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'MRI03',
  TRY_CAST(research_id AS INTEGER),
  'main.mri_imaging',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(date_of_exam,''), '|',
         COALESCE(mri_label,'')),
  TO_JSON(struct_pack(
    date_of_exam := date_of_exam,
    thyroid_normal := thyroid_normal,
    thyroid_nodule := thyroid_nodule,
    thyroid_enlarged := thyroid_enlarged,
    thyroid_postsurgical := thyroid_postsurgical,
    thyroid_mass_effect := thyroid_mass_effect,
    substernal_goiter := substernal_goiter,
    substernal_extension := substernal_extension
  )),
  'MRI03 thyroid_normal=1 but abnormality flags also 1',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.mri_imaging_clean
WHERE mri_normal_contradiction_flag;

COMMENT ON TABLE main.mri_imaging IS
'MRI imaging row-per-exam (715 rows). Normalization flags on manuscript_workspace.mri_imaging_clean: mri_parse_error_flag (MRI01=45), mri_ln_underspecified_flag (MRI02=129 advisory — registry 71, overshoot), mri_normal_contradiction_flag (MRI03=5). 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_31';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.mri_imaging','table',
   'manuscript_workspace.mri_imaging_clean',
   'MRI01,MRI02,MRI03','prompt_31','column_only',DATE '2026-04-23',
   'MRI01=45 parse errors (registry match); MRI02=129 LN-mentioned-no-location (registry 71, overshoot — advisory only); MRI03=5 thyroid_normal-contradiction (registry match).',
   NULL,
   'Flags on mri_imaging_clean. MRI01 (45) + MRI03 (5) queued. MRI02 is advisory — registry number needs reconciliation before queuing.');
