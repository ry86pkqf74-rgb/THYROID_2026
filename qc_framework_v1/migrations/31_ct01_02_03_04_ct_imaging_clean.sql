-- ============================================================================
-- Migration 31 — CT01/CT02/CT03/CT04: CT imaging normalization
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   CT01 — LN mentioned but no location        (4,080 rows observed — registry 975)
--   CT02 — thyroid_not_visualized+abnormality   (140 rows observed — registry 170)
--   CT03 — thyroid_normal+abnormality            (23 rows observed — registry 23 exact)
--   CT04 — tracheal_deviation, direction missing (5,298 rows observed — registry 5,233)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.ct_imaging (7,701 rows). tracheal_deviation enum = {present, none,
-- not_mentioned, NULL}; tracheal_deviation_direction carries free-text
-- directions including 'not_mentioned', 'unknown', 'none', 'null',
-- 'present_unspecified'. A "direction missing" test treats these placeholder
-- values as missing.
--
-- CT01 signal uses lymph_nodes_mentioned=TRUE AND lymph_node_locations IS NULL
-- (or placeholder-null strings). Observed count is higher than the registry
-- estimate; parser may have been tightened since registry draft. Logan to
-- reconcile — flag surfaced on cohort clean view for downstream triage.
--
-- Output:
--   manuscript_workspace.ct_imaging_clean (VIEW)
--     + ct_ln_underspecified_flag         (CT01)
--     + ct_internal_contradiction_flag    (CT02 or CT03)
--     + ct02_notvisualized_contradiction  (CT02 component)
--     + ct03_normal_contradiction         (CT03 component)
--     + ct_tracheal_direction_missing_flag (CT04)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.ct_imaging_clean AS
SELECT
  c.*,
  -- CT01: LN mentioned but no location string
  (COALESCE(c.lymph_nodes_mentioned, FALSE)
     AND (c.lymph_node_locations IS NULL
          OR c.lymph_node_locations IN ('','null','not_mentioned','unknown','none')))
    AS ct_ln_underspecified_flag,
  -- CT02: thyroid_not_visualized=TRUE but other thyroid abnormality flags also TRUE
  (COALESCE(c.thyroid_not_visualized, FALSE)
     AND (COALESCE(c.thyroid_nodule, FALSE)
          OR COALESCE(c.thyroid_enlarged, FALSE)
          OR COALESCE(c.thyroid_heterogeneous, FALSE)
          OR COALESCE(c.thyroid_postsurgical, FALSE)
          OR COALESCE(c.thyroid_other_abnormality, FALSE)
          OR COALESCE(c.goiter_present, FALSE)
          OR COALESCE(c.substernal_extension, FALSE)))
    AS ct02_notvisualized_contradiction,
  -- CT03: thyroid_normal=TRUE but abnormality flags also TRUE
  (COALESCE(c.thyroid_normal, FALSE)
     AND (COALESCE(c.thyroid_nodule, FALSE)
          OR COALESCE(c.thyroid_enlarged, FALSE)
          OR COALESCE(c.thyroid_heterogeneous, FALSE)
          OR COALESCE(c.thyroid_postsurgical, FALSE)
          OR COALESCE(c.thyroid_other_abnormality, FALSE)
          OR COALESCE(c.goiter_present, FALSE)
          OR COALESCE(c.substernal_extension, FALSE)))
    AS ct03_normal_contradiction,
  -- rolled-up internal contradiction flag
  ((COALESCE(c.thyroid_not_visualized, FALSE)
     AND (COALESCE(c.thyroid_nodule, FALSE)
          OR COALESCE(c.thyroid_enlarged, FALSE)
          OR COALESCE(c.thyroid_heterogeneous, FALSE)
          OR COALESCE(c.thyroid_postsurgical, FALSE)
          OR COALESCE(c.thyroid_other_abnormality, FALSE)
          OR COALESCE(c.goiter_present, FALSE)
          OR COALESCE(c.substernal_extension, FALSE)))
   OR
   (COALESCE(c.thyroid_normal, FALSE)
     AND (COALESCE(c.thyroid_nodule, FALSE)
          OR COALESCE(c.thyroid_enlarged, FALSE)
          OR COALESCE(c.thyroid_heterogeneous, FALSE)
          OR COALESCE(c.thyroid_postsurgical, FALSE)
          OR COALESCE(c.thyroid_other_abnormality, FALSE)
          OR COALESCE(c.goiter_present, FALSE)
          OR COALESCE(c.substernal_extension, FALSE))))
    AS ct_internal_contradiction_flag,
  -- CT04: any non-'none' deviation signal but direction unresolvable
  (c.tracheal_deviation IS NOT NULL
     AND c.tracheal_deviation <> 'none'
     AND (c.tracheal_deviation_direction IS NULL
          OR c.tracheal_deviation_direction IN ('','null','not_mentioned','unknown','none','present_unspecified')))
    AS ct_tracheal_direction_missing_flag
FROM main.ct_imaging c;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('CT01','CT02','CT03','CT04');

-- CT02: thyroid_not_visualized contradiction
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'CT02',
  TRY_CAST(research_id AS INTEGER),
  'main.ct_imaging',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(date_of_exam,''), '|',
         COALESCE(ct_column,'')),
  TO_JSON(struct_pack(
    date_of_exam := date_of_exam,
    thyroid_not_visualized := thyroid_not_visualized,
    thyroid_nodule := thyroid_nodule,
    thyroid_enlarged := thyroid_enlarged,
    thyroid_heterogeneous := thyroid_heterogeneous,
    thyroid_postsurgical := thyroid_postsurgical,
    thyroid_other_abnormality := thyroid_other_abnormality,
    goiter_present := goiter_present,
    substernal_extension := substernal_extension
  )),
  'CT02 thyroid_not_visualized=TRUE but other thyroid abnormality flags also TRUE',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.ct_imaging_clean
WHERE ct02_notvisualized_contradiction;

-- CT03: thyroid_normal contradiction
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'CT03',
  TRY_CAST(research_id AS INTEGER),
  'main.ct_imaging',
  CONCAT(COALESCE(CAST(research_id AS VARCHAR),''), '|',
         COALESCE(date_of_exam,''), '|',
         COALESCE(ct_column,'')),
  TO_JSON(struct_pack(
    date_of_exam := date_of_exam,
    thyroid_normal := thyroid_normal,
    thyroid_nodule := thyroid_nodule,
    thyroid_enlarged := thyroid_enlarged,
    thyroid_heterogeneous := thyroid_heterogeneous,
    thyroid_postsurgical := thyroid_postsurgical,
    thyroid_other_abnormality := thyroid_other_abnormality,
    goiter_present := goiter_present,
    substernal_extension := substernal_extension
  )),
  'CT03 thyroid_normal=TRUE but abnormality flags also TRUE',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.ct_imaging_clean
WHERE ct03_normal_contradiction;

COMMENT ON TABLE main.ct_imaging IS
'CT imaging row-per-exam. Normalization flags live on manuscript_workspace.ct_imaging_clean: ct_ln_underspecified_flag (CT01), ct_internal_contradiction_flag with ct02/ct03 split, ct_tracheal_direction_missing_flag (CT04). 2026-04-23 snapshot: CT01=4,080 / CT02=140 / CT03=23 / CT04=5,298.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_30';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.ct_imaging','table',
   'manuscript_workspace.ct_imaging_clean',
   'CT01,CT02,CT03,CT04','prompt_30','column_only',DATE '2026-04-23',
   'CT01=4,080 LN-mentioned-no-location (registry est 975); CT02=140 not-visualized-contradiction (registry 170); CT03=23 normal-contradiction (match); CT04=5,298 tracheal-direction-missing (registry 5,233). CT01 and CT04 overshoot registry — Logan to reconcile.',
   NULL,
   'Flags on ct_imaging_clean. CT02 (140) + CT03 (23) queued under matching issue_ids. CT01 and CT04 are advisory flags; no queue emissions until registry numbers reconciled.');
