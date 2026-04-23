-- ============================================================================
-- Migration 27 — US01/US02/US03: size/location/aggregate handling
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     US01 (sizeless rows)        — 3,647 rows (3,067 shell + 580 sizeless)
--                US02 (locationless rows)    — 5,039 rows (3,067 shell + 1,972 locationless)
--                US03 (aggregate-in-nodule)  —   141 rows
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.canonical_us_nodule_v2 cross-tab (37,579 rows):
--   is_agg=F, size populated, loc populated        31,819   nodule_with_measures
--   is_agg=F, all size NULL, all loc NULL           3,067   shell
--   is_agg=F, size populated, all loc NULL          1,972   nodule_locationless
--   is_agg=F, all size NULL, loc populated            580   nodule_sizeless
--   is_agg=T (size populated)                         133   aggregate_row
--   is_agg=T (all size NULL)                            8   aggregate_row
--
-- Classifier priority (top wins):
--   1. aggregate_row   — is_aggregate_row = TRUE                         (141)
--   2. shell           — all 6 size cols NULL AND all 3 loc cols NULL  (3,067)
--   3. nodule_sizeless — all 6 size cols NULL                           (580)
--   4. nodule_locationless — all 3 loc cols NULL                      (1,972)
--   5. nodule_with_measures — fall-through                           (31,819)
--
-- nodule_index_within_exam IS NULL: 0 rows — placeholder from prompt does not
-- catch anything, so is_aggregate_row IS the source of truth for US03.
--
-- Output:
--   manuscript_workspace.canonical_us_nodule_v2_filtered
--     + us_row_type ∈ {nodule_with_measures, nodule_sizeless, nodule_locationless,
--                      aggregate_row, shell}
--     + size_all_null_flag
--     + location_all_null_flag
--
-- Downstream per-nodule analytics: us_row_type = 'nodule_with_measures'.
-- Aggregate rows are NOT deleted — they remain addressable by audit tooling.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_us_nodule_v2_filtered AS
SELECT
  n.*,
  (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
   AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
    AS size_all_null_flag,
  (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
    AS location_all_null_flag,
  CASE
    WHEN n.is_aggregate_row THEN 'aggregate_row'
    WHEN (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
          AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
         AND (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
      THEN 'shell'
    WHEN (n.length_mm IS NULL AND n.width_mm IS NULL AND n.height_mm IS NULL
          AND n.volume_ml IS NULL AND n.size_cm_max IS NULL AND n.extracted_size_cm IS NULL)
      THEN 'nodule_sizeless'
    WHEN (n.laterality IS NULL AND n.location_raw IS NULL AND n.location_detail IS NULL)
      THEN 'nodule_locationless'
    ELSE 'nodule_with_measures'
  END AS us_row_type
FROM main.canonical_us_nodule_v2 n;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('US01','US02','US03');

-- US01: sizeless (not aggregate)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'US01',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_us_nodule_v2',
  CAST(nodule_master_id AS VARCHAR),
  TO_JSON(struct_pack(
    us_exam_id := us_exam_id,
    nodule_index_within_exam := nodule_index_within_exam,
    us_row_type := us_row_type,
    resolution_rule := resolution_rule
  )),
  CONCAT('US01 sizeless row (us_row_type=', us_row_type, ')'),
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_us_nodule_v2_filtered
WHERE us_row_type IN ('shell','nodule_sizeless');

-- US02: locationless (not aggregate)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'US02',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_us_nodule_v2',
  CAST(nodule_master_id AS VARCHAR),
  TO_JSON(struct_pack(
    us_exam_id := us_exam_id,
    nodule_index_within_exam := nodule_index_within_exam,
    us_row_type := us_row_type,
    resolution_rule := resolution_rule
  )),
  CONCAT('US02 locationless row (us_row_type=', us_row_type, ')'),
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_us_nodule_v2_filtered
WHERE us_row_type IN ('shell','nodule_locationless');

-- US03: aggregate rows embedded in per-nodule table
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'US03',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_us_nodule_v2',
  CAST(nodule_master_id AS VARCHAR),
  TO_JSON(struct_pack(
    us_exam_id := us_exam_id,
    nodule_index_within_exam := nodule_index_within_exam,
    us_row_type := us_row_type,
    size_cm_max := size_cm_max,
    resolution_rule := resolution_rule
  )),
  'US03 aggregate row in per-nodule table — should be filtered out of per-nodule analytics',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_us_nodule_v2_filtered
WHERE us_row_type = 'aggregate_row';

COMMENT ON COLUMN main.canonical_us_nodule_v2.is_aggregate_row IS
'TRUE when row summarizes the entire gland rather than a single nodule (141 rows, US03). Filter these out of per-nodule analytics via manuscript_workspace.canonical_us_nodule_v2_filtered.us_row_type=aggregate_row.';

COMMENT ON COLUMN main.canonical_us_nodule_v2.length_mm IS
'Per-nodule length in mm. 3,647 rows have all 6 size columns NULL (US01). See manuscript_workspace.canonical_us_nodule_v2_filtered.us_row_type for row-type classification.';

COMMENT ON COLUMN main.canonical_us_nodule_v2.laterality IS
'Per-nodule laterality. 5,039 rows have all 3 location columns NULL (US02). See manuscript_workspace.canonical_us_nodule_v2_filtered.us_row_type.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_26';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_us_nodule_v2.(length_mm,width_mm,height_mm,volume_ml,size_cm_max,extracted_size_cm,laterality,location_raw,location_detail,is_aggregate_row)','column_group',
   'manuscript_workspace.canonical_us_nodule_v2_filtered',
   'US01,US02,US03','prompt_26','column_only',DATE '2026-04-23',
   '3,647 sizeless rows (US01); 5,039 locationless rows (US02); 141 aggregate rows (US03). 3,067 of these are "shell" rows with both size AND location all-NULL — likely LLM-reparse candidates.',
   NULL,
   'us_row_type ∈ {nodule_with_measures (31,819), nodule_sizeless (580), nodule_locationless (1,972), aggregate_row (141), shell (3,067)}. Downstream per-nodule analytics filter us_row_type=nodule_with_measures. 3,647 US01 + 5,039 US02 + 141 US03 queue rows emitted.');
