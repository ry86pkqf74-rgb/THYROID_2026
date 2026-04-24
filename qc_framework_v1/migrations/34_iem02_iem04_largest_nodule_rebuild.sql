-- ============================================================================
-- Migration 34 — IEM02 + IEM04: largest_nodule_cm rebuild from canonical
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   IEM02 — largest_nodule_cm NULL despite canonical having sizes
--           (7,319 rows total; 4,289 recoverable, 3,030 remain unresolvable)
--   IEM04 — largest_nodule_cm disagrees with canonical by >0.1 cm
--           (7 rows)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Canonical source of truth: main.canonical_us_nodule_v2.size_cm_max, filtered
-- to NOT is_aggregate_row, aggregated by us_exam_id with MAX. View-based
-- resolution — no mutation of main.imaging_exam_master_v1.
--
-- Columns surfaced on the clean view:
--   largest_nodule_cm_canonical  DOUBLE  (from canonical_us_nodule_v2)
--   largest_nodule_cm_final      DOUBLE  (COALESCE of IEM → canonical)
--   largest_nodule_cm_source     VARCHAR ∈ {iem_original, canonical_backfill, mismatch_iem_kept, unresolved}
--   iem02_size_recovered_flag    BOOLEAN
--   iem04_size_mismatch_flag     BOOLEAN
--
-- Resolution rule for mismatches (IEM04, 7 rows): prefer IEM value (carries
-- the legacy analytic history), but flag for human review. If the reviewer
-- finds canonical correct, they can update IEM row-by-row via queue resolution.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.imaging_exam_master_v1_size_clean AS
WITH canonical_size AS (
  SELECT us_exam_id AS exam_id, MAX(size_cm_max) AS canonical_max
  FROM main.canonical_us_nodule_v2
  WHERE NOT is_aggregate_row
  GROUP BY us_exam_id
)
SELECT
  i.*,
  c.canonical_max AS largest_nodule_cm_canonical,
  COALESCE(i.largest_nodule_cm, c.canonical_max) AS largest_nodule_cm_final,
  CASE
    WHEN i.largest_nodule_cm IS NOT NULL AND c.canonical_max IS NOT NULL
         AND ABS(i.largest_nodule_cm - c.canonical_max) > 0.1 THEN 'mismatch_iem_kept'
    WHEN i.largest_nodule_cm IS NOT NULL THEN 'iem_original'
    WHEN c.canonical_max IS NOT NULL THEN 'canonical_backfill'
    ELSE 'unresolved'
  END AS largest_nodule_cm_source,
  (i.largest_nodule_cm IS NULL AND c.canonical_max IS NOT NULL) AS iem02_size_recovered_flag,
  (i.largest_nodule_cm IS NOT NULL AND c.canonical_max IS NOT NULL
   AND ABS(i.largest_nodule_cm - c.canonical_max) > 0.1) AS iem04_size_mismatch_flag
FROM main.imaging_exam_master_v1 i
LEFT JOIN canonical_size c ON i.exam_id = c.exam_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('IEM02','IEM04');

-- IEM02: NULL largest_nodule_cm where canonical ALSO has no size (3,030 unresolvable rows)
-- The 4,289 recoverable rows are resolved silently by the COALESCE; no queue needed.
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'IEM02',
  research_id,
  'main.imaging_exam_master_v1',
  exam_id,
  TO_JSON(struct_pack(
    exam_id := exam_id,
    source := source,
    n_nodules := n_nodules,
    largest_nodule_cm := largest_nodule_cm,
    largest_nodule_cm_canonical := largest_nodule_cm_canonical
  )),
  'IEM02 largest_nodule_cm NULL and canonical_us_nodule_v2 also lacks size — unresolvable within current data',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.imaging_exam_master_v1_size_clean
WHERE largest_nodule_cm_source='unresolved'
  AND source IN ('raw_us_tirads_excel_v1','raw_us_tirads_scored_v1');  -- US sources only; 12_slots sources don't have canonical coverage

-- IEM04: disagreement (7 rows)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'IEM04',
  research_id,
  'main.imaging_exam_master_v1',
  exam_id,
  TO_JSON(struct_pack(
    exam_id := exam_id,
    iem_value := largest_nodule_cm,
    canonical_value := largest_nodule_cm_canonical,
    abs_diff := ABS(largest_nodule_cm - largest_nodule_cm_canonical)
  )),
  'IEM04 largest_nodule_cm IEM vs canonical_us_nodule_v2 differ by >0.1 cm',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.imaging_exam_master_v1_size_clean
WHERE iem04_size_mismatch_flag;

COMMENT ON TABLE main.imaging_exam_master_v1 IS
'Imaging exam master (13,347 rows, 3 sources). Size-clean view: manuscript_workspace.imaging_exam_master_v1_size_clean exposes largest_nodule_cm_final (IEM→canonical COALESCE), largest_nodule_cm_source, iem02_size_recovered_flag, iem04_size_mismatch_flag. 4,289 IEM02 rows backfilled from canonical; 7 IEM04 mismatches queued. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_33';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.imaging_exam_master_v1','table',
   'manuscript_workspace.imaging_exam_master_v1_size_clean',
   'IEM02,IEM04','prompt_33','column_only',DATE '2026-04-23',
   'IEM02: 7,319 NULL largest_nodule_cm (4,289 recovered from canonical, 3,030 unresolvable). IEM04: 7 IEM-vs-canonical mismatches >0.1 cm. View surfaces final value + source tag.',
   NULL,
   'COALESCE(IEM, canonical_us_nodule_v2.size_cm_max) on size_clean view. IEM04 7 rows queued. IEM02 unresolvable rows queued only when from US sources (12_slots source has no canonical coverage).');
