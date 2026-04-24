-- ============================================================================
-- Migration 35 — IEM03 + IEM05: n_nodules rebuild from canonical
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:
--   IEM03 — n_nodules disagrees with canonical_us_nodule_v2 count per exam
--           (116 exams observed — registry 19; all in raw_us_tirads_scored_v1)
--   IEM05 — raw_us_tirads_scored_v1 source tag overcounts
--           (2,506 rows; source marker only — fix is IEM03's canonical rebuild)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Canonical source of truth: COUNT(DISTINCT nodule_index_within_exam) over
-- main.canonical_us_nodule_v2 filtered to NOT is_aggregate_row.
--
-- All 116 IEM03 mismatches concentrate in source=raw_us_tirads_scored_v1
-- (legacy pathway). Both US sources and the 12_slots source are clean on
-- this axis. View-based rebuild — no main.* mutation.
--
-- Columns surfaced on the clean view:
--   n_nodules_canonical     BIGINT (COUNT DISTINCT nodule_index_within_exam)
--   n_nodules_final         BIGINT (canonical if present, else IEM)
--   n_nodules_source        VARCHAR ∈ {canonical, iem_original, legacy_source_kept, unresolved}
--   iem03_n_nodules_mismatch_flag  BOOLEAN
--   iem05_legacy_source_flag       BOOLEAN (source=raw_us_tirads_scored_v1)
--
-- Resolution: canonical wins (authoritative). For legacy-source rows (IEM05),
-- the canonical value replaces IEM value at the view layer; no row-by-row
-- human review needed. 116 rows queued for audit trail so downstream analysts
-- can see the delta.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.imaging_exam_master_v1_count_clean AS
WITH canonical_count AS (
  SELECT us_exam_id AS exam_id,
         COUNT(DISTINCT nodule_index_within_exam) AS canonical_n
  FROM main.canonical_us_nodule_v2
  WHERE NOT is_aggregate_row
  GROUP BY us_exam_id
)
SELECT
  i.*,
  c.canonical_n AS n_nodules_canonical,
  COALESCE(c.canonical_n, i.n_nodules) AS n_nodules_final,
  CASE
    WHEN c.canonical_n IS NOT NULL AND i.n_nodules IS DISTINCT FROM c.canonical_n
      THEN 'canonical'
    WHEN c.canonical_n IS NOT NULL THEN 'canonical'
    WHEN i.n_nodules IS NOT NULL THEN 'iem_original'
    ELSE 'unresolved'
  END AS n_nodules_source,
  (c.canonical_n IS NOT NULL AND i.n_nodules IS DISTINCT FROM c.canonical_n) AS iem03_n_nodules_mismatch_flag,
  (i.source='raw_us_tirads_scored_v1') AS iem05_legacy_source_flag
FROM main.imaging_exam_master_v1 i
LEFT JOIN canonical_count c ON i.exam_id = c.exam_id;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('IEM03','IEM05');

-- IEM03: mismatches queued for audit trail (not human review — canonical wins automatically)
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'IEM03',
  research_id,
  'main.imaging_exam_master_v1',
  exam_id,
  TO_JSON(struct_pack(
    exam_id := exam_id,
    source := source,
    iem_n := n_nodules,
    canonical_n := n_nodules_canonical,
    delta := (n_nodules - n_nodules_canonical)
  )),
  'IEM03 n_nodules IEM vs canonical_us_nodule_v2 mismatch (canonical is authoritative; view now serves canonical value)',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.imaging_exam_master_v1_count_clean
WHERE iem03_n_nodules_mismatch_flag;

-- IEM05: source-level marker; no queue (informational — all 2,506 rows ride on the IEM03 rebuild)

COMMENT ON TABLE main.imaging_exam_master_v1 IS
'Imaging exam master (13,347 rows, 3 sources). Count-clean view: manuscript_workspace.imaging_exam_master_v1_count_clean serves n_nodules_final (canonical→IEM COALESCE), n_nodules_source, iem03_n_nodules_mismatch_flag (116 rows, all from raw_us_tirads_scored_v1), iem05_legacy_source_flag. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_34';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.imaging_exam_master_v1','table',
   'manuscript_workspace.imaging_exam_master_v1_count_clean',
   'IEM03,IEM05','prompt_34','column_only',DATE '2026-04-23',
   'IEM03: 116 n_nodules mismatches (registry 19 — count risen since snapshot); all in raw_us_tirads_scored_v1. IEM05: 2,506 legacy-source rows flagged. Canonical wins at view layer.',
   NULL,
   'n_nodules_final on count_clean view replaces IEM with canonical_us_nodule_v2 count. 116 rows queued as audit trail. IEM05 source flag is advisory — no queue emission (rolled into IEM03 fix).');
