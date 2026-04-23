-- ============================================================================
-- Migration 17 — PATH13: flag duplicate path event rows
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH13
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Dedup key: (research_id, surgery_date, tumor_ordinal, specimen_id, laterality, site)
--   total rows: 6,689
--   distinct keys: 6,686
--   dup groups: 3 (each pair of 2) — 6 rows total, 3 excess
--
-- Small queue — 3 groups — manually reviewed. Not dropping rows; flagging
-- the group and exposing row_rank so downstream can "take first" deterministically.
--
-- Output:
--   manuscript_workspace.canonical_path_malignant_events_v1_dedup_flag
--     — all rows + dup_group_size + dup_row_rank (1 = first by synoptic_row_ix,
--       2+ = duplicates to suppress).
--
-- Queue under PATH13, one row per dup GROUP (not per row).
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_dedup_flag AS
WITH ranked AS (
    SELECT
        e.*,
        COUNT(*) OVER (
            PARTITION BY e.research_id, e.surgery_date, e.tumor_ordinal,
                         e.specimen_id, e.laterality, e.site
        ) AS dup_group_size,
        ROW_NUMBER() OVER (
            PARTITION BY e.research_id, e.surgery_date, e.tumor_ordinal,
                         e.specimen_id, e.laterality, e.site
            ORDER BY e.synoptic_row_ix, e.build_ts
        ) AS dup_row_rank
    FROM main.canonical_path_malignant_events_v1 e
)
SELECT
    r.*,
    (r.dup_group_size > 1 AND r.dup_row_rank > 1) AS dup_suppress_flag
FROM ranked r;

-- Idempotent queue — one row per dup GROUP
DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH13';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'PATH13',
    CAST(research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|',
        CAST(research_id AS VARCHAR),
        CAST(surgery_date AS VARCHAR),
        CAST(tumor_ordinal AS VARCHAR),
        CAST(COALESCE(specimen_id, '') AS VARCHAR),
        CAST(COALESCE(laterality, '') AS VARCHAR),
        CAST(COALESCE(site, '') AS VARCHAR)
    ) AS source_pk,
    TO_JSON(struct_pack(
        dup_group_size := COUNT(*),
        synoptic_row_ix_list := STRING_AGG(CAST(synoptic_row_ix AS VARCHAR), ','
                                            ORDER BY synoptic_row_ix)
    )),
    CONCAT('Duplicate key group of size ', CAST(COUNT(*) AS VARCHAR),
           ' on (research_id, surgery_date, tumor_ordinal, specimen_id, laterality, site)'),
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_path_malignant_events_v1
GROUP BY research_id, surgery_date, tumor_ordinal, specimen_id, laterality, site
HAVING COUNT(*) > 1;

COMMENT ON TABLE main.canonical_path_malignant_events_v1 IS
'Canonical per-tumor-per-surgery pathology events. 6,689 rows, 3 duplicate key groups (PATH13). Use manuscript_workspace.canonical_path_malignant_events_v1_dedup_flag to suppress duplicates via dup_suppress_flag=false.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_16';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1','table',
   'manuscript_workspace.canonical_path_malignant_events_v1_dedup_flag',
   'PATH13','prompt_16','pointer_only',DATE '2026-04-23',
   '3 duplicate key groups on (research_id, surgery_date, tumor_ordinal, specimen_id, laterality, site).',
   NULL,
   'Dedup via dup_suppress_flag=false (keeps lowest synoptic_row_ix). Queue holds 3 group-level review rows.');
