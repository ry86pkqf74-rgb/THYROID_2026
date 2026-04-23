-- ============================================================================
-- Migration 01 — PATH01 / OP05: re-key path_malignant to global operative namespace
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     PATH01 (broken surgery_episode_id on path), OP05 (orphan path rows)
-- Author:        Logan Glosser
-- Date:          2026-04-22
-- ----------------------------------------------------------------------------
-- Context:
--   main.canonical_operative_events_v1 is clean: 11,773 globally-unique
--   surgery_episode_id values, 1:1 with (research_id, CAST(resolved_surgery_date
--   AS TIMESTAMP)).  main.canonical_path_malignant_events_v1.surgery_episode_id
--   still carries patient-local ordinals (only 3 distinct values; 1,434 NULL).
--   Solve by LEFT JOIN on (research_id, surgery_date), not MD5 hashing.
--
-- Coverage probes (pre-run):
--   11,773 op rows — 100% cast_ok on resolved_surgery_date → TIMESTAMP
--   (research_id, dt) unique on op — no LEFT JOIN fan-out
--   path: 6,689 rows total, 0 NULL surgery_date
--
-- Contract:
--   - main.* untouched
--   - view rebuilt via CREATE OR REPLACE (idempotent)
--   - md5_fallback rows emitted to qc_manual_review_queue_v1 (issue_id='OP05')
--     via INSERT guarded by NOT EXISTS on (issue_id, source_table, source_pk)
--     so re-running does not duplicate queue rows
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_keyed AS
SELECT
    p.*,
    op.surgery_episode_id AS surgery_episode_uid_global,
    CASE
        WHEN op.surgery_episode_id IS NULL AND p.surgery_date IS NOT NULL
            THEN md5(CAST(p.research_id AS VARCHAR) || '|' || CAST(p.surgery_date AS VARCHAR))
        ELSE NULL
    END AS surgery_episode_uid_fallback,
    COALESCE(
        CAST(op.surgery_episode_id AS VARCHAR),
        CASE
            WHEN p.surgery_date IS NOT NULL
                THEN md5(CAST(p.research_id AS VARCHAR) || '|' || CAST(p.surgery_date AS VARCHAR))
            ELSE NULL
        END
    ) AS surgery_episode_uid,
    CASE
        WHEN op.surgery_episode_id IS NOT NULL THEN 'operative_match'
        WHEN p.surgery_date IS NOT NULL        THEN 'md5_fallback'
        ELSE                                        'unknown_no_date'
    END AS surgery_episode_uid_source
FROM main.canonical_path_malignant_events_v1 p
LEFT JOIN main.canonical_operative_events_v1 op
       ON p.research_id = op.research_id
      AND p.surgery_date = CAST(op.resolved_surgery_date AS TIMESTAMP);

-- ---------------------------------------------------------------------------
-- Queue emission for md5_fallback and unknown_no_date rows
-- Idempotent via NOT EXISTS guard on (issue_id, source_table, source_pk)
-- ---------------------------------------------------------------------------

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'OP05'                                             AS issue_id,
    k.research_id                                      AS research_id,
    'main.canonical_path_malignant_events_v1'          AS source_table,
    CAST(k.path_surgery_id AS VARCHAR)                 AS source_pk,
    TO_JSON(struct_pack(
        surgery_date      := k.surgery_date,
        tumor_ordinal     := k.tumor_ordinal,
        primary_histology := k.primary_histology,
        specimen_id       := k.specimen_id,
        uid_source        := k.surgery_episode_uid_source
    ))                                                 AS context_json,
    'path_malignant row has no matching operative episode' AS reason
FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed k
WHERE k.surgery_episode_uid_source IN ('md5_fallback','unknown_no_date')
  AND NOT EXISTS (
      SELECT 1
      FROM manuscript_workspace.qc_manual_review_queue_v1 q
      WHERE q.issue_id     = 'OP05'
        AND q.source_table = 'main.canonical_path_malignant_events_v1'
        AND q.source_pk    = CAST(k.path_surgery_id AS VARCHAR)
  );
