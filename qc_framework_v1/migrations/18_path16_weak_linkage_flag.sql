-- ============================================================================
-- Migration 18 — PATH16: flag weak-linkage pathology events
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH16
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- canonical_path_malignant_events_v1.linkage_confidence_tier distribution:
--   high_confidence  3,460
--   NULL             3,026  (events where linkage not assigned; not in scope here)
--   plausible          127
--   exact_match         67
--   weak                 9  ← PATH16 target: linkage_score < 0.4, typically from
--                             STL+TEM resolution rule w/ low data_completeness_pct
--
-- Queue holds the 9 weak-tier rows for chart review. Downstream can filter
-- WHERE linkage_confidence_tier NOT IN ('weak') to exclude.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_path_malignant_events_v1_weak_linkage_flag AS
SELECT
    e.*,
    (e.linkage_confidence_tier = 'weak') AS path_weak_linkage_flag
FROM main.canonical_path_malignant_events_v1 e;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH16';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
    'PATH16',
    CAST(e.research_id AS INTEGER),
    'main.canonical_path_malignant_events_v1',
    CONCAT_WS('|',
        CAST(e.research_id AS VARCHAR),
        CAST(e.surgery_date AS VARCHAR),
        CAST(e.tumor_ordinal AS VARCHAR),
        CAST(COALESCE(e.specimen_id, '') AS VARCHAR)
    ),
    TO_JSON(struct_pack(
        linkage_score := e.linkage_score,
        resolution_rule := e.resolution_rule,
        data_completeness_pct := e.data_completeness_pct,
        source_tables := e.source_tables
    )),
    CONCAT('Weak linkage (tier=weak, score=', CAST(e.linkage_score AS VARCHAR),
           ', completeness=', CAST(e.data_completeness_pct AS VARCHAR), '%)'),
    'open',
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_path_malignant_events_v1 e
WHERE e.linkage_confidence_tier = 'weak';

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.linkage_confidence_tier IS
'Per-row linkage confidence. 4 values: exact_match(67) > high_confidence(3,460) > plausible(127) > weak(9). 3,026 NULL (linkage not assigned). PATH16 targets weak tier — see manuscript_workspace.canonical_path_malignant_events_v1_weak_linkage_flag.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_17';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1.linkage_confidence_tier','column',
   'manuscript_workspace.canonical_path_malignant_events_v1_weak_linkage_flag',
   'PATH16','prompt_17','column_only',DATE '2026-04-23',
   '9 rows at linkage_confidence_tier=weak require chart review; score range 0.069 - 0.337.',
   NULL,
   'path_weak_linkage_flag boolean surfaces violation; queue emits 9 rows under PATH16.');
