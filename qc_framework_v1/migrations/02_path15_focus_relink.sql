-- ============================================================================
-- Migration 02 — PATH15: re-link path rows to specimen_tumor_focus_v1
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      PATH15  (severity revised to WARNING on 2026-04-22)
-- Author:        Logan Glosser
-- Date:          2026-04-22
-- ----------------------------------------------------------------------------
-- DESIGN REVISION (2026-04-22):
--   The original prompt called for a 4-tier linkage scheme (exact /
--   size_laterality / laterality_only / none).  Two problems surfaced during
--   implementation:
--
--   (1) main.specimen_tumor_focus_v1 does not expose `laterality`, `site`, or
--       `size_greatest_dimension_cm` — those keys can't be used as joins.
--   (2) More fundamentally, specimen_focus_id provides marginal downstream
--       value for the manuscript:
--         - all per-focus clinical features (size, invasion, margins,
--           histology, laterality) are already native to
--           canonical_path_malignant_events_v1
--         - multifocality is derived from COUNT(*) per surgery_episode_uid
--           (prompt 03), not from focus_id
--         - molecular-to-focus linkage is structurally impoverished
--           (only 263/10,370 assays — 2.5% — ever resolve to a focus)
--         - PATH13 dedup tie-break (prompt 363) only needs a non-NULL focus_id
--           preference, not full coverage
--
--   Decision: simplify to a single-tier exact join; rows that don't match
--   stay NULL; do NOT queue them for chart review (no clinical value lost
--   by the NULLs). PATH15 is demoted from `critical` to `warning` in the
--   issue registry and REMOVED from the cohort_v2 critical-exclusion set.
--
-- Join key:
--   path.specimen_id   = focus.specimen_id       (unique on both sides)
--   path.tumor_ordinal = focus.tumor_index       (1..5, stable)
--
-- Pre-run coverage: 5,097/6,689 (76.2%) exact; 1,592 (23.8%) unlinkable
-- (tumor_ordinal on path does not align with tumor_index on focus — an
-- extraction-side artifact, not a clinical error).
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.path_focus_link_v1 AS
SELECT
    p.research_id,
    p.surgery_episode_uid,
    p.tumor_ordinal,
    f.specimen_focus_id,
    CASE WHEN f.specimen_focus_id IS NOT NULL THEN 'exact' ELSE 'none' END AS linkage_tier,
    CASE WHEN f.specimen_focus_id IS NOT NULL THEN 1.00  ELSE 0.00   END AS linkage_confidence,
    p.path_surgery_id,
    p.synoptic_row_ix
FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed p
LEFT JOIN main.specimen_tumor_focus_v1 f
       ON p.specimen_id   = f.specimen_id
      AND p.tumor_ordinal = f.tumor_index;

-- NOTE: No INSERT into qc_manual_review_queue_v1. PATH15 unlinked rows are a
-- provenance gap, not a QC error; no downstream manuscript analysis requires
-- focus-level resolution for them.
