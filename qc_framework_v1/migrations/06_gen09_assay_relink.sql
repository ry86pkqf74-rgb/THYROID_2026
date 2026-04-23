-- ============================================================================
-- Migration 06 — GEN09: re-link specimen_genomic_assay_v1 (research_id-only)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN09
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.specimen_genomic_assay_v1: 10,370 rows / 10,026 patients.
--
-- Per Logan 2026-04-23: do not over-engineer. All linkage collapses to
-- research_id — no tiering on date / platform / surgery / specimen_id.
-- The view just reports, for every assay row, whether its research_id is
-- present in canonical_molecular_genetics_v2 and/or specimen_master_v1.
-- Downstream joins bind on research_id.
--
-- Final distribution:
--   rid in both      : 1,175
--   rid in sm only   : 6,702  (specimen-linkable, no molecular test)
--   rid in canon only:   313  (molecular-linkable, no specimen row)
--   rid in neither   : 2,180 rows / 2,177 patients  → queue as orphans
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.specimen_genomic_assay_v1_relinked AS
SELECT
    a.genomic_assay_id,
    a.research_id,
    a.molecular_episode_id AS legacy_molecular_episode_id,
    a.platform,
    a.test_date_native,
    a.fna_episode_id,
    a.surgery_episode_id,
    a.specimen_id      AS specimen_id_native,
    a.payload_field,
    a.source_table,
    EXISTS (SELECT 1 FROM main.canonical_molecular_genetics_v2 c
            WHERE c.research_id = a.research_id)
        AS rid_in_canonical_molecular,
    EXISTS (SELECT 1 FROM main.specimen_master_v1 s
            WHERE s.research_id = a.research_id)
        AS rid_in_specimen_master
FROM main.specimen_genomic_assay_v1 a;

-- ---------------------------------------------------------------------------
-- QC queue emission (idempotent) — one row per orphan research_id
-- (rid absent from both molecular and specimen sources).
-- ---------------------------------------------------------------------------
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'GEN09' AS issue_id,
    research_id,
    'specimen_genomic_assay_v1' AS source_table,
    CAST(research_id AS VARCHAR) AS source_pk,
    TO_JSON(struct_pack(
        n_assay_rows := COUNT(*),
        distinct_platforms := COUNT(DISTINCT platform)
    )) AS context_json,
    'research_id absent from both canonical_molecular_genetics_v2 and specimen_master_v1' AS reason
FROM manuscript_workspace.specimen_genomic_assay_v1_relinked r
WHERE rid_in_canonical_molecular = FALSE
  AND rid_in_specimen_master = FALSE
  AND NOT EXISTS (
      SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
      WHERE q.issue_id = 'GEN09'
        AND q.source_table = 'specimen_genomic_assay_v1'
        AND q.source_pk = CAST(r.research_id AS VARCHAR)
  )
GROUP BY research_id;
