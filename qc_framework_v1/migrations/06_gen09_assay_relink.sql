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
-- Final distribution (10,370 rows / 10,026 patients):
--   rid in both      : 1,175
--   rid in sm only   : 6,702   (specimen-linkable, no molecular test parsed)
--   rid in canon only:   313   (molecular-linkable, no specimen_master row)
--   rid in neither   : 2,180 rows / 2,177 patients
--
-- Gut-check outcome — no queue emitted.
--   * The 2,177 "neither" patients: 2,176/2,177 had surgery AND are in
--     canonical_path_benign_events_v1, 0/2,177 in path_malignant. All carry
--     platform='Other' with no date and no payload — they are empty
--     placeholder rows for benign-cohort patients who never had a molecular
--     test. Absence from canonical_molecular is correct. Absence from
--     specimen_master is a separate coverage gap, not a GEN09 defect.
--   * The 313 "molecular-only" rows / 243 patients: also benign-cohort
--     (0/243 in path_malignant, 243/243 in path_benign). All had surgery;
--     43/243 have notes-derived molecular extractions, so the molecular
--     source may be op-note (not FNA). specimen_master's missing rows
--     are the same benign-cohort coverage gap.
--   Queuing either group under GEN09 would be a category error — neither
--   represents a linkage failure that a row-level human review can fix.
--   The view itself IS the GEN09 resolution: downstream joins on
--   research_id, with the two boolean flags exposing the coverage gap
--   directly for cohort construction.
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

-- No queue emission. Gut-check established that the "unlinked" rows are
-- benign-cohort placeholders or benign-cohort specimen_master coverage
-- gaps, neither of which is a GEN09 defect. The view exposes the
-- research_id-level presence/absence directly; downstream consumers bind
-- on research_id and can filter with the two boolean flags.
