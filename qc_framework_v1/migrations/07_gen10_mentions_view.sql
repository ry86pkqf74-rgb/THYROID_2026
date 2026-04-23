-- ============================================================================
-- Migration 07 — GEN10: expose notes-derived molecular as a mentions layer
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN10
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Rationale:
--   `main.canonical_molecular_genetics_from_notes_v2` is an NLP
--   entity-extraction layer, not a structured genomic assay result. Its name
--   invites accidental peer-joining with `canonical_molecular_genetics_v2`.
--   Per prompt 07, expose it under a name that explicitly signals "mentions":
--   `manuscript_workspace.molecular_mentions_from_notes_v2`.
--
-- Pre-state: 1,738 rows / 605 patients / 28 columns.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.molecular_mentions_from_notes_v2 AS
SELECT * FROM main.canonical_molecular_genetics_from_notes_v2;

-- No queue emission — rename-style view with no row-level defects to triage.
-- Deprecation note added to qc_framework_v1/README.md stating the source
-- table must not be joined as a peer of canonical_molecular_genetics_v2.
