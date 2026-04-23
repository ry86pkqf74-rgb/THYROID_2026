-- ============================================================================
-- Migration 09 — Cleanup pass for prompts 01-08 (no new features, audit only)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: main (metadata only), manuscript_workspace, views_readable
-- Issue IDs:     (none — this is a deprecation-marking + repoint pass)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Rationale:
--   Prompts 01-08 landed 8 manuscript_workspace views that supersede parts of
--   main.*. Per design contract we never mutate main.*, but we also never
--   added anything that makes the superseded object *discoverable* as
--   deprecated. This migration does exactly that and nothing else:
--
--     (1) COMMENT ON TABLE/COLUMN for each superseded object, pointing at its
--         manuscript_workspace replacement. Visible in duckdb_tables()/
--         duckdb_columns() metadata and \d+ output; non-breaking.
--     (2) Re-point views_readable.Genetics_from_Notes_LLM to the clean
--         source (manuscript_workspace.molecular_mentions_from_notes_v2).
--     (3) Create manuscript_workspace.canonical_deprecation_log_v1 as a
--         single-table index of every deprecation across the QC framework,
--         so future Logan / future reviewers can see the debt in one place.
--     (4) Sanity-verify row counts are unchanged (no mutation occurred).
--
--   Scope: prompts 01-08 only. Subsequent prompts append to
--   canonical_deprecation_log_v1 as they close.
--
-- NOT DOING (deliberately):
--   * Dropping any main.* table/view — prompt 46 (cohort_v2 assembly) is the
--     gate for hard drops. Early drops break unrelated downstream scripts.
--   * Renaming main.* objects — DuckDB ALTER TABLE RENAME TO is catalog-only;
--     dependent view bodies keep the old name and fail on recompile. Comments
--     are zero-risk; renames are a coordinated-commit problem best deferred.
--   * Touching canonical_path_malignant_events_v1 semantics — the _keyed
--     view ADDS columns (global namespace keys), it does not obsolete the
--     base table. Light pointer comment only.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- (0) Clear probe comments left during COMMENT syntax verification
-- ---------------------------------------------------------------------------

COMMENT ON TABLE main.canonical_molecular_genetics_from_notes_v2 IS NULL;
COMMENT ON COLUMN main.canonical_molecular_genetics_v2.molecular_episode_id IS NULL;

-- ---------------------------------------------------------------------------
-- (1) COMMENT ON TABLE — deprecated tables
-- ---------------------------------------------------------------------------

COMMENT ON TABLE main.canonical_molecular_genetics_from_notes_v2 IS
'DEPRECATED 2026-04-23 (GEN10). This is an NLP *mentions* layer — entity
extractions from narrative notes (op notes, path reports, addenda). It is
NOT a structured assay result and MUST NOT be joined as a peer of
main.canonical_molecular_genetics_v2. Downstream reads should use
manuscript_workspace.molecular_mentions_from_notes_v2 (naming disambiguates).
Table retained for audit/provenance; no schedule for drop.';

COMMENT ON TABLE main.specimen_genomic_assay_v1 IS
'LINKAGE DEPRECATED 2026-04-23 (GEN09). Table is kept as-is for audit, but
its linkage columns (fna_episode_id, surgery_episode_id, molecular_episode_id)
are unreliable — ~98% of rows fail to resolve under the original schema.
Downstream joins should bind on research_id via
manuscript_workspace.specimen_genomic_assay_v1_relinked, which exposes two
boolean presence flags (rid_in_canonical_molecular, rid_in_specimen_master)
for cohort construction.';

-- ---------------------------------------------------------------------------
-- (2) COMMENT ON TABLE — pointer comments for tables that are NOT deprecated
--     but have a cleaner manuscript_workspace derivative
-- ---------------------------------------------------------------------------

COMMENT ON TABLE main.canonical_path_malignant_events_v1 IS
'SOURCE TABLE. A global re-key (adds cross-domain stable keys) is available at
manuscript_workspace.canonical_path_malignant_events_v1_keyed — prefer the
_keyed view for joins that need global namespace keys (PATH01/OP05).';

COMMENT ON TABLE main.manuscript_cohort_v1 IS
'SOURCE TABLE. Cleaned cohort view with normalized histology is available at
manuscript_workspace.manuscript_cohort_v1_histology_clean — adds three
columns (histology_final_clean, histology_metastatic_prefix_flag,
histology_variant_extracted). Final manuscript_cohort_v2 assembly (prompt 46)
will be the canonical read-path.';

-- ---------------------------------------------------------------------------
-- (3) COMMENT ON COLUMN — deprecated columns
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN main.canonical_molecular_genetics_v2.molecular_episode_id IS
'DEPRECATED 2026-04-23 (GEN01). Broken key — 3 distinct values across 1,384
rows, zero discriminative value. Use
manuscript_workspace.molecular_episode_uid_v1.molecular_episode_uid instead
(stable md5 over research_id | resolved_test_date | platform | report_text_ref;
1,383 UIDs / 1,384 rows / 1 residual byte-identical duplicate).';

COMMENT ON COLUMN main.manuscript_cohort_v1.histology_final IS
'RAW FIELD — not normalized. 59 distinct values with whitespace issues,
case inconsistency, typos, embedded newlines, and metastatic prefixes.
Use manuscript_workspace.manuscript_cohort_v1_histology_clean for analysis:
- histology_final_clean (controlled-vocab, 19 buckets)
- histology_metastatic_prefix_flag
- histology_variant_extracted
(HIST01/02/03 resolved 2026-04-23.)';

-- ---------------------------------------------------------------------------
-- (4) Re-point views_readable.Genetics_from_Notes_LLM → clean source
--     (Genetics_Testing and path_malignant_events_VIEW_v1 left alone —
--      their source columns are not deprecated.)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW views_readable.Genetics_from_Notes_LLM AS
SELECT * FROM manuscript_workspace.molecular_mentions_from_notes_v2;

-- ---------------------------------------------------------------------------
-- (5) Deprecation log — one row per deprecation decision
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS manuscript_workspace.canonical_deprecation_log_v1 (
    deprecated_object     VARCHAR NOT NULL,     -- fully qualified schema.name[.column]
    object_kind           VARCHAR NOT NULL,     -- 'table' | 'column' | 'view'
    superseding_object    VARCHAR NOT NULL,     -- fully qualified replacement
    issue_id              VARCHAR,              -- QC framework issue code
    closing_prompt        VARCHAR,              -- e.g. 'prompt_07'
    deprecation_kind      VARCHAR NOT NULL,     -- 'full' | 'linkage_only' | 'column_only' | 'pointer_only'
    deprecated_date       DATE NOT NULL,
    reason                VARCHAR NOT NULL,
    hard_drop_gate        VARCHAR,              -- which later prompt, if any, is allowed to DROP
    notes                 VARCHAR
);

-- Idempotent load (clear then re-insert so rerunning the migration stays clean)
DELETE FROM manuscript_workspace.canonical_deprecation_log_v1
WHERE closing_prompt IN ('prompt_01','prompt_05','prompt_06','prompt_07','prompt_08');

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.canonical_path_malignant_events_v1',
   'table',
   'manuscript_workspace.canonical_path_malignant_events_v1_keyed',
   'PATH01/OP05',
   'prompt_01',
   'pointer_only',
   DATE '2026-04-23',
   'Re-keyed view adds global-namespace cross-domain keys; base table is still the source of truth for columns not touched by re-keying.',
   NULL,
   'No schedule for drop. Base table is the write-path for upstream ingest.'),

  ('main.canonical_molecular_genetics_v2.molecular_episode_id',
   'column',
   'manuscript_workspace.molecular_episode_uid_v1.molecular_episode_uid',
   'GEN01',
   'prompt_05',
   'column_only',
   DATE '2026-04-23',
   'Column holds 3 distinct values across 1,384 rows (effectively useless as a key).',
   'prompt_46',
   'Replacement UID view has 1,383 UIDs/1,384 rows/1 byte-identical residual dup.'),

  ('main.specimen_genomic_assay_v1',
   'table',
   'manuscript_workspace.specimen_genomic_assay_v1_relinked',
   'GEN09',
   'prompt_06',
   'linkage_only',
   DATE '2026-04-23',
   'Table linkage columns unreliable (~98% broken). Replacement binds on research_id with two boolean presence flags for downstream cohort construction.',
   NULL,
   'Table retained for audit; relinked view is the join target.'),

  ('main.canonical_molecular_genetics_from_notes_v2',
   'table',
   'manuscript_workspace.molecular_mentions_from_notes_v2',
   'GEN10',
   'prompt_07',
   'full',
   DATE '2026-04-23',
   'Naming invites incorrect peer-joining with canonical_molecular_genetics_v2. This is an NLP mentions layer; the _mentions_ name makes that explicit.',
   'prompt_46',
   'views_readable.Genetics_from_Notes_LLM re-pointed to the replacement in this migration.'),

  ('main.manuscript_cohort_v1.histology_final',
   'column',
   'manuscript_workspace.manuscript_cohort_v1_histology_clean',
   'HIST01/HIST02/HIST03',
   'prompt_08',
   'column_only',
   DATE '2026-04-23',
   'Dirty raw field: 59 distinct values, whitespace/case/typo/embedded-newline/metastatic-prefix issues.',
   NULL,
   'Raw column retained for audit; use histology_final_clean + histology_metastatic_prefix_flag + histology_variant_extracted from the clean view.');

-- ---------------------------------------------------------------------------
-- (6) Sanity checks — row counts are unchanged
-- ---------------------------------------------------------------------------
-- Leave these as verification queries in README; migration itself performs
-- no mutations beyond metadata and the log table.
-- ============================================================================
