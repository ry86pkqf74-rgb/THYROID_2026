-- =============================================================================
-- Migration 127 -- Audit query refinement: exclude na cols from gate 5
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session — post mig_124 molecular_from_notes)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Refine the canonical 5-gate audit query so gate-5 (date violations on
--         verified tables) does NOT flag columns that are correctly tagged
--         `verification_status='na'` in the column registry.
--
-- Background:
--   The mig_117 audit template + mig_117 addendum (regex word-boundary
--   refinement) gave us a stable 5-gate audit. After Cursor 16 closed
--   canonical_molecular_genetics_from_notes_v2 (mig_124), gate-5 jumped from
--   22 to 23. The new violator was canonical_molecular_genetics_from_notes_v2
--   .note_date — a VARCHAR col that matches the date regex but is correctly
--   tagged `na_provenance` in canonical_column_verification_registry_v1
--   (auto_provenance_skip).
--
--   The audit query currently filters on column-name regex + data type but does
--   not consult the verification_status. So `na` cols that match the regex
--   become false-positive gate-5 hits. This is the second time the audit has
--   needed refinement (first: mig_117 addendum word-boundary regex on
--   `updated_tirads_category`).
--
-- Refinement: extend gate-5 query with a LEFT JOIN to
--   canonical_column_verification_registry_v1 and exclude rows where
--   `r.verification_status='na'`. Cols that aren't in the registry at all
--   (NULL after LEFT JOIN) are still flagged — only explicit na exclusions
--   pass through.
--
-- Effect:
--   * Three na_provenance cols drop from gate 5 (all `note_date` on verified
--     tables, all `auto_provenance_skip`):
--       - canonical_molecular_genetics_from_notes_v2.note_date
--       - canonical_pathology_clinical_events_v1.note_date
--       - canonical_cervical_ln_clinical_events_v1.note_date
--     The latter two had CF-117-DATE-RETYPE notes appended by mig_117 (when
--     they were treated as clinical dates), but the project's resolved view
--     is na_provenance (they identify which note the data came from, i.e.
--     provenance, not clinical event timing). The mig_117 notes remain as
--     historical breadcrumbs but are superseded by the na tagging.
--   * All remaining gate-5 violators (CF-100/117/119/120/mig122 family) stay
--     flagged because they're tagged `verified` (verify-with-note pattern).
--   * Confirmed gate 5 result post-refinement: 20 (was 23).
--
-- New canonical audit query (date-violation gate 5):
--
--   WITH verified_tables AS (
--     SELECT table_name FROM main.canonical_table_signoff_registry_v1
--     WHERE table_status='verified' AND table_name LIKE 'canonical_%'
--   ),
--   audit_allowlist AS (
--     SELECT col_name FROM (VALUES
--       ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),
--       ('llm_extracted_at'),('verified_ts'),('signed_off_ts'),
--       ('registered_ts'),('updated_at'),('created_at'),('promoted_at'),
--       ('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
--       ('ingestion_date'),('lab_datetime')
--     ) v(col_name)
--   )
--   SELECT COUNT(*) AS gate5_date_violations
--   FROM information_schema.columns c
--   JOIN verified_tables v ON c.table_name = v.table_name
--   LEFT JOIN main.canonical_column_verification_registry_v1 r
--     ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
--   WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
--     AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
--     AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
--     AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
--     AND COALESCE(r.verification_status,'unknown') != 'na'
--     AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
--          OR (c.data_type='VARCHAR' AND (
--                regexp_matches(c.column_name, '(^|_)dates?(_|$)')
--             OR regexp_matches(c.column_name, '(^|_)dt(_|$)')
--          )));
--
-- This becomes the new canonical audit query template (replaces mig_117/109).
--
-- This is a documentation-only update to the canonical audit template; no
-- registry data changes. The next audit run will use this query.
-- =============================================================================

-- 127a — append explanatory note to molecular_from_notes.note_date so future
-- reviewers see why this col is na (and why it intentionally appears in the
-- raw information_schema scan but not in refined gate 5)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_127: VARCHAR note_date is na_provenance (auto skip); '
            || 'matches gate-5 date regex but is filtered out by the refined '
            || 'audit query (verification_status=''na'' exclusion). Not a CF.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_molecular_genetics_from_notes_v2'
  AND column_name = 'note_date';

-- =============================================================================
-- end of migration 127 -- audit query refinement (na filter)
-- Confirmed post-refinement gate 5: 20 (was 23) — 3 na_provenance note_date
-- cols filtered. All future audit runs SHOULD use the query template embedded
-- in the comment block above.
-- =============================================================================
