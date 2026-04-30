-- =============================================================================
-- mig_197 — data dictionary extract (canonical verified tables × registry)
-- =============================================================================
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Batch: mig_197_data_dictionary_refresh_with_cf_annotations_20260430
-- Target: thyroid_canonical_publication_v1_0.main
-- Posture: SELECT-only — run via Path-C / MotherDuck read token; pipe to CSV.
--
-- Output columns mirror supplementary data-dictionary appendix:
--   schema_name, table_name, column_name, data_type, is_nullable,
--   verification_status, verification_method, batch_id, verified_ts,
--   cf_tags, notes_excerpt
--
-- CF tags are derived downstream (Python or ad hoc) via
-- regexp CF-[A-Za-z0-9_-]+ against registry notes; DuckDB portability varies.
-- Companion script: scripts/render_mig197_data_dictionary_readonly.py
-- =============================================================================

USE "thyroid_canonical_publication_v1_0";
USE "thyroid_canonical_publication_v1_0".main;

SELECT
  c.table_schema AS schema_name,
  c.table_name,
  c.column_name,
  c.data_type,
  c.is_nullable,
  r.verification_status,
  r.verification_method,
  r.batch_id,
  r.verified_ts,
  r.notes AS registry_notes
FROM information_schema.columns AS c
INNER JOIN main.canonical_column_verification_registry_v1 AS r
  ON r.schema_name = c.table_schema
 AND r.table_name = c.table_name
 AND r.column_name = c.column_name
INNER JOIN main.canonical_table_signoff_registry_v1 AS ts
  ON ts.schema_name = c.table_schema
 AND ts.table_name = c.table_name
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'main'
  AND ts.table_status = 'verified'
  AND r.verification_status IN ('verified', 'na')
ORDER BY c.table_name, c.ordinal_position;
