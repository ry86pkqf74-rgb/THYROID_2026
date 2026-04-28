-- =============================================================================
-- Migration 73 -- clean + rename pathology_diagnosis/extended in
--                 canonical_fna_events_v1 to mirror source workbook semantics
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Logan directive: "I want the table to be similar to my original
--         source table, except with one FNA per row rather than 1 patient
--         ID per row. Preop FNA#N Date / FNA#N Specimen Received /
--         FNA #N path extended / FNA#N History / Bethesda #N."
--
-- Pre-investigation:
--   1,450 pathology_diagnosis rows had content diverging from source
--   FNA Bethesda > History cell. 1,870 pathology_extended rows similarly
--   diverged from source FNA Bethesda > Path extended cell. The divergent
--   content did NOT come from note_entities_llm_pathology (op-notes / H&P)
--   or clinical_notes_long (no FNA cytopath notes). Provenance was unknown
--   -- likely older workbook version or deprecated extraction pipeline.
--
-- Action (per Logan):
--   "Unless FNA reports were found outside of the excel sheet attached
--   then they are useless." -> bulk-replace divergent content with current
--   source workbook cells. No carry-forward of mystery content.
--
--   Also rename the two columns so the FNA table self-documents:
--     pathology_diagnosis -> fna_history          (mirrors source 'History')
--     pathology_extended  -> fna_pathology_report (mirrors source 'Path extended')
--
-- Net effect:
--   * 1,450 rows updated: fna_history (formerly pathology_diagnosis) <- src.history_raw
--   * 1,870 rows updated: fna_pathology_report (formerly pathology_extended) <- src.path_raw
--   * 2 ALTER TABLE RENAME COLUMN
--   * 2 registry rows: column_name updated + verification_status='verified'
--     (verification_method=mechanical_source_compare, batch_id=
--     mig_73_fna_history_and_path_report)
--   * Post-state: 8,054/8,054 rows match source for both columns (100%).
--   * canonical_fna_events_v1 n_verified: 18 -> 20 of 39.
--
-- No dependent views referenced either column (verified pre-execution).
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

UPDATE main.canonical_fna_events_v1 db
SET pathology_diagnosis = src.history_raw
FROM manuscript_workspace.fna_source_long_v1_step_b src
WHERE db.research_id = src.research_id
  AND db.fna_index = src.fna_index
  AND COALESCE(TRIM(db.pathology_diagnosis), '') <> COALESCE(TRIM(src.history_raw), '');

UPDATE main.canonical_fna_events_v1 db
SET pathology_extended = src.path_raw
FROM manuscript_workspace.fna_source_long_v1_step_b src
WHERE db.research_id = src.research_id
  AND db.fna_index = src.fna_index
  AND COALESCE(TRIM(db.pathology_extended), '') <> COALESCE(TRIM(src.path_raw), '');

ALTER TABLE main.canonical_fna_events_v1
  RENAME COLUMN pathology_diagnosis TO fna_history;

ALTER TABLE main.canonical_fna_events_v1
  RENAME COLUMN pathology_extended TO fna_pathology_report;

UPDATE main.canonical_column_verification_registry_v1
SET column_name = 'fna_history',
    verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id = 'mig_73_fna_history_and_path_report',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_73: renamed pathology_diagnosis -> fna_history; '
            || 'bulk-replaced 1,450 divergent rows with source workbook History cell; '
            || 'now 100% match against FNAs 12_5_2025.xlsx > FNA Bethesda > History.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'pathology_diagnosis';

UPDATE main.canonical_column_verification_registry_v1
SET column_name = 'fna_pathology_report',
    verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'mechanical_source_compare',
    batch_id = 'mig_73_fna_history_and_path_report',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
            || ' | mig_73: renamed pathology_extended -> fna_pathology_report; '
            || 'bulk-replaced 1,870 divergent rows with source workbook Path extended cell; '
            || 'now 100% match against FNAs 12_5_2025.xlsx > FNA Bethesda > Path extended.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  AND column_name = 'pathology_extended';

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 73
-- =============================================================================
