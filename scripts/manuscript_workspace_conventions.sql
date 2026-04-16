-- ============================================================================
-- manuscript_workspace.__conventions — engineering conventions reference
-- Date:   2026-04-16
-- Author: THYROID_2026 canonical-finalization run (Group B + C learnings)
--
-- Drafted by the project lead from pre-flight + mid-run discoveries during
-- Scripts 237-246. Committed before Script 247 (lock) so the lock's final
-- assertion block can verify this table exists with >= 5 rows.
--
-- Shape mirrors manuscript_workspace.__readme: small reference table,
-- one row per convention. Each row documents a stability-interface rule
-- that downstream consumers can rely on. Do NOT modify retroactively —
-- add new rows for new conventions; mark old ones with deprecated flags
-- (via a future schema extension) if they need to be retired.
-- ============================================================================

CREATE OR REPLACE TABLE manuscript_workspace.__conventions AS
SELECT * FROM (VALUES
  (
    'as_aliasing',
    'column_naming',
    'When a canonical table column is derived from a source-table column with a different name, ALWAYS use AS aliasing in the assembly SQL with the canonical name (not the source name). The alias IS the contract; downstream consumers join on canonical names. Source-name leakage breaks the abstraction.',
    'Script 237 (canonical_patient_master assembly) — every CTE-to-master mapping uses explicit AS even when names match, to make the contract visible at the SELECT level.',
    'Group B'
  ),
  (
    'cohort_scoping',
    'table_design',
    'canonical_*_characteristics_v1 tables cover only patients for whom the characterization is meaningful. They are NOT cohort-shape-aligned with canonical_patient_master. Use CPM for cohort-wide queries (10,871 pts); use canonical detail tables for per-(tumor / nodule / event) queries on patients who have data of that grain. The set difference between CPM and a detail table''s patient set is documented per detail-table in detail_table_registry_v1.description (e.g., "2,449 benign tumor-free pts intentionally absent" for canonical_tumor_characteristics_v1).',
    'Script 245 (canonical_tumor_characteristics_v1, 8,422 of 10,871 pts) — 2,449 benign post-2015 patients intentionally absent. Script 246 (canonical_us_nodule_characteristics_v1, 6,126 of 10,871 pts) — 4,745 patients absent due to no nodule-level imaging data.',
    'Group C'
  ),
  (
    'rid_type_consistency',
    'join_safety',
    'research_id type is NOT uniform across source tables. canonical_patient_master.research_id = VARCHAR. Most imaging tables (imaging_nodule_master_v1, us_nodules_tirads, etc.) = INTEGER/BIGINT. ALL joins between CPM and source tables MUST cast: CAST(source.research_id AS VARCHAR) = cpm.research_id. Python set-comparisons across tables of different RID types will silently report 0% overlap when the actual overlap is 100%. Use SQL JOINs with explicit CAST, never Python set intersection on raw columns.',
    'Script 246 pre-flight — initial Python set comparison reported 0% CPM-overlap for canonical_us_nodule_characteristics_v1 candidates. Was a VARCHAR vs BIGINT artifact; explicit CAST showed 100% alignment. Caught before raising as architectural blocker.',
    'Group C pre-flight'
  ),
  (
    'catalog_vs_queryable_drift',
    'cleanup_safety',
    'After DROP TABLE in MotherDuck, the table may persist in information_schema.tables and SHOW ALL TABLES output for some time even though SELECT against it raises Catalog Error. Empty-table sweeps that rely on information_schema row counts are not sufficient — they must also probe with a guarded SELECT to confirm the table is actually queryable. A "ghost" table in information_schema is not a Phase 7 invariant failure on its own, but DOES need to be filtered out of the registry and __readme, both of which should rely on queryable-only enumeration.',
    'Script 246 dropped imaging_nodule_long_v2 from main; storage layer dropped successfully but information_schema still reports it as a BASE TABLE in main. SELECT raises "Table does not exist". Script 247 must handle this case in its sweep and registry/__readme regeneration logic.',
    'Discovered post-Group-C, pre-Script-247'
  ),
  (
    'pre_flight_decision_log',
    'audit_trail',
    'Every canonical-table-building script (237+) MUST emit a scripts/output/{NNN}_decision_log.json containing an entry for each pre-flight discovery that diverged from the prompt''s original assertions. Each entry: {category, original_assertion, revised_assertion, rationale, revised_value_expected}. This is the manuscript-defensibility trail — when a reviewer asks "why is N=8,422 not 10,871?", the answer is in the decision log, not buried in commit history.',
    'Scripts 245 (8 entries) and 246 (6 entries). 245_tem_only_patients.json supplements with the 2,449 patient list for traceability.',
    'Group C established as standard'
  )
) AS t(convention_id, category, rule, exemplar, established_in);

COMMENT ON TABLE manuscript_workspace.__conventions IS
  'Engineering conventions for thyroid_canonical_publication_v1_0. Each row is a stability-interface rule that downstream consumers can rely on. Updated as new conventions emerge from script-execution learnings. Do not modify retroactively — add new rows for new conventions, mark old ones with a deprecated_at column if needed.';

-- LOG: assertions
-- ASSERT: table created with >= 5 rows
SELECT COUNT(*) >= 5 AS ok FROM manuscript_workspace.__conventions;

-- ASSERT: every row has all required columns populated
SELECT
  COUNT(*) = 0 AS ok FROM manuscript_workspace.__conventions
WHERE convention_id IS NULL OR category IS NULL OR rule IS NULL
   OR exemplar IS NULL OR established_in IS NULL;

-- ASSERT: convention_id values are unique
SELECT COUNT(*) = COUNT(DISTINCT convention_id) AS ok
FROM manuscript_workspace.__conventions;

-- ASSERT: table COMMENT is set
SELECT comment IS NOT NULL AND LENGTH(comment) > 0 AS ok
FROM duckdb_tables()
WHERE database_name='thyroid_canonical_publication_v1_0'
  AND schema_name='manuscript_workspace' AND table_name='__conventions';
