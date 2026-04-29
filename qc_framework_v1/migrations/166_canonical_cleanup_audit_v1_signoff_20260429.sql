-- =============================================================================
-- Migration 166 — manuscript_workspace.canonical_cleanup_audit_v1 sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   54 / mig_166
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig166_canonical_cleanup_audit_signoff_20260429.md
-- batch_id: mig_166_canonical_cleanup_audit_v1_signoff_20260429
--
-- Scope:  Registry-only writes ON MotherDuck RW `thyroid_canonical_publication_v1_0`.
-- Path C / Cowork executes APPLY after review — **do not RW from agent.**
--
-- §2 Pre-flight (MotherDuck `thyroid_canonical_publication_v1_0`, probe 2026-04-29):
--   * Physical table: `manuscript_workspace.canonical_cleanup_audit_v1` — **18** cols,
--     **120** rows (118 `main` inventory objects + 2 `manuscript_workspace` audit rows;
--     classifier refresh per `canonical_cleanup_resume_20260417` / `v2_signal_based_20260417`).
--   * **No** `research_id` column — object-grain governance inventory (not CPM spine).
--
-- §2e BOOLEAN cohort sweep (`canonical_cleanup_audit_v1`):
--   * `is_referenced_by_view`: TRUE=**6**, FALSE=**114**, NULL=**0**
--   * `is_referenced_by_script`: TRUE=**120**, FALSE=**0**, NULL=**0**
--     → **CF-mig166-COHORT-NEAR-UNIFORM-TRUE-is_referenced_by_script** (presence flag;
--       artifact of “referenced by ≥1 script” inventory rule — informational).
--   * `is_identical_to_twin`: TRUE=**0**, FALSE=**0**, NULL=**120** (no twin-equality rows
--       materialized in current classifier output).
--   * `has_version_twin`: TRUE=**0**, FALSE=**120**, NULL=**0**
--     → **CF-mig166-COHORT-NEAR-UNIFORM-FALSE-has_version_twin** (Type-B; registry v2
--       classifier finds **0** version twins — verified as faithful, not discarded).
--
-- §2f / §2g DATE / VARCHAR-with-units:
--   * `last_modified_in_db` TIMESTAMP (partial fill **26**/120 non-null per date-normalization
--     pass — remainder legitimately null).
--   * `classified_at` TIMESTAMP WITH TIME ZONE (120/120 non-null post classifier run).
--   * No `*_date` **DATE** columns; no VARCHAR numeric-with-units columns.
--
-- §3 Methodology vs mig_165:
--   * After **mig_165** mass `auto_governance_audit_table_skip`, all **18** cols were `na`
--     and **table_status** was already **`verified`** (ledger-complete via all-`na`).
--   * **mig_166** elevates **15** audit-signal / classifier-output cols to **`verified`**
--     (derivation vs scripts **272** / **266c** classifier inventory + **275** phase-5
--     inventory lineage). **3** cols remain **`na`**: identifier-without-cohort-test
--     (`n_distinct_research_id`, registry **`auto_identifier_skip`**) + audit timestamps
--     (`last_modified_in_db`, `classified_at`, **`auto_provenance_skip`**).
--
-- Post-apply targets:
--   * Column ledger: **15** `verified`, **3** `na`, **0** `not_started`.
--   * `canonical_table_signoff_registry_v1`: remains **`verified`**; **`signoff_migration`**
--     advances to this file; column counts resynced from live registry.
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshot (archive DB — column registry slice)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig166_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig166_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'canonical_cleanup_audit_v1';

-- -----------------------------------------------------------------------------
-- Section B — (header comments above — §2e boolean / dtype audits)
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Section C — Elevate audit-signal columns → verified
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit',
    batch_id            = 'mig_166_canonical_cleanup_audit_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_166 Lane 54: governance inventory col verified vs '
                          || 'Script 272/266c classifier v2 + 275 phase-5 inventory; '
                          || 'CF-mig166-SCRIPT-ALL-TRUE on is_referenced_by_script (see header).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'canonical_cleanup_audit_v1'
  AND verification_status IN ('not_started', 'na')
  AND column_name IN (
    'object_name',
    'object_type',
    'status',
    'destination',
    'reason',
    'row_count',
    'is_referenced_by_view',
    'is_referenced_by_script',
    'is_identical_to_twin',
    'n_view_refs',
    'n_script_refs',
    'has_version_twin',
    'twin_name',
    'classifier_version',
    'notes'
  );

-- -----------------------------------------------------------------------------
-- Section D — Re-affirm na disposition (identifier + audit timestamps)
--    Idempotent: keeps status=na; stamps mig_166 provenance where still na.
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verified_by         = 'logan',
    verification_method = CASE column_name
                            WHEN 'n_distinct_research_id' THEN 'auto_identifier_skip'
                            ELSE 'auto_provenance_skip'
                          END,
    batch_id            = 'mig_166_canonical_cleanup_audit_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_166 Lane 54: remains na — '
                          || CASE column_name
                               WHEN 'n_distinct_research_id' THEN
                                 'object-grain inventory; BIGINT optional per inventoried object (114/120 non-null).'
                               WHEN 'last_modified_in_db' THEN
                                 'catalog TIMESTAMP provenance; partial coverage expected.'
                               WHEN 'classified_at' THEN
                                 'classifier run TIMESTAMP WITH TIME ZONE; calendar semantics under CF-100-DATE-RETYPE umbrella.'
                             END
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'canonical_cleanup_audit_v1'
  AND verification_status = 'na'
  AND column_name IN (
    'n_distinct_research_id',
    'last_modified_in_db',
    'classified_at'
  );

-- -----------------------------------------------------------------------------
-- Section E — Resync `canonical_table_signoff_registry_v1` (mig_159 §159g pattern)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/166_canonical_cleanup_audit_v1_signoff_20260429.sql',
    notes           = COALESCE(ts.notes, '')
                      || ' | mig_166: canonical_cleanup_audit_v1 — 15 verified + 3 na '
                      || '(identifier + audit timestamps); column ledger refined after mig_165 mass-na.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'manuscript_workspace'
    AND table_name = 'canonical_cleanup_audit_v1'
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 166 — canonical_cleanup_audit_v1 column sign-off (18 cols)
-- =============================================================================
