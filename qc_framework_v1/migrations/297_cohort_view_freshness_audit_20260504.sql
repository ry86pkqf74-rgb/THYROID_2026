-- mig_297 — manuscript_workspace cohort_* freshness audit + repoint (2026-05-04)
-- Context: across mig_280 (BinderException fix), mig_283 (12 views fixed),
-- mig_285 (M032 NLP augment), mig_286 (M037 NLP augment), several cohort views
-- have aged out of step with canonical SSOTs. This pass re-probes every
-- manuscript_workspace.cohort_* view and repoints stale references to:
--   * canonical_recurrence_v1            -> canonical_recurrence_patient_rollup_v1   (mig_269 SSOT)
--   * canonical_recurrence_resolved_v1   -> canonical_recurrence_patient_rollup_v1
--   * recurrence_event_clean_v1          -> canonical_recurrence_patient_rollup_v1
--   * nlp_tirads_max_category (col)      -> tirads_resolved (col, mig_288/mig_294b)  [manual]
--
-- Target DB: thyroid_canonical_publication_v1_0 (USE before running unqualified DDL).
--
-- Apply path: `.venv/bin/python scripts/mig_297_cohort_view_freshness_audit.py`
--   (writes pre-snapshots, applies CREATE OR REPLACE for table-level safe swaps,
--    surfaces column-level deprecations + compile/drift failures as
--    needs_manual_repoint, and inserts main.signoff_migration row).
--
-- Closes: CF-mig283-COHORT-FRESHNESS.

-- -----------------------------------------------------------------------------
-- §1 Inventory probe (run-only, executed by the python apply script)
-- -----------------------------------------------------------------------------

-- Per-view freshness flags (table_name, deprecated-ref booleans, def length).
WITH v AS (
  SELECT view_name, sql AS view_definition
  FROM duckdb_views()
  WHERE database_name = 'thyroid_canonical_publication_v1_0'
    AND schema_name   = 'manuscript_workspace'
    AND view_name ILIKE 'cohort_%'
)
SELECT
  view_name,
  view_definition ILIKE '%canonical_recurrence_v1%'          AS uses_legacy_recur_v1,
  view_definition ILIKE '%canonical_recurrence_resolved_v1%' AS uses_legacy_recur_resolved,
  view_definition ILIKE '%recurrence_event_clean_v1%'        AS uses_legacy_recur_clean,
  view_definition ILIKE '%nlp_tirads_max_category%'          AS uses_dirty_tirads,
  LENGTH(view_definition) AS def_len
FROM v
ORDER BY view_name;

-- -----------------------------------------------------------------------------
-- §2 Pre-snapshot pattern (executed by the python apply script per stale view)
-- -----------------------------------------------------------------------------

-- For each cohort_* view that is auto-repointed, the apply script writes:
--   CREATE OR REPLACE TABLE
--     "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_<view>_pre_mig_297_20260504 AS
--   SELECT database_name AS view_catalog,
--          schema_name   AS view_schema,
--          view_name,
--          sql           AS view_definition,
--          CURRENT_TIMESTAMP AS snapshot_at
--   FROM duckdb_views()
--   WHERE database_name = 'thyroid_canonical_publication_v1_0'
--     AND schema_name   = 'manuscript_workspace'
--     AND view_name     = '<view>';

-- -----------------------------------------------------------------------------
-- §3 Repoint pattern (executed by the python apply script per stale view)
-- -----------------------------------------------------------------------------

-- Whole-word table-name substitution applied to the archived view DDL,
-- followed by CREATE OR REPLACE VIEW <view> AS <rewritten DDL>.
-- A row-count drift guard reverts the change (re-applying the original DDL
-- from the snapshot) if the post-rewrite count differs from the pre-rewrite
-- count by more than 1%. Compile failures also trigger revert and surface
-- the view in the disposition table as needs_manual_repoint.

-- -----------------------------------------------------------------------------
-- §4 Registry signoff (issued by the apply script)
-- -----------------------------------------------------------------------------

-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
-- VALUES (
--   'mig_297',
--   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
--   'cursor_agent_mig297',
--   'mig_297: Cohort view freshness audit. Probed N manuscript_workspace.cohort_* '
--   'views; auto-repointed K via whole-word swap of legacy recurrence tables to '
--   'canonical_recurrence_patient_rollup_v1; flagged M for manual repoint '
--   '(column-level nlp_tirads_max_category -> tirads_resolved or compile/drift '
--   'failure). Pre-snapshots in "Thyroid 2026 UPdated".archive_pub_v1_0 as '
--   'view_def_<view>_pre_mig_297_20260504. Disposition: '
--   'scripts/output/mig_297_disposition.md. Closes CF-mig283-COHORT-FRESHNESS.'
-- );
