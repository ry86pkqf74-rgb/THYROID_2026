-- mig_302 — Repoint views_readable.Patient_Master_Canonical (drop stale nlp_tirads_max_category projection)
--
-- Context: mig_294b dropped main.canonical_patient_master.nlp_tirads_max_category.
-- Tireads SSOT on CPM: tirads_resolved (mig_288). Friendly read-layer view may still
-- bind the removed column unless refreshed.
--
-- Target DB: thyroid_canonical_publication_v1_0 (USE main — connect_locked).
--
-- Prefer apply script (pre-snapshot + guard + verify + log):
--   .venv/bin/python scripts/mig_302_views_readable_patient_master_tirads.py
--   .venv/bin/python scripts/mig_302_views_readable_patient_master_tirads.py --dry-run
--
-- Skip: readonly_share (separate DB).
--
-- Closes: mig_294b downstream consumer audit (views_readable).

-- -----------------------------------------------------------------------------
-- §1 Snapshot prior view DDL (also executed by Python into archive_pub_v1_0)
-- -----------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_patient_master_canonical_pre_mig302_20260504 AS
-- SELECT
--   CURRENT_TIMESTAMP AS snapshot_at,
--   database_name,
--   schema_name,
--   view_name,
--   sql AS view_definition
-- FROM duckdb_views()
-- WHERE database_name = 'thyroid_canonical_publication_v1_0'
--   AND schema_name = 'views_readable'
--   AND view_name = 'Patient_Master_Canonical';

-- -----------------------------------------------------------------------------
-- §2 Mirror live CPM (includes tirads_resolved)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW views_readable.Patient_Master_Canonical AS
SELECT *
FROM main.canonical_patient_master;

-- -----------------------------------------------------------------------------
-- §3 Registry signoff (after verify — script inserts; or manual once)
-- -----------------------------------------------------------------------------
-- INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
-- ('mig_302', CURRENT_TIMESTAMP, 'cursor_composer_mig302',
--  'mig_302: Patient_Master_Canonical repointed SELECT * FROM main.canonical_patient_master; '
--  'snapshot view_def_patient_master_canonical_pre_mig302_20260504 on archive_pub_v1_0.');

