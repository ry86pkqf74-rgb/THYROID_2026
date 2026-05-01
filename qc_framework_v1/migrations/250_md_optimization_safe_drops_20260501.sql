-- =============================================================================
-- mig_250 — MD optimization: safe drop of pre-migration backups + scaffolds
-- Date:    2026-05-01
-- Lane:    mig_250 (Cowork-direct)
-- Author:  Cowork at HEAD f6b00a1, executed on Logan's "Confirm safe then drop"
--          greenlight 2026-05-01
-- =============================================================================
--
-- Scope:
--   Drops 30 objects identified as safe via dependency check 2026-05-01:
--   * Zero view references (verified via information_schema.views.view_definition)
--   * Not in canonical_table_signoff_registry_v1 with table_status='verified'
--   * Names match pre-migration / migration-scaffold convention
--
-- HELD BACK (NOT DROPPED — re-verified as load-bearing 2026-05-01):
--   * main.cupm_v2_canonical_backfill_v1
--     - Referenced by main.canonical_us_patient_master_VIEW_v2
--     - In signoff registry as verified
--   * manuscript_workspace.biochemical_concern_backfill_v1
--     - In signoff registry as verified governance artifact
--
-- Expected impact:
--   * ~30 objects removed (228 tables → 198, manuscript_workspace 94 → 86)
--   * Estimated 100-150K rows + several wide (1591-col) snapshots removed
--   * gate1=218 unchanged (none of these are in canonical_table_signoff_registry_v1)
--   * gates 2-5 = 0 unchanged
--   * Cohort parity (10871×3) unchanged
--
-- Rollback note:
--   These tables are pre-migration backups; they are reproducible by re-running
--   the source migrations (mig188 / script_387/389/396 / val_mig171b/180b/194).
--   Recovery cost = re-running those scripts, not a hard data loss.
--
-- =============================================================================

-- §1 — archive_pub_v1_0 schema (18 tables, frozen pre-mig backups)
DROP TABLE IF EXISTS archive_pub_v1_0.canonical_complications_patient_rollup_v1_legacy_20260422;
DROP TABLE IF EXISTS archive_pub_v1_0.canonical_us_exam_master_VIEW_v2_legacy_20260422_body;
DROP TABLE IF EXISTS archive_pub_v1_0.canonical_us_patient_master_VIEW_v2_legacy_20260422_body;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_ete_pre390_20260422;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_ete_pre392_20260422_234621;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre391_20260422_223618;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre_dtc_null_n_stage_group_fill_20260423_024412;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre_malignant_null_stage_group_closeout_20260423_034419;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre_manual_review_queue_sortout_20260423_041534;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre_pdtc_rid6275_stage_group_20260423_045808;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_pre_tn_primary_from_v2_fill_20260423_030702;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_stage_group_pre393_20260422_235819;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452;
DROP TABLE IF EXISTS archive_pub_v1_0.cpm_t_sync_pre395_20260423_001407;
DROP TABLE IF EXISTS archive_pub_v1_0.detail_table_registry_v1_pre389_1_20260422T212806Z;
DROP TABLE IF EXISTS archive_pub_v1_0.detail_table_registry_v1_pre_mig60_20260424;
DROP TABLE IF EXISTS archive_pub_v1_0.queue_pre_manual_review_queue_sortout_20260423_041534;
DROP TABLE IF EXISTS archive_pub_v1_0.queue_pre_pdtc_rid6275_stage_group_20260423_045808;

-- Drop the now-empty archive_pub_v1_0 schema itself
DROP SCHEMA IF EXISTS archive_pub_v1_0 CASCADE;

-- §2 — main.val_mig* migration validation scaffolds (3 tables)
DROP TABLE IF EXISTS main.val_mig171b_canonical_us_ln_build_v1;
DROP TABLE IF EXISTS main.val_mig180b_nlp_upstream_lineage_v1;
DROP TABLE IF EXISTS main.val_mig194_canonical_us_thyroid_gland_shell_only_v1;

-- §3 — manuscript_workspace pre-migration snapshots + script prestates (9 tables)
DROP TABLE IF EXISTS manuscript_workspace.mig188_pre_snapshot_path_malignant;
DROP TABLE IF EXISTS manuscript_workspace.mig188_pre_snapshot_patient_master;
DROP TABLE IF EXISTS manuscript_workspace.mig188_pre_snapshot_registry;
DROP TABLE IF EXISTS manuscript_workspace.script_387_prestate_v1;
DROP TABLE IF EXISTS manuscript_workspace.script_389_prestate_v1;
DROP TABLE IF EXISTS manuscript_workspace.script_396_prestate_v1;
DROP TABLE IF EXISTS manuscript_workspace.script_396_prestate_benign_v1;
DROP TABLE IF EXISTS manuscript_workspace.script_396_prestate_gland_v1;
DROP TABLE IF EXISTS manuscript_workspace.tsh_suppressed_backfill_v1;

-- §4 — Supplemental drops (mig_250b, added 2026-05-01 after empty-table second-pass scan)
-- Both empty (rows=0), no inbound view references, not in signoff registry.
DROP TABLE IF EXISTS manuscript_workspace.canonical_logan_review_log_v1;
DROP TABLE IF EXISTS manuscript_workspace.cr_crr_reconcile_candidates_20260429;

-- =============================================================================
-- Post-drop verification (run separately)
-- =============================================================================
-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
--   Expect gate1=218, gates 2-5=0, cohort_parity TRUE, 10871×3
--
-- SELECT table_schema, COUNT(*) FILTER (WHERE table_type='BASE TABLE') AS n_tables,
--                       COUNT(*) FILTER (WHERE table_type='VIEW') AS n_views
-- FROM information_schema.tables
-- WHERE table_catalog='thyroid_canonical_publication_v1_0'
-- GROUP BY table_schema ORDER BY n_tables DESC;
--   Expect: archive_pub_v1_0 absent, main 110 (was 113), manuscript_workspace 85 (was 94)
-- =============================================================================
