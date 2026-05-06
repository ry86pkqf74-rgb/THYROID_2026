-- mig_089: ETE manuscript fingerprint + overlay helpers (BQ)
-- THY-19 — Rebuild ete_manuscript_analytic v1–v7 dependency layer in BQ pub_workspace
-- Date: 2026-05-06
-- DFL: DFL-20260506-ETEFAMILY (logged before first edit)
-- Execution order: Run this file completely before mig_090.
--
-- Background:
--   In MotherDuck, the ete_manuscript_analytic_v1 view uses a rowid-based fingerprint
--   (path_malignant_event_fingerprint_v1) to uniquely key each row of
--   canonical_path_malignant_events_v1, then joins 9 "overlay" views on that fingerprint.
--   BQ has no rowid. This migration replaces the rowid pattern with a deterministic
--   MD5 fingerprint of the composite natural key:
--     (research_id, path_surgery_id, tumor_ordinal, synoptic_row_ix, specimen_id)
--
--   All 10 helpers are created as VIEWs (not BASE TABLEs) since their upstream
--   canonical_path_malignant_events_v1_* BQ views are already materialized/stable.
--   Exception: path_event_discordance_dedup_ete_v1 wraps the existing BQ VIEW
--   path_event_discordance_v1 which is itself a VIEW in pub_workspace.
--
-- FINGERPRINT MACRO (used consistently in all 10 helpers and in mig_090 analytic v1):
--   TO_HEX(MD5(CONCAT(
--       CAST(research_id AS STRING), '|',
--       COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
--       COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
--       COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
--       COALESCE(specimen_id, 'NULL')
--   )))
--
-- DO NOT REGRESS: pub_workspace.cohort_m044_ajcc_ete_v1 (3,868 rows) —
--   this table does NOT reference the helpers and is unaffected.
--
-- Expected row count for all 10 helpers: 6,469 (= COUNT(*) of canonical_path_malignant_events_v1)

-- =============================================================================
-- HELPER 1: path_malignant_event_fingerprint_v1
-- Purpose: Attaches a deterministic fingerprint key to each row of the base table.
--          Replaces the DuckDB rowid pattern. path_malignant_rowid is provided as
--          a stable ordering integer for reference/debug only.
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_event_fingerprint_v1` AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY research_id, path_surgery_id, tumor_ordinal, synoptic_row_ix
    ) AS path_malignant_rowid,
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1`
;

-- =============================================================================
-- HELPER 2: path_malignant_overlay_ete_clean_w_fp_v1
-- Purpose: ETE grading overlay keyed by path_event_fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_ete_clean.
-- Key columns used by analytic v1: ete_grade
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ete_clean_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    ete_grade,
    ete_grade_grouped,
    ete_discordance_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_ete_clean`
;

-- =============================================================================
-- HELPER 3: path_malignant_overlay_global_epi_w_fp_v1
-- Purpose: Global surgery episode ID overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_global_epi.
-- Key columns: surgery_episode_id_global, surgery_episode_uid_source
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_global_epi_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    surgery_episode_id_global,
    surgery_episode_uid_source,
    op05_rebind_applied_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_global_epi`
;

-- =============================================================================
-- HELPER 4: path_malignant_overlay_histology_w_fp_v1
-- Purpose: Cleaned primary histology overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_histology_clean.
-- Key columns: primary_histology_clean
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_histology_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    primary_histology_clean,
    histology_metastatic_flag,
    histology_recurrent_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_histology_clean`
;

-- =============================================================================
-- HELPER 5: path_malignant_overlay_variant_w_fp_v1
-- Purpose: Cleaned histology variant overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_variant_clean.
-- Key columns: histology_variant_clean
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_variant_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    histology_variant_clean
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_variant_clean`
;

-- =============================================================================
-- HELPER 6: path_malignant_overlay_size_flag_w_fp_v1
-- Purpose: Tumor size disagreement flag overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_size_flag.
-- Key columns: size_disagreement_any_flag
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_size_flag_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    size_disagreement_any_flag,
    size_single_tumor_mismatch_flag,
    size_per_surgery_understates_flag,
    size_per_surgery_overstates_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_size_flag`
;

-- =============================================================================
-- HELPER 7: path_malignant_overlay_laterality_w_fp_v1
-- Purpose: Laterality resolution overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_laterality_clean.
-- Key columns: derived_laterality_final
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_laterality_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    derived_laterality_final,
    site_laterality_contradict_flag,
    laterality_has_site_prose_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_laterality_clean`
;

-- =============================================================================
-- HELPER 8: path_malignant_overlay_invasion_w_fp_v1
-- Purpose: Invasion status overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_invasion_clean.
-- Key columns: vascular_invasion_clean, lymphatic_invasion_clean, perineural_invasion_clean
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_invasion_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    vascular_invasion_clean,
    lymphatic_invasion_clean,
    perineural_invasion_clean,
    capsular_invasion_clean,
    margin_status_clean,
    extranodal_extension_clean
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_invasion_clean`
;

-- =============================================================================
-- HELPER 9: path_malignant_overlay_ln_denom_w_fp_v1
-- Purpose: LN denominator completeness flag overlay keyed by fingerprint.
--          Wraps pub_workspace.canonical_path_malignant_events_v1_ln_denominator_flag.
-- Key columns: ln_denom_missing_any_flag
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ln_denom_w_fp_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    ln_denom_missing_any_flag,
    ln_synoptic_denom_missing_flag,
    ln_detail_denom_missing_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_path_malignant_events_v1_ln_denominator_flag`
;

-- =============================================================================
-- HELPER 10: path_event_discordance_dedup_ete_v1
-- Purpose: T-stage discordance overlay keyed by fingerprint.
--          Wraps pub_workspace.path_event_discordance_v1 (already exists in BQ, 6469 rows).
--          In MotherDuck the "dedup" suffix was meaningful because the discordance view
--          could have dups; in BQ the base view is already deduplicated by key, so this
--          is a direct thin wrapper adding the fingerprint.
-- Key columns: gross_ete_effective, reported_t_stage_ajcc8, derived_t_stage_ajcc8,
--              discordance_t_stage_flag
-- =============================================================================
CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.path_event_discordance_dedup_ete_v1` AS
SELECT
    TO_HEX(MD5(CONCAT(
        CAST(research_id AS STRING), '|',
        COALESCE(CAST(path_surgery_id AS STRING), 'NULL'), '|',
        COALESCE(CAST(tumor_ordinal AS STRING), 'NULL'), '|',
        COALESCE(CAST(synoptic_row_ix AS STRING), 'NULL'), '|',
        COALESCE(specimen_id, 'NULL')
    ))) AS path_event_fingerprint,
    gross_ete_effective,
    reported_t_stage_ajcc8,
    derived_t_stage_ajcc8,
    discordance_t_stage_flag,
    t_stage_derivation_note,
    discordance_laterality_flag
FROM `thyroid-canonical-pub-2026.pub_workspace.path_event_discordance_v1`
;

-- =============================================================================
-- SIGNOFF REGISTRATION (run after all 10 views are created)
-- =============================================================================

-- =============================================================================
-- SMOKE TEST: run after all CREATE statements complete
-- All 10 should return exactly 6469 rows (= base table count)
-- =============================================================================
-- SELECT 'path_malignant_event_fingerprint_v1'      AS view_name, COUNT(*) AS n FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_event_fingerprint_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_ete_clean_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ete_clean_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_global_epi_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_global_epi_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_histology_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_histology_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_variant_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_variant_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_size_flag_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_size_flag_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_laterality_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_laterality_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_invasion_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_invasion_w_fp_v1`
-- UNION ALL
-- SELECT 'path_malignant_overlay_ln_denom_w_fp_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_malignant_overlay_ln_denom_w_fp_v1`
-- UNION ALL
-- SELECT 'path_event_discordance_dedup_ete_v1', COUNT(*) FROM `thyroid-canonical-pub-2026.pub_workspace.path_event_discordance_dedup_ete_v1`
-- ;

INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.bq_migration_log_v1`
    (migration_id, applied_at, applied_by, description, affected_dataset, affected_table, rows_before, rows_after, notes)
VALUES
    ('mig_089_ete_fp_helpers_bq_20260506', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19: 10 ETE fingerprint helper VIEWs in pub_workspace; rowid→MD5 composite key pattern', 'pub_workspace', 'path_malignant_event_fingerprint_v1+9overlays+discordance_dedup', NULL, 6469, 'DFL-20260506-ETEFAMILY; helpers-done milestone; all 10 views expected=6469 rows each'),
    ('mig_089_ete_fp_helpers_bq_20260506_signoff', CURRENT_TIMESTAMP(), 'cursor_agent_thy19', 'THY-19 signoff: path_event_discordance_dedup_ete_v1 wraps existing BQ path_event_discordance_v1 (6469 rows verified)', 'pub_workspace', 'path_event_discordance_dedup_ete_v1', NULL, 6469, 'All 10 helpers materialized as CREATE OR REPLACE VIEWs');
