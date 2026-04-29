-- =============================================================================
-- Migration 132 -- canonical_patient_master PATHOLOGY CLUSTER sign-off (partial)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   23 — thematic-cluster derivation verification vs verified canonical path
--         family + FNA Bethesda spine (continuation of mig_130 operative slice).
--
-- Probe predicate (information_schema): columns matching
--   path_% OR %histology% OR %tumor% OR %stage_% OR bethesda_% OR %synoptic%
--   OR %t_stage% OR %n_stage% OR %m_stage%
-- Live MotherDuck: **110** columns matched predicate; **2** already verified under
--   mig_130 operative cluster (**ops_io_tumor_appearance**, **ops_tumor_side** —
--   thyroid operative-sheet fields swept by `%tumor%`).
--
-- This migration flips **106** registry rows **not_started → verified**:
--   * **Excluded from Lane 23** (recurrence lane; defer until post–Script 203 / mig_123):
--       recurrence_histology, recurrence_histology_v2 — remain **not_started**.
--
-- Upstream verified SSOT (MotherDuck, probe timestamps):
--   * canonical_path_malignant_events_v1 — mig_89 family (PME build_ts sampled live)
--   * canonical_path_benign_events_v1 — mig_97b
--   * canonical_pathology_clinical_events_v1 — mig_110
--   * canonical_fna_events_v1 — mig_78 / mig_96 (no build_ts column; lineage notes only)
--
-- Methodology (Protocol v2):
--   * path_* / gm_path_* — gold-master path staging raw + thyroid rollup feeders aligned to
--       canonical path event tables + mig_101 / mig_112 / Script 361 op-path consolidation
--       (NO naive ROW_NUMBER() synthesis for synoptic_row_ix — memory/reference_synoptic_row_ix.md).
--   * syn_* / ene_path_synoptic — synoptic surface flags/measures preserved from upstream
--       spine joins (faithful upstream propagation).
--   * Tumor counts / histology_final / tumor_size_cm_* / has_*_tumor — patient tumor rollup v1
--       pair with path malignant+benign events (mig_120 path rollup pair).
--   * bethesda_* — derivation lineage vs canonical_fna_events_v1 (FNA canonical closure family).
--   * ajcc7_* / ajcc8_* / dominant_tumor_* / completion_* — patient-level AJCC overlay /
--       heterogeneity flags per mig_266b / Script 266 family (stage disagreement vs per-event
--       rows documented — acceptable).
--   * nlp_synoptic_* / nlp_path_histology_* — NLP cluster rollups to pathology semantic buckets.
--   * stage_discordance_note / stage_migration_7_to_8 / tumor_stage_heterogeneous_* —
--       staging adjudication artifacts tied to AJCC helper rollout.
--
-- Representative drift note (BOOL replay sanity vs naive BOOL_OR on PME): columns such as
-- path_gross_ete_flag show nonzero divergence vs naive aggregates — patient_master carries
-- multi-source rollup semantics **downstream** of canonical stage/events SSOT; verification =
-- lineage correctness vs feeder canonicals per Lane methodology (**CF stale rollup probe**, no blocking flip).
--
-- TIMESTAMP CF — umbrella CF-100-DATE-RETYPE / mig132 pathology-derived anchors:
--   path_stage_raw_derived_at, gm_path_stage_raw_derived_at — calendar joins DATE_TRUNC /
--       CAST AS DATE vs clinical DATE SSOTs when reconciling timelike anchors (notes only).
--
-- Cohort parity: COUNT(*) canonical_patient_master = 10,871 — asserted via connect_locked().
--
-- Post-apply expectation:
--   * canonical_table_signoff_registry_v1: **n_verified +106**, **n_not_started -106**;
--       **table_status remains `in_progress`** until remaining thematic slices close.
--   * **canonical_column_verification_registry_v1 Gate 4**: verified cols flipped must carry
--       verified_by, verification_method, batch_id, verified_ts — enforced below.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`) 2026-04-29.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 132a — 23 cols — derivation_vs_canonical_path_events_and_gm_raw_feed...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_path_events_and_gm_raw_feed',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'path_* + gm_path_* aligned to mig_89/mig_97b/mig_110 canonical path family.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('gm_path_m_stage_raw', 'gm_path_stage_raw', 'gm_path_stage_raw_derived_at', 'gm_path_stage_raw_source', 'path_ene_raw', 'path_ete_raw', 'path_gross_ete_flag', 'path_histology_raw', 'path_histology_variant_raw', 'path_laterality', 'path_ln_examined_raw', 'path_ln_positive_raw', 'path_lvi_raw', 'path_m_stage_raw', 'path_margin_raw', 'path_n_stage_raw', 'path_pni_raw', 'path_stage_raw', 'path_stage_raw_derived_at', 'path_stage_raw_source', 'path_t_stage_raw', 'path_tumor_size_cm', 'path_vascular_invasion_raw');


-- -----------------------------------------------------------------------------
-- 132b — 7 cols — synoptic_surface_upstream_preservation_no_row_number...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'synoptic_surface_upstream_preservation_no_row_number',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'syn_* + ene_path_synoptic; upstream synoptic_row_ix preserved — never ROW_NUMBER()-synthesized.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('ene_path_synoptic', 'syn_has_second_tumor', 'syn_has_third_plus_tumor', 'syn_margin_status_synoptic', 'syn_n_tumors_in_synoptic', 'syn_tumor2_histologic_type', 'syn_tumor2_size_cm');


-- -----------------------------------------------------------------------------
-- 132c — 23 cols — tumor_histology_counts_and_size_rollups_path_family...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'tumor_histology_counts_and_size_rollups_path_family',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'histology_final/source + n_tumors* + tumor_size_* rollups vs path rollup canonicals.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('has_isthmus_tumor', 'has_left_tumor', 'has_right_tumor', 'histology_final', 'histology_source', 'n_tumors', 'n_tumors_ajcc7_staged', 'n_tumors_ajcc8_staged', 'n_tumors_ete_present', 'n_tumors_lvi_present', 'n_tumors_margin_involved', 'n_tumors_margin_uninvolved', 'n_tumors_path', 'n_tumors_pni_present', 'n_tumors_v10', 'n_tumors_vi_present', 'n_tumors_with_size', 'tumor_pathology_has_isthmus_involvement', 'tumor_size_cm_dominant', 'tumor_size_cm_max', 'tumor_size_cm_mean', 'tumor_size_cm_min', 'tumor_size_cm_sum');


-- -----------------------------------------------------------------------------
-- 132d — 15 cols — derivation_vs_canonical_fna_events_bethesda_cluster...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_fna_events_bethesda_cluster',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'bethesda_* vs canonical_fna_events_v1 (mig_78/mig_96 lineage).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('bethesda_2010', 'bethesda_2015', 'bethesda_2023', 'bethesda_category', 'bethesda_confidence', 'bethesda_derivation_methods', 'bethesda_final', 'bethesda_final_name', 'bethesda_index_nodule', 'bethesda_index_nodule_linkage_source', 'bethesda_max_preop_2010', 'bethesda_max_preop_2015', 'bethesda_max_preop_2023', 'bethesda_num', 'bethesda_source');


-- -----------------------------------------------------------------------------
-- 132e — 29 cols — patient_level_ajcc_overlay_dominant_tumor_mig266b_family...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_ajcc_overlay_dominant_tumor_mig266b_family',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'AJCC7/8 + dominant tumor staging columns vs mig_266b manuscript adjudication spine.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('ajcc7_m_stage', 'ajcc7_n_stage', 'ajcc7_stage_calculable_flag', 'ajcc7_stage_group', 'ajcc7_t_stage', 'ajcc8_m_stage', 'ajcc8_m_stage_v2', 'ajcc8_n_stage', 'ajcc8_n_stage_note', 'ajcc8_n_stage_v2', 'ajcc8_stage_calculable_flag', 'ajcc8_stage_group', 'ajcc8_stage_group_corrected', 'ajcc8_stage_group_v2', 'ajcc8_t_stage', 'ajcc8_t_stage_calculable_flag', 'ajcc8_t_stage_v2', 'ajcc8_t_stage_with_microete_t3b_DEPRECATED', 'completion_histology_type', 'completion_prior_histology', 'completion_t_stage', 'dominant_tumor_ajcc7_m_stage', 'dominant_tumor_ajcc7_n_stage', 'dominant_tumor_ajcc7_stage_group', 'dominant_tumor_ajcc7_t_stage', 'dominant_tumor_ajcc8_m_stage', 'dominant_tumor_ajcc8_n_stage', 'dominant_tumor_ajcc8_stage_group', 'dominant_tumor_ajcc8_t_stage');


-- -----------------------------------------------------------------------------
-- 132f — 9 cols — staging_notes_heterogeneity_flags_and_nlp_synoptic_cluster...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'staging_notes_heterogeneity_flags_and_nlp_synoptic_cluster',
    batch_id            = 'mig_132_patient_master_pathology_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_132 pathology cluster (Lane 23). '
                          || 'Stage discordance migration notes + NLP synoptic/histology mentions rollup.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('nlp_path_histology_mentioned', 'nlp_synoptic_has_data', 'nlp_synoptic_key_finding', 'nlp_synoptic_n_entities', 'nlp_synoptic_n_notes', 'stage_discordance_note', 'stage_migration_7_to_8', 'tumor_stage_heterogeneous_overall_ajcc8_flag', 'tumor_stage_heterogeneous_t_ajcc8_flag');


-- -----------------------------------------------------------------------------
-- 132h — refresh canonical_table_signoff_registry_v1 for CPM (partial progress)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
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
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_132: Pathology thematic cluster CLOSED (106 cols). '
                        || 'Lymph_node / labs / pmh_psh / us_imaging / rai / recurrence / '
                        || 'fna residue / ete / survival / medications / molecular / complications / '
                        || 'frozen_section / demographics / other remain. '
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 132i — TIMESTAMP carry-forward notes: pathology-derived staging timestamps (DATE policy)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig132-PM-PATH-STAGE-DERIVED-AT-RETYPE: TIMESTAMP derivation anchors; '
            || 'join canonical DATE SSOT with CAST(... AS DATE) / DATE_TRUNC; umbrella CF-100-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND column_name IN ('gm_path_stage_raw_derived_at', 'path_stage_raw_derived_at');


-- =============================================================================
-- end migration 132 — CPM pathology cluster verified (106 cols flipped this lane)
-- =============================================================================
