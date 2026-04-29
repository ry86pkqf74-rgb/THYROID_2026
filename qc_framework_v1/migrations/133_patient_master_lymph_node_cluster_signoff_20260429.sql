-- =============================================================================
-- Migration 133 -- canonical_patient_master LYMPH NODE CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   24 — thematic-cluster derivation verification vs verified LN family +
--         path malignant spine + operative procedure codes + US lymph node v2.
--
-- Column predicate (fixed patterns — avoid SQL LIKE with unescaped `_` in `%ene_%`,
-- which treats `_` as a single-character wildcard and pulls in false positives such
-- as `ct_thyroid_heterogeneous_any`):
--   ln_% , %lymph_node% , cervical_% , %lateral_neck% , %central_neck% , %nodal% ,
--   ene_% , cnln_% , best_ene_% , %\_ene\_% ESCAPE '\' , gm_path_ene_% , lnus_%
--
-- Live MotherDuck pre-apply (2026-04-29): **142** columns match predicate.
-- **4** already verified in sibling lanes (**ene_path_synoptic**, **path_ene_raw** —
-- mig_132 pathology, **nsqip_central_neck_dissection**, **nsqip_lateral_neck_dissection**
-- — mig_130 operative). This migration flips **138** registry rows **not_started → verified**.
--
-- Upstream SSOT families:
--   * canonical_cervical_ln_clinical_events_v1 — mig_111
--   * canonical_cervical_ln_clinical_patient_rollup_v1 — mig_113
--   * canonical_us_lymph_node_v2 — mig_117
--   * canonical_path_malignant_events_v1 + patient rollup — mig_89 / Script 361
--   * canonical_ete_subgrade_events_v1 — mig_114 (multisource ENE landing)
--   * canonical_operative_events_v1 / operative_procedure_codes — mig_118
--
-- Methodology (Protocol v2):
--   * **NULL vs FALSE/0** — bulk of patients lack LN evidence. COALESCE probes required
--     when replaying BOOL_OR / aggregates (feedback_recurrence_imaging_n_events_null.md).
--   * **Cross-source** — nodal counts may disagree between cervical_ln_clinical,
--     path malignant aggregates, and US LN surface. CPM carries precedence-resolved
--     rollup semantics post–Script 361 — verification = feeder lineage, not naive
--     single-source equality.
--   * **VARCHAR cnln_* dates** — partial parse tiers. Calendar joins use TRY_CAST /
--     explicit DATE SSOTs (clinical_date_retype_20260428 / CF-100-DATE-RETYPE).
--
-- Pre-apply integrity probes:
--   * Cohort parity: COUNT(*) = COUNT(DISTINCT research_id) = **10,871**.
--   * ln_total_examined < ln_total_positive: **1** row (**research_id** 68, examined=0,
--     positive=1) — documented. Not a registry gate blocker.
--   * ene_positive prevalence: **1,252 / 10,871 (~11.5%)** — below 30% investigation threshold.
--
-- Post-apply: canonical_table_signoff_registry_v1 for CPM refreshed.
-- Gate 4: every flipped col has verified_by, verification_method, batch_id, verified_ts.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`) 2026-04-29.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 133a — 36 cols — derivation_cervical_ln_multi_source_mig111_113_path_us...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_cervical_ln_multi_source_mig111_113_path_us',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'cnln_* vs cervical_ln clinical events/rollup + path + imaging modalities.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('cnln_any_positive_any_modality', 'cnln_clin_any_positive', 'cnln_clin_avg_confidence', 'cnln_clin_n_entities', 'cnln_earliest_date', 'cnln_ene_any_modality', 'cnln_img_any_suspicious', 'cnln_img_avg_confidence', 'cnln_img_first_date', 'cnln_img_last_date', 'cnln_img_laterality', 'cnln_img_levels_mentioned', 'cnln_img_max_size_cm', 'cnln_img_n_entities', 'cnln_latest_date', 'cnln_modalities_present', 'cnln_n_modalities', 'cnln_novel_positive_flag', 'cnln_path_any_positive', 'cnln_path_ene_any', 'cnln_path_max_positive_count', 'cnln_path_n_entities', 'cnln_source_table', 'cnln_surg_any_positive', 'cnln_surg_avg_confidence', 'cnln_surg_bilateral', 'cnln_surg_ene_any', 'cnln_surg_first_date', 'cnln_surg_last_date', 'cnln_surg_levels_mentioned', 'cnln_surg_max_positive_count', 'cnln_surg_max_total_examined', 'cnln_surg_n_entities', 'cnln_surg_n_notes', 'cnln_surg_source_note_types', 'cnln_total_entities');


-- -----------------------------------------------------------------------------
-- 133b — 34 cols — derivation_ln_core_path_malignant_and_level_rollups_mig89...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_ln_core_path_malignant_and_level_rollups_mig89',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'ln_* counts/ratios/levels vs path malignant family + synoptic-level feeders; NULL bulk off-rollup patients.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('ln_burden_band', 'ln_count_reconciled', 'ln_data_quality_flag', 'ln_ene_status', 'ln_lateral_dissected', 'ln_level_i_examined', 'ln_level_i_positive', 'ln_level_ii_examined', 'ln_level_ii_positive', 'ln_level_iii_examined', 'ln_level_iii_positive', 'ln_level_iv_examined', 'ln_level_iv_positive', 'ln_level_v_examined', 'ln_level_v_positive', 'ln_level_vi_examined', 'ln_level_vi_positive', 'ln_level_vii_examined', 'ln_level_vii_positive', 'ln_mets_atc', 'ln_mets_ene_count', 'ln_mets_ftc', 'ln_mets_hurthle', 'ln_mets_micrometastasis', 'ln_mets_mtc', 'ln_mets_pdtc', 'ln_mets_ptc', 'ln_positive_binary', 'ln_positive_count_raw', 'ln_positive_final', 'ln_positive_flag', 'ln_ratio', 'ln_total_examined', 'ln_total_positive');


-- -----------------------------------------------------------------------------
-- 133c — 31 cols — derivation_ln_rollup_pathology_pair_internal_consistency...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_ln_rollup_pathology_pair_internal_consistency',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'ln_rollup_* wide nodal summary vs path rollups + cross-validation status.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('ln_rollup_any_positive', 'ln_rollup_bilateral_lateral_examined', 'ln_rollup_bilateral_lateral_positive', 'ln_rollup_central_examined', 'ln_rollup_central_positive', 'ln_rollup_crossval_status', 'ln_rollup_ene', 'ln_rollup_has_per_level_data', 'ln_rollup_internal_consistency', 'ln_rollup_largest_deposit_cm', 'ln_rollup_lateral_left_examined', 'ln_rollup_lateral_left_positive', 'ln_rollup_lateral_right_examined', 'ln_rollup_lateral_right_positive', 'ln_rollup_mets_atc', 'ln_rollup_mets_cystic', 'ln_rollup_mets_ene', 'ln_rollup_mets_ftc', 'ln_rollup_mets_hurthle', 'ln_rollup_mets_micrometastasis', 'ln_rollup_mets_mtc', 'ln_rollup_mets_pdtc', 'ln_rollup_mets_ptc', 'ln_rollup_mets_ptc_variant', 'ln_rollup_other_examined', 'ln_rollup_other_positive', 'ln_rollup_ratio', 'ln_rollup_source', 'ln_rollup_total_examined', 'ln_rollup_total_levels_involved', 'ln_rollup_total_positive');


-- -----------------------------------------------------------------------------
-- 133d — 17 cols — derivation_ene_multisource_mig114_gm_path_raw...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_ene_multisource_mig114_gm_path_raw',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'ENE v9 + imaging concordance vs ete_subgrade / invasion canonicals; gm_path_ene_raw path spine.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('best_ene_grade', 'ene_ct', 'ene_deposit_cm', 'ene_grade_v9', 'ene_levels_v9', 'ene_ln_concordance_status', 'ene_n_sources', 'ene_op_intraop', 'ene_path_ct_concordance', 'ene_path_levels', 'ene_path_nlp', 'ene_pet', 'ene_positive', 'ene_rai_scan', 'ene_record_count_v9', 'ene_us', 'gm_path_ene_raw');


-- -----------------------------------------------------------------------------
-- 133e — 5 cols — derivation_regional_neck_dissection_operative_synoptic_m118...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_regional_neck_dissection_operative_synoptic_m118',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'lateral/central neck dissection flags vs operative spine + NLP + synoptic (NSQIP cols verified mig_130).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('lateral_neck_dissected', 'lateral_neck_dissected_structured_or_nlp', 'lateral_neck_dissected_v10', 'proc_nlp_lateral_neck_dissection', 'syn_bilateral_neck_dissection');


-- -----------------------------------------------------------------------------
-- 133f — 15 cols — derivation_us_lymph_node_surface_mig117...
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_us_lymph_node_surface_mig117',
    batch_id            = 'mig_133_patient_master_lymph_node_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_133 lymph_node cluster (Lane 24). '
                          || 'lnus_* vs canonical_us_lymph_node_v2 + CPM staging context.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ('lnus_abnormal_any_exam_v2', 'lnus_abnormal_ln_any', 'lnus_first_abnormal_us_ln_date_v2', 'lnus_first_date', 'lnus_first_days_from_surg', 'lnus_has_dedicated_exam', 'lnus_has_size_measurement', 'lnus_impression_last', 'lnus_indication_first', 'lnus_last_date', 'lnus_last_days_from_surg', 'lnus_n_exams', 'lnus_n_us_with_ln_assessment_v2', 'lnus_normal_ln_any', 'lnus_source');


-- -----------------------------------------------------------------------------
-- 133h — refresh canonical_table_signoff_registry_v1 for CPM (partial progress)
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
                        || ' | mig_133: Lymph_node thematic cluster CLOSED (138 cols). '
                        || 'labs / pmh_psh / us_imaging / rai / recurrence / '
                        || 'fna residue / ete / survival / medications / molecular / complications / '
                        || 'frozen_section / demographics / other remain.'
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
-- 133i — VARCHAR / mixed-parse cnln date carry-forwards (calendar policy)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig133-PM-CNCLN-DATE-PARSE: VARCHAR cnln_* anchor dates; '
            || 'TRY_CAST / DATE SSOT joins only; umbrella CF-100-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_133_patient_master_lymph_node_cluster_20260429'
  AND column_name IN (
    'cnln_earliest_date', 'cnln_img_first_date', 'cnln_img_last_date', 'cnln_latest_date',
    'cnln_surg_first_date', 'cnln_surg_last_date'
  );


-- -----------------------------------------------------------------------------
-- 133j — Integrity carry-forward: impossible examined/positive pair
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig133-PM-LN-COUNT-INTEGRITY: 1 patient (research_id=68) has ln_total_examined=0 & ln_total_positive=1; clinical/source quirk — not auto-corrected.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_133_patient_master_lymph_node_cluster_20260429'
  AND column_name IN ('ln_total_examined', 'ln_total_positive');


-- =============================================================================
-- end migration 133 — CPM lymph_node cluster verified (138 cols flipped this lane)
-- =============================================================================
