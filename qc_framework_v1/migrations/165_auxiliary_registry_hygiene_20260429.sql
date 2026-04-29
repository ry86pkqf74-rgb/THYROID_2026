-- =============================================================================
-- Migration 165 — Auxiliary registry hygiene (mass auto-na + CF staging)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   53 / mig_165
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md
-- batch_id: mig_165_auxiliary_registry_hygiene_20260429
--
-- Scope: Registry-only writes ON MotherDuck RW `thyroid_canonical_publication_v1_0`.
-- Path C / Cowork executes APPLY after independent review — **do not RW from agent**.
--
-- Cowork probe corrections vs draft prompt (main-only schema join blind spot):
-- * **85** auxiliary `not_started` rows split **`main` (30)** + **`manuscript_workspace` (56)** —
--   **ALL** rows have physical backing when keyed by **`registry.schema_name`** (0 orphan DELETEs).
-- * Draft cited **53 “stale”** rows — those tables live under **`manuscript_workspace`**, not `main`.
--
-- Live probes (MotherDuck `thyroid_canonical_publication_v1_0`, read-only 2026-04-29):
-- * Gate1 baseline `COUNT(*) WHERE table_status='verified'` on **canonical_table_signoff_registry_v1**
--   = **88** verified tables pre-mig_165.
-- * Expected Gate1 uplift: **+77** → **165** verified tables (**76** existing auxiliary tables flipped +
--   **1** new Tier-1 registration `note_entities_llm_presenting_symptoms`).
-- * **`recurrence_event_clean_v1`** remains **`not_started`** — **CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY**
--   (defer real Tier-2 verification to mig_163 lane per prompt §8).
--
-- DELETE blocks: **NONE** — zero registry rows without physical backing after schema-qualified join.
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshots (archive DB — full registry slice for affected objects)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig_165_auxiliary_registry_hygiene_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig165_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
    ('main', 'clinical_notes_long'),
    ('main', 'clinical_note_ln_extracted_v1'),
    ('main', 'path_synoptics'),
    ('main', 'ct_imaging'),
    ('main', 'mri_imaging'),
    ('main', 'nuclear_med'),
    ('main', 'thyroid_sizes'),
    ('main', 'thyroid_weights'),
    ('main', 'note_entities_operative_detail'),
    ('main', 'note_entities_procedures'),
    ('main', 'imaging_exam_master_v1'),
    ('main', '__readme'),
    ('main', 'data_dictionary_v279'),
    ('main', 'cupm_v2_canonical_backfill_v1'),
    ('main', 'ete_adjudication_v1'),
    ('main', 'patient_completion_oed_path_linkage_v1'),
    ('main', 'nsqip_enrichment'),
    ('main', 'nsqip_patient_summary'),
    ('main', 'specimen_genomic_assay_v1'),
    ('main', 'specimen_master_v1'),
    ('main', 'specimen_source_xref_v1'),
    ('main', 'specimen_tumor_focus_v1'),
    ('main', 'tg_postop_surveillance_windows_v1'),
    ('main', 'tg_timeline_patient_summary_v1'),
    ('manuscript_workspace', 'agent_adjudication_log_v1'),
    ('manuscript_workspace', 'archive_candidate_review_v1'),
    ('manuscript_workspace', 'archive_move_log_v1'),
    ('manuscript_workspace', 'canonical_cleanup_audit_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_concordance_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_discordance_canonical_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_vs_tp_hist1_discordance_v1'),
    ('manuscript_workspace', 'cpm_backfill_log_v1'),
    ('manuscript_workspace', 'cpm_histologic_classification_audit_v1'),
    ('manuscript_workspace', 'cpm_is_malignant_flag_review_v1'),
    ('manuscript_workspace', 'cpm_missing_data_provenance_v1'),
    ('manuscript_workspace', 'cpm_reconciliation_provenance_v1'),
    ('manuscript_workspace', 'cpm_stage_group_manual_review_v1'),
    ('manuscript_workspace', 'cpm_tirads_audit_classification_v1'),
    ('manuscript_workspace', 'cpm_tirads_canonical_coverage_v1'),
    ('manuscript_workspace', 'cpm_tnm_cross_source_disagreements_v1'),
    ('manuscript_workspace', 'genetics_per_test_discordance_v1'),
    ('manuscript_workspace', 'lab_orphan_audit_v1'),
    ('manuscript_workspace', 'lab_orphan_cohort_review_v1'),
    ('manuscript_workspace', 'ln_crossval_v1'),
    ('manuscript_workspace', 'manuscript_dive_map_v1'),
    ('manuscript_workspace', 'manuscript_feasibility_v1'),
    ('manuscript_workspace', 'n_surgeries_v1_v2_conflict_v1'),
    ('manuscript_workspace', 'nlp_rollup_promotion_audit_v1'),
    ('manuscript_workspace', 'path_tumor_size_chart_review_queue_v1'),
    ('manuscript_workspace', 'path_tumor_size_correction_queue_v1'),
    ('manuscript_workspace', 'path_tumor_size_multifocal_enumeration_notes_v1'),
    ('manuscript_workspace', 'pi_review_queue_v1'),
    ('manuscript_workspace', 'qc_manual_review_queue_v1'),
    ('manuscript_workspace', 'qc_rules_v1'),
    ('manuscript_workspace', 'qc_tir03_llm_candidates_v1'),
    ('manuscript_workspace', 'qc_usln01_llm_candidates_v1'),
    ('manuscript_workspace', 'qc_violations_v1'),
    ('manuscript_workspace', 'recurrence_imaging_suspicious_candidates_v1'),
    ('manuscript_workspace', 'schema_reorg_move_log_v1'),
    ('manuscript_workspace', 'schema_reorg_orphan_references_v1'),
    ('manuscript_workspace', 'script_387_dedup_probe_v1'),
    ('manuscript_workspace', 'script_388_archive_move_log_v1'),
    ('manuscript_workspace', 'script_389_archive_move_log_v1'),
    ('manuscript_workspace', 'tg_orphan_cancer_text_investigation_queue_v1'),
    ('manuscript_workspace', 'us_llm_absorption_mapping_v1'),
    ('manuscript_workspace', 'us_nodule_conflict_queue_v1'),
    ('manuscript_workspace', 'us_raw_index0_conflict_v1'),
    ('manuscript_workspace', 'us_raw_index_mismatch_v1'),
    ('manuscript_workspace', 'v1_1_finalization_audit_v1'),
    ('manuscript_workspace', 'vc_complication_tiering_v1'),
    ('manuscript_workspace', 'detail_table_registry_v1'),
    ('manuscript_workspace', 'main_schema_keep_list_v1'),
    ('manuscript_workspace', 'object_domain_map_v1'),
    ('manuscript_workspace', 'registry_end_to_end_validation_v1'),
    ('manuscript_workspace', 'registry_v2_resolution_audit_v1'),
    ('manuscript_workspace', 'registry_v2_unresolved_pointers_v1'),
    ('main', 'note_entities_llm_presenting_symptoms')
    ) AS v(schema_name, table_name)
);

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig_165_auxiliary_registry_hygiene_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig165_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
    ('main', 'clinical_notes_long'),
    ('main', 'clinical_note_ln_extracted_v1'),
    ('main', 'path_synoptics'),
    ('main', 'ct_imaging'),
    ('main', 'mri_imaging'),
    ('main', 'nuclear_med'),
    ('main', 'thyroid_sizes'),
    ('main', 'thyroid_weights'),
    ('main', 'note_entities_operative_detail'),
    ('main', 'note_entities_procedures'),
    ('main', 'imaging_exam_master_v1'),
    ('main', '__readme'),
    ('main', 'data_dictionary_v279'),
    ('main', 'cupm_v2_canonical_backfill_v1'),
    ('main', 'ete_adjudication_v1'),
    ('main', 'patient_completion_oed_path_linkage_v1'),
    ('main', 'nsqip_enrichment'),
    ('main', 'nsqip_patient_summary'),
    ('main', 'specimen_genomic_assay_v1'),
    ('main', 'specimen_master_v1'),
    ('main', 'specimen_source_xref_v1'),
    ('main', 'specimen_tumor_focus_v1'),
    ('main', 'tg_postop_surveillance_windows_v1'),
    ('main', 'tg_timeline_patient_summary_v1'),
    ('manuscript_workspace', 'agent_adjudication_log_v1'),
    ('manuscript_workspace', 'archive_candidate_review_v1'),
    ('manuscript_workspace', 'archive_move_log_v1'),
    ('manuscript_workspace', 'canonical_cleanup_audit_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_concordance_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_discordance_canonical_v1'),
    ('manuscript_workspace', 'cpm_ajcc_dominant_vs_tp_hist1_discordance_v1'),
    ('manuscript_workspace', 'cpm_backfill_log_v1'),
    ('manuscript_workspace', 'cpm_histologic_classification_audit_v1'),
    ('manuscript_workspace', 'cpm_is_malignant_flag_review_v1'),
    ('manuscript_workspace', 'cpm_missing_data_provenance_v1'),
    ('manuscript_workspace', 'cpm_reconciliation_provenance_v1'),
    ('manuscript_workspace', 'cpm_stage_group_manual_review_v1'),
    ('manuscript_workspace', 'cpm_tirads_audit_classification_v1'),
    ('manuscript_workspace', 'cpm_tirads_canonical_coverage_v1'),
    ('manuscript_workspace', 'cpm_tnm_cross_source_disagreements_v1'),
    ('manuscript_workspace', 'genetics_per_test_discordance_v1'),
    ('manuscript_workspace', 'lab_orphan_audit_v1'),
    ('manuscript_workspace', 'lab_orphan_cohort_review_v1'),
    ('manuscript_workspace', 'ln_crossval_v1'),
    ('manuscript_workspace', 'manuscript_dive_map_v1'),
    ('manuscript_workspace', 'manuscript_feasibility_v1'),
    ('manuscript_workspace', 'n_surgeries_v1_v2_conflict_v1'),
    ('manuscript_workspace', 'nlp_rollup_promotion_audit_v1'),
    ('manuscript_workspace', 'path_tumor_size_chart_review_queue_v1'),
    ('manuscript_workspace', 'path_tumor_size_correction_queue_v1'),
    ('manuscript_workspace', 'path_tumor_size_multifocal_enumeration_notes_v1'),
    ('manuscript_workspace', 'pi_review_queue_v1'),
    ('manuscript_workspace', 'qc_manual_review_queue_v1'),
    ('manuscript_workspace', 'qc_rules_v1'),
    ('manuscript_workspace', 'qc_tir03_llm_candidates_v1'),
    ('manuscript_workspace', 'qc_usln01_llm_candidates_v1'),
    ('manuscript_workspace', 'qc_violations_v1'),
    ('manuscript_workspace', 'recurrence_imaging_suspicious_candidates_v1'),
    ('manuscript_workspace', 'schema_reorg_move_log_v1'),
    ('manuscript_workspace', 'schema_reorg_orphan_references_v1'),
    ('manuscript_workspace', 'script_387_dedup_probe_v1'),
    ('manuscript_workspace', 'script_388_archive_move_log_v1'),
    ('manuscript_workspace', 'script_389_archive_move_log_v1'),
    ('manuscript_workspace', 'tg_orphan_cancer_text_investigation_queue_v1'),
    ('manuscript_workspace', 'us_llm_absorption_mapping_v1'),
    ('manuscript_workspace', 'us_nodule_conflict_queue_v1'),
    ('manuscript_workspace', 'us_raw_index0_conflict_v1'),
    ('manuscript_workspace', 'us_raw_index_mismatch_v1'),
    ('manuscript_workspace', 'v1_1_finalization_audit_v1'),
    ('manuscript_workspace', 'vc_complication_tiering_v1'),
    ('manuscript_workspace', 'detail_table_registry_v1'),
    ('manuscript_workspace', 'main_schema_keep_list_v1'),
    ('manuscript_workspace', 'object_domain_map_v1'),
    ('manuscript_workspace', 'registry_end_to_end_validation_v1'),
    ('manuscript_workspace', 'registry_v2_resolution_audit_v1'),
    ('manuscript_workspace', 'registry_v2_unresolved_pointers_v1'),
    ('main', 'note_entities_llm_presenting_symptoms')
    ) AS v(schema_name, table_name)
);

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 165b-main-tier1-raw-mirror
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'auto_tier1_raw_mirror_skip',
    batch_id            = 'mig_165_auxiliary_registry_hygiene_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane (165b-main-tier1-raw-mirror): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = 'main'
  AND table_name IN (
    'clinical_notes_long',
    'clinical_note_ln_extracted_v1',
    'path_synoptics',
    'ct_imaging',
    'mri_imaging',
    'nuclear_med',
    'thyroid_sizes',
    'thyroid_weights',
    'note_entities_operative_detail',
    'note_entities_procedures',
    'imaging_exam_master_v1'
  )
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 165c-main-registry-governance
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'auto_registry_governance_skip',
    batch_id            = 'mig_165_auxiliary_registry_hygiene_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane (165c-main-registry-governance): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = 'main'
  AND table_name IN (
    '__readme',
    'data_dictionary_v279'
  )
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 165d-main-governance-audit
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'auto_governance_audit_table_skip',
    batch_id            = 'mig_165_auxiliary_registry_hygiene_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane (165d-main-governance-audit): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = 'main'
  AND table_name IN (
    'cupm_v2_canonical_backfill_v1',
    'ete_adjudication_v1',
    'patient_completion_oed_path_linkage_v1',
    'nsqip_enrichment',
    'nsqip_patient_summary',
    'specimen_genomic_assay_v1',
    'specimen_master_v1',
    'specimen_source_xref_v1',
    'specimen_tumor_focus_v1',
    'tg_postop_surveillance_windows_v1',
    'tg_timeline_patient_summary_v1'
  )
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 165e-manuscript_workspace-registry-governance
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'auto_registry_governance_skip',
    batch_id            = 'mig_165_auxiliary_registry_hygiene_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane (165e-manuscript_workspace-registry-governance): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name IN (
    'detail_table_registry_v1',
    'main_schema_keep_list_v1',
    'object_domain_map_v1',
    'registry_end_to_end_validation_v1',
    'registry_v2_resolution_audit_v1',
    'registry_v2_unresolved_pointers_v1'
  )
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 165f-manuscript_workspace-governance-audit
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = 'auto_governance_audit_table_skip',
    batch_id            = 'mig_165_auxiliary_registry_hygiene_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane (165f-manuscript_workspace-governance-audit): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name IN (
    'agent_adjudication_log_v1',
    'archive_candidate_review_v1',
    'archive_move_log_v1',
    'canonical_cleanup_audit_v1',
    'cpm_ajcc_dominant_concordance_v1',
    'cpm_ajcc_dominant_discordance_canonical_v1',
    'cpm_ajcc_dominant_vs_tp_hist1_discordance_v1',
    'cpm_backfill_log_v1',
    'cpm_histologic_classification_audit_v1',
    'cpm_is_malignant_flag_review_v1',
    'cpm_missing_data_provenance_v1',
    'cpm_reconciliation_provenance_v1',
    'cpm_stage_group_manual_review_v1',
    'cpm_tirads_audit_classification_v1',
    'cpm_tirads_canonical_coverage_v1',
    'cpm_tnm_cross_source_disagreements_v1',
    'genetics_per_test_discordance_v1',
    'lab_orphan_audit_v1',
    'lab_orphan_cohort_review_v1',
    'ln_crossval_v1',
    'manuscript_dive_map_v1',
    'manuscript_feasibility_v1',
    'n_surgeries_v1_v2_conflict_v1',
    'nlp_rollup_promotion_audit_v1',
    'path_tumor_size_chart_review_queue_v1',
    'path_tumor_size_correction_queue_v1',
    'path_tumor_size_multifocal_enumeration_notes_v1',
    'pi_review_queue_v1',
    'qc_manual_review_queue_v1',
    'qc_rules_v1',
    'qc_tir03_llm_candidates_v1',
    'qc_usln01_llm_candidates_v1',
    'qc_violations_v1',
    'recurrence_imaging_suspicious_candidates_v1',
    'schema_reorg_move_log_v1',
    'schema_reorg_orphan_references_v1',
    'script_387_dedup_probe_v1',
    'script_388_archive_move_log_v1',
    'script_389_archive_move_log_v1',
    'tg_orphan_cancer_text_investigation_queue_v1',
    'us_llm_absorption_mapping_v1',
    'us_nodule_conflict_queue_v1',
    'us_raw_index0_conflict_v1',
    'us_raw_index_mismatch_v1',
    'v1_1_finalization_audit_v1',
    'vc_complication_tiering_v1'
  )
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 165g — CF stamps on deferred analytic / Tier-2 tables (column registry untouched)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-imaging_fna_linkage_v3: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'imaging_fna_linkage_v3'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-imaging_patient_summary_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'imaging_patient_summary_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-manuscript_cohort_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'manuscript_cohort_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-patient_cross_domain_timeline_v2: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'patient_cross_domain_timeline_v2'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-tumor_stage_heterogeneity_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'tumor_stage_heterogeneity_v1'
  AND table_status = 'not_started';
UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'main'
  AND table_name = 'recurrence_event_clean_v1'
  AND table_status = 'not_started';
UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-episode_analysis_resolved_v1_dedup: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'episode_analysis_resolved_v1_dedup'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-lesion_analysis_resolved_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'lesion_analysis_resolved_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-ln_master_rollup_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'ln_master_rollup_v1'
  AND table_status = 'not_started';

UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || ' | CF-mig165-AUX-NEEDS-REAL-VERIFY-patient_analysis_resolved_v1: analytic / Tier-2 feeder deferred real verification (mig_165 Lane 53 classification).'
WHERE schema_name = 'manuscript_workspace'
  AND table_name = 'patient_analysis_resolved_v1'
  AND table_status = 'not_started';
-- -----------------------------------------------------------------------------
-- 165h — Register Tier-1 raw mirror `note_entities_llm_presenting_symptoms` (orphan BASE TABLE)
-- -----------------------------------------------------------------------------
INSERT INTO main.canonical_column_verification_registry_v1
       (schema_name, table_name, column_name, data_type, ordinal_position,
        category, upstream_source, verification_status, verified_by, verified_ts,
        verification_method, batch_id, notes)
SELECT ic.table_schema,
       ic.table_name,
       ic.column_name,
       ic.data_type,
       ic.ordinal_position,
       'source' AS category,
       NULL AS upstream_source,
       'na' AS verification_status,
       'logan' AS verified_by,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
       'auto_tier1_raw_mirror_skip' AS verification_method,
       'mig_165_auxiliary_registry_hygiene_20260429' AS batch_id,
       'mig_165 orphan Tier-1 LLM mirror registration + immediate na classification (Lane 53).'
FROM information_schema.columns AS ic
JOIN information_schema.tables AS it
  ON it.table_catalog = ic.table_catalog
 AND it.table_schema = ic.table_schema
 AND it.table_name = ic.table_name
WHERE ic.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND ic.table_schema = 'main'
  AND ic.table_name = 'note_entities_llm_presenting_symptoms'
  AND it.table_type = 'BASE TABLE'
  AND NOT EXISTS (
        SELECT 1
        FROM main.canonical_column_verification_registry_v1 AS r
        WHERE r.schema_name = ic.table_schema
          AND r.table_name = ic.table_name
          AND r.column_name = ic.column_name
      );

INSERT INTO main.canonical_table_signoff_registry_v1
       (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
        table_status, signed_off_ts, signoff_migration, priority_tier, notes)
SELECT sub.schema_name,
       sub.table_name,
       sub.n_columns_total,
       0 AS n_verified,
       0 AS n_not_started,
       0 AS n_failed,
       sub.n_na,
       'verified' AS table_status,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS signed_off_ts,
       'qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql' AS signoff_migration,
       NULL AS priority_tier,
       'mig_165 Tier-1 raw mirror mirror-only classification — all cols na.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_columns_total,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_presenting_symptoms'
  GROUP BY 1, 2
) AS sub
WHERE NOT EXISTS (
    SELECT 1 FROM main.canonical_table_signoff_registry_v1 ts
    WHERE ts.schema_name = sub.schema_name AND ts.table_name = sub.table_name
);

-- -----------------------------------------------------------------------------
-- 165i — Resync canonical_table_signoff_registry_v1 aggregates (mig_159 §159g pattern)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total   = subq.n_total,
    n_verified        = subq.n_verified,
    n_not_started     = subq.n_not_started,
    n_failed          = COALESCE(subq.n_failed, 0),
    n_na              = subq.n_na,
    table_status      = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts       = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration   = 'qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql',
    notes               = COALESCE(ts.notes, '')
                          || ' | mig_165: auxiliary lane AUTO sign-off rollup (verified when na+verified clears queue).'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE (schema_name, table_name) IN (
      SELECT * FROM (VALUES
      ('main', 'clinical_notes_long'),
          ('main', 'clinical_note_ln_extracted_v1'),
          ('main', 'path_synoptics'),
          ('main', 'ct_imaging'),
          ('main', 'mri_imaging'),
          ('main', 'nuclear_med'),
          ('main', 'thyroid_sizes'),
          ('main', 'thyroid_weights'),
          ('main', 'note_entities_operative_detail'),
          ('main', 'note_entities_procedures'),
          ('main', 'imaging_exam_master_v1'),
          ('main', '__readme'),
          ('main', 'data_dictionary_v279'),
          ('main', 'cupm_v2_canonical_backfill_v1'),
          ('main', 'ete_adjudication_v1'),
          ('main', 'patient_completion_oed_path_linkage_v1'),
          ('main', 'nsqip_enrichment'),
          ('main', 'nsqip_patient_summary'),
          ('main', 'specimen_genomic_assay_v1'),
          ('main', 'specimen_master_v1'),
          ('main', 'specimen_source_xref_v1'),
          ('main', 'specimen_tumor_focus_v1'),
          ('main', 'tg_postop_surveillance_windows_v1'),
          ('main', 'tg_timeline_patient_summary_v1'),
          ('manuscript_workspace', 'agent_adjudication_log_v1'),
          ('manuscript_workspace', 'archive_candidate_review_v1'),
          ('manuscript_workspace', 'archive_move_log_v1'),
          ('manuscript_workspace', 'canonical_cleanup_audit_v1'),
          ('manuscript_workspace', 'cpm_ajcc_dominant_concordance_v1'),
          ('manuscript_workspace', 'cpm_ajcc_dominant_discordance_canonical_v1'),
          ('manuscript_workspace', 'cpm_ajcc_dominant_vs_tp_hist1_discordance_v1'),
          ('manuscript_workspace', 'cpm_backfill_log_v1'),
          ('manuscript_workspace', 'cpm_histologic_classification_audit_v1'),
          ('manuscript_workspace', 'cpm_is_malignant_flag_review_v1'),
          ('manuscript_workspace', 'cpm_missing_data_provenance_v1'),
          ('manuscript_workspace', 'cpm_reconciliation_provenance_v1'),
          ('manuscript_workspace', 'cpm_stage_group_manual_review_v1'),
          ('manuscript_workspace', 'cpm_tirads_audit_classification_v1'),
          ('manuscript_workspace', 'cpm_tirads_canonical_coverage_v1'),
          ('manuscript_workspace', 'cpm_tnm_cross_source_disagreements_v1'),
          ('manuscript_workspace', 'genetics_per_test_discordance_v1'),
          ('manuscript_workspace', 'lab_orphan_audit_v1'),
          ('manuscript_workspace', 'lab_orphan_cohort_review_v1'),
          ('manuscript_workspace', 'ln_crossval_v1'),
          ('manuscript_workspace', 'manuscript_dive_map_v1'),
          ('manuscript_workspace', 'manuscript_feasibility_v1'),
          ('manuscript_workspace', 'n_surgeries_v1_v2_conflict_v1'),
          ('manuscript_workspace', 'nlp_rollup_promotion_audit_v1'),
          ('manuscript_workspace', 'path_tumor_size_chart_review_queue_v1'),
          ('manuscript_workspace', 'path_tumor_size_correction_queue_v1'),
          ('manuscript_workspace', 'path_tumor_size_multifocal_enumeration_notes_v1'),
          ('manuscript_workspace', 'pi_review_queue_v1'),
          ('manuscript_workspace', 'qc_manual_review_queue_v1'),
          ('manuscript_workspace', 'qc_rules_v1'),
          ('manuscript_workspace', 'qc_tir03_llm_candidates_v1'),
          ('manuscript_workspace', 'qc_usln01_llm_candidates_v1'),
          ('manuscript_workspace', 'qc_violations_v1'),
          ('manuscript_workspace', 'recurrence_imaging_suspicious_candidates_v1'),
          ('manuscript_workspace', 'schema_reorg_move_log_v1'),
          ('manuscript_workspace', 'schema_reorg_orphan_references_v1'),
          ('manuscript_workspace', 'script_387_dedup_probe_v1'),
          ('manuscript_workspace', 'script_388_archive_move_log_v1'),
          ('manuscript_workspace', 'script_389_archive_move_log_v1'),
          ('manuscript_workspace', 'tg_orphan_cancer_text_investigation_queue_v1'),
          ('manuscript_workspace', 'us_llm_absorption_mapping_v1'),
          ('manuscript_workspace', 'us_nodule_conflict_queue_v1'),
          ('manuscript_workspace', 'us_raw_index0_conflict_v1'),
          ('manuscript_workspace', 'us_raw_index_mismatch_v1'),
          ('manuscript_workspace', 'v1_1_finalization_audit_v1'),
          ('manuscript_workspace', 'vc_complication_tiering_v1'),
          ('manuscript_workspace', 'detail_table_registry_v1'),
          ('manuscript_workspace', 'main_schema_keep_list_v1'),
          ('manuscript_workspace', 'object_domain_map_v1'),
          ('manuscript_workspace', 'registry_end_to_end_validation_v1'),
          ('manuscript_workspace', 'registry_v2_resolution_audit_v1'),
          ('manuscript_workspace', 'registry_v2_unresolved_pointers_v1'),
          ('main', 'note_entities_llm_presenting_symptoms')
      ) AS v(schema_name, table_name)
    )
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

COMMIT;

-- =============================================================================
-- end migration 165 — auxiliary registry hygiene (Lane 53)
-- =============================================================================
