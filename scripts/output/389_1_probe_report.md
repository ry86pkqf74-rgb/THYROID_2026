# Script 389.1 — Registry Schema Migration · Phase 0 Probe

Generated: 2026-04-22T21:30:18.163263+00:00
PUB DB: `thyroid_canonical_publication_v1_0`
Registry: `thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1` (144 rows, 15 columns)
Archive schema: `archive_pub_v1_0` (4 snapshot tables)
Move log: `thyroid_canonical_publication_v1_0.manuscript_workspace.archive_move_log_v1`
New columns already present: **True**

## Current registry columns

| ordinal | column | type |
|---:|---|---|
| 1 | `detail_table_name` | `VARCHAR` |
| 2 | `schema_name` | `VARCHAR` |
| 3 | `join_key` | `VARCHAR` |
| 4 | `grain` | `VARCHAR` |
| 5 | `total_rows` | `BIGINT` |
| 6 | `total_patients` | `BIGINT` |
| 7 | `domain` | `VARCHAR` |
| 8 | `feeds_master_columns` | `VARCHAR` |
| 9 | `description` | `VARCHAR` |
| 10 | `canonical_version` | `VARCHAR` |
| 11 | `feeds_master_columns_secondary` | `VARCHAR` |
| 12 | `feeds_master_columns_array` | `VARCHAR[]` |
| 13 | `needs_manual_review` | `BOOLEAN` |
| 14 | `superseded_by` | `VARCHAR` |
| 15 | `renamed_by_script` | `VARCHAR` |

## Proposed backfill map (17 rows)

Rule: `renamed_by_script` is the highest script number derivable from either the `_pre<NNN>_` archive name or the `script` column in `archive_move_log_v1`. `superseded_by` is left NULL by default (no deterministic name-rule mapping); fill manually in a follow-up if a close-out names a specific replacement.

| detail_table_name | schema | renamed_by_script | superseded_by | live in main? | archive matches | move-log scripts |
|---|---|---|---|---|---|---|
| `_molecular_patient_rollup_v227` | `main` | `297` | NULL | no | — | `297_archive_stale_objects` |
| `extracted_braf_recovery_v1` | `main` | `346` | NULL | no | — | `346_archive_extracted_legacy` |
| `extracted_ete_subgraded_v1` | `main` | `346` | NULL | no | — | `346_archive_extracted_legacy` |
| `extracted_fna_bethesda_v1` | `main` | `346` | NULL | no | — | `346_archive_extracted_legacy` |
| `extracted_postop_labs_expanded_v1` | `main` | `346` | NULL | no | — | `346_archive_extracted_legacy` |
| `extracted_ras_patient_summary_v1` | `main` | `346` | NULL | no | — | `346_archive_extracted_legacy` |
| `frozen_section_event_v1` | `tier2` | `387` | NULL | no | — | `339_build_tier2_master_and_move_events`<br>`387_pub_v1_0_cleanup` |
| `ln_extract_noncohort_orphan_v279` | `manuscript_workspace` | `297` | NULL | no | — | `297_archive_stale_objects` |
| `path_size_adjudication_v241` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `patient_tier2_master_v1` | `tier2` | `387` | NULL | no | — | `387_pub_v1_0_cleanup` |
| `patient_tumor_rollup_v1` | `main` | `348` | NULL | no | — | `348_archive_older_masters` |
| `ret_note_entity_adjudication_v226` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `ret_patient_adjudicated_v226` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `tirads_llm_validation_v2` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `tirads_v2_reports_raw` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `tumor_pathology` | `main` | `325` | NULL | no | — | `325_archive_duplicates_round2` |
| `vc_paralysis_recalibration_v236` | `manuscript_workspace` | `297` | NULL | no | — | `297_archive_stale_objects` |

## Registry rows missing from `main` with NO archive snapshot (58)

These look retired on paper but have no archive evidence. Manual review — they may be `manuscript_workspace.*` audit rows, or they may be honest gaps that need a separate cleanup pass.

| detail_table_name | schema | move-log entries |
|---|---|---|
| `_molecular_patient_rollup_v227` | `main` | `297_archive_stale_objects` → `"Thyroid 2026 UPdated".archive_pub_v1_0."_molecular_patient_rollup_v227_pre297_20260421T040430Z"` |
| `analysis_molecular_subset_v1` | `main` | — |
| `build_pipeline` | `synthetic` | — |
| `canonical_molecular_tested_v1` | `main` | — |
| `cpm_ajcc_dominant_concordance_v1` | `manuscript_workspace` | — |
| `cpm_ajcc_dominant_discordance_canonical_v1` | `manuscript_workspace` | — |
| `cpm_ajcc_dominant_vs_tp_hist1_discordance_v1` | `manuscript_workspace` | — |
| `cpm_ete_self_contradiction_queue_v1` | `manuscript_workspace` | — |
| `cpm_hypopara_adjudication_log_v1` | `manuscript_workspace` | — |
| `cpm_is_malignant_flag_review_v1` | `manuscript_workspace` | — |
| `episode_analysis_resolved_v1_dedup` | `main` | — |
| `extracted_braf_recovery_v1` | `main` | `346_archive_extracted_legacy` → `"Thyroid 2026 UPdated".archive_pub_v1_0."extracted_braf_recovery_v1_pre346_20260421T125134Z"` |
| `extracted_ete_subgraded_v1` | `main` | `346_archive_extracted_legacy` → `"Thyroid 2026 UPdated".archive_pub_v1_0."extracted_ete_subgraded_v1_pre346_20260421T125134Z"` |
| `extracted_fna_bethesda_v1` | `main` | `346_archive_extracted_legacy` → `"Thyroid 2026 UPdated".archive_pub_v1_0."extracted_fna_bethesda_v1_pre346_20260421T125134Z"` |
| `extracted_postop_labs_expanded_v1` | `main` | `346_archive_extracted_legacy` → `"Thyroid 2026 UPdated".archive_pub_v1_0."extracted_postop_labs_expanded_v1_pre346_20260421T125134Z"` |
| `extracted_ras_patient_summary_v1` | `main` | `346_archive_extracted_legacy` → `"Thyroid 2026 UPdated".archive_pub_v1_0."extracted_ras_patient_summary_v1_pre346_20260421T125134Z"` |
| `frozen_section_event_v1` | `tier2` | `339_build_tier2_master_and_move_events` → `"Thyroid 2026 UPdated".archive_pub_v1_0."frozen_section_event_v1_preSCHEMAREORG_20260421T053651Z"`<br>`387_pub_v1_0_cleanup` → `"Thyroid 2026 UPdated"."tier2_legacy_20260422"."frozen_section_event_v1"` |
| `lesion_analysis_resolved_v1` | `main` | — |
| `ln_crossval_v1` | `main` | — |
| `ln_extract_noncohort_orphan_v279` | `manuscript_workspace` | `297_archive_stale_objects` → `"Thyroid 2026 UPdated".archive_pub_v1_0."ln_extract_noncohort_orphan_v279_pre297_20260421T040438Z"` |
| `ln_master_rollup_v1` | `main` | — |
| `molecular_assay_dictionary` | `main` | — |
| `molecular_code_crosswalk` | `main` | — |
| `molecular_ingestion_runs` | `main` | — |
| `molecular_results` | `main` | — |
| `molecular_test_episode_v2` | `main` | — |
| `molecular_testing` | `main` | — |
| `molecular_variant_long` | `main` | — |
| `nlp_rollup_promotion_audit_v1` | `manuscript_workspace` | — |
| `note_entities_genetics` | `main` | — |
| `note_entities_llm_functional_outcomes` | `main` | — |
| `note_entities_llm_labs` | `main` | — |
| `note_entities_llm_parathyroid_detail` | `main` | — |
| `note_entities_llm_patient_decision_adherence` | `main` | — |
| `note_entities_llm_physical_exam` | `main` | — |
| `note_entities_llm_rad_treatment` | `main` | — |
| `note_entities_llm_survival_followup` | `main` | — |
| `note_entities_llm_synoptic_pathology_enrichment` | `main` | — |
| `note_entities_llm_tg_kinetics` | `main` | — |
| `path_size_adjudication_v241` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."path_size_adjudication_v241_pre325_20260421T045712Z"` |
| `path_tumor_size_chart_review_queue_v1` | `manuscript_workspace` | — |
| `path_tumor_size_correction_queue_v1` | `manuscript_workspace` | — |
| `path_tumor_size_multifocal_enumeration_notes_v1` | `manuscript_workspace` | — |
| `patient_analysis_resolved_v1` | `main` | — |
| `patient_tier2_master_v1` | `tier2` | `387_pub_v1_0_cleanup` → `"Thyroid 2026 UPdated"."tier2_legacy_20260422"."patient_tier2_master_v1"` |
| `patient_tumor_rollup_v1` | `main` | `348_archive_older_masters` → `"Thyroid 2026 UPdated".archive_pub_v1_0."patient_tumor_rollup_v1_pre348_20260421T125715Z"` |
| `ret_note_entity_adjudication_v226` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."ret_note_entity_adjudication_v226_pre325_20260421T045713Z"` |
| `ret_patient_adjudicated_v226` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."ret_patient_adjudicated_v226_pre325_20260421T045714Z"` |
| `survival_cohort_enriched` | `main` | — |
| `tg_orphan_cancer_text_investigation_queue_v1` | `manuscript_workspace` | — |
| `thyroseq_molecular_enrichment` | `main` | — |
| `tirads_llm_haiku_vs_qwen_v1` | `manuscript_workspace` | — |
| `tirads_llm_validation_v2` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."tirads_llm_validation_v2_pre325_20260421T045716Z"` |
| `tirads_v2_reports_raw` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."tirads_v2_reports_raw_pre325_20260421T045715Z"` |
| `tumor_pathology` | `main` | `325_archive_duplicates_round2` → `"Thyroid 2026 UPdated".archive_pub_v1_0."tumor_pathology_pre325_20260421T045711Z"` |
| `ultrasound_reports` | `main` | — |
| `us_nodules_tirads` | `main` | — |
| `vc_paralysis_recalibration_v236` | `manuscript_workspace` | `297_archive_stale_objects` → `"Thyroid 2026 UPdated".archive_pub_v1_0."vc_paralysis_recalibration_v236_pre297_20260421T040443Z"` |

## Archive snapshots with no matching registry row (1)

Likely fine — these are objects that were archived but never lived in the registry (e.g. `__readme_pre*_backup`, view-DDL snapshots). Listed for completeness only.

| original_name | archive snapshots |
|---|---|
| `detail_table_registry_v1` | `detail_table_registry_v1_pre389_1_20260422T212806Z` |

## Archive table-name parse failures (0)

Tables whose name did not match either the `_pre<NNN>_<stamp>` or `_legacy_<stamp>` regex. Should be empty for a healthy archive.

_None._

