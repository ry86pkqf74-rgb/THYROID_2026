# CPM feeder audit — Script 361 (2026-04-22)

Read-only audit produced by Step 9 of Script 361. Identifies CPM `nlp_*` columns that may be sourced from one of the 7 deprecated operative-pathology tables. A follow-up script must repoint these feeders to the new canonical event/rollup tables.

**CPM total columns:** 1532 | **nlp_\* columns:** 118

## Per-table grep hits (`git grep -l <table> -- scripts/`)

| deprecated table | feeder script files |
|---|---|
| `canonical_tumor_characteristics_v1` | `scripts/245_canonical_tumor_characteristics.py`, `scripts/247_canonical_v1_0_lock.py`, `scripts/258_repoint_detail_pointer.py`, `scripts/266_preflight.py`, `scripts/266a_discovery.py`, `scripts/266b_per_tumor_ajcc_buildout.py`, `scripts/266c_wide_format_slots_and_renames.py`, `scripts/270b_phase_a_step_2_registry.py`, `scripts/361_op_path_consolidation.py`, `scripts/frozen/259_final_verification_lock.py` (+27 more) |
| `canonical_benign_diagnosis_v1` | `scripts/200_canonical_diagnosis_standardization.py`, `scripts/210_database_audit_backup.py`, `scripts/223_ingest_and_publish.py`, `scripts/223_publish_canonical.py`, `scripts/233_canonical_finalization.py`, `scripts/237_close_registry_gaps.py`, `scripts/239_fix_rai_benign_recovery.py`, `scripts/245_canonical_tumor_characteristics.py`, `scripts/361_op_path_consolidation.py`, `scripts/frozen/228_registry_backfill.py` (+22 more) |
| `canonical_malignant_diagnosis_v1` | `scripts/200_canonical_diagnosis_standardization.py`, `scripts/210_database_audit_backup.py`, `scripts/223_ingest_and_publish.py`, `scripts/223_publish_canonical.py`, `scripts/233_canonical_finalization.py`, `scripts/239_fix_rai_benign_recovery.py`, `scripts/245_canonical_tumor_characteristics.py`, `scripts/265_probe2.py`, `scripts/361_op_path_consolidation.py`, `scripts/frozen/228_registry_backfill.py` (+21 more) |
| `canonical_diagnosis_unified_v1` | `scripts/200_canonical_diagnosis_standardization.py`, `scripts/209_nlp_entity_crossvalidation.py`, `scripts/210_database_audit_backup.py`, `scripts/213_data_dictionary.py`, `scripts/223_ingest_and_publish.py`, `scripts/223_publish_canonical.py`, `scripts/233_canonical_finalization.py`, `scripts/245_canonical_tumor_characteristics.py`, `scripts/250_registry_pointer_rebuild.py`, `scripts/270c_divergent_reclassification_audit.py` (+27 more) |
| `tumor_episode_master_v2` | `scripts/100_canonical_metrics_registry.py`, `scripts/100_episode_linkage_v2_hardening.py`, `scripts/101_multi_episode_linkage_hardening.py`, `scripts/101_review_ops.py`, `scripts/103_fact_lineage_materialize.py`, `scripts/105_manuscript_freeze_v1.py`, `scripts/111_llm_extraction_validation.py`, `scripts/117_md_contract_views.py`, `scripts/118_parquet_release_bundle.py`, `scripts/128_multimodal_contract_mm_v1.py` (+94 more) |
| `synoptic_tumor_long_v1` | `scripts/108_synoptic_tumor_long_v1.py`, `scripts/109_synoptic_encounter_qc.py`, `scripts/119_md_formalization_validate.py`, `scripts/124_md_live_release_audit.py`, `scripts/126_final_master_release.py`, `scripts/138_md_specimen_fhir_layer.py`, `scripts/139_md_specimen_identity_layer.py`, `scripts/223_ingest_and_publish.py`, `scripts/223_publish_canonical.py`, `scripts/230_path_synoptic_rollup.py` (+56 more) |
| `path_outcome_classification_v1` | `scripts/115_path_outcome_classification.py`, `scripts/223_ingest_and_publish.py`, `scripts/223_publish_canonical.py`, `scripts/233_canonical_finalization.py`, `scripts/340_schema_reorg_audit.py`, `scripts/361_op_path_consolidation.py`, `scripts/frozen/228_registry_backfill.py`, `scripts/frozen/265_canonical_finalization.py`, `scripts/output/236_preflight_inventory.csv`, `scripts/output/250_run.log` (+14 more) |

## Likely CPM column ↔ deprecated source matches

| CPM column | likely feeder table | matched source column |
|---|---|---|
| (none) | | |
