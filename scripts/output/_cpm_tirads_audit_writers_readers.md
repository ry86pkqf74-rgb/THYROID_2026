# CPM TIRADS — Phase 0 writer/reader grep

Search dirs: scripts, sql, manuscripts, studies, lakehouse, utils, app | excluded: scripts/output, scripts/archive, .venv, __pycache__, leading-underscore scratch files

| column | writers (file count) | readers (file count) | writer files | reader files |
|---|---:|---:|---|---|
| `imaging_tirads_source` | 1 | 4 | 48_build_analysis_resolved_layer.py | 207_canonical_master_expansion.py, 48_build_analysis_resolved_layer.py, cpm_cols_pre.txt, schema_inventory.md |
| `max_tirads_ever` | 5 | 19 | 252_recompute_max_tirads.py, 265_canonical_finalization.py, 272_canonical_cleanup_phase1.py, 301_canonical_us_patient_master_v1.py, 50_multinodule_imaging.py | 207_canonical_master_expansion.py, 209_nlp_entity_crossvalidation.py, 228_registry_backfill.py, 246_canonical_us_nodule_characteristics.py, 252_recompute_max_tirads.py, 259_final_verification_lock.py, +13 more |
| `preop_tirads_best` | 5 | 5 | 204_canonical_master_assembly.py, 205_canonical_consolidation.py, 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 252_recompute_max_tirads.py | 207_canonical_master_expansion.py, 209_nlp_entity_crossvalidation.py, 252_recompute_max_tirads.py, cpm_cols_pre.txt, PLAN.md |
| `preop_tirads_category` | 2 | 3 | 204_canonical_master_assembly.py, 205_canonical_consolidation.py | 209_nlp_entity_crossvalidation.py, cpm_cols_pre.txt, schema_inventory.md |
| `preop_tirads_worst` | 2 | 1 | 204_canonical_master_assembly.py, 205_canonical_consolidation.py | cpm_cols_pre.txt |
| `tirads_best_category_v12` | 2 | 7 | 221_tirads_v2_integration.py, 265_canonical_finalization.py | 207_canonical_master_expansion.py, 209_nlp_entity_crossvalidation.py, 246_canonical_us_nodule_characteristics.py, 273_registry_curation.py, ajcc8_t_stage_view_migration_proposal.md, cpm_cols_pre.txt, +1 more |
| `tirads_best_combined` | 2 | 3 | 205_canonical_consolidation.py, 265_canonical_finalization.py | 213_data_dictionary.py, 228_registry_backfill.py, cpm_cols_pre.txt |
| `tirads_best_score_v12` | 4 | 13 | 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 265_canonical_finalization.py, 271_tirads_imaging_finalization.py | 207_canonical_master_expansion.py, 209_nlp_entity_crossvalidation.py, 228_registry_backfill.py, 246_canonical_us_nodule_characteristics.py, 265_canonical_finalization.py, 273_registry_curation.py, +7 more |
| `tirads_concordant_count_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_has_acr_recalc_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_mismatch_count_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_n_nodule_records_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_n_sources_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_nodule_size_max_mm_v12` | 0 | 5 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, ajcc8_t_stage_view_migration_proposal.md, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_nodules_scored_combined` | 1 | 1 | 205_canonical_consolidation.py | cpm_cols_pre.txt |
| `tirads_reliability_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_source_v12` | 0 | 4 | (none) | 207_canonical_master_expansion.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `tirads_worst_category_v12` | 2 | 9 | 221_tirads_v2_integration.py, 265_canonical_finalization.py | 207_canonical_master_expansion.py, 209_nlp_entity_crossvalidation.py, 246_canonical_us_nodule_characteristics.py, 252_recompute_max_tirads.py, 273_registry_curation.py, ajcc8_t_stage_view_migration_proposal.md, +3 more |
| `tirads_worst_combined` | 3 | 3 | 205_canonical_consolidation.py, 221_tirads_v2_integration.py, 265_canonical_finalization.py | 213_data_dictionary.py, 228_registry_backfill.py, cpm_cols_pre.txt |
| `tirads_worst_score_v12` | 1 | 5 | 271_tirads_imaging_finalization.py | 207_canonical_master_expansion.py, 265_canonical_finalization.py, 273_registry_curation.py, cpm_cols_pre.txt, schema_inventory.md |
| `worst_tirads_category` | 1 | 4 | 50_multinodule_imaging.py | 207_canonical_master_expansion.py, 213_data_dictionary.py, cpm_cols_pre.txt, schema_inventory.md |
| `imaging_tirads_best` | 4 | 6 | 204_canonical_master_assembly.py, 205_canonical_consolidation.py, 301_canonical_us_patient_master_v1.py, 48_build_analysis_resolved_layer.py | 207_canonical_master_expansion.py, 48_build_analysis_resolved_layer.py, 56_pre_manuscript_audit.py, 62_run_primary_descriptives.py, cpm_cols_pre.txt, schema_inventory.md |
| `imaging_updated_tirads_category_cpm_v1` | 0 | 1 | (none) | 375_cpm_column_cleanup_and_audit.py |
| `imaging_tirads_worst` | 5 | 8 | 204_canonical_master_assembly.py, 205_canonical_consolidation.py, 252_recompute_max_tirads.py, 301_canonical_us_patient_master_v1.py, 48_build_analysis_resolved_layer.py | 252_recompute_max_tirads.py, 48_build_analysis_resolved_layer.py, cpm_cols_pre.txt, explain_plan_01.txt, query_log.sql, run_feasibility.py, +2 more |
| `tirads_worst_points_v271` | 1 | 0 | 271_tirads_imaging_finalization.py | (none) |
| `tirads_best_points_v271` | 1 | 0 | 271_tirads_imaging_finalization.py | (none) |
| `tirads_source_system_v271` | 1 | 0 | 271_tirads_imaging_finalization.py | (none) |
| `imaging_laterality_rollup` | 3 | 0 | 271_tirads_imaging_finalization.py, 271a_fix_concordance_three_valued.py, 271b_laterality_normalization.py | (none) |
| `pathology_vs_imaging_laterality_concordant` | 3 | 0 | 271_tirads_imaging_finalization.py, 271a_fix_concordance_three_valued.py, 271b_laterality_normalization.py | (none) |
| `tumor_pathology_laterality_v271b` | 1 | 0 | 271b_laterality_normalization.py | (none) |
| `imaging_laterality_rollup_v271b` | 1 | 0 | 271b_laterality_normalization.py | (none) |
| `pathology_vs_imaging_laterality_concordant_v271b` | 1 | 0 | 271b_laterality_normalization.py | (none) |
| `tirads_v2_n_nodules_scored` | 3 | 1 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py | prompt6_353_repoint_orphan_view.py |
| `tirads_v2_worst_category` | 4 | 2 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 301_canonical_us_patient_master_v1.py, 328_tirads_v2_gap_a_cast_fix.py | 336_final_main_audit.py, prompt6_353_repoint_orphan_view.py |
| `tirads_v2_max_points` | 3 | 0 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py | (none) |
| `tirads_v2_largest_nodule_cm` | 3 | 0 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py | (none) |
| `tirads_v2_any_ete_on_us` | 4 | 0 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py, 329_tirads_v2_gap_b_report_reroll.py | (none) |
| `tirads_v2_any_interval_growth` | 4 | 0 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py, 329_tirads_v2_gap_b_report_reroll.py | (none) |
| `tirads_v2_any_fna_recommended` | 4 | 0 | 221_tirads_v2_integration.py, 280_synoptic_rollup_rebuild.py, 328_tirads_v2_gap_a_cast_fix.py, 329_tirads_v2_gap_b_report_reroll.py | (none) |
| `tirads_v2_n_reports` | 5 | 0 | 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 221c_rollup_threevalue_patch.py, 280_synoptic_rollup_rebuild.py, 329_tirads_v2_gap_b_report_reroll.py | (none) |
| `tirads_v2_any_suspicious_ln_on_us` | 5 | 1 | 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 221c_rollup_threevalue_patch.py, 280_synoptic_rollup_rebuild.py, 329_tirads_v2_gap_b_report_reroll.py | 336_final_main_audit.py |
| `tirads_v2_shortest_followup_months` | 5 | 0 | 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 221c_rollup_threevalue_patch.py, 280_synoptic_rollup_rebuild.py, 329_tirads_v2_gap_b_report_reroll.py | (none) |
| `tirads_v2_worst_rank` | 2 | 3 | 221_tirads_v2_integration.py, 328_tirads_v2_gap_a_cast_fix.py | prompt6_348_older_masters.py, prompt6_353_completion_audit.py, prompt6_353_repoint_orphan_view.py |
| `tirads_v2_worst_rank_source` | 0 | 1 | (none) | prompt6_348_older_masters.py |
| `tirads_v2_any_fna_recommended_report` | 4 | 2 | 221_tirads_v2_integration.py, 221b_suspicious_ln_reextraction.py, 221c_rollup_threevalue_patch.py, 329_tirads_v2_gap_b_report_reroll.py | prompt6_348_older_masters.py, prompt6_353_completion_audit.py |
| `tirads_v2_any_fna_recommended_report_source` | 0 | 1 | (none) | prompt6_348_older_masters.py |
| `imaging_tirads_best_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `imaging_tirads_worst_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `imaging_updated_tirads_category_cpm_v2` | 0 | 1 | (none) | 375_cpm_column_cleanup_and_audit.py |
| `imaging_laterality_rollup_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `max_tirads_ever_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `preop_tirads_best_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `preop_tirads_category_v2` | 1 | 1 | 368_cpm_us_cutover_to_v2.py | 369_us_v2_views_and_registry.py |
| `nlp_tirads_has_component_detail` | 2 | 1 | 212_nlp_entity_rollup.py, runpod_402_tirads_granular_qwen25_rerun.py | cpm_cols_pre.txt |
| `nlp_tirads_has_data` | 3 | 1 | 212_nlp_entity_rollup.py, 236_canonical_finalization.py, runpod_402_tirads_granular_qwen25_rerun.py | cpm_cols_pre.txt |
| `nlp_tirads_max_category` | 2 | 1 | 212_nlp_entity_rollup.py, runpod_402_tirads_granular_qwen25_rerun.py | cpm_cols_pre.txt |
| `nlp_tirads_n_entities` | 2 | 1 | 212_nlp_entity_rollup.py, runpod_402_tirads_granular_qwen25_rerun.py | cpm_cols_pre.txt |
| `nlp_tirads_n_notes` | 2 | 1 | 212_nlp_entity_rollup.py, runpod_402_tirads_granular_qwen25_rerun.py | cpm_cols_pre.txt |
