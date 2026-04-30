-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY

# mig_190 — smaller open-CF triage sweep

**Run ID:** `mig_190_smaller_cf_triage_sweep_20260430`  
**Batch:** `mig_190_smaller_cf_triage_sweep_20260430`  
**Run timestamp (UTC):** `2026-04-30T03:31:48.237110+00:00`  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Posture:** READ-ONLY scoping; SELECT-only against MotherDuck via `connect_locked()`; no registry/data DDL or DML.  
**Target DB:** `thyroid_canonical_publication_v1_0`  

## Executive summary

- Probed **11** mid-tier CF tags from the Cowork deep-probe list.
- Dispositions: **A/open=0**, **B/tag-only trace=7**, **C/closed-but-stale=4**.
- No disposition-A CFs were found; therefore no conditional mig_193 mini-lane prompt was authored by this sweep.
- All output artifacts are local files only; this lane intentionally does **not** append close-out notes to the registry.

## §1 Inventory of 11 CFs

| CF tag                                                  |   proposed_n_cols |   proposed_n_tables | proposed_tables                                                   |
|:--------------------------------------------------------|------------------:|--------------------:|:------------------------------------------------------------------|
| CF-mig58                                                |                17 |                   2 | canonical_parathyroid_patient_rollup_v1; canonical_patient_master |
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any |                17 |                   1 | canonical_patient_master                                          |
| CF-mig166-SCRIPT-ALL-TRUE                               |                15 |                   1 | canonical_cleanup_audit_v1                                        |
| CF-PMH-MULTISOURCE-DISAGREEMENT                         |                15 |                   1 | canonical_pmh_events_v1                                           |
| CF-mig156-N-                                            |                15 |                   1 | canonical_patient_master                                          |
| CF-mig145-CT-AIRWAY-COMMENT-PROXY                       |                15 |                   1 | canonical_patient_master                                          |
| CF-mig151-PROC-NLP-VS-CODES-GRAIN                       |                14 |                   1 | canonical_patient_master                                          |
| CF-mig156-ANY-RECURRENCE-                               |                13 |                   1 | canonical_patient_master                                          |
| CF-mig134-PM-LAB-DATE-ANCHOR                            |                13 |                   1 | canonical_patient_master                                          |
| CF-mig154-MARGIN-MM-VARCHAR-RETYPE                      |                12 |                   1 | canonical_patient_master                                          |
| CF-mig136-104-ontology                                  |                12 |                   1 | (probe) / canonical_patient_master                                |

## §2 Per-CF detail pages

### CF-mig58

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 02:11:33`
- **Existing batch_ids:** `mig_106_parathyroid_rollup_signoff_20260429,mig_151_patient_master_meds_radtx_proc_cluster_20260429`
- **Methods used:** `derivation_re_derivation_with_string_agg_ordering_artifact,derivation_vs_canonical_medications_events_v1_via_script215`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; string aggregation ordering artifact / trace-only
- **Notes excerpt:** | mig_151 meds/radtx/proc cluster (Lane 41). med_nlp_*: Script 215 SOURCE2 replay vs note_entities_medications present-only; list_sort note_types for STRING_AGG order-independence (CF-mig58). CF-mig151-MEDNLP-SPARSE-VS-ROLLUP: not canonical_medications_events tier. | mig_151b: CF-mig151-MED-METHODO
- **Disposition rationale:** Verified methods are real derivation probes. Notes document zero value-set drift for parathyroid strings and list_sort/list-set ordering handling for NLP medication note_types. Remaining CF records an ordering artifact, not unresolved data content.
- **Recommended action:** Leave tag for trace; optional future deterministic STRING_AGG rebuild only if presentation stability requires it.

<details><summary>Affected registry rows</summary>

| table_name                              | column_name                               | verification_status   | verification_method                                         | batch_id                                                |
|:----------------------------------------|:------------------------------------------|:----------------------|:------------------------------------------------------------|:--------------------------------------------------------|
| canonical_parathyroid_patient_rollup_v1 | autotransplant_locations                  | verified              | derivation_re_derivation_with_string_agg_ordering_artifact  | mig_106_parathyroid_rollup_signoff_20260429             |
| canonical_parathyroid_patient_rollup_v1 | parathyroid_pathologies                   | verified              | derivation_re_derivation_with_string_agg_ordering_artifact  | mig_106_parathyroid_rollup_signoff_20260429             |
| canonical_patient_master                | med_nlp_calcitriol                        | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcitriol_date                   | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcitriol_days_from_surg         | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcitriol_n_mentions             | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcium_supplement                | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcium_supplement_date           | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcium_supplement_days_from_surg | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_calcium_supplement_n_mentions     | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_extraction_method                 | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_levothyroxine                     | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_levothyroxine_date                | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_levothyroxine_days_from_surg      | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_levothyroxine_n_mentions          | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_n_source_notes                    | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master                | med_nlp_note_types                        | verified              | derivation_vs_canonical_medications_events_v1_via_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |

</details>

### CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any

- **Disposition:** `C`
- **Last verified_ts:** `2026-04-29 16:41:12`
- **Existing batch_ids:** `mig_156_patient_master_framework_provenance_cluster_20260429,mig_156b_framework_cleanup_20260429`
- **Methods used:** `helper_placeholder_pending_real_extraction_per_mig148b_pattern,internal_consistency_prm_rule_master_v1`
- **Statuses:** `na,verified`
- **Type-A / Type-B fit:** Type-B placeholder: yes for prm_high_risk_marker_any only; companion PRM rows verified
- **Notes excerpt:** | mig_156 framework cluster (156b PRM). Patient-rule-master rollups from canonical_fna_events_v1 spine for first/last FNA calendar dates + margins/recurrence facets; cf. CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any.
- **Disposition rationale:** The exact target column was reclassified verified→na by mig_156b with helper_placeholder_pending_real_extraction_per_mig148b_pattern after 0 TRUE / 10,305 FALSE / 566 NULL. The other 16 PRM columns have a real internal-consistency method.
- **Recommended action:** Safe-to-prune / append close-out note in apply lane: CLOSED by mig_156b; retain trace on PRM family if desired.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                       | verification_status   | verification_method                                            | batch_id                                                     |
|:-------------------------|:----------------------------------|:----------------------|:---------------------------------------------------------------|:-------------------------------------------------------------|
| canonical_patient_master | prm_first_fna_date                | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_first_fna_days_from_surg      | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_fna_n_sources                 | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_fna_source_tables             | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_high_risk_marker_any          | na                    | helper_placeholder_pending_real_extraction_per_mig148b_pattern | mig_156b_framework_cleanup_20260429                          |
| canonical_patient_master | prm_hypocalcemia_lab_flag         | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_hypoparathyroidism_lab_flag   | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_last_fna_date                 | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_last_fna_days_from_surg       | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_margin_confidence             | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_margin_source                 | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_molecular_risk_category       | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_n_recurrence_sources          | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_recurrence_detection_category | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_rln_worst_grade               | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_size_concordance              | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | prm_structural_disease_flag       | verified              | internal_consistency_prm_rule_master_v1                        | mig_156_patient_master_framework_provenance_cluster_20260429 |

</details>

### CF-mig166-SCRIPT-ALL-TRUE

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 20:57:57`
- **Existing batch_ids:** `mig_166_canonical_cleanup_audit_v1_signoff_20260429`
- **Methods used:** `derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A near-uniform/presence classifier flag: yes; documented governance inventory behavior
- **Notes excerpt:** | mig_165 auxiliary lane (165f-manuscript_workspace-governance-audit): mass auto-na classification (Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`). | mig_166 Lane 54: governance inventory col verified vs Script 272/266c classifier v2 + 275 phase-5 inventory;
- **Disposition rationale:** Rows are verified by derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit. The all-TRUE script-reference condition is an expected governance audit inventory signal, not a patient-data defect.
- **Recommended action:** Leave alone; retain as governance trace.

<details><summary>Affected registry rows</summary>

| table_name                 | column_name             | verification_status   | verification_method                                                     | batch_id                                            |
|:---------------------------|:------------------------|:----------------------|:------------------------------------------------------------------------|:----------------------------------------------------|
| canonical_cleanup_audit_v1 | classifier_version      | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | destination             | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | has_version_twin        | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | is_identical_to_twin    | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | is_referenced_by_script | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | is_referenced_by_view   | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | n_script_refs           | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | n_view_refs             | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | notes                   | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | object_name             | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | object_type             | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | reason                  | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | row_count               | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | status                  | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |
| canonical_cleanup_audit_v1 | twin_name               | verified              | derivation_vs_classifier_inventory_272_266c_275_canonical_cleanup_audit | mig_166_canonical_cleanup_audit_v1_signoff_20260429 |

</details>

### CF-PMH-MULTISOURCE-DISAGREEMENT

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-28 20:59:04`
- **Existing batch_ids:** `mig_107_pmh_events_signoff_20260428`
- **Methods used:** `extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; multisource provenance disagreement trace
- **Notes excerpt:** | mig_107: Protocol v2 PMH verification. Legacy source note_entities_problem_list was re-derived from archived source archive_pub_v1_0.note_entities_problem_list_pre365_20260422_064230; LLM source note_entities_llm_past_medical_hx was re-derived from parsed result_json entities using script 365 SQL
- **Disposition rationale:** mig_107 verified legacy+LLM PMH extraction faithfulness: 12,444 expected/canonical/joined, 0 missing, 0 extra, 0 mismatches; synthetic rows were verified-as-injected. CF documents the known legacy-vs-LLM source mixture.
- **Recommended action:** Leave alone; retain for manuscript methods/provenance appendix.

<details><summary>Affected registry rows</summary>

| table_name              | column_name                   | verification_status   | verification_method                                                  | batch_id                            |
|:------------------------|:------------------------------|:----------------------|:---------------------------------------------------------------------|:------------------------------------|
| canonical_pmh_events_v1 | anchor_source                 | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | days_from_first_thyroidectomy | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | evidence_span_hash            | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | evidence_strength             | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | extractor_name                | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | finding_date                  | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | finding_status                | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | finding_text                  | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | finding_value                 | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | finding_value_norm            | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | is_preexisting                | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | llm_confidence                | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | med_status                    | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | mention_note_date             | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |
| canonical_pmh_events_v1 | source_note_type              | verified              | extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic | mig_107_pmh_events_signoff_20260428 |

</details>

### CF-mig156-N-

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 16:40:09`
- **Existing batch_ids:** `mig_156_patient_master_framework_provenance_cluster_20260429`
- **Methods used:** `derivation_vs_canonical_upstream_counters_v1`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; count-rollup lineage trace
- **Notes excerpt:** | mig_156 framework cluster (156a N-counts). Per-feed COUNT aggregates + surgery-count lineage; v2 sibling cols vs legacy feeders per CF-mig156-N-*-V1-V2-* + FNA episode drift CF in header.
- **Disposition rationale:** All 15 rows are verified by derivation_vs_canonical_upstream_counters_v1. Notes describe per-feed COUNT aggregates, surgery-count lineage, v2 sibling comparisons, and FNA drift context.
- **Recommended action:** Leave alone; retain trace unless a later apply lane wants a closed-note suffix.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                        | verification_status   | verification_method                          | batch_id                                                     |
|:-------------------------|:-----------------------------------|:----------------------|:---------------------------------------------|:-------------------------------------------------------------|
| canonical_patient_master | n_confirmed_complications          | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_fna_cytology_records             | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_fna_episodes                     | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_notes_documenting_tsh_suppressed | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_stimulated_tg_measurements       | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_surgeries                        | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_surgeries_source                 | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_surgeries_v2                     | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_tg_measurements_structured       | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_tgab_measurements                | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_us_exams                         | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_us_exams_v2                      | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_us_nodules_total                 | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_us_nodules_total_v2              | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | n_us_with_ln_assessment            | verified              | derivation_vs_canonical_upstream_counters_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |

</details>

### CF-mig145-CT-AIRWAY-COMMENT-PROXY

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 05:45:13`
- **Existing batch_ids:** `mig_145_patient_master_ct_imaging_cluster_20260429`
- **Methods used:** `derivation_vs_canonical_ct_imaging_v1_corrected_enum_filter,patient_level_aggregate_ct_per_exam`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A sparse presence flags: partial; CT rollup NULL semantics / proxy limitation
- **Notes excerpt:** | mig_145 CT cluster (Lane 35). Patient-level BOOL_OR / presence rules over structured `ct_imaging` fields replayed against CPM (`pathologic_lymph_nodes` flag for ct_pathologic_ln_any). Uniformity quirks: CF-mig145-CT-AIRWAY-COMMENT-PROXY, CF-mig145-CT-TRACHEAL-NOTMENTIONED-OVERREACH (see migration
- **Disposition rationale:** CT patient-level rollups are verified against canonical_ct_imaging_v1, with mig_145b correcting the related tracheal-not-mentioned overreach. The remaining airway-comment proxy tag describes source-field semantics and CT rollup NULL behavior rather than a pending fix.
- **Recommended action:** Leave tag for trace; cite as CT-source proxy limitation if airway fields are used in methods.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                      | verification_status   | verification_method                                         | batch_id                                           |
|:-------------------------|:---------------------------------|:----------------------|:------------------------------------------------------------|:---------------------------------------------------|
| canonical_patient_master | ct_airway_compromise_any         | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_goiter_present_any            | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_ln_enlarged_any               | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_ln_suspicious_any             | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_pathologic_ln_any             | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_substernal_extension_any      | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_enlarged_any          | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_heterogeneous_any     | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_nodule_any            | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_normal_any            | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_not_visualized_any    | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_other_abnormality_any | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_thyroid_postsurgical_any      | verified              | patient_level_aggregate_ct_per_exam                         | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_tracheal_deviation_any        | verified              | derivation_vs_canonical_ct_imaging_v1_corrected_enum_filter | mig_145_patient_master_ct_imaging_cluster_20260429 |
| canonical_patient_master | ct_tracheal_narrowing_any        | verified              | derivation_vs_canonical_ct_imaging_v1_corrected_enum_filter | mig_145_patient_master_ct_imaging_cluster_20260429 |

</details>

### CF-mig151-PROC-NLP-VS-CODES-GRAIN

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 02:11:33`
- **Existing batch_ids:** `mig_151_patient_master_meds_radtx_proc_cluster_20260429`
- **Methods used:** `extraction_faithfulness_vs_note_entities_procedures_script215`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A sparse presence flags: partial; grain mismatch trace
- **Notes excerpt:** | mig_151 proc_nlp_* (Lane 41). Regex patient rollup vs note_entities_procedures; proc_nlp_lateral_neck_dissection verified separately; canonical_operative_procedure_codes_v1 = mention+linkage grain (CF-mig151-PROC-NLP-VS-CODES-GRAIN).
- **Disposition rationale:** All 14 proc_nlp_* columns are verified by extraction faithfulness against note_entities_procedures/script215. Notes explicitly state that canonical_operative_procedure_codes_v1 is mention+linkage grain and not the same object as the NLP patient rollup.
- **Recommended action:** Leave alone; retain for methods appendix on NLP-vs-code grain.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                          | verification_status   | verification_method                                           | batch_id                                                |
|:-------------------------|:-------------------------------------|:----------------------|:--------------------------------------------------------------|:--------------------------------------------------------|
| canonical_patient_master | proc_nlp_extraction_method           | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_laryngoscopy                | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_laryngoscopy_date           | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_laryngoscopy_days_from_surg | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_laryngoscopy_n_mentions     | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_mrnd                        | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_mrnd_n_mentions             | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_n_source_notes              | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_note_types                  | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_parathyroid_autotransplant  | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_tracheostomy                | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_tracheostomy_date           | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_tracheostomy_days_from_surg | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |
| canonical_patient_master | proc_nlp_tracheostomy_n_mentions     | verified              | extraction_faithfulness_vs_note_entities_procedures_script215 | mig_151_patient_master_meds_radtx_proc_cluster_20260429 |

</details>

### CF-mig156-ANY-RECURRENCE-

- **Disposition:** `C`
- **Last verified_ts:** `2026-04-29 16:40:23`
- **Existing batch_ids:** `mig_156_patient_master_framework_provenance_cluster_20260429`
- **Methods used:** `cross_domain_aggregation_any_overlap_rules_v1`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; already addressed by later recurrence/invasion apply lanes
- **Notes excerpt:** | mig_156 framework cluster (156d ANY-*). BOOL_OR / union semantics across path / invasion / molecular / complication feeds; capsular naive probe <=50 drift; recurrence OR wider than canonical_recurrence_v1 (CF-mig156-ANY-RECURRENCE-*).
- **Disposition rationale:** Rows are verified with cross_domain_aggregation_any_overlap_rules_v1. Cowork prior report records the specific CF-mig156-ANY-RECURRENCE undercount closure by mig_163b; several sibling ANY fields also carry later mig_177b/mig_179 notes. Current tag is stale trace on the broad ANY-family.
- **Recommended action:** Safe-to-prune / append close-out note in apply lane: CLOSED by mig_163b for recurrence-specific undercount; retain broad ANY-family trace where non-recurrence columns still need provenance.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                           | verification_status   | verification_method                           | batch_id                                                     |
|:-------------------------|:--------------------------------------|:----------------------|:----------------------------------------------|:-------------------------------------------------------------|
| canonical_patient_master | any_airway_anywhere                   | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_analysis_eligible_complication    | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_capsular_anywhere                 | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_confirmed_complication            | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_confirmed_complication_flag       | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_disease_concern_flag              | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_fusion_positive                   | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_fusion_positive_inferred_negative | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_lymphatic_microscopic_anywhere    | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_perineural_anywhere               | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_recurrence_flag                   | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_soft_tissue_anywhere              | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |
| canonical_patient_master | any_vascular_microscopic_anywhere     | verified              | cross_domain_aggregation_any_overlap_rules_v1 | mig_156_patient_master_framework_provenance_cluster_20260429 |

</details>

### CF-mig134-PM-LAB-DATE-ANCHOR

- **Disposition:** `C`
- **Last verified_ts:** `2026-04-29 00:08:49`
- **Existing batch_ids:** `mig_134_patient_master_labs_cluster_20260429`
- **Methods used:** `derivation_canonical_labs_rollups_mig115_script347,derivation_script224_biochemical_concern_tier3_helper`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; date-anchor/retype stale tag
- **Notes excerpt:** | mig_134 labs cluster (Lane 25). lab_* aggregates vs mig_115 verified per-analyte canonicals + Script 347 longitudinal_lab_VIEW_v1 union; NULL-safe count semantics. | CF-mig134-PM-LAB-DATE-ANCHOR: derived *_first_date/*_last_date are DATE on CPM; joins vs TIMESTAMP lab_datetime use CAST/ DATE_TRUN
- **Disposition rationale:** Rows are verified by canonical lab rollups/script347 or Script224 biochemical helper. Notes already specify DATE-on-CPM vs TIMESTAMP lab source comparisons using CAST/DATE_TRUNC; cowork backlog says mig_160 closes this date-retype/anchor family.
- **Recommended action:** Safe-to-prune / append close-out note in apply lane: CLOSED by mig_160 date-retype family; retain methods caveat for DATE-vs-TIMESTAMP comparisons.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                    | verification_status   | verification_method                                   | batch_id                                     |
|:-------------------------|:-------------------------------|:----------------------|:------------------------------------------------------|:---------------------------------------------|
| canonical_patient_master | biochemical_concern_first_date | verified              | derivation_script224_biochemical_concern_tier3_helper | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_calcium_first_date         | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_calcium_last_date          | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_calcium_most_recent_date   | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_pth_first_date             | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_pth_last_date              | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_pth_most_recent_date       | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_tsh_first_date             | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_tsh_last_date              | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_tsh_most_recent_date       | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_vitd_first_date            | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_vitd_last_date             | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |
| canonical_patient_master | lab_vitd_most_recent_date      | verified              | derivation_canonical_labs_rollups_mig115_script347    | mig_134_patient_master_labs_cluster_20260429 |

</details>

### CF-mig154-MARGIN-MM-VARCHAR-RETYPE

- **Disposition:** `C`
- **Last verified_ts:** `2026-04-29 16:39:02`
- **Existing batch_ids:** `mig_154_patient_master_pathology_invasion_cluster_20260429`
- **Methods used:** `derivation_vs_canonical_path_malignant_events_v1`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A/Type-B: no; explicitly closed retype
- **Notes excerpt:** | mig_154 pathology-invasion (154e margin). Ordinal worst + R-class + margin_status_final resolver; DOUBLE mm triple from synoptic distance feed (CF-mig154-MARGIN-MM-VARCHAR-RETYPE CLEAR). Spot rids 5817/4279/8191/1062/6555.
- **Disposition rationale:** Notes explicitly say CF-mig154-MARGIN-MM-VARCHAR-RETYPE CLEAR. The verified method replays margin ordinal/R-class/status and DOUBLE-mm triples from canonical_path_malignant_events_v1.
- **Recommended action:** Safe-to-prune / append close-out note in apply lane: CLOSED/CLEAR by mig_154.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                | verification_status   | verification_method                              | batch_id                                                   |
|:-------------------------|:---------------------------|:----------------------|:-------------------------------------------------|:-----------------------------------------------------------|
| canonical_patient_master | closest_margin_mm          | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | closest_margin_mm_max      | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | closest_margin_mm_min      | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_all_uninvolved      | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_involved_any        | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_ord_worst           | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_r_class_v10         | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_r_classification    | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_status              | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_status_final        | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_status_final_source | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |
| canonical_patient_master | margin_status_true         | verified              | derivation_vs_canonical_path_malignant_events_v1 | mig_154_patient_master_pathology_invasion_cluster_20260429 |

</details>

### CF-mig136-104-ontology

- **Disposition:** `B`
- **Last verified_ts:** `2026-04-29 00:35:40`
- **Existing batch_ids:** `mig_136_patient_master_pmh_psh_cluster_20260429`
- **Methods used:** `extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215`
- **Statuses:** `verified`
- **Type-A / Type-B fit:** Type-A sparse presence flags: partial; ontology/grain mismatch trace
- **Notes excerpt:** | mig_136 PMH+PSH cluster (Lane 26). Replay from note_entities_llm_past_surgical_hx per 215 thresholds; STRING_AGG parity list_sort(note_types); not 1:1 with canonical_psh_events_v1 (mig_104) ontology vs LLM entities — see CF-mig136-104-ontology. | CF-mig136-104-ONTOLOGY: pshx_nlp_* LLM entity aggr
- **Disposition rationale:** All 12 PSH NLP rows are verified by extraction faithfulness against note_entities_llm_past_surgical_hx/script215. Notes explicitly state pshx_nlp_* LLM entity aggregates are not the same vocabulary object as canonical_psh_events_v1 / mig_104 ontology; comparisons are informational.
- **Recommended action:** Leave alone; retain trace for NLP ontology/grain appendix.

<details><summary>Affected registry rows</summary>

| table_name               | column_name                                 | verification_status   | verification_method                                                     | batch_id                                        |
|:-------------------------|:--------------------------------------------|:----------------------|:------------------------------------------------------------------------|:------------------------------------------------|
| canonical_patient_master | pshx_nlp_prior_fna                          | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_fna_n_mentions               | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_neck_dissection              | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_parathyroidectomy            | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_rai                          | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_rai_date                     | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_rai_days_from_surg           | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_rai_n_mentions               | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_thyroidectomy                | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_thyroidectomy_date           | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_thyroidectomy_days_from_surg | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |
| canonical_patient_master | pshx_nlp_prior_thyroidectomy_n_mentions     | verified              | extraction_faithfulness_vs_note_entities_llm_past_surgical_hx_script215 | mig_136_patient_master_pmh_psh_cluster_20260429 |

</details>

## §3 Summary triage table

| cf                                                      | disposition   |   n_cols |   n_tables | statuses    | latest_verified     | pattern_fit                                                                                 | recommended_action                                                                                                                                                                            |
|:--------------------------------------------------------|:--------------|---------:|-----------:|:------------|:--------------------|:--------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CF-mig58                                                | B             |       17 |          2 | verified    | 2026-04-29 02:11:33 | Type-A/Type-B: no; string aggregation ordering artifact / trace-only                        | Leave tag for trace; optional future deterministic STRING_AGG rebuild only if presentation stability requires it.                                                                             |
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | C             |       17 |          1 | na,verified | 2026-04-29 16:41:12 | Type-B placeholder: yes for prm_high_risk_marker_any only; companion PRM rows verified      | Safe-to-prune / append close-out note in apply lane: CLOSED by mig_156b; retain trace on PRM family if desired.                                                                               |
| CF-mig166-SCRIPT-ALL-TRUE                               | B             |       15 |          1 | verified    | 2026-04-29 20:57:57 | Type-A near-uniform/presence classifier flag: yes; documented governance inventory behavior | Leave alone; retain as governance trace.                                                                                                                                                      |
| CF-PMH-MULTISOURCE-DISAGREEMENT                         | B             |       15 |          1 | verified    | 2026-04-28 20:59:04 | Type-A/Type-B: no; multisource provenance disagreement trace                                | Leave alone; retain for manuscript methods/provenance appendix.                                                                                                                               |
| CF-mig156-N-                                            | B             |       15 |          1 | verified    | 2026-04-29 16:40:09 | Type-A/Type-B: no; count-rollup lineage trace                                               | Leave alone; retain trace unless a later apply lane wants a closed-note suffix.                                                                                                               |
| CF-mig145-CT-AIRWAY-COMMENT-PROXY                       | B             |       15 |          1 | verified    | 2026-04-29 05:45:13 | Type-A sparse presence flags: partial; CT rollup NULL semantics / proxy limitation          | Leave tag for trace; cite as CT-source proxy limitation if airway fields are used in methods.                                                                                                 |
| CF-mig151-PROC-NLP-VS-CODES-GRAIN                       | B             |       14 |          1 | verified    | 2026-04-29 02:11:33 | Type-A sparse presence flags: partial; grain mismatch trace                                 | Leave alone; retain for methods appendix on NLP-vs-code grain.                                                                                                                                |
| CF-mig156-ANY-RECURRENCE-                               | C             |       13 |          1 | verified    | 2026-04-29 16:40:23 | Type-A/Type-B: no; already addressed by later recurrence/invasion apply lanes               | Safe-to-prune / append close-out note in apply lane: CLOSED by mig_163b for recurrence-specific undercount; retain broad ANY-family trace where non-recurrence columns still need provenance. |
| CF-mig134-PM-LAB-DATE-ANCHOR                            | C             |       13 |          1 | verified    | 2026-04-29 00:08:49 | Type-A/Type-B: no; date-anchor/retype stale tag                                             | Safe-to-prune / append close-out note in apply lane: CLOSED by mig_160 date-retype family; retain methods caveat for DATE-vs-TIMESTAMP comparisons.                                           |
| CF-mig154-MARGIN-MM-VARCHAR-RETYPE                      | C             |       12 |          1 | verified    | 2026-04-29 16:39:02 | Type-A/Type-B: no; explicitly closed retype                                                 | Safe-to-prune / append close-out note in apply lane: CLOSED/CLEAR by mig_154.                                                                                                                 |
| CF-mig136-104-ontology                                  | B             |       12 |          1 | verified    | 2026-04-29 00:35:40 | Type-A sparse presence flags: partial; ontology/grain mismatch trace                        | Leave alone; retain trace for NLP ontology/grain appendix.                                                                                                                                    |

### Disposition legend

- **A genuinely open:** pending/TBD methods or notes describing unresolved data work; would need focused mini-lane.
- **B tag-only / retain for trace:** real method and verified status; note documents source limitation, grain mismatch, proxy semantics, or trace-only concern.
- **C already-closed-but-stale:** later migration or note already cleared the issue; safe for a later apply lane to append a CLOSED suffix while retaining history.

## §4 Manuscript appendix candidate list

| cf                                | disposition   | appendix_pattern                                                                                                                                                                | source_tables                                                    |
|:----------------------------------|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------|
| CF-mig58                          | B             | Non-deterministic STRING_AGG ordering was audited with set-equality and handled by list sorting; values were not changed.                                                       | canonical_parathyroid_patient_rollup_v1,canonical_patient_master |
| CF-PMH-MULTISOURCE-DISAGREEMENT   | B             | PMH events were assembled from archived legacy problem-list entities plus LLM PMH entities; multisource disagreements were documented but extraction faithfulness was verified. | canonical_pmh_events_v1                                          |
| CF-mig145-CT-AIRWAY-COMMENT-PROXY | B             | CT airway compromise and tracheal flags rely on structured CT-imaging proxy fields; null means no CT rollup, and proxy limitations were audited.                                | canonical_patient_master                                         |
| CF-mig151-PROC-NLP-VS-CODES-GRAIN | B             | Procedure NLP patient rollups and operative procedure code/linkage tables are different grains; comparisons are interpretive, not direct row parity.                            | canonical_patient_master                                         |
| CF-mig156-ANY-RECURRENCE-         | C             | Any-recurrence used cross-domain/hybrid union semantics; the canonical-only undercount was closed by a later recurrence apply lane.                                             | canonical_patient_master                                         |
| CF-mig134-PM-LAB-DATE-ANCHOR      | C             | Lab date rollups use calendar DATE comparisons from TIMESTAMP lab sources via CAST/DATE_TRUNC; non-Tg lab date fidelity remains a methods caveat when relevant.                 | canonical_patient_master                                         |
| CF-mig136-104-ontology            | B             | Past surgical history NLP aggregates and canonical PSH event ontology are distinct vocabularies/grains; direct parity is not expected.                                          | canonical_patient_master                                         |

## §5 Mini-lane authoring queue

No disposition-A CFs were identified. No `CURSOR_PROMPT_mig193_*` files were authored in this lane.

## §6 Deliverables

- `qc_framework_v1/reports/mig_190_smaller_cf_triage_sweep_20260430.md`
- `exports/mig190_cf_triage_20260430/per_cf_inventory.csv`
- `exports/mig190_cf_triage_20260430/manuscript_appendix_candidates.csv`
- `exports/mig190_cf_triage_20260430/manifest.json`

