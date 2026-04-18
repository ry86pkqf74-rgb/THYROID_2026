# THYROID_2026 Finalization — 2026-04-18

Produced by Script 272.

## Final summary

```
================================================================
CANONICAL FINALIZATION COMPLETE — thyroid_canonical_publication_v1_0
================================================================
canonical_patient_master : 10871 × 1526 cols
Invariants               : RIDs=10871  null_rid=0  null_fna=0
main base tables         : 116
manuscript_workspace     : 42 tables + 67 views
detail_table_registry_v1 : 130 rows, 512 pass, 67 fail
Unresolved pointers      : 18 (all flagged needs_manual_review)
Archive destination      : "Thyroid 2026 UPdated".archive_pub_v1_0 (214 objs)
Data dictionary          : main.data_dictionary_v279 (sole, registered)
================================================================
```

> **`canonical_patient_master` in `thyroid_canonical_publication_v1_0` is the authoritative publication dataset.** It holds 10871 patients × 1526 columns. All invariants pass.

## Unresolved registry pointers (18)

All entries below are flagged `needs_manual_review=true` in `manuscript_workspace.detail_table_registry_v1`. They fall into two groups:

1. **Deferred oversized wildcards** — wildcards whose CPM expansion exceeds the 25-col safety limit. Per-pattern expansion CSVs are in `scripts/output/272_wildcard_oversized_*.csv` for review.
2. **Real registry curation gaps** — feed_col entries that don't match any CPM column even after the parser, prose-skip, and wildcard passes. These are typically stale pointers (e.g. column dropped by an earlier script), domain shorthand (HTN/obesity/FNA/dysphagia), or fragments of a parenthesized list.

| detail_table_name | feed_col |
|---|---|
| `build_pipeline` | `ajcc8_t_stage_corrected` |
| `build_pipeline` | `n_molecular_tests_v7` |
| `extracted_tirads_validated_v1` | `echogenicity` |
| `extracted_tirads_validated_v1` | `margin` |
| `extracted_tirads_validated_v1` | `shape` |
| `note_entities_complications` | `comp_* upstream` |
| `note_entities_llm_cervical_ln_detail` | `cnln_* columns` |
| `note_entities_llm_past_medical_hx` | `HTN` |
| `note_entities_llm_past_medical_hx` | `obesity` |
| `note_entities_llm_past_medical_hx` | `pmhx_nlp_* columns (diabetes` |
| `note_entities_llm_past_medical_hx` | `radiation` |
| `note_entities_llm_past_surgical_hx` | `FNA` |
| `note_entities_llm_presenting_symptoms` | `dysphagia` |
| `note_entities_operative_detail` | `op_nlp_* columns` |
| `note_entities_problem_list` | `pmhx_nlp_* columns (indirectly)` |
| `nsqip_patient_summary` | `nsqip_* columns (full set of 80+)` |
| `operative_episode_detail_v2` | `op_* columns` |
| `path_synoptics` | `syn_* columns (~50)` |

## Cohort filter_type classification (m032/m038 + 24 thin wrappers)

- **dedicated_filtered**: 37
- **dedicated_full_cohort**: 2
- **thin_wrapper**: 24

Thin-wrapper assignment used the data-based heuristic from the coworker's Prompt 21 §3 method (row count = 10,871 AND column set ⊆ `cohort_descriptive_full_cohort_v1`). All `thin_wrapper` rows are flagged `filter_type_provisional=true` for PI confirmation.

## Wildcard expansion ledger

See `scripts/output/272_wildcard_expansion_log.csv` for the full ledger.
`wildcard_expansion_whitelist_cnln` records the explicit human approval of `cnln_*` (36 cols).

## Phase 5 status

Skipped — `manuscript_workspace.canonical_cleanup_audit_v1` reports 0 objects with `has_version_twin=true` and 0 unreferenced objects. There is nothing to archive based on those signals. Dictionary predecessors (`v240` and `v266a`) are already inventoried in `scripts/output/272_archive_inventory.csv` (10 backup snapshots, 1,490 → 1,590 row growth tracing dictionary expansion).
