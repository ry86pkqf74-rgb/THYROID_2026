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

## Script 273 Addendum — Registry Curation & Metadata Fill (2026-04-18)

Produced by `scripts/273_registry_curation.py`.

```
================================================================
REGISTRY CURATION COMPLETE — thyroid_canonical_publication_v1_0
================================================================
canonical_patient_master : 10871 × 1526 cols (unchanged)
Invariants               : RIDs=10871  null_rid=0  null_fna=0
Registry rows            : 130 (unchanged)
Registry metadata gaps   : total_rows NULL=0, total_patients NULL=0
Unresolved pointers      : 0 (baseline 18, delta -18)
needs_manual_review=true : 0 rows (baseline 35, delta -35)
Thin-wrapper PI review   : 24 provisional (flagged for PI, not changed)
E2E validation           : 781 pass / 39 fail (baseline 512/67)
Archive destination      : "Thyroid 2026 UPdated".archive_pub_v1_0 (216 objs; +2 pre273 snapshots)
Schema counts            : main=116  ws=46t+67v
================================================================
```

### What changed

- **Phase 1** filled NULL `total_rows` (3) and NULL `total_patients` (8) in `manuscript_workspace.detail_table_registry_v1`. Catalog/crosswalk rows without `research_id` (`__readme`, `data_dictionary_v279`, `molecular_assay_dictionary`, `molecular_code_crosswalk`, `molecular_ingestion_runs`, `specimen_source_xref_v1`) get `total_patients = 0` plus a description note. Two queue tables (`tirads_reextraction_queue_v1`, `tumor_stage_heterogeneity_v1`) get the live `COUNT(DISTINCT research_id)`.
- **Phase 2** resolved all 18 unresolved pointers via explicit enumeration against `information_schema.columns` — no wildcard guessing:
  - `extracted_tirads_validated_v1` → 12 `tirads_*_v12` rollup columns (3 prose tokens replaced).
  - `note_entities_llm_past_medical_hx` → 59 `pmhx_nlp_*` columns enumerated (replaced 4 prose tokens HTN/obesity/radiation/etc.).
  - `note_entities_llm_past_surgical_hx` → 15 `pshx_nlp_*` primary + 4 `nlp_pshx_*` secondary.
  - `note_entities_llm_presenting_symptoms` → 5 `sx_nlp_*` primary + 4 `nlp_symptoms_*` secondary.
  - `note_entities_operative_detail` → 44 `op_nlp_*` columns.
  - `note_entities_problem_list` → upstream marker; secondary lists 59 `pmhx_nlp_*` (indirect contributor).
  - `note_entities_complications` → upstream marker (`comp_*` family owned by `complication_phenotype_v1`).
  - `note_entities_llm_cervical_ln_detail` → upstream marker; secondary lists 36 `cnln_*` cols.
  - `nsqip_patient_summary` → 102 `nsqip_*` columns enumerated.
  - `path_synoptics` → 41 `syn_*` enumerated as primary; non-syn secondary preserved.
  - `operative_episode_detail_v2` → 11 cross-surgery rollups (op_% \ op_nlp_% \ ops_%) as primary; 48 `ops_*` (op_sheet_data) + 44 `op_nlp_*` (note_entities_operative_detail) tagged in secondary.
  - `build_pipeline` → removed 2 stale tokens dropped by Script 267 (`n_molecular_tests_v7`, `ajcc8_t_stage_corrected`); other 185 tokens preserved.
- **Phase 3** built `manuscript_workspace.thin_wrapper_pi_review_v273` (24 rows) with row counts, column counts, and Jaccard overlap vs `cohort_descriptive_full_cohort_v1`. PI sign-off lives in the `pi_confirmation` column. Script 274 will act on the answers — no `filter_type_provisional` rows touched here.
- **Phase 4** rebuilt end-to-end registry validation as `manuscript_workspace.registry_end_to_end_validation_v273`. Pass count rose to **781** (vs 512) and fail count fell to **39** (vs 67) thanks to Phase 2 enumeration converting wildcards/prose into real CPM-resolvable pointers.
- **Phase 5** verified all CPM invariants still hold (10,871 × 1,526; 0 NULL RIDs; 0 NULL `fna_path_outcome`).

### Archive snapshots created

Both snapshots live in `"Thyroid 2026 UPdated".archive_pub_v1_0`:

- `detail_table_registry_v1_pre273_20260418T095652Z` — true pre-273 registry (`needs_manual_review`=35, `unresolved`=18). Authoritative pre-state.
- `detail_table_registry_v1_pre273_20260418T095858Z` — captured at the start of the second (clean) invocation. Useful only as additional audit trail.

### Permanent `needs_manual_review=true` (post-273)

**0 rows.** Every previously-flagged row was either resolved (real CPM enumeration), reclassified to upstream marker, or had its prose tokens replaced.

### Still flagged provisional (Script 274 input)

- `manuscript_dive_map_v1.filter_type_provisional = TRUE` on **24 thin_wrapper rows** (m048–m071, m076). These remain provisional pending PI confirmation in `manuscript_workspace.thin_wrapper_pi_review_v273`. Script 274 will un-set the flag after PI sign-off and reclassify any rejected rows to `dedicated_filtered`.

### New / refreshed manuscript_workspace objects

| Object | Rows | Purpose |
|---|---|---|
| `registry_v2_resolution_audit_v273` | 934 | Post-curation per-(detail_table, feed_col) audit |
| `registry_v2_unresolved_pointers_v273` | 0 | Subset where neither resolved nor auto-repaired |
| `registry_end_to_end_validation_v273` | 820 | E2E checks (table+rid+join+per-feed_col membership) |
| `thin_wrapper_pi_review_v273` | 24 | PI confirmation queue for provisional thin_wrappers |
