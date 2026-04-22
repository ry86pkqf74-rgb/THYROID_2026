# CPM TIRADS audit — Part A classification report

**Database:** `thyroid_canonical_publication_v1_0`
**Table:** `main.canonical_patient_master` (10,871 rows)
**Run mode:** read-only audit (no schema mutations)

## Headline findings

1. **No paired (legacy, v2) column on CPM is more than 84% in agreement** under type-coerced equality. The closest match is `worst_tirads_category` ↔ `tirads_v2_worst_category` at 83.7% (2,358/2,817 rows agree).
2. **Several "obvious pairs" disagree on >70% of overlapping rows**. The legacy and v2 derivations are not interchangeable; they encode different aggregation logic, different upstream sources, or different vocabularies.
3. **`_v12` columns are still actively read by 8 cohort views in `manuscript_workspace`** (see `cohort_descriptive_full_cohort_v1`, `cohort_m011_*`, `cohort_m025_*`, `cohort_m045_*`, `cohort_m075_*`, `cohort_m076_*`). No `_v12` column can be dropped before those views are migrated.
4. **Genuine semantic mismatches** confirmed by column comments and value samples: `max_tirads_ever` (BIGINT category) vs `max_tirads_ever_v2` (DOUBLE points); `pathology_vs_imaging_laterality_concordant` (BOOLEAN) vs `_v271b` (5-valued VARCHAR); `tirads_source_v12` (`excel_complete_structured`) vs `tirads_source_system_v271` (`cunc_v1_points_acr2017`) — these label different pipelines, not the same thing.
5. **No clean auto-DROPs**. The audit emits 0 confident DROPs. Every legacy column needs Logan's sign-off before Part B; the heuristic surfaces them as PRESERVE_DIFFERENT_SEMANTIC or INVESTIGATE.

## Summary counts

| recommendation | n |
|---|---:|
| DROP | 0 |
| RENAME_TO_V2 | 0 |
| PRESERVE_DIFFERENT_SEMANTIC | 18 |
| INVESTIGATE | 14 |
| **TOTAL** | **32** |

## Full classification table

| col | type | v2 counterpart | v2 type | n_leg | n_v2 | n_both | n_agree | n_disagree | n_leg_only | n_v2_only | reco | rationale |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `imaging_laterality_rollup_v271b` | VARCHAR | `imaging_laterality_rollup_v2` | VARCHAR | 3439 | 3439 | 3439 | 2342 | 1097 | 0 | 0 | **INVESTIGATE** | Middle-zone agreement 68.1% on 3439 rows — partial backfill, stale data, or logic change suspected. |
| `imaging_tirads_source` | VARCHAR | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart and 4 script reader(s). VARCHAR source system label; possibly redundant with tirads_source_system_v271 — flagged |
| `preop_tirads_worst` | BIGINT | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart and 1 script reader(s). no v2 counterpart (only preop_tirads_best_v2 exists) |
| `tirads_best_points_v271` | DOUBLE | — | — | 1326 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart. no v2 best_points — would need MIN aggregate from canonical_us_nodule_v2 |
| `tirads_concordant_count_v12` | BIGINT | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired (per-nodule acr2017_vs_updated_concordant on canonical_us_nodule_v2) |
| `tirads_has_acr_recalc_v12` | BOOLEAN | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart and 4 script reader(s). no v2 — concept retired |
| `tirads_mismatch_count_v12` | BIGINT | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired |
| `tirads_n_nodule_records_v12` | BIGINT | `tirads_v2_n_nodules_scored` | BIGINT | 3474 | 2465 | 2463 | 1250 | 1213 | 1011 | 2 | **INVESTIGATE** | Middle-zone agreement 50.8% on 2463 rows — partial backfill, stale data, or logic change suspected. |
| `tirads_n_sources_v12` | BIGINT | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired |
| `tirads_nodules_scored_combined` | BIGINT | `tirads_v2_n_nodules_scored` | BIGINT | 3439 | 2465 | 2463 | 1342 | 1121 | 976 | 2 | **INVESTIGATE** | Middle-zone agreement 54.5% on 2463 rows — partial backfill, stale data, or logic change suspected. |
| `tirads_reliability_v12` | DOUBLE | — | — | 3474 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired (no reliability score in v2) |
| `tirads_source_system_v271` | VARCHAR | — | — | 1326 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart. label-only column for v271 points; superseded if v2 path becomes canonical |
| `tumor_pathology_laterality_v271b` | VARCHAR | — | — | 3986 | <NA> | <NA> | <NA> | <NA> | <NA> | <NA> | **INVESTIGATE** | No v2 counterpart. newer name; no _v2 — keep (and consider rename to drop _v271b suffix once stabilized) |
| `worst_tirads_category` | VARCHAR | `tirads_v2_worst_category` | VARCHAR | 3439 | 2819 | 2817 | 2358 | 459 | 622 | 2 | **INVESTIGATE** | Middle-zone agreement 83.7% on 2817 rows — partial backfill, stale data, or logic change suspected. |
| `imaging_laterality_rollup` | VARCHAR | `imaging_laterality_rollup_v271b` | VARCHAR | 3439 | 3439 | 3439 | 1197 | 2242 | 0 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 34.8% agreement — pair likely measures different things despite naming. |
| `imaging_tirads_best` | BIGINT | `imaging_tirads_best_v2` | VARCHAR | 3474 | 1226 | 1226 | 240 | 986 | 2248 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 19.6% agreement — pair likely measures different things despite naming. |
| `imaging_tirads_worst` | BIGINT | `imaging_tirads_worst_v2` | VARCHAR | 3474 | 1226 | 1226 | 279 | 947 | 2248 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 22.8% agreement — pair likely measures different things despite naming. |
| `imaging_updated_tirads_category_cpm_v1` | VARCHAR | `imaging_updated_tirads_category_cpm_v2` | VARCHAR | 3474 | 1226 | 1226 | 363 | 863 | 2248 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 29.6% agreement — pair likely measures different things despite naming. |
| `max_tirads_ever` | BIGINT | `max_tirads_ever_v2` | DOUBLE | 3439 | 1300 | 1300 | <NA> | <NA> | 2139 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Pair has different semantics: BIGINT category code 1-5 vs DOUBLE points (0-13+). Different semantics — DO NOT compare. Both should be kept under clarifying names.. Recommend renaming pair to clarifying names. |
| `pathology_vs_imaging_laterality_concordant` | BOOLEAN | `pathology_vs_imaging_laterality_concordant_v271b` | VARCHAR | 3364 | 10871 | 3364 | <NA> | <NA> | 0 | 7507 | **PRESERVE_DIFFERENT_SEMANTIC** | Pair has different semantics: legacy BOOLEAN; v271b is multi-valued VARCHAR — type mismatch, see _v271b row. Recommend renaming pair to clarifying names. |
| `pathology_vs_imaging_laterality_concordant_v271b` | VARCHAR | `pathology_vs_imaging_laterality_concordant` | BOOLEAN | 10871 | 3364 | 3364 | 920 | 2444 | 7507 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 27.3% agreement — pair likely measures different things despite naming. |
| `preop_tirads_best` | BIGINT | `preop_tirads_best_v2` | VARCHAR | 3474 | 1043 | 1043 | 267 | 776 | 2431 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 25.6% agreement — pair likely measures different things despite naming. |
| `preop_tirads_category` | VARCHAR | `preop_tirads_category_v2` | VARCHAR | 3474 | 1043 | 1043 | 319 | 724 | 2431 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 30.6% agreement — pair likely measures different things despite naming. |
| `tirads_best_category_v12` | VARCHAR | `imaging_tirads_best_v2` | VARCHAR | 3474 | 1226 | 1226 | 240 | 986 | 2248 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 19.6% agreement — pair likely measures different things despite naming. |
| `tirads_best_combined` | INTEGER | `imaging_tirads_best_v2` | VARCHAR | 3439 | 1226 | 1226 | 588 | 638 | 2213 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 48.0% agreement — pair likely measures different things despite naming. |
| `tirads_best_score_v12` | BIGINT | `tirads_v2_worst_category` | VARCHAR | 3474 | 2819 | 2817 | <NA> | <NA> | 657 | 2 | **PRESERVE_DIFFERENT_SEMANTIC** | Pair has different semantics: v12.score = MIN tirads_acr_recalculated (category code 1-5); v2 has only worst_category not best — semantic mismatch. Recommend renaming pair to clarifying names. |
| `tirads_nodule_size_max_mm_v12` | DOUBLE | `tirads_v2_largest_nodule_cm` | DOUBLE | 3439 | 2440 | 2438 | 48 | 2390 | 1001 | 2 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 2.0% agreement — pair likely measures different things despite naming. |
| `tirads_source_v12` | VARCHAR | `tirads_source_system_v271` | VARCHAR | 3474 | 1326 | 1326 | 0 | 1326 | 2148 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 0.0% agreement — pair likely measures different things despite naming. |
| `tirads_worst_category_v12` | VARCHAR | `imaging_tirads_worst_v2` | VARCHAR | 3474 | 1226 | 1226 | 279 | 947 | 2248 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 22.8% agreement — pair likely measures different things despite naming. |
| `tirads_worst_combined` | INTEGER | `imaging_tirads_worst_v2` | VARCHAR | 3439 | 1226 | 1226 | 325 | 901 | 2213 | 0 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 26.5% agreement — pair likely measures different things despite naming. |
| `tirads_worst_points_v271` | DOUBLE | `tirads_v2_max_points` | DOUBLE | 1326 | 1357 | 459 | 127 | 332 | 867 | 898 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 27.7% agreement — pair likely measures different things despite naming. |
| `tirads_worst_score_v12` | BIGINT | `tirads_v2_worst_category` | VARCHAR | 3474 | 2819 | 2817 | 1276 | 1541 | 657 | 2 | **PRESERVE_DIFFERENT_SEMANTIC** | Only 45.3% agreement — pair likely measures different things despite naming. |

## Hitlists by bucket

### DROP (0 cols)

_(none)_

### RENAME_TO_V2 (0 cols)

_(none)_

### PRESERVE_DIFFERENT_SEMANTIC (18 cols)

1. `imaging_laterality_rollup` → `imaging_laterality_rollup_v271b` — Only 34.8% agreement — pair likely measures different things despite naming.
    - writers: 271_tirads_imaging_finalization.py; 271a_fix_concordance_three_valued.py; 271b_laterality_normalization.py
    - readers: (none)
2. `imaging_tirads_best` → `imaging_tirads_best_v2` — Only 19.6% agreement — pair likely measures different things despite naming.
    - writers: 204_canonical_master_assembly.py; 205_canonical_consolidation.py; 301_canonical_us_patient_master_v1.py; 48_build_analysis_resolved_layer.py
    - readers: 207_canonical_master_expansion.py; 48_build_analysis_resolved_layer.py; 56_pre_manuscript_audit.py; 62_run_primary_descriptives.py; cpm_cols_pre.txt; schema_inventory.md
3. `imaging_tirads_worst` → `imaging_tirads_worst_v2` — Only 22.8% agreement — pair likely measures different things despite naming.
    - writers: 204_canonical_master_assembly.py; 205_canonical_consolidation.py; 252_recompute_max_tirads.py; 301_canonical_us_patient_master_v1.py; 48_build_analysis_resolved_layer.py
    - readers: 252_recompute_max_tirads.py; 48_build_analysis_resolved_layer.py; cpm_cols_pre.txt; explain_plan_01.txt; query_log.sql; run_feasibility.py; schema_inventory.md; PLAN.md
4. `imaging_updated_tirads_category_cpm_v1` → `imaging_updated_tirads_category_cpm_v2` — Only 29.6% agreement — pair likely measures different things despite naming.
    - writers: (none)
    - readers: 375_cpm_column_cleanup_and_audit.py
5. `max_tirads_ever` → `max_tirads_ever_v2` — Pair has different semantics: BIGINT category code 1-5 vs DOUBLE points (0-13+). Different semantics — DO NOT compare. Both should be kept under clarifying names.. Recommend renaming pair to clarifying names.
    - writers: 252_recompute_max_tirads.py; 265_canonical_finalization.py; 272_canonical_cleanup_phase1.py; 301_canonical_us_patient_master_v1.py; 50_multinodule_imaging.py
    - readers: 207_canonical_master_expansion.py; 209_nlp_entity_crossvalidation.py; 228_registry_backfill.py; 246_canonical_us_nodule_characteristics.py; 252_recompute_max_tirads.py; 259_final_verification_lock.py; 264_final_acceptance_addendum.py; 277_canonical_cleanup_phase7_verification.py; 50_multinodule_imaging.py; cpm_cols_pre.txt; drift_report.md; phase1_dryrun_probe.py; phase1_run.log; phase1_stdout.log; preflight.py; schema_recon.py; schema_inventory.md; FINALIZATION_REPORT_v1_1.md; PLAN.md
6. `pathology_vs_imaging_laterality_concordant` → `pathology_vs_imaging_laterality_concordant_v271b` — Pair has different semantics: legacy BOOLEAN; v271b is multi-valued VARCHAR — type mismatch, see _v271b row. Recommend renaming pair to clarifying names.
    - writers: 271_tirads_imaging_finalization.py; 271a_fix_concordance_three_valued.py; 271b_laterality_normalization.py
    - readers: (none)
7. `pathology_vs_imaging_laterality_concordant_v271b` → `pathology_vs_imaging_laterality_concordant` — Only 27.3% agreement — pair likely measures different things despite naming.
    - writers: 271b_laterality_normalization.py
    - readers: (none)
8. `preop_tirads_best` → `preop_tirads_best_v2` — Only 25.6% agreement — pair likely measures different things despite naming.
    - writers: 204_canonical_master_assembly.py; 205_canonical_consolidation.py; 221_tirads_v2_integration.py; 221b_suspicious_ln_reextraction.py; 252_recompute_max_tirads.py
    - readers: 207_canonical_master_expansion.py; 209_nlp_entity_crossvalidation.py; 252_recompute_max_tirads.py; cpm_cols_pre.txt; PLAN.md
9. `preop_tirads_category` → `preop_tirads_category_v2` — Only 30.6% agreement — pair likely measures different things despite naming.
    - writers: 204_canonical_master_assembly.py; 205_canonical_consolidation.py
    - readers: 209_nlp_entity_crossvalidation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1; VIEW:manuscript_workspace.cohort_m045_multimodal_risk_v1
10. `tirads_best_category_v12` → `imaging_tirads_best_v2` — Only 19.6% agreement — pair likely measures different things despite naming.
    - writers: 221_tirads_v2_integration.py; 265_canonical_finalization.py
    - readers: 207_canonical_master_expansion.py; 209_nlp_entity_crossvalidation.py; 246_canonical_us_nodule_characteristics.py; 273_registry_curation.py; ajcc8_t_stage_view_migration_proposal.md; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_descriptive_full_cohort_v1; VIEW:manuscript_workspace.cohort_m011_tirads_fna_genetics_v1; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1; VIEW:manuscript_workspace.cohort_m045_multimodal_risk_v1; VIEW:manuscript_workspace.cohort_m053_nondiagnostic_fna_v1; VIEW:manuscript_workspace.cohort_m064_frozen_decision_v1; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1; VIEW:manuscript_workspace.cohort_m076_ln_surveillance_v1
11. `tirads_best_combined` → `imaging_tirads_best_v2` — Only 48.0% agreement — pair likely measures different things despite naming.
    - writers: 205_canonical_consolidation.py; 265_canonical_finalization.py
    - readers: 213_data_dictionary.py; 228_registry_backfill.py; cpm_cols_pre.txt
12. `tirads_best_score_v12` → `tirads_v2_worst_category` — Pair has different semantics: v12.score = MIN tirads_acr_recalculated (category code 1-5); v2 has only worst_category not best — semantic mismatch. Recommend renaming pair to clarifying names.
    - writers: 221_tirads_v2_integration.py; 221b_suspicious_ln_reextraction.py; 265_canonical_finalization.py; 271_tirads_imaging_finalization.py
    - readers: 207_canonical_master_expansion.py; 209_nlp_entity_crossvalidation.py; 228_registry_backfill.py; 246_canonical_us_nodule_characteristics.py; 265_canonical_finalization.py; 273_registry_curation.py; 57_freeze_manuscript_cohort.py; 58_missingness_summary.py; 59_dataset_summary.py; 61_table1_preview.py; ajcc8_t_stage_view_migration_proposal.md; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_descriptive_full_cohort_v1; VIEW:manuscript_workspace.cohort_m011_tirads_fna_genetics_v1; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1; VIEW:manuscript_workspace.cohort_m045_multimodal_risk_v1; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
13. `tirads_nodule_size_max_mm_v12` → `tirads_v2_largest_nodule_cm` — Only 2.0% agreement — pair likely measures different things despite naming.
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; ajcc8_t_stage_view_migration_proposal.md; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_descriptive_full_cohort_v1; VIEW:manuscript_workspace.cohort_m011_tirads_fna_genetics_v1; VIEW:manuscript_workspace.cohort_m050_tumor_size_volume_v1; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
14. `tirads_source_v12` → `tirads_source_system_v271` — Only 0.0% agreement — pair likely measures different things despite naming.
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md
15. `tirads_worst_category_v12` → `imaging_tirads_worst_v2` — Only 22.8% agreement — pair likely measures different things despite naming.
    - writers: 221_tirads_v2_integration.py; 265_canonical_finalization.py
    - readers: 207_canonical_master_expansion.py; 209_nlp_entity_crossvalidation.py; 246_canonical_us_nodule_characteristics.py; 252_recompute_max_tirads.py; 273_registry_curation.py; ajcc8_t_stage_view_migration_proposal.md; cpm_cols_pre.txt; schema_inventory.md; PLAN.md; VIEW:manuscript_workspace.cohort_descriptive_full_cohort_v1; VIEW:manuscript_workspace.cohort_m011_tirads_fna_genetics_v1; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1; VIEW:manuscript_workspace.cohort_m076_ln_surveillance_v1
16. `tirads_worst_combined` → `imaging_tirads_worst_v2` — Only 26.5% agreement — pair likely measures different things despite naming.
    - writers: 205_canonical_consolidation.py; 221_tirads_v2_integration.py; 265_canonical_finalization.py
    - readers: 213_data_dictionary.py; 228_registry_backfill.py; cpm_cols_pre.txt
17. `tirads_worst_points_v271` → `tirads_v2_max_points` — Only 27.7% agreement — pair likely measures different things despite naming.
    - writers: 271_tirads_imaging_finalization.py
    - readers: (none)
18. `tirads_worst_score_v12` → `tirads_v2_worst_category` — Only 45.3% agreement — pair likely measures different things despite naming.
    - writers: 271_tirads_imaging_finalization.py
    - readers: 207_canonical_master_expansion.py; 265_canonical_finalization.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1

### INVESTIGATE (14 cols)

1. `imaging_laterality_rollup_v271b` → `imaging_laterality_rollup_v2` — Middle-zone agreement 68.1% on 3439 rows — partial backfill, stale data, or logic change suspected.
    - writers: 271b_laterality_normalization.py
    - readers: (none)
2. `imaging_tirads_source` (no v2 counterpart) — No v2 counterpart and 4 script reader(s). VARCHAR source system label; possibly redundant with tirads_source_system_v271 — flagged
    - writers: 48_build_analysis_resolved_layer.py
    - readers: 207_canonical_master_expansion.py; 48_build_analysis_resolved_layer.py; cpm_cols_pre.txt; schema_inventory.md
3. `preop_tirads_worst` (no v2 counterpart) — No v2 counterpart and 1 script reader(s). no v2 counterpart (only preop_tirads_best_v2 exists)
    - writers: 204_canonical_master_assembly.py; 205_canonical_consolidation.py
    - readers: cpm_cols_pre.txt
4. `tirads_best_points_v271` (no v2 counterpart) — No v2 counterpart. no v2 best_points — would need MIN aggregate from canonical_us_nodule_v2
    - writers: 271_tirads_imaging_finalization.py
    - readers: (none)
5. `tirads_concordant_count_v12` (no v2 counterpart) — No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired (per-nodule acr2017_vs_updated_concordant on canonical_us_nodule_v2)
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
6. `tirads_has_acr_recalc_v12` (no v2 counterpart) — No v2 counterpart and 4 script reader(s). no v2 — concept retired
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md
7. `tirads_mismatch_count_v12` (no v2 counterpart) — No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
8. `tirads_n_nodule_records_v12` → `tirads_v2_n_nodules_scored` — Middle-zone agreement 50.8% on 2463 rows — partial backfill, stale data, or logic change suspected.
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m075_tirads_multi_nodule_v1
9. `tirads_n_sources_v12` (no v2 counterpart) — No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1
10. `tirads_nodules_scored_combined` → `tirads_v2_n_nodules_scored` — Middle-zone agreement 54.5% on 2463 rows — partial backfill, stale data, or logic change suspected.
    - writers: 205_canonical_consolidation.py
    - readers: cpm_cols_pre.txt
11. `tirads_reliability_v12` (no v2 counterpart) — No v2 counterpart but 1 cohort view(s) read this column. Need to design a replacement or rename before drop. no v2 — concept retired (no reliability score in v2)
    - writers: (none)
    - readers: 207_canonical_master_expansion.py; 273_registry_curation.py; cpm_cols_pre.txt; schema_inventory.md; VIEW:manuscript_workspace.cohort_m025_tirads_performance_v1
12. `tirads_source_system_v271` (no v2 counterpart) — No v2 counterpart. label-only column for v271 points; superseded if v2 path becomes canonical
    - writers: 271_tirads_imaging_finalization.py
    - readers: (none)
13. `tumor_pathology_laterality_v271b` (no v2 counterpart) — No v2 counterpart. newer name; no _v2 — keep (and consider rename to drop _v271b suffix once stabilized)
    - writers: 271b_laterality_normalization.py
    - readers: (none)
14. `worst_tirads_category` → `tirads_v2_worst_category` — Middle-zone agreement 83.7% on 2817 rows — partial backfill, stale data, or logic change suspected.
    - writers: 50_multinodule_imaging.py
    - readers: 207_canonical_master_expansion.py; 213_data_dictionary.py; cpm_cols_pre.txt; schema_inventory.md

## v2 columns NOT paired with any legacy (informational)

- `tirads_v2_any_ete_on_us`
- `tirads_v2_any_fna_recommended`
- `tirads_v2_any_fna_recommended_report`
- `tirads_v2_any_fna_recommended_report_source`
- `tirads_v2_any_interval_growth`
- `tirads_v2_any_suspicious_ln_on_us`
- `tirads_v2_n_reports`
- `tirads_v2_shortest_followup_months`
- `tirads_v2_worst_rank`
- `tirads_v2_worst_rank_source`

## Sample-disagreement tables in `manuscript_workspace`

- `manuscript_workspace.cpm_tirads_audit_sample_imaging_tirads_best_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_imaging_tirads_worst_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_preop_tirads_best_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_preop_tirads_category_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_imaging_updated_tirads_category_cpm_v1_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_best_category_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_worst_category_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_worst_score_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_n_nodule_records_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_nodule_size_max_mm_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_source_v12_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_worst_points_v271_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_imaging_laterality_rollup_v271b_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_pathology_vs_imaging_laterality_concordant_v271b_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_imaging_laterality_rollup_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_best_combined_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_worst_combined_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_tirads_nodules_scored_combined_v1` (10 rows)
- `manuscript_workspace.cpm_tirads_audit_sample_worst_tirads_category_v1` (10 rows)

