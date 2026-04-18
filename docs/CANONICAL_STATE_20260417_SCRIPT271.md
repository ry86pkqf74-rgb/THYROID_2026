# Canonical State — 2026-04-17 — Script 271

**Database:** `thyroid_canonical_publication_v1_0` (WRITE)
**Archive:** `"Thyroid 2026 UPdated".archive_pub_v1_0`
**Snapshot timestamp:** `20260418T010726Z` (Step 1 pre-271 snapshots)
**Step 8 dictionary rebuild:** `20260418T011245Z`
**Script:** `scripts/271_tirads_imaging_finalization.py`

## Starting state (pre-271)
- `canonical_patient_master`: 10,871 rows × **1,514** columns
- `canonical_us_nodule_characteristics_v1` (cunc_v1): 37,016 rows × **36** columns
- `imaging_nodule_master_v1` (inm_v1): 37,016 rows × **24** columns
- main-schema base tables: 115

## Ending state (post-271)
- `canonical_patient_master`: 10,871 rows × **1,519** columns ✓
  - **−1 dropped:** `imaging_nodule_size_cm_v11` (Step 2)
  - **+6 added:** `tirads_worst_points_v271`, `tirads_best_points_v271`,
    `tirads_source_system_v271` (Step 4); `imaging_laterality_rollup`,
    `imaging_has_structured_components`,
    `pathology_vs_imaging_laterality_concordant` (Step 6)
  - Legacy `tirads_worst_score_v12` / `tirads_best_score_v12` re-COMMENTed
    as "category code, NOT ACR points" (Step 4 — column retained, not dropped)
- `canonical_us_nodule_characteristics_v1`: 37,016 rows × **39** columns ✓
  - Atomic-swap rebuild via `_cunc_v2`; deprecated v1 archived to
    `archive_pub_v1_0.canonical_us_nodule_characteristics_v1_deprecated_20260418`.
  - **−1 dropped:** `tirads_category` (broken dual short/long naming)
  - **Renamed:** `tirads_acr_recalculated` → `tirads_category_code_legacy_v1`;
    `tirads_category_modified` → `tirads_category_modified_legacy_v1`
  - **+4 added:** `tirads_category_v2`, `tirads_band_ambiguous` (Step 3);
    `calcifications_coverage_status`, `tirads_score_component_complete` (Step 5)
- `imaging_nodule_master_v1`: 37,016 rows × **25** columns ✓
  - **+1 added:** `dominant_nodule_flag` (n_dominant=13,364)
- main-schema base tables: 116 (= 115 + 1 new `tirads_reextraction_queue_v1`,
  4,363 rows; the deprecated cunc_v1 was archived and dropped, so net +1)

## P0/P1/P2 disposition (PROMPT 19 Extended)
| Item | Status | Evidence |
|------|--------|----------|
| P0a — drop `imaging_nodule_size_cm_v11` | DONE | Step 2; `271_step2_drop_v11.json` |
| P0b — rebuild cunc_v1 TIRADS category from points | DONE | Step 3; band audit TR1=0/TR2=2/TR3=3/TR4=4-6/TR5=7+ verified |
| P1a — points-based patient TIRADS rollup + 6-patient audit | DONE | Step 4; 1,326 patients populated; audit table in `271_step4_points_rollup.json` |
| P1b — calcifications coverage flag (no back-fill, just flag) | DONE | Step 5; 4,363 nodules queued (99.2% of scored) |
| P2 — `dominant_nodule_flag` + 3 new CPM cols | DONE | Step 6 |

## TIRADS band audit (post-rebuild)
| `tirads_category_v2` | min pts | max pts | n |
|---|---|---|---|
| TR1 | 0 | 0 | 758 |
| TR2 | 2 | 2 | 914 |
| TR3 | 3 | 3 | 982 |
| TR4 | 4 | 6 | 654 |
| TR5 | 7 | 13 | 96 |
| NULL (1pt or no score) | 1 | 1 | 33,612 |

## Coverage statistics (post-Step 5)
- `calcifications_coverage_status` distribution:
  - `extracted`: 5,149  (these have all 5 ACR components)
  - `not_extracted`: 4,363  (have score but no calcifications — re-extraction queue)
  - `absent_from_report`: 27,504
- `tirads_score_component_complete = TRUE`: 5,149

## Imaging laterality rollup (post-Step 6)
- mixed: 1,356; bilateral: 1,197; right: 446; left: 384; isthmus: 56; NULL: 7,432
- `imaging_has_structured_components`: TRUE=3,439, FALSE=7,432
- `pathology_vs_imaging_laterality_concordant`: TRUE=848, FALSE=10,023 (FALSE
  includes both real disagreement and NULL on either side)

## Open items
1. **TIRADS re-extraction queue** (`tirads_reextraction_queue_v1`, 4,363 rows)
   populated — LLM re-extraction not run in this script.
2. **Placeholder cunc_v1 patients** (~7,432 patients with no scored nodule and no
   structured components) remain un-recoverable from upstream.
3. **Legacy v12 columns** retained on CPM with corrected COMMENTs. Analysis code
   should prefer `tirads_*_points_v271` going forward.
4. **Repo source sweep** (`scripts/output/271_repo_source_sweep.md`) lists
   priority TODO items in `app/imaging_nodule_dashboard.py`,
   `app/patient_timeline_explorer.py`, `app/qa_workbench.py`, and
   `scripts/266a_dictionary_and_feeder_registration.py`.

## Key invariants
- `canonical_patient_master`: 10,871 / 10,871 / 0 (rows / distinct rid / NULL rid)
  verified after every mutating step.
- `manuscript_workspace` schema **NOT** touched (Script 220 ETE views depend on
  current view DDLs).
- `manuscript_workspace.cohort_descriptive_full_cohort_v1` row-count check
  performed in Step 9.

## Archive snapshots created in Step 1
All `*_pre271_20260418T010726Z` tables in `"Thyroid 2026 UPdated".archive_pub_v1_0`:

| source table | snapshot rows |
|---|---|
| canonical_patient_master | 10,871 |
| canonical_us_nodule_characteristics_v1 | 37,016 |
| imaging_nodule_master_v1 | 37,016 |
| data_dictionary_v266a | 1,529 |
| __readme | 115 |

Plus the `archive_pub_v1_0.canonical_us_nodule_characteristics_v1_deprecated_20260418`
created during Step 3's atomic swap.
