# Canonical Publication DB Finalization Report

**Date:** 2026-04-16  
**Script:** `scripts/233_canonical_finalization.py`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Archive DB:** `"Thyroid 2026 UPdated".archive_pub_v1_0`  

## Final Invariants

| Check | Value | Expected |
|---|---|---|
| `canonical_patient_master` rows | 10,871 | 10,871 |
| distinct `research_id` | 10,871 | 10,871 |
| NULL `research_id` | 0 | 0 |
| NULL `fna_path_outcome` | 0 | 0 |

All invariants held at every phase checkpoint.

## Publication DB Summary

- `main` base tables: **111**
- `__readme` rows: **110** (rebuilt, 0 stale pointers)
- `manuscript_workspace.detail_table_registry_v1` rows: **109** (all counts refreshed)
- `data_dictionary_v221` rows: **1471** (rebuilt locally from `information_schema`)
- Manuscript + cohort views: **65** validated, **0 broken**

## Phase 1 — Audit Fixes at the Source

### 1A — Recurrence flag reconciliation (Issue #2)

Changed `any_recurrence_flag` derivation from legacy-only to:

```
(recurrence_flag_v2 OR recurrence_flag_scoring OR structural_recurrence_flag)
AND recurrence_definition <> 'no_recurrence_evidence'
```

| Metric | Before | After |
|---|---|---|
| `any_recurrence_flag = TRUE` | 1,946 | 384 |
| Phantom (flag=TRUE & definition='no_recurrence_evidence') | 1,521 | 0 |

Prior values preserved in `any_recurrence_flag_prev_233`.

### 1B — time_to_recurrence + negative quarantine (Issue #3)

| Metric | After |
|---|---|
| Negative `time_to_recurrence_days` | 0 |
| Negative `recurrence_days_from_surg` | 0 |
| Quarantined `recurrence_days_from_surg_quarantined` | 12 |

Pipeline-side fix: `scripts/203_canonical_recurrence.py` Tiers 4 & 5 now derive `time_to_recurrence_days` from `(recurrence_date - first_surgery_date)` with a `>= 0` guard, and an `assert` blocks any rebuild that would emit negatives.

### 1C — Follow-up recovery (Issue #1)

| Metric | Before | After | Δ |
|---|---|---|---|
| Zero-followup patients | 6,810 | 6,700 | −110 |
| NULL `first_surgery_date` | 2,140 | 0 | −2,140 |

Extended date-union sources (all live in pub DB): `followup_or_death_date`, `death_date`, `last_tg_date`, `cpm.last_contact_date`, `tg_postop_surveillance_windows_v1.window_last_date`, `rai_treatment_episode_v2.resolved_rai_date`, `note_entities_llm_{survival_followup,recurrence}.note_date`, `ultrasound_reports.ultrasound_date`, `ct_imaging.date_of_exam`, `mri_imaging.date_of_exam`, `nuclear_med.scandate`. Surgery-date recovery from `operative_episode_detail_v2`, `nsqip_enrichment.nsqip_operation_date`, `path_synoptics.surg_date`.

Prior values preserved in `followup_days_prev_233`, `followup_years_prev_233`, `last_contact_date_prev_233`, `last_contact_source_prev_233`, `first_surgery_date_prev_233`.

Pipeline-side fix: `scripts/218_followup_recovery.py` now targets the publication DB and includes `tg_postop_surveillance_windows_v1.window_last_date` in its union.

### 1D — recurrence_site (Issue #4)

Already closed in live data: **0 residual** cases where `recurrence_site IS NULL AND recurrence_site_text IS NOT NULL`. No change applied.

### 1E — mortality_type (Issue #5)

Added convenience column `mortality_type` on `canonical_patient_master`:

| mortality_type | n |
|---|---|
| `cancer_cohort_death` | 96 |
| `all_cause_non_cancer_death` | 96 |
| `unknown_cohort_death` | 0 |
| NULL (alive) | 10,679 |

## Phase 2 — `__readme` Rebuild

Archived prior table as `"Thyroid 2026 UPdated".archive_pub_v1_0.__readme_<TS>`. Rebuilt from `information_schema.tables`. Removed 6 stale pointers (`thyroid_scoring_py_v1`, `md_synoptic_tumor_long_v1`, `md_extracted_fna_bethesda_v1`, `data_dictionary_v221`, `data_dictionary_v2`, `data_dictionary_parquet_v221`) — they live in the reference DB; the pub DB keeps only clean/finalized artifacts. Added rows for 8 tables previously missing from the catalog (including `_molecular_patient_rollup_v227`, `ete_adjudication_v1`, `patient_tumor_rollup_v1`, `ret_note_entity_adjudication_v226`, `ret_patient_adjudicated_v226`, `serial_imaging_us`).

## Phase 3 — `detail_table_registry_v1` + `canonical_detail_pointer_v1`

- Registry rows refreshed in place: **109** (all `total_rows` and `total_patients` recomputed).
- Upserted / clarified entries: `patient_tumor_rollup_v1`, `ete_adjudication_v1`, `_molecular_patient_rollup_v227`, `ret_patient_adjudicated_v226`, `ret_note_entity_adjudication_v226`. `qa_fusion_parse_triage_v1` was upserted then removed once Phase 4 evicted the underlying table.
- `canonical_detail_pointer_v1` refreshed — per-CPM-column pointer joining each of 1,471 columns to its detail table via `feeds_master_columns`. 12 exact-match mappings surfaced; remaining columns appear as unmapped rows (some registry entries use freeform prose for `feeds_master_columns`, which is listed as a non-blocking follow-up below).

## Phase 4 — Non-Publication Artifact Eviction

| Table (archived) | Rows | Destination |
|---|---|---|
| `__readme_20260416T153344Z` | 109 | `"Thyroid 2026 UPdated".archive_pub_v1_0.__readme_20260416T153344Z` |
| `__readme_20260416T153411Z` | 109 | `"Thyroid 2026 UPdated".archive_pub_v1_0.__readme_20260416T153411Z` |
| `__readme_20260416T154105Z` | 109 | `"Thyroid 2026 UPdated".archive_pub_v1_0.__readme_20260416T154105Z` |
| `detail_table_registry_v1_20260416T153420Z` | 110 | `"Thyroid 2026 UPdated".archive_pub_v1_0.detail_table_registry_v1_20260416T153420Z` |
| `qa_fusion_parse_triage_v1_20260416T153535Z` | 1,170 | `"Thyroid 2026 UPdated".archive_pub_v1_0.qa_fusion_parse_triage_v1_20260416T153535Z` |

Drops executed: `main.qa_fusion_parse_triage_v1` (1,170 rows) — moved to `"Thyroid 2026 UPdated".archive_pub_v1_0.qa_fusion_parse_triage_v1_<TS>`, not referenced by any view, registry row removed post-eviction. No other suspect-named tables exist in the pub DB.

## Phase 5 — Data Dictionary + Validation

- `main.data_dictionary_v221` rebuilt with 1 row per CPM column (1,471 columns).
- Columns: `column_name, data_type, is_nullable, ordinal_position, non_null_count, coverage_pct, inferred_source, description`. `inferred_source` joined via `canonical_detail_pointer_v1`; `non_null_count` / `coverage_pct` populated from live CPM.
- All **65 views** validated (0 broken after eviction).

## Pipeline Regression Guard (source-of-truth edits)

- `scripts/203_canonical_recurrence.py`: DB retargeted to `thyroid_canonical_publication_v1_0`; Tier 4/5 `time_to_recurrence_days` computed with `>= 0` guard; runtime `assert` blocks negative emission.
- `scripts/218_followup_recovery.py`: DB retargeted to pub DB, `CANONICAL` renamed to `canonical_patient_master`, union extended with `tg_postop_surveillance_windows_v1.window_last_date`.

## Residual Items (nice-to-have, non-blocking)

1. Some `detail_table_registry_v1.feeds_master_columns` entries use freeform prose; a future normalization pass would boost `canonical_detail_pointer_v1` coverage beyond 12 mapped columns.
2. 6,700 patients remain in the zero-followup bucket because no post-surgery contact exists in any current source; when additional note-entity or LN-longitudinal tables land, re-run Phase 1C (`--phase 1c`) idempotently to capture them.
3. The retained `*_prev_233` snapshot columns on `canonical_patient_master` can be dropped after downstream consumers have confirmed the new values; keep until the next manuscript freeze.

## Closes

Coworker audit issues **#1 (follow-up recovery)**, **#2 (phantom recurrences)**, **#3 (negative t2r)**, **#4 (recurrence_site — verified)**, **#5 (mortality_type)**.

---
*Generated by `scripts/233_canonical_finalization.py` on 2026-04-16.*