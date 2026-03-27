# Multi-tumor pathology capture audit

**Generated:** `run_multi_tumor_pathology_audit.py`  
**Executive verdict:** **PARTIALLY COMPLETE**

## Summary

| Layer | Status |
|-------|--------|
| **A. Original Excel → `path_synoptics` (wide Parquet)** | **VERIFIED COMPLETE** — 11,688 rows; row index and `research_id` order match Excel; per-slot nonempty flags identical for tumors 1–5 (`mismatches: {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}`). |
| **B. Canonical `tumor_episode_master_v2`** | **NOT COMPLETE for multi-foci** — built as **tumor_ordinal = 1 only** from `path_synoptics` tumor_1 fields (`scripts/22_canonical_episodes_v2.py`). |
| **C. `tumor_pathology` workbook** | **Separate ingest** (`FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx`) — **no `tumor_2+` columns**; patient-level `histology_1_*` only. |
| **D. Proposal2 / `ptc_cohort` / `exports/ptc_full.csv`** | **Tumor-1-centric** — joins `tumor_pathology` + `path_synoptics` for ETE on **tumor_1** only (`scripts/03_research_views.py`). |

## A. Source structure

- **Ingest:** `scripts/01_ingest_all_files.py` — `FILE_MAP` loads `All Diagnoses & synoptic 12_1_2025.xlsx` → processed Parquet (table name in manifest: `synoptic_pathology`; analytic table used across repo: `processed/path_synoptics.parquet`). Column names are snake_cased via `standardize_columns()`.
- **Source file sheet:** `synoptics + Dx merged` (275 columns).
- **Tumor blocks in Parquet (`path_synoptics`):** tumor_1: 39 cols; tumor_2: 28; tumor_3: 23; tumor_4: 20; tumor_5: 21.
- **Benign vs malignant:** Same synoptic sheet carries benign disease checkboxes and separate `Tumor_N_*` blocks; `tumor_pathology` / `benign_pathology` are **additional** Excel sources (not re-proven here).

Full tumor-N column names are in DuckDB `information_schema` or `DESCRIBE read_parquet('processed/path_synoptics.parquet')`.

## B. Trace extraction and reshape

| Component | Behavior |
|-----------|----------|
| Wide ingest | All `tumor_[1-5]_*` columns preserved; no loop cap at tumor 1. |
| Long / per-tumor canonical row | **Missing** in `tumor_episode_master_v2` — only one engineered row per pathology/surgery join (tumor 1). |
| Phase 10 `extracted_multi_tumor_aggregate_v1` | Patient-level **worst-of** rollup (post-remediation includes angio/margin/ETE/vessel/size through **tumor 5**). **LN sum** still only tumor_1 `ln_involved` + tumor_2 `lns_involved` (schema has no per-tumor LN fields for tumors 3–5). |
| Script **`scripts/108_synoptic_tumor_long_v1.py`** | New **one-row-per-nonempty-focus** table with `tumor_index` and `source_column_prefix` for lineage. Local build from current Parquet: **~11,103** lesion rows (sum of populated slots ≈ 11,105; minor string-emptyness edge cases). |

**Collapse rules used downstream (when multi-tumor considered):**

- `extracted_multi_tumor_aggregate_v1`: worst-grade heuristic for margin/angio/ETE; **max** diameter; **sum** LN (tumor 1+2 only).
- Scoring / AJCC in `51b` / manuscript cohort: driven by patient-level summaries that **default to tumor 1** unless explicitly joined to aggregate/long tables.

## C. Counts (source vs wide output)

See **`multi_tumor_source_vs_output_counts.csv`**.

Slot-count distribution (number of tumor slots with **any** nonempty field in that slot’s column group):  
`{0: 2629, 1: 7680, 2: 920, 3: 306, 4: 98, 5: 55}`

**`tumor_episode_master_v2` tumor_ordinal distribution (local DB):**

```
|   tumor_ordinal |     n |
|----------------:|------:|
|               1 | 11688 |
```

## D. Proposal2 / manuscript impact

Variables **at risk if interpreted as “whole specime” but only tumor 1 used**:

- Tumor **size** — `largest_tumor_cm` from `tumor_pathology` / tumor_1 synoptic; **105** specimen rows where max(tumor 2–5) > tumor 1 size (see discrepant CSV).
- **ETE** — `tumor_1_extrathyroidal_ext` in `ptc_full.csv`; secondary-foci ETE may differ (worst-ETE now in aggregate table after remediation).
- **Histology subtype** — classic PTC filter uses **histology_1** / variant; additional foci may differ (**histology_differs_across_foci** in discrepant CSV).
- **Multifocality** — `tumor_1_multiple_tumor` and pathology-derived flags; multi-foci **data** exist in wide form but not as multiple canonical lesion rows until `synoptic_tumor_long_v1`.
- **Nodal burden** — synoptic **per-tumor** LN fields only for tumors 1–2; central neck often **specimen-level** on tumor 1 columns.

## E. Remediation

See **`MULTI_TUMOR_PATHOLOGY_REMEDIATION_SUMMARY.md`** — Phase 10 SQL extended to tumors **4–5**; new long table builder **108**; local DuckDB: rerun Phase 8 engine + materialization after deploy.

## F. Code / file paths

- `scripts/01_ingest_all_files.py` — Excel ingest + `standardize_columns`
- `processed/path_synoptics.parquet` — wide analytic spine
- `scripts/22_canonical_episodes_v2.py` — `tumor_episode_master_v2` (tumor 1)
- `notes_extraction/extraction_audit_engine_v8.py` — `extracted_multi_tumor_aggregate_v1` SQL
- `scripts/03_research_views.py` — `ptc_cohort` / exports
- `studies/proposal2_ete_staging/proposal2_ete_analysis.py` — reads `exports/ptc_full.csv`
- `scripts/108_synoptic_tumor_long_v1.py` — **`synoptic_tumor_long_v1`**
