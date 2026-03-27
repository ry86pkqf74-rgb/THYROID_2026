# Multi-tumor pathology remediation summary

## What was verified (proof, not assumption)

- **Row-aligned parity:** `raw/All Diagnoses & synoptic 12_1_2025.xlsx` (sheet `synoptics + Dx merged`) and `processed/path_synoptics.parquet` both have **11,688** rows; `Research ID number` matches `research_id` in order; for each tumor slot **1–5**, the set of “any column in this slot nonempty” flags is **identical** between Excel and Parquet (**0** mismatches per slot). See `run_multi_tumor_pathology_audit.py` and `multi_tumor_source_vs_output_counts.csv`.

- **Wide capture:** All `tumor_[1-5]_*` column families present in Parquet (39 / 28 / 23 / 20 / 21 columns respectively). Secondary slots are populated on **1,379** rows with any tumor 2–5 data.

- **Canonical gap:** `tumor_episode_master_v2` remains **tumor_ordinal = 1 only** (11,688 rows locally); it does **not** explode multi-foci into separate lesion rows.

## What was changed in code

1. **`notes_extraction/extraction_audit_engine_v8.py`** — `extracted_multi_tumor_aggregate_v1` SQL:
   - **Eligibility:** `multi_tumor_pts` now includes any patient with **non-empty histology text** in tumor **3, 4, or 5** (not only tumor 2).
   - **Worst-of rollups:** Angioinvasion, margin, ETE, vessel quantify, and max size now include **tumor 4 and 5** wherever those columns exist.
   - **LN sum:** Unchanged — `path_synoptics` only exposes LN positive counts for **tumor_1** (`ln_involved`) and **tumor_2** (`lns_involved`); there are no per-focus LN columns for tumors 3–5 (schema limitation).

2. **`scripts/108_synoptic_tumor_long_v1.py`** *(new)* — builds **`synoptic_tumor_long_v1`**:
   - One row per **non-empty** tumor slot per synoptic row (vectorized).
   - Provenance: `source_column_prefix`, `source_path_file`, `synoptic_row_ix`, `build_git_sha`.
   - Writes `processed/synoptic_tumor_long_v1.parquet` and refreshes local `thyroid_master.duckdb` table `synoptic_tumor_long_v1`.
   - Optional `--md` pushes to MotherDuck (token required).

3. **`scripts/26_motherduck_materialize_v2.py`** — added `md_synoptic_tumor_long_v1` → `synoptic_tumor_long_v1` to **MATERIALIZATION_MAP** for RO share mirroring after cloud deploy.

## Deploy / rerun checklist (MotherDuck)

1. Re-run Phase 10 extraction deploy that executes `build_multi_tumor_aggregate_sql()` (e.g. `notes_extraction` Phase 10 / `audit_and_refine_phase10` per your pipeline).
2. Run `scripts/108_synoptic_tumor_long_v1.py --md` (or load Parquet via your standard promote path).
3. Run `scripts/26_motherduck_materialize_v2.py --md` to refresh `md_*` mirrors.

## Proposal2 / manuscript follow-up (not auto-run)

- `exports/ptc_full.csv` / `ptc_cohort` still use **`tumor_1_*`** and `tumor_pathology.histology_1_*`. For size/risk/staging that should reflect **dominant or worst focus**, join `synoptic_tumor_long_v1` or `extracted_multi_tumor_aggregate_v1` explicitly and document the rule in methods.
- Re-run `scripts/04_publication_exports.py` (or your cohort rebuild) only after you decide those join rules.

## Artifacts

| Artifact | Path |
|---------|------|
| Audit | `studies/proposal2_ete_staging/MULTI_TUMOR_PATHOLOGY_CAPTURE_AUDIT.md` |
| Counts | `studies/proposal2_ete_staging/multi_tumor_source_vs_output_counts.csv` |
| Flagged rows | `studies/proposal2_ete_staging/multi_tumor_discrepant_cases.csv` |
| Long table (local) | `processed/synoptic_tumor_long_v1.parquet` |
