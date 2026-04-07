# Deterministic fix log — specimen genomics closure

## Code / policy changes (repo)

1. **`scripts/49_enhanced_linkage_v3.py`**
   - Added `--md-sa` (prefer `MD_SA_TOKEN`), `--md-env`, and `--database` for MotherDuck alignment with other governed scripts.
   - Corrected `--md` help text (MotherDuck, not local file).
   - `connect_md()` now resolves database via `resolve_database_for_env` when appropriate.

2. **`scripts/119_md_formalization_validate.py`**
   - **Check 12 (release-mode):** If `main.molecular_results` is empty but `main.molecular_test_episode_v2` has rows → **FAIL** (was PASS with “skipped” semantics).
   - **Check 12b (release-mode):** If `molecular_test_episode_v2` is non-empty but `main.molecular_testing` is missing → **FAIL**, documenting that date spines and FNA–molecular linkage cannot work without script-22 source.

3. **`scripts/41_ingest_thyroseq_excel.py`**
   - **`--input-profile cohort_thyroseq_afirma_12_5`:** maps wide cohort **`THYROSEQ_AFIRMA_12_5.xlsx`** columns to ThyroSeq-complete field names (`Thyroseq Mutation`, `Pathology`, etc.), synthetic unique **Pt. MRN** per row for hashing, **`ThyroSeq Test Date`** from earliest parsable **`DATE_*`**; **`--md-sa`** prefers **`MD_SA_TOKEN`**.

4. **`scripts/145_md_materialize_molecular_testing.py`**
   - Materializes **`main.molecular_testing`** from **`raw/THYROSEQ_AFIRMA_12_5.xlsx`** on MotherDuck (long rows per assay column / slot), then applies **`MOLECULAR_TEST_EPISODE_V2_SQL`** so **`molecular_test_episode_v2`** matches script-22 semantics. **Does not** replace parquet-backed tables via `register_parquets`.

## MotherDuck operations (no fuzzy merges)

- Re-ran **`49_enhanced_linkage_v3.py --md --md-sa --md-env prod`** to rebuild v3 linkage tables on **`Thyroid 2026`**. No change to FNA–molecular cardinality: root cause is **missing `molecular_testing` + null `test_date_native`**, not a stale linkage artifact.

- After **145** on **`Thyroid 2026`**: **`main.molecular_testing`** populated (~10.9k rows, patient grain, slot 1); **`molecular_test_episode_v2`** rebuilt to match; **49** + **140** re-run; **119** release-mode: **12b PASS**, **12 FAIL** ( **`molecular_results`** still empty), specimen genomic review **WARN** still dominated by linkage/date gaps.

- **`41_ingest_thyroseq_excel.py`** (prod, cohort profile): **`131`** + **`117 --contract-views-only`**; **`41`** with **`--input-profile cohort_thyroseq_afirma_12_5`**, **`--md-sa`**, **`--md-env prod`** → **~10,862** **`main.molecular_results`** rows, **~1,640** variant-long rows; **119** release-mode **0 FAIL** (check 12 **PASS**).

## Explicitly not done (would be fuzzy or out of scope)

- No automatic resolution of `qa.specimen_genomic_link_review_v1` rows.
- No heuristic pairing of molecular episodes to FNA without governed dates.
- **Afirma:** **`42_ingest_afirma.py`** not run (no structured Afirma export in **`raw/`** this session).
