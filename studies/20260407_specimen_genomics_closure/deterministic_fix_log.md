# Deterministic fix log — specimen genomics closure

## Code / policy changes (repo)

1. **`scripts/49_enhanced_linkage_v3.py`**
   - Added `--md-sa` (prefer `MD_SA_TOKEN`), `--md-env`, and `--database` for MotherDuck alignment with other governed scripts.
   - Corrected `--md` help text (MotherDuck, not local file).
   - `connect_md()` now resolves database via `resolve_database_for_env` when appropriate.

2. **`scripts/119_md_formalization_validate.py`**
   - **Check 12 (release-mode):** If `main.molecular_results` is empty but `main.molecular_test_episode_v2` has rows → **FAIL** (was PASS with “skipped” semantics).
   - **Check 12b (release-mode):** If `molecular_test_episode_v2` is non-empty but `main.molecular_testing` is missing → **FAIL**, documenting that date spines and FNA–molecular linkage cannot work without script-22 source.

3. **`scripts/145_md_materialize_molecular_testing.py`**
   - Materializes **`main.molecular_testing`** from **`raw/THYROSEQ_AFIRMA_12_5.xlsx`** on MotherDuck (long rows per assay column / slot), then applies **`MOLECULAR_TEST_EPISODE_V2_SQL`** so **`molecular_test_episode_v2`** matches script-22 semantics. **Does not** replace parquet-backed tables via `register_parquets`.

## MotherDuck operations (no fuzzy merges)

- Re-ran **`49_enhanced_linkage_v3.py --md --md-sa --md-env prod`** to rebuild v3 linkage tables on **`Thyroid 2026`**. No change to FNA–molecular cardinality: root cause is **missing `molecular_testing` + null `test_date_native`**, not a stale linkage artifact.

- After **145** on **`Thyroid 2026`**: **`main.molecular_testing`** populated (~10.9k rows, patient grain, slot 1); **`molecular_test_episode_v2`** rebuilt to match; **49** + **140** re-run; **119** release-mode: **12b PASS**, **12 FAIL** ( **`molecular_results`** still empty), specimen genomic review **WARN** still dominated by linkage/date gaps.

- **`41_ingest_thyroseq_excel.py`** was **not** run to prod for this closure: cohort xlsx lacks **`Req Patient/Source Name`** and other **41**-expected headers — **41** dry-run fails locally with **`KeyError`**. Governed **`molecular_results`** activation still requires the correct ThyroSeq export or a schema adapter.

## Explicitly not done (would be fuzzy or out of scope)

- No automatic resolution of `qa.specimen_genomic_link_review_v1` rows.
- No heuristic pairing of molecular episodes to FNA without governed dates.
- No successful ThyroSeq/Afirma **41**/**42** ingest to `main.molecular_results`: repo workbook **`THYROSEQ_AFIRMA_12_5.xlsx`** does not match **41**’s expected column layout; approved vendor export still required for governed layer.
