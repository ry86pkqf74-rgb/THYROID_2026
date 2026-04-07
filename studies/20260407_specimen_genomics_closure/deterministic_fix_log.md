# Deterministic fix log — specimen genomics closure

## Code / policy changes (repo)

1. **`scripts/49_enhanced_linkage_v3.py`**
   - Added `--md-sa` (prefer `MD_SA_TOKEN`), `--md-env`, and `--database` for MotherDuck alignment with other governed scripts.
   - Corrected `--md` help text (MotherDuck, not local file).
   - `connect_md()` now resolves database via `resolve_database_for_env` when appropriate.

2. **`scripts/119_md_formalization_validate.py`**
   - **Check 12 (release-mode):** If `main.molecular_results` is empty but `main.molecular_test_episode_v2` has rows → **FAIL** (was PASS with “skipped” semantics).
   - **Check 12b (release-mode):** If `molecular_test_episode_v2` is non-empty but `main.molecular_testing` is missing → **FAIL**, documenting that date spines and FNA–molecular linkage cannot work without script-22 source.

## MotherDuck operations (no fuzzy merges)

- Re-ran **`49_enhanced_linkage_v3.py --md --md-sa --md-env prod`** to rebuild v3 linkage tables on **`Thyroid 2026`**. No change to FNA–molecular cardinality: root cause is **missing `molecular_testing` + null `test_date_native`**, not a stale linkage artifact.

## Explicitly not done (would be fuzzy or out of scope)

- No automatic resolution of `qa.specimen_genomic_link_review_v1` rows.
- No heuristic pairing of molecular episodes to FNA without governed dates.
- No ThyroSeq/Afirma workbook ingest to `main.molecular_results` (inputs not in repo; PHI-bound).
