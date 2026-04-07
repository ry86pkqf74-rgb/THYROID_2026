# Molecular assay dictionary / crosswalk — backlog (2026-04-07)

**Priority:** Deferred relative to manuscript publication gates (see [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../20260407_publication_signoff_live/final_verdict_memo.md)).

## Live MotherDuck profile (`md:Thyroid 2026`)

See [`live_profile_snapshot.md`](live_profile_snapshot.md):

- `main.molecular_results`: **0** rows → no distinct `assay_name` / classifier strings to profile in-cloud.
- `main.molecular_assay_dictionary`: **0** rows.
- `main.molecular_code_crosswalk`: **17** rows (baseline seed only).

## Before / after metrics (exact-match curation)

| Metric | Before (live) | After |
|--------|----------------|-------|
| `molecular_results` rows | 0 | N/A (no backfill run) |
| Unmapped assay values | N/A | N/A |
| `null` loinc_code rows | N/A | N/A |

## TODO (when molecular_results is populated)

1. Profile distinct: `assay_name`, `panel_version`, `platform`, `vendor`, `interpretation`, raw classifier / variant strings (no fuzzy mapping).
2. Audit `molecular_assay_dictionary` + `molecular_code_crosswalk` coverage vs those distinct sets.
3. Apply **exact-match-only** dictionary updates; institution-specific LOINC only when definitively known.
4. Re-run this memo with before/after SQL exports (counts only; no raw note text).

## Scripts / docs (repo)

- `docs/AFIRMA_INGEST.md`, `docs/MOLECULAR_FACT_LINEAGE.md`, `data_dictionary.md`
- `scripts/131_molecular_results_layer.py`, `scripts/41_ingest_thyroseq_excel.py`, `scripts/42_ingest_afirma.py`
