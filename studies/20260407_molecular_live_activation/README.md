# Molecular layer live activation (MotherDuck dev)

**Date:** 2026-04-07 (UTC)  
**Target database:** `Thyroid 2026 Molecular Dev 20260407` (`--md-env dev` per `config/motherduck_environments.yml`)  
**Attribution:** set `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_molecular_live_activation/1.0` and `MOTHERDUCK_SESSION_HINT=molecular_live_activation_<UTC_YYYYMMDD_HHMM>` for each session.

## What was wrong

- `main.molecular_results` was **empty** because the governed ingest scripts (`41_ingest_thyroseq_excel.py`, `42_ingest_afirma.py`) had not been executed against this catalog after the molecular DDL existed—not a contract-view bug.
- Release validation then **skipped** molecular contract checks when row count was zero (`scripts/119_md_formalization_validate.py`).

## Approved / governed inputs used

| Assay    | Path | Notes |
|----------|------|--------|
| ThyroSeq | `inputs/thyroseq_governed_dev_7508.xlsx` | Minimal workbook; cohort key `research_id = 7508` from live `patient_analysis_resolved_v1` (non-PHI identifier only in this doc). |
| Afirma   | `inputs/afirma_structured_governed_7508.csv` | Same `research_id`; classifier strings aligned to `molecular_code_crosswalk` (e.g. `Benign`, `Suspicious`). |

Synthetic placeholder patient text fields in the ThyroSeq workbook avoid institutional PHI; linkage is **explicit** via `Research ID number` (see code change in script 41).

## Commands (replay)

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular_live_activation/1.0"
export MOTHERDUCK_SESSION_HINT="molecular_live_activation_$(date -u +%Y%m%d_%H%M)"

.venv/bin/python scripts/131_molecular_results_layer.py --execute --md --md-env dev
.venv/bin/python scripts/117_md_contract_views.py --md --md-env dev --contract-views-only
.venv/bin/python scripts/41_ingest_thyroseq_excel.py --md --md-env dev \
  --input studies/20260407_molecular_live_activation/inputs/thyroseq_governed_dev_7508.xlsx
.venv/bin/python scripts/42_ingest_afirma.py --md --md-env dev \
  --input studies/20260407_molecular_live_activation/inputs/afirma_structured_governed_7508.csv
.venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute --md --md-env dev
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env dev --release-mode \
  --output-dir studies/20260407_molecular_live_activation
```

## Outcome

- `main.molecular_results`: one row each for `41_thyroseq_excel_workbook` and `42_afirma_structured_file` for `research_id` 7508 (non–zero contract slice; validation exercises checksum, provenance, variant enum, allele-fraction checks).
- `main.molecular_fact_long_v`: structured rows show `fact_provenance_category = assay_structured_import`, `source_table` / `test_date_native` / `molecular_result_id` / variant linkage where applicable (see `validation_report.md`).
- To promote to **QA** or **prod**, re-run the same ingest sequence against `--md-env qa` or prod after policy sign-off (`MOTHERDUCK_DATABASE` must name the writable catalog).
