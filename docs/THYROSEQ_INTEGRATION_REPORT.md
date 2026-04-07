# ThyroSeq Integration Report

**Generated:** 2026-04-07 01:59  
**Batch ID:** `4d91c336-439`  
**Git SHA:** `bb24165`  

_This file is overwritten on every `41_ingest_thyroseq_excel.py` run. The metrics below reflect the most recent local/MotherDuck validation run committed with the integration changes, not the full cohort workbook._

## Prerequisites (normalized layer)

Governed tables `molecular_results` and `molecular_variant_long` must exist on the target database. Apply DDL once per environment:

```text
.venv/bin/python scripts/131_molecular_results_layer.py --execute
.venv/bin/python scripts/131_molecular_results_layer.py --execute --md --md-env dev
```

Local `--local` runs default to `thyroid_master_local.duckdb` unless `LOCAL_DUCKDB_PATH` points at the file where 131 was applied (often `thyroid_master.duckdb`).

The ingest step **replaces** all rows with `source_table = 41_thyroseq_excel_workbook` in `molecular_results` (and tied variants) on each successful execute, then reloads from the current workbook.

## Summary Metrics

| Metric | Count |
|--------|-------|
| Source rows ingested | 1 |
| High-confidence matches | 1 |
| Manual review required | 0 |
| Unmatched rows | 0 |
| Molecular enrichment rows | 1 |
| Normalized molecular_results | 1 |
| Normalized molecular_variant_long | 3 |
| Follow-up lab rows | 0 |
| Follow-up event rows | 1 |
| Fill actions | 0 |
| Conflicts | 0 |
| Parse failures | 0 |

## Match Method Breakdown

| Method | Count |
|--------|-------|
| exact_mrn_dob_name | 1 |

## Output Tables

| Table | Description |
|-------|-------------|
| `stg_thyroseq_excel_raw` | Raw staging with all original columns + identifiers |
| `stg_thyroseq_match_results` | Patient matching results |
| `stg_thyroseq_parsed` | Parsed/normalized fields |
| `thyroseq_molecular_enrichment` | Molecular findings (staging / legacy wide flags) |
| `molecular_results` | Governed assay envelope (one row per ThyroSeq test; normalized layer) |
| `molecular_variant_long` | Governed atomic variants (SNV / FUSION / CNV per call) |
| `thyroseq_followup_labs` | Serial Tg/TgAb/TSH values (long format) |
| `thyroseq_followup_events` | Surgery/RAI/imaging events (long format) |
| `thyroseq_fill_actions` | Audit log of field fills |
| `thyroseq_review_queue` | Items requiring manual review |

## Normalized export artifacts

CSV mirrors of the governed layer (same ingest batch):

- `molecular_results.csv` — assay envelope + `payload_checksum` + workbook provenance in JSON
- `molecular_variant_long.csv` — long variants with allele fractions (0..1) and QC flags

## Export Directory

`exports/thyroseq_integration_20260407_0158/`
