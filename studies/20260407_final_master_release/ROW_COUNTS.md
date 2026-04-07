# Row counts — live `main` / `qa` at validation (2026-04-07)

Values from `metrics_snapshot.json` (MotherDuck query after final corrections).

| Object | Rows |
|--------|-----:|
| `main.canonical_extracted_fact_long_v2` | 123,577 |
| `main.canonical_fact_quarantine_v2` | 199 |
| `main.note_extraction_runs` | 3 |
| `main.longitudinal_lab_canonical_v1` | 76,971 |
| `main.thyroglobulin_lab_canonical_v1` | 76,971 |
| `main.master_fact_long_verified_v1` | 123,577 |
| `main.master_patient_rollup_verified_v1` | 5,574 |
| `main.master_source_lineage_v1` | 123,577 |
| `qa.manual_review_queue` | 5,622 |

Parquet bundle totals (33 files) are summarized in `parquet_bundle_manifest.json` in this directory.
