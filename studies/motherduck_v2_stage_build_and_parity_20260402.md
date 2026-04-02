# MotherDuck V2 Stage Build And Parity — 2026-04-02

Generated at: 2026-04-02T09:08:13.337752+00:00

## Build target

- MotherDuck catalog: `Thyroid 2026`
- MotherDuck schema: `v2_stage`
- Build method: DuckDB CLI `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_parquet(...)`
- Source set: 14 domain parquets plus `note_entities_llm_combined.parquet` from `processed/output/v2_parquets/`

## Result

- Built tables in MotherDuck `v2_stage`: 15
- CLI canary write succeeded before the bulk load.
- All staged tables loaded successfully into the visible `Thyroid 2026` catalog.
- Legacy repo-configured targets remain stale: `thyroid_research_2026` and the old RO share path still fail to attach for this token.

## Parity by table

| Table | Local rows | MotherDuck rows | Schema cols | Missing stricter provenance cols | Status |
| --- | ---: | ---: | ---: | --- | --- |
| `note_entities_llm_combined` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_complications` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_genetics` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_imaging` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_labs` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_medications` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_operative_v2_enrichment` | 11037 | 11037 | 17 | none | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_parathyroid_per_gland` | 11037 | 11037 | 17 | none | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_pathology` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_physical_exam` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_problem_list` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_procedures` | 11037 | 11037 | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_recurrence` | 11037 | 11037 | 17 | none | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_staging` | 11037 | 11037 | 17 | none | `row-parity-ok; schema-parity-ok` |
| `note_entities_llm_tirads_granular` | 11037 | 11037 | 17 | none | `row-parity-ok; schema-parity-ok` |

## Provenance mismatch summary

- Provenance-complete staged files: 5
  - `note_entities_llm_operative_v2_enrichment.parquet`
  - `note_entities_llm_parathyroid_per_gland.parquet`
  - `note_entities_llm_recurrence.parquet`
  - `note_entities_llm_staging.parquet`
  - `note_entities_llm_tirads_granular.parquet`
- Lagging staged files: 10
  - `note_entities_llm_combined.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_complications.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_genetics.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_imaging.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_labs.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_medications.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_pathology.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_physical_exam.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_problem_list.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`
  - `note_entities_llm_procedures.parquet` missing: `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`

## Interpretation

- MotherDuck is now usable as a writable staging target through the visible `Thyroid 2026` catalog.
- The successful build does not rehabilitate the legacy repo wiring: `motherduck_client.py` still points at a non-existent RW database and a non-existent RO share for this token.
- The `v2_stage` tables are parity mirrors of the local staged parquets, not canonical reconciled thyroid tables.
- Promotion should still remain blocked until the legacy thyroid access path is corrected or intentionally replaced and the lagging provenance columns are normalized for the older copied artifacts.