# V2 Parquet Provenance Gap Audit — 2026-04-02

## Scope

This audit compares the schemas of all parquet files currently staged under `processed/output/v2_parquets/` and treats the union of observed columns as the stricter current V2 staging schema.

Observed full column set:

- `domain`
- `extracted_at`
- `linkage_date`
- `llm_base_url`
- `llm_model`
- `note_date`
- `note_index`
- `note_row_id`
- `note_type`
- `preprocess_batch_id`
- `preprocess_script_version`
- `preprocessed_at_utc`
- `research_id`
- `result_json`
- `source_column`
- `source_sheet`
- `source_workbook`

The only stricter-schema fields still missing from some older copied artifacts are:

- `preprocess_batch_id`
- `preprocess_script_version`
- `preprocessed_at_utc`

## Provenance-complete staged files

These files already carry the full 17-column stricter schema:

- `note_entities_llm_operative_v2_enrichment.parquet`
- `note_entities_llm_parathyroid_per_gland.parquet`
- `note_entities_llm_recurrence.parquet`
- `note_entities_llm_staging.parquet`
- `note_entities_llm_tirads_granular.parquet`

## Lagging staged files

These files still use the older 14-column schema and are missing the 3 preprocessing fields:

| File | Column count | Missing stricter-schema columns |
| --- | ---: | --- |
| `note_entities_llm_combined.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_complications.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_genetics.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_imaging.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_labs.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_medications.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_pathology.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_physical_exam.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_problem_list.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |
| `note_entities_llm_procedures.parquet` | 14 | `preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc` |

## Conclusion

The staged V2 parquet set is still mixed-schema.

- 5 files are provenance-complete under the current stricter schema.
- 10 files are older copied artifacts that still lack the 3 preprocessing fields.

That is sufficient reason to keep `processed/output/v2_parquets/` as a live staging area rather than promoting it to a canonical frozen bundle.