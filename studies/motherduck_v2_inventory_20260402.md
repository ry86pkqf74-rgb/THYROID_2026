# MotherDuck V2 Inventory / Consistency Sweep — 2026-04-02

## Outcome

MotherDuck-side reconciliation is currently blocked by auth/catalog scope, not by query logic.

The only repo-visible token context exposed these catalogs:

- `md_information_schema`
- `my_db`
- `rosflow`
- `sample_data`

`my_db` is empty in the current session, and the expected thyroid catalog (`thyroid_research_2026`) is not visible from any repo-visible token source.

## Auth paths tested

Without printing secret values, the following repo-visible auth contexts were probed:

| Auth source | Result |
| --- | --- |
| `LOCAL_DB_PATH` | missing |
| `MD_SA_TOKEN` | missing |
| `MOTHERDUCK_TOKEN` | connected, but only generic/sample catalogs visible |

## Local V2 parquet inventory

The accessible local inventory under `output/v2_parquets/` contains 15 parquet files, including the legacy combined file and 14 domain files.

| File | Rows | Unique `note_row_id` | `source_sheet` present | `source_column` present |
| --- | ---: | ---: | --- | --- |
| `note_entities_llm_combined.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_complications.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_genetics.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_imaging.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_labs.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_medications.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_operative_v2_enrichment.parquet` | 11,037 | 11,037 | yes | yes |
| `note_entities_llm_parathyroid_per_gland.parquet` | 11,037 | 11,037 | yes | yes |
| `note_entities_llm_pathology.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_physical_exam.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_problem_list.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_procedures.parquet` | 11,037 | 11,037 | no | no |
| `note_entities_llm_recurrence.parquet` | 11,037 | 11,037 | yes | yes |
| `note_entities_llm_staging.parquet` | 11,037 | 11,037 | yes | yes |
| `note_entities_llm_tirads_granular.parquet` | 11,037 | 11,037 | yes | yes |

## Consistency findings

- All inventoried local parquets are row-complete at 11,037 rows and 11,037 unique `note_row_id` values.
- The older domain artifacts are not provenance-complete by the stricter V2 standard: several still lack `source_sheet` and `source_column`.
- Because the accessible MotherDuck catalog is empty/non-thyroid, there is currently nothing authoritative to compare these local artifacts against in-cloud.

## Canonical-promotion recommendation

Do not promote the current local V2 parquet set into a more canonical tracked/exported location yet.

Reason:

1. MotherDuck cannot currently serve as the reconciliation target.
2. Provenance completeness is inconsistent across the local V2 artifacts.
3. The extraction fleet is still in-flight on several high-value tail domains.

Recommended next promotion gate:

1. Restore access to the actual thyroid MotherDuck catalog or share.
2. Re-run the inventory against that real target.
3. Normalize missing provenance on any older copied parquet artifacts.
4. Only then freeze a canonical bundle from `output/v2_parquets/` into a dated manifest-based export location.
