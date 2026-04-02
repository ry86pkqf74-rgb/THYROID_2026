# Local vLLM Hardening Validation — 2026-04-02

## What changed

The local vLLM wrapper was hardened to:

- validate structured JSON with Pydantic
- batch note processing in groups of 50 for local qwen-30b-class runs
- stamp deterministic row-level fields into output parquet rows
- force the local output contract fields required for downstream review

## Validation note

No live local OpenAI-compatible endpoint was available during this session:

- `http://localhost:8000/v1/models` refused connection
- `http://localhost:11434/v1/models` refused connection

Because of that, the wrapper was validated with a deterministic fake OpenAI-compatible client that exercised the real local code path: prompt building, Pydantic validation, `EntityMatch` conversion, row stamping, DataFrame schema gating, and parquet write.

## 10-note test result

Test artifact:

- `studies/vllm_local_hardening_20260402/note_entities_llm_test10.parquet`

Observed summary:

| Metric | Value |
| --- | --- |
| Rows written | 10 |
| Unique `note_row_id` | 10 |
| Configured batch size | 50 |
| Required columns present | yes |
| Missing `research_id` | 0 |
| Missing `episode_id` | 10 |
| Missing `note_row_id` | 0 |
| Missing `evidence_span` | 0 |
| Missing `extraction_timestamp_utc` | 0 |
| Missing `llm_model` | 0 |
| Missing `llm_prompt_version` | 0 |
| Missing `confidence_score` | 0 |
| `verification_status` values | `pending` |
| `llm_model` values | `qwen-30b-instruct-vLLM` |

## Schema verification

The parquet was read back directly with PyArrow. Key on-disk fields now include:

- `research_id`
- `note_row_id`
- `episode_id`
- `note_type`
- `note_index`
- `source_sheet`
- `source_column`
- `confidence_score`
- `extraction_timestamp_utc`
- `llm_model`
- `llm_prompt_version`
- `verification_status`

## Important caveat

`episode_id` is currently null in the local 10-note test because the source parquet used by the local wrapper (`processed/clinical_notes_long.parquet`) does not contain an `episode_id` column. The wrapper now preserves the field explicitly, but it cannot fabricate it.
