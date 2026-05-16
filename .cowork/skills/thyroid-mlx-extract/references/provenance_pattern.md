# Provenance pattern — required BQ columns

Every row written to `pub_canonical.note_entities_llm_*` must include these columns. They mirror the schema used by your existing `note_entities_complications`, `note_entities_llm_pathology`, etc., so `canonical_table_signoff_registry_v1` and `cowork_sf_validation_log_v1` work unchanged.

## Required columns

| Column | Type | Source | Notes |
|---|---|---|---|
| `research_id` | STRING | source row | Always present |
| `source_pk` | STRING | source row | Task-specific PK from `bq/pull.py` |
| `entity_domain` | STRING | `TaskSpec.domain` | e.g. 'molecular', 'pathology' |
| `event_date` | DATE | source row | Surgery date, scan date, FNA date, etc. |
| `result_json` | STRING | extractor output | Full Pydantic schema dump as JSON |
| `extraction_run_id` | STRING | `provenance.new_run_id()` | `<task>_<model>_<utc>_<short_uuid>` |
| `extractor_name` | STRING | constant | `thyroid-mlx-extract` |
| `extractor_version` | STRING | package version | e.g. `0.1.0` |
| `model_name` | STRING | `ModelSpec.key` | e.g. `medgemma27b` |
| `model_version` | STRING | `ModelSpec.hf_repo` | Full HuggingFace repo path |
| `prompt_version` | STRING | template version | Bump on prompt edits |
| `llm_provider` | STRING | constant | `mlx-community` |
| `llm_sdk` | STRING | constant | `mlx-lm` |
| `llm_sdk_version` | STRING | runtime | `mlx_lm.__version__` |
| `raw_response_sha256` | STRING | `provenance.sha256(raw_text)` | Deterministic audit trail |
| `verification_status` | STRING | adjudicator | `primary_only`, `agreed`, `disagreed`, `both_failed` |
| `confidence_score` | FLOAT | schema | Model-reported confidence (0–1) |
| `extraction_timestamp_utc` | TIMESTAMP | `provenance.utc_now_iso()` | When this row was extracted |
| `elapsed_seconds` | FLOAT | runtime | Wall-clock per extraction |

## Versioning rules

- **Bump `prompt_version`** every time you edit a prompt file. Same model + different prompt is a different extraction.
- **Bump output table version (`_v1` → `_v2`)** when you change the Pydantic schema. Old rows stay in `_v1`; new schema goes to `_v2`. Don't migrate; analyst joins resolve this via `canonical_*` views.
- **`extraction_run_id` is immutable** — never reuse, always uniquely identifies a single execution.

## Workspace → canonical promotion

1. `thyroid-mlx push <task> --workspace` writes to `pub_workspace.note_entities_llm_<task>_<v>`
2. Analyst review (manual or scripted QC against `extracted_*_v*` derived tables)
3. Bulk insert from workspace → canonical, then signoff registry update
4. Add row to `canonical_table_signoff_registry_v1` with the extraction_run_id range

This matches the existing pattern; the skill should never write directly to `pub_canonical` on a first run.
