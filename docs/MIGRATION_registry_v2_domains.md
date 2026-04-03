# Migration Note: Registry-Driven 28-Domain Architecture (2026-04-03)

## What Changed

### 1. Extraction Domain Registry (`config/extraction_domain_registry.yaml`)
- **Before**: 8 domains (staging, genetics, procedures, operative_detail, complications, medications, problem_list, llm).
- **After**: 28 domains — the original 8 v1 domains plus 20 new v2 domains sourced from LLM prompt files in `llm_extraction/prompts/`.
- The YAML is the **single source of truth** for the domain inventory. All downstream scripts now consume `llm_extraction.registry.load_registry()` instead of hardcoded lists.

### 2. Registry Loader (`llm_extraction/registry.py`)
- New typed Python API: `load_registry()`, `validate_registry()`, `DomainSpec`, `Registry`.
- Cached via `@lru_cache` — one YAML parse per process.

### 3. MotherDuck Hardening (`utils/md_connect.py`)
- `_resolve_md_token()` unified token resolution (env vars + toml fallback).
- `connect_md_or_file()` gains optional `env` parameter for dev/qa/prod routing.
- Script 77 bug fixed: `connect()` now delegates to `utils.md_connect.connect_md_or_file`.

### 4. Registry-Driven Scripts
| Script | Change |
|--------|--------|
| `llm_extraction/run_extraction.py` | `DOMAIN_TO_FILE` loaded from registry |
| `scripts/02b_register_notes_entities.py` | `ENTITY_TABLES` + `CANONICAL_AND_RUN_TABLES` from registry |
| `scripts/09b_fabric_upload_notes_entities.py` | `DOMAIN_TO_FILE` + `CANONICAL_RELEASE_STEMS` from registry |
| `scripts/103_fact_lineage_materialize.py` | `ENTITY_DOMAIN_MAP` from registry; produces both v1 and v2 canonical outputs |
| `scripts/111_llm_extraction_validation.py` | `existing_note_entities()` target_domains from registry |
| `llm_extraction/extract_llm.py` | `_load_system_prompt()` accepts `domain=` kwarg for registry-routed prompt selection |

### 5. Canonical Output v2 (`canonical_extracted_fact_long_v2`)
- Script 103 now writes **both** `canonical_extracted_fact_long_v1.parquet` (v1 domains only) and `canonical_extracted_fact_long_v2.parquet` (all 28 domains).
- v1 is preserved for backward compatibility; v2 is additive.

## Backward Compatibility

- **v1 artifacts untouched**: `canonical_extracted_fact_long_v1`, `canonical_fact_quarantine_v1`, `note_extraction_runs` — all existing tables and Parquet files continue to be produced with identical content.
- **Hardcoded fallback**: Every registry consumer wraps the import in `try/except` with a hardcoded v1 fallback. If `pyyaml` is missing or the YAML is malformed, all scripts revert to the original 8-domain behavior.
- **No schema changes**: v2 domains use the same `ENTITY_SCHEMA_COLUMNS` (55-column) contract as v1.
- **No DuckDB table renames**: v1 tables keep their names (`note_entities_staging`, etc.). v2 domains use `note_entities_llm_<domain>` naming to avoid collisions.

## How to Add a New Domain

1. Add a new entry under `domains:` in `config/extraction_domain_registry.yaml`.
2. Create the prompt file under `llm_extraction/prompts/`.
3. Run `llm_extraction/run_extraction.py` — the new domain will be included automatically.
4. Run `scripts/103_fact_lineage_materialize.py` — the domain will appear in `canonical_extracted_fact_long_v2`.

## Deployment Order

No deployment order change. The registry is a configuration file consumed at import time.
