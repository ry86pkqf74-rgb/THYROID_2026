# Migration: Registry-Driven Multi-Domain Extraction

**Date:** 2026-04-03  
**Schema version:** `entity_schema_v3_2026-04-03`

## Summary

The extraction pipeline has been upgraded from a hardcoded 8-domain map to a
registry-driven 28-domain system. All domain metadata is now defined in a
single YAML file and consumed by extractors, runners, and downstream scripts
through the `llm_extraction.registry` module.

## What changed

### 1. `config/extraction_domain_registry.yaml` (v2 → v3)

Every domain now carries five new fields:

| Field | Type | Purpose |
|---|---|---|
| `note_scope` | `all \| op_note \| path_report` | Which note types the domain is relevant to |
| `canonical_target` | string | DuckDB table / parquet stem used as the canonical landing zone |
| `linkage_anchor_family` | enum | Cross-domain linkage group (pathology, molecular, operative, imaging, rai, followup, demographics, audit) |
| `dedupe_key` | list[str] | Columns that define row uniqueness for deduplication |
| `qa_tier` | `critical \| standard \| informational \| debug` | QA severity classification |

Schema version bumped to `entity_schema_v3_2026-04-03`.

### 2. `llm_extraction/registry.py`

- `DomainSpec` dataclass extended with the five new fields and convenience
  properties (`is_operative_scoped`, `is_path_report_scoped`).
- `Registry` gains query helpers: `domains_for_note_scope()`,
  `domains_by_qa_tier()`, `domains_by_linkage_family()`, and
  `resolve_domain()` (raises `ValueError` for unknown domains).
- `validate_registry()` expanded: checks `note_scope`, `qa_tier`,
  `linkage_anchor_family` against allowed value sets; verifies every LLM
  domain has at least one prompt; verifies `canonical_target` is populated.

### 3. `llm_extraction/extract_llm.py`

- `extract()` and the full LLM call chain (`_call_llm`, `_build_prompt`,
  `_parse_llm_response`) now accept an optional `domain` keyword argument.
- When `domain` is set, the registry-resolved prompt file is used instead
  of the operative/general fallback chain.
- Backward compatible: omitting `domain` preserves the existing behaviour.

### 4. `llm_extraction/run_extraction.py`

- `DOMAIN_TO_FILE` is now populated directly from the registry at import
  time (28 entries). A legacy 8-domain fallback dict is retained for
  documentation but is never used when the registry loads successfully.
- `--target` validation uses `Registry.resolve_domain()` — unknown domains
  produce an immediate, descriptive error.
- New `--merge-audit` flag writes a merged `note_entities_llm.parquet`
  combining all LLM domain outputs as a debugging/audit artifact.
- New `--validate-only` flag runs registry validation and exits.
- Telemetry now logs `registry_version` in the run metadata.

### 5. Tests (`tests/test_registry_and_md_connect.py`)

- New `TestRegistryV3Fields` class covers all five new fields across all
  28 domains.
- New `TestRegistryValidation` class covers prompt existence, output stem
  resolution, unknown-domain rejection, and LLM-prompt completeness.
- Existing tests updated for schema v3 prefix.

## Backward compatibility

- **Output filenames unchanged.** All existing parquet stems (`note_entities_staging`,
  `note_entities_genetics`, etc.) are preserved.
- **`--target DOMAIN` unchanged.** All v1 domain names continue to work.
- **`note_entities_llm` audit artifact.** No longer written by default.
  Pass `--merge-audit` to restore the merged audit parquet.
- **Downstream consumers** (`02b_register`, `103_fact_lineage`,
  `09b_fabric`) already read from the registry and will pick up the new
  domains automatically.

## Validation

Run the registry validation standalone:

```bash
.venv/bin/python llm_extraction/run_extraction.py --validate-only
```

Run the test suite:

```bash
.venv/bin/python -m pytest tests/test_registry_and_md_connect.py -v
```
