# Fact provenance contract v1

## Scope

Applies to:

- `canonical_extracted_fact_long_v1` (clean)
- `canonical_fact_quarantine_v1` (clean schema **plus** `quarantine_reason`, `quarantine_date`)

## Identity and linkage

| Column | Semantics |
|--------|-----------|
| `fact_id` | Stable MD5 over `(research_id, note_row_id, entity_type, entity_value_raw, fact_domain, row_ordinal)` **excluding** run-specific fields |
| `research_id` | Patient identifier |
| `note_row_id` | Key into `clinical_notes_long` |
| `fact_domain` | Source entity table domain (`staging`, `genetics`, …, `llm`) |
| `canonical_domain` | Frozen alias of `fact_domain` for external contracts |
| `canonical_fact_type` | Frozen alias of `entity_type` |
| `inferred_surgery_episode_id` | Nearest `operative_episode_detail_v2.surgery_episode_id` by date distance |
| `inferred_surgery_date` | Surgery date for that episode |
| `ep_distance_days` | Absolute day distance between reference date (`entity_date` else `clin_note_date`) and inferred surgery date |
| `surgery_key` | Composite key string for episode-aware joins |
| `linkage_confidence` | **0–1** heuristic: higher when episode match is close in time; discounted for multi-surgery patients and large `ep_distance_days` |

## Source document provenance

| Column | Semantics |
|--------|-----------|
| `source_file_id` | Workbook file id for release metrics; populated from `clin_source_workbook` after notes merge |
| `clin_source_workbook`, `clin_source_sheet`, `clin_source_column`, `clin_excel_row_0based`, `clin_note_date` | From `clinical_notes_long` merge |

## Extraction run / model

| Column | Semantics |
|--------|-----------|
| `extraction_run_id` | UUID for one `run_extraction.py` invocation |
| `extractor_name` | Python class name (e.g. `StagingExtractor`, `LLMExtractor`) |
| `extractor_version` | `EXTRACTOR_BUILD_VERSION` in `llm_extraction/vocab.py` / registry `schema_version` |
| `extraction_method` | `regex`, `llm_github_models`, `llm_openai`, etc. |
| `model_name`, `model_version` | LLM API model id; null for regex |
| `prompt_version` | `'<file>|<sha256-12>'` for LLM prompts on disk; `regex_only` for regex |
| `extracted_at` | UTC ISO timestamp per entity row |

## Evidence and text span

| Column | Semantics |
|--------|-----------|
| `evidence_span` | Snippet from note (length-capped in LLM path) |
| `evidence_start`, `evidence_end` | Offsets within chunk where applicable |
| `evidence_global_start`, `evidence_global_end` | Character offsets in full note when located |
| `source_text_span_start`, `source_text_span_end` | Contract copy of global span (nullable) |
| `source_text_hash` | SHA-256 hex of trimmed `evidence_span` (null if empty) |
| `source_line` | 1-based line hint from LLM JSON when present |

## Dates and confidence

| Column | Semantics |
|--------|-----------|
| `entity_date` | Normalised `YYYY-MM-DD` when valid |
| `note_date` | Encounter / note header date from long table |
| `date_confidence` | LLM 0–1 score; null for pure regex |
| `date_source_type` | `regex_extractor` \| `explicit_lab` \| `note_body` \| `encounter_fallback` \| `unknown` (derived in materializer) |

## Verification (LLM)

| Column | Semantics |
|--------|-----------|
| `verification_status` | e.g. `verified_substring`, `rejected`, `unverified` |
| `verification_step` | e.g. `substring_ok`, `substring_check`, `no_evidence_text` |
| `verifier_name` | `evidence_substring_verifier` when span checked |
| `verifier_version` | `1.0` |
| `raw_response_sha256` | Hash of raw LLM JSON for the note response |

## Quarantine extensions

| Column | Semantics |
|--------|-----------|
| `quarantine_reason` | `multi_surgery_episode_ambiguous` \| `low_confidence_llm_date` \| `temporal_conflict_entity_vs_surgery` |
| `quarantine_date` | UTC date (ISO) when materializer ran |

## Reference

- Materializer: `scripts/103_fact_lineage_materialize.py`
- Schema columns: `llm_extraction/vocab.py` → `ENTITY_SCHEMA_COLUMNS`
- Machine-readable map: `config/extraction_domain_registry.yaml`
