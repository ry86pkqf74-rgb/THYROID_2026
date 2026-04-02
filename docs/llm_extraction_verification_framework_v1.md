# LLM extraction and verification framework v1

## Components

- **Runner:** `llm_extraction/run_extraction.py` — orchestrates regex + optional `LLMExtractor`.
- **LLM module:** `llm_extraction/extract_llm.py` — OpenAI-compatible chat JSON; GitHub Models preferred when `GITHUB_TOKEN` is set, else `OPENAI_API_KEY`.
- **Telemetry:** `llm_extraction/run_telemetry.py` — thread-safe counters and `note_extraction_runs` persistence.
- **Verification:** Deterministic substring check of `evidence_text` against full note text; outcomes recorded in `verification_status` / `verification_step`.

## Model and prompt versioning

- **Model id** is stored per entity row as `model_name` / `model_version` (same string from the client configuration: e.g. `openai/gpt-4o-mini`).
- **Prompt version** is `'<relative_filename>|<12-char sha256>'` of the on-disk prompt file:
  - Non-operative: `prompts/lab_date_extraction_v1.txt`
  - Operative: `prompts/operative_note_extraction_v1.txt`
- If files are missing, prompt version falls back to `embedded_fallback|0` (default string in code).

## Verification steps

1. **LLM output:** Parse `entities[]` from JSON; invalid JSON → `parse_failures` in telemetry.
2. **Evidence locate:** For each entity, search `evidence_text` in the full note:
   - Found → `verification_status = verified_substring`, `verification_step = substring_ok`, `verifier_name = evidence_substring_verifier`, `verifier_version = 1.0`.
   - Not found → `rejected`, `verification_step = substring_check` (still attributed to verifier for audit).
3. **No evidence text** → `unverified`, `no_evidence_text`; verifier fields null.

## Failure taxonomy vs run log

| `failure_stage` (`note_extraction_runs`) | Meaning |
|------------------------------------------|---------|
| `none` | No LLM-level failures recorded in telemetry counters |
| `llm_disabled` | No API token; LLM path not invoked |
| `llm_api_error` | One or more API / transport failures (including exhausted retries) |
| `llm_parse_error` | One or more JSON / schema parse failures |

**Distinguishing outcomes**

- **`success`** in `note_extraction_runs` is `true` when the run finished without LLM API/parse failures (`failure_stage` is `none` or `llm_disabled`). It is `false` when `failure_stage` is `llm_api_error` or `llm_parse_error` (pipeline still wrote a row). It does **not** guarantee every note had LLM entities.
- **`failure_stage = llm_disabled`** with **`output_record_count = 0`** is a configuration state, not a failed HTTP call.
- Per-note API failures increment `api_failures` in the `warnings` JSON; inspect `retry_count` for aggregate rate-limit retries.

## Operational notes

- Parallel note workers (`--workers` > 1) require thread-safe telemetry (implemented via locks).
- Targeted reruns (`--target`, `--research-ids`) still append a full `note_extraction_runs` row describing that invocation.
- After any extraction change, rerun `scripts/103_fact_lineage_materialize.py` so canonical and quarantine parquets align with entity outputs.

## Related docs

- `docs/fact_provenance_contract_v1.md`
- `docs/final_clean_dataset_release_spec_v1.md`
- `docs/dataset_hardening_execution_plan_20260401.md`
