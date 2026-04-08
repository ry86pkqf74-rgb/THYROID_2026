# Specimen / FHIR CI hardening — offline coverage (2026-04-08)

## Scope

- `.github/workflows/ci.yml` — `multimodal-tests` job extended with identity + script offline tests; MotherDuck env vars cleared in that job.
- `scripts/141_fhir_specimen_json_export.py` — `--local-duckdb` for file DB; extracted `count_fhir_source_tables`, `run_export`.
- `scripts/144_md_repo_current_state_summary.py` — `--introspect-local`; extracted `collect_live_introspection`, `build_markdown`.
- `tests/test_specimen_fhir_scripts_offline.py` — offline tests for 141 / 143 / 144.
- `docs/specimen_fhir_contract_review.md` — CI section.

## Commands run (evidence)

Host: local dev env, Python 3.14.2 (`.venv`).

### py_compile

```text
python -m py_compile scripts/141_fhir_specimen_json_export.py \
  scripts/144_md_repo_current_state_summary.py \
  tests/test_specimen_fhir_scripts_offline.py
```
Exit 0.

### Ruff (pyflakes-equivalent F on changed files)

```text
ruff check scripts/141_fhir_specimen_json_export.py \
  scripts/144_md_repo_current_state_summary.py \
  tests/test_specimen_fhir_scripts_offline.py --select F
```
Exit 0 — `All checks passed!`

### Mypy (pyproject.toml scope)

```text
mypy
```
Exit 0 — `Success: no issues found in 60 source files`

### Pytest (CI multimodal subset + new file)

```text
MD_SA_TOKEN="" MOTHERDUCK_TOKEN="" LOCAL_DB_PATH="" python -m pytest \
  tests/test_multimodal_contract_mm_v1.py \
  tests/test_imaging_fna_linkage_mm_v1.py \
  tests/test_specimen_identity_layer.py \
  tests/test_specimen_fhir_layer.py \
  tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_scripts_offline.py \
  -v --tb=short
```

Result: **44 passed** (5.57s). One third-party `dateutil` DeprecationWarning from multimodal tests.

## CI gap audit (before change)

| Test file | Was in `multimodal-tests` | After |
|-----------|---------------------------|--------|
| `test_specimen_identity_layer.py` | No | Yes |
| `test_specimen_fhir_layer.py` | Yes | Yes |
| `test_specimen_fhir_qa_diagnostics.py` | Yes | Yes |
| `test_specimen_genomics_binding.py` | Yes | Yes |
| `test_specimen_fhir_scripts_offline.py` | N/A (new) | Yes |

`test_specimen_fhir_release_gate.py` remains out of this job (not requested).

## MotherDuck token

No workflow change for secrets: offline job clears `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN` / `LOCAL_DB_PATH`. Local dev tokens continue to resolve from env or `.streamlit/secrets.toml` per `motherduck_client.get_token()` (unchanged).
