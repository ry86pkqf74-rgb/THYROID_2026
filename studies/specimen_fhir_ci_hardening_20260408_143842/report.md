# Specimen / FHIR CI hardening (2026-04-08 UTC)

## Summary

- **Workflow:** `.github/workflows/ci.yml`
- **Goal:** Blocking offline coverage for specimen identity, FHIR layer, QA diagnostics, genomics binding, and scripts **141 / 143 / 144** without MotherDuck secrets.

## Tests ↔ CI mapping

| Test module | Role | CI job |
|-------------|------|--------|
| `tests/test_specimen_identity_layer.py` | Identity / fingerprint layer | `llm-extraction-gold` |
| `tests/test_specimen_fhir_layer.py` | FHIR resource / tail DDL contracts | `llm-extraction-gold` |
| `tests/test_specimen_fhir_qa_diagnostics.py` | QA diagnostic view contracts | `llm-extraction-gold` |
| `tests/test_specimen_genomics_binding.py` | Genomics binding rules | `llm-extraction-gold` |
| `tests/test_specimen_fhir_scripts_offline.py` | **141** NDJSON export + reconstruct paths; **143** local `--db-path` deploy; **144** `--introspect-local`; **142** DDL direct apply; **138** dry-run smoke | `llm-extraction-gold` |
| `tests/test_specimen_fhir_release_gate.py` | Release-gate / writer attribution surface | `multimodal-tests` |
| `tests/test_multimodal_contract_mm_v1.py` | Multimodal schema contract | `multimodal-tests` |
| `tests/test_imaging_fna_linkage_mm_v1.py` | Imaging ↔ FNA linkage | `multimodal-tests` |

## Design choices

1. **No secrets in offline jobs:** `llm-extraction-gold` and `multimodal-tests` clear `MD_SA_TOKEN`, `MOTHERDUCK_TOKEN`, and `LOCAL_DB_PATH`. Script **141** is covered via `duckdb` file DB + `--local-duckdb` subprocess and `run_export()` imports; **143** via `--db-path` + `--skip-snapshot`; **144** via `--introspect-local --db-path`.

2. **141 CLI:** New `test_141_cli_local_duckdb_subprocess` exercises `parse_args()` / `main()` path with exactly one of `--md | --read-scaling | --local-duckdb`.

3. **Push triggers:** `studies/**` added to `on.push.paths` so study artifacts (including this report) can trigger CI when pushed to `main`.

4. **Dedup:** Specimen core tests moved from `multimodal-tests` into `llm-extraction-gold` so one blocking job runs LLM offline contracts and specimen/FHIR together; `multimodal-tests` keeps mm contract, imaging↔FNA, and `test_specimen_fhir_release_gate.py`.

## Local verification (maintainer)

```bash
python -m py_compile scripts/141_fhir_specimen_json_export.py scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py scripts/144_md_repo_current_state_summary.py
ruff check tests/test_specimen_fhir_scripts_offline.py
mypy
python -m pytest \
  tests/test_specimen_identity_layer.py \
  tests/test_specimen_fhir_layer.py \
  tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_scripts_offline.py \
  tests/test_specimen_fhir_release_gate.py \
  tests/test_multimodal_contract_mm_v1.py \
  tests/test_imaging_fna_linkage_mm_v1.py \
  -v --tb=short
```

MotherDuck tokens for interactive runs: use `motherduck.local.toml` from `motherduck.local.toml.example` (gitignored); CI jobs above stay env-cleared.