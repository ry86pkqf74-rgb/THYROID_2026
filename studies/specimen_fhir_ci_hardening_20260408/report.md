# Specimen / FHIR — offline CI hardening (2026-04-08)

## Objective

Close gaps between implemented specimen/FHIR scripts/tests and the **offline** GitHub Actions path (`multimodal-tests`), without MotherDuck secrets or writable local `thyroid_master.duckdb`.

## Before / after — `multimodal-tests` pytest matrix

### Before

| Module | In CI |
|--------|-------|
| `tests/test_multimodal_contract_mm_v1.py` | yes |
| `tests/test_imaging_fna_linkage_mm_v1.py` | yes |
| `tests/test_specimen_identity_layer.py` | yes |
| `tests/test_specimen_fhir_layer.py` | yes |
| `tests/test_specimen_fhir_qa_diagnostics.py` | yes |
| `tests/test_specimen_genomics_binding.py` | yes |
| `tests/test_specimen_fhir_release_gate.py` | **no** |
| `tests/test_specimen_fhir_scripts_offline.py` | yes |

### After

| Module | In CI |
|--------|-------|
| `tests/test_specimen_fhir_release_gate.py` | **yes** |
| `tests/test_specimen_fhir_scripts_offline.py` | yes (expanded; see below) |

`tests/test_specimen_identity_layer.py` was already in the workflow; it remains explicitly listed.

## Tests added (this change-set)

In `tests/test_specimen_fhir_scripts_offline.py`:

1. **`test_142_ddl_applies_directly_matches_143_path`** — Applies `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` directly on the same stub schema used for 143 (deploy-path parity with script 143).
2. **`test_144_collect_live_introspection_graceful_without_md_information_schema`** — Asserts `collect_live_introspection()` emits `current_database`, specimen/FHIR row-count bullets, `qa.release_manifest` lines, and a non-fatal telemetry note when `md_information_schema.query_history` / `recent_queries` are absent (file DuckDB).
3. **`test_144_build_markdown_includes_release_manifest_heading`** — Asserts `build_markdown()` includes the checked-in release-manifest section and preserves query-history fallback text.
4. **`test_138_orchestrator_dry_run_smoke`** — Subprocess: `scripts/138_md_specimen_fhir_layer.py --dry-run --study-dir <tmp>` exits 0 (no MotherDuck).

Workflow: `.github/workflows/ci.yml` — added `tests/test_specimen_fhir_release_gate.py` to the pytest invocation.

Docs: `docs/specimen_fhir_contract_review.md` — CI section updated to list all offline modules and 138 dry-run coverage.

## Commands run (validation)

```bash
cd "/Users/ros/THyroid 2026"
python3 -m py_compile tests/test_specimen_fhir_scripts_offline.py
python3 -m ruff check tests/test_specimen_fhir_scripts_offline.py --select F
python3 -m mypy tests/test_specimen_fhir_scripts_offline.py
python3 -m pytest \
  tests/test_multimodal_contract_mm_v1.py \
  tests/test_imaging_fna_linkage_mm_v1.py \
  tests/test_specimen_identity_layer.py \
  tests/test_specimen_fhir_layer.py \
  tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_release_gate.py \
  tests/test_specimen_fhir_scripts_offline.py \
  -v --tb=short
```

## Pass / fail output (local)

```
======================== 64 passed, 1 warning in 1.96s =========================
```

(Deprecation warning from `dateutil` in an unrelated code path.)

## Git commit (this hardening)

Exact object name for the commit that adds this report (run from a clone that contains this path):

```bash
git log -1 --format=%H -- studies/specimen_fhir_ci_hardening_20260408/report.md
```
