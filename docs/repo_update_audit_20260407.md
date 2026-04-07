# Repository current-state audit (2026-04-07)

Static review of contract docs, orchestrators, connection helpers, CI, and tests, plus **one live MotherDuck read-only probe** (no writes). This file records evidence as-of the audit commit; reruns supersede it.

## Scope (files inspected)

- `README.md`, `AGENTS.md`
- `config/extraction_domain_registry.yaml`, `config/motherduck_environments.yml`
- `docs/motherduck_database_contract_v1.md`, `docs/release_runbook.md`
- `llm_extraction/run_extraction.py`
- `scripts/111_llm_extraction_validation.py`, `scripts/119_md_formalization_validate.py`, `scripts/124_md_live_release_audit.py`, `scripts/126_final_master_release.py`, `scripts/137_md_molecular_release_workflow.py`, `scripts/138_md_specimen_fhir_layer.py` (referenced via 119/124), `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py`
- `utils/md_connect.py`, `motherduck_client.py`
- `.github/workflows/ci.yml`
- Representative tests: `tests/test_v2_domain_fanout_and_validation.py`, `tests/test_multimodal_contract_mm_v1.py`, `tests/test_specimen_fhir_layer.py`, `tests/test_specimen_fhir_qa_diagnostics.py`, `tests/test_imaging_fna_linkage_mm_v1.py`, `tests/test_specimen_genomics_binding.py`

## Live MotherDuck probe (read-only, prod catalog)

Executed from this repo with a resolved RW token; **SELECT only** for catalog introspection:

| Object | Result |
|--------|--------|
| `md_information_schema.databases` | Accessible |
| `md_information_schema.snapshots` | **Not present** (catalog error; hint referenced `storage_info_history`) |
| `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` | Accessible (**186** rows) |
| `md_information_schema.query_log` | **Not present** (hint: `query_history`) |
| `md_information_schema.query_history` | Accessible; columns include `start_time`, `query_text`, `user_agent`, … |
| `md_information_schema.recent_queries` | Accessible |

**Code change from audit:** `scripts/124_md_live_release_audit.py` previously queried `snapshots` and `query_log`; it now prefers `DATABASE_SNAPSHOTS` and `query_history`, with fallbacks documented in-script.

## Previously reported risks — resolution status (from repo evidence)

### 1. v2 per-domain fan-out

**Resolved in code and tests.** `llm_extraction/run_extraction.py` documents LLM v2 fan-out and iterates `reg.v2_domains` on full runs. `tests/test_v2_domain_fanout_and_validation.py` asserts unique `note_entities_llm_*` stems and that `run_llm_for_domain` passes `domain=` and stamps `entity_domain`.

### 2. Per-domain validation

**Partially addressed.** Offline registry + gold tests run in CI (`tests/test_llm_extraction_regression.py`, `test_fleet_registry_parity.py`, etc.). Script `111_llm_extraction_validation.py` remains the lineage/side-by-side builder; domain-wide MotherDuck validation is not a single CI job—release posture comes from `112` + `119`.

### 3. Separate dev / qa / prod MotherDuck catalogs

**Configured.** `config/motherduck_environments.yml` maps `dev`, `qa`, and `prod` to distinct `database` names. `motherduck_client.py` documents the same. `docs/release_runbook.md` instructs `MOTHERDUCK_ENV` / `--md-env` for QA vs prod paths.

### 4. Multimodal / specimen CI coverage

**Present offline; optional MD strict gate.** `.github/workflows/ci.yml` job `multimodal-tests` runs `test_multimodal_contract_mm_v1.py`, `test_imaging_fna_linkage_mm_v1.py`, `test_specimen_fhir_layer.py`, `test_specimen_fhir_qa_diagnostics.py`, `test_specimen_genomics_binding.py`. A separate **manual** workflow dispatch job `multimodal-md-contract-gate` runs `129` → `128` with `--strict-release` against MotherDuck when enabled.

## Documentation vs automation mismatches noted

- `README.md` states publication is blocked when MRQ reflects **synthetic** verification—**governance**, not an automated `119` failure if every row has a **non-NULL** `verification_status` (see `release_gap_list_20260407.md`).
- `README.md` release-mode bullet list says validators enforce an **empty pending** MRQ slice; the implementation defines pending as rows not counted in `verification_status IS NOT NULL` (equivalent to NULL-only pending count for a simple nullable column).

## Stale or conflicting artifacts (non-exhaustive)

- Timestamped folders under `studies/` (e.g. `20260407_formalization_validation_release_mode` vs `20260407_live_truth_and_lineage_contract_audit`)—`README.md` explicitly tells operators which tree is authoritative for **current** `119` evidence.
- `docs/motherduck_database_contract_v1.md` § “MD_INFORMATION_SCHEMA” lists `database_snapshots` and `query_history`; preflight code must match what the attached catalog exposes (this audit aligned `124` to `DATABASE_SNAPSHOTS` + `query_history`).

---

*Prepared as part of the 2026-04-07 repo audit; no production data mutations performed for this document.*
