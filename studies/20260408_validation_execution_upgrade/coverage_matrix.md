# Validation execution coverage matrix (2026-04-08)

This table maps **validation concern → how it is executed in automation**, after the CI upgrade. Blocking jobs run on `push` (path-filtered) and `pull_request` to `main` unless noted.

| Concern | Blocking CI | Offline / fixture | MotherDuck read (CI secret job) | MotherDuck write (manual / ops workflow) |
|--------|-------------|-------------------|----------------------------------|------------------------------------------|
| **Provenance / fact / quarantine contracts** | `llm-extraction-gold` → `tests/test_fact_provenance_contract.py` | Yes | — | — |
| **Registry + fleet parity** | `llm-extraction-gold` → `tests/test_registry_and_md_connect.py`, `tests/test_fleet_registry_parity.py` (token-cleared env) | Yes | — | — |
| **Multimodal + imaging↔FNA contract** | `multimodal-tests` → `test_multimodal_contract_mm_v1`, `test_imaging_fna_linkage_mm_v1`, `test_specimen_fhir_release_gate` | Yes (DuckDB in tests) | Optional: `workflow_dispatch` `multimodal-md-contract-gate` (129→128 strict; **writes** `mm_contract_dev` unless promotion) | `motherduck_episode_pipeline.yml` |
| **Linkage confidence tiers (imaging–FNA, pathology–RAI logic)** | `validation-contracts-offline` → `tests/test_linkage_confidence.py` | Yes | — | — |
| **Script 29: lab canonical SQL (`VAL_LAB_CANONICAL_SQL`)** | `validation-contracts-offline` → `tests/test_validation_engine_lab_sql_offline.py` | Yes (in-memory) | Full table build: manual `validation-engine-motherduck` (`29 --md`, **writes** `val_*`) | `motherduck_episode_pipeline.yml` (22–25 then 29) |
| **Script 29: registry surface (`val_provenance_traceability`, `val_unlinked_linkable`, …)** | `validation-contracts-offline` → `tests/test_validation_engine_import_contracts_offline.py` | Asserts `ALL_VALIDATION_SQL` includes lab + provenance + linkage-related names | — | Full SQL deploy: manual 29 `--md` or episode pipeline |
| **Lab schema / tiers / plausibility (canonical table contract)** | `validation-contracts-offline` → `tests/test_lab_canonical_contract_offline.py` | Yes | — | — |
| **Lab smoke on real `longitudinal_lab_canonical_v1`** | Not in CI (requires `thyroid_master.duckdb`) | — | `lint-and-syntax` checks table exists + row-level queries on prod share | — |
| **V2 promotion / parquet↔MD parity (112)** | `motherduck-formalization` | Uses on-disk `processed/output/v2_parquets` | `112_v2_domain_promotion_gate.py --motherduck-check` | — |
| **Formalization structural validate (119)** | `motherduck-formalization` | — | `119_md_formalization_validate.py --md` (non–release-mode) | — |
| **Formalization strict release (119 `--release-mode`)** | Not blocking on every PR | — | Manual workflow: `ci.yml` → `run_md_release_validation=true` → `md-formalization-strict-release` | Read-only queries; **writes report files** under `studies/20260408_validation_execution_upgrade/release_mode_<run_id>` + artifact |
| **Stage loader dry-run (116)** | `motherduck-formalization` | — | `--md --dry-run` | — |
| **Provenance traceability tables (`val_provenance_traceability`)** | Indirect: import test ensures SQL is registered | — | Populated only when full 29 runs on MD | Script **46** + 29 on operator path |

## Excluded from hosted CI by design

- `tests/test_lab_canonical.py` — **`local_db`** marker; requires local `thyroid_master.duckdb` with `longitudinal_lab_canonical_v1`. Run locally:  
  `pytest tests/test_lab_canonical.py -m local_db`
- Full `scripts/29_validation_engine.py --md` on every push — **would CREATE OR REPLACE** many `val_*` tables; gated to **manual** `validation-engine-motherduck` or `motherduck_episode_pipeline.yml`.

## Token configuration

- **GitHub Actions**: `MD_SA_TOKEN` and/or `MOTHERDUCK_TOKEN` (and optional `LOCAL_DB_PATH` JWT for legacy clients).  
- **Local dev**: `motherduck.local.toml` from `motherduck.local.toml.example` (gitignored); never commit secrets.
