# CI execution — before / after

## Before

- **Syntax / Lint** failed at **Smoke test — MotherDuck RO share** with `master_cohort` missing on RO share.
- **motherduck-formalization** `needs:` did not include **data-contract-gate-offline** (only `ruff-and-mypy`, `lint-and-syntax`, `validation-contracts-offline`).
- **data-contract-gate-offline** ran pytest for 145 only; no explicit **CLI** invocation of `scripts/145_data_contract_gate.py` in CI.
- Docs stated pytest-only for 145 in CI.

## After

- RO smoke counts `DISTINCT research_id` from **`manuscript_cohort_v1`** on the share (documented table).
- **motherduck-formalization** `needs:` includes **data-contract-gate-offline** (explicit list).
- **data-contract-gate-offline** adds an offline **145 CLI smoke** (minimal parquet → script → artifact assertions).
- `docs/data_contract_gate.md` and `README.md` CI table updated to match.
- Workflow-level `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` added to reduce Node 20 deprecation noise from Actions.
