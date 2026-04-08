# Before / after: validation execution visibility (2026-04-08)

## Before

- **Blocking CI** already ran: ruff F, mypy, doc path check; offline LLM/specimen/multimodal suites; `lint-and-syntax` (MotherDuck smoke + manuscript metrics); **116 → 112 → 119** (`--md` without `--release-mode`).
- **Gaps (invisible on default PR CI)**:
  - `scripts/29_validation_engine.py` — only invoked in **manual** `.github/workflows/motherduck_episode_pipeline.yml`; no offline execution of its **lab validation SQL** or **ALL_VALIDATION_SQL** registry.
  - `tests/test_linkage_confidence.py` — present in repo but **not** listed in any CI pytest step.
  - `tests/test_lab_canonical.py` — requires local DuckDB; **not** suitable for hosted runners; no separate **fixture-backed** twin.
  - `scripts/119_md_formalization_validate.py --release-mode` — no dedicated **workflow_dispatch** entry in `ci.yml` (only non–release 119 in `motherduck-formalization`).

## After

- **New blocking job** `validation-contracts-offline`:
  - `tests/test_validation_engine_lab_sql_offline.py` — executes `VAL_LAB_CANONICAL_SQL` from script **29** on an in-memory stub.
  - `tests/test_validation_engine_import_contracts_offline.py` — asserts **provenance / lab / linkage** validation names stay in `ALL_VALIDATION_SQL`.
  - `tests/test_lab_canonical_contract_offline.py` — tier / schema / plausibility contract without a real DB file.
  - `tests/test_linkage_confidence.py` — linkage tier logic (already fully offline).
- **`motherduck-formalization`** now **depends on** `validation-contracts-offline` so formalization does not run if offline contracts fail.
- **New manual jobs** (secrets required):
  - `md-formalization-strict-release` — `python scripts/119_md_formalization_validate.py --md --md-sa --release-mode` + artifact upload (**read-only** against MotherDuck data; writes markdown under `studies/…`).
  - `validation-engine-motherduck` — `python scripts/29_validation_engine.py --md` (**writes** `val_*` — operator-triggered only).
- **Test split**: `tests/test_lab_canonical.py` marked **`local_db`**; shared constants live in `tests/lab_canonical_contract.py`.
- **Documentation**: `coverage_matrix.md` (this folder) is the single map from concern → execution tier.

## What did not change

- No new validation **framework**; re-used script **29** SQL and constants only.
- Existing validator architecture and SQL in `29_validation_engine.py` unchanged.
- `112` / `119` (non–release) path unchanged aside from ordering dependency on offline job.
