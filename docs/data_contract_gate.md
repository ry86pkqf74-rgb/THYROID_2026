# Data contract gate (script 145)

## Purpose

A **thin, YAML-driven** validation layer that version-checks representative datasets (schema, enums, numeric bounds, dates, composite uniqueness, provenance, and a small set of linkage-prep builtins). It emits **append-only audit events** with a **deterministic SHA-256 hash chain** per run.

This **complements** existing gates; it does **not** replace them.

| Existing component | Role | Relationship to 145 |
|--------------------|------|----------------------|
| `scripts/112_v2_domain_promotion_gate.py` | 8-criteria promotion to `main` | 145 does not score promotions; it can run on **promoted** parquet/table extracts as an extra contract pass. |
| `scripts/29_validation_engine.py` | Materializes `val_*` SQL validation tables on DuckDB | 145 is **Python + YAML**, file/table oriented, and writes **artifacts** (parquet/JSONL/markdown) instead of SQL views. |
| `scripts/119_md_formalization_validate.py` | Release structure + presentation/traceability checks | 145 **reuses** the same provenance column expectations (see `canonical_extracted_fact_long_v2` contract) as **declarative** rules, not as a duplicate of check 10–13. |
| `qa.manual_review_queue` / `qa.domain_validation` | MotherDuck QA plane | Default **`--write-qa` is off**; when on, 145 writes **`qa_surface/` files only** under `--output-dir` for manual ingest — it does **not** insert into `qa.*` unless you add a separate, explicit workflow. |

## Contract schema (YAML)

Each file supports, as applicable:

- `columns` — `name`, `dtype` (`int` / `float` / `string` / `date`), `nullable`, `allowed_values`, `no_future_date`, `provenance_required`
- `composite_unique` — uniqueness groups; optional `only_when_value_numeric_not_null` (lab dedup pattern)
- `conditional_numeric_bounds` — `when_column` / `when_value` / `column` / `min` / `max` (lab plausibility)
- `foreign_keys` — `name`, `local_columns`, `ref_columns`, and **`ref_csv` or `ref_parquet`** (repo-relative path to a lookup table)
- `builtin_rules` — linkage-prep checks keyed by `id` (see imaging contract)
- `cross_field_rules` — reserved for future structured rules (empty in seed contracts)

## Contracts

Versioned YAML lives under `config/data_contracts/`:

- `longitudinal_lab_canonical_v1.yaml` — lab tiers, date status, conditional plausibility bounds (from `tests/lab_canonical_contract.py`).
- `imaging_fna_linkage_prep_v1.yaml` — denormalized imaging+FNA **staging** rows; builtin rules for discordant laterality and FNA-after-surgery (ideas from `tests/test_imaging_fna_linkage_mm_v1.py`).
- `canonical_extracted_fact_long_v2.yaml` — required identity + provenance columns aligned with `docs/motherduck_database_contract_v1.md` §3 and `REQUIRED_ENTITY_COLUMNS` / `DESIRED_PROVENANCE_COLUMNS` in script 119.

## CLI

```bash
.venv/bin/python scripts/145_data_contract_gate.py \
  --contract-name longitudinal_lab_canonical_v1 \
  --input-path path/to/file.parquet \
  --output-dir studies/20260408_data_contract_gate/run_001
```

- **Default:** offline — parquet/CSV input or local `--db-path` with `read_only`; **no** MotherDuck writes.
- **`--md` / `--md-sa`:** read `--table` via MotherDuck (token from env or gitignored `motherduck.local.toml` per `motherduck.local.toml.example`). Still **no DB writes** unless you explicitly add a future ingest step for `qa_surface/` outputs.
- **`--strict`:** exit code 1 if any **error**-severity violation exists.
- **`--write-qa`:** emit `qa_surface/` summary + parquet under `--output-dir` only.

## Outputs

- `violations.parquet`
- `audit_events.jsonl` (+ `audit_events.parquet` when non-empty)
- `run_metrics.json`
- `summary.md`

### Audit ledger

Each event includes: `run_id`, `ts_utc`, `dataset_name`, `row_locator`, `column_name`, `rule_id`, `severity`, `action`, `observed_value`, `expected_constraint`, `suggested_fix`, `source_file_id`, `extraction_run_id`, `row_fingerprint_sha256`, `prev_event_hash`, `event_hash`. Events are sorted deterministically; `event_hash` chains from `prev_event_hash` (genesis: 64 zeros).

No raw note text is written to artifacts.

## CI

Workflow job **data-contract-gate-offline** (`.github/workflows/ci.yml`) runs:

1. `pytest tests/test_data_contract_gate.py` — **no secrets**, no MotherDuck.
2. A **CLI smoke** that writes a minimal parquet, runs `scripts/145_data_contract_gate.py --contract-name longitudinal_lab_canonical_v1 --input-path … --output-dir …`, and asserts expected artifacts (`violations.parquet`, `audit_events.jsonl`, `run_metrics.json`, `summary.md`). Same offline contract as `test_main_cli_offline_writes_artifacts`.

Downstream job **motherduck-formalization** lists **data-contract-gate-offline** in `needs:` so formalization does not run until both pytest and the 145 CLI path succeed.

## Integration note

Run 145 **after** extracting a table to Parquet (or against a read-only DuckDB snapshot) when you want a **portable audit bundle** with a **hash-chained** ledger. Continue to use **112 / 29 / 119** for promotion, SQL validation, and formalization sign-off.
