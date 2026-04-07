# Linkage / molecular release rehearsal — 2026-04-07

Evidence root: `studies/20260407_linkage_release_rehearsal/`.

MotherDuck RW auth: `.streamlit/secrets.toml` (no secrets logged). Read-scaling token keys were **not** present in that file.

## Pass / fail by step

| Step | Result | Notes |
|------|--------|--------|
| 1a Ruff `--select F` | **PASS** | `lint_and_types.txt` |
| 1b Mypy (pyproject `files`) | **PASS** | 54 source files |
| 1c Pytest (MD/linkage/specimen focus) | **PASS** | 107 tests; see `pytest_results.txt` |
| 2 `make md-smoke` | **PASS** (after Makefile fix) | Previously failed: Make only checked env vars, not `get_token()` / secrets.toml |
| 3 `make md-v2-gate-md-dryrun` | **PASS** (Make rc 0) | Gate 112 **PASS**; tail `119` structural: **1 FAIL** label on `note_extraction_runs` local vs MD (see fixes below), **exit 0** in structural mode by design |
| 4 `make md-live-release-dryrun` | **PARTIAL** | Default tag `20260407`: **115** fail — `release_20260407` already exists (fail-closed). **Replay** with `MD_RELEASE_TAG=20260411`: **124 DONE PASS** (see second half of `md_live_release_dryrun.txt`) |
| 5 `119 --md-env qa --release-mode` | **FAIL** | `qa_validate_release_mode.txt` — `note_extraction_runs` parity **PASS** after local parquet refresh; remaining **5 FAIL**: QA catalog `main.*` missing `molecular_results` / specimen FHIR objects (MotherDuck suggests same tables on prod attach `Thyroid 2026`). **Interpretation:** QA clone schema drift vs prod; refresh QA from prod per sandbox runbook, or run strict 119 against prod for contract-only sign-off |
| 6 `137 promote --tag 20260407` (no `--execute`) | **PARTIAL** | Manifest: `molecular_promote_workflow/workflow_manifest.json` — `qa-validate` **1**, `prod-audit` **1** (115 duplicate tag); `backup-prod`, `try-named-snapshot`, `writer-snapshot`, `refresh-readers` **0** |
| 7 Read-scaling | **PARTIAL** | **Writer** `--dry-run`: **PASS** (`read_scaling_path.txt`). **Reader**: skipped — no `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` in secrets.toml. Dashboard freshness path documented in `docs/motherduck_read_scaling_dashboard.md` |

## Exact commands (evidence)

```bash
cd THYROID_2026
mkdir -p studies/20260407_linkage_release_rehearsal

# 1 — quality
.venv/bin/ruff check scripts app utils llm_extraction motherduck_client.py dashboard.py --select F
.venv/bin/mypy
.venv/bin/pytest tests/test_smoke_test_md_connection.py tests/test_motherduck_token_modes.py \
  tests/test_motherduck_connect_hardening.py tests/test_md_stage_loader_transaction.py \
  tests/test_registry_and_md_connect.py tests/test_md_read_scaling_refresh.py \
  tests/test_md_contract_views_transaction.py tests/test_imaging_fna_linkage_mm_v1.py \
  tests/test_fact_provenance_contract.py tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py -q --tb=short

# 2–4 — Make (RW token via env or secrets.toml after Makefile fix)
make md-smoke
make md-v2-gate-md-dryrun
MD_RELEASE_TAG=20260407 make md-live-release-dryrun   # expect 115 fail if schema exists
MD_RELEASE_TAG=20260411 make md-live-release-dryrun    # full dry-run chain

# 5 — QA strict
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode \
  --output-dir studies/20260407_linkage_release_rehearsal/119_qa_release_mode

# 6 — rehearsal orchestrator
.venv/bin/python scripts/137_md_molecular_release_workflow.py promote --tag 20260407 \
  --output-dir studies/20260407_linkage_release_rehearsal/molecular_promote_workflow

# 7 — read scaling
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --dry-run
# Reader (needs MD_READ_SCALING_TOKEN): skipped here
```

## Fail-closed behavior

- **Smoke / connect:** Missing RW token → fail (Make now uses `get_token()` so secrets.toml satisfies the guard).
- **115 release snapshot:** Duplicate `release_YYYYMMDD` → **exit 1**, audit aborts (observed for `20260407`).
- **119 `--release-mode`:** Any `FAIL` check → **exit 1** (`RELEASE BLOCKED`).

## Repo readiness for dev/QA-only promotion test

- **Not fully green** until: (1) QA MotherDuck DB is refreshed so `main.molecular_results` and specimen/FHIR tables match prod expectations, or strict validation is run against **prod** with ops approval; (2) new **`MD_RELEASE_TAG`** chosen when running **115** / **124** so the `release_*` schema does not already exist; (3) optional: configure **read-scaling** token to validate **136** reader + dashboard snapshot path.

## Code / artifact fixes applied this session

- **Makefile** — `check_md_rw_token` uses `motherduck_client.get_token()` so MotherDuck targets work with `.streamlit/secrets.toml` without exporting env vars.
- **`scripts/137_md_molecular_release_workflow.py`** — `promote --tag` / `promote --output-dir`; `getattr` guards for Namespace fields when `promote` delegates to other commands; `force_native_snapshot` guard.
- **Local `processed/note_extraction_runs.parquet`** — refreshed from prod MotherDuck (gitignored) so parity check matches MD (3 rows).

## Blockers (concrete)

1. **QA catalog drift** — release-mode **119** fails checks 12–13 on QA (`molecular_results`, `specimen_master_v1`, `qa.val_*`, diagnostics views) while prod attachment names are suggested in errors.
2. **Release tag collision** — `release_20260407` already on prod → dry-run/final **115** must use a **new** tag for a real promotion rehearsal.
3. **Read-scaling token** — not in local secrets; reader **REFRESH DATABASE** path not executed.
