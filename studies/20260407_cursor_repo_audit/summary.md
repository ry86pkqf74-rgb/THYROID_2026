# Cursor repo + MotherDuck read-only audit — 2026-04-07

Operational summary for release planning. **No production mutations**, no `136 --execute`, no `137`, no local DuckDB table writes.

## Token source modes (labels only; never printed secret values)

| Mode | Result |
|------|--------|
| `rw_token_mode` (from `motherduck_client.token_mode()`) | `secrets.toml:MOTHERDUCK_TOKEN` |
| `read_scaling_token_mode` (from `motherduck_client.read_scaling_token_mode()`) | `none` |

Read-scaling smoke (section E) was **not executed** on a live reader connection because no read-scaling token was resolved. **136 reader dry-run SQL** was still printed (see below).

## Catalog mapping (from `config/motherduck_environments.yml`)

| Environment | MotherDuck database name |
|-------------|-------------------------|
| `dev` | `Thyroid 2026 Molecular Dev 20260407` |
| `qa` | `Thyroid 2026 Molecular QA 20260407` |
| `prod` | `Thyroid 2026` |

## Documentation prerequisites

Read and applied: `AGENTS.md` (prefix), `README.md` (prefix), `docs/REPO_ARCHITECTURE_V2.md`, `docs/motherduck_database_contract_v1.md`, `docs/release_runbook.md`, `docs/motherduck_read_scaling_dashboard.md`, `config/motherduck_environments.yml`.

**Missing from workspace:** `.cursor/rules/full-capability-mode.mdc` (not present under `THYROID_2026/`; glob found 0 matches).

## MotherDuck attachment (read/write token / prod default)

| Check | Outcome |
|-------|---------|
| `smoke_test_md_connection.py --md` | **PASS** — fail-closed gate passed; attached `md:Thyroid 2026`; DuckDB v1.4.4 |
| `130_md_env_bootstrap.py inspect` | **PASS** — listed catalogs including prod (DUCKLAKE), dev/qa clones, PrePromote backup; recent `DATABASE_SNAPSHOTS` rows visible |

## Read scaling

| Item | Outcome |
|------|---------|
| Token present | **No** (`read_scaling_token_mode=none`) |
| `MotherDuckClient.for_env("prod").connect_read_scaling(session_hint=...)` | **Skipped** (no token) |
| `136_md_read_scaling_snapshot_refresh.py reader --md-env prod --dry-run` | **PASS** — emitted: `REFRESH DATABASE "Thyroid 2026"` |

## Lint / typecheck / tests

| Command | Result |
|---------|--------|
| `ruff check scripts app utils llm_extraction motherduck_client.py dashboard.py --select F` | **PASS** (all checks passed) |
| `mypy` | **PASS** (Success: no issues found in 54 source files; one `annotation-unchecked` note in tests) |
| `pytest -q tests/test_motherduck_connect_hardening.py tests/test_motherduck_token_modes.py tests/test_md_read_scaling_refresh.py tests/test_smoke_test_md_connection.py tests/test_registry_and_md_connect.py` | **PASS** (74 passed) |

## Strict QA validation — `119_md_formalization_validate.py` (`--md-env qa --release-mode`)

| Result | Detail |
|--------|--------|
| **Outcome** | **22 PASS / 0 WARN / 0 FAIL** |
| Output dir | `studies/20260407_cursor_qa_release_mode/` (`validation_report.md`) |
| Notes | Check 12 skipped (no `main.molecular_results`). Check 13 skipped on QA (`synoptic_tumor_long_v1` absent — specimen/FHIR checks skipped). |

## Live release audit — `124_md_live_release_audit.py` (`--md-env prod --dry-run`)

### Run A — requested tag `20260407`

| Step | Outcome |
|------|---------|
| Preflight | **PASS** (9 DBs attached); informational: `md_information_schema.snapshots` query **Catalog Error** (table not found — may be plan/tier) |
| Stage 116 dry-run | **OK** |
| Promotion 112 | **OK** |
| … middle steps … | **OK** (including 117, 125 dry-runs) |
| **115 release snapshot** | **FAILED** — `Schema release_20260407 already exists. Use a different tag.` |
| **Verdict** | **ABORT** (exit code 1) — **operational blocker for repeating the same calendar tag** in dry-run when that release schema already exists |

### Run B — supplementary full-chain dry-run (non-colliding tag)

To observe the remainder of the chain without mutating prod, a second dry-run used `--tag cursor_repo_audit_20260407` and `--output-dir studies/20260407_cursor_live_release_dryrun_rerun`.

| Result | Detail |
|--------|--------|
| **124 final verdict** | **PASS** |
| **119 (prod, structural)** at end of chain | **24 PASS / 1 WARN / 0 FAIL** (`studies/20260407_cursor_live_release_dryrun_rerun/validation_run/validation_report.md`) |
| WARN | Specimen-adjacent review burden: `genomic_link_review` open/pending=**9966**; `specimen_merge_review` open/pending=**1** |
| Step `132_molecular_fact_lineage_views.py --validate-only` | **Exit 0** but logged **Catalog Error**: `main.molecular_fact_long_base_v` (and related views) **do not exist** on prod; DuckDB suggested **`Thyroid 2026 Molecular Dev 20260407`** objects instead — **prod vs dev lineage view drift** for molecular long tables |

**Primary requested output path** for tag `20260407`: `studies/20260407_cursor_live_release_dryrun/` contains logs up to the 115 failure; full success artifacts are under `studies/20260407_cursor_live_release_dryrun_rerun/`.

## Business-tier observability (optional)

| Query | Outcome |
|-------|---------|
| `MD_INFORMATION_SCHEMA.QUERY_HISTORY` (sample 15 rows, via `utils.md_observability`) | **Allowed** (`query_history_ok=True`) |
| Attribution | In the **last 15 rows**, **no** `user_agent` contained `cursor` / `cursor-agent` (UA may not appear in history immediately, history window is short, or field differs) |

## Exact commands executed

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"

.venv/bin/python -c "from motherduck_client import token_mode, read_scaling_token_mode; print('rw_token_mode=', token_mode()); print('read_scaling_token_mode=', read_scaling_token_mode())"

.venv/bin/ruff check scripts app utils llm_extraction motherduck_client.py dashboard.py --select F
.venv/bin/mypy
.venv/bin/pytest -q tests/test_motherduck_connect_hardening.py tests/test_motherduck_token_modes.py tests/test_md_read_scaling_refresh.py tests/test_smoke_test_md_connection.py tests/test_registry_and_md_connect.py

.venv/bin/python scripts/smoke_test_md_connection.py --md --custom-user-agent "cursor-agent/1.0(repo_audit;thyroid_2026)" --session-hint "cursor_repo_audit"
.venv/bin/python scripts/130_md_env_bootstrap.py inspect

.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode --output-dir studies/20260407_cursor_qa_release_mode

.venv/bin/python scripts/124_md_live_release_audit.py --md --dry-run --md-env prod --tag 20260407 --output-dir studies/20260407_cursor_live_release_dryrun

.venv/bin/python scripts/124_md_live_release_audit.py --md --dry-run --md-env prod --tag cursor_repo_audit_20260407 --output-dir studies/20260407_cursor_live_release_dryrun_rerun

.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod --dry-run
```

Plus a short Python one-liner using `MotherDuckClient.for_env('prod').connect_rw()` and `utils.md_observability.run_safe(..., SQL_QUERY_HISTORY_SAMPLE)` (no query text printed in logs).

## Next actions

### a) Safe read-only

- Treat **124 + `--tag YYYYMMDD`** as **invalid for dry-run** when `release_YYYYMMDD` already exists; use a **fresh tag** (or policy-defined suffix) for rehearsal.
- Investigate **prod missing `main.molecular_fact_long_*` views** while **dev** has them (132 validation hints) — confirm whether prod promotion should materialize 132 or分子 lineage is dev-only by design.
- Optionally add `MD_READ_SCALING_TOKEN` to secrets for dashboard/reader verification (never substitute for RW promotion).

### b) Dev / QA-only mutations

- Refresh dev/qa clones if schema drift blocks testing (per runbook — **not done here**).
- Deploy or test molecular lineage scripts on **dev** before prod if views are intended on prod.

### c) Production mutations

- None from this audit. Any prod promotion remains **operator-run** via runbook (`130` backup, `119` qa, `124`, `136`, etc.) **after** manuscript/automation gates satisfied.

## Release readiness (automation lens)

- **QA `119 --release-mode`:** **PASS** (22 checks, no FAIL).
- **Prod `124` dry-run with calendar tag `20260407`:** **BLOCKED** at **115** (release schema already exists) — **procedural**, not a data regression signal.
- **Prod `124` dry-run with unused tag:** **PASS**; embedded prod **119** structural: **PASS with WARN** (specimen/genomic review burden + molecular view presence gap noted above).

For **signed manuscript release**, README and runbook still separate **human MRQ / non-synthetic adjudication** from automation PASS; this audit does **not** certify manuscript sign-off.
