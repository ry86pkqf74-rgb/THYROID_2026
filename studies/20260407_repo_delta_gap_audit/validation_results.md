## Local code quality

| Step | Result |
|------|--------|
| **ruff** `--select F` | **PASS** (after `pip install ruff` into `.venv` — module was not preinstalled) |
| **pyflakes** | **PASS** (no output) |
| **mypy** (full project) | **PASS** (after fixing `tests/test_publication_governance.py` nullability) |
| **pytest** (user-listed subset) | **64 passed** |

## Code change

- **`tests/test_publication_governance.py`:** assert `fetchone()` is not `None` before indexing — fixes mypy `[index]` errors.

## MotherDuck RW

| Check | Result |
|-------|--------|
| `smoke_test_md_connection.py --md` | **PASS** |
| `130_md_env_bootstrap.py inspect` | **PASS** |
| `144_md_repo_current_state_summary.py --md` | **PASS** (wrote `CURRENT_MOTHERDUCK_REPO_STATE.md`) |
| `make md-v2-gate-md-dryrun` | **PASS** (116 dry-run, 112 PASS, **119 structural** completed with 3 FAIL on **local** canonical parquet `-1`) |
| `119 --release-mode` | **EXIT 1** — **3 FAIL** (same local canonical parity): **33 PASS / 3 WARN / 3 FAIL**; governance **5b PASS**; specimen/FHIR **PASS**; molecular **PASS** + **WARN** |

## Read-scaling

| Check | Result |
|-------|--------|
| `read_scaling_token_mode()` | `none` |
| `connect_read_scaling()` | **RuntimeError** (expected — no RS token) |
| `136 … reader --dry-run` | **PASS** (SQL echo) |
| `136 … writer --dry-run` | **PASS** (SQL echo) |

**Business-style read-scaling:** **Not demonstrated** (no `MD_READ_SCALING_TOKEN` in env or secrets).

## `make md-live-release-dryrun`

- **Started** via `make md-live-release-dryrun` (`124_md_live_release_audit.py --md --dry-run --tag 20260408`).
- **Observed:** partial artifacts under `studies/20260408_motherduck_live_release_audit/` while the Make process was **still running** (stdout may be buffered). **Exit code not confirmed** before commit — treat **124** as **in progress** unless the shell job has finished.
- For a full transcript, re-run with `python -u scripts/124_md_live_release_audit.py --md --dry-run --tag …`.

## Live lab wave sanity

- Query `main.longitudinal_lab_canonical_v1` ingestion_wave: `wave_tgab_structured_ehr`, `wave_tg_structured_ehr`, **`final_institutional_20260407`** (989 rows).
