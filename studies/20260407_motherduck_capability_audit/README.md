# MotherDuck capability audit (2026-04-07)

Pure read-only / dry-run verification of repo MotherDuck integration against **locally configured credentials** (no secrets pasted in chat). Token values are never stored in this folder.

## Evidence bundle

| File | Purpose |
|------|---------|
| `token_source_modes.txt` | `token_mode()`, `read_scaling_token_mode()`, `resolve_database_for_env` for dev/qa/prod |
| `md_smoke_output.txt` | `scripts/smoke_test_md_connection.py --md` |
| `md_inspect_output.txt` | `scripts/130_md_env_bootstrap.py inspect` (catalog type, snapshots) |
| `CURRENT_MOTHERDUCK_REPO_STATE.md` | `scripts/144_md_repo_current_state_summary.py --md` output |
| `current_state_output.md` | Copy of the same summary (checklist artifact) |
| `prepromote_capability_probe.txt` | `130 prepromote-backup` dry-run SQL (no `--execute`) |
| `read_scaling_validation.txt` | Read-scaling token probe, 136 reader/writer `--dry-run`, session-hint presence |
| `fail_closed_separation_test.txt` | Subprocess RS-only surface → smoke `--md` must exit 1 |
| `make_targets_audit.txt` | `make md-smoke`, `md-v2-gate-md-dryrun`, `md-live-release-dryrun` (see note below) |
| `pytest_md_audit.txt` | Selected pytest modules |
| `ducklake_snapshot_clone_notes.md` | Consolidated DuckLake / clone / snapshot semantics |
| `conclusions.md` | Short verdict table |
| `commands_run.md` | Command list and exit codes |

## Credential source (this run)

- **Read/write:** `secrets.toml:MOTHERDUCK_TOKEN` (env vars for RW were unset at detection time; resolution order is env-first, then `.streamlit/secrets.toml`).
- **Read-scaling:** none configured (`read_scaling_token_mode(): none`).

## Note on `make md-live-release-dryrun`

The first `make` invocation buffered `124` subprocess output for a long interval (no `python -u`), which looked like a hang. The audit appended a **re-run** with `PYTHONUNBUFFERED=1` and `python -u`; that run completed with `124_direct_exit=0`. See `make_targets_audit.txt`.

**Follow-up:** `Makefile` targets `md-live-release-dryrun` and `md-live-release-final` now use `$(PYTHON) -u` when invoking script **124** so operators see streaming stdout during long runs.

## Related docs

- `docs/motherduck_database_contract_v1.md`
- `docs/motherduck_read_scaling_dashboard.md`
- `docs/motherduck_sandbox_clone_runbook.md`
- `docs/release_runbook.md`
