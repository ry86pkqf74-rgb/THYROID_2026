# Commands run — 2026-04-08 MotherDuck capability audit

Working directory: `THYROID_2026` repo root. Interpreter: `.venv/bin/python` unless noted.

## 1. Token-source detection

```bash
.venv/bin/python -c "… token_mode(), read_scaling_token_mode(), resolve_database_for_env('dev'|'qa'|'prod'), credential_mix …"
```

Exit code: **0**

## 2. Fail-closed smoke (RW path)

```bash
.venv/bin/python scripts/smoke_test_md_connection.py --md
```

Exit code: **0**  
Captured: `md_smoke_output.txt`

## 3. Catalog inspection

```bash
.venv/bin/python scripts/130_md_env_bootstrap.py inspect
```

Exit code: **0**  
Captured: `md_bootstrap_inspect.txt`  
Note: `MD_SA_TOKEN` was not set in the agent shell; `--md-sa` duplicate inspect not run.

## 4. Current-state reconciliation

```bash
.venv/bin/python scripts/144_md_repo_current_state_summary.py --md \
  --output studies/20260408_motherduck_capability_audit/md_current_state_refresh.txt
```

Exit code: **0**  
Sidecar stdout: `144_stdout.txt` (if present)

## 5. Repo-supported dry-run path (RW token present)

```bash
make md-smoke
```

Exit code: **0**

```bash
make md-v2-gate-md-dryrun
```

Exit code: **0** (116 dry-run + 112 + 119 structural)

```bash
make md-live-release-dryrun
```

Exit code: **0** (124 dry-run; wall time ~27.5 min this run)  
Captured: `dryrun_release_path.txt` (copy of `dryrun_release_path_partial.txt`)

## 6. Read-scaling validation

```bash
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod --dry-run
# exit 0 — prints REFRESH DATABASE SQL only

.venv/bin/python -c "MotherDuckClient.for_env('prod').connect_read_scaling(); SELECT current_database(), current_timestamp; …"
# exit 0 after successful run (one early attempt raised TransactionException; retry succeeded)

.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
# exit 0 — OK reader: REFRESH DATABASE \"Thyroid 2026\"

.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --dry-run
# exit 0 — prints CREATE SNAPSHOT OF \"Thyroid 2026\"
```

Captured: appended to `read_scaling_validation.txt`

## 7. Credential-separation attempt

```bash
set -a; source .env.motherduck; set +a
unset MOTHERDUCK_TOKEN MD_SA_TOKEN motherduck_token
# then smoke_test --md and connect_rw probe
```

Captured: `fail_closed_separation_test.txt`  
Outcome: **not** a true RS-only environment — `python-dotenv` in `motherduck_client` reloads RW from repo `.env` / `.env.motherduck` at import (`override=False` still populates missing keys). Smoke and `connect_rw` still **PASS**.

## Lint / verification

No Python source changes in this audit folder; repository lint had no new modules to compile for this deliverable.
