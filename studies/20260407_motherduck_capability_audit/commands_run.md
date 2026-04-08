# Commands run — MotherDuck capability audit

Repo root: `THYROID_2026`. Date folder: `studies/20260407_motherduck_capability_audit/`.

All invocations used `.venv/bin/python` from the repo root. RW capability used **only** locally saved `.streamlit/secrets.toml` (token source label: `secrets.toml:MOTHERDUCK_TOKEN`); no tokens were printed.

## 1. Token source detection

```bash
.venv/bin/python -c "from motherduck_client import token_mode, read_scaling_token_mode, resolve_database_for_env; ..."
```

→ `token_source_modes.txt`

## 2. RW fail-closed validation

| Command | Exit |
|---------|------|
| `.venv/bin/python scripts/smoke_test_md_connection.py --md` | 0 |
| `.venv/bin/python scripts/130_md_env_bootstrap.py inspect` | 0 |
| `.venv/bin/python scripts/144_md_repo_current_state_summary.py --md --output studies/20260407_motherduck_capability_audit/CURRENT_MOTHERDUCK_REPO_STATE.md` | 0 |

Outputs: `md_smoke_output.txt`, `md_inspect_output.txt`, `CURRENT_MOTHERDUCK_REPO_STATE.md`, `current_state_output.md` (copy).

## 3. DuckLake / prepromote (dry-run only, no `--execute`)

```bash
.venv/bin/python scripts/130_md_env_bootstrap.py prepromote-backup --label capability_probe_20260407
```

→ `prepromote_capability_probe.txt`, exit **0** (SQL printed only).

## 4. Read-scaling helpers

```bash
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod --dry-run
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --dry-run
```

Plus inline probe for `connect_read_scaling()` and env keys `MD_READ_SCALING_SESSION_HINT` / `MOTHERDUCK_READ_SCALING_SESSION_HINT` (set/unset only, no values).

→ `read_scaling_validation.txt`

## 5. Credential-separation proof

Subprocess from `/tmp` with RW env vars unset and `MD_READ_SCALING_TOKEN` set to a **non-secret placeholder**, so `.streamlit/secrets.toml` is not on the token path:

```bash
cd /tmp && env -u MOTHERDUCK_TOKEN -u MD_SA_TOKEN -u motherduck_token -u LOCAL_DB_PATH \
  MD_READ_SCALING_TOKEN="placeholder_rs_only_probe" \
  PYTHONPATH=<REPO> <REPO>/.venv/bin/python <REPO>/scripts/smoke_test_md_connection.py --md
```

→ exit **1**, message requiring RW token — **PASS** (documented in `fail_closed_separation_test.txt`).

## 6. Make targets

```bash
make md-smoke
make md-v2-gate-md-dryrun
make md-live-release-dryrun
```

- `md-smoke`: exit **0**
- `md-v2-gate-md-dryrun`: exit **0** (embedded `119` reported 28 PASS / 3 WARN / 3 FAIL in its summary text; Make still exited 0)
- `md-live-release-dryrun`: subprocess **SIGTERM** during long buffered wait (audit intervention); see re-run below.

Re-run (unbuffered, appended to same log):

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -u scripts/124_md_live_release_audit.py --md --dry-run --tag 20260408
```

→ `124_direct_exit=0` appended to `make_targets_audit.txt`.

**Side effect:** `124` also wrote artifacts under `studies/20260408_motherduck_live_release_audit/` (not part of this audit folder; do not assume it is committed).

**Repo tweak (post-audit):** `Makefile` now runs script 124 with `python -u` under `md-live-release-dryrun` / `md-live-release-final` for unbuffered logs.

## 7. Tests

```bash
.venv/bin/pytest -q tests/test_md_read_scaling_refresh.py \
  tests/test_motherduck_connect_hardening.py \
  tests/test_motherduck_token_modes.py \
  tests/test_smoke_test_md_connection.py
```

→ **33 passed** (see `pytest_md_audit.txt`).
