# MotherDuck Business-tier validation (hard evidence)

**Run date:** 2026-04-08 (operator machine, America/New_York timestamps in some DuckDB fields).  
**Token reporting:** source labels only; no secret values.

## Executive summary

Live **fail-closed** MotherDuck attach to catalog **Thyroid 2026** succeeded. **MD_INFORMATION_SCHEMA** probes (**DATABASES**, **DATABASE_SNAPSHOTS**, **QUERY_HISTORY**, **RECENT_QUERIES**) returned rows. Production catalog type is **DUCKLAKE**; dev/qa sandboxes are **DEFAULT**. **Read/write** token resolved from **motherduck.local.toml:MOTHERDUCK_TOKEN** (no **MD_SA_TOKEN**, no read-scaling token in this run). **Session hint** appears on **RECENT_QUERIES**; **QUERY_HISTORY** did not yet show the same short-lived `biztier_sess_*` rows (lag or scope — **partial**). **Custom user_agent** in connection URI did not appear in `user_agent` column (DuckDB default string observed — treat custom UA in history as **not proven** for this org/driver). Repo fixes: **Makefile** Python resolution without `.venv`; pytest isolation from real **motherduck.local.toml**.

## Token proof

| Check | Result |
|--------|--------|
| RW token available | **Yes** (length 467, label `motherduck.local.toml:MOTHERDUCK_TOKEN`) |
| Service-account token | **Not proven** (no `MD_SA_TOKEN` in env or active toml keys for this run) |
| Read-scaling token | **No** (`read_scaling_token_mode` = `none`) |
| Precedence | Env > … > **motherduck.local.toml** > `.streamlit/secrets.toml` (secrets absent); SA wins over personal when both set (**unit-tested**) |
| Read-scaling rejected on RW path | **Proven** (live: `get_token()` non-null; read-scaling-only tests pass with toml isolated) |

**Token source/mode (this run):** `motherduck.local.toml:MOTHERDUCK_TOKEN` only.  
**Files present:** `motherduck.local.toml` yes; `.env.motherduck`, `.streamlit/secrets.toml`, `.env` absent.

## Capability matrix

| Capability | Status | Notes |
|------------|--------|-------|
| Fail-closed `--md` smoke | **Proven** | `make md-smoke` / `scripts/smoke_test_md_connection.py --md` |
| `PRAGMA database_list` + md gate | **Proven** | 12 entries; fail-closed passed |
| `current_catalog` / `current_database` | **Proven** | `'Thyroid 2026'` / `'Thyroid 2026'` |
| Cloud vs local file | **Proven** | Not local-only path; **md** verification |
| Service-account-preferred path | **Partial** | Code + Make prefer SA when set; **not exercised** (no SA token) |
| Read-scaling connect | **Not proven** | No `MD_READ_SCALING_TOKEN` configured |
| Custom user agent in query history | **Not proven** | Column shows `duckdb/v1.4.4(osx_arm64) python/3.14` |
| Session hint in telemetry | **Proven** | `session_name` populated in **RECENT_QUERIES** |
| `MD_INFORMATION_SCHEMA.DATABASES` | **Proven** | e.g. prod **DUCKLAKE**, dev/qa **DEFAULT** |
| `DATABASE_SNAPSHOTS` | **Proven** | Rows for **Thyroid 2026** (`snapshot_name` NULL — automatic) |
| `QUERY_HISTORY` / `RECENT_QUERIES` | **Partial** | Readable; short session not seen in **QUERY_HISTORY** immediately |
| dev/qa/prod env mapping | **Proven** | `config/motherduck_environments.yml` + live DB rows |
| Sandbox bootstrap dry-run | **Proven** | `130 inspect` prints DDL context without `--execute` |
| DuckLake snapshot constraints | **Proven** | Aligns with `motherduck_sandbox_clone_runbook.md` |

## Commands and evidence excerpts

```bash
# Token labels (no secrets)
python3 -c "from motherduck_client import token_mode, read_scaling_token_mode; print(token_mode()); print(read_scaling_token_mode())"
# → motherduck.local.toml:MOTHERDUCK_TOKEN
# → none

# Smoke + catalog probe
python3 scripts/smoke_test_md_connection.py --md --catalog-probe
# → Connected to MotherDuck (md:Thyroid 2026), verified, probes ok row_count=…

# Env bootstrap inspect (read-only)
python3 scripts/130_md_env_bootstrap.py inspect
# → Thyroid 2026 DUCKLAKE; Dev/QA DEFAULT; DATABASE_SNAPSHOTS sample rows

# Focused tests (after test isolation fix)
pytest tests/test_motherduck_token_modes.py tests/test_smoke_test_md_connection.py tests/test_registry_and_md_connect.py -q
# → 65 passed
```

**Sample (-databases):** `('Thyroid 2026', 'DUCKLAKE', …)`, `('Thyroid 2026 Molecular Dev 20260407', 'DEFAULT', …)`, `('Thyroid 2026 Molecular QA 20260407', 'DEFAULT', …)`.

## Mismatches / caveats

1. **Smokes/tests without `.venv`:** **Makefile** now falls back to `python3` when `.venv/bin/python` is missing.
2. **Developer `motherduck.local.toml` leaked into pytest:** fixed by patching `LOCAL_MOTHERDUCK_TOML_PATH` in tests that require “no RW token”.
3. **`current_setting('custom_user_agent')`** empty in-probe; history `user_agent` is default DuckDB — do not assume MotherDuck custom UA is stored in that column without org-specific confirmation.

## Code changes (this commit)

- `Makefile`: portable `PYTHON` resolution.
- `tests/test_smoke_test_md_connection.py`, `tests/test_registry_and_md_connect.py`: isolate from repo `motherduck.local.toml`.
