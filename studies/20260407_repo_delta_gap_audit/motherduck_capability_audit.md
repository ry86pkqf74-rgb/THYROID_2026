## Token resolution (labels only — no values)

| Function | Result (2026-04-08) |
|----------|---------------------|
| `token_mode()` | `secrets.toml:MOTHERDUCK_TOKEN` |
| `read_scaling_token_mode()` | `none` |
| `resolve_database_for_env("dev")` | `Thyroid 2026 Molecular Dev 20260407` |
| `resolve_database_for_env("qa")` | `Thyroid 2026 Molecular QA 20260407` |
| `resolve_database_for_env("prod")` | `Thyroid 2026` |

**Environment:** `MOTHERDUCK_TOKEN`, `MD_SA_TOKEN`, `MD_READ_SCALING_TOKEN` were **unset** in the shell; RW credential came **only** from **`.streamlit/secrets.toml`**.

## Read/write (fail-closed)

- **`scripts/smoke_test_md_connection.py --md`:** **PASS** — attached `Thyroid 2026`, verified MotherDuck.
- **`scripts/130_md_env_bootstrap.py inspect`:** **PASS** — prod catalog **DUCKLAKE**; snapshot history present with unnamed automatic snapshots.

## Read-scaling / Business-style reader

- **`connect_read_scaling()`:** **RuntimeError** — no read-scaling token (expected credential separation).
- **Credential separation:** With **only** RW token in secrets, **RW** paths work; **read-scaling** path **refuses** — **PASS** for separation semantics (no RS token to test positive path).
- **`MD_READ_SCALING_SESSION_HINT` / `MOTHERDUCK_SESSION_HINT`:** **not set** — nothing to honor on RS path.

## `136_md_read_scaling_snapshot_refresh.py` (dry-run)

- **`reader --md-env prod --dry-run`:** Emitted `REFRESH DATABASE "Thyroid 2026"` (SQL preview).
- **`writer --md-env prod --dry-run`:** Emitted `CREATE SNAPSHOT OF "Thyroid 2026"` (SQL preview).

## DuckLake / rollback

- **Prod type:** **DUCKLAKE** (from `130 inspect`).
- **Named snapshots:** Not assumed for rollback; **automatic** snapshot rows exist with `snapshot_name` **NULL**.
- **Pre-promote strategy (dry-run only):** `130 prepromote-backup --label audit_probe_20260407` printed `CREATE DATABASE "Thyroid 2026 Molecular PrePromote audit_probe_20260407" FROM "Thyroid 2026";` — **no `--execute`** (no prod mutation).

## Make vs secrets.toml

- **`make md-v2-gate-md-dryrun`:** **Succeeded** with tokens **only** in `secrets.toml` — the Makefile’s `check_md_rw_token` uses **`get_token()`**, which loads that file.

## Pro plan label

- **Not inferred** from token strings. **RW** MotherDuck access **works**; **read-scaling token absent** — **cannot** confirm Business read-scaling tier from this workspace alone.
