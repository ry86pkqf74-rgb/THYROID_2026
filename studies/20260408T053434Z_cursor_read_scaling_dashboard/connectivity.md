# Read-scaling + RW connectivity probe

UTC generated: `2026-04-08T05:35:20.176308+00:00`

## Token diagnostics (no secret values)

- `token_mode()` (RW): `secrets.toml:MOTHERDUCK_TOKEN`
- `read_scaling_token_mode()`: `none`
- RW configured: `True` (length 467)
- Read-scaling configured: `False` (length 0)

> **Note:** `.streamlit/secrets.toml` on this host only had `MOTHERDUCK_TOKEN` when probed; add `MD_READ_SCALING_TOKEN` for `connect_read_scaling()` / `136 reader` live.

## `connect_read_scaling()` (Business read-scaling token)

**Not run:** no read-scaling token.
- Expected error: `No read-scaling MotherDuck token. Set MD_READ_SCALING_TOKEN (or MOTHERDUCK_READ_SCALING_TOKEN), optionally with MD_READ_SCALING_SESSION_HINT.`

## RW `connect_rw()` (writer / dashboard fallback — not read-scaling identity)

- `current_database()`: `Thyroid 2026`
- `information_schema.tables` (main): `146`
- `master_patient_rollup_verified_v1` row count: `2702`
- `master_fact_long_verified_v1` row count: `20188`

### QUERY_HISTORY (filtered; no query text)
- user_agent = cursor_dashboard_read_scaling_v1: **0**
- session_name ILIKE cursor_dashboard_ro_%: **2**

## Script 136 (this session)

- `writer --md-env prod` (live): **OK** — `CREATE SNAPSHOT OF "Thyroid 2026"`
- `reader --md-env prod` (live): **failed** — no `MD_READ_SCALING_TOKEN` / alias (see traceback in logs)
