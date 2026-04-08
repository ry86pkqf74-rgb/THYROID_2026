# Read-scaling connectivity probe

UTC generated: `2026-04-08T05:28:48.178883+00:00`

## Token diagnostics (no secret values)

- `token_mode()` (RW resolution): `secrets.toml:MOTHERDUCK_TOKEN`
- `read_scaling_token_mode()`: `none`
- RW token configured: `True` (length 467)
- Read-scaling token configured: `False` (length 0)

## Read-scaling connection (`connect_read_scaling`)

**Skipped:** no `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` in environment or `.streamlit/secrets.toml`.

Expected `RuntimeError` if called; verification:
- Raised: `RuntimeError`: No read-scaling MotherDuck token. Set MD_READ_SCALING_TOKEN (or MOTHERDUCK_READ_SCALING_TOKEN), optionally with MD_READ_SCALING_SESSION_HINT.

## RW sanity check (allowed for telemetry only; not read-scaling path)

- RW `current_database()`: `Thyroid 2026`

### QUERY_HISTORY filter (may be empty or permission-denied)
- `user_agent exact`: **0**
- `session_name LIKE cursor_dashboard_ro_%`: **0**
