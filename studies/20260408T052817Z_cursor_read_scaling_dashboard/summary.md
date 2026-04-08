# Read-scaling & dashboard read path validation

**Study folder:** `studies/20260408T052817Z_cursor_read_scaling_dashboard/`  
**UTC:** 2026-04-08 (run aligned with `connectivity.md`)

## Token modes detected

| Credential | Source |
|------------|--------|
| RW (`token_mode`) | `secrets.toml:MOTHERDUCK_TOKEN` (length 467) |
| Read-scaling (`read_scaling_token_mode`) | **none** — `MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` not set in env or `secrets.toml` |

Dashboard env flags during validation shell: `MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN=1`, `MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH=1`, `MOTHERDUCK_CUSTOM_USER_AGENT=cursor_dashboard_read_scaling_v1`, `MD_READ_SCALING_SESSION_HINT=cursor_dashboard_ro_20260408T052817Z`.

## Live read-scaling (`connect_read_scaling`)

**Not executed successfully** — no read-scaling token. Calling `MotherDuckClient.for_env(...).connect_read_scaling()` raises `RuntimeError` with the expected “No read-scaling MotherDuck token” message (see `connectivity.md`).

Per project rules, add `MD_READ_SCALING_TOKEN` (or alias) to **gitignored** `.streamlit/secrets.toml` or the environment, then re-run the connectivity script.

## Writer snapshot / reader refresh

| Step | Result |
|------|--------|
| `136 … reader --md-env prod --dry-run` | Printed `REFRESH DATABASE "Thyroid 2026"` |
| `136 … writer --md-env prod --dry-run` | Printed `CREATE SNAPSHOT OF "Thyroid 2026"` |
| Live `writer` / `reader` | **Skipped** — live reader requires read-scaling token; optional live pair only when **both** RW and read-scaling tokens exist |

## RW connectivity (telemetry only)

- `connect_rw()` to prod succeeded; `current_database()` = `Thyroid 2026`.
- `MD_INFORMATION_SCHEMA.QUERY_HISTORY` filtered counts:
  - `user_agent = 'cursor_dashboard_read_scaling_v1'`: **0**
  - `session_name ILIKE 'cursor_dashboard_ro_%'`: **0**

Prior audits note MotherDuck may store library-style `user_agent` strings rather than the custom integration label; zero rows here may reflect attribution storage, not connection failure.

## Dashboard code path vs docs

**Aligned:**

- Order: RO share attempts (token order per `MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN`), optional `connect_read_scaling()` when `MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH` and RS token exist, then RW fallback / local.
- Session hints: read-scaling branch uses `MD_READ_SCALING_SESSION_HINT` / `MOTHERDUCK_READ_SCALING_SESSION_HINT`; share path uses RW vs read-scaling hint profiles via `_share_session_hint(origin)`.

**Changes applied (this session):**

- `dashboard.py`: pass `custom_user_agent` explicitly on `MotherDuckConfig` / `MotherDuckClient.for_env` from `MOTHERDUCK_CUSTOM_USER_AGENT` (equivalent to previous env fallback inside the client, but clearer for Streamlit).
- Hydrate `MOTHERDUCK_CUSTOM_USER_AGENT`, `MOTHERDUCK_SESSION_HINT`, and read-scaling session hint keys from `st.secrets` into `os.environ` when missing (same pattern as tokens).
- `.streamlit/secrets.toml.example`: documented optional UA / session hints.

**No mismatch requiring further code change** for env flag names (`MOTHERDUCK_*` / `THYROID_*` aliases) — `motherduck_client.py` implements both.

## Recommended next steps

1. Add a Business read-scaling token to `.streamlit/secrets.toml` as `MD_READ_SCALING_TOKEN` (never commit).
2. Re-run:
   - Direct `connect_read_scaling()` connectivity probe (counts + optional verified tables).
   - `136 … reader --md-env prod` after an approved writer snapshot.
3. Re-check `QUERY_HISTORY` / `RECENT_QUERIES` with filters appropriate to how MotherDuck surfaces `session_name` / `user_agent` for your org.
