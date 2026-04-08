# Read-scaling dashboard validation — run `20260408T053434Z`

## Token status

| Role | Mode | Notes |
|------|------|--------|
| RW | `secrets.toml:MOTHERDUCK_TOKEN` | Present (len 467). Used for writer snapshot + RW connectivity. |
| Read-scaling | **`none`** | **`MD_READ_SCALING_TOKEN` not present** in `.streamlit/secrets.toml` on this host (only key loaded: `MOTHERDUCK_TOKEN`). |

“MD token in .toml” is satisfied for **read/write** (`MOTHERDUCK_TOKEN`). A **separate** Business read-scaling reader token is still required for `connect_read_scaling()` and `scripts/136 … reader` (live).

## Steps executed

| Step | Result |
|------|--------|
| Env: `MOTHERDUCK_CUSTOM_USER_AGENT`, `MD_READ_SCALING_SESSION_HINT`, dashboard prefer/allow flags | Set for shell/session |
| `136 reader --dry-run` | OK — `REFRESH DATABASE "Thyroid 2026"` |
| `136 writer --dry-run` | OK — `CREATE SNAPSHOT OF "Thyroid 2026"` |
| **`136 writer --md-env prod` (live)** | **OK** — snapshot created |
| **`136 reader --md-env prod` (live)** | **Failed** (expected) — `RuntimeError`: no read-scaling token |
| `connect_read_scaling()` | Not run (no token) |
| `connect_rw()` probe | OK — `Thyroid 2026`, 146 main tables; `master_patient_rollup_verified_v1` = 2702 rows; `master_fact_long_verified_v1` = 20188 rows |
| `QUERY_HISTORY` filters | `user_agent` exact = 0; `session_name ILIKE 'cursor_dashboard_ro_%'` = **2** |

## Dashboard / docs

- Env flags in `motherduck_client.py` / `dashboard.py` match `docs/motherduck_read_scaling_dashboard.md`.
- Dashboard continues to use RO share first (RW token until RS token is added and prefer-flag ordering applies).

## Complete read-scaling path (operator)

1. Obtain MotherDuck **read-scaling** API token (reader; not the RW token).
2. Add to gitignored `.streamlit/secrets.toml`:

   ```toml
   MD_READ_SCALING_TOKEN = "..."
   ```

   Or pipe safely:

   ```bash
   printf '%s\n' "$MD_READ_SCALING_TOKEN" | .venv/bin/python scripts/merge_streamlit_read_scaling_token.py
   ```

3. Re-run:

   ```bash
   .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
   ```

4. Optional: re-run a short `connect_read_scaling()` probe and refresh `connectivity.md`.

## Repo changes (this session)

- `scripts/merge_streamlit_read_scaling_token.py` — stdin merge helper for RS token (parallel to `merge_streamlit_motherduck_token.py`).
- This study folder: `connectivity.md`, `summary.md`.
