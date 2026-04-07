# Read-only preflight — `md_stage_refresh_20260406_2356`

**UTC (approx):** before `116_md_stage_loader.py --md`

## Credentials (env-backed)

- **token_mode():** `env:MOTHERDUCK_TOKEN`
- **Policy:** The executing Python process used `MOTHERDUCK_TOKEN` in `os.environ`. If the invoking shell had no token, the process bootstrap copied the value from `.streamlit/secrets.toml` into `os.environ` so `token_mode()` is env-backed and MotherDuck helpers see a normal env var (no token values are logged or committed).

## MotherDuck read checks

- **current_database:** `Thyroid 2026`
- **Fail-closed path:** `utils.md_connect.connect_md_fail_closed(thyroid_master.duckdb)` — verified (`PRAGMA database_list` included MotherDuck attachment).
- **v2_stage.load_inventory row count (pre-refresh):** 60
- **Post-staging refresh inventory rows:** 90 (30 loads × 3 history rows retained — see `116` summary; full detail in MotherDuck)

## Parity note

Preflight did not modify data. Staging refresh and QA hydration followed only after this check passed.
