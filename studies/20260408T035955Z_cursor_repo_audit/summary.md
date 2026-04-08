# Operator memo — Cursor repo vs MotherDuck audit

**UTC folder:** `studies/20260408T035955Z_cursor_repo_audit`  
**Run date:** 2026-04-08 (session uses repo root `.streamlit/secrets.toml` for RW token)

## Working constraints (from AGENTS.md) — abbreviated

- MotherDuck RW token: `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN` in env or gitignored `.streamlit/secrets.toml`; never log secret values (length / SET / MISSING only).
- No production mutation without consent; this audit used read-only / validation-only commands (no `130 --execute`, no promotion, no local DuckDB writes).
- Prefer repo helpers (`motherduck_client`, `utils/md_connect`); no ad-hoc `duckdb.connect("md:...")`.
- PHI: do not emit full clinical note text in logs or reports.
- Read-scaling token is separate from RW; do not pass read-scaling credentials into RW paths.
- Study artifacts belong under `studies/`.
- Prefer `MD_SA_TOKEN` for automation when available; this session resolved RW via `secrets.toml:MOTHERDUCK_TOKEN`.

## Token modes (evidence-only, no secrets)

| Path | Result |
|------|--------|
| `token_mode()` | `secrets.toml:MOTHERDUCK_TOKEN` |
| `read_scaling_token_mode()` | `none` |
| `get_token()` | resolved; length **467** |
| `get_read_scaling_token()` | not resolved |

**Attribution env (session):**  
`MOTHERDUCK_CUSTOM_USER_AGENT=cursor_repo_audit_v1`  
`MOTHERDUCK_SESSION_HINT=cursor_repo_audit_20260408T035955Z`  
(`MD_READ_SCALING_SESSION_HINT` not set — no RO token.)

## Commands run and exit codes

| Step | Command | Exit |
|------|---------|------|
| Smoke | `.venv/bin/python scripts/smoke_test_md_connection.py --md` | **0** (log: `PASS`) |
| Catalog inspect | `.venv/bin/python scripts/130_md_env_bootstrap.py inspect` | **0** |
| Catalog inspect (SA preference) | `.venv/bin/python scripts/130_md_env_bootstrap.py --md-sa inspect` | **0** |
| **Operator note:** | `inspect --md-sa` (subcommand first) fails argparse; global `--md-sa` must precede `inspect`. Docstring in `130` corrected. | — |
| State summary | `.venv/bin/python scripts/144_md_repo_current_state_summary.py --md --output .../CURRENT_MOTHERDUCK_REPO_STATE.md` | **0** |
| Review triage | `.venv/bin/python scripts/120_review_queue_triage.py --md --output-root .../triage_exports` | **0** |
| Formalization QA release | `.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode --output-dir .../qa_release_mode` | **1** (blocked) |
| Formalization prod (non-release) | `.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env prod --output-dir .../119_prod_non_release` | **0** |
| Observability probe | `md_information_schema.query_history` / `recent_queries` + UA sample | **0** |

**Not run:** `make md-smoke` — unnecessary; smoke script succeeded with same token resolution as Make’s `get_token()` check.

## Artifacts produced

- `smoke_test_md.log` — MotherDuck connection verify + `PASS`
- `130_inspect_default.log`, `130_inspect_md_sa.log` — catalog list includes **prod** `Thyroid 2026` (DUCKLAKE), **QA** `Thyroid 2026 Molecular QA 20260407`, dev / prepromote clones
- `CURRENT_MOTHERDUCK_REPO_STATE.md` — live repo vs MD summary (`144`)
- `triage_exports/review_queue_triage_20260408_040040/` — triage bundle
- `qa_release_mode/validation_report.md` — **119 release-mode on QA**
- `119_prod_non_release/validation_report.md` — structural **119** on prod (no `--release-mode`)
- `119_qa_release_mode.log`, `119_prod_non_release.log`, `144_current_state.log`, `120_triage.log`
- `observability/query_history_probe.log`, `audit_queries_summary.md`, `recent_queries_user_agents_sample.txt`
- `read_scaling_smoke/SKIPPED.md`

## Triage export summary (script 120)

- **total** 11,244; **pending** 0; **reviewed** 11,244  
- **worklist CSVs:** 0  
- Bundle directory: `triage_exports/review_queue_triage_20260408_040040`

## QA existence and validation

- **QA database exists** on MotherDuck: `Thyroid 2026 Molecular QA 20260407` (see `130` inspect output).
- **`119 --md-env qa --release-mode`:** **FAILED** (exit 1). Summary from log: **19 PASS / 0 WARN / 6 FAIL**. Failures included:
  - Canonical row-count parity vs local filesystem for `canonical_extracted_fact_long_v2`, `canonical_fact_quarantine_v2`, `note_extraction_runs` (local=-1 vs MD counts — expected when local parquet inventory missing in CI-style run).
  - Publication governance: synthetic `verification_status` placeholders; NULL `decision_batch_id` on promotion decisions.
  - **Check 12b:** `main.molecular_testing` missing on QA catalog while `molecular_test_episode_v2` populated — upstream spine gap on QA.
- **Prod structural `119` (no release mode):** exit **0** with **28 PASS / 3 WARN / 3 FAIL** in printed summary (see `119_prod_non_release/validation_report.md` for detail). **Not** a release sign-off.

## Read scaling

- **Not available** in this environment (no read-scaling token). See `read_scaling_smoke/SKIPPED.md`.

## Observability

- `information_schema.columns` filtered to `md_information_schema` returned empty for `%query%` / `%recent%` table discovery (driver/catalog behavior); direct `SELECT` from `md_information_schema.recent_queries` succeeded.
- Filter `user_agent ILIKE '%cursor_repo_audit%'` returned **no rows** in `query_history` and `recent_queries` for this session — MotherDuck appears to record **`user_agent`** as client library strings (sample last 2h: `duckdb/v1.4.4(osx_amd64) python/3.14`), not the custom integration UA. Session attribution may be in other columns (e.g. `session_name`) not filtered here.

## Key blockers (release / publication)

1. **QA release-mode 119** does not pass — address synthetic MRQ governance, `decision_batch_id`, molecular_testing spine on QA, and local canonical parity inputs if required for release checks.
2. **Prod non-release 119** still reports FAIL buckets on row-count parity (local=-1) and possibly other structural items — use the generated `validation_report.md` as SSOT for that run.
3. **Read-scaling** token absent — dashboard/reader load tests not exercised.

## Recommended next 3 actions

1. Reconcile **QA** catalog with prod or refresh clone so `molecular_testing` and governance tables match release expectations; re-run **`119 --md-env qa --release-mode`**.
2. Replace synthetic MRQ verification with human-reviewed CSV + hydrate path; stamp **`decision_batch_id`** on promotion decisions per `publication_governance_gate.md`.
3. If Business read-scaling is required, add **`MD_READ_SCALING_TOKEN`** to secrets and run **`136_md_read_scaling_snapshot_refresh.py`** / dashboard probe per `motherduck_read_scaling_dashboard.md`.

## Repo fix bundled with this audit

- `scripts/130_md_env_bootstrap.py` module docstring: correct placement of **`[--md-sa]` before `inspect`** to match argparse.
