# MotherDuck capability audit — 2026-04-08

Pure capability audit from **locally available tokens** (modes only; **no** secret values). No commercial plan name inferred — only whether **read/write**, **read-scaling-style** paths, and repo **fail-closed** wiring behave as implemented.

## Summary table

| Capability | Result |
|------------|--------|
| RW token available | Yes (`env:MOTHERDUCK_TOKEN`) |
| RS token available | Yes (`secrets.toml:MD_READ_SCALING_TOKEN`) |
| `connect_rw` validated (smoke + query) | Yes |
| `connect_read_scaling` validated (`SELECT current_database(), current_timestamp`) | Yes (after one transient `TransactionException`, retry succeeded) |
| `PRAGMA database_list` fail-closed gate | Yes — smoke prints “MotherDuck connection verified” |
| Session hint env set (`MD_READ_SCALING_SESSION_HINT`) | No (not required for success) |
| Session hint path in code | Yes — `motherduck_client` + dashboard references |
| Reader dry-run helper (`136 reader --dry-run`) | Yes |
| Writer dry-run helper (`136 writer --dry-run`) | Yes |
| Business-style read-scaling usable on this machine | **Yes** — RS token connects; `REFRESH DATABASE "Thyroid 2026"` completed |

## Environment resolution

- `resolve_database_for_env("dev")` → `Thyroid 2026 Molecular Dev 20260407`
- `resolve_database_for_env("qa")` → `Thyroid 2026 Molecular QA 20260407`
- `resolve_database_for_env("prod")` → `Thyroid 2026`

Source: `config/motherduck_environments.yml` via `motherduck_client.resolve_database_for_env`.

## Catalog / DuckLake

From `130_md_env_bootstrap.py inspect`:

- **Prod** `Thyroid 2026` is **type `DUCKLAKE`** (`ducklake=True`).
- Dev/QA DBs present as **DEFAULT** clones (naming matches YAML).
- **Recent `DATABASE_SNAPSHOTS`** rows exist for prod (clone/read-scaling style history visible in `MD_INFORMATION_SCHEMA`).

## Fail-closed behavior

- `scripts/smoke_test_md_connection.py --md` uses `connect_md_fail_closed` → verifies MotherDuck attachment via `PRAGMA database_list` semantics in `utils/md_connect.py`.
- **PASS** on this run (exit **0**).

## Current-state vs checked-in signoff artifacts

- Fresh `144` output: `md_current_state_refresh.txt` — **`qa.manual_review_queue (NULL verification_status): 0`**.
- Checked-in `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` (2026-04-07) also showed **0** NULL verification rows; older counts/specimen rows may differ (expected drift).
- **`144` masking caveat:** it only reports the **COUNT** of NULL `verification_status`. It does **not** surface non-NULL placeholder / “synthetic automation only” governance posture described in `studies/20260407_publication_signoff_live/final_verdict_memo.md`. Operators must still use **`119_md_formalization_validate.py --md --release-mode`**, MRQ rollups, and human review for manuscript sign-off — not NULL-count alone.
- Live `119` during `make md-v2-gate-md-dryrun` / `124` dry-run: **31 PASS / 3 WARN / 0 FAIL** (molecular dictionary + specimen review burden WARNs).

## Dry-run rehearsal readiness

- `make md-smoke` → **PASS** (0)
- `make md-v2-gate-md-dryrun` → **PASS** (0)
- `make md-live-release-dryrun` → **PASS** (0)

## Operator caution: “dry-run” vs QA DDL

The `124_md_live_release_audit.py --dry-run` chain **invoked** `scripts/114_qa_schema_setup.py --md` **without** `--dry-run`, which applies/verifies QA DDL on the live MotherDuck database. This is **not** a pure read-only audit step. For a strict **no remote mutation** audit, skip `make md-live-release-dryrun` or adjust the runbook/script — **this audit executed the stock Make target as requested.**

## Credential separation

See `fail_closed_separation_test.txt`. Shell `unset` of RW env vars does **not** simulate RS-only mode if `motherduck_client` repopulates RW from `.env` / `.env.motherduck` at import. A dedicated RS-only subprocess test requires an environment **without** RW in those files (or a test harness), not implemented here.

## Evidence files

- `token_source_modes.txt`
- `commands_run.md`
- `md_smoke_output.txt`
- `md_bootstrap_inspect.txt`
- `md_current_state_refresh.txt`
- `read_scaling_validation.txt`
- `fail_closed_separation_test.txt`
- `dryrun_release_path.txt`
