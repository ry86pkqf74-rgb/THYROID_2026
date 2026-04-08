# Stale artifact findings

## `studies/CURRENT_MOTHERDUCK_REPO_STATE.md`

- **Embedded `Commit SHA`** (`3f54a52…`) did not match repo **HEAD** at reconciliation (`78d0edfc…`).
- **Action:** Relabeled header blocks (not a live refresh). Operators should run `scripts/144_md_repo_current_state_summary.py --md` with a local RW token (`motherduck.local.toml` / env per AGENTS.md) to refresh counts and align the embedded SHA after promotions.

## `docs/motherduck_v2_staging_runbook.md`

- **Issue:** Stated `MOTHERDUCK_ENV` optional row claimed all envs map to `Thyroid 2026`, contradicting `config/motherduck_environments.yml` and `motherduck_client.resolve_database_for_env`.
- **Action:** Corrected table row to describe dev/qa/prod mapping and overrides.

## `docs/motherduck_canonical_upload_20260402.md`

- **Issue:** Line documenting initial YAML as “all envs point to `Thyroid 2026`” is accurate for **2026-04-02** but misleading without context for readers in **2026-04+**.
- **Action:** Added short supersession callout at top; preserved historical body.

## `studies/20260407_publication_signoff_live/README.md`

- **Status:** Already self-describes point-in-time validation and points to newer folders for “current” automation. No edit required in this task.

## Release tags / DuckLake wording

- **Live tags** in old `CURRENT_MOTHERDUCK_REPO_STATE.md` (e.g. `20260408r3`) are **historical** until **144 --md** is re-run.
- **DuckLake vs native snapshot** language remains authoritative in `docs/motherduck_sandbox_clone_runbook.md` and `docs/release_runbook.md` §3.1; no contradiction found in `.env.motherduck.example`.
