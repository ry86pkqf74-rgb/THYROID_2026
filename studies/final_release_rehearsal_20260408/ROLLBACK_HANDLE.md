# Rollback handle — THYROID_2026 final release

**TAG:** `20260408`  
**Policy:** Pre-promote **zero-copy backup clone** is the primary rollback handle (not named DuckLake snapshots).

## Rehearsal (no `--execute`)

Script **130** `prepromote-backup` **prints only** unless global `--execute` is passed:

- Planned catalog name (example from rehearsal):  
  `Thyroid 2026 Molecular PrePromote 20260408_<UTC>_promote`
- SQL pattern:  
  `CREATE DATABASE "<PrePromoteName>" FROM "Thyroid 2026";`

**DuckLake note:** Named `CREATE SNAPSHOT` is **not** supported on prod (`Thyroid 2026`); output shows `[skip]` and suggests zero-copy clone / history in `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS`. Do not assume named snapshot selectors.

## Live path (only with `PROCEED_PROD_WRITE` + green gates)

1. `130_md_env_bootstrap.py --execute prepromote-backup --label <label>`  
   Prefer `--md-sa` when `MD_SA_TOKEN` is configured in `motherduck.local.toml` or env.
2. Rollback procedure: swap traffic to the PrePromote database or recreate prod from it — see `docs/release_runbook.md`.
3. After promote: **136 writer** (prod snapshot for read-scaling) and **136 reader** (`REFRESH DATABASE`) per runbook; rehearsal used `--dry-run` on those steps.

## Reader refresh

Read-scaling sessions should use:

- `MD_READ_SCALING_SESSION_HINT=thyroid_final_release_ro_20260408` (or current TAG) when a read-scaling token exists.

After any successful live release, analysts/dashboards on read-scaling shares need **136 reader** refresh (or equivalent) to see new snapshots.
