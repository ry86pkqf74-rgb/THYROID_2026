# DuckLake, snapshots, and zero-copy clones — audit notes

Sources: live `130 inspect` output (`md_inspect_output.txt`), dry-run `prepromote-backup` (`prepromote_capability_probe.txt`), and repo runbooks (`docs/motherduck_sandbox_clone_runbook.md`, `docs/release_runbook.md`).

## Production catalog type

- **`Thyroid 2026`** is **`DUCKLAKE`** in `MD_INFORMATION_SCHEMA.DATABASES` (see `md_inspect_output.txt`: `type='DUCKLAKE' ducklake=True`).

## Named `CREATE SNAPSHOT`

- Repo automation (`scripts/130_md_env_bootstrap.py` `snapshot` subcommand) **skips** named snapshots for DuckLake unless `--force-native-snapshot` — aligned with documented MotherDuck behavior for non-native catalogs.
- Live `DATABASE_SNAPSHOTS` rows for prod show **`snapshot_name` = NULL** (automatic / unnamed history), with many `snapshot_id` rows — consistent with “history exists, named snapshot DDL not the operator path for DuckLake.”

## Pre-promote rollback / dry-run clone

Dry-run (no `--execute`):

```text
CREATE DATABASE "Thyroid 2026 Molecular PrePromote capability_probe_20260407" FROM "Thyroid 2026";
```

This matches the **DuckLake-safe** pattern: **latest-state zero-copy clone**, no `SNAPSHOT_ID` / `SNAPSHOT_NAME` selector. Documented rollback posture: `docs/release_runbook.md` §3.1 and `docs/motherduck_sandbox_clone_runbook.md`.

## Read-scaling writer SQL (136)

- Writer dry-run prints `CREATE SNAPSHOT OF "Thyroid 2026"` (unnamed). Whether execution is allowed on DuckLake is a **server capability** question; this audit did **not** execute writer snapshot DDL (only `--dry-run`).

## Marketing / “Pro” label

This audit does **not** assert MotherDuck **“Pro”** or **“Business”** subscription labels. Read-scaling is described in MotherDuck’s public docs as a product capability; **on this machine**, no `MD_READ_SCALING_TOKEN` was configured, so **read-scaling attach was not exercised live**.
