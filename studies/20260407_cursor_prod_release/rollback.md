# Rollback handles — cursor prod release (2026-04-07)

## PrePromote zero-copy clones (catalog-level)

| Label / purpose | Exact MotherDuck database name |
|-----------------|--------------------------------|
| Step 3 (explicit handle before promote) | `Thyroid 2026 Molecular PrePromote 20260407_cursor_prod_release` |
| Step 5 orchestrator (`137 promote --execute`, default backup label) | `Thyroid 2026 Molecular PrePromote 20260410_20260407_161831_promote` |

Rollback playbook (from `docs/release_runbook.md`): stop writes to prod; repoint consumers or recreate `Thyroid 2026` from one of the above clones per MotherDuck ops policy; run reader refresh after writer state is stable.

## Schema-scoped publication

- **Immutable slice:** `release_20260410` in catalog `Thyroid 2026` (created during the live `124` run).
- **Prior releases:** `release_20260409` and earlier `release_*` schemas documented in `qa.release_manifest` remain available for analyst downgrade.

## Named snapshot / time-travel

- Prod catalog type is **DUCKLAKE**. Named `CREATE SNAPSHOT name OF …` is **not** supported; automatic rows in `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` are for audit only (see `docs/motherduck_sandbox_clone_runbook.md`).
- Do **not** rely on `SNAPSHOT_ID` / `SNAPSHOT_NAME` on `CREATE DATABASE … FROM "Thyroid 2026"` for DuckLake sources.

## Post-release data repair (MRQ)

- After `124`, `qa.manual_review_queue` gained a new batch with `run_label = promotion_gate` and NULL `verification_status`, duplicated alongside historical reviewed rows. **114** now backfills verification from prior matching keys after each hydrate; a **one-time** `backfill_mrq_verification_from_prior_rows` was executed on prod for `promotion_gate` before `119` could PASS.
