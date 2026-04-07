# MotherDuck sandbox: zero-copy dev / qa (THYROID_2026)

This runbook describes how to split **dev**, **qa**, and **prod** onto separate MotherDuck databases using Business-tier zero-copy cloning, with a safe bootstrap script.

## Constraints (discovered)

| Topic | Detail |
|--------|--------|
| Production catalog | `Thyroid 2026` is **DUCKLAKE** (`MD_INFORMATION_SCHEMA.DATABASES.type`). |
| Named snapshots | `CREATE SNAPSHOT name OF "Thyroid 2026"` fails with *database is not a native duckdb database* (see `studies/20260407_release_candidate_audit/named_snapshot_error.txt`). |
| Snapshot selectors | Per [MotherDuck `CREATE DATABASE`](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database), `SNAPSHOT_TIME` / `SNAPSHOT_ID` / `SNAPSHOT_NAME` on **`CREATE DATABASE … FROM source`** apply only when the **source** is **native** storage — **not** for DuckLake sources. |
| Automatic history | `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` still lists rows for DuckLake (unnamed / automatic); use for auditing, not for named `CREATE SNAPSHOT`. |
| Clones from DuckLake | Use a **latest-state** zero-copy clone: `CREATE DATABASE "…" FROM "Thyroid 2026"` with **no** snapshot clause. |

## Generated SQL (reference)

**Inspect catalog (read-only):**

```sql
SELECT name, type, created_ts
FROM MD_INFORMATION_SCHEMA.DATABASES
WHERE name ILIKE '%thyroid%'
ORDER BY name;

SELECT database_name, snapshot_id, snapshot_name, created_ts
FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS
WHERE database_name = 'Thyroid 2026'
ORDER BY created_ts DESC
LIMIT 15;
```

**Named snapshot (native DBs only — not for current prod DuckLake):**

```sql
CREATE SNAPSHOT "pre_schema_20260407" OF "Thyroid 2026";
```

**Zero-copy clone (DuckLake-safe — latest state):**

```sql
CREATE OR REPLACE DATABASE "Thyroid 2026 Molecular Dev 20260407" FROM "Thyroid 2026";
CREATE OR REPLACE DATABASE "Thyroid 2026 Molecular QA 20260407" FROM "Thyroid 2026";
```

When applying manually the first time (empty target), `CREATE DATABASE … FROM …` is enough. For idempotent automation, the bootstrap script uses `CREATE OR REPLACE … FROM …` when `--execute` is set.

**Refresh dev from latest prod:**

```sql
DROP DATABASE IF EXISTS "Thyroid 2026 Molecular Dev 20260407";
CREATE DATABASE "Thyroid 2026 Molecular Dev 20260407" FROM "Thyroid 2026";
```

**Snapshot-point clone (native source only):**

```sql
CREATE DATABASE audit_db FROM native_prod (SNAPSHOT_NAME 'prod_backup');
CREATE DATABASE audit_db FROM native_prod (SNAPSHOT_ID '3f2504e0-4f89-11d3-9a0c-0305e82c3301');
CREATE DATABASE audit_db FROM native_prod (SNAPSHOT_TIME '2025-07-29 14:30:25.123456');
```

## Environment mapping

After bootstrap, `config/motherduck_environments.yml` is:

- **prod:** `"Thyroid 2026"`
- **dev:** `"Thyroid 2026 Molecular Dev 20260407"`
- **qa:** `"Thyroid 2026 Molecular QA 20260407"`

The date suffix should be bumped when you create a **new** pair of sandboxes; update the YAML to match, or override with `MOTHERDUCK_DATABASE`.

## Bootstrap script

`scripts/130_md_env_bootstrap.py` — all DDL is gated by **`--execute`** (default is dry-run).

Put global options **before** the subcommand (for example `--execute` before `clone`).

```bash
cd THYROID_2026
.venv/bin/python scripts/130_md_env_bootstrap.py inspect
.venv/bin/python scripts/130_md_env_bootstrap.py snapshot --name pre_schema_20260407   # skips for DuckLake unless forced
.venv/bin/python scripts/130_md_env_bootstrap.py clone --dev --qa                      # print SQL only
.venv/bin/python scripts/130_md_env_bootstrap.py --execute clone --dev --qa            # apply (uses RW token)
.venv/bin/python scripts/130_md_env_bootstrap.py validate --database "Thyroid 2026 Molecular Dev 20260407"
.venv/bin/python scripts/130_md_env_bootstrap.py --execute refresh-dev --latest
```

Authentication: same as the rest of the repo — `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` or `.streamlit/secrets.toml` (see `motherduck_client.py`).

## Validation

After clone, `validate` should report `current_database` matching the dev (or qa) name and a non-zero `main` table count when promotion has populated `main`.

---

**References:** [CREATE SNAPSHOT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-snapshot), [CREATE DATABASE](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database), [DATABASE_SNAPSHOTS](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/md_information_schema/database_snapshots).
