# MotherDuck V2 Inventory / Consistency Sweep — 2026-04-02

## Outcome

MotherDuck remains unusable as a thyroid reconciliation target in this session.

The exact state after re-probing with both the DuckDB CLI and the repo's own `motherduck_client.py` resolver is:

1. The workspace token is real and usable, but it is only discoverable through `.streamlit/secrets.toml` in this session, not through exported shell env vars.
2. The visible database list includes `Thyroid 2026`, but that catalog's `main` schema has no visible thyroid tables.
3. The repo-configured RW target `thyroid_research_2026` does not exist for this token.
4. The repo-configured RO share path `md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c` does not exist for this token.
5. The only populated non-sample catalog reachable to this token is `rosflow`, which is unrelated to thyroid.

Because of that, there is still no authoritative in-cloud thyroid object set available for local-vs-MotherDuck V2 reconciliation.

## Token / auth findings

Without exposing secret values, the reachable auth sources were:

| Source | Observed behavior |
| --- | --- |
| `MOTHERDUCK_TOKEN` in exported shell env | absent in fresh terminal sessions |
| `MD_SA_TOKEN` | absent |
| `LOCAL_DB_PATH` as JWT/PAT fallback | absent |
| `.streamlit/secrets.toml` `MOTHERDUCK_TOKEN` | present and usable |

The repo resolver reports:

- `TOKEN_MODE secrets.toml:MOTHERDUCK_TOKEN`
- `TOKEN_PRESENT True`

## Exact commands tried

### 1. Root database enumeration through DuckDB CLI with token loaded from workspace secrets

```bash
TOKEN=$(.venv/bin/python -c "import toml; print(toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN'])")
duckdb "md:?motherduck_token=$TOKEN" -c "SHOW DATABASES"
```

Observed result:

```text
Thyroid 2026
md_information_schema
my_db
rosflow
sample_data
```

### 2. Direct inspection of the visible thyroid catalog with DuckDB CLI

```bash
duckdb "md:Thyroid 2026?motherduck_token=$TOKEN" -c "SELECT current_database()"
duckdb "md:Thyroid 2026?motherduck_token=$TOKEN" -c "SELECT table_catalog, COUNT(*) FROM information_schema.tables GROUP BY 1 ORDER BY 1"
```

Observed result:

```text
current_database() = Thyroid 2026

table_catalog counts:
md_information_schema = 8
rosflow = 13
sample_data = 8
```

No `Thyroid 2026` tables appeared in `information_schema.tables`.

### 3. Repo-configured RO share attach via repo resolver

```bash
.venv/bin/python -c "from motherduck_client import MotherDuckClient; client = MotherDuckClient.for_env('prod'); con = client.connect_ro_share()"
```

Observed result:

```text
Failed to attach '_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c':
no database/share named '_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c' found
```

### 4. Repo-configured RW catalog attach via repo resolver

```bash
.venv/bin/python -c "from motherduck_client import MotherDuckClient, MotherDuckConfig, resolve_database_for_env; con = MotherDuckClient(MotherDuckConfig(database=resolve_database_for_env())).connect_rw()"
```

Observed result:

```text
RESOLVED_DB thyroid_research_2026
Failed to attach 'thyroid_research_2026':
no database/share named 'thyroid_research_2026' found
```

### 5. Sanity sweep of the other visible user databases

```bash
.venv/bin/python -c "import toml, duckdb; token = toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN']; ... connect to my_db and rosflow ..."
```

Observed result:

- `my_db` table set is the same `rosflow` + `sample_data` + `md_information_schema` mix.
- `rosflow` contains only `rosflow_*` tables and views.
- No thyroid tables were found in either catalog.

## Diagnosis

This is not a generic MotherDuck outage and not a quoting bug.

The most defensible diagnosis is:

1. The workspace token is valid.
2. The repo's legacy thyroid RW database name and RO share path are both stale or no longer shared to this token.
3. A new database name, access grant, or share mapping likely replaced the historical `thyroid_research_2026` / `thyroid_research_ro` objects.
4. The visible `Thyroid 2026` catalog is either an empty shell, a newly created placeholder, or a database where the thyroid tables have not been materialized/shared to this principal.

## Operational consequence

Do not attempt MotherDuck parity reconciliation yet.

There is no reachable thyroid table inventory to compare against the 14 staged V2 domain parquets, so any parity report would be fabricated.

## Local V2 inventory status

Local V2 staging remains intact in `output/v2_parquets/`:

- 14 domain parquets
- 1 combined parquet

Those staged artifacts remain the live landing zone only.

## Addendum — CLI staging build executed

After the blocker diagnosis above, a direct DuckDB CLI write canary was run successfully against the visible `Thyroid 2026` catalog, followed by a scoped staging build.

Executed build shape:

1. `CREATE SCHEMA IF NOT EXISTS v2_stage`
2. `CREATE OR REPLACE TABLE v2_stage.<parquet_stem> AS SELECT * FROM read_parquet(...)`
3. verified row counts against local parquet metadata

Result:

- 15 tables now exist in `Thyroid 2026.v2_stage`
- all 15 have row parity with the local staged parquets at 11,037 rows each
- the catalog is therefore usable as a writable staging target via the DuckDB CLI

What did **not** change:

- the repo's legacy RW database name `thyroid_research_2026` is still stale for this token
- the repo's legacy RO share path is still stale for this token
- `Thyroid 2026.main` still does not expose the historical thyroid canonical tables the repo expects

That means the current usable state is:

- `Thyroid 2026.v2_stage` = valid MotherDuck staging area created in this session
- legacy repo-wired thyroid canonical access path = still broken/stale

The concrete parity report for the successful staging build is in `studies/motherduck_v2_stage_build_and_parity_20260402.md`.
