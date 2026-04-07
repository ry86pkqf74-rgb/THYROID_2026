# Backup and recovery artifact

## Live database

| Property | Value |
|----------|--------|
| Platform | MotherDuck |
| Database | `Thyroid 2026` |
| Catalog type | **DUCKLAKE** (`md_information_schema.databases.type`) |
| Historical snapshot retention (as reported) | 7 days (platform metadata) |

## Native snapshot API

Preflight query to `md_information_schema.snapshots` failed in this environment (**catalog reports no snapshots table**). Per project runbook, **DuckLake databases may not support** the same native `CREATE SNAPSHOT` / PIT workflows as native DuckDB databases on MotherDuck.

## Governed recovery strategy (authoritative)

1. **Append-only `release_YYYYMMDD*` schemas** — never overwritten by `115_release_snapshot.py` (fails closed if schema exists).
2. **`qa.release_manifest`** — machine-readable list of tags, tables, row counts, timestamps.
3. **Curated parquet bundle** — `exports/parquet_release_20260407_final2/manifest.json` includes SHA-256 checksums per file.
4. **Local git + study artifacts** — this folder documents operational evidence and validation verdicts.

## This release

- **Tag:** `20260407_final2`
- **Schema:** `release_20260407_final2`
- **Bundle directory:** `exports/parquet_release_20260407_final2/`

For forensic replay, combine (3) and MotherDuck `release_*` schema dumps as needed.
