# Database attachment, engine type, snapshots

**Evidence:** queries captured in `snapshot_metadata.md` (sections *Session*, *MD_INFORMATION_SCHEMA.DATABASES*, *DATABASE_SNAPSHOTS*).

## Attachment target

| Property | Value |
|----------|--------|
| `current_database()` | `Thyroid 2026` |
| Config default / override | From `config/motherduck_environments.yml` → `"Thyroid 2026"`; overridable via `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` |

## Native vs DuckLake

From `MD_INFORMATION_SCHEMA.DATABASES`:

| name | type | historical_snapshot_retention |
|------|------|-------------------------------|
| Thyroid 2026 | **DUCKLAKE** | 7 days |

Other databases on the account (e.g. `my_db`) show `DEFAULT`; this catalog is explicitly **DUCKLAKE**.

## Snapshot availability

- **Automatic / storage snapshots:** `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` returns many rows for `database_name = 'Thyroid 2026'` with timestamps and byte stats — feature is **available** for this database.
- **Named snapshot DDL:** `scripts/126_release_candidate_motherduck_audit.py` supports optional `--create-named-snapshot`; this audit run did **not** execute a named snapshot (read-mostly pack).

## Schemas (table counts)

From `information_schema.tables` grouped by `table_schema`:

| Schema | n_tables (audit time) |
|--------|----------------------:|
| main | 85 |
| v2_stage | 38 |
| qa | 11 |
| release_20260406 | 6 |
| release_20260407 | 5 |
| release_20260408 | 10 |
| release_20260409 | 10 |
| mm_contract_dev | 26 |

## Key object presence (verified live)

| Object | Rows / status |
|--------|----------------|
| `v2_stage.*` (23 domain stems + loader tables) | 23 stems @ 11,037 rows each; see `row_count_reconciliation.md` |
| `v2_stage.load_inventory` | 150 |
| `main.canonical_extracted_fact_long_v2` | 123,577 |
| `main.canonical_fact_quarantine_v2` | 199 |
| `main.note_extraction_runs` | 3 |
| `qa.manual_review_queue` | 16,866 (0 pending) |
| `qa.release_manifest` | 4 rows |
| `main.thyroglobulin_lab_canonical_v1` | 76,971 |
| `main.longitudinal_lab_canonical_v1` | 76,971 |
| `release_20260409.*` | Present (see `release_manifest_summary.md`) |
