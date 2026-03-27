# Manuscript Publication Freeze Workflow

> Immutable, reproducible snapshot of all manuscript-critical tables before
> drafting and submission.  Created 2026-03-15.

## Purpose

Lock a **versioned analysis state** in both local DuckDB prod and the local
export directory so that:

1. Every manuscript number can be traced to a specific table snapshot.
2. A later data refresh (e.g. pending lab/med extract) produces a **v2**
   freeze that can be compared against v1.
3. Reviewers can verify reproducibility from the Parquet + manifest bundle.

## Quick-Start

```bash
# Full freeze from local DuckDB prod
.venv/bin/python scripts/105_manuscript_freeze_v1.py --md

# Dry run (inventory only, no data export)
.venv/bin/python scripts/105_manuscript_freeze_v1.py --md --dry-run

# Freeze + create versioned TABLE copies in local DuckDB
.venv/bin/python scripts/105_manuscript_freeze_v1.py --md --stamp

# Create a second freeze after external data arrives
.venv/bin/python scripts/105_manuscript_freeze_v1.py --md --version v2
```

## Flags

| Flag | Description |
|------|-------------|
| `--md` | Read from local DuckDB prod (recommended) |
| `--dry-run` | Inventory + validation only; no data exported |
| `--stamp` | Create `_freeze_v1` suffixed TABLE copies in local DuckDB |
| `--version TAG` | Freeze version tag (default: `v1`) |
| `--skip-data` | Skip Parquet/CSV export; manifest + inventory only |

## Output Structure

```
exports/manuscript_freeze_v1/
├── manifest.json              # Full provenance, checksums, inventory
├── table_inventory.csv        # Per-table status, row counts, drift
├── rowcount_summary.json      # Compact {table: row_count} map
├── metadata.json              # Git SHA, timestamp, python/duckdb versions
├── verification_report.json   # Post-export integrity check results
├── stamp_results.json         # (if --stamp) local DuckDB stamp log
└── data/
    ├── manuscript_cohort_v1.parquet
    ├── manuscript_cohort_v1.csv
    ├── patient_analysis_resolved_v1.parquet
    ├── episode_analysis_resolved_v1_dedup.parquet
    └── ...                    # one Parquet + optional CSV per table
```

## Table Tiers

| Tier | Behavior | Tables |
|------|----------|--------|
| **Tier 1** | Fail-closed: freeze aborts if any table is missing or row count drifts >1% | Primary cohort, scoring, cancer subset, survival, labs, recurrence, complications |
| **Tier 2** | Warn: logged but freeze continues | Supporting analysis views, canonical episodes, refined extraction |
| **Tier 3** | Informational: included if present | Validation/QA tables |

## How the Freeze Is Versioned

Each freeze creates a directory named `exports/manuscript_freeze_{version}/`.
The `--version` flag defaults to `v1`.  When external data arrives:

1. Re-run the ETL pipeline to ingest new data.
2. Run `scripts/105_manuscript_freeze_v1.py --md --version v2`.
3. Compare `manifest.json` between v1 and v2 to quantify row-count drift
   per table.
4. All v1 exports remain untouched — nothing is overwritten.

## Stamped Copies in local DuckDB

With `--stamp`, the script creates versioned TABLE copies in local DuckDB prod:

```sql
-- e.g. for manuscript_cohort_v1 with --version v1
CREATE TABLE manuscript_cohort_v1_freeze_v1 AS
  SELECT * FROM manuscript_cohort_v1;
```

These frozen copies persist in local DuckDB and are immune to upstream VIEW
rebuilds.  Use them for point-in-time audits:

```sql
SELECT COUNT(*) FROM manuscript_cohort_v1_freeze_v1;
```

## Manifest Schema

```json
{
  "freeze_version": "v1",
  "freeze_type": "manuscript_publication_freeze",
  "created_at": "2026-03-15T12:34:56+00:00",
  "source": {
    "type": "local DuckDB_prod",
    "database": "thyroid_master.duckdb",
    "is_prod": true
  },
  "git": {
    "sha_full": "975cfa2bb0a29262...",
    "sha_short": "975cfa2",
    "dirty": false
  },
  "table_count": { "total": 33, "present": 30, "missing": 3 },
  "exports": [
    {
      "table": "manuscript_cohort_v1",
      "parquet": "manuscript_cohort_v1.parquet",
      "parquet_sha256": "abc123...",
      "rows": 10871,
      "columns": 139
    }
  ]
}
```

## Integration with Existing Tooling

| Script | Relationship |
|--------|-------------|
| `scripts/90_manuscript_freeze_rebuild.py` | Regenerates tables from upstream scripts; run **before** 105 |
| `scripts/26_local DuckDB_materialize_v2.py` | Materializes md_* mirrors; run **before** 105 |
| `scripts/91_promotion_gate.py` | Gate validation; can run **after** 105 as a cross-check |
| `scripts/95_environment_promotion.py` | Environment promotion; **independent** of 105 |

### Recommended pre-freeze sequence

```bash
# 1. Rebuild analysis tables
.venv/bin/python scripts/90_manuscript_freeze_rebuild.py --md

# 2. Materialize to local DuckDB
.venv/bin/python scripts/26_local DuckDB_materialize_v2.py --md

# 3. Freeze
.venv/bin/python scripts/105_manuscript_freeze_v1.py --md --stamp

# 4. Verify (optional cross-check)
.venv/bin/python scripts/91_promotion_gate.py --env prod
```

## Refresh Workflow (Future Data Arrival)

When the pending external lab/medication extract is available:

1. Ingest the new data through the existing ETL chain.
2. Re-run scripts 26 and 90 to rebuild and materialize.
3. Create a new freeze: `--md --version v2 --stamp`.
4. Diff manifests to quantify changes:

```python
import json
v1 = json.load(open("exports/manuscript_freeze_v1/manifest.json"))
v2 = json.load(open("exports/manuscript_freeze_v2/manifest.json"))
for e1 in v1["inventory"]:
    e2 = next((x for x in v2["inventory"] if x["table"] == e1["table"]), None)
    if e2 and e1["actual_rows"] != e2["actual_rows"]:
        print(f"{e1['table']}: {e1['actual_rows']} → {e2['actual_rows']}")
```

5. Update manuscript tables/figures if drift exceeds clinical significance.
6. Archive both v1 and v2 bundles with the Zenodo submission.

## Caveats

- **Git dirty** flag: The manifest records whether the working tree had
  uncommitted changes at freeze time.  Prefer freezing from a clean commit.
- **Network dependency**: `--md` requires `LOCAL_DB_PATH` in the
  environment or `.streamlit/secrets.toml`.
- **Local fallback**: Running without `--md` reads `thyroid_master.duckdb`,
  which may have stale or incomplete data.
