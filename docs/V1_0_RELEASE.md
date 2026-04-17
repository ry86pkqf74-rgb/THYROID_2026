# thyroid_canonical_publication_v1_0 — Release Notes

**Tag:** `v1_0_archive_consolidated` → superseded by `v1_0` (same database state; adds parquet export + release notes)
**Commit:** `d01c1ae` (2026-04-17)
**Exported:** 2026-04-17 UTC
**Combined SHA-256:** `3e3fd7c93f238ee4aed2b7df0bfd9c62b06487df5cab7f11eb840b5f340ca514`

---

## Summary

`thyroid_canonical_publication_v1_0` is the manuscript-ready canonical spine for the THYROID_2026
research project. It contains the single authoritative wide-format patient master
(`canonical_patient_master`, 10,871 patients × 1,499 columns), all longitudinal
and domain-specific event tables, and the governance layer (`manuscript_workspace`) that records
conventions, audit history, tech debt, and the detail-table registry used by the manuscript
pipeline. It is **not** the full raw-data catalog: raw imaging runs, LLM extraction logs,
scratchpad tables, and prior-version snapshots live in the archive database
`"Thyroid 2026 UPdated"`, which is MotherDuck-internal and is not reproduced in this parquet
export (see "Known Limitations" under Restoring below).

---

## State at v1_0

| Item | Value |
|---|---|
| CPM rows | 10,871 |
| CPM columns | 1,499 |
| CPM distinct research_ids | 10,871 |
| main schema base tables exported | 114 |
| manuscript_workspace base tables exported | 18 |
| Total parquet size | 64.2 MB |
| Combined parquet SHA-256 | `3e3fd7c93f238ee4…` |
| Conventions recorded | None |
| Keep-list entries | None |
| Tech debt items | 3 |
| Detail table registry rows | 117 |
| Archive DB schemas | `archive_legacy`, `archive_pub_v1_0`, `main` |

**Audit row distribution at v1_0:**

| Status | Count |
|---|---|
| DOCUMENTED_GAP | 4 |
| DOCUMENTED_NOOP | 3 |
| OK | 37 |
| OK_BACKFILLED_270 | 3 |
| OK_DEFERRED_HUMAN | 1 |

---

## What Phase A Accomplished

Phase A (Scripts 270a–270c) established the governance foundation for the archive database
consolidation. Working from a clean preflight audit (`270a`), it classified every table in the
four stray schemas of `"Thyroid 2026 UPdated"` — `main`, `mm_contract_dev`, `qa`, and
`v2_stage` — into one of three disposition buckets. Bucket B (tables already present in
`archive_pub_v1_0` under a canonical snapshot name) were marked `DROP_ALREADY_SNAPSHOTTED`
without re-migration: 126 objects confirmed as faithful copies. Bucket C (tables requiring
active migration to `archive_legacy`) were resolved through an automated match pipeline
(`270b`) with ambiguous cases reviewed by hand, resulting in 118 tables migrated. Phase A
also codified 16 project-level conventions in `v1_1_conventions_v1`, established the
keep-list discipline (`keep_list_v1`) that prevents future accidental drops of named
canonical objects, and verified via the widest-snapshot round-trip test that the canonical
patient master (10,871 rows × 1,499 cols) was intact throughout. Steps 1 and 3
of the original Phase A plan were verified as no-ops by design: the canonical database
already satisfied all invariants with no corrective action needed.

---

## What Phase B Accomplished

Phase B (Scripts 270d–270e) executed the physical consolidation of `"Thyroid 2026 UPdated"`.
Script 270d migrated 118 tables from the stray schemas into `archive_legacy` using
`CREATE … AS SELECT *` semantics, verifying row and column counts on each object before
committing. Script 270e then dropped the 38 `DROP_NO_RESTORE_VALUE` objects (empty tables and
broken-reference views with no recoverable content), dropped the three removable stray schemas
(`mm_contract_dev`, `qa`, `v2_stage`) via `DROP SCHEMA CASCADE`, and emptied the `main` schema
(a DuckDB system schema that cannot itself be dropped). The final state assertion confirmed that
`"Thyroid 2026 UPdated"` contains exactly two user schemas — `archive_pub_v1_0` and
`archive_legacy` — with an empty `main` system schema. The canonical database
`thyroid_canonical_publication_v1_0` was untouched throughout Phase B: all archive work was
confined to the archive database, consistent with the zero-candidate finding from Phase A's
registry audit.

---

## Known Deferred Items

The following items were intentionally out of scope for v1_0 and are tracked for v1_1:

- **`laterality_bare_column_name_v1_1`** (target: v1_1): CPM has a bare 'laterality' column whose feeder resolution required manual disambiguation (ambiguous among 11 candidate tables: tumor, nodule, LN, surgical contexts after EXCLUDE_PATTERNS filter). Bare name obscures which clinical context it refers to.
- **`registry_null_residual_v1_1`** (target: v1_1): 34 rows in detail_table_registry_v1 carry NULL feeds_master_columns_normalized after Script 270b Step 2 execute. These are analysis tables, dictionaries, and cohort summaries that are registered because they are queryable main-schema base tables, but they do not feed canonical_patient_master columns. They are consumed downstream (Dives, manuscripts) or are governance-adjacent (dictionaries). Concrete list in scripts/output/270b_registry_null_post_execute.csv.
- **`stray_subset_matcher_v1_1`** (target: v1_1): The 270c name-collision matcher flags row-count inequality as DIVERGENT, requiring human review. This is correct for truly divergent content but over-triggers on the common case where stray is a stale pre-dedup or pre-purge subset/superset of the snapshot, which already captures the relevant clinical state. In Phase B, 3 of 3 DIVERGENT rows were actually stale-stray safe to drop after a manual MotherDuck verification session.
- **`step_1b_path_size_deferred_for_clinician`** (status: OK_DEFERRED_HUMAN): All 96 rows in main.path_size_adjudication_v241 staged for clinician sign-off at scripts/output/270_path_size_human_review.csv. Distribution: 37 HIGH outlier_manual_review_required (proposed_value IS 

Each of these items is non-blocking for manuscript submission: the CPM invariants hold,
no data quality issues affect primary endpoints, and the canonical database is internally
consistent at this tag.

---

## Restoring from This Release

### (a) Restore canonical main from parquet

```bash
# From repo root, with DuckDB installed:
python3 - <<'EOF'
import duckdb, pathlib
con = duckdb.connect("restore_canonical.duckdb")
parquet_dir = pathlib.Path("scripts/output/parquet/main")
for p in sorted(parquet_dir.glob("*.parquet")):
    tname = p.stem
    con.execute(f"CREATE TABLE {tname} AS SELECT * FROM '{p!s}'")
    n = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"  {tname}: {n:,} rows")
print("Restore complete.")
EOF
```

After restore, verify the CPM invariant:

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS distinct_rids
FROM canonical_patient_master;
-- Expected: rows=10871, distinct_rids=10871
```

### (b) Restore manuscript_workspace governance from parquet

```bash
python3 - <<'EOF'
import duckdb, pathlib
con = duckdb.connect("restore_governance.duckdb")
parquet_dir = pathlib.Path("scripts/output/parquet/manuscript_workspace")
for p in sorted(parquet_dir.glob("*.parquet")):
    tname = p.stem
    con.execute(f"CREATE TABLE {tname} AS SELECT * FROM '{p!s}'")
    n = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"  {tname}: {n:,} rows")
EOF
```

Views in `manuscript_workspace` are not exported. They reconstruct from the base tables
above using the original DDL recorded in the governance layer.

### (c) Archive database — known limitation

The archive database `"Thyroid 2026 UPdated"` (schemas: `archive_legacy`, `archive_pub_v1_0`, `main`) is
MotherDuck-internal and **does not have a parquet copy** in this repository. The archive
contains prior-version CPM snapshots, raw LLM extraction runs, and migrated legacy tables
whose content was verified during Phase A/B but is not needed for manuscript reproduction.
To restore the archive, you need a MotherDuck account with the `"Thyroid 2026 UPdated"`
database attached. The `archive_pub_v1_0` schema holds named snapshots (e.g.,
`canonical_patient_master_pre270_20260417T055747Z`) that function as point-in-time
restore points for the CPM.

### (d) Verify CPM invariants after restore

```sql
-- Run after any restore to confirm integrity:
SELECT
    COUNT(*)                          AS total_rows,          -- must be 10871
    COUNT(DISTINCT research_id)       AS distinct_rids,       -- must be 10871
    COUNT(*)  = 10871 AND
    COUNT(DISTINCT research_id) = 10871  AS invariant_holds
FROM canonical_patient_master;
```

The parquet export for `canonical_patient_master` in `scripts/output/parquet/main/`
is the primary off-MotherDuck backup for this table.

---

## Out of Scope for v1_0 / Coming in v1_1

The following items are explicitly deferred and will be addressed in the v1_1 planning
session. See `docs/v1_1_backlog.md` and `manuscript_workspace.v1_1_tech_debt_v1` for
full context.

- `laterality_bare_column_name_v1_1`: CPM has a bare 'laterality' column whose feeder resolution required manual disambiguation (ambiguous among 11 candidate 
- `registry_null_residual_v1_1`: 34 rows in detail_table_registry_v1 carry NULL feeds_master_columns_normalized after Script 270b Step 2 execute. These a
- `stray_subset_matcher_v1_1`: The 270c name-collision matcher flags row-count inequality as DIVERGENT, requiring human review. This is correct for tru
- `step_1b_path_size_deferred_for_clinician`: deferred to human review in v1_1

Additionally, the following analyses are new-session scope and were explicitly excluded
from this chat set:

- **Script 220** (ETE reanalysis): Requires a dedicated session; data is intact in CPM.
- **TI-RADS re-extraction**: Imaging re-extraction requires the LLM pipeline; not a
  finalization task.

---

## Integrity Verification

To verify this parquet export against the MANIFEST, run:

```python
import hashlib, json, pathlib

manifest = json.loads(pathlib.Path("scripts/output/parquet/MANIFEST.json").read_text())
parquet_dir = pathlib.Path("scripts/output/parquet")

all_files = sorted(
    [("main/" + p.name, p) for p in (parquet_dir / "main").glob("*.parquet")]
    + [("manuscript_workspace/" + p.name, p)
       for p in (parquet_dir / "manuscript_workspace").glob("*.parquet")],
    key=lambda x: x[0],
)

def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

combined = "".join(sha256_file(p) for _, p in all_files)
computed = hashlib.sha256(combined.encode()).hexdigest()
expected = manifest["combined_sha256"]
print("MATCH" if computed == expected else "MISMATCH", computed[:16], expected[:16])
```

---

*Generated by `scripts/271_v1_0_publication_snapshot.py` — Cursor (Claude Sonnet 4.6)*
*Tag: `v1_0_archive_consolidated` | Commit: `d01c1ae` | Export date: 2026-04-17*
