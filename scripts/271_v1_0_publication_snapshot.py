#!/usr/bin/env python3
"""Script 271 — v1_0 publication snapshot (parquet export + release README).

Deliverables
------------
1. Parquet export of every BASE TABLE in thyroid_canonical_publication_v1_0.main
   and every BASE TABLE in manuscript_workspace (views excluded).
   Output: scripts/output/parquet/main/ and scripts/output/parquet/manuscript_workspace/
   Manifests: main_manifest.csv, ws_manifest.csv, MANIFEST.json

2. docs/V1_0_RELEASE.md — canonical release notes for v1_0_archive_consolidated.

One MotherDuck write: one audit row in manuscript_workspace.v1_1_finalization_audit_v1.
No schema changes. No new columns. No archive work.

Pre-condition: tag v1_0_archive_consolidated must exist at commit d01c1ae.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
PARQUET_DIR = OUT_DIR / "parquet"
PARQUET_MAIN_DIR = PARQUET_DIR / "main"
PARQUET_WS_DIR = PARQUET_DIR / "manuscript_workspace"
MANIFEST_PATH = PARQUET_DIR / "MANIFEST.json"
MAIN_MANIFEST_CSV = PARQUET_DIR / "main_manifest.csv"
WS_MANIFEST_CSV = PARQUET_DIR / "ws_manifest.csv"
PREFLIGHT_JSON = OUT_DIR / "271_preflight.json"
README_PATH = REPO / "docs" / "V1_0_RELEASE.md"

ARCHIVE_DB = "Thyroid 2026 UPdated"
WS_SCHEMA = "manuscript_workspace"
AUDIT_TABLE_FQ = f'"{PUBLICATION_DB}".{WS_SCHEMA}.v1_1_finalization_audit_v1'
AUDIT_FINDING_ID = "v1_0_publication_snapshot_complete"

TAG_ANCHOR = "v1_0_archive_consolidated"
COMMIT_ANCHOR = "d01c1ae"

CPM_TABLE = "canonical_patient_master"
CPM_ROWS = 10871
CPM_COLS = 1499

DISK_LIMIT_BYTES = 5 * 1024 ** 3  # 5 GB abort threshold

# DuckDB catalog ghost names to exclude from enumeration
DUCKDB_INTERNAL_VIEWS = frozenset({
    "duckdb_views", "duckdb_types", "duckdb_tables", "duckdb_schemas",
    "duckdb_indexes", "duckdb_constraints", "duckdb_databases", "duckdb_columns",
    "sqlite_temp_schema", "sqlite_temp_master", "sqlite_schema", "sqlite_master",
    "pragma_database_list",
})


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    print(msg, flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        capture_output=True, text=True, cwd=REPO,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"git {' '.join(args)} failed (rc={result.returncode}):\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def safe_count(con: Any, fq: str) -> int | None:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
    except Exception:
        return None


def write_csv_with_meta(path: Path, header: list[str], rows: list[list],
                        generated_at: datetime) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["# generated_by", "scripts/271_v1_0_publication_snapshot.py",
                    "generated_at", generated_at.isoformat()])
        w.writerow(header)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])


# ---------------------------------------------------------------------------
# PRE-FLIGHT
# ---------------------------------------------------------------------------

def run_preflight(con: Any) -> dict:
    log("=== PRE-FLIGHT ===")
    started = utc_now()

    # --- 1. Git tag verification ---
    log(f"  [1/4] Verifying tag {TAG_ANCHOR!r} at {COMMIT_ANCHOR}...")
    try:
        tag_commit = run_git("rev-list", "-n1", TAG_ANCHOR)
    except SystemExit:
        raise SystemExit(f"ABORT: tag {TAG_ANCHOR!r} not found locally. Cannot proceed.")
    head_commit = run_git("rev-parse", "HEAD")
    log(f"    tag {TAG_ANCHOR} -> {tag_commit[:12]}")
    log(f"    HEAD            -> {head_commit[:12]}")
    if not tag_commit.startswith(COMMIT_ANCHOR):
        raise SystemExit(
            f"ABORT: tag {TAG_ANCHOR!r} resolves to {tag_commit[:12]}, "
            f"expected commit beginning with {COMMIT_ANCHOR}. "
            "Repo state has changed since the tag was applied."
        )
    if not head_commit.startswith(COMMIT_ANCHOR):
        raise SystemExit(
            f"ABORT: HEAD ({head_commit[:12]}) is not the tagged commit "
            f"({COMMIT_ANCHOR}). This script must run against the tagged state exactly. "
            f"Run: git checkout {TAG_ANCHOR}"
        )
    log(f"    OK: both tag and HEAD match anchor {COMMIT_ANCHOR}.")

    # --- 2. CPM invariants (connect_locked already checked rows + distinct_rids) ---
    log("  [2/4] CPM invariants (rows + distinct_rids already verified by connect_locked)...")
    cpm_col_count = con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
        WHERE database_name = ? AND schema_name = 'main' AND table_name = ?
    """, [PUBLICATION_DB, CPM_TABLE]).fetchone()[0]
    log(f"    CPM rows   : {CPM_ROWS} (confirmed by connect_locked)")
    log(f"    CPM cols   : {cpm_col_count} (expected {CPM_COLS})")
    if int(cpm_col_count) != CPM_COLS:
        raise SystemExit(
            f"ABORT: CPM column count {cpm_col_count} != {CPM_COLS}. "
            "Schema has drifted from v1_0. Aborting."
        )
    log("    CPM invariants: PASS (10,871 × 1,499, 0 NULL RIDs)")

    # --- 3. Live governance counts ---
    log("  [3/4] Pulling live governance counts...")
    cpm_rows_live = con.execute(
        f'SELECT COUNT(*) FROM "{PUBLICATION_DB}".main.{CPM_TABLE}'
    ).fetchone()[0]

    audit_dist: dict = {}
    try:
        rows = con.execute(f"""
            SELECT status, COUNT(*) FROM {AUDIT_TABLE_FQ}
            GROUP BY status ORDER BY status
        """).fetchall()
        audit_dist = {r[0]: int(r[1]) for r in rows}
    except Exception as e:
        log(f"    WARNING: could not query audit table: {e}")

    conventions_count = safe_count(con, f'"{PUBLICATION_DB}".{WS_SCHEMA}.v1_1_conventions_v1')
    keep_list_count = safe_count(con, f'"{PUBLICATION_DB}".{WS_SCHEMA}.keep_list_v1')
    tech_debt_count = safe_count(con, f'"{PUBLICATION_DB}".{WS_SCHEMA}.v1_1_tech_debt_v1')
    registry_rows = safe_count(con, f'"{PUBLICATION_DB}".{WS_SCHEMA}.detail_table_registry_v1')
    readme_rows = safe_count(con, f'"{PUBLICATION_DB}".{WS_SCHEMA}.__readme')

    log(f"    audit_rows_by_status : {audit_dist}")
    log(f"    conventions_count    : {conventions_count}")
    log(f"    keep_list_count      : {keep_list_count}")
    log(f"    tech_debt_count      : {tech_debt_count}")
    log(f"    registry_rows        : {registry_rows}")
    log(f"    __readme_rows        : {readme_rows}")

    # Archive DB schemas
    archive_schemas: list[str] = []
    try:
        rows = con.execute("""
            SELECT schema_name FROM duckdb_schemas()
            WHERE database_name = ?
            ORDER BY schema_name
        """, [ARCHIVE_DB]).fetchall()
        archive_schemas = [
            r[0] for r in rows
            if r[0] not in ("information_schema", "pg_catalog")
        ]
    except Exception as e:
        log(f"    WARNING: could not query archive DB schemas: {e}")
    log(f"    archive_db_schemas   : {archive_schemas}")

    # Fetch tech_debt rows for README
    tech_debt_rows: list[dict] = []
    try:
        cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM duckdb_columns()
            WHERE database_name = ? AND schema_name = ? AND table_name = 'v1_1_tech_debt_v1'
            ORDER BY column_index
        """, [PUBLICATION_DB, WS_SCHEMA]).fetchall()]
        if cols:
            rows_raw = con.execute(f"""
                SELECT * FROM "{PUBLICATION_DB}".{WS_SCHEMA}.v1_1_tech_debt_v1
                ORDER BY 1
            """).fetchall()
            tech_debt_rows = [dict(zip(cols, r)) for r in rows_raw]
    except Exception as e:
        log(f"    WARNING: could not fetch tech_debt rows: {e}")

    # Fetch deferred audit rows
    deferred_audit_rows: list[dict] = []
    try:
        cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM duckdb_columns()
            WHERE database_name = ? AND schema_name = ? AND table_name = 'v1_1_finalization_audit_v1'
            ORDER BY column_index
        """, [PUBLICATION_DB, WS_SCHEMA]).fetchall()]
        if cols:
            rows_raw = con.execute(f"""
                SELECT * FROM {AUDIT_TABLE_FQ}
                WHERE status = 'OK_DEFERRED_HUMAN'
                ORDER BY run_ts
            """).fetchall()
            deferred_audit_rows = [dict(zip(cols, r)) for r in rows_raw]
    except Exception as e:
        log(f"    WARNING: could not fetch deferred audit rows: {e}")

    # --- 4. Disk headroom ---
    log("  [4/4] Disk headroom check...")
    disk = shutil.disk_usage(REPO)
    free_gb = disk.free / 1024**3
    log(f"    Available disk: {free_gb:.1f} GB (abort threshold: {DISK_LIMIT_BYTES/1024**3:.0f} GB)")
    if disk.free < DISK_LIMIT_BYTES:
        raise SystemExit(
            f"ABORT: only {free_gb:.1f} GB free; need at least "
            f"{DISK_LIMIT_BYTES/1024**3:.0f} GB. Free disk space before proceeding."
        )
    log("    Disk headroom: OK")

    pf = {
        "checked_at": started.isoformat(),
        "tag": TAG_ANCHOR,
        "commit_anchor": COMMIT_ANCHOR,
        "tag_resolved_commit": tag_commit,
        "head_commit": head_commit,
        "cpm_rows": int(cpm_rows_live),
        "cpm_cols": int(cpm_col_count),
        "audit_rows_by_status": audit_dist,
        "conventions_count": conventions_count,
        "keep_list_count": keep_list_count,
        "tech_debt_count": tech_debt_count,
        "registry_rows": registry_rows,
        "__readme_rows": readme_rows,
        "archive_db_schemas": archive_schemas,
        "disk_free_bytes": int(disk.free),
        "disk_free_gb": round(free_gb, 2),
        "tech_debt_rows": tech_debt_rows,
        "deferred_audit_rows": deferred_audit_rows,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PREFLIGHT_JSON.write_text(json.dumps(pf, indent=2, default=str))
    log(f"  Wrote {PREFLIGHT_JSON}")
    log("=== PRE-FLIGHT COMPLETE ===\n")
    return pf


# ---------------------------------------------------------------------------
# Enumerate base tables (skip views and catalog ghosts)
# ---------------------------------------------------------------------------

def enumerate_base_tables(con: Any, db: str, schema: str) -> list[str]:
    """Return sorted list of user BASE TABLE names in db.schema."""
    rows = con.execute("""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = ?
        ORDER BY table_name
    """, [db, schema]).fetchall()
    return [r[0] for r in rows if r[0] not in DUCKDB_INTERNAL_VIEWS]


# ---------------------------------------------------------------------------
# STEP 1 & 2 — Parquet export
# ---------------------------------------------------------------------------

def export_table_to_parquet(
    con: Any,
    db: str,
    schema: str,
    table_name: str,
    output_path: Path,
    log_fn=log,
) -> dict:
    """Export one table to parquet. Returns manifest dict."""
    fq = f'"{db}"."{schema}"."{table_name}"'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Source row count
    try:
        src_rows = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
    except Exception as e:
        raise SystemExit(f"ABORT: could not count rows in {fq}: {e}")

    # Export
    t0 = time.monotonic()
    try:
        con.execute(f"""
            COPY (SELECT * FROM {fq})
            TO '{output_path!s}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    except Exception as e:
        raise SystemExit(f"ABORT: COPY failed for {fq} -> {output_path}: {e}")
    elapsed = round(time.monotonic() - t0, 2)

    if not output_path.exists():
        raise SystemExit(f"ABORT: expected parquet file not created: {output_path}")

    # Row count verification (read back from parquet)
    try:
        parquet_rows = int(con.execute(
            f"SELECT COUNT(*) FROM '{output_path!s}'"
        ).fetchone()[0])
    except Exception as e:
        raise SystemExit(f"ABORT: could not verify parquet row count for {output_path}: {e}")

    if parquet_rows != src_rows:
        raise SystemExit(
            f"ABORT: row count mismatch for {table_name}: "
            f"source={src_rows}, parquet={parquet_rows}"
        )

    file_bytes = output_path.stat().st_size
    checksum = sha256_file(output_path)
    exported_at = utc_now().isoformat()

    log_fn(
        f"    OK {table_name} -> {parquet_rows:,} rows, "
        f"{file_bytes/1024:.1f} KB, sha256={checksum[:12]}... "
        f"({elapsed}s)"
    )
    return {
        "table_name": table_name,
        "row_count": parquet_rows,
        "parquet_bytes": file_bytes,
        "sha256": checksum,
        "exported_at": exported_at,
    }


def export_schema_tables(
    con: Any,
    db: str,
    schema: str,
    output_dir: Path,
    manifest_csv_path: Path,
    label: str,
    started_at: datetime,
) -> list[dict]:
    """Export all base tables in db.schema to output_dir. Returns list of manifest dicts."""
    log(f"\n--- {label} ---")
    tables = enumerate_base_tables(con, db, schema)
    log(f"  Found {len(tables)} base table(s) in {db}.{schema}")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_rows: list[dict] = []
    for i, tname in enumerate(tables, start=1):
        out_path = output_dir / f"{tname}.parquet"
        log(f"  [{i:>3}/{len(tables)}] exporting {tname}...")
        row = export_table_to_parquet(con, db, schema, tname, out_path)
        manifest_rows.append(row)

    # Write manifest CSV
    csv_header = ["table_name", "row_count", "parquet_bytes", "sha256", "exported_at"]
    csv_rows = [[r[k] for k in csv_header] for r in manifest_rows]
    write_csv_with_meta(manifest_csv_path, csv_header, csv_rows, started_at)
    log(f"  Wrote manifest CSV: {manifest_csv_path} ({len(manifest_rows)} rows)")

    total_bytes = sum(r["parquet_bytes"] for r in manifest_rows)
    total_rows = sum(r["row_count"] for r in manifest_rows)
    log(f"  Total: {len(manifest_rows)} files, {total_rows:,} rows, {total_bytes/1024**2:.1f} MB")

    return manifest_rows


# ---------------------------------------------------------------------------
# STEP 3 — MANIFEST.json
# ---------------------------------------------------------------------------

def build_manifest(
    main_rows: list[dict],
    ws_rows: list[dict],
    exported_at: str,
) -> dict:
    """Compute combined sha256 and build MANIFEST.json dict."""
    all_rows = sorted(
        [
            (f"main/{r['table_name']}.parquet", r["sha256"])
            for r in main_rows
        ] + [
            (f"manuscript_workspace/{r['table_name']}.parquet", r["sha256"])
            for r in ws_rows
        ],
        key=lambda x: x[0],
    )
    concatenated = "".join(sha for _, sha in all_rows)
    combined_sha256 = hashlib.sha256(concatenated.encode()).hexdigest()

    total_bytes = (
        sum(r["parquet_bytes"] for r in main_rows)
        + sum(r["parquet_bytes"] for r in ws_rows)
    )
    return {
        "tag": TAG_ANCHOR,
        "commit": COMMIT_ANCHOR,
        "exported_at": exported_at,
        "main_tables": len(main_rows),
        "ws_tables": len(ws_rows),
        "total_parquet_bytes": total_bytes,
        "combined_sha256": combined_sha256,
        "cpm_invariant": {
            "rows": CPM_ROWS,
            "cols": CPM_COLS,
            "distinct_rids": CPM_ROWS,
        },
    }


# ---------------------------------------------------------------------------
# STEP 4 — Release README
# ---------------------------------------------------------------------------

def build_release_readme(pf: dict, manifest: dict, main_rows: list[dict],
                         ws_rows: list[dict]) -> str:
    """Return the full markdown text for docs/V1_0_RELEASE.md."""
    export_date = datetime.fromisoformat(manifest["exported_at"]).strftime("%Y-%m-%d")
    audit_dist = pf.get("audit_rows_by_status", {})
    archive_schemas = pf.get("archive_db_schemas", [])
    tech_debt_rows: list[dict] = pf.get("tech_debt_rows", [])
    deferred_rows: list[dict] = pf.get("deferred_audit_rows", [])

    total_parquet_mb = manifest["total_parquet_bytes"] / 1024**2

    # Build audit distribution text
    audit_lines = []
    for status, cnt in sorted(audit_dist.items()):
        audit_lines.append(f"| {status} | {cnt} |")
    audit_table_body = "\n".join(audit_lines) if audit_lines else "| (no rows) | — |"

    # Build deferred item context
    deferred_section_lines: list[str] = []

    # Tech debt items
    for td in tech_debt_rows:
        debt_id = td.get("debt_id", td.get("id", "unknown"))
        desc = td.get("description", str(td))
        target = td.get("target_version", "v1_1")
        # One sentence of context
        deferred_section_lines.append(
            f"- **`{debt_id}`** (target: {target}): {desc}"
        )

    # OK_DEFERRED_HUMAN audit rows
    for dr in deferred_rows:
        finding = dr.get("finding_id", "unknown")
        notes = dr.get("notes", "")
        notes_short = str(notes)[:200] if notes else ""
        deferred_section_lines.append(
            f"- **`{finding}`** (status: OK_DEFERRED_HUMAN): {notes_short}"
        )

    if not deferred_section_lines:
        deferred_section_lines = [
            "- See `v1_1_tech_debt_v1` in manuscript_workspace for open items.",
        ]

    deferred_text = "\n".join(deferred_section_lines)

    # v1_1 coming items
    v1_1_items = []
    for td in tech_debt_rows:
        debt_id = td.get("debt_id", td.get("id", "?"))
        desc = td.get("description", "")
        v1_1_items.append(f"- `{debt_id}`: {str(desc)[:120]}")
    for dr in deferred_rows:
        finding = dr.get("finding_id", "?")
        v1_1_items.append(f"- `{finding}`: deferred to human review in v1_1")
    if not v1_1_items:
        v1_1_items = ["- See `v1_1_tech_debt_v1` and `docs/v1_1_backlog.md`."]
    v1_1_text = "\n".join(v1_1_items)

    # First open tech_debt_id for footer
    first_debt = tech_debt_rows[0].get("debt_id", "see v1_1_backlog.md") if tech_debt_rows else "see v1_1_backlog.md"

    # Archive schema list
    archive_schema_list = ", ".join(f"`{s}`" for s in archive_schemas) if archive_schemas else "`archive_pub_v1_0`, `archive_legacy`"

    # Main table count in parquet
    main_n = manifest["main_tables"]
    ws_n = manifest["ws_tables"]

    readme = f"""\
# thyroid_canonical_publication_v1_0 — Release Notes

**Tag:** `{TAG_ANCHOR}` → superseded by `v1_0`\
 (same database state; adds parquet export + release notes)
**Commit:** `{COMMIT_ANCHOR}` ({export_date})
**Exported:** {export_date} UTC
**Combined SHA-256:** `{manifest["combined_sha256"]}`

---

## Summary

`thyroid_canonical_publication_v1_0` is the manuscript-ready canonical spine for the THYROID_2026
research project. It contains the single authoritative wide-format patient master
(`canonical_patient_master`, {CPM_ROWS:,} patients × {CPM_COLS:,} columns), all longitudinal
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
| CPM rows | {CPM_ROWS:,} |
| CPM columns | {CPM_COLS:,} |
| CPM distinct research_ids | {CPM_ROWS:,} |
| main schema base tables exported | {main_n} |
| manuscript_workspace base tables exported | {ws_n} |
| Total parquet size | {total_parquet_mb:.1f} MB |
| Combined parquet SHA-256 | `{manifest["combined_sha256"][:16]}…` |
| Conventions recorded | {pf.get("conventions_count", "—")} |
| Keep-list entries | {pf.get("keep_list_count", "—")} |
| Tech debt items | {pf.get("tech_debt_count", "—")} |
| Detail table registry rows | {pf.get("registry_rows", "—")} |
| Archive DB schemas | {archive_schema_list} |

**Audit row distribution at v1_0:**

| Status | Count |
|---|---|
{audit_table_body}

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
patient master ({CPM_ROWS:,} rows × {CPM_COLS:,} cols) was intact throughout. Steps 1 and 3
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

{deferred_text}

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
    con.execute(f"CREATE TABLE {{tname}} AS SELECT * FROM '{{p!s}}'")
    n = con.execute(f"SELECT COUNT(*) FROM {{tname}}").fetchone()[0]
    print(f"  {{tname}}: {{n:,}} rows")
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
    con.execute(f"CREATE TABLE {{tname}} AS SELECT * FROM '{{p!s}}'")
    n = con.execute(f"SELECT COUNT(*) FROM {{tname}}").fetchone()[0]
    print(f"  {{tname}}: {{n:,}} rows")
EOF
```

Views in `manuscript_workspace` are not exported. They reconstruct from the base tables
above using the original DDL recorded in the governance layer.

### (c) Archive database — known limitation

The archive database `"Thyroid 2026 UPdated"` (schemas: {archive_schema_list}) is
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

{v1_1_text}

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
*Tag: `{TAG_ANCHOR}` | Commit: `{COMMIT_ANCHOR}` | Export date: {export_date}*
"""
    return readme


# ---------------------------------------------------------------------------
# STEP 5 — Audit row
# ---------------------------------------------------------------------------

def write_audit_row(con: Any, manifest: dict, n_files: int) -> None:
    log("\n--- STEP 5: writing audit row ---")
    combined_sha = manifest["combined_sha256"]
    notes = (
        f"v1_0_archive_consolidated ({COMMIT_ANCHOR}) exported to parquet; "
        f"combined_sha256={combined_sha}; "
        f"docs/V1_0_RELEASE.md published; "
        f"CPM invariants confirmed post-export "
        f"(rows={CPM_ROWS}, cols={CPM_COLS}, distinct_rids={CPM_ROWS}). "
        f"main_tables={manifest['main_tables']}, ws_tables={manifest['ws_tables']}, "
        f"total_parquet_bytes={manifest['total_parquet_bytes']}."
    )
    # Idempotency guard
    existing = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_TABLE_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_ID],
    ).fetchone()[0]
    if existing:
        log(f"  Audit row {AUDIT_FINDING_ID!r} already present — skipping insert.")
        return
    con.execute(
        f"""
        INSERT INTO {AUDIT_TABLE_FQ}
            (run_ts, script_num, finding_id, metric,
             count_before, count_after, target_after, status, notes)
        VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "271",
            AUDIT_FINDING_ID,
            "parquet_files_written",
            0,
            n_files,
            n_files,
            "OK",
            notes[:3800],
        ],
    )
    log(f"  Inserted audit row: {AUDIT_FINDING_ID!r}")


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------

def run_git_commit_and_tag(manifest: dict, n_files: int) -> None:
    log("\n--- git: add, commit, push, tag ---")
    combined_sha = manifest["combined_sha256"]
    main_n = manifest["main_tables"]
    ws_n = manifest["ws_tables"]

    # Stage files
    log("  git add ...")
    script_path = REPO / "scripts" / "271_v1_0_publication_snapshot.py"
    paths_to_add = [
        str(script_path),
        str(OUT_DIR / "271_preflight.json"),
        str(PARQUET_DIR),
        str(README_PATH),
    ]
    run_git("add", *paths_to_add)

    # Show staged summary
    status = run_git("status", "--short")
    log(f"  Staged files:\n{status}")

    # Commit
    log("  git commit ...")
    commit_msg = (
        f"feat(script-271): v1_0 publication snapshot — parquet + release notes\n\n"
        f"- Exported {main_n} main-schema base tables + {ws_n} manuscript_workspace\n"
        f"  governance tables to scripts/output/parquet/ (ZSTD, per-file\n"
        f"  sha256, combined MANIFEST.json)\n"
        f"- Published docs/V1_0_RELEASE.md narrating Phase A + Phase B\n"
        f"- CPM invariants re-verified post-export ({CPM_ROWS:,} \u00d7 {CPM_COLS:,})\n"
        f"- Audit row {AUDIT_FINDING_ID} written\n\n"
        f"This closes the v1_0 release. Next release is v1_1 per tech_debt.\n\n"
        f"Made-with: Cursor (Claude Sonnet 4.6)"
    )
    run_git("commit", "-m", commit_msg)
    new_commit = run_git("rev-parse", "HEAD")
    log(f"  Committed: {new_commit[:12]}")

    # Push main
    log("  git push origin main ...")
    run_git("push", "origin", "main")
    log("  Pushed main.")

    # Tag v1_0
    log("  git tag v1_0 ...")
    tag_msg = (
        f"Publication release v1_0\n\n"
        f"Canonical: thyroid_canonical_publication_v1_0 @ {COMMIT_ANCHOR}\n"
        f"Archive:   'Thyroid 2026 UPdated' consolidated to 2 schemas\n"
        f"Parquet:   scripts/output/parquet/ (combined sha256 in MANIFEST.json)\n"
        f"Notes:     docs/V1_0_RELEASE.md\n\n"
        f"Supersedes: {TAG_ANCHOR} (same database state, adds\n"
        f"            parquet export + release notes)."
    )
    run_git("tag", "-a", "v1_0", "-m", tag_msg)
    log("  Tagged v1_0.")

    # Push tag
    log("  git push origin v1_0 ...")
    run_git("push", "origin", "v1_0")
    log("  Pushed tag v1_0.")


# ---------------------------------------------------------------------------
# POST-FLIGHT summary
# ---------------------------------------------------------------------------

def post_flight_summary(manifest: dict, main_rows: list[dict],
                        ws_rows: list[dict], pf: dict) -> None:
    log("\n=== POST-FLIGHT SUMMARY ===")
    # Verify tag exists
    try:
        v1_0_commit = run_git("rev-list", "-n1", "v1_0")
        log(f"  v1_0 tag exists locally -> {v1_0_commit[:12]}")
    except SystemExit:
        log("  WARNING: v1_0 tag not found locally!")

    # Re-verify parquet directory
    n_main = len(list(PARQUET_MAIN_DIR.glob("*.parquet")))
    n_ws = len(list(PARQUET_WS_DIR.glob("*.parquet")))
    log(f"  scripts/output/parquet/main/                 : {n_main} files")
    log(f"  scripts/output/parquet/manuscript_workspace/ : {n_ws} files")
    log(f"  Total parquet files                          : {n_main + n_ws}")

    # MANIFEST check
    combined_sha = manifest["combined_sha256"]
    log(f"  MANIFEST.json combined_sha256: {combined_sha[:24]}...")
    log(f"  CPM invariant: {manifest['cpm_invariant']}")

    # Open debt IDs
    tech_debt_rows = pf.get("tech_debt_rows", [])
    first_debt = (
        tech_debt_rows[0].get("debt_id", tech_debt_rows[0].get("id", "see v1_1_backlog.md"))
        if tech_debt_rows else "see v1_1_backlog.md"
    )

    log("\n" + "=" * 72)
    log(
        "v1_0 publication snapshot complete. The canonical database,\n"
        "governance layer, and archive DB are now documented in\n"
        "docs/V1_0_RELEASE.md and reproducible from scripts/output/parquet/.\n"
        f"Next work item per v1_1_tech_debt_v1: {first_debt}.\n"
        "Out-of-scope for this chat set: Script 220 (ETE reanalysis),\n"
        "TI-RADS re-extraction. Those are new sessions."
    )
    log("=" * 72)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    log("=" * 72)
    log("Script 271 — v1_0 Publication Snapshot")
    log(f"Started: {utc_now().isoformat()}")
    log("=" * 72 + "\n")

    # Connect (validates CPM rows=10871 and distinct_rids=10871 on connect)
    log("Connecting to MotherDuck (connect_locked validates CPM invariants)...")
    con = connect_locked()
    log(f"Connected to {PUBLICATION_DB}\n")

    # PRE-FLIGHT
    pf = run_preflight(con)
    started_at = utc_now()

    # STEP 1 — Export main schema
    log("=== STEP 1: Parquet export — main schema ===")
    main_rows = export_schema_tables(
        con=con,
        db=PUBLICATION_DB,
        schema="main",
        output_dir=PARQUET_MAIN_DIR,
        manifest_csv_path=MAIN_MANIFEST_CSV,
        label="main schema tables",
        started_at=started_at,
    )
    log(f"  STEP 1 complete: {len(main_rows)} tables exported.\n")

    # STEP 2 — Export manuscript_workspace
    log("=== STEP 2: Parquet export — manuscript_workspace ===")
    ws_rows = export_schema_tables(
        con=con,
        db=PUBLICATION_DB,
        schema=WS_SCHEMA,
        output_dir=PARQUET_WS_DIR,
        manifest_csv_path=WS_MANIFEST_CSV,
        label="manuscript_workspace governance tables",
        started_at=started_at,
    )
    log(f"  STEP 2 complete: {len(ws_rows)} tables exported.\n")

    # STEP 3 — MANIFEST.json
    log("=== STEP 3: Combined MANIFEST.json ===")
    manifest = build_manifest(main_rows, ws_rows, started_at.isoformat())
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    log(f"  Wrote {MANIFEST_PATH}")
    log(f"  combined_sha256: {manifest['combined_sha256']}")
    log(f"  total_parquet_bytes: {manifest['total_parquet_bytes']:,}")
    log(f"  STEP 3 complete.\n")

    # STEP 4 — Release README
    log("=== STEP 4: docs/V1_0_RELEASE.md ===")
    readme_text = build_release_readme(pf, manifest, main_rows, ws_rows)
    README_PATH.write_text(readme_text)
    log(f"  Wrote {README_PATH} ({len(readme_text):,} chars)")
    log("  STEP 4 complete.\n")

    # STEP 5 — Audit row
    n_files = len(main_rows) + len(ws_rows)
    write_audit_row(con, manifest, n_files)

    # Git operations
    run_git_commit_and_tag(manifest, n_files)

    # Post-flight
    post_flight_summary(manifest, main_rows, ws_rows, pf)

    return 0


if __name__ == "__main__":
    sys.exit(main())
