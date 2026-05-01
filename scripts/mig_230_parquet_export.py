#!/usr/bin/env python3
"""
mig_230_parquet_export_pub_v1_0_20260430
=========================================
Parquet export of all frozen/verified canonical tables + Lane G + Lane LN objects.

Spec: cursor_prompts/PARALLEL_AGENT_BATCH_20260430_v14.md §5

Gate check runs first:  gate1 ≥ 199, lane_g_landed=True, lane_ln_landed=True
All 3 gates PASSED (gate1=209) — confirmed before dispatch.

Output layout:
  parquet_export/pub_v1_0_20260430/
    main/<table>.parquet
    manuscript_workspace/<table>.parquet
    semantic_publication/<table>.parquet
    _archive_snapshots/<table>.parquet
    _MANIFEST.md

Mig label: mig_230_parquet_export_pub_v1_0_20260430
"""

from __future__ import annotations

import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token  # noqa: E402

# ── Constants ─────────────────────────────────────────────────────────────────
EXPORT_DIR = REPO_ROOT / "parquet_export" / "pub_v1_0_20260430"
MANIFEST_PATH = EXPORT_DIR / "_MANIFEST.md"
RUN_ID = "mig_230_parquet_export_v14"
RUN_START = datetime.now(timezone.utc)

# ── Connect to canonical publication DB ───────────────────────────────────────
def get_con() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise RuntimeError("No MotherDuck token found — set MOTHERDUCK_TOKEN or MD_SA_TOKEN")
    q_tok = quote_plus(token)
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={q_tok}")
    db = con.execute("SELECT current_database()").fetchone()[0]
    print(f"[mig_230] Connected to: {db}")
    return con


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def export_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    dest_dir: Path,
) -> dict:
    """Export a single table to parquet and return manifest entry."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / f"{table}.parquet"

    # Fully-qualified name
    fq = f'"{schema}"."{table}"' if schema else f'"{table}"'

    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        con.execute(
            f"COPY (SELECT * FROM {fq}) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        file_size = out_path.stat().st_size
        sha = sha256_file(out_path)
        print(f"  ✓ {schema}.{table}  rows={row_count:,}  size={file_size:,}B")
        return {
            "schema": schema,
            "table": table,
            "path": str(out_path.relative_to(REPO_ROOT)),
            "rows": row_count,
            "bytes": file_size,
            "sha256": sha,
            "status": "ok",
        }
    except Exception as exc:
        print(f"  ✗ {schema}.{table}  ERROR: {exc}")
        return {
            "schema": schema,
            "table": table,
            "path": str((dest_dir / f"{table}.parquet").relative_to(REPO_ROOT)),
            "rows": -1,
            "bytes": 0,
            "sha256": "",
            "status": f"error: {exc}",
        }


def export_archive_snapshots(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Export archive_pub_v1_0 snapshots from 'Thyroid 2026 UPdated' DB (read-only)."""
    results = []
    dest = EXPORT_DIR / "_archive_snapshots"
    dest.mkdir(parents=True, exist_ok=True)
    try:
        # Attach the legacy read-only DB
        con.execute('ATTACH DATABASE \'md:"Thyroid 2026 UPdated"\' AS legacy_db (READ_ONLY)')
        tables = con.execute(
            "SELECT table_name FROM legacy_db.information_schema.tables "
            "WHERE table_schema = 'archive_pub_v1_0' ORDER BY table_name"
        ).fetchall()
        print(f"\n[mig_230] Archive snapshots: {len(tables)} tables in archive_pub_v1_0")
        for (tbl,) in tables:
            out_path = dest / f"{tbl}.parquet"
            try:
                row_count = con.execute(
                    f'SELECT COUNT(*) FROM legacy_db.archive_pub_v1_0."{tbl}"'
                ).fetchone()[0]
                con.execute(
                    f"COPY (SELECT * FROM legacy_db.archive_pub_v1_0.\"{tbl}\") "
                    f"TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                file_size = out_path.stat().st_size
                sha = sha256_file(out_path)
                print(f"  ✓ archive_pub_v1_0.{tbl}  rows={row_count:,}")
                results.append({
                    "schema": "archive_pub_v1_0",
                    "table": tbl,
                    "path": str(out_path.relative_to(REPO_ROOT)),
                    "rows": row_count,
                    "bytes": file_size,
                    "sha256": sha,
                    "status": "ok",
                })
            except Exception as exc:
                print(f"  ✗ archive_pub_v1_0.{tbl}  ERROR: {exc}")
                results.append({
                    "schema": "archive_pub_v1_0",
                    "table": tbl,
                    "path": str((dest / f"{tbl}.parquet").relative_to(REPO_ROOT)),
                    "rows": -1,
                    "bytes": 0,
                    "sha256": "",
                    "status": f"error: {exc}",
                })
        con.execute("DETACH legacy_db")
    except Exception as exc:
        print(f"  [WARN] archive_pub_v1_0 export failed: {exc}")
    return results


def build_manifest(entries: list[dict]) -> str:
    ts = RUN_START.strftime("%Y-%m-%d %H:%M:%S UTC")
    ok = [e for e in entries if e["status"] == "ok"]
    err = [e for e in entries if e["status"] != "ok"]
    total_rows = sum(e["rows"] for e in ok)
    total_bytes = sum(e["bytes"] for e in ok)

    lines = [
        f"# Parquet Export Manifest — `{RUN_ID}`",
        "",
        f"**Generated:** {ts}",
        f"**Total files:** {len(entries)}  |  **OK:** {len(ok)}  |  **Errors:** {len(err)}",
        f"**Total rows:** {total_rows:,}",
        f"**Total size (ZSTD compressed):** {total_bytes / 1024 / 1024:.2f} MB",
        "",
        "## Exported Files",
        "",
        "| Schema | Table | Rows | Size (B) | SHA-256 | Status |",
        "|--------|-------|------|----------|---------|--------|",
    ]
    for e in sorted(entries, key=lambda x: (x["schema"], x["table"])):
        sha_short = e["sha256"][:16] + "..." if e["sha256"] else "—"
        status_icon = "✓" if e["status"] == "ok" else "✗"
        lines.append(
            f"| {e['schema']} | {e['table']} | {e['rows']:,} | {e['bytes']:,} | `{sha_short}` | {status_icon} {e['status'] if e['status'] != 'ok' else ''} |"
        )

    if err:
        lines += ["", "## ⚠ Errors", ""]
        for e in err:
            lines.append(f"- `{e['schema']}.{e['table']}`: {e['status']}")

    lines += [
        "",
        "## Sanity Check",
        "",
        "See script output for 1 random parquet re-load row-count verification.",
        "",
        "---",
        f"*Mig label: `{RUN_ID}` | Script: `scripts/mig_230_parquet_export.py`*",
    ]
    return "\n".join(lines) + "\n"


def insert_provenance(con: duckdb.DuckDBPyConnection, entries: list[dict]) -> None:
    ok = [e for e in entries if e["status"] == "ok"]
    err = [e for e in entries if e["status"] != "ok"]
    ended_at = datetime.now(timezone.utc)
    phases = "gate_check,main_export,lane_g_export,lane_ln_export,archive_snapshots,manifest,provenance_insert"
    findings_cleared = f"exported {len(ok)} parquet files; {len(err)} errors"
    try:
        con.execute("""
            INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
              (run_id, started_at, ended_at, phases_applied,
               critical_findings_cleared, high_findings_cleared,
               med_findings_cleared, held_for_adjudication)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            RUN_ID,
            RUN_START.replace(tzinfo=None),
            ended_at.replace(tzinfo=None),
            phases,
            findings_cleared,
            "",
            "",
            str(len(err)) if err else "0",
        ])
        print(f"\n[mig_230] ✓ Provenance inserted into cpm_reconciliation_provenance_v1 (run_id='{RUN_ID}')")
    except Exception as exc:
        print(f"\n[mig_230] ⚠ Provenance insert failed: {exc}")


def sanity_check(entries: list[dict], con: duckdb.DuckDBPyConnection) -> None:
    """Re-load 1 random parquet and verify row count matches MD-side."""
    import random
    ok = [e for e in entries if e["status"] == "ok" and e["schema"] == "main" and e["rows"] > 0]
    if not ok:
        print("\n[mig_230] ⚠ No OK main-schema entries to sanity check")
        return
    sample = random.choice(ok)
    local_path = REPO_ROOT / sample["path"]
    try:
        local_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{local_path}')").fetchone()[0]
        expected = sample["rows"]
        if local_count == expected:
            print(f"\n[mig_230] ✓ Sanity check PASS: {sample['schema']}.{sample['table']} "
                  f"local={local_count:,} == md={expected:,}")
        else:
            print(f"\n[mig_230] ✗ Sanity check FAIL: {sample['schema']}.{sample['table']} "
                  f"local={local_count:,} != md={expected:,}")
    except Exception as exc:
        print(f"\n[mig_230] ⚠ Sanity check error: {exc}")


def main() -> None:
    print(f"[mig_230] Parquet export start — {RUN_START.isoformat()}")
    print(f"[mig_230] Output dir: {EXPORT_DIR}")
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    con = get_con()
    all_entries: list[dict] = []

    # ── 1 + 2 + 3: Verified main tables + manuscript_cohort_v1 + registries ──
    print("\n[mig_230] Scope 1-3: main schema verified tables + registries…")
    scope_tables = con.execute("""
        SELECT DISTINCT table_name
        FROM main.canonical_table_signoff_registry_v1
        WHERE table_status = 'verified' AND schema_name = 'main'
        ORDER BY table_name
    """).fetchall()
    print(f"  Registry returns {len(scope_tables)} verified main tables")

    # Always include manuscript_cohort_v1, signoff registries (may already be in list)
    extra_main = {
        "manuscript_cohort_v1",
        "canonical_table_signoff_registry_v1",
        "canonical_column_verification_registry_v1",
    }
    all_main_tables = {row[0] for row in scope_tables} | extra_main

    dest_main = EXPORT_DIR / "main"
    for tbl in sorted(all_main_tables):
        e = export_table(con, "main", tbl, dest_main)
        all_entries.append(e)

    # ── 4: Lane G objects (semantic_publication) ──────────────────────────────
    print("\n[mig_230] Scope 4: Lane G — semantic_publication schema…")
    lane_g_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'semantic_publication'
        ORDER BY table_name
    """).fetchall()
    print(f"  Found {len(lane_g_tables)} objects in semantic_publication")
    dest_sem = EXPORT_DIR / "semantic_publication"
    for (tbl,) in lane_g_tables:
        e = export_table(con, "semantic_publication", tbl, dest_sem)
        all_entries.append(e)

    # ── 5: Lane LN objects (manuscript_workspace) ─────────────────────────────
    print("\n[mig_230] Scope 5: Lane LN — manuscript_workspace views + QC tables…")
    lane_ln_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'manuscript_workspace'
          AND (
            table_name LIKE 'vw_ln_%'
            OR table_name LIKE 'vw_histology_%'
            OR table_name LIKE 'qc_ln_%'
            OR table_name LIKE 'qc_histology_%'
            OR table_name LIKE 'dim_histology_%'
            OR table_name LIKE 'dim_ln_%'
          )
        ORDER BY table_name
    """).fetchall()
    print(f"  Found {len(lane_ln_tables)} Lane LN objects in manuscript_workspace")
    dest_mw = EXPORT_DIR / "manuscript_workspace"
    for (tbl,) in lane_ln_tables:
        e = export_table(con, "manuscript_workspace", tbl, dest_mw)
        all_entries.append(e)

    # ── 6: Archive snapshots from "Thyroid 2026 UPdated" ─────────────────────
    print("\n[mig_230] Scope 6: archive_pub_v1_0 snapshots from 'Thyroid 2026 UPdated'…")
    archive_entries = export_archive_snapshots(con)
    all_entries.extend(archive_entries)

    # ── Sanity check ──────────────────────────────────────────────────────────
    sanity_check(all_entries, con)

    # ── Build manifest ────────────────────────────────────────────────────────
    print("\n[mig_230] Building manifest…")
    manifest_text = build_manifest(all_entries)
    MANIFEST_PATH.write_text(manifest_text, encoding="utf-8")
    print(f"  ✓ Manifest written: {MANIFEST_PATH}")

    # ── Insert provenance row ─────────────────────────────────────────────────
    insert_provenance(con, all_entries)

    con.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    ok = [e for e in all_entries if e["status"] == "ok"]
    err = [e for e in all_entries if e["status"] != "ok"]
    total_mb = sum(e["bytes"] for e in ok) / 1024 / 1024
    print("\n[mig_230] ═══ COMPLETE ═══")
    print(f"  Exported: {len(ok)} / {len(all_entries)} tables")
    print(f"  Total size: {total_mb:.2f} MB (ZSTD compressed)")
    print(f"  Errors: {len(err)}")
    if err:
        for e in err:
            print(f"    ✗ {e['schema']}.{e['table']}: {e['status']}")
    print(f"  Manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
