#!/usr/bin/env python3
"""
Full MotherDuck Export — Phase 1: Export EVERYTHING
====================================================
Run this on your local Windows machine (Python 3.10+, duckdb installed).

What it does:
  1. Connects to md:thyroid_research_2026
  2. Inventories every table and view
  3. Exports each to Parquet (zstd compression) in a structured folder
  4. Dumps full schema DDL to schema.sql
  5. Writes a verification report (row counts, key field spot-checks)

Usage:
  set MOTHERDUCK_TOKEN=<your token>
  python scripts/export_motherduck_full.py

  Or pass the export path:
  python scripts/export_motherduck_full.py --output D:/Secure/Thyroid_Export_20260327
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb


# ── Config ────────────────────────────────────────────────────────────────────

DATABASE = "thyroid_research_2026"
DEFAULT_OUTPUT = Path("D:/Secure/Thyroid_Export_20260327")

# Key fields to spot-check per table (table_name -> list of columns)
SPOT_CHECK_FIELDS = {
    "master_cohort": ["research_id"],
    "canonical_episodes": ["research_id", "episode_id"],
    "nsqip_cases": ["research_id"],
    "tumor_pathology": ["research_id"],
    "molecular_testing": ["research_id"],
}


def get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        token = os.getenv("MD_SA_TOKEN")
    if not token:
        print("ERROR: Set MOTHERDUCK_TOKEN or MD_SA_TOKEN environment variable.")
        sys.exit(1)
    return token


def connect(token: str) -> duckdb.DuckDBPyConnection:
    print(f"Connecting to md:{DATABASE} ...")
    con = duckdb.connect(f"md:{DATABASE}?motherduck_token={token}")
    print("  Connected.\n")
    return con


def inventory(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return list of {name, type, row_count} for all tables and views."""
    rows = con.execute("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_type, table_name
    """).fetchall()

    objects = []
    for name, ttype in rows:
        try:
            count = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        except Exception as e:
            count = -1
            print(f"  WARNING: Could not count {name}: {e}")
        objects.append({"name": name, "type": ttype, "row_count": count})
    return objects


def export_schema_ddl(con: duckdb.DuckDBPyConnection, output_dir: Path) -> Path:
    """Export full DDL for all tables and views to schema.sql."""
    ddl_path = output_dir / "schema.sql"
    ddl_lines = [
        f"-- MotherDuck full schema export: {DATABASE}",
        f"-- Exported: {datetime.now().isoformat()}",
        "",
    ]

    # Tables
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()

    for (tname,) in tables:
        try:
            create_sql = con.execute(
                f"SELECT sql FROM duckdb_tables() WHERE table_name = '{tname}'"
            ).fetchone()
            if create_sql and create_sql[0]:
                ddl_lines.append(f"-- TABLE: {tname}")
                ddl_lines.append(create_sql[0] + ";")
                ddl_lines.append("")
            else:
                # Fallback: reconstruct from information_schema
                cols = con.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = '{tname}'
                    ORDER BY ordinal_position
                """).fetchall()
                col_defs = []
                for cname, dtype, nullable in cols:
                    null_str = "" if nullable == "YES" else " NOT NULL"
                    col_defs.append(f"    {cname} {dtype}{null_str}")
                ddl_lines.append(f"-- TABLE: {tname}")
                ddl_lines.append(f'CREATE TABLE "{tname}" (')
                ddl_lines.append(",\n".join(col_defs))
                ddl_lines.append(");")
                ddl_lines.append("")
        except Exception as e:
            ddl_lines.append(f"-- TABLE: {tname} (ERROR: {e})")
            ddl_lines.append("")

    # Views
    views = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'VIEW'
        ORDER BY table_name
    """).fetchall()

    for (vname,) in views:
        try:
            view_sql = con.execute(
                f"SELECT sql FROM duckdb_views() WHERE view_name = '{vname}'"
            ).fetchone()
            if view_sql and view_sql[0]:
                ddl_lines.append(f"-- VIEW: {vname}")
                ddl_lines.append(view_sql[0] + ";")
                ddl_lines.append("")
            else:
                ddl_lines.append(f"-- VIEW: {vname} (no DDL available)")
                ddl_lines.append("")
        except Exception as e:
            ddl_lines.append(f"-- VIEW: {vname} (ERROR: {e})")
            ddl_lines.append("")

    ddl_path.write_text("\n".join(ddl_lines), encoding="utf-8")
    print(f"  Schema DDL -> {ddl_path}")
    return ddl_path


def export_tables(
    con: duckdb.DuckDBPyConnection, objects: list[dict], output_dir: Path
) -> dict[str, dict]:
    """Export each table/view to Parquet. Returns {name: {path, rows, seconds}}."""
    tables_dir = output_dir / "tables"
    views_dir = output_dir / "views"
    tables_dir.mkdir(parents=True, exist_ok=True)
    views_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    for obj in objects:
        name = obj["name"]
        is_view = obj["type"] == "VIEW"
        dest_dir = views_dir if is_view else tables_dir
        parquet_path = dest_dir / f"{name}.parquet"

        print(f"  Exporting {name} ({obj['type']}, {obj['row_count']:,} rows) ...")
        t0 = time.time()
        try:
            con.execute(f"""
                COPY (SELECT * FROM "{name}")
                TO '{parquet_path.as_posix()}'
                (FORMAT PARQUET, COMPRESSION 'zstd')
            """)
            elapsed = time.time() - t0
            results[name] = {
                "path": str(parquet_path),
                "rows_expected": obj["row_count"],
                "seconds": round(elapsed, 2),
                "status": "OK",
            }
            print(f"    -> {parquet_path.name}  ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - t0
            results[name] = {
                "path": str(parquet_path),
                "rows_expected": obj["row_count"],
                "seconds": round(elapsed, 2),
                "status": f"ERROR: {e}",
            }
            print(f"    -> ERROR: {e}")

    return results


def verify_exports(output_dir: Path, export_results: dict) -> list[dict]:
    """Re-read each exported Parquet and compare row counts."""
    print("\n=== VERIFICATION ===")
    verification = []
    local_con = duckdb.connect()

    for name, info in export_results.items():
        if info["status"] != "OK":
            verification.append({
                "table": name,
                "expected": info["rows_expected"],
                "actual": None,
                "match": False,
                "note": info["status"],
            })
            continue

        try:
            actual = local_con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{info['path']}')"
            ).fetchone()[0]
            match = actual == info["rows_expected"]
            verification.append({
                "table": name,
                "expected": info["rows_expected"],
                "actual": actual,
                "match": match,
                "note": "OK" if match else f"MISMATCH (expected {info['rows_expected']}, got {actual})",
            })
            status = "OK" if match else "MISMATCH"
            print(f"  {name:50s} | expected {info['rows_expected']:>10,} | actual {actual:>10,} | {status}")
        except Exception as e:
            verification.append({
                "table": name,
                "expected": info["rows_expected"],
                "actual": None,
                "match": False,
                "note": f"READ ERROR: {e}",
            })
            print(f"  {name:50s} | READ ERROR: {e}")

    # Spot-check key fields
    print("\n=== SPOT CHECKS ===")
    for tname, fields in SPOT_CHECK_FIELDS.items():
        parquet_path = output_dir / "tables" / f"{tname}.parquet"
        if not parquet_path.exists():
            print(f"  {tname}: file not found, skipping spot-check")
            continue
        for field in fields:
            try:
                distinct = local_con.execute(
                    f"SELECT COUNT(DISTINCT \"{field}\") FROM read_parquet('{parquet_path.as_posix()}')"
                ).fetchone()[0]
                nulls = local_con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}') WHERE \"{field}\" IS NULL"
                ).fetchone()[0]
                print(f"  {tname}.{field}: {distinct:,} distinct, {nulls:,} nulls")
            except Exception as e:
                print(f"  {tname}.{field}: ERROR - {e}")

    local_con.close()
    return verification


def main():
    parser = argparse.ArgumentParser(description="Full MotherDuck database export")
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Export destination folder (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    output_dir: Path = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    token = get_token()
    con = connect(token)

    # 1. Inventory
    print("=== INVENTORY ===")
    objects = inventory(con)
    n_tables = sum(1 for o in objects if o["type"] == "BASE TABLE")
    n_views = sum(1 for o in objects if o["type"] == "VIEW")
    total_rows = sum(o["row_count"] for o in objects if o["row_count"] > 0)
    print(f"\n  Found {n_tables} tables, {n_views} views ({total_rows:,} total rows)\n")

    for obj in objects:
        label = "TABLE" if obj["type"] == "BASE TABLE" else "VIEW "
        print(f"    {label} | {obj['name']:50s} | {obj['row_count']:>10,} rows")
    print()

    # 2. Schema DDL
    print("=== SCHEMA DDL ===")
    export_schema_ddl(con, output_dir)
    print()

    # 3. Export all tables/views to Parquet
    print("=== EXPORTING TO PARQUET (zstd) ===")
    export_results = export_tables(con, objects, output_dir)
    print()

    # 4. Verify
    verification = verify_exports(output_dir, export_results)

    # 5. Write manifest
    manifest = {
        "database": DATABASE,
        "exported_at": datetime.now().isoformat(),
        "output_dir": str(output_dir),
        "tables_count": n_tables,
        "views_count": n_views,
        "total_rows": total_rows,
        "objects": objects,
        "export_results": export_results,
        "verification": verification,
    }
    manifest_path = output_dir / "export_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print(f"\n  Manifest -> {manifest_path}")

    # Summary
    ok_count = sum(1 for v in verification if v["match"])
    fail_count = len(verification) - ok_count
    print(f"\n{'='*60}")
    print("  EXPORT COMPLETE")
    print(f"  Output:       {output_dir}")
    print(f"  Tables:       {n_tables}")
    print(f"  Views:        {n_views}")
    print(f"  Verified OK:  {ok_count}")
    if fail_count:
        print(f"  FAILED:       {fail_count}  <-- CHECK THESE")
    print(f"{'='*60}")

    con.close()

    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
