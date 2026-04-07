#!/usr/bin/env python3
"""Load episode/linkage/analysis contract tables into MotherDuck main schema.

Reads validated parquets from exports/manuscript_freeze_v1/data/ and materializes
them as tables in main, then creates contract views from the companion DDL file.
Also creates the longitudinal_lab_deduped_v consumption view.

Usage:
  .venv/bin/python scripts/117_md_contract_views.py --md
  .venv/bin/python scripts/117_md_contract_views.py --md --dry-run
  .venv/bin/python scripts/117_md_contract_views.py --db-path thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
FREEZE_DIR = ROOT / "exports" / "manuscript_freeze_v1" / "data"
DDL_PATH = ROOT / "scripts" / "sql" / "117_contract_views_ddl.sql"

EPISODE_TABLES = {
    "tumor_episode_master_v2": "tumor_episode_master_v2.parquet",
    "molecular_test_episode_v2": "molecular_test_episode_v2.parquet",
    "rai_treatment_episode_v2": "rai_treatment_episode_v2.parquet",
    "operative_episode_detail_v2": "operative_episode_detail_v2.parquet",
}

CANONICAL_TABLES = {
    "canonical_extracted_fact_long_v2": ROOT / "processed" / "canonical_extracted_fact_long_v2.parquet",
    "canonical_fact_quarantine_v2": ROOT / "processed" / "canonical_fact_quarantine_v2.parquet",
    "note_extraction_runs": ROOT / "processed" / "note_extraction_runs.parquet",
    "longitudinal_lab_canonical_v1": ROOT / "processed" / "longitudinal_lab_canonical_v1.parquet",
    "thyroglobulin_lab_canonical_v1": ROOT / "processed" / "thyroglobulin_lab_canonical_v1.parquet",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load episode/linkage contract tables into MotherDuck.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Local DuckDB path.")
    p.add_argument("--dry-run", action="store_true", help="Show what would be done.")
    p.add_argument("--skip-canonical", action="store_true",
                   help="Skip canonical fact tables (already loaded by 103).")
    return p.parse_args()


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file
        return connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    return duckdb.connect(args.db_path)


def load_table_from_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    pq_path: Path,
    dry_run: bool = False,
) -> int:
    """Load a single parquet file into main.<table_name>. Returns remote row count (or 0 if skipped)."""
    if not pq_path.exists():
        print(f"  [skip] {pq_path.name} not found")
        return 0

    local_count = len(pd.read_parquet(pq_path))

    if dry_run:
        print(f"  [dry-run] main.{table_name}: {local_count:,} rows from {pq_path.name}")
        return local_count

    con.execute(
        f"CREATE OR REPLACE TABLE main.{table_name} "
        f"AS SELECT * FROM read_parquet('{pq_path}')"
    )
    md_count = con.execute(
        f"SELECT COUNT(*) FROM main.{table_name}"
    ).fetchone()[0]

    status = "OK" if local_count == md_count else "MISMATCH"
    print(f"  [{status}] main.{table_name}: local={local_count:,}  remote={md_count:,}")
    if local_count != md_count:
        raise RuntimeError(
            f"row parity failed for main.{table_name}: "
            f"local={local_count:,} remote={md_count:,}"
        )
    return int(md_count)


def apply_ddl(
    con: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
    *,
    on_error: str = "raise",
) -> None:
    """Execute the companion DDL file for contract views.

    on_error
        "raise" — propagate failures.
        "warn" — log statement failures and continue.
    """
    if not DDL_PATH.exists():
        print(f"  [warn] DDL file not found: {DDL_PATH}")
        return

    ddl = DDL_PATH.read_text(encoding="utf-8")
    stmts = []
    for line in ddl.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        stmts.append(line)

    sql_text = "\n".join(stmts)
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        if dry_run:
            label = stmt[:80].replace("\n", " ")
            print(f"  [dry-run] {label}...")
            continue
        try:
            con.execute(stmt)
        except Exception as exc:
            if on_error == "raise":
                raise
            print(f"  [warn] DDL failed: {exc}")
            print(f"         Statement: {stmt[:120]}...")


def _run_contract_writes_in_transaction(
    con: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
    *,
    inject_after_n_table_loads: int | None = None,
) -> None:
    """Commit base tables under one transaction; DDL/views afterward (warn-and-continue)."""
    tables_loaded = 0

    def _bump_after_load() -> None:
        nonlocal tables_loaded
        tables_loaded += 1
        if inject_after_n_table_loads is not None and (
            tables_loaded == inject_after_n_table_loads
        ):
            raise RuntimeError("__TEST_INJECT_AFTER_PARTIAL_LOAD__")

    con.execute("BEGIN TRANSACTION")
    try:
        print("\n  [txn] BEGIN (main table loads + parquet parity only)")

        print("=== Phase 2: Canonical table materialization ===")
        if not args.skip_canonical:
            for table_name, pq_path in CANONICAL_TABLES.items():
                load_table_from_parquet(con, table_name, pq_path, dry_run=False)
                _bump_after_load()
        else:
            print("  [skip] canonical tables (--skip-canonical)")

        print("\n=== Phase 3: Episode/linkage contract tables ===")
        for table_name, pq_filename in EPISODE_TABLES.items():
            pq_path = FREEZE_DIR / pq_filename
            load_table_from_parquet(con, table_name, pq_path, dry_run=False)
            _bump_after_load()

        written_tables: list[tuple[str, Path]] = []
        if not args.skip_canonical:
            written_tables.extend(CANONICAL_TABLES.items())
        for table_name, pq_filename in EPISODE_TABLES.items():
            written_tables.append((table_name, FREEZE_DIR / pq_filename))

        print("\n=== Post-write row parity (parquet vs main, before COMMIT) ===")
        for table_name, pq_path in written_tables:
            if not pq_path.exists():
                continue
            local_count = len(pd.read_parquet(pq_path))
            md_count = con.execute(
                f"SELECT COUNT(*) FROM main.{table_name}"
            ).fetchone()[0]
            if local_count != md_count:
                raise RuntimeError(
                    f"post-write parity failed for main.{table_name}: "
                    f"local={local_count:,} remote={md_count:,}"
                )
            print(f"  [parity OK] main.{table_name}: {md_count:,} rows")

        con.execute("COMMIT")
        print(
            "  [txn] COMMIT: main table loads persisted "
            "(views/DDL afterward; failures there do not roll back tables)."
        )
    except SystemExit:
        raise
    except BaseException as exc:
        try:
            con.execute("ROLLBACK")
            print(f"  [txn] ROLLBACK after error: {exc}")
        except Exception as rb_exc:
            print(f"  [txn] ROLLBACK failed (connection state unclear): {rb_exc}")
        raise

    print("\n=== Contract views (DDL) ===")
    apply_ddl(con, dry_run=False, on_error="warn")
    print("  [ddl] contract views pass finished (review warnings for any partial failures)")

    print("\n=== Verification (views) ===")
    try:
        for view_name in [
            "longitudinal_lab_deduped_v",
            "linkage_summary_v",
            "episode_completeness_summary_v",
        ]:
            cnt = con.execute(
                f"SELECT COUNT(*) FROM main.{view_name}"
            ).fetchone()[0]
            print(f"  [verify] main.{view_name}: {cnt:,} rows")
    except Exception as exc:
        print(f"  [warn] view verification failed: {exc}")


def main() -> None:
    args = parse_args()
    con = get_connection(args)

    try:
        if args.dry_run:
            print("=== Phase 2: Canonical table materialization ===")
            if not args.skip_canonical:
                for table_name, pq_path in CANONICAL_TABLES.items():
                    load_table_from_parquet(con, table_name, pq_path, args.dry_run)
            else:
                print("  [skip] canonical tables (--skip-canonical)")

            print("\n=== Phase 3: Episode/linkage contract tables ===")
            for table_name, pq_filename in EPISODE_TABLES.items():
                pq_path = FREEZE_DIR / pq_filename
                load_table_from_parquet(con, table_name, pq_path, args.dry_run)

            print("\n=== Contract views (DDL) ===")
            apply_ddl(con, args.dry_run)
        else:
            _run_contract_writes_in_transaction(
                con, args, inject_after_n_table_loads=None
            )
    finally:
        con.close()

    print("\n  [done] contract views setup complete")


if __name__ == "__main__":
    main()
