#!/usr/bin/env python3
"""Bulk-load v2 domain parquets into MotherDuck v2_stage with load_inventory tracking.

Creates (or refreshes) all v2 domain tables in the v2_stage schema and maintains
a load_inventory table that records row counts, timestamps, and provenance for
every load operation.  Verifies local-vs-remote row parity before proceeding.

Usage:
  .venv/bin/python scripts/116_md_stage_loader.py --md
  .venv/bin/python scripts/116_md_stage_loader.py --md --dry-run
  .venv/bin/python scripts/116_md_stage_loader.py --db-path thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry

DEFAULT_V2_DIR = ROOT / "processed" / "output" / "v2_parquets"
DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"

LOAD_INVENTORY_DDL = """\
CREATE TABLE IF NOT EXISTS v2_stage.load_inventory (
    load_id             INTEGER,
    domain_name         VARCHAR NOT NULL,
    parquet_stem        VARCHAR NOT NULL,
    tier                VARCHAR,
    qa_tier             VARCHAR,
    source_path         VARCHAR,
    local_row_count     BIGINT NOT NULL,
    md_row_count        BIGINT NOT NULL,
    row_match           BOOLEAN NOT NULL,
    loaded_at           TIMESTAMP NOT NULL DEFAULT current_timestamp,
    git_sha             VARCHAR,
    registry_version    VARCHAR
)
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk-load v2 parquets into MotherDuck v2_stage.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Local DuckDB path.")
    p.add_argument("--v2-parquets-dir", default=str(DEFAULT_V2_DIR), help="V2 parquet directory.")
    p.add_argument("--dry-run", action="store_true", help="Verify without writing.")
    p.add_argument("--include-v1", action="store_true", help="Also stage v1 domain parquets.")
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


def run_v2_stage_transaction(
    con: duckdb.DuckDBPyConnection,
    domains_to_load: list[tuple[str, str, str, str, Path]],
    sha: str,
    reg_ver: str,
    *,
    inject_after_n_domain_loads: int | None = None,
) -> None:
    """Bulk-load domains into v2_stage under a single transaction (COMMIT or ROLLBACK).

    inject_after_n_domain_loads
        When set, raises RuntimeError after exactly that many domains have been fully
        loaded (table created + load_inventory row inserted). Intended for transactional tests.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS v2_stage")
        print("  [schema] v2_stage ensured")

        con.execute(LOAD_INVENTORY_DDL)

        next_id_row = con.execute(
            "SELECT COALESCE(MAX(load_id), 0) + 1 FROM v2_stage.load_inventory"
        ).fetchone()
        next_id = next_id_row[0] if next_id_row else 1

        loaded = 0
        mismatches = []
        now = datetime.now(timezone.utc).isoformat()

        for name, stem, tier, qa_tier, pq_path in domains_to_load:
            local_count = len(pd.read_parquet(pq_path))
            con.execute(
                f"CREATE OR REPLACE TABLE v2_stage.{stem} "
                f"AS SELECT * FROM read_parquet('{pq_path}')"
            )
            md_count = con.execute(
                f"SELECT COUNT(*) FROM v2_stage.{stem}"
            ).fetchone()[0]
            match = local_count == md_count

            con.execute(
                "INSERT INTO v2_stage.load_inventory VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    next_id,
                    name,
                    stem,
                    tier,
                    qa_tier,
                    str(pq_path),
                    local_count,
                    md_count,
                    match,
                    now,
                    sha,
                    reg_ver,
                ],
            )
            next_id += 1

            status = "OK" if match else "MISMATCH"
            print(
                f"  [{status}] v2_stage.{stem}: local={local_count:,}  md={md_count:,}"
            )
            if not match:
                mismatches.append(stem)
            loaded += 1

            if inject_after_n_domain_loads is not None and (
                loaded == inject_after_n_domain_loads
            ):
                raise RuntimeError("__TEST_INJECT_AFTER_PARTIAL_LOAD__")

        print(f"\n  [summary] {loaded} tables loaded into v2_stage (within transaction)")
        print(f"  [summary] {len(mismatches)} row-count mismatches (immediate check)")

        if mismatches:
            con.execute("ROLLBACK")
            print("  [txn] ROLLBACK: row parity failed during load (MISMATCH rows above).")
            print(f"  [error] Mismatches in: {mismatches}")
            sys.exit(1)

        print("\n  [post-write] Re-verifying parquet vs v2_stage row counts before COMMIT…")
        post_failures: list[str] = []
        for name, stem, tier, qa_tier, pq_path in domains_to_load:
            local_count = len(pd.read_parquet(pq_path))
            md_count = con.execute(
                f"SELECT COUNT(*) FROM v2_stage.{stem}"
            ).fetchone()[0]
            if local_count != md_count:
                post_failures.append(
                    f"{stem}: local={local_count:,} md={md_count:,}"
                )
                print(
                    f"  [post-write FAIL] v2_stage.{stem}: "
                    f"local={local_count:,} md={md_count:,}"
                )

        if post_failures:
            con.execute("ROLLBACK")
            print(
                "  [txn] ROLLBACK: post-write row parity check failed "
                f"({len(post_failures)} table(s))."
            )
            sys.exit(1)

        con.execute("COMMIT")
        print("  [txn] COMMIT completed successfully (no rollback).")

        inv_count = con.execute(
            "SELECT COUNT(*) FROM v2_stage.load_inventory"
        ).fetchone()[0]
        print(f"  [inventory] v2_stage.load_inventory: {inv_count:,} total rows")
    except SystemExit:
        raise
    except BaseException as exc:
        try:
            con.execute("ROLLBACK")
            print(f"  [txn] ROLLBACK after unexpected error: {exc}")
        except Exception as rb_exc:
            print(f"  [txn] ROLLBACK failed (connection state unclear): {rb_exc}")
        raise


def main() -> None:
    args = parse_args()
    registry = load_registry()
    v2_dir = Path(args.v2_parquets_dir)
    sha = git_sha()
    reg_ver = registry.schema_version

    if not v2_dir.is_dir():
        print(f"  [error] v2 parquets directory not found: {v2_dir}")
        sys.exit(1)

    domains_to_load: list[tuple[str, str, str, str, Path]] = []

    for name, spec in registry.v2_domains.items():
        if not spec.canonical_output:
            continue
        pq_path = v2_dir / f"{spec.parquet_stem}.parquet"
        if pq_path.exists():
            domains_to_load.append((name, spec.parquet_stem, spec.tier, spec.qa_tier, pq_path))
        else:
            print(f"  [warn] missing: {pq_path}")

    if args.include_v1:
        v1_dir = ROOT / "processed"
        for name, spec in registry.v1_domains.items():
            if not spec.canonical_output:
                continue
            pq_path = v1_dir / f"{spec.parquet_stem}.parquet"
            if pq_path.exists():
                domains_to_load.append((name, spec.parquet_stem, spec.tier, spec.qa_tier, pq_path))

    for sp_name, sp in registry.sub_prompt_domains.items():
        pq_path = v2_dir / f"{sp.parquet_stem}.parquet"
        if pq_path.exists():
            parent = registry.domains.get(sp.parent_domain)
            tier = parent.tier if parent else "v2"
            qa_tier = parent.qa_tier if parent else "standard"
            domains_to_load.append((sp_name, sp.parquet_stem, tier, qa_tier, pq_path))

    print(f"  [inventory] {len(domains_to_load)} parquets to load")

    if not domains_to_load:
        print("  [error] No parquets found to load.")
        sys.exit(1)

    con = get_connection(args)
    try:
        if args.dry_run:
            print("  [dry-run] Would execute:")
            print("    CREATE SCHEMA IF NOT EXISTS v2_stage")
            for name, stem, tier, qa_tier, pq_path in domains_to_load:
                local_count = len(pd.read_parquet(pq_path))
                print(f"    CREATE OR REPLACE TABLE v2_stage.{stem}  ({local_count:,} rows from {pq_path.name})")
            print(f"    {len(domains_to_load)} inventory rows into v2_stage.load_inventory")
            return

        run_v2_stage_transaction(
            con,
            domains_to_load,
            sha,
            reg_ver,
            inject_after_n_domain_loads=None,
        )

    finally:
        con.close()

    print("  [done] v2_stage loading complete")


if __name__ == "__main__":
    main()
