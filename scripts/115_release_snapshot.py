#!/usr/bin/env python3
"""Create an immutable release snapshot schema in MotherDuck.

Copies canonical tables from main to a dated release_YYYYMMDD schema and
records the release in qa.release_manifest. Release schemas are read-only
after creation -- corrections require a new release tag.

Usage:
  .venv/bin/python scripts/115_release_snapshot.py --md --tag 20260410
  .venv/bin/python scripts/115_release_snapshot.py --md --tag 20260410 --dry-run
  .venv/bin/python scripts/115_release_snapshot.py --db-path thyroid_master.duckdb --tag 20260410
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CANONICAL_TABLES = (
    "canonical_extracted_fact_long_v1",
    "canonical_extracted_fact_long_v2",
    "canonical_fact_quarantine_v1",
    "canonical_fact_quarantine_v2",
    "thyroglobulin_lab_canonical_v1",
    "note_extraction_runs",
)

# Final manuscript master: core facts + labs + analyst presentation layer (views materialized as tables).
FINAL_MASTER_TABLES = CANONICAL_TABLES + (
    "longitudinal_lab_canonical_v1",
    "master_fact_long_verified_v1",
    "master_patient_rollup_verified_v1",
    "master_source_lineage_v1",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create immutable release snapshot schema.")
    p.add_argument("--tag", required=True, help="Release tag, e.g. 20260410")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Override table list (default: canonical tables).",
    )
    p.add_argument("--created-by", default="scripts/115_release_snapshot.py")
    p.add_argument(
        "--final-master",
        action="store_true",
        help="Snapshot FINAL_MASTER_TABLES (includes lab canonical + master_* verified views).",
    )
    return p.parse_args()


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def registry_version() -> str:
    try:
        from llm_extraction.registry import load_registry
        return load_registry().schema_version
    except Exception:
        return "unknown"


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file
        return connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    return duckdb.connect(args.db_path)


def main() -> None:
    args = parse_args()
    tag = args.tag.strip()
    schema_name = f"release_{tag}"
    if args.tables:
        tables = args.tables
    elif args.final_master:
        tables = list(FINAL_MASTER_TABLES)
    else:
        tables = list(CANONICAL_TABLES)
    sha = git_sha()
    reg_ver = registry_version()

    con = get_connection(args)
    try:
        try:
            existing = [r[0] for r in con.execute("SHOW SCHEMAS").fetchall()]
        except Exception:
            existing = [
                r[0] for r in con.execute(
                    "SELECT DISTINCT schema_name FROM information_schema.schemata"
                ).fetchall()
            ]
        if schema_name in existing:
            print(f"  [error] Schema {schema_name} already exists. Use a different tag.")
            sys.exit(1)

        if args.dry_run:
            print(f"  [dry-run] CREATE SCHEMA {schema_name}")
            for t in tables:
                print(
                    f"  [dry-run] CREATE TABLE {schema_name}.{t} AS "
                    f"SELECT *, '{tag}' AS release_tag FROM main.{t}"
                )
            print("  [dry-run] INSERT INTO qa.release_manifest ...")
            return

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"CREATE SCHEMA {schema_name}")
            print(f"  [schema] created {schema_name}")

            row_counts: dict[str, int] = {}
            for t in tables:
                con.execute(f"""
                    CREATE TABLE {schema_name}.{t} AS
                    SELECT *, '{tag}' AS release_tag
                    FROM main.{t}
                """)
                cnt = con.execute(f"SELECT COUNT(*) FROM {schema_name}.{t}").fetchone()[0]
                row_counts[t] = int(cnt)
                print(f"  [copy] {schema_name}.{t}: {cnt:,} rows")

            print("\n  [post-write] Row parity vs main (before COMMIT)…")
            for t in tables:
                main_cnt = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
                rel_cnt = con.execute(
                    f"SELECT COUNT(*) FROM {schema_name}.{t}"
                ).fetchone()[0]
                if int(main_cnt) != int(rel_cnt):
                    raise RuntimeError(
                        f"parity failed for {t}: main={main_cnt:,} "
                        f"{schema_name}={rel_cnt:,}"
                    )
                print(
                    f"  [parity OK] {schema_name}.{t}: {rel_cnt:,} rows "
                    f"(matches main.{t})"
                )

            con.execute(
                """
                INSERT INTO qa.release_manifest
                (release_tag, git_sha, registry_version, tables_included, row_counts, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    tag,
                    sha,
                    reg_ver,
                    json.dumps(list(row_counts.keys())),
                    json.dumps(row_counts),
                    datetime.now(timezone.utc).isoformat(),
                    args.created_by,
                ],
            )
            print("  [manifest] recorded in qa.release_manifest")

            con.execute("COMMIT")
            print("  [txn] COMMIT completed successfully (no rollback).")

            print(
                f"\n  Release {tag} created with {len(row_counts)} tables in {schema_name}"
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
    finally:
        con.close()


if __name__ == "__main__":
    main()
