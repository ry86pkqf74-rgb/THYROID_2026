#!/usr/bin/env python3
"""Apply governed molecular results layer DDL (normalized assay + variant tables).

Creates `molecular_results`, `molecular_variant_long`, `molecular_assay_dictionary`,
`molecular_code_crosswalk`, optional `molecular_ingestion_runs`, indexes, seed
crosswalk rows, and Streamlit-oriented contract views. Does **not** alter
`molecular_testing`, `thyroseq_*`, or other existing ThyroSeq tables.

Companion SQL: scripts/sql/131_molecular_results_layer_ddl.sql

Usage:
  .venv/bin/python scripts/131_molecular_results_layer.py --execute
  .venv/bin/python scripts/131_molecular_results_layer.py --execute --md
  .venv/bin/python scripts/131_molecular_results_layer.py --execute --md --md-sa
  .venv/bin/python scripts/131_molecular_results_layer.py --validate-only --execute --md

Token resolution matches ``motherduck_client`` / ``.streamlit/secrets.toml``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DDL_PATH = ROOT / "scripts" / "sql" / "131_molecular_results_layer_ddl.sql"
DEFAULT_LOCAL_DB = ROOT / os.getenv("LOCAL_DUCKDB_PATH", "thyroid_master.duckdb")

LAYER_TABLES = (
    "molecular_ingestion_runs",
    "molecular_assay_dictionary",
    "molecular_code_crosswalk",
    "molecular_results",
    "molecular_variant_long",
)

def _strip_full_line_comments(ddl: str) -> str:
    lines = []
    for line in ddl.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def split_sql_statements(sql_text: str) -> list[str]:
    stmts: list[str] = []
    for stmt in sql_text.split(";"):
        s = stmt.strip()
        if s:
            stmts.append(s)
    return stmts


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    from motherduck_client import resolve_database_for_env

    if args.md:
        from utils.md_connect import connect_md_or_file

        pref_sa = bool(getattr(args, "md_sa", False))
        override_db = (args.database or "").strip()
        if override_db:
            os.environ["MOTHERDUCK_DATABASE"] = override_db
        elif not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get("MOTHERDUCK_DB"):
            env_name = (args.md_env or os.getenv("MOTHERDUCK_ENV") or "prod").strip()
            os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(env_name)
        con = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=pref_sa,
            custom_user_agent=os.getenv(
                "MOTHERDUCK_CUSTOM_USER_AGENT",
                "THYROID_2026_scripts/131_molecular_results_layer",
            ),
        )
        return con

    dbp = Path(args.db_path)
    if not dbp.parent.exists():
        dbp.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Local DuckDB: {dbp}")
    return duckdb.connect(str(dbp))


def apply_ddl(con: duckdb.DuckDBPyConnection, *, dry_run: bool) -> None:
    if not DDL_PATH.is_file():
        raise SystemExit(f"DDL missing: {DDL_PATH}")
    raw = DDL_PATH.read_text(encoding="utf-8")
    body = _strip_full_line_comments(raw)
    statements = split_sql_statements(body)
    if dry_run:
        print(f"  [dry-run] {len(statements)} statement(s) from {DDL_PATH.name}")
        for i, st in enumerate(statements[:5], 1):
            print(f"    {i}. {st[:100].replace(chr(10), ' ')}...")
        if len(statements) > 5:
            print(f"    … +{len(statements) - 5} more")
        return
    for stmt in statements:
        con.execute(stmt)


def run_validation(con: duckdb.DuckDBPyConnection) -> None:
    print("\n── Row counts (main schema) ──")
    for tbl in LAYER_TABLES:
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?",
                [tbl],
            ).fetchone()[0]
            if n == 0:
                print(f"  {tbl}: (table missing)")
                continue
            c = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
            print(f"  main.{tbl}: {c:,}")
        except Exception as e:
            print(f"  main.{tbl}: ERROR {e}")

    print("\n── Preserved tables spot-check (this script does not alter them) ──")
    preserved = (
        "molecular_testing",
        "thyroseq_molecular_enrichment",
        "thyroseq_followup_labs",
        "thyroseq_followup_events",
        "thyroseq_fill_actions",
        "thyroseq_review_queue",
    )
    for tbl in preserved:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
            print(f"  main.{tbl}: {n:,} rows")
        except Exception:
            print(f"  main.{tbl}: (absent — ok if not loaded in this database)")

    print("\n── Contract views ──")
    for v in (
        "molecular_results_contract_v1",
        "molecular_variant_long_contract_v1",
        "molecular_results_enriched_v1",
        "molecular_normalization_review_v1",
    ):
        try:
            con.execute(f"SELECT 1 FROM main.{v} LIMIT 1")
            print(f"  main.{v}: OK (readable)")
        except Exception as e:
            print(f"  main.{v}: ERROR {e}")

    try:
        xw = con.execute(
            "SELECT domain, COUNT(*) FROM main.molecular_code_crosswalk GROUP BY 1 ORDER BY 1"
        ).fetchall()
        print("\n── molecular_code_crosswalk by domain ──")
        for d, k in xw:
            print(f"  {d}: {k:,} code(s)")
    except Exception as e:
        print(f"  crosswalk summary: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true", help="Run DDL (default is dry-run).")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (read/write token required).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN.")
    p.add_argument(
        "--md-env",
        default=None,
        help="MOTHERDUCK_ENV selector (dev|qa|prod) when MOTHERDUCK_DATABASE is unset.",
    )
    p.add_argument(
        "--database",
        default=None,
        help="Override MotherDuck database name (sets MOTHERDUCK_DATABASE for this process).",
    )
    p.add_argument(
        "--db-path",
        default=str(DEFAULT_LOCAL_DB),
        help="Local DuckDB path when not using --md.",
    )
    p.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip DDL; only run validation queries (use after --execute).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.execute

    if args.validate_only:
        con = connect(args)
        try:
            run_validation(con)
        finally:
            con.close()
        return

    print(f"Schema DDL: {DDL_PATH}")
    con = connect(args)
    try:
        apply_ddl(con, dry_run=dry_run)
        if not dry_run:
            print("  DDL applied.")
            run_validation(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
