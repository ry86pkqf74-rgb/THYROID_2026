#!/usr/bin/env python3
"""
132_molecular_fact_lineage_views.py — Apply unified molecular fact lineage views.

Installs views from scripts/sql/132_molecular_fact_lineage_views.sql:
  main.molecular_fact_long_base_v
  main.molecular_fact_long_v  (synonym consumer: main.molecular_results_unified_v)
  main.molecular_fact_lineage_qa_duplicate_candidates_v

Prerequisites:
  - canonical_extracted_fact_long_v2 (103_fact_lineage_materialize.py)
  - molecular_results / molecular_variant_long (131 DDL + 41/42 ingest)
  - qa.manual_review_queue (114_qa_schema_ddl.sql) for genetics review overlay

Usage:
  .venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute
  .venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute --md --md-env dev
  .venv/bin/python scripts/132_molecular_fact_lineage_views.py --validate-only --md
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DDL_PATH = ROOT / "scripts" / "sql" / "132_molecular_fact_lineage_views.sql"
DB_PATH = ROOT / "thyroid_master.duckdb"


def _strip_full_line_comments(ddl: str) -> str:
    lines: list[str] = []
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


def connect(args: argparse.Namespace):
    if args.md:
        from utils.md_connect import connect_md_or_file

        if (args.database or "").strip():
            os.environ["MOTHERDUCK_DATABASE"] = args.database.strip()
        elif not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get("MOTHERDUCK_DB"):
            from motherduck_client import resolve_database_for_env

            env_name = (args.md_env or os.getenv("MOTHERDUCK_ENV") or "prod").strip()
            os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(env_name)
        return connect_md_or_file(
            DB_PATH,
            md=True,
            fail_closed=True,
            prefer_service_account=bool(getattr(args, "md_sa", False)),
            env=args.md_env,
            custom_user_agent=os.getenv(
                "MOTHERDUCK_CUSTOM_USER_AGENT",
                "THYROID_2026_scripts/132_molecular_fact_lineage_views",
            ),
        )
    return __import__("duckdb").connect(str(DB_PATH))


def apply_ddl(con, *, dry_run: bool) -> None:
    if not DDL_PATH.is_file():
        raise SystemExit(f"Missing {DDL_PATH}")
    raw = DDL_PATH.read_text(encoding="utf-8")
    body = _strip_full_line_comments(raw)
    statements = split_sql_statements(body)
    if dry_run:
        print(f"  [dry-run] {len(statements)} statement(s) from {DDL_PATH.name}")
        return
    for stmt in statements:
        con.execute(stmt)
    print(f"  Applied {len(statements)} view DDL statement(s).")


def run_validation(con) -> None:
    print("\n── Molecular lineage view counts ──")
    for v in (
        "molecular_fact_long_base_v",
        "molecular_fact_long_v",
        "molecular_results_unified_v",
        "molecular_fact_lineage_qa_duplicate_candidates_v",
    ):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{v}").fetchone()[0]
            print(f"  main.{v}: {n:,}")
        except Exception as e:
            print(f"  main.{v}: ERROR {e}")

    print("\n── Primary vs supporting (precedence) ──")
    try:
        total = con.execute("SELECT COUNT(*) FROM main.molecular_fact_long_v").fetchone()[0]
        primary = con.execute(
            "SELECT COUNT(*) FROM main.molecular_fact_long_v WHERE included_in_primary_analytics"
        ).fetchone()[0]
        supporting = total - primary
        print(f"  Total unified rows: {total:,}")
        print(f"  included_in_primary_analytics TRUE:  {primary:,}")
        print(f"  included_in_primary_analytics FALSE: {supporting:,} (note rows superseded by assay)")
    except Exception as e:
        print(f"  ERROR {e}")

    print("\n── Duplicate assay-event candidates (note vs structured, ±21d) ──")
    try:
        d = con.execute(
            "SELECT COUNT(*) FROM main.molecular_fact_lineage_qa_duplicate_candidates_v"
        ).fetchone()[0]
        print(f"  Pair rows in QA view: {d:,}")
    except Exception as e:
        print(f"  ERROR {e}")

    print("\n── Sample rows: primary assay with note support ──")
    try:
        rows = con.execute(
            """
            SELECT research_id, entity_type, fact_provenance_category, record_role,
                   included_in_primary_analytics, event_date, source_stream,
                   substring(COALESCE(entity_value_raw, ''), 1, 120) AS excerpt
            FROM main.molecular_fact_long_v
            WHERE assay_has_note_support AND source_stream IN ('molecular_results', 'molecular_variant_long')
            ORDER BY research_id
            LIMIT 5
            """
        ).fetchdf()
        print(rows.to_string(index=False))
    except Exception as e:
        print(f"  ERROR {e}")

    print("\n── Sample rows: supporting note (suppressed from primary analytics) ──")
    try:
        rows = con.execute(
            """
            SELECT research_id, entity_type, fact_provenance_category, record_role,
                   included_in_primary_analytics, event_date, matched_molecular_result_id,
                   substring(COALESCE(entity_value_raw, ''), 1, 120) AS excerpt
            FROM main.molecular_fact_long_v
            WHERE source_stream = 'note_genetics' AND NOT included_in_primary_analytics
            ORDER BY research_id
            LIMIT 5
            """
        ).fetchdf()
        print(rows.to_string(index=False))
    except Exception as e:
        print(f"  ERROR {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true", help="Apply view DDL (default is dry-run).")
    p.add_argument("--md", action="store_true", help="Target MotherDuck.")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument("--md-env", default=None, help="dev|qa|prod when database not set.")
    p.add_argument("--database", default=None, help="Override MotherDuck database name.")
    p.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip DDL; run validation queries only.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.execute and not args.validate_only
    con = connect(args)
    try:
        if args.validate_only:
            run_validation(con)
            return
        apply_ddl(con, dry_run=dry_run)
        if not dry_run:
            run_validation(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
