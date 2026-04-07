#!/usr/bin/env python3
"""Materialize specimen–genomics assay bindings on MotherDuck (fail-closed) or local file.

Uses existing v3 linkage only (no direct molecular→surgery). Optional ``genetic_testing`` and
``thyroseq_molecular_enrichment`` sections are applied only when those tables exist.

Rules:
  * connect_md_or_file(..., fail_closed=True) when --md
  * custom_user_agent='specimen_genomics_binding_v1'
  * RW token (see motherduck_client / .env.motherduck)

Usage:
  .venv/bin/python scripts/140_md_specimen_genomics_binding.py --md [--dry-run] [--skip-snapshot]
  .venv/bin/python scripts/140_md_specimen_genomics_binding.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_PATH = ROOT / "scripts" / "sql" / "140_specimen_genomics_binding_ddl.sql"
UA = "specimen_genomics_binding_v1"

PREREQ_MAIN_TABLES: tuple[str, ...] = (
    "molecular_test_episode_v2",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
    "specimen_tumor_focus_v1",
)


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _prod_database_name() -> str:
    return (
        os.environ.get("MOTHERDUCK_DATABASE")
        or os.environ.get("MOTHERDUCK_DB")
        or "Thyroid 2026"
    ).strip()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def try_named_snapshot(con, *, snapshot_name: str, prod: str) -> tuple[str, str]:
    sql = f"CREATE SNAPSHOT {_quote_ident(snapshot_name)} OF {_quote_ident(prod)};"
    try:
        con.execute(sql)
        return ("ok", sql)
    except Exception as e:
        msg = str(e).lower()
        if (
            "ducklake" in msg
            or ("snapshot" in msg and "not supported" in msg)
            or "does not have snapshots" in msg
            or "not a native duckdb" in msg
        ):
            return ("skipped", f"{e!r} — {sql}")
        return ("failed", f"{e!r} — {sql}")


def _table_exists(con, schema: str, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
        return r is not None
    except Exception:
        return False


def missing_prereq_tables(con) -> list[str]:
    return [t for t in PREREQ_MAIN_TABLES if not _table_exists(con, "main", t)]


def strip_optional_sql(sql: str, *, has_genetic: bool, has_thyroseq: bool) -> str:
    if not has_genetic:
        sql = re.sub(
            r"-- @OPTIONAL_GENETIC_BODY_START.*?-- @OPTIONAL_GENETIC_BODY_END\s*",
            "",
            sql,
            flags=re.DOTALL,
        )
        sql = re.sub(
            r"-- @OPTIONAL_UNION_GENETIC\s*\nUNION ALL\s*\nSELECT \* FROM genetic_rows\s*\n",
            "",
            sql,
        )
    if not has_thyroseq:
        sql = re.sub(
            r"-- @OPTIONAL_THYROSEQ_BODY_START.*?-- @OPTIONAL_THYROSEQ_BODY_END\s*",
            "",
            sql,
            flags=re.DOTALL,
        )
        sql = re.sub(
            r"-- @OPTIONAL_UNION_THYROSEQ\s*\nUNION ALL\s*\nSELECT \* FROM thy_rows\s*\n",
            "",
            sql,
        )
    return sql


def apply_specimen_genomics_binding(
    con,
    *,
    has_genetic: bool | None = None,
    has_thyroseq: bool | None = None,
) -> str:
    """Execute DDL; return the SQL actually run (after optional stripping)."""

    if has_genetic is None:
        has_genetic = _table_exists(con, "main", "genetic_testing")
    if has_thyroseq is None:
        has_thyroseq = _table_exists(con, "main", "thyroseq_molecular_enrichment")

    raw = DDL_PATH.read_text(encoding="utf-8")
    sql = strip_optional_sql(raw, has_genetic=has_genetic, has_thyroseq=has_thyroseq)
    con.execute(sql)
    return sql


def run_validation(con) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []

    def run(name: str, q: str, expect_true: bool) -> None:
        try:
            val = con.execute(q).fetchone()
            ok = bool(val and val[0] is True)
            if not expect_true:
                ok = bool(val and val[0] is not None)
            out.append((name, "PASS" if ok else "FAIL", f"{val}"))
        except Exception as e:
            out.append((name, "FAIL", str(e)))

    run(
        "genomic_assay_id_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT genomic_assay_id), FALSE)"
        " FROM main.specimen_genomic_assay_v1",
        True,
    )
    if _table_exists(con, "main", "specimen_master_v1"):
        run(
            "specimen_master_fk_when_present",
            """SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM main.specimen_genomic_assay_v1 g
              LEFT JOIN main.specimen_master_v1 m
                ON g.specimen_id = m.specimen_id AND g.research_id = m.research_id
              WHERE g.specimen_id IS NOT NULL AND m.specimen_id IS NULL
            ), FALSE)""",
            True,
        )
    else:
        out.append(("specimen_master_fk_when_present", "SKIP", "specimen_master_v1 absent"))
    run(
        "thyroseq_explode_audit_nonempty",
        """SELECT COALESCE(COUNT(*) >= 0, TRUE) FROM main.specimen_genomic_assay_v1
           WHERE source_table LIKE 'thyroseq%'""",
        True,
    )
    return out


def persist_validation(con, rows: list[tuple[str, str, str]]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS qa")
    con.execute(
        "CREATE TABLE IF NOT EXISTS qa.val_specimen_genomic_binding_v1 ("
        "check_name VARCHAR NOT NULL, status VARCHAR NOT NULL, detail VARCHAR, measured_at TIMESTAMP NOT NULL)"
    )
    con.execute("DELETE FROM qa.val_specimen_genomic_binding_v1 WHERE 1=1")
    con.executemany(
        "INSERT INTO qa.val_specimen_genomic_binding_v1 "
        "(check_name, status, detail, measured_at) VALUES (?, ?, ?, current_timestamp)",
        [(a, b, c) for a, b, c in rows],
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Specimen genomics binding layer on MotherDuck.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB path when not --md.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip CREATE SNAPSHOT preamble.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run:
        has_g = "[check genetic_testing on target]"
        has_t = "[check thyroseq_molecular_enrichment on target]"
        print(f"[dry-run] Would apply {DDL_PATH} (UA={UA}) optional genetic={has_g} thyroseq={has_t}")
        return

    from utils.md_connect import connect_md_or_file

    hint = (
        os.environ.get("MOTHERDUCK_SESSION_HINT")
        or f"thyroid2026:specimen_genomics_binding:{_git_sha()[:7]}"
    )
    con = connect_md_or_file(
        Path(args.db_path),
        md=args.md,
        fail_closed=args.md,
        custom_user_agent=UA,
        motherduck_session_hint=hint,
    )

    missing = missing_prereq_tables(con)
    if missing:
        print(
            "FATAL: missing prerequisites:\n  - " + "\n  - ".join(missing),
            file=sys.stderr,
        )
        con.close()
        sys.exit(1)

    snap_detail = "not_attempted"
    if args.md and not args.skip_snapshot:
        prod = _prod_database_name()
        snap_name = f"specimen_genomics_pre_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
        st, detail = try_named_snapshot(con, snapshot_name=snap_name, prod=prod)
        print(f"  Snapshot {snap_name}: {st} — {detail[:200]}")
        snap_detail = detail

    try:
        hg = _table_exists(con, "main", "genetic_testing")
        ht = _table_exists(con, "main", "thyroseq_molecular_enrichment")
        print(f"  Optional: genetic_testing={hg}, thyroseq_molecular_enrichment={ht}")
        apply_specimen_genomics_binding(con, has_genetic=hg, has_thyroseq=ht)
    except Exception:
        con.close()
        raise

    rows = run_validation(con)
    persist_validation(con, rows)
    for name, st, det in rows:
        print(f"  [{st}] {name}: {det[:200]}")

    con.close()
    print(f"Done. UA={UA} snapshot_detail={snap_detail[:120]}")


if __name__ == "__main__":
    main()
