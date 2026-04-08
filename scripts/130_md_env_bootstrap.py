#!/usr/bin/env python3
"""MotherDuck environment bootstrap: snapshots, zero-copy clones, dev/qa wiring.

Uses the same token resolution as ``motherduck_client`` (env vars and
``.streamlit/secrets.toml``). **Mutations require ``--execute``**; otherwise only
SQL is printed (dry-run).

Official MotherDuck SQL references:
  - https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-snapshot
  - https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database

DuckLake constraint: production ``Thyroid 2026`` is type ``DUCKLAKE`` — named
``CREATE SNAPSHOT`` and snapshot selectors on ``CREATE DATABASE ... FROM`` are
**not** supported for DuckLake sources; use a bare zero-copy clone from latest.

Usage (place global flags like ``--execute``, ``--md-sa``, and ``--date-tag`` **before** the subcommand)::

  .venv/bin/python scripts/130_md_env_bootstrap.py [--md-sa] inspect
  .venv/bin/python scripts/130_md_env_bootstrap.py snapshot --name pre_schema_20260407
  .venv/bin/python scripts/130_md_env_bootstrap.py clone --dev --qa
  .venv/bin/python scripts/130_md_env_bootstrap.py --execute clone --dev [--md-sa]
  .venv/bin/python scripts/130_md_env_bootstrap.py --execute refresh-dev --latest
  .venv/bin/python scripts/130_md_env_bootstrap.py validate --database \"Thyroid 2026 Molecular Dev 20260407\"
  .venv/bin/python scripts/130_md_env_bootstrap.py prepromote-backup --label 20260407_1530
  .venv/bin/python scripts/130_md_env_bootstrap.py --execute prepromote-backup --label release_20260409 --md-sa
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import get_token  # noqa: E402
from urllib.parse import quote_plus  # noqa: E402


DEFAULT_PROD = "Thyroid 2026"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def utc_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def dev_database_name(tag: str) -> str:
    return f"Thyroid 2026 Molecular Dev {tag}"


def qa_database_name(tag: str) -> str:
    return f"Thyroid 2026 Molecular QA {tag}"


def prepromote_database_name(label: str) -> str:
    """Deterministic long-lived rollback clone name (label must be filesystem-safe)."""
    safe = "".join(c for c in label if c.isalnum() or c in " _-").strip()
    if not safe:
        raise ValueError("prepromote --label must contain at least one alphanumeric character")
    return f"Thyroid 2026 Molecular PrePromote {safe}"


def connect_rw(*, prefer_service_account: bool) -> duckdb.DuckDBPyConnection:
    token = get_token(prefer_service_account=prefer_service_account)
    if not token:
        raise SystemExit(
            "No MotherDuck token. Set MOTHERDUCK_TOKEN or MD_SA_TOKEN "
            "(or .streamlit/secrets.toml)."
        )
    qs = f"motherduck_token={quote_plus(token)}"
    ua = os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_md_bootstrap/1.0")
    if ua:
        qs = f"{qs}&custom_user_agent={quote_plus(ua)}"
    con = duckdb.connect(f"md:?{qs}")
    hint = (os.getenv("MOTHERDUCK_SESSION_HINT") or "").strip()
    if hint:
        safe = hint.replace("'", "''")
        try:
            con.execute(f"SET motherduck_session_hint='{safe}'")
        except Exception:
            pass
    return con


def use_db(con: duckdb.DuckDBPyConnection, database: str) -> None:
    con.execute(f"USE {quote_ident(database)}")


@dataclass(frozen=True)
class CatalogInfo:
    name: str
    db_type: str | None

    @property
    def is_ducklake(self) -> bool:
        return (self.db_type or "").upper() == "DUCKLAKE"


def fetch_prod_catalog_info(con: duckdb.DuckDBPyConnection, prod: str) -> CatalogInfo | None:
    try:
        row = con.execute(
            "SELECT name, type FROM MD_INFORMATION_SCHEMA.DATABASES WHERE name = ?",
            [prod],
        ).fetchone()
        if not row:
            return None
        return CatalogInfo(str(row[0]), str(row[1]) if row[1] is not None else None)
    except Exception:
        return None


def sql_create_named_snapshot(snapshot_name: str, prod: str) -> str:
    return f"CREATE SNAPSHOT {quote_ident(snapshot_name)} OF {quote_ident(prod)};"


def sql_clone_latest(new_db: str, source: str, *, or_replace: bool) -> str:
    OR = "OR REPLACE " if or_replace else ""
    return f"CREATE {OR}DATABASE {quote_ident(new_db)} FROM {quote_ident(source)};"


def sql_clone_snapshot_id(new_db: str, source: str, snapshot_id: str, *, or_replace: bool) -> str:
    sid = snapshot_id.replace("'", "''")
    OR = "OR REPLACE " if or_replace else ""
    return (
        f"CREATE {OR}DATABASE {quote_ident(new_db)} FROM {quote_ident(source)} "
        f"(SNAPSHOT_ID '{sid}');"
    )


def sql_clone_snapshot_name(new_db: str, source: str, snapshot_name: str, *, or_replace: bool) -> str:
    sn = snapshot_name.replace("'", "''")
    OR = "OR REPLACE " if or_replace else ""
    return (
        f"CREATE {OR}DATABASE {quote_ident(new_db)} FROM {quote_ident(source)} "
        f"(SNAPSHOT_NAME '{sn}');"
    )


def sql_drop_database(db: str) -> str:
    return f"DROP DATABASE IF EXISTS {quote_ident(db)};"


def run_sql(con: duckdb.DuckDBPyConnection, sql: str, *, execute: bool) -> None:
    print(sql)
    if execute:
        con.execute(sql)


def cmd_inspect(con: duckdb.DuckDBPyConnection, prod: str) -> None:
    use_db(con, prod)
    print("-- session")
    print(con.execute("SELECT current_database(), current_timestamp").fetchdf().to_string(index=False))
    print()
    print("-- MD_INFORMATION_SCHEMA.DATABASES (Thyroid*)")
    df = con.execute(
        "SELECT name, type, created_ts FROM MD_INFORMATION_SCHEMA.DATABASES "
        "WHERE name ILIKE '%thyroid%' ORDER BY name"
    ).fetchdf()
    print(df.to_string(index=False))
    print()
    info = fetch_prod_catalog_info(con, prod)
    if info:
        print(f"-- catalog '{prod}' type={info.db_type!r} ducklake={info.is_ducklake}")
    print()
    print("-- recent DATABASE_SNAPSHOTS (prod)")
    df2 = con.execute(
        "SELECT database_name, snapshot_id, snapshot_name, created_ts "
        "FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS "
        "WHERE database_name = ? "
        "ORDER BY created_ts DESC LIMIT 15",
        [prod],
    ).fetchdf()
    print(df2.to_string(index=False))


def cmd_snapshot(
    con: duckdb.DuckDBPyConnection,
    prod: str,
    name: str,
    *,
    execute: bool,
    force: bool,
) -> None:
    info = fetch_prod_catalog_info(con, prod)
    if info and info.is_ducklake and not force:
        print(
            f"[skip] {prod!r} is DUCKLAKE — named CREATE SNAPSHOT is not supported "
            f"(use zero-copy clone from latest; automatic history still listed in "
            f"MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS). Re-run with --force-native-snapshot "
            f"to attempt anyway."
        )
        sql = sql_create_named_snapshot(name, prod)
        print("-- would run (dry-run unless --execute):")
        print(sql)
        return
    sql = sql_create_named_snapshot(name, prod)
    run_sql(con, sql, execute=execute)
    if execute:
        print("[ok] snapshot DDL applied")


def cmd_clone(
    con: duckdb.DuckDBPyConnection,
    prod: str,
    tag: str,
    *,
    dev: bool,
    qa: bool,
    execute: bool,
    snapshot_id: str | None,
    snapshot_name: str | None,
) -> None:
    info = fetch_prod_catalog_info(con, prod)
    if snapshot_id or snapshot_name:
        if info and info.is_ducklake:
            raise SystemExit(
                "Snapshot selectors (SNAPSHOT_ID / SNAPSHOT_NAME) are not supported when "
                "cloning from a DUCKLAKE source. Omit --snapshot-id / --snapshot-name "
                "and use a latest-state zero-copy clone."
            )
    targets: list[tuple[str, str]] = []
    if dev:
        targets.append(("dev", dev_database_name(tag)))
    if qa:
        targets.append(("qa", qa_database_name(tag)))
    if not targets:
        raise SystemExit("Pass --dev and/or --qa")

    or_rep = execute  # idempotent application: OR REPLACE only when actually running
    for env_label, dbname in targets:
        print(f"-- {env_label} -> {dbname!r}")
        if snapshot_id:
            sql = sql_clone_snapshot_id(dbname, prod, snapshot_id, or_replace=or_rep)
        elif snapshot_name:
            sql = sql_clone_snapshot_name(dbname, prod, snapshot_name, or_replace=or_rep)
        else:
            sql = sql_clone_latest(dbname, prod, or_replace=or_rep)
        run_sql(con, sql, execute=execute)
        print()

    print("Suggested config/motherduck_environments.yml mapping:")
    print("  prod:", quote_ident(prod))
    if dev:
        print("  dev: ", quote_ident(dev_database_name(tag)))
    if qa:
        print("  qa:  ", quote_ident(qa_database_name(tag)))


def cmd_refresh_dev(
    con: duckdb.DuckDBPyConnection,
    prod: str,
    tag: str,
    *,
    execute: bool,
    latest: bool,
    snapshot_id: str | None,
    snapshot_name: str | None,
) -> None:
    dev = dev_database_name(tag)
    info = fetch_prod_catalog_info(con, prod)
    if snapshot_id or snapshot_name:
        if info and info.is_ducklake:
            raise SystemExit(
                "Cannot refresh from SNAPSHOT_ID / SNAPSHOT_NAME for DUCKLAKE prod. "
                "Use --latest (replace clone from current prod)."
            )
    if latest:
        print(sql_drop_database(dev))
        print(sql_clone_latest(dev, prod, or_replace=False))
        if execute:
            con.execute(sql_drop_database(dev))
            con.execute(sql_clone_latest(dev, prod, or_replace=False))
            print("[ok] dev DB replaced from latest prod")
        return
    if snapshot_id:
        print(sql_drop_database(dev))
        print(sql_clone_snapshot_id(dev, prod, snapshot_id, or_replace=False))
        if execute:
            con.execute(sql_drop_database(dev))
            con.execute(sql_clone_snapshot_id(dev, prod, snapshot_id, or_replace=False))
            print("[ok] dev DB replaced from snapshot_id")
        return
    if snapshot_name:
        print(sql_drop_database(dev))
        print(sql_clone_snapshot_name(dev, prod, snapshot_name, or_replace=False))
        if execute:
            con.execute(sql_drop_database(dev))
            con.execute(sql_clone_snapshot_name(dev, prod, snapshot_name, or_replace=False))
            print("[ok] dev DB replaced from SNAPSHOT_NAME")
        return
    raise SystemExit("Pass --latest or --snapshot-id or --snapshot-name")


def cmd_validate(con: duckdb.DuckDBPyConnection, database: str) -> None:
    use_db(con, database)
    cur = con.execute("SELECT current_database()").fetchone()
    n_main = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = 'main'"
    ).fetchone()
    cur_db = cur[0] if cur else "?"
    main_cnt = n_main[0] if n_main else "?"
    print(f"current_database={cur_db!r} main_table_count={main_cnt}")


def cmd_prepromote_backup(
    con: duckdb.DuckDBPyConnection,
    prod: str,
    label: str,
    *,
    execute: bool,
) -> str:
    """Create (or replace) a zero-copy clone of prod for rollback / audit."""
    db = prepromote_database_name(label)
    or_rep = execute
    sql = sql_clone_latest(db, prod, or_replace=or_rep)
    print(f"-- prepromote rollback handle -> {db!r}")
    print(sql)
    if execute:
        con.execute(sql)
        print("[ok] prepromote backup catalog created")
    print(
        "\nRollback: swap traffic to this database, or recreate prod from it; "
        "see docs/release_runbook.md (Rollback procedure)."
    )
    return db


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Global flags (--execute, --md-sa, --prod-database, --date-tag) must appear "
            "**before** the subcommand, e.g.  "
            "`python scripts/130_md_env_bootstrap.py --execute clone --dev --qa`."
        ),
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Run DDL (snapshot, clone, refresh). Without this flag, only print SQL.",
    )
    p.add_argument(
        "--md-sa",
        action="store_true",
        help="Automation flag for subprocesses; RW token order is fixed (MD_SA_TOKEN wins when set).",
    )
    p.add_argument("--prod-database", default=DEFAULT_PROD, help="Production catalog name (default: Thyroid 2026).")
    p.add_argument(
        "--date-tag",
        default=None,
        metavar="YYYYMMDD",
        help="Suffix for dev/qa database names (default: today UTC).",
    )

    sub = p.add_subparsers(dest="command", required=True)

    s_ins = sub.add_parser("inspect", help="List catalog type, databases, recent snapshots.")
    s_ins.set_defaults(_handler="inspect")

    s_snap = sub.add_parser("snapshot", help="Named CREATE SNAPSHOT (native only; blocked for DUCKLAKE unless forced).")
    s_snap.add_argument("--name", required=True, metavar="NAME", help="Snapshot name, e.g. pre_schema_20260407")
    s_snap.add_argument(
        "--force-native-snapshot",
        action="store_true",
        help="Attempt CREATE SNAPSHOT even when prod is DUCKLAKE (expected to fail).",
    )
    s_snap.set_defaults(_handler="snapshot")

    s_clone = sub.add_parser("clone", help="Zero-copy CREATE DATABASE ... FROM prod (latest or snapshot selectors if native).")
    s_clone.add_argument("--dev", action="store_true")
    s_clone.add_argument("--qa", action="store_true")
    s_clone.add_argument("--snapshot-id", default=None, metavar="UUID")
    s_clone.add_argument("--snapshot-name", default=None, metavar="NAME")
    s_clone.set_defaults(_handler="clone")

    s_ref = sub.add_parser("refresh-dev", help="DROP + re-clone dev database from prod.")
    s_ref.add_argument("--latest", action="store_true", help="Clone from current prod (DuckLake-safe).")
    s_ref.add_argument("--snapshot-id", default=None)
    s_ref.add_argument("--snapshot-name", default=None)
    s_ref.set_defaults(_handler="refresh_dev")

    s_val = sub.add_parser("validate", help="USE database and count main tables.")
    s_val.add_argument("--database", required=True, help="MotherDuck database name to validate.")
    s_val.set_defaults(_handler="validate")

    s_pre = sub.add_parser(
        "prepromote-backup",
        help="CREATE [OR REPLACE] DATABASE … FROM prod — DuckLake-safe rollback handle before promotion.",
    )
    s_pre.add_argument(
        "--label",
        required=True,
        metavar="LABEL",
        help="Unique suffix e.g. 20260407_1530 or release_20260409 (alphanumeric, space, _, -).",
    )
    s_pre.set_defaults(_handler="prepromote_backup")

    s_print = sub.add_parser("print-env", help="Print suggested dev/qa names for a date tag (no connection).")
    s_print.set_defaults(_handler="print_env")

    return p


def main() -> int:
    args = build_parser().parse_args()
    tag = args.date_tag or utc_yyyymmdd()
    prod: str = args.prod_database

    if args.command == "print-env":
        print("prod:", quote_ident(prod))
        print("dev: ", quote_ident(dev_database_name(tag)))
        print("qa:  ", quote_ident(qa_database_name(tag)))
        return 0

    con = connect_rw(prefer_service_account=args.md_sa)
    try:
        if args._handler == "inspect":
            cmd_inspect(con, prod)
        elif args._handler == "snapshot":
            cmd_snapshot(
                con, prod, args.name, execute=args.execute, force=args.force_native_snapshot
            )
        elif args._handler == "clone":
            cmd_clone(
                con,
                prod,
                tag,
                dev=args.dev,
                qa=args.qa,
                execute=args.execute,
                snapshot_id=args.snapshot_id,
                snapshot_name=args.snapshot_name,
            )
        elif args._handler == "refresh_dev":
            cmd_refresh_dev(
                con,
                prod,
                tag,
                execute=args.execute,
                latest=args.latest,
                snapshot_id=args.snapshot_id,
                snapshot_name=args.snapshot_name,
            )
        elif args._handler == "validate":
            cmd_validate(con, args.database)
        elif args._handler == "prepromote_backup":
            cmd_prepromote_backup(con, prod, args.label, execute=args.execute)
        else:
            raise SystemExit("unknown command")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
