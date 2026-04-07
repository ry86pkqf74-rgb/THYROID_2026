#!/usr/bin/env python3
"""Create a MotherDuck database snapshot (writer) or refresh read-scaling readers.

Writer (ETL / release operator, read/write token)::

  MOTHERDUCK_TOKEN=... python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod

Reader (dashboard / read-scaling token)::

  MD_READ_SCALING_TOKEN=... python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod

Or refresh every attached read-scaling DB::

  python scripts/136_md_read_scaling_snapshot_refresh.py reader --all

Uses the same credential resolution as other THYROID_2026 scripts (env + .streamlit/secrets.toml).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402
from utils.md_read_scaling_refresh import run_reader_refresh, run_writer_snapshot  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MotherDuck CREATE SNAPSHOT (writer) or REFRESH DATABASE (reader)."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("writer", help="CREATE SNAPSHOT OF <db> (requires read/write token)")
    w.add_argument(
        "--md-env",
        default=None,
        choices=["dev", "qa", "prod"],
        help="MotherDuck catalog (default: MOTHERDUCK_ENV or prod)",
    )
    w.add_argument(
        "--database",
        default=None,
        help="Override database name (else resolve_database_for_env)",
    )
    w.add_argument(
        "--snapshot-name",
        default=None,
        help="Optional named snapshot (default: unnamed CREATE SNAPSHOT OF db)",
    )
    w.add_argument(
        "--prefer-sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN",
    )
    w.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL only; do not connect",
    )

    r = sub.add_parser("reader", help="REFRESH DATABASE[S] (read-scaling token)")
    r.add_argument(
        "--md-env",
        default=None,
        choices=["dev", "qa", "prod"],
        help="MotherDuck catalog for connect_read_scaling",
    )
    r.add_argument(
        "--database",
        default=None,
        help="Database name for REFRESH DATABASE (not used with --all)",
    )
    r.add_argument(
        "--all",
        action="store_true",
        help="Run REFRESH DATABASES",
    )
    r.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL only",
    )
    return p.parse_args()


def _resolve_db(args: argparse.Namespace) -> str:
    if getattr(args, "database", None):
        return str(args.database).strip()
    from motherduck_client import resolve_database_for_env

    env = args.md_env or __import__("os").environ.get("MOTHERDUCK_ENV", "prod")
    return resolve_database_for_env(env)


def main() -> None:
    args = parse_args()
    import os

    if args.cmd == "writer":
        db = _resolve_db(args)
        from utils.md_read_scaling_refresh import sql_create_snapshot

        sql = sql_create_snapshot(db, snapshot_name=args.snapshot_name)
        if args.dry_run:
            print(sql)
            return
        client = MotherDuckClient.for_env(
            args.md_env or os.getenv("MOTHERDUCK_ENV", "prod"),
            use_service_account=args.prefer_sa,
        )
        con = client.connect_rw()
        try:
            run_writer_snapshot(con, db, snapshot_name=args.snapshot_name)
            print(f"OK writer: {sql}")
        finally:
            con.close()
        return

    # reader
    db_arg = getattr(args, "database", None)
    db = db_arg.strip() if db_arg else _resolve_db(args)
    from utils.md_read_scaling_refresh import sql_refresh_database

    sql = (
        sql_refresh_database(None, mode="all")
        if args.all
        else sql_refresh_database(db, mode="single")
    )
    if args.dry_run:
        print(sql)
        return
    md_env = args.md_env or os.getenv("MOTHERDUCK_ENV", "prod")
    client = MotherDuckClient.for_env(md_env)
    con = client.connect_read_scaling()
    try:
        run_reader_refresh(con, None if args.all else db, refresh_all=args.all)
        print(f"OK reader: {sql}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
