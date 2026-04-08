#!/usr/bin/env python3
"""Smoke test: verify DuckDB connection layer (local file or fail-closed MotherDuck).

Usage:
    .venv/bin/python scripts/smoke_test_md_connection.py          # local file
    .venv/bin/python scripts/smoke_test_md_connection.py --md     # MotherDuck (fail-closed)

With ``--md``, connects via :func:`utils.md_connect.connect_md_fail_closed`, which uses the
same ``PRAGMA database_list`` verification as :func:`utils.md_connect.connect_md_or_file`
(``fail_closed=True``). There is no silent fallback to a local file; missing token, connection
failure, or a connection that does not attach MotherDuck exits the process with code 1.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "thyroid_master.duckdb"


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test DB connection layer")
    ap.add_argument(
        "--md",
        action="store_true",
        help="Connect via MotherDuck (fail-closed; shared verification in utils/md_connect.py)",
    )
    ap.add_argument(
        "--custom-user-agent",
        dest="custom_user_agent",
        default=None,
        metavar="STRING",
        help="Optional MotherDuck custom_user_agent for query attribution (--md only)",
    )
    ap.add_argument(
        "--session-hint",
        dest="motherduck_session_hint",
        default=None,
        metavar="STRING",
        help="Optional session_hint / affinity string (--md only; also see MOTHERDUCK_SESSION_HINT)",
    )
    ap.add_argument(
        "--catalog-probe",
        action="store_true",
        help="After attach (--md only), run PRAGMA database_list + MotherDuck info-schema probes "
        "(best-effort; no token output).",
    )
    args = ap.parse_args()
    if args.catalog_probe and not args.md:
        print("FAIL: --catalog-probe requires --md")
        return 1

    from utils.md_connect import connect_md_fail_closed, connect_md_or_file

    if args.md:
        con = connect_md_fail_closed(
            DB_PATH,
            custom_user_agent=args.custom_user_agent,
            motherduck_session_hint=args.motherduck_session_hint,
        )
    else:
        con = connect_md_or_file(DB_PATH, md=False)

    try:
        info = con.execute("PRAGMA version").fetchone()
        md_extra = None
        if args.md:
            # Lightweight catalog probe after fail-closed attach (token never printed).
            row = con.execute(
                "SELECT current_catalog(), current_database()"
            ).fetchone()
            md_extra = row
            if args.catalog_probe:
                _run_catalog_probe(con)
    except Exception as exc:
        print(f"FAIL: query error — {exc}")
        return 1
    finally:
        con.close()

    version = info[0] if info else "unknown"
    print(f"DuckDB version : {version}")
    if md_extra is not None:
        print(f"Catalog / DB    : {md_extra[0]!r} / {md_extra[1]!r}")
    print(f"Connection type : {'MotherDuck (cloud)' if args.md else 'Local file'}")
    print("PASS")
    return 0


def _run_catalog_probe(con) -> None:
    """Read-only introspection for operator proof matrix (never logs secrets)."""

    def _q(label: str, sql: str) -> None:
        try:
            rows = con.execute(sql).fetchall()
            print(f"  [probe:{label}] ok row_count={len(rows)}")
        except Exception as exc:
            print(f"  [probe:{label}] unavailable — {exc}")

    print("  --- MotherDuck catalog probe (read-only) ---")
    try:
        dbl = con.execute("PRAGMA database_list").fetchall()
        print(f"  [probe:PRAGMA database_list] ok entries={len(dbl)}")
    except Exception as exc:
        print(f"  [probe:PRAGMA database_list] fail — {exc}")
    _q("md_information_schema.databases", "FROM md_information_schema.databases LIMIT 20")
    _q("md_information_schema.database_snapshots", "FROM md_information_schema.database_snapshots LIMIT 20")
    _q("md_information_schema.query_history", "FROM md_information_schema.query_history LIMIT 20")
    _q("md_information_schema.recent_queries", "FROM md_information_schema.recent_queries LIMIT 20")
    try:
        ua = con.execute("SELECT current_setting('custom_user_agent')").fetchone()
        if ua and str(ua[0] or "").strip():
            print("  [probe:custom_user_agent] ok non-empty")
        else:
            print("  [probe:custom_user_agent] empty")
    except Exception as exc:
        print(f"  [probe:custom_user_agent] unavailable — {exc}")
    try:
        hint = con.execute("SELECT current_setting('motherduck_session_hint')").fetchone()
        if hint and str(hint[0] or "").strip():
            print("  [probe:motherduck_session_hint] ok non-empty")
        else:
            print("  [probe:motherduck_session_hint] empty")
    except Exception as exc:
        print(f"  [probe:motherduck_session_hint] unavailable — {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
