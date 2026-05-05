#!/usr/bin/env python3
"""Pull Snowflake VALIDATION_RUN_LOG_V1 into MotherDuck ``main.cowork_sf_validation_log_v1``.

Source: ``THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1`` (17 rows per ``VALIDATE_ALL_COHORTS()``;
24 rows per ``VALIDATE_ALL_COHORTS_V3()`` — see ``snowflake_trial/sql_drops/mig_309_sp_v3.sql``).

Requires ``SNOWFLAKE_PAT``. With ``--md``: MotherDuck RW token via ``motherduck.local.toml`` or
``MD_SA_TOKEN`` / ``MOTHERDUCK_TOKEN``; catalog locked to ``thyroid_canonical_publication_v1_0``.

Usage (repo root, ``.venv``)::

    SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/35_pull_sf_validation_log.py --md
    SNOWFLAKE_PAT=... .venv/bin/python snowflake_trial/scripts/35_pull_sf_validation_log.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_DIR = Path(__file__).resolve().parent
for _p in (REPO_ROOT, _SCRIPT_DIR):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

from _sf_client import get_cursor  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--md",
        action="store_true",
        help="Write mirror to MotherDuck (fail-closed; requires RW token).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from Snowflake only; do not write MotherDuck.",
    )
    args = ap.parse_args()
    if args.md and args.dry_run:
        print("FATAL: use only one of --md or --dry-run", file=sys.stderr)
        return 1
    if not args.md and not args.dry_run:
        print("FATAL: pass --md to write MotherDuck or --dry-run to preview.", file=sys.stderr)
        return 1
    if not os.getenv("SNOWFLAKE_PAT"):
        print("FATAL: SNOWFLAKE_PAT is not set.", file=sys.stderr)
        return 1

    ctx, cur = get_cursor()
    try:
        cur.execute("SELECT * FROM THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1")
        sf_df = cur.fetch_pandas_all()
    finally:
        ctx.close()

    sf_df.columns = [str(c).upper() for c in sf_df.columns]
    n = len(sf_df)
    print(f"Snowflake VALIDATION_RUN_LOG_V1 rows fetched: {n}")
    if n == 0:
        print("WARN: source table is empty; MD mirror will be cleared on --md.")

    need = {"RUN_ID", "RUN_TS", "CHECK_NAME", "EXPECTED", "OBSERVED", "STATUS", "NOTES"}
    have = set(sf_df.columns)
    if not need <= have:
        miss = sorted(need - have)
        print(f"FATAL: SF dataframe missing columns {miss}; got {sorted(have)}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("--dry-run: skip MotherDuck write.")
        return 0

    from utils.md_connect import connect_md_fail_closed  # noqa: E402

    md = connect_md_fail_closed(REPO_ROOT / "thyroid_master.duckdb")
    try:
        md.execute("USE thyroid_canonical_publication_v1_0")
        md.execute(
            """
            CREATE TABLE IF NOT EXISTS main.cowork_sf_validation_log_v1 (
              sf_run_id BIGINT,
              sf_run_ts TIMESTAMP,
              check_name VARCHAR,
              expected VARCHAR,
              observed VARCHAR,
              status VARCHAR,
              notes VARCHAR,
              pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        md.execute("DELETE FROM main.cowork_sf_validation_log_v1")
        md.register("_sf_validation_run_log", sf_df)
        try:
            md.execute(
                """
                INSERT INTO main.cowork_sf_validation_log_v1
                  (sf_run_id, sf_run_ts, check_name, expected, observed, status, notes)
                SELECT
                  RUN_ID::BIGINT,
                  CAST(RUN_TS AS TIMESTAMP),
                  CHECK_NAME::VARCHAR,
                  EXPECTED::VARCHAR,
                  OBSERVED::VARCHAR,
                  STATUS::VARCHAR,
                  NOTES::VARCHAR
                FROM _sf_validation_run_log
                """
            )
        finally:
            md.unregister("_sf_validation_run_log")
        cnt = md.execute("SELECT COUNT(*) FROM main.cowork_sf_validation_log_v1").fetchone()[0]
        print(f"Mirrored {cnt} rows to main.cowork_sf_validation_log_v1")
    finally:
        md.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
