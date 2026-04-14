#!/usr/bin/env python3
"""
Upload LN cross-validation and master rollup parquets to MotherDuck.

Run this after ln_crossval.py and ln_master_rollup.py have generated their outputs.
Retry-safe: uses CREATE OR REPLACE TABLE.

Usage:
  .venv/bin/python scripts/ln_upload_to_md.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient


def main() -> None:
    crossval_pq = REPO / "output" / "ln_crossval.parquet"
    rollup_pq = REPO / "output" / "ln_master_rollup.parquet"

    for pq in (crossval_pq, rollup_pq):
        if not pq.exists():
            print(f"ERROR: Missing {pq}. Run ln_crossval.py / ln_master_rollup.py first.")
            sys.exit(1)

    client = MotherDuckClient.for_env("prod")
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            print(f"Connecting to MotherDuck (attempt {attempt + 1}/{max_attempts})...")
            con = client.connect_rw()
            print("Connected!")

            print("Uploading ln_crossval_v1...")
            con.execute(f"""
                CREATE OR REPLACE TABLE ln_crossval_v1 AS
                SELECT * FROM read_parquet('{crossval_pq}')
            """)
            rc = con.execute("SELECT COUNT(*) FROM ln_crossval_v1").fetchone()
            print(f"  ln_crossval_v1: {rc[0]} rows")

            print("Uploading ln_master_rollup_v1...")
            con.execute(f"""
                CREATE OR REPLACE TABLE ln_master_rollup_v1 AS
                SELECT * FROM read_parquet('{rollup_pq}')
            """)
            rc = con.execute("SELECT COUNT(*) FROM ln_master_rollup_v1").fetchone()
            print(f"  ln_master_rollup_v1: {rc[0]} rows")

            print("Upload complete!")
            con.close()
            return

        except Exception as e:
            print(f"  Failed: {str(e)[:150]}")
            if attempt < max_attempts - 1:
                delay = 10 * (attempt + 1)
                print(f"  Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print("  All attempts failed. MotherDuck may be experiencing an outage.")
                sys.exit(1)


if __name__ == "__main__":
    main()
