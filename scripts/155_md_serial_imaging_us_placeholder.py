#!/usr/bin/env python3
"""
Create or replace ``serial_imaging_us`` on MotherDuck when the table is missing.

``scripts/22_canonical_episodes_v2.py`` and linkage audits expect this table to
exist; the Imaging_12 / institutional serial-US Excel feed may not be loaded in
cloud.  This script materializes an **empty** table with the columns used by
``IMAGING_NODULE_LONG_V2_SQL`` (see ``scripts/22_canonical_episodes_v2.py``).

Populate later from institutional Parquet/Excel — do not invent rows.

Usage:
  .venv/bin/python scripts/155_md_serial_imaging_us_placeholder.py --md
  .venv/bin/python scripts/155_md_serial_imaging_us_placeholder.py --md --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

DDL = """
CREATE OR REPLACE TABLE serial_imaging_us (
    research_id INTEGER,
    us_date VARCHAR,
    dominant_nodule_size_on_us DOUBLE,
    us_findings_impression VARCHAR,
    us_impression VARCHAR,
    dominant_nodule_location VARCHAR
)
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if not args.md:
        args.md = True

    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb")
    try:
        before = None
        try:
            row = con.execute("SELECT COUNT(*) FROM serial_imaging_us").fetchone()
            before = int(row[0]) if row is not None else None
        except Exception:
            before = None
        print(f"[155] serial_imaging_us before: {before!r} (None = missing)")
        if args.dry_run:
            print("[155] dry-run: would CREATE OR REPLACE empty serial_imaging_us (6 cols)")
            return 0
        con.execute(DDL)
        row2 = con.execute("SELECT COUNT(*) FROM serial_imaging_us").fetchone()
        after = int(row2[0]) if row2 is not None else 0
        print(f"[155] serial_imaging_us after: {after} rows (schema only)")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
