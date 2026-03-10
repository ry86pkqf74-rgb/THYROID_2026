#!/usr/bin/env python3
"""
09b_motherduck_upload_notes_entities.py — Upload notes + entity tables to MotherDuck

REQUIRES --confirm flag to actually write.  Without it, only prints
what it would do (dry-run mode).

Tables uploaded:
  clinical_notes_long
  note_entities_staging
  note_entities_genetics
  note_entities_procedures
  note_entities_complications
  note_entities_medications
  note_entities_problem_list
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig

PROCESSED = ROOT / "processed"

TABLES_TO_UPLOAD = [
    "clinical_notes_long",
    "note_entities_staging",
    "note_entities_genetics",
    "note_entities_procedures",
    "note_entities_complications",
    "note_entities_medications",
    "note_entities_problem_list",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload notes entity tables to MotherDuck")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually upload (without this flag, runs in dry-run mode)",
    )
    args = parser.parse_args()

    print("=" * 70)
    if args.confirm:
        print("  MOTHERDUCK UPLOAD — LIVE MODE")
    else:
        print("  MOTHERDUCK UPLOAD — DRY RUN (pass --confirm to execute)")
    print("=" * 70)

    available: list[tuple[str, Path]] = []
    for tbl in TABLES_TO_UPLOAD:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            size_mb = pq.stat().st_size / (1024 * 1024)
            print(f"  Found {tbl:40s}  ({size_mb:.2f} MB)")
            available.append((tbl, pq))
        else:
            print(f"  SKIP  {tbl:40s}  (parquet not found)")

    if not available:
        print("\n  No tables to upload.")
        sys.exit(0)

    if not args.confirm:
        print(f"\n  Would upload {len(available)} tables.")
        print("  Re-run with --confirm to execute.")
        sys.exit(0)

    client = MotherDuckClient()
    con = client.connect_rw()

    for tbl, pq in available:
        try:
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Uploaded {tbl:40s}  {cnt:>8,} rows")
        except Exception as exc:
            print(f"  FAILED  {tbl}: {exc}")

    con.close()
    print(f"\n{'=' * 70}")
    print("  UPLOAD COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
