#!/usr/bin/env python3
"""129_md_pull_canonical_parquets.py — Refresh local processed/*.parquet from MotherDuck main.

Overwrites the same paths that ``103_fact_lineage_materialize.py`` writes so local
validation (e.g. 119 row parity) matches cloud after a cloud-only canonical fix.

Tables (when present on MotherDuck):
  processed/canonical_extracted_fact_long_v1.parquet
  processed/canonical_fact_quarantine_v1.parquet
  processed/canonical_extracted_fact_long_v2.parquet
  processed/canonical_fact_quarantine_v2.parquet
  processed/note_extraction_runs.parquet

Usage:
  .venv/bin/python scripts/129_md_pull_canonical_parquets.py --md
  .venv/bin/python scripts/129_md_pull_canonical_parquets.py --md --md-sa
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

PROCESSED = ROOT / "processed"

DEFAULT_TABLES: tuple[str, ...] = (
    "canonical_extracted_fact_long_v1",
    "canonical_fact_quarantine_v1",
    "canonical_extracted_fact_long_v2",
    "canonical_fact_quarantine_v2",
    "note_extraction_runs",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Read from MotherDuck (fail-closed).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"))
    p.add_argument("--processed-dir", type=Path, default=PROCESSED)
    p.add_argument("--md-user-agent", default=None)
    p.add_argument("--md-session-hint", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("  ERROR: --md required (pull from MotherDuck only).")
        sys.exit(2)

    out_dir = args.processed_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ua = args.md_user_agent or os.environ.get(
        "MOTHERDUCK_CUSTOM_USER_AGENT",
        "THYROID_2026_md_pull_canonical/1.0",
    )
    con = connect_md_or_file(
        Path(args.db_path),
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
        custom_user_agent=ua,
        motherduck_session_hint=args.md_session_hint,
    )
    try:
        for tbl in DEFAULT_TABLES:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?",
                [tbl],
            ).fetchone()[0]
            if not exists:
                print(f"  [skip] main.{tbl} not on MotherDuck")
                continue
            dest = out_dir / f"{tbl}.parquet"
            # Absolute path for COPY (cross-platform)
            dest_abs = dest.resolve()
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
            con.execute(
                f"COPY (SELECT * FROM main.{tbl}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(dest_abs)],
            )
            print(f"  [ok] main.{tbl} → {dest.name} ({int(n):,} rows)")
    finally:
        con.close()

    print("=" * 60)
    print(f"  DONE — parquets under {out_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
