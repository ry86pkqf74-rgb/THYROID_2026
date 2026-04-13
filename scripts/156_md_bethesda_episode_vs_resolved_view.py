#!/usr/bin/env python3
"""
Deploy ``v_fna_bethesda_episode_vs_resolved_v1`` on MotherDuck (read-only DDL: VIEW).

Requires ``v_fna_episode_bethesda_resolved_v1`` (``scripts/151_source_truth_confirmation_v1.py --md``).

Usage:
  .venv/bin/python scripts/156_md_bethesda_episode_vs_resolved_view.py --md
  .venv/bin/python scripts/156_md_bethesda_episode_vs_resolved_view.py --md --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

SQL_PATH = ROOT / "scripts" / "sql" / "v_fna_bethesda_episode_vs_resolved_v1.sql"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if not args.md:
        args.md = True

    sql = SQL_PATH.read_text(encoding="utf-8")
    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb")
    try:
        if args.dry_run:
            print(f"[156] dry-run: would deploy VIEW from {SQL_PATH.name}")
            return 0
        con.execute(sql)
        row = con.execute(
            "SELECT COUNT(*) FROM v_fna_bethesda_episode_vs_resolved_v1"
        ).fetchone()
        n = int(row[0]) if row is not None else 0
        print(f"[156] v_fna_bethesda_episode_vs_resolved_v1 OK ({n} rows)")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
