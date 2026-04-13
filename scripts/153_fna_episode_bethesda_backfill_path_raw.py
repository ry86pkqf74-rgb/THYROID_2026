#!/usr/bin/env python3
"""
Backfill fna_episode_master_v2.bethesda_category where script 152 left NULL but
deterministic values exist in other episode columns or raw text.

Phase A — pathology_diagnosis: single digit 1–6 only (e.g. path field holds "2"
while bethesda_raw was empty; TRY_CAST(bethesda AS INT) never ran on path).

Phase B — bethesda_raw: leading Bethesda class 1–6 with optional trailing
asterisk (e.g. "2*") — matches classification in v_fna_episode_bethesda_resolved_v1.

Run:
    .venv/bin/python scripts/153_fna_episode_bethesda_backfill_path_raw.py --md
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

PHASE_A = """
UPDATE fna_episode_master_v2 AS e
SET bethesda_category = CAST(TRIM(CAST(e.pathology_diagnosis AS VARCHAR)) AS INTEGER)
WHERE e.bethesda_category IS NULL
  AND e.pathology_diagnosis IS NOT NULL
  AND regexp_matches(TRIM(CAST(e.pathology_diagnosis AS VARCHAR)), '^[1-6]$')
"""

# DuckDB regexp: first character is class 1–6; optional whitespace and * only
PHASE_B = """
UPDATE fna_episode_master_v2 AS e
SET bethesda_category = CAST(
    regexp_extract(TRIM(CAST(e.bethesda_raw AS VARCHAR)), '^([1-6])') AS INTEGER
)
WHERE e.bethesda_category IS NULL
  AND e.bethesda_raw IS NOT NULL
  AND TRIM(CAST(e.bethesda_raw AS VARCHAR)) <> ''
  AND regexp_matches(
    TRIM(CAST(e.bethesda_raw AS VARCHAR)),
    '^[1-6]\\s*\\*?$'
  )
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    p.add_argument("--dry-run", action="store_true", help="Counts only, no UPDATE")
    args = p.parse_args()
    if not args.md:
        args.md = True

    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb")

    def _null_count() -> int:
        row = con.execute(
            "SELECT COUNT(*) FROM fna_episode_master_v2 WHERE bethesda_category IS NULL"
        ).fetchone()
        return int(row[0]) if row else 0

    before = _null_count()
    print(f"[153] bethesda_category IS NULL before: {before}")

    if args.dry_run:
        pa = con.execute(
            """
            SELECT COUNT(*) FROM fna_episode_master_v2 e
            WHERE e.bethesda_category IS NULL
              AND e.pathology_diagnosis IS NOT NULL
              AND regexp_matches(TRIM(CAST(e.pathology_diagnosis AS VARCHAR)), '^[1-6]$')
            """
        ).fetchone()
        pb = con.execute(
            """
            SELECT COUNT(*) FROM fna_episode_master_v2 e
            WHERE e.bethesda_category IS NULL
              AND e.bethesda_raw IS NOT NULL
              AND TRIM(CAST(e.bethesda_raw AS VARCHAR)) <> ''
              AND regexp_matches(
                TRIM(CAST(e.bethesda_raw AS VARCHAR)),
                '^[1-6]\\s*\\*?$'
              )
            """
        ).fetchone()
        print(f"[153] dry-run Phase A match rows: {int(pa[0]) if pa else 0}")
        print(f"[153] dry-run Phase B match rows: {int(pb[0]) if pb else 0}")
        con.close()
        return 0

    con.execute(PHASE_A)
    mid = _null_count()
    print(f"[153] after Phase A NULL count: {mid}")

    con.execute(PHASE_B)
    after = _null_count()
    print(f"[153] after Phase B NULL count: {after}")
    print(f"[153] total reduction: {before - after}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
