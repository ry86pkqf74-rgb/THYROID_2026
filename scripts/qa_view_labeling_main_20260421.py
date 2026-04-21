#!/usr/bin/env python3
"""
Phase 6.1 + smoke: main.* VIEWs must be named with _VIEW_ (Logan 2026-04-21), with
platform MotherDuck catalog views excluded. Run with MotherDuck RW (see motherduck_client).

  uv run python scripts/qa_view_labeling_main_20260421.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig

DB = "thyroid_canonical_publication_v1_0"
SQL_VIOLATIONS = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_type = 'VIEW'
  AND table_name NOT LIKE '%\\_VIEW\\_%' ESCAPE '\\'
  AND table_name NOT IN (
    'database_snapshots', 'databases', 'owned_shares',
    'query_history', 'recent_queries', 'shared_with_me',
    'storage_info', 'storage_info_history'
  )
ORDER BY 1
"""
VIEWS = [
    "canonical_us_exam_master_VIEW_v2",
    "canonical_us_patient_master_VIEW_v2",
    "molecular_fusions_unnested_VIEW_v2",
    "molecular_variants_unnested_VIEW_v2",
]


def main() -> int:
    con = MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()
    bad = con.execute(SQL_VIOLATIONS).fetchall()
    if bad:
        print("FAIL: main VIEWs without _VIEW_ (non-platform):", [r[0] for r in bad])
        con.close()
        return 1
    print("OK: Phase 6.1 naming check (0 violations).")
    for v in VIEWS:
        n = con.execute(f'SELECT count(*) AS n FROM "main"."{v}"').fetchone()[0]
        print(f"  {v}: {n} rows")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
