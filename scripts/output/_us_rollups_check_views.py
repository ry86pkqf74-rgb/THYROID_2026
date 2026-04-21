#!/usr/bin/env python3
"""Find views that reference the moved raw tables, anywhere in the publication DB."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB


def main() -> int:
    con = connect_locked()

    rows = con.execute(f"""
        SELECT table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_catalog = '{PUB}'
          AND (LOWER(view_definition) LIKE '%ultrasound_reports%'
               OR LOWER(view_definition) LIKE '%us_nodules_tirads%')
        ORDER BY table_schema, table_name
    """).fetchall()

    print(f"Views referencing ultrasound_reports OR us_nodules_tirads: {len(rows)}")
    for sc, nm, defn in rows:
        print(f"\n--- {sc}.{nm} ---")
        s = (defn or "").lower()
        for needle in ("ultrasound_reports", "us_nodules_tirads"):
            if needle in s:
                idx = s.find(needle)
                ctx = (defn or "")[max(0, idx - 60):idx + len(needle) + 60]
                print(f"    contains {needle}: ...{ctx}...")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
