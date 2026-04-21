#!/usr/bin/env python3
"""Final sanity check after Phases 4-7 + Phase 6 script edits.

Confirms:
  - main.canonical_us_*_master_v2 are VIEWs and return non-empty
  - raw.ultrasound_reports + raw.us_nodules_tirads are BASE TABLEs with expected counts
  - main.ultrasound_reports / main.us_nodules_tirads no longer exist
  - views_readable.US_Reports_Raw + US_Nodules_TIRADS still return non-empty
"""
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
    print("=" * 60)
    print("Final state — main.* US/tirads/ultrasound/nodule")
    print("=" * 60)
    for n, t in con.execute(f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog='{PUB}' AND table_schema='main'
          AND (LOWER(table_name) LIKE '%us%' OR LOWER(table_name) LIKE '%tirads%'
               OR LOWER(table_name) LIKE '%ultrasound%' OR LOWER(table_name) LIKE '%nodule%')
        ORDER BY table_type, table_name
    """).fetchall():
        print(f"  {t:<12s} {n}")

    print("\n" + "=" * 60)
    print("Final state — raw.*")
    print("=" * 60)
    for n, t in con.execute(f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog='{PUB}' AND table_schema='raw'
        ORDER BY table_name
    """).fetchall():
        print(f"  {t:<12s} {n}")

    print("\n" + "=" * 60)
    print("Smoke counts")
    print("=" * 60)
    for fq, label in [
        ("main.canonical_us_exam_master_v2", "exam_master view"),
        ("main.canonical_us_patient_master_v2", "patient_master view"),
        ("main.canonical_us_nodule_v2", "nodule_v2 (table)"),
        ("main.canonical_us_thyroid_gland_v2", "gland_v2 (table)"),
        ("main.canonical_us_lymph_node_v2", "lymph_node_v2 (table)"),
        ("raw.ultrasound_reports", "raw ultrasound_reports"),
        ("raw.us_nodules_tirads", "raw us_nodules_tirads"),
        ("views_readable.US_Reports_Raw", "views_readable.US_Reports_Raw"),
        ("views_readable.US_Nodules_TIRADS", "views_readable.US_Nodules_TIRADS"),
    ]:
        n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        print(f"  {label:<35s} {n:>8d}")

    print("\n" + "=" * 60)
    print("Confirm main.* raw feeds GONE")
    print("=" * 60)
    for nm in ("ultrasound_reports", "us_nodules_tirads"):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{nm}").fetchone()[0]
            print(f"  main.{nm} STILL EXISTS rows={n}  *** UNEXPECTED ***")
        except Exception as e:
            print(f"  main.{nm}: gone (as expected)  [{type(e).__name__}]")

    print("\n" + "=" * 60)
    print("Confirm archive snapshots present in 'Thyroid 2026 UPdated'")
    print("=" * 60)
    for n in ("archived_canonical_us_exam_master_v2",
              "archived_canonical_us_patient_master_v2"):
        try:
            r = con.execute(
                f'SELECT COUNT(*) FROM "Thyroid 2026 UPdated".us_legacy_20260421.{n}'
            ).fetchone()[0]
            print(f"  {n}: {r}")
        except Exception as e:
            print(f"  {n}: ERROR {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
