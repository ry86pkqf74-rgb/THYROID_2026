#!/usr/bin/env python3
"""Inspect current schemas of the 5 US tables to design correct view DDL."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB

TABLES = [
    "canonical_us_nodule_v2",
    "canonical_us_thyroid_gland_v2",
    "canonical_us_lymph_node_v2",
    "canonical_us_exam_master_v2",
    "canonical_us_patient_master_v2",
]


def main() -> int:
    con = connect_locked()
    for t in TABLES:
        print(f"\n=== {t} ===")
        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [PUB, t],
        ).fetchall()
        n = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        npts = "n/a"
        if any(c[0] == "research_id" for c in cols):
            npts = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM main.{t}"
            ).fetchone()[0]
        print(f"rows={n}  patients={npts}  cols={len(cols)}")
        for cn, dt in cols:
            print(f"    {cn:<48s} {dt}")

    print("\n=== TIRADS column probe on canonical_us_nodule_v2 ===")
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog = '{PUB}' AND table_schema = 'main' "
        f"AND table_name = 'canonical_us_nodule_v2' "
        f"AND (column_name ILIKE '%tirads%' OR column_name ILIKE '%category%')"
    ).fetchall()
    for (c,) in cols:
        print(f"    {c}")

    print("\n=== Sample distinct values of TIRADS-related cols ===")
    for col in ["acr2017_tirads_category", "updated_tirads_category",
                "source_tirads_v2", "tirads_reported_in_text"]:
        try:
            rows = con.execute(
                f"SELECT {col}, COUNT(*) FROM main.canonical_us_nodule_v2 "
                f"GROUP BY 1 ORDER BY 2 DESC LIMIT 12"
            ).fetchall()
            print(f"  {col}:")
            for v, n in rows:
                print(f"    {str(v)[:40]:<40s} {n}")
        except Exception as e:
            print(f"  {col}: ERROR {e}")

    print("\n=== exam_master sample row to see what's currently stored ===")
    rows = con.execute(
        "SELECT research_id, exam_date, worst_tirads_category_this_exam, "
        "best_tirads_category_this_exam, count_tr1, count_tr2, count_tr3, "
        "count_tr4, count_tr5 "
        f"FROM main.canonical_us_exam_master_v2 LIMIT 5"
    ).fetchall()
    for r in rows:
        print(f"  {r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
