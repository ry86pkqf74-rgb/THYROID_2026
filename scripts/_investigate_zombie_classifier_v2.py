#!/usr/bin/env python3
"""Comprehensive cross-tab to find the user's zombie classifier."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
TBL = f'"{PUB}".main.canonical_us_nodule_v2'


def main() -> int:
    tok = get_token()
    con = duckdb.connect(f"md:{PUB}?motherduck_token={tok}")

    print("=" * 78)
    print("Comprehensive cross-tab")
    print("=" * 78)
    rows = con.execute(
        f"""
        SELECT
          COALESCE(source_tables_cunc_legacy,'<NULL>')              AS prov,
          source_base                                                AS sb,
          source_tirads_v2                                           AS sv2,
          source_tirads_llm                                          AS sll,
          source_dynamics_llm                                        AS sdl,
          source_fna_linkage                                         AS sfl,
          nlp_backfill_pending                                       AS pend,
          is_aggregate_row                                           AS agg,
          CASE WHEN location_raw IS NULL THEN 'NULL'
               WHEN location_raw LIKE '%;%' THEN 'semi'
               ELSE 'plain' END                                      AS loc_pattern,
          COUNT(*)                                                   AS n
        FROM {TBL}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        HAVING COUNT(*) >= 10
        ORDER BY n DESC
        LIMIT 60
        """
    ).fetchall()
    hdr = ("provenance", "sb", "sv2", "sll", "sdl", "sfl",
           "pend", "agg", "loc", "n")
    print(f"  {hdr[0]:<55} {' '.join(h for h in hdr[1:9])}  {hdr[9]:>7}")
    for r in rows:
        prov = (r[0] or '<NULL>')[:55]
        flags = "  ".join(
            ("T" if v is True else "F" if v is False else "?")
            for v in r[1:8]
        )
        loc = r[8]
        n = r[9]
        print(f"  {prov:<55} {flags}  {loc:<5} {n:>7,}")

    print("\n" + "=" * 78)
    print("Quick-target probes (looking for 2,152 / 27 / 18,310 / 17,090)")
    print("=" * 78)
    probes = [
        ("source_base=T, source_tirads_llm=F",
         "source_base=TRUE AND source_tirads_llm=FALSE"),
        ("source_base=T, source_tirads_llm=F, NO ;",
         "source_base=TRUE AND source_tirads_llm=FALSE "
         "AND COALESCE(location_raw,'') NOT LIKE '%;%'"),
        ("source_base=T, source_tirads_llm=F, has ;",
         "source_base=TRUE AND source_tirads_llm=FALSE "
         "AND COALESCE(location_raw,'') LIKE '%;%'"),
        ("source_base=T, source_tirads_llm=T (LLM-enriched legacy)",
         "source_base=TRUE AND source_tirads_llm=TRUE"),
        ("source_base=F, source_tirads_llm=T (pure LLM insert)",
         "source_base=FALSE AND source_tirads_llm=TRUE"),
        ("source_tirads_llm=T",
         "source_tirads_llm=TRUE"),
        ("source_tirads_llm=T, NO ;",
         "source_tirads_llm=TRUE AND COALESCE(location_raw,'') NOT LIKE '%;%'"),
        ("source_tirads_llm=T, has ;",
         "source_tirads_llm=TRUE AND COALESCE(location_raw,'') LIKE '%;%'"),
        ("nlp_backfill_pending=T (legacy needing LLM)",
         "nlp_backfill_pending=TRUE"),
        ("nlp_backfill_pending=T, has ;",
         "nlp_backfill_pending=TRUE AND COALESCE(location_raw,'') LIKE '%;%'"),
        ("nlp_backfill_pending=T AND no LLM at all",
         "nlp_backfill_pending=TRUE "
         "AND source_tirads_llm=FALSE AND source_dynamics_llm=FALSE"),
        ("source_tables LIKE %llm% AND has ;",
         "LOWER(COALESCE(source_tables_cunc_legacy,'')) LIKE '%llm%' "
         "AND COALESCE(location_raw,'') LIKE '%;%'"),
        ("only source_tables imaging_nodule_master AND pending=F",
         "source_tables_cunc_legacy='imaging_nodule_master_v1' "
         "AND nlp_backfill_pending=FALSE"),
        ("nodule_index_within_exam=1 AND nlp_backfill_pending=T",
         "nodule_index_within_exam=1 AND nlp_backfill_pending=TRUE"),
    ]
    for label, where in probes:
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {TBL} WHERE {where}"
        ).fetchone()[0])
        print(f"  {label:<60} {n:>7,}")

    print("\n" + "=" * 78)
    print("EXAM-grain check: how many distinct (research_id, exam_date)")
    print("for various candidate zombie defs (target ~664 exams)")
    print("=" * 78)
    exam_probes = [
        ("nlp_backfill_pending=TRUE",
         "nlp_backfill_pending=TRUE"),
        ("nlp_backfill_pending=TRUE AND no LLM",
         "nlp_backfill_pending=TRUE "
         "AND source_tirads_llm=FALSE AND source_dynamics_llm=FALSE"),
        ("source_base=T AND source_tirads_llm=F",
         "source_base=TRUE AND source_tirads_llm=FALSE"),
    ]
    for label, where in exam_probes:
        n_exams = int(con.execute(
            f"SELECT COUNT(DISTINCT (research_id, exam_date)) FROM {TBL} "
            f"WHERE {where}"
        ).fetchone()[0])
        n_rows = int(con.execute(
            f"SELECT COUNT(*) FROM {TBL} WHERE {where}"
        ).fetchone()[0])
        print(f"  {label:<60} rows={n_rows:>7,} exams={n_exams:>7,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
