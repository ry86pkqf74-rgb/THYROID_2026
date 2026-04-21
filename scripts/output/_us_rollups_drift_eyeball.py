#!/usr/bin/env python3
"""Eyeball the 45 non-TIRADS, non-rank diff rows + 3 sanity queries on the
449 view-only exam keys."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
TBL_EXAM = f"{PUB}.main.canonical_us_exam_master_v2"
CAND_EXAM = f"{PUB}.manuscript_workspace.candidate_us_exam_master_v2"

NON_TIRADS_NON_RANK_DIFF_COLS = [
    "n_nodules_on_exam", "largest_nodule_cm", "second_largest_nodule_cm",
    "bilateral_flag", "isthmus_nodule_flag",
]


def main() -> int:
    con = connect_locked()

    # ── 1. 45 non-TIRADS, non-rank diff rows ───────────────────────────
    print("=" * 72)
    print("45 non-TIRADS, non-rank diff rows: side-by-side dump")
    print("=" * 72)

    pred = " OR ".join([
        f't."{c}" IS DISTINCT FROM v."{c}"'
        for c in NON_TIRADS_NON_RANK_DIFF_COLS
    ])
    sql = f"""
        SELECT
          t.research_id, t.exam_date,
          t.n_nodules_on_exam       AS t_n_nod,    v.n_nodules_on_exam       AS v_n_nod,
          t.largest_nodule_cm       AS t_largest,  v.largest_nodule_cm       AS v_largest,
          t.second_largest_nodule_cm AS t_2nd,     v.second_largest_nodule_cm AS v_2nd,
          t.bilateral_flag          AS t_bilat,    v.bilateral_flag          AS v_bilat,
          t.isthmus_nodule_flag     AS t_isth,     v.isthmus_nodule_flag     AS v_isth
        FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
          ON t.research_id IS NOT DISTINCT FROM v.research_id
         AND t.exam_date   IS NOT DISTINCT FROM v.exam_date
        WHERE {pred}
        ORDER BY t.research_id, t.exam_date
    """
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.execute(sql).description]
    print(f"  {len(rows)} rows total. Showing all of them.")
    print()
    print(" | ".join(f"{c:<12s}" for c in cols))
    print("-" * (15 * len(cols)))
    for r in rows:
        print(" | ".join(f"{str(v)[:12]:<12s}" for v in r))

    # Aggregate "what changed" tally
    print("\n  aggregate change-direction tally:")

    def count_change(col_t: str, col_v: str, condition: str) -> int:
        q = f"""
            SELECT COUNT(*) FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
              ON t.research_id IS NOT DISTINCT FROM v.research_id
             AND t.exam_date   IS NOT DISTINCT FROM v.exam_date
            WHERE ({pred}) AND ({condition})
        """
        return con.execute(q).fetchone()[0]

    print(f"    largest_nodule_cm: NULL -> value = "
          f"{count_change('largest_nodule_cm','largest_nodule_cm', 't.largest_nodule_cm IS NULL AND v.largest_nodule_cm IS NOT NULL')}")
    print(f"    largest_nodule_cm: value -> NULL = "
          f"{count_change('largest_nodule_cm','largest_nodule_cm', 't.largest_nodule_cm IS NOT NULL AND v.largest_nodule_cm IS NULL')}")
    print(f"    largest_nodule_cm: value -> different value = "
          f"{count_change('largest_nodule_cm','largest_nodule_cm', 't.largest_nodule_cm IS NOT NULL AND v.largest_nodule_cm IS NOT NULL AND t.largest_nodule_cm != v.largest_nodule_cm')}")

    print("\n    largest_nodule_cm ratio v/t (when both populated and != ):")
    rows = con.execute(f"""
        SELECT t.research_id, t.exam_date, t.largest_nodule_cm AS t_val,
               v.largest_nodule_cm AS v_val,
               ROUND(v.largest_nodule_cm / NULLIF(t.largest_nodule_cm, 0), 3) AS ratio
        FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
          ON t.research_id IS NOT DISTINCT FROM v.research_id
         AND t.exam_date   IS NOT DISTINCT FROM v.exam_date
        WHERE t.largest_nodule_cm IS NOT NULL AND v.largest_nodule_cm IS NOT NULL
          AND t.largest_nodule_cm != v.largest_nodule_cm
        ORDER BY ratio
    """).fetchall()
    if rows:
        for r in rows:
            print(f"      rid={r[0]}  date={r[1]}  table={r[2]}  view={r[3]}  ratio={r[4]}")
    else:
        print("      none — all largest_nodule_cm diffs are NULL <-> value transitions")

    print(f"\n    n_nodules_on_exam: NULL -> value = "
          f"{count_change('n_nodules_on_exam','n_nodules_on_exam', 't.n_nodules_on_exam IS NULL AND v.n_nodules_on_exam IS NOT NULL')}")
    print(f"    n_nodules_on_exam: value -> different value = "
          f"{count_change('n_nodules_on_exam','n_nodules_on_exam', 't.n_nodules_on_exam IS NOT NULL AND v.n_nodules_on_exam IS NOT NULL AND t.n_nodules_on_exam != v.n_nodules_on_exam')}")

    rows = con.execute(f"""
        SELECT t.research_id, t.exam_date, t.n_nodules_on_exam AS t_n,
               v.n_nodules_on_exam AS v_n
        FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
          ON t.research_id IS NOT DISTINCT FROM v.research_id
         AND t.exam_date   IS NOT DISTINCT FROM v.exam_date
        WHERE t.n_nodules_on_exam IS NOT NULL
          AND v.n_nodules_on_exam IS NOT NULL
          AND t.n_nodules_on_exam != v.n_nodules_on_exam
    """).fetchall()
    print("    n_nodules_on_exam value changes (table -> view):")
    for r in rows:
        print(f"      rid={r[0]}  date={r[1]}  table_n={r[2]}  view_n={r[3]}")

    # ── 2. Three sanity queries on the 449 view-only keys ──────────────
    keys_cte = (
        f"WITH keys AS ("
        f"  SELECT v.research_id, v.exam_date FROM {CAND_EXAM} v "
        f"  LEFT JOIN {TBL_EXAM} t "
        f"    ON v.research_id IS NOT DISTINCT FROM t.research_id "
        f"   AND v.exam_date   IS NOT DISTINCT FROM t.exam_date "
        f"  WHERE t.research_id IS NULL"
        f")"
    )

    print("\n" + "=" * 72)
    print("Sanity query 1: NULL or nonsensical dates")
    print("=" * 72)
    r = con.execute(f"""
        {keys_cte}
        SELECT
          COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_dates,
          COUNT(*) FILTER (WHERE exam_date < '1990-01-01') AS before_1990,
          COUNT(*) FILTER (WHERE exam_date > CURRENT_DATE) AS future_dates,
          MIN(exam_date) AS earliest,
          MAX(exam_date) AS latest,
          COUNT(*) AS n_keys
        FROM keys
    """).fetchone()
    print(f"  null_dates={r[0]}  before_1990={r[1]}  future_dates={r[2]}")
    print(f"  earliest={r[3]}  latest={r[4]}  n_keys={r[5]}")

    print("\n" + "=" * 72)
    print("Sanity query 2: research_ids present in canonical_patient_master")
    print("=" * 72)
    r = con.execute(f"""
        {keys_cte}
        SELECT COUNT(*) AS orphan_research_id_keys,
               COUNT(DISTINCT k.research_id) AS orphan_distinct_rids
        FROM keys k
        LEFT JOIN {PUB}.main.canonical_patient_master p
          ON k.research_id = p.research_id
        WHERE p.research_id IS NULL
    """).fetchone()
    print(f"  orphan key rows   = {r[0]}")
    print(f"  orphan distinct rids = {r[1]}")

    print("\n" + "=" * 72)
    print("Sanity query 3: source attribution across the 3 child tables")
    print("=" * 72)
    r = con.execute(f"""
        {keys_cte}
        SELECT
          COUNT(*) FILTER (WHERE n.research_id IS NOT NULL) AS from_nodule_v2,
          COUNT(*) FILTER (WHERE g.research_id IS NOT NULL) AS from_gland_v2,
          COUNT(*) FILTER (WHERE l.research_id IS NOT NULL) AS from_lymph_node_v2,
          COUNT(*) AS n_keys
        FROM keys k
        LEFT JOIN {PUB}.main.canonical_us_nodule_v2 n
          ON n.research_id = k.research_id
         AND n.exam_date IS NOT DISTINCT FROM k.exam_date
         AND n.is_aggregate_row IS NOT TRUE
        LEFT JOIN {PUB}.main.canonical_us_thyroid_gland_v2 g
          ON g.research_id = k.research_id
         AND g.exam_date IS NOT DISTINCT FROM k.exam_date
        LEFT JOIN {PUB}.main.canonical_us_lymph_node_v2 l
          ON l.research_id = k.research_id
         AND l.exam_date IS NOT DISTINCT FROM k.exam_date
    """).fetchone()
    print(f"  from_nodule_v2     = {r[0]}")
    print(f"  from_gland_v2      = {r[1]}")
    print(f"  from_lymph_node_v2 = {r[2]}")
    print(f"  n_keys total       = {r[3]}")

    # Bonus: provenance via source_* flags on the new nodule rows
    print("\n  bonus: source_* attribution on the 449 nodule_v2 rows backing these keys")
    rows = con.execute(f"""
        {keys_cte}
        SELECT
          COUNT(*) AS n,
          COUNT(*) FILTER (WHERE n.source_base) AS src_base,
          COUNT(*) FILTER (WHERE n.source_tirads_v2) AS src_tirads_v2,
          COUNT(*) FILTER (WHERE n.source_tirads_llm) AS src_tirads_llm,
          COUNT(*) FILTER (WHERE n.source_dynamics_llm) AS src_dynamics_llm,
          COUNT(*) FILTER (WHERE n.source_us_nodules_tirads) AS src_us_nodules_tirads,
          COUNT(*) FILTER (WHERE n.source_fna_linkage) AS src_fna_linkage
        FROM keys k
        JOIN {PUB}.main.canonical_us_nodule_v2 n
          ON n.research_id = k.research_id
         AND n.exam_date IS NOT DISTINCT FROM k.exam_date
         AND n.is_aggregate_row IS NOT TRUE
    """).fetchone()
    cols = ["n", "src_base", "src_tirads_v2", "src_tirads_llm",
            "src_dynamics_llm", "src_us_nodules_tirads", "src_fna_linkage"]
    for c, v in zip(cols, rows):
        print(f"    {c:<25s} {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
