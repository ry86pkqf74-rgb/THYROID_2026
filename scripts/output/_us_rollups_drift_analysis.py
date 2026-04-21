#!/usr/bin/env python3
"""Categorize the parity drift between live table and candidate view.

Answers:
  1. What are the 449 keys in view but not in table? (newer data added since last 366 run?)
  2. Of the 6,967 keys present in BOTH, how many differ ONLY in TIRADS-derived columns
     (acceptable per prompt) vs ALSO in non-TIRADS columns (needs human call)?
  3. exam_rank drift — is it purely a side-effect of the new keys, or also an order change?
  4. Patient master: same per-column breakdown over the FULL 3,661 differing patients.
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
TBL_EXAM = f"{PUB}.main.canonical_us_exam_master_VIEW_v2"
TBL_PT = f"{PUB}.main.canonical_us_patient_master_VIEW_v2"
CAND_EXAM = f"{PUB}.manuscript_workspace.candidate_us_exam_master_v2"
CAND_PT = f"{PUB}.manuscript_workspace.candidate_us_patient_master_v2"


def main() -> int:
    con = connect_locked()

    # ── 1. 449 view-only exam keys ──────────────────────────────────────
    print("=" * 60)
    print("1. View-only exam keys (449)")
    print("=" * 60)

    rows = con.execute(f"""
        SELECT v.research_id, v.exam_date, v.n_nodules_on_exam,
               v.has_gland_findings, v.has_us_ln_findings
        FROM {CAND_EXAM} v
        LEFT JOIN {TBL_EXAM} t
          ON v.research_id IS NOT DISTINCT FROM t.research_id
         AND v.exam_date   IS NOT DISTINCT FROM t.exam_date
        WHERE t.research_id IS NULL
        LIMIT 12
    """).fetchall()
    print("  sample of 12:")
    for r in rows:
        print(f"    rid={r[0]} date={r[1]} n_nod={r[2]} has_gland={r[3]} has_ln={r[4]}")

    print("\n  origin breakdown:")
    rows = con.execute(f"""
        SELECT
          BOOL_OR(n.research_id IS NOT NULL) AS in_nodule,
          BOOL_OR(g.research_id IS NOT NULL) AS in_gland,
          BOOL_OR(l.research_id IS NOT NULL) AS in_ln,
          COUNT(*)                            AS n_keys
        FROM (
          SELECT v.research_id, v.exam_date FROM {CAND_EXAM} v
          LEFT JOIN {TBL_EXAM} t
            ON v.research_id IS NOT DISTINCT FROM t.research_id
           AND v.exam_date   IS NOT DISTINCT FROM t.exam_date
          WHERE t.research_id IS NULL
        ) miss
        LEFT JOIN main.canonical_us_nodule_v2 n
          ON n.research_id = miss.research_id AND n.exam_date IS NOT DISTINCT FROM miss.exam_date
          AND n.is_aggregate_row IS NOT TRUE
        LEFT JOIN main.canonical_us_thyroid_gland_v2 g
          ON g.research_id = miss.research_id AND g.exam_date IS NOT DISTINCT FROM miss.exam_date
        LEFT JOIN main.canonical_us_lymph_node_v2 l
          ON l.research_id = miss.research_id AND l.exam_date IS NOT DISTINCT FROM miss.exam_date
        GROUP BY miss.research_id, miss.exam_date
    """).fetchall()
    src = {"nodule_only": 0, "gland_only": 0, "ln_only": 0, "multi": 0, "none": 0}
    for in_n, in_g, in_l, _ in rows:
        s = sum([bool(in_n), bool(in_g), bool(in_l)])
        if s == 0:
            src["none"] += 1
        elif s > 1:
            src["multi"] += 1
        elif in_n:
            src["nodule_only"] += 1
        elif in_g:
            src["gland_only"] += 1
        elif in_l:
            src["ln_only"] += 1
    print(f"    by source presence: {src}  (total={sum(src.values())})")

    # ── 2. Common-key per-row diff classification ──────────────────────
    print("\n" + "=" * 60)
    print("2. Common-key per-row diff classification (exam_master)")
    print("=" * 60)

    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = 'main' "
        "AND table_name = 'canonical_us_exam_master_VIEW_v2' "
        "ORDER BY ordinal_position",
        [PUB],
    ).fetchall()]
    key_cols = ["research_id", "exam_date"]
    non_key = [c for c in cols if c not in key_cols]

    tirads_cols = {
        "worst_tirads_category_this_exam",
        "worst_tirads_points_this_exam",
        "best_tirads_category_this_exam",
        "count_tr1", "count_tr2", "count_tr3", "count_tr4", "count_tr5",
    }
    rank_cols = {"exam_rank_for_patient"}
    other_cols = [c for c in non_key if c not in tirads_cols and c not in rank_cols]

    on = " AND ".join([f't."{k}" IS NOT DISTINCT FROM v."{k}"' for k in key_cols])

    def diff_count(col_subset: list[str]) -> int:
        if not col_subset:
            return 0
        pred = " OR ".join(
            [f't."{c}" IS DISTINCT FROM v."{c}"' for c in col_subset]
        )
        return con.execute(
            f"SELECT COUNT(*) FROM {TBL_EXAM} t JOIN {CAND_EXAM} v "
            f"ON {on} WHERE {pred}"
        ).fetchone()[0]

    print(f"  rows with TIRADS-only column diffs: "
          f"{diff_count(list(tirads_cols))}")
    print(f"  rows with rank-only column diffs:   "
          f"{diff_count(list(rank_cols))}")
    print(f"  rows with non-TIRADS, non-rank diffs: "
          f"{diff_count(other_cols)}")

    print("\n  per-non-TIRADS-column diff counts (those with any diff):")
    for c in other_cols:
        n = diff_count([c])
        if n > 0:
            print(f"    {c:<40s} {n}")

    # ── 3. exam_rank drift origin ──────────────────────────────────────
    print("\n" + "=" * 60)
    print("3. exam_rank_for_patient drift")
    print("=" * 60)
    rank_diff = con.execute(f"""
        SELECT COUNT(*) FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
        ON {on}
        WHERE t.exam_rank_for_patient IS DISTINCT FROM v.exam_rank_for_patient
    """).fetchone()[0]
    rank_diff_pts = con.execute(f"""
        SELECT COUNT(DISTINCT t.research_id) FROM {TBL_EXAM} t JOIN {CAND_EXAM} v
        ON {on}
        WHERE t.exam_rank_for_patient IS DISTINCT FROM v.exam_rank_for_patient
    """).fetchone()[0]
    print(f"  rows with rank diff:    {rank_diff}")
    print(f"  patients with rank diff: {rank_diff_pts}")

    rank_diff_in_view_only_pts = con.execute(f"""
        SELECT COUNT(DISTINCT v.research_id) FROM {CAND_EXAM} v
        LEFT JOIN {TBL_EXAM} t ON {on}
        WHERE t.research_id IS NULL
    """).fetchone()[0]
    print(f"  patients touched by view-only keys: {rank_diff_in_view_only_pts}")

    # ── 4. Patient master full per-column diff ─────────────────────────
    print("\n" + "=" * 60)
    print("4. Patient master per-column diff counts (all 3,661 differing)")
    print("=" * 60)
    pt_cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = 'main' "
        "AND table_name = 'canonical_us_patient_master_VIEW_v2' "
        "ORDER BY ordinal_position",
        [PUB],
    ).fetchall()]
    pt_non_key = [c for c in pt_cols if c != "research_id"]
    on_pt = "t.research_id IS NOT DISTINCT FROM v.research_id"
    for c in pt_non_key:
        n = con.execute(
            f"SELECT COUNT(*) FROM {TBL_PT} t JOIN {CAND_PT} v ON {on_pt} "
            f"WHERE t.\"{c}\" IS DISTINCT FROM v.\"{c}\""
        ).fetchone()[0]
        if n > 0:
            print(f"  {c:<45s} {n}")

    print("\n" + "=" * 60)
    print("5. Net effect on TIRADS coverage")
    print("=" * 60)
    print("  exam_master.worst_tirads_category_this_exam:")
    n_t = con.execute(
        f"SELECT COUNT(*) FROM {TBL_EXAM} "
        f"WHERE worst_tirads_category_this_exam IS NOT NULL"
    ).fetchone()[0]
    n_v = con.execute(
        f"SELECT COUNT(*) FROM {CAND_EXAM} "
        f"WHERE worst_tirads_category_this_exam IS NOT NULL"
    ).fetchone()[0]
    print(f"    table populated: {n_t:>6d}")
    print(f"    view  populated: {n_v:>6d}")
    print(f"    delta:           {n_v - n_t:+d}")

    print("  patient_master.max_tirads_category_ever:")
    n_t = con.execute(
        f"SELECT COUNT(*) FROM {TBL_PT} WHERE max_tirads_category_ever IS NOT NULL"
    ).fetchone()[0]
    n_v = con.execute(
        f"SELECT COUNT(*) FROM {CAND_PT} WHERE max_tirads_category_ever IS NOT NULL"
    ).fetchone()[0]
    print(f"    table populated: {n_t:>6d}")
    print(f"    view  populated: {n_v:>6d}")
    print(f"    delta:           {n_v - n_t:+d}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
