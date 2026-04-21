#!/usr/bin/env python3
"""Deeper inspection: how stale is the current exam_master table?"""
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

    print("=== exam_master populated-column counts ===")
    cols_to_check = [
        "exam_date", "us_exam_id", "n_nodules_on_exam", "largest_nodule_cm",
        "second_largest_nodule_cm", "bilateral_flag", "isthmus_nodule_flag",
        "worst_tirads_category_this_exam", "worst_tirads_points_this_exam",
        "best_tirads_category_this_exam", "count_tr1", "count_tr2",
        "count_tr3", "count_tr4", "count_tr5", "has_gland_findings",
        "has_us_ln_findings", "n_us_ln_total_on_exam", "n_abnormal_us_ln_on_exam",
        "exam_rank_for_patient", "is_preop_exam", "any_nlp_backfill_pending_on_exam",
    ]
    for c in cols_to_check:
        r = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {c} IS NOT NULL) AS not_null, "
            f"       COUNT(*) FILTER (WHERE {c} IS NULL)     AS is_null "
            f"FROM main.canonical_us_exam_master_VIEW_v2"
        ).fetchone()
        print(f"  {c:<40s} not_null={r[0]:>7d}  null={r[1]:>7d}")

    print("\n=== exam_master rows with NULL exam_date ===")
    n = con.execute(
        "SELECT COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2 "
        "WHERE exam_date IS NULL"
    ).fetchone()[0]
    print(f"  rows with exam_date IS NULL: {n}")

    print("\n=== exam_master sample non-null TIRADS rows ===")
    rows = con.execute(
        "SELECT research_id, exam_date, n_nodules_on_exam, "
        "  worst_tirads_category_this_exam, best_tirads_category_this_exam, "
        "  count_tr1, count_tr2, count_tr3, count_tr4, count_tr5 "
        "FROM main.canonical_us_exam_master_VIEW_v2 "
        "WHERE worst_tirads_category_this_exam IS NOT NULL "
        "LIMIT 5"
    ).fetchall()
    for r in rows:
        print(f"  {r}")

    print("\n=== gland_v2 rows with NULL exam_date ===")
    n = con.execute(
        "SELECT COUNT(*) FROM main.canonical_us_thyroid_gland_v2 "
        "WHERE exam_date IS NULL"
    ).fetchone()[0]
    print(f"  rows with exam_date IS NULL: {n}")

    print("\n=== nodule_v2 rows with NULL exam_date ===")
    n = con.execute(
        "SELECT COUNT(*) FROM main.canonical_us_nodule_v2 "
        "WHERE exam_date IS NULL"
    ).fetchone()[0]
    print(f"  rows with exam_date IS NULL: {n}")

    print("\n=== nodule_v2 distinct (research_id, exam_date) pairs (excl. aggregate) ===")
    n = con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT research_id, exam_date "
        "FROM main.canonical_us_nodule_v2 WHERE is_aggregate_row IS NOT TRUE)"
    ).fetchone()[0]
    print(f"  distinct exam keys with non-aggregate nodules: {n}")

    print("\n=== Worst TIRADS categories distribution in exam_master ===")
    rows = con.execute(
        "SELECT worst_tirads_category_this_exam, COUNT(*) "
        "FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r}")

    print("\n=== n_nodules_on_exam distribution ===")
    rows = con.execute(
        "SELECT n_nodules_on_exam IS NOT NULL AS populated, "
        "       COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2 GROUP BY 1"
    ).fetchall()
    for r in rows:
        print(f"  {r}")

    print("\n=== patient_master populated-column counts ===")
    cols_to_check = [
        "n_us_exams", "first_us_date", "last_us_date", "preop_us_available_flag",
        "max_tirads_category_ever", "max_tirads_points_ever",
        "tirads_category_at_first_exam", "tirads_category_at_last_preop_exam",
        "n_nodules_total_across_exams", "bilateral_disease_flag_ever",
        "multifocal_flag_ever", "first_high_risk_tirads_date",
        "has_us_ln_findings_ever", "any_suspicious_us_ln_ever",
        "first_abnormal_us_ln_date", "has_gland_findings_ever",
        "any_nlp_backfill_pending_for_patient",
    ]
    for c in cols_to_check:
        r = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {c} IS NOT NULL) AS not_null, "
            f"       COUNT(*) FILTER (WHERE {c} IS NULL)     AS is_null "
            f"FROM main.canonical_us_patient_master_VIEW_v2"
        ).fetchone()
        print(f"  {c:<40s} not_null={r[0]:>7d}  null={r[1]:>7d}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
