#!/usr/bin/env python3
"""
Script 230: Build patient_tumor_rollup_v1 from synoptic_tumor_long_v1

Fixes these bugs in canonical_patient_master_v221:
  - margin_status_final: 99% of mETE incorrectly labeled R1 (should be ~16%)
  - lvi_grade_final_v13: 92-95% collapsed to 'present_ungraded'
  - path_n_tumors, multifocal_flag, path_multifocal_flag: 100% NULL
  - Tumors 2-5 never rolled up to patient level
  - bilateral_disease_flag: only 37% populated
  - Angioinvasion not flowed to canonical master

Defensive guards:
  - USE thyroid_canonical_publication_v1_0 locks search path
  - All DDL/DML uses fully-qualified names
  - Row-count assertions at key checkpoints detect duplicate-listing bugs

Repository: https://github.com/ry86pkqf74-rgb/THYROID_2026
Database: thyroid_canonical_publication_v1_0 on MotherDuck

Usage:
    python scripts/230_path_synoptic_rollup.py
    python scripts/230_path_synoptic_rollup.py --validate-only  # no writes
"""
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, assert_row_count, assert_distinct_rids, PUBLICATION_DB

SQL_FILE = Path(__file__).parent / "230_path_synoptic_rollup.sql"
FQ = f"{PUBLICATION_DB}.main"


def run_validation(con):
    print("\n" + "="*72)
    print("PRE-FLIGHT VALIDATION")
    print("="*72)

    q = f"""
    SELECT
      COUNT(*) AS n_rows,
      COUNT(DISTINCT research_id) AS n_pts,
      MAX(tumor_index) AS max_tumors,
      COUNT(size_greatest_dimension_cm) AS n_with_size,
      COUNT(margin_status) AS n_with_margin,
      COUNT(extrathyroidal_extension) AS n_with_ete,
      COUNT(lymphatic_invasion) AS n_with_lvi,
      COUNT(angioinvasion) AS n_with_vi,
      COUNT(perineural_invasion) AS n_with_pni,
      COUNT(capsular_invasion) AS n_with_capsular
    FROM {FQ}.synoptic_tumor_long_v1
    """
    src = con.execute(q).df()
    print("\nSource (synoptic_tumor_long_v1):")
    for c in src.columns:
        print(f"  {c:<25} = {int(src[c].iloc[0]):,}")

    # Integrity: source should have 11,103 rows
    assert_row_count(con, f"{FQ}.synoptic_tumor_long_v1", 11103)

    # Current canonical bug evidence
    print("\nCurrent canonical_patient_master.margin_r_class (PTC ETE cohort — BUGGY):")
    q = f"""
    SELECT
      CASE ete_grade
        WHEN 'false' THEN 'No ETE' WHEN 'microscopic' THEN 'mETE'
        WHEN 'gross' THEN 'gETE' ELSE 'Present (ungraded)' END AS ete_grp,
      margin_r_class,
      COUNT(*) AS n
    FROM {FQ}.canonical_patient_master
    WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, n DESC
    """
    print(con.execute(q).df().to_string(index=False))


def build_rollup(con):
    print("\n" + "="*72)
    print("BUILDING patient_tumor_rollup_v1")
    print("="*72)
    sql = SQL_FILE.read_text()
    print(f"Executing {SQL_FILE.name} ({len(sql):,} chars)...")
    con.execute(sql)
    print("✓ Table created")


def post_validate(con):
    print("\n" + "="*72)
    print("POST-BUILD VALIDATION")
    print("="*72)

    # Row count + distinct-patient invariant
    n = assert_row_count(con, f"{FQ}.patient_tumor_rollup_v1", 8422, tolerance=200)
    assert_distinct_rids(con, f"{FQ}.patient_tumor_rollup_v1")
    print(f"✓ patient_tumor_rollup_v1: {n:,} rows, all distinct research_id")

    # Margin fix verification + row-count invariant
    print("\n" + "-"*72)
    print("MARGIN FIX VERIFICATION (PTC ETE cohort)")
    print("-"*72)
    q = f"""
    SELECT
      CASE cpm.ete_grade
        WHEN 'false' THEN 'No ETE' WHEN 'microscopic' THEN 'mETE'
        WHEN 'gross' THEN 'gETE' ELSE 'Present' END AS ete_grp,
      r.r_class_true,
      COUNT(*) AS n
    FROM {FQ}.canonical_patient_master cpm
    LEFT JOIN {FQ}.patient_tumor_rollup_v1 r USING (research_id)
    WHERE cpm.diagnosis_primary='PTC' AND cpm.ete_grade IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, n DESC
    """
    out = con.execute(q).df()
    print(out.to_string(index=False))

    # Critical: total must equal cohort size (3,254) — catches any duplicate join
    total = int(out["n"].sum())
    if total != 3254:
        raise SystemExit(
            f"INVARIANT FAILURE: PTC ETE cohort totals {total}, expected 3,254. "
            f"A duplicate join occurred."
        )
    print(f"\n✓ PTC ETE cohort row-count invariant: {total} = 3,254")

    # mETE should flip to majority R0
    mete = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE r.r_class_true='R0') AS r0,
          COUNT(*) FILTER (WHERE r.r_class_true='R1') AS r1,
          COUNT(*) AS total
        FROM {FQ}.canonical_patient_master cpm
        LEFT JOIN {FQ}.patient_tumor_rollup_v1 r USING (research_id)
        WHERE cpm.diagnosis_primary='PTC' AND cpm.ete_grade='microscopic'
    """).df().iloc[0]
    r0_pct = 100 * mete['r0'] / mete['total']
    print(f"\nmETE margin fix: R0={mete['r0']} ({r0_pct:.1f}%), R1={mete['r1']}, total={mete['total']}")
    if mete['total'] != 2934:
        raise SystemExit(f"mETE total {mete['total']} != 2934 — duplicate join")
    if r0_pct <= 50:
        raise SystemExit(f"mETE R0 rate {r0_pct:.1f}% <= 50% — fix incomplete")
    print("✓ mETE now correctly majority R0")

    # Multifocal consistency
    mf = con.execute(f"""
        SELECT
          SUM(CASE multifocal_flag_path WHEN TRUE THEN 1 ELSE 0 END) AS n_multifocal,
          SUM(CASE WHEN n_tumors_path > 1 THEN 1 ELSE 0 END) AS n_multi_by_count
        FROM {FQ}.patient_tumor_rollup_v1
    """).df().iloc[0]
    if mf['n_multifocal'] != mf['n_multi_by_count']:
        raise SystemExit(
            f"multifocal_flag_path ({mf['n_multifocal']}) inconsistent with "
            f"n_tumors_path>1 ({mf['n_multi_by_count']})"
        )
    if mf['n_multifocal'] < 1000:
        raise SystemExit(f"Only {mf['n_multifocal']} multifocal — too few")
    print(f"✓ Multifocal: {mf['n_multifocal']:,} patients, consistent with n_tumors_path>1")

    # Tumor count distribution
    print("\nTumor count distribution:")
    print(con.execute(f"""
        SELECT n_tumors_path, COUNT(*) AS n_patients
        FROM {FQ}.patient_tumor_rollup_v1 GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False))

    # LVI fix — 4-level signal recovered
    print("\n" + "-"*72)
    print("LVI DISTRIBUTION (FIXED — 4-level)")
    print("-"*72)
    print(con.execute(f"""
        SELECT
          lvi_ordinal_worst,
          CASE lvi_ordinal_worst
            WHEN 0 THEN 'absent' WHEN 1 THEN 'focal'
            WHEN 2 THEN 'present_ungraded' WHEN 3 THEN 'extensive'
            ELSE 'NULL/indet' END AS label,
          COUNT(*) AS n
        FROM {FQ}.patient_tumor_rollup_v1 GROUP BY 1, 2 ORDER BY 1
    """).df().to_string(index=False))

    print("\n✓ All post-build validations passed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate-only", action="store_true",
                    help="Run pre-flight validation only, no writes")
    args = ap.parse_args()

    print(f"Script 230: patient_tumor_rollup_v1 build")
    print(f"Database: {PUBLICATION_DB}")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

    con = connect_locked()
    run_validation(con)

    if args.validate_only:
        print("\n[validate-only] skipping build")
        return

    build_rollup(con)
    post_validate(con)

    print(f"\nFinished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    main()
