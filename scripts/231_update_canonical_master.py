#!/usr/bin/env python3
"""
Script 231: Update canonical_patient_master with fixed pathology rollup columns

Prerequisites: Script 230 must have been run first (patient_tumor_rollup_v1 exists).

What this does:
  1. Backs up current canonical_patient_master to _v221_backup (idempotent)
  2. Creates canonical_patient_master_v222 with v221 + rollup fixes joined
  3. Swaps the `canonical_patient_master` alias to v222
  4. Keeps v221 for audit trail

Defensive guards:
  - USE statement locks search path
  - Fully qualified table refs everywhere
  - Row-count invariants before, during, after

Repository: https://github.com/ry86pkqf74-rgb/THYROID_2026
Database: thyroid_canonical_publication_v1_0 on MotherDuck

Usage:
    python scripts/231_update_canonical_master.py
    python scripts/231_update_canonical_master.py --dry-run
"""
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, assert_row_count, assert_distinct_rids, PUBLICATION_DB

SQL_FILE = Path(__file__).parent / "231_update_canonical_master.sql"
FQ = f"{PUBLICATION_DB}.main"


def check_prereq(con):
    print("\nChecking prerequisites...")
    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='main' AND table_name='patient_tumor_rollup_v1'
    """).fetchone()[0]
    if exists == 0:
        raise SystemExit("patient_tumor_rollup_v1 does not exist. Run script 230 first.")

    # Assert rollup integrity
    rows = assert_row_count(con, f"{FQ}.patient_tumor_rollup_v1", 8422, tolerance=200)
    assert_distinct_rids(con, f"{FQ}.patient_tumor_rollup_v1")
    print(f"  ✓ patient_tumor_rollup_v1: {rows:,} rows, all distinct research_id")

    cpm_rows = assert_row_count(con, f"{FQ}.canonical_patient_master", 10871)
    assert_distinct_rids(con, f"{FQ}.canonical_patient_master")
    print(f"  ✓ canonical_patient_master: {cpm_rows:,} rows, all distinct research_id")


def dry_run_validation(con):
    print("\n" + "="*72)
    print("DRY-RUN: What would change")
    print("="*72)

    # Before
    print("\nBEFORE — Current canonical margin_r_class (PTC ETE cohort):")
    q = f"""
    SELECT
      CASE ete_grade
        WHEN 'false' THEN 'No ETE' WHEN 'microscopic' THEN 'mETE'
        WHEN 'gross' THEN 'gETE' ELSE 'Present' END AS ete_grp,
      margin_r_class,
      COUNT(*) AS n
    FROM {FQ}.canonical_patient_master
    WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1, n DESC
    """
    print(con.execute(q).df().to_string(index=False))

    # After (simulated via the join)
    print("\nAFTER (simulated) — r_class_true from patient_tumor_rollup_v1:")
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
    GROUP BY 1, 2 ORDER BY 1, n DESC
    """
    after = con.execute(q).df()
    print(after.to_string(index=False))

    # Invariant: cohort total stays at 3,254
    total = int(after['n'].sum())
    if total != 3254:
        raise SystemExit(
            f"DRY-RUN INVARIANT FAILURE: cohort total {total} != 3,254. "
            f"Duplicate join detected. Aborting before writes."
        )
    print(f"✓ Dry-run cohort total invariant: {total} = 3,254")

    # Show LVI fix
    print("\nBEFORE — Current lvi_grade_final_v13:")
    print(con.execute(f"""
        SELECT lvi_grade_final_v13, COUNT(*) AS n
        FROM {FQ}.canonical_patient_master
        WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
    """).df().to_string(index=False))

    print("\nAFTER — lvi_ordinal_worst (0=absent 1=focal 2=present 3=extensive):")
    print(con.execute(f"""
        SELECT r.lvi_ordinal_worst, COUNT(*) AS n
        FROM {FQ}.canonical_patient_master cpm
        LEFT JOIN {FQ}.patient_tumor_rollup_v1 r USING (research_id)
        WHERE cpm.diagnosis_primary='PTC' AND cpm.ete_grade IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False))

    # Multifocal fix
    print("\nBEFORE — multifocal_flag / path_multifocal_flag / path_n_tumors coverage:")
    print(con.execute(f"""
        SELECT
          COUNT_IF(multifocal_flag IS NOT NULL) AS mult_flag_nn,
          COUNT_IF(path_multifocal_flag IS NOT NULL) AS path_mult_nn,
          COUNT_IF(path_n_tumors IS NOT NULL) AS path_n_tum_nn
        FROM {FQ}.canonical_patient_master
    """).df().to_string(index=False))

    print("\nAFTER — multifocal_flag_path, n_tumors_path from rollup:")
    print(con.execute(f"""
        SELECT
          COUNT_IF(r.multifocal_flag_path IS NOT NULL) AS mfp_nn,
          COUNT_IF(r.n_tumors_path IS NOT NULL) AS ntp_nn,
          SUM(CAST(r.multifocal_flag_path AS INTEGER)) AS n_multifocal
        FROM {FQ}.canonical_patient_master cpm
        LEFT JOIN {FQ}.patient_tumor_rollup_v1 r USING (research_id)
    """).df().to_string(index=False))


def run_update(con):
    print("\n" + "="*72)
    print("RUNNING UPDATE")
    print("="*72)
    sql = SQL_FILE.read_text()
    # Strip comment-only lines before splitting on ";" so that header comment
    # blocks don't get merged with the first SQL statement (which would cause
    # the segment to be filtered as a comment).
    cleaned_lines = [ln for ln in sql.split("\n") if not ln.strip().startswith("--")]
    cleaned_sql = "\n".join(cleaned_lines)
    statements = [s.strip() for s in cleaned_sql.split(";") if s.strip()]
    for i, stmt in enumerate(statements, 1):
        preview = stmt[:80].replace("\n", " ")
        print(f"\n[{i}/{len(statements)}] {preview}...")
        con.execute(stmt)
        print("  ✓")
    print("\n✓ All statements executed")


def post_validation(con):
    print("\n" + "="*72)
    print("POST-UPDATE VALIDATION")
    print("="*72)

    # Row count preserved
    assert_row_count(con, f"{FQ}.canonical_patient_master", 10871)
    assert_distinct_rids(con, f"{FQ}.canonical_patient_master")
    print("✓ canonical_patient_master: 10,871 rows, all distinct research_id")

    # New columns present
    new_cols = [
        "tumor_size_cm_dominant", "tumor_size_cm_max", "n_tumors_path",
        "multifocal_flag_path", "margin_involved_any", "r_class_true",
        "margin_status_true", "lvi_any_present_path", "lvi_ordinal_worst",
        "vi_any_present_path", "pni_any_present_path", "capsular_any_present_path"
    ]
    for c in new_cols:
        r = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema='main' AND table_name='canonical_patient_master'
              AND column_name='{c}'
        """).fetchone()[0]
        if r != 1:
            raise SystemExit(f"Column {c} missing or duplicated ({r} matches)")
    print(f"✓ All {len(new_cols)} new columns present with exactly 1 definition each")

    # Margin sanity check
    print("\nNEW margin distribution (PTC ETE cohort):")
    out = con.execute(f"""
        SELECT
          CASE ete_grade
            WHEN 'false' THEN 'No ETE' WHEN 'microscopic' THEN 'mETE'
            WHEN 'gross' THEN 'gETE' ELSE 'Present' END AS ete_grp,
          r_class_true,
          COUNT(*) AS n
        FROM {FQ}.canonical_patient_master
        WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, n DESC
    """).df()
    print(out.to_string(index=False))

    # Cohort total invariant
    total = int(out['n'].sum())
    if total != 3254:
        raise SystemExit(
            f"POST-UPDATE INVARIANT FAILURE: cohort total {total} != 3,254"
        )
    print(f"✓ PTC ETE cohort: {total} rows (matches expected 3,254)")

    # mETE R0 majority
    mete = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE r_class_true='R0') AS r0,
          COUNT(*) FILTER (WHERE r_class_true='R1') AS r1,
          COUNT(*) FILTER (WHERE r_class_true='Rx') AS rx,
          COUNT(*) FILTER (WHERE r_class_true IS NULL) AS unk,
          COUNT(*) AS total
        FROM {FQ}.canonical_patient_master
        WHERE diagnosis_primary='PTC' AND ete_grade='microscopic'
    """).df().iloc[0]
    r0_pct = 100 * mete['r0'] / mete['total']
    print(f"\nmETE: R0={mete['r0']} ({r0_pct:.1f}%), R1={mete['r1']}, Rx={mete['rx']}, unk={mete['unk']}, total={mete['total']}")
    if mete['total'] != 2934:
        raise SystemExit(f"mETE total {mete['total']} != 2934")
    if r0_pct <= 50:
        raise SystemExit(f"mETE R0 rate {r0_pct:.1f}% <= 50% — fix incomplete")
    print("✓ mETE now correctly majority R0")

    # v221 backup preserved
    assert_row_count(con, f"{FQ}.canonical_patient_master_v221", 10871)
    assert_row_count(con, f"{FQ}.canonical_patient_master_v221_backup", 10871)
    print("✓ v221 backup preserved: 10,871 rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Show what would change, don't run")
    args = ap.parse_args()

    print("Script 231: canonical_patient_master update")
    print(f"Database: {PUBLICATION_DB}")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

    con = connect_locked()
    check_prereq(con)
    dry_run_validation(con)

    if args.dry_run:
        print("\n[--dry-run] skipping write")
        return

    print("\n" + "="*72)
    ans = input("PROCEED with update? Type 'yes' to confirm: ").strip().lower()
    if ans != "yes":
        print("Aborted.")
        return

    run_update(con)
    post_validation(con)
    print(f"\nFinished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    main()
