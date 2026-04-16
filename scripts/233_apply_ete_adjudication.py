#!/usr/bin/env python3
"""
Script 233: Apply reviewed ETE adjudications to canonical_patient_master

After script 232 produces ete_adjudication_v1, a human reviewer should inspect
the adjudications (particularly low-confidence ones) and either accept them or
override them. This script applies the adjudicated_grade to canonical_patient_master
for patients where ete_grade was 'present_ungraded' or 'true'.

The update:
  - Creates a new column `ete_grade_adjudicated` with the adjudicated grade
  - Creates a new column `ete_grade_final_v2` which is:
      ete_grade_adjudicated if original was 'present_ungraded' or 'true'
      ete_grade            otherwise
  - Creates `ete_adjudication_confidence` and `ete_adjudication_evidence`
    columns for provenance

Defensive guards: locked search path + fully qualified refs + row-count invariants.

Usage:
    python scripts/233_apply_ete_adjudication.py
    python scripts/233_apply_ete_adjudication.py --min-confidence high
    python scripts/233_apply_ete_adjudication.py --dry-run
"""
import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, assert_row_count, assert_distinct_rids, PUBLICATION_DB

FQ = f"{PUBLICATION_DB}.main"


def apply_adjudications(con, min_confidence="medium", dry_run=False):
    conf_map = {
        "low": ["low", "medium", "high"],
        "medium": ["medium", "high"],
        "high": ["high"],
    }
    allowed = conf_map[min_confidence]
    in_list = ",".join(f"'{c}'" for c in allowed)

    # Show planned application
    q = f"""
    SELECT adjudicated_grade, adjudicated_confidence, COUNT(*) AS n
    FROM {FQ}.ete_adjudication_v1
    WHERE adjudicated_confidence IN ({in_list})
      AND adjudicated_grade IN ('microscopic','gross','absent')
    GROUP BY 1, 2 ORDER BY 1, 2
    """
    plan = con.execute(q).df()
    print(f"Adjudications that will be applied (min_confidence={min_confidence}):")
    print(plan.to_string(index=False))
    total = int(plan["n"].sum()) if len(plan) else 0
    print(f"\nTotal patients affected: {total}")

    if dry_run:
        print("\n[--dry-run] no writes")
        return

    if total == 0:
        print("Nothing to apply.")
        return

    print("\nBuilding canonical_patient_master_v223 with ete_grade_final_v2 column...")
    sql_statements = [
        # Backup v222 snapshot
        f"""CREATE TABLE IF NOT EXISTS {FQ}.canonical_patient_master_v222_backup AS
            SELECT * FROM {FQ}.canonical_patient_master""",

        # Drop previous v223 if exists
        f"DROP TABLE IF EXISTS {FQ}.canonical_patient_master_v223",

        # Build v223
        f"""CREATE TABLE {FQ}.canonical_patient_master_v223 AS
            SELECT
              cpm.*,
              e.adjudicated_grade AS ete_grade_adjudicated,
              e.adjudicated_confidence AS ete_adjudication_confidence,
              e.evidence_quote AS ete_adjudication_evidence,
              e.reasoning AS ete_adjudication_reasoning,
              e.ajcc8_t_adjustment AS ete_adjudication_t_adjustment,
              CASE
                WHEN cpm.ete_grade IN ('present_ungraded','true')
                     AND e.adjudicated_grade IN ('microscopic','gross','absent')
                     AND e.adjudicated_confidence IN ({in_list})
                THEN e.adjudicated_grade
                ELSE cpm.ete_grade
              END AS ete_grade_final_v2
            FROM {FQ}.canonical_patient_master cpm
            LEFT JOIN {FQ}.ete_adjudication_v1 e USING (research_id)""",

        # Drop old v222 in main schema, replace with a clean copy of the backup
        f"DROP TABLE IF EXISTS {FQ}.canonical_patient_master_v222",
        f"""CREATE TABLE {FQ}.canonical_patient_master_v222 AS
            SELECT * FROM {FQ}.canonical_patient_master_v222_backup""",

        # Swap alias to v223
        f"DROP TABLE {FQ}.canonical_patient_master",
        f"""CREATE TABLE {FQ}.canonical_patient_master AS
            SELECT * FROM {FQ}.canonical_patient_master_v223""",

        # Update comment
        f"""COMMENT ON TABLE {FQ}.canonical_patient_master IS
            'Master analytical table v223. Built from v222 + ete_adjudication_v1. '
            'New column ete_grade_final_v2 = ete_grade with present_ungraded/true cases '
            'replaced by Claude Haiku 4.5 adjudication (min_confidence={min_confidence}). '
            'Original ete_grade column preserved unchanged for audit.'""",
    ]

    for i, stmt in enumerate(sql_statements, 1):
        preview = " ".join(stmt.split())[:80]
        print(f"  [{i}/{len(sql_statements)}] {preview}...")
        con.execute(stmt)

    # Post-validation
    assert_row_count(con, f"{FQ}.canonical_patient_master", 10871)
    assert_distinct_rids(con, f"{FQ}.canonical_patient_master")

    new_dist = con.execute(f"""
        SELECT ete_grade_final_v2, COUNT(*) AS n
        FROM {FQ}.canonical_patient_master
        WHERE diagnosis_primary='PTC' AND ete_grade_final_v2 IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
    """).df()
    print("\nNew ete_grade_final_v2 distribution (PTC):")
    print(new_dist.to_string(index=False))

    # Invariant: total should still be 3,254 (PTC with some ETE data)
    total_ptc = int(new_dist['n'].sum())
    total_ptc_canonical = con.execute(f"""
        SELECT COUNT(*) FROM {FQ}.canonical_patient_master
        WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
    """).fetchone()[0]
    if total_ptc != total_ptc_canonical:
        raise SystemExit(
            f"ete_grade_final_v2 populated for {total_ptc} PTC patients, "
            f"but ete_grade is populated for {total_ptc_canonical}. Mismatch."
        )
    print(f"✓ ete_grade_final_v2 population matches ete_grade: {total_ptc}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-confidence", choices=["low", "medium", "high"], default="medium")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"Script 233: Apply ETE adjudications")
    print(f"Database: {PUBLICATION_DB}")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

    con = connect_locked()

    # Prerequisite checks
    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='ete_adjudication_v1'
    """).fetchone()[0]
    if exists == 0:
        raise SystemExit("ete_adjudication_v1 does not exist. Run script 232 first.")

    apply_adjudications(con, args.min_confidence, args.dry_run)
    print(f"\nFinished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    main()
