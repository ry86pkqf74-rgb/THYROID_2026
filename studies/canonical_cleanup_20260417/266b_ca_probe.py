"""c/a probe for 266b ETE vocabulary tightening.

Find tumors whose extrathyroidal_extension == 'c/a' and inspect the rest of
their synoptic_tumor_long_v1 row to determine whether 'c/a' means
capsular involvement (-> microscopic), complete absence (-> absent), or
ambiguous (-> keep unclassified).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore

OUT = Path(__file__).resolve().parent / "266b_ca_probe.json"


def main() -> int:
    con = connect_locked()
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' AND table_name='synoptic_tumor_long_v1' "
        "ORDER BY ordinal_position"
    ).fetchall()]
    print("synoptic_tumor_long_v1 columns:", cols)

    print()
    print("Total tumors with c/a:")
    n_total = con.execute(
        "SELECT COUNT(*) FROM main.synoptic_tumor_long_v1 "
        "WHERE LOWER(TRIM(extrathyroidal_extension)) = 'c/a'"
    ).fetchone()[0]
    print(f"  {n_total}")

    print()
    print("Pattern probes:")
    out = {"n_total_ca": n_total, "patterns": {}}
    out["patterns"]["capsular_invasion_dist"] = con.execute(
        "SELECT capsular_invasion, COUNT(*) FROM main.synoptic_tumor_long_v1 "
        "WHERE LOWER(TRIM(extrathyroidal_extension)) = 'c/a' "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    print("  capsular_invasion distribution:")
    for r in out["patterns"]["capsular_invasion_dist"]:
        print(f"    {r}")

    out["patterns"]["margin_status_dist"] = con.execute(
        "SELECT margin_status, COUNT(*) FROM main.synoptic_tumor_long_v1 "
        "WHERE LOWER(TRIM(extrathyroidal_extension)) = 'c/a' "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    print("  margin_status distribution:")
    for r in out["patterns"]["margin_status_dist"]:
        print(f"    {r}")

    # Sample 5 full rows
    print()
    print("Sample 5 rows (key cols):")
    sample_cols = ["research_id", "surg_date", "tumor_index",
                   "extrathyroidal_extension", "capsular_invasion",
                   "margin_status", "lymphatic_invasion", "perineural_invasion",
                   "histologic_type", "site"]
    sel = ", ".join(sample_cols)
    rows = con.execute(
        f"SELECT {sel} FROM main.synoptic_tumor_long_v1 "
        "WHERE LOWER(TRIM(extrathyroidal_extension)) = 'c/a' LIMIT 5"
    ).fetchall()
    for r in rows:
        print(" ", dict(zip(sample_cols, [str(x) if x is not None else None for x in r])))
    out["samples"] = [dict(zip(sample_cols, [str(x) if x is not None else None for x in r]))
                      for r in rows]

    # Cross-check: do these patients also appear in CPM with a particular
    # ete_grade_final_v2 value?
    print()
    out["cpm_grade_for_ca_patients"] = con.execute(
        """
        SELECT cpm.ete_grade_final_v2, COUNT(*) AS n
        FROM main.canonical_patient_master cpm
        JOIN (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.synoptic_tumor_long_v1
          WHERE LOWER(TRIM(extrathyroidal_extension)) = 'c/a'
        ) ca USING (research_id)
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print("CPM ete_grade_final_v2 for c/a patients:")
    for r in out["cpm_grade_for_ca_patients"]:
        print(f"  {r}")

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nWrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
