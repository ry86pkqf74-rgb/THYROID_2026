#!/usr/bin/env python3
"""
02_build_duckdb_full.py — Register Parquet tables + create analytic views

Thyroid Cancer Research Lakehouse
Creates: master_cohort (one row per patient, data-availability flags),
         lab_timeline, imaging_timeline views.
"""

import duckdb
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

TABLES = [
    "thyroid_sizes",
    "tumor_pathology",
    "benign_pathology",
    "thyroid_weights",
    "fna_cytology",
    "frozen_sections",
    "ultrasound_reports",
    "ct_imaging",
    "mri_imaging",
    "nuclear_med",
    "thyroglobulin_labs",
    "anti_thyroglobulin_labs",
    "parathyroid",
    "clinical_notes_long",
]


def main() -> None:
    print("=" * 70)
    print("  THYROID RESEARCH LAKEHOUSE — STEP 2: BUILD DUCKDB")
    print("=" * 70)

    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    # ── Register every Parquet as a table ─────────────────────────
    registered: list[str] = []
    for tbl in TABLES:
        pq = PROCESSED / f"{tbl}.parquet"
        if not pq.exists():
            print(f"  ⚠️  Skipping {tbl} — parquet not found")
            continue
        con.execute(f"CREATE TABLE {tbl} AS SELECT * FROM read_parquet('{pq}')")
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        n_id = con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {tbl}"
        ).fetchone()[0]
        cols = len(con.execute(f"DESCRIBE {tbl}").fetchall())
        print(f"  ✅  {tbl:35s}  {cnt:>8,} rows  {n_id:>6,} patients  {cols:>3} cols")
        registered.append(tbl)

    if not registered:
        print("  ❌  No tables registered — nothing to build.")
        sys.exit(1)

    # ── master_cohort view ───────────────────────────────────────
    print(f"\n{'─' * 70}")
    print("  Creating views …")

    union_parts = "\n    UNION\n    ".join(
        f"SELECT DISTINCT research_id FROM {t}" for t in registered
    )

    has_flags = ",\n".join(
        f"    CASE WHEN _{t}.rid IS NOT NULL THEN TRUE ELSE FALSE END AS has_{t}"
        for t in registered
    )

    left_joins = "\n".join(
        f"    LEFT JOIN (SELECT DISTINCT research_id AS rid FROM {t}) _{t}"
        f" ON _all.research_id = _{t}.rid"
        for t in registered
    )

    demo_cte = ""
    demo_cols = ""
    demo_join = ""
    tp_ok = "tumor_pathology" in registered
    bp_ok = "benign_pathology" in registered
    if tp_ok or bp_ok:
        sources = []
        if tp_ok:
            sources.append(
                "SELECT research_id, age_at_surgery, sex, surgery_date "
                "FROM tumor_pathology"
            )
        if bp_ok:
            sources.append(
                "SELECT research_id, age_at_surgery, sex, surgery_date "
                "FROM benign_pathology"
            )
        demo_union = "\n        UNION ALL\n        ".join(sources)
        demo_cte = f""",
    _demo AS (
        SELECT DISTINCT ON (research_id)
            research_id AS rid, age_at_surgery, sex, surgery_date
        FROM (
            {demo_union}
        ) _du
        ORDER BY research_id, surgery_date DESC NULLS LAST
    )"""
        demo_cols = ",\n    _demo.age_at_surgery,\n    _demo.sex,\n    _demo.surgery_date"
        demo_join = "    LEFT JOIN _demo ON _all.research_id = _demo.rid"

    master_sql = f"""
CREATE OR REPLACE VIEW master_cohort AS
WITH _ids AS (
    {union_parts}
){demo_cte}
SELECT
    _all.research_id{demo_cols},
{has_flags}
FROM _ids _all
{demo_join}
{left_joins}
WHERE _all.research_id IS NOT NULL
  AND _all.research_id != ''
ORDER BY TRY_CAST(_all.research_id AS INTEGER), _all.research_id
"""

    try:
        con.execute(master_sql)
        cnt = con.execute("SELECT COUNT(*) FROM master_cohort").fetchone()[0]
        print(f"  ✅  master_cohort          {cnt:>8,} unique patients")
    except Exception as exc:
        print(f"  ❌  master_cohort failed: {exc}")

    con.close()
    print(f"\n{'=' * 70}")
    print("  DUCKDB BUILD COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
