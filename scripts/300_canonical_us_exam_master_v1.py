"""
Script 300 — Build canonical_us_exam_master_v1 (per-exam rollup).

Rolls canonical_us_nodule_master_v1 (Script 299) up to the exam grain.

Usage:
    python 300_canonical_us_exam_master_v1.py            # dry-run
    python 300_canonical_us_exam_master_v1.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "300_canonical_us_exam_master_v1"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


EXAM_SQL = """
CREATE OR REPLACE TABLE main.canonical_us_exam_master_v1 AS
WITH nodules AS (
    SELECT * FROM main.canonical_us_nodule_master_v1
),
exam_agg AS (
    SELECT
        research_id,
        exam_date,
        COUNT(*) AS n_nodules_on_exam,
        MAX(size_cm) AS largest_nodule_cm,
        -- second largest
        (SELECT MAX(n2.size_cm) FROM main.canonical_us_nodule_master_v1 n2
         WHERE n2.research_id = n.research_id AND n2.exam_date = n.exam_date
           AND n2.size_cm < MAX(n.size_cm)) AS second_largest_nodule_cm,
        -- bilateral
        CASE WHEN COUNT(DISTINCT CASE WHEN laterality IN ('right','left')
                        THEN laterality END) >= 2 THEN TRUE ELSE FALSE END
            AS bilateral_flag,
        BOOL_OR(CASE WHEN LOWER(COALESCE(laterality,'')) LIKE '%isthmus%'
                THEN TRUE ELSE FALSE END) AS isthmus_nodule_flag,
        -- worst TIRADS
        MAX(tirads_category_v2) AS worst_tirads_category_this_exam,
        MAX(tirads_points_total) AS worst_tirads_points_this_exam,
        -- TIRADS category counts
        SUM(CASE WHEN tirads_category_v2 = 'TR5' THEN 1 ELSE 0 END) AS count_tr5,
        SUM(CASE WHEN tirads_category_v2 = 'TR4' THEN 1 ELSE 0 END) AS count_tr4,
        SUM(CASE WHEN tirads_category_v2 = 'TR3' THEN 1 ELSE 0 END) AS count_tr3,
        SUM(CASE WHEN tirads_category_v2 = 'TR2' THEN 1 ELSE 0 END) AS count_tr2,
        SUM(CASE WHEN tirads_category_v2 = 'TR1' THEN 1 ELSE 0 END) AS count_tr1,
        -- Longitudinal rank
        ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY exam_date ASC) AS exam_rank_for_patient
    FROM nodules n
    GROUP BY research_id, exam_date
)
SELECT
    e.*,
    CASE
        WHEN e.exam_date < CAST(c.first_surgery_date AS DATE)
        THEN TRUE ELSE FALSE
    END AS is_preop_exam
FROM exam_agg e
LEFT JOIN main.canonical_patient_master c ON e.research_id = c.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 300 — canonical_us_exam_master_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Check source
    src = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(DISTINCT (research_id, exam_date))
        FROM main.canonical_us_nodule_master_v1
    """).fetchone()
    log(f"  Source (nodule master): {src[0]} rows, {src[1]} patients, "
        f"{src[2]} distinct (rid, exam_date)")

    if not args.commit:
        log("  (dry-run — no table created)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(EXAM_SQL)

    post = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.canonical_us_exam_master_v1
    """).fetchone()
    log(f"  Built: {post[0]} rows, {post[1]} patients")

    # Grain check
    grain = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT (research_id, exam_date))
        FROM main.canonical_us_exam_master_v1
    """).fetchone()
    log(f"  Grain check: rows={grain[0]}, distinct_key={grain[1]}")

    # Sample distributions
    dist = con.execute("""
        SELECT worst_tirads_category_this_exam, COUNT(*)
        FROM main.canonical_us_exam_master_v1
        WHERE worst_tirads_category_this_exam IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  Worst TIRADS distribution:")
    for d in dist:
        log(f"    {d[0]}: {d[1]}")

    preop = con.execute("""
        SELECT COUNT(*) FILTER (WHERE is_preop_exam),
               COUNT(*) FILTER (WHERE NOT is_preop_exam OR is_preop_exam IS NULL)
        FROM main.canonical_us_exam_master_v1
    """).fetchone()
    log(f"  Preop exams: {preop[0]}, post-op/unknown: {preop[1]}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 300 complete.")


if __name__ == "__main__":
    main()
