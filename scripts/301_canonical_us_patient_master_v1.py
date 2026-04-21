"""
Script 301 — Build canonical_us_patient_master_v1 (per-patient rollup).

Rolls canonical_us_exam_master_v1 (Script 300) up to the patient grain.
Backfills CPM TIRADS columns conservatively (v1-NULL-only).

Usage:
    python 301_canonical_us_patient_master_v1.py            # dry-run
    python 301_canonical_us_patient_master_v1.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "301_canonical_us_patient_master_v1"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
            backfilled_at TIMESTAMP,
            cpm_column VARCHAR,
            source_description VARCHAR,
            threshold VARCHAR,
            n_rows_updated BIGINT,
            n_distinct_rid BIGINT,
            sample_values VARCHAR,
            script VARCHAR
        )
    """)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


PATIENT_SQL = """
CREATE OR REPLACE TABLE main.canonical_us_patient_master_v1 AS
WITH exam AS (
    SELECT * FROM main.canonical_us_exam_master_v1
),
patient_agg AS (
    SELECT
        research_id,
        TRUE AS has_any_us,
        COUNT(*) AS n_us_exams,
        MIN(exam_date) AS first_us_date,
        MAX(exam_date) AS last_us_date,
        BOOL_OR(is_preop_exam) AS preop_us_available_flag,
        MAX(worst_tirads_category_this_exam) AS max_tirads_category_ever,
        MAX(worst_tirads_points_this_exam) AS max_tirads_points_ever,
        -- TIRADS at first exam
        MAX(CASE WHEN exam_rank_for_patient = 1
            THEN worst_tirads_category_this_exam END) AS tirads_category_at_first_exam,
        -- TIRADS at last preop exam
        MAX(CASE WHEN is_preop_exam = TRUE
            THEN worst_tirads_category_this_exam END) AS tirads_category_at_last_preop_exam,
        SUM(n_nodules_on_exam) AS n_nodules_total_across_exams,
        BOOL_OR(bilateral_flag) AS bilateral_disease_flag_ever,
        BOOL_OR(CASE WHEN n_nodules_on_exam > 1 THEN TRUE ELSE FALSE END) AS multifocal_flag_ever,
        BOOL_OR(CASE WHEN worst_tirads_category_this_exam IN ('TR4','TR5')
                THEN TRUE ELSE FALSE END) AS any_suspicious_nodule_ever,
        MIN(CASE WHEN worst_tirads_category_this_exam IN ('TR4','TR5')
            THEN exam_date END) AS first_high_risk_tirads_date
    FROM exam
    GROUP BY research_id
)
SELECT * FROM patient_agg
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 301 — canonical_us_patient_master_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    src = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.canonical_us_exam_master_v1
    """).fetchone()
    log(f"  Source (exam master): {src[0]} rows, {src[1]} patients")

    if not args.commit:
        log("  (dry-run — no table created)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(PATIENT_SQL)

    post = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.canonical_us_patient_master_v1
    """).fetchone()
    log(f"  Built: {post[0]} rows, {post[1]} patients")

    if post[0] > 10871:
        raise SystemExit(
            f"Patient master has {post[0]} rows > 10871 (one per patient max)"
        )

    # Backfill CPM TIRADS columns
    log("  Backfilling CPM TIRADS columns (v1-NULL-only)...")

    backfills = [
        ("imaging_tirads_best",    "max_tirads_category_ever"),
        ("imaging_tirads_worst",   "max_tirads_category_ever"),
        ("tirads_v2_worst_category", "max_tirads_category_ever"),
        ("max_tirads_ever",        "max_tirads_category_ever"),
    ]

    for cpm_col, src_col in backfills:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM.{cpm_col}: column does not exist — skipping")
            continue

        pre_pop = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE "{cpm_col}" IS NOT NULL
        """).fetchone()[0]

        plan = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN main.canonical_us_patient_master_v1 p ON c.research_id = p.research_id
            WHERE c."{cpm_col}" IS NULL
              AND p."{src_col}" IS NOT NULL
        """).fetchone()[0]

        if plan > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master AS c
                   SET "{cpm_col}" = p."{src_col}"
                  FROM main.canonical_us_patient_master_v1 AS p
                 WHERE c.research_id = p.research_id
                   AND c."{cpm_col}" IS NULL
                   AND p."{src_col}" IS NOT NULL
            """)

            post_pop = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master
                WHERE "{cpm_col}" IS NOT NULL
            """).fetchone()[0]
            actual = post_pop - pre_pop
            log(f"  CPM.{cpm_col}: +{actual} (pre={pre_pop}, post={post_pop})")

            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                dt.datetime.utcnow(), cpm_col,
                f"from canonical_us_patient_master_v1.{src_col}",
                "v1 NULL only", actual, None, None, SCRIPT
            ])
        else:
            log(f"  CPM.{cpm_col}: 0 new fills")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 301 complete.")


if __name__ == "__main__":
    main()
