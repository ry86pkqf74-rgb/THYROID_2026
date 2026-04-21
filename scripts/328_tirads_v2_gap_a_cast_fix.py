"""
Script 328 — TIRADS v2 Gap A: VARCHAR↔BIGINT cast fix.

tirads_v2_nodules_raw has 3,021 distinct RIDs. cpm.tirads_v2_worst_category
is populated for only 2,465.  Delta = 556 patients lost to research_id
type mismatch in joins (VARCHAR vs BIGINT).

Fix: rebuild tirads_v2_nodule_patient_rollup_v1 with explicit CAST on
both sides of every join. Backfill CPM where NULL.

Usage:
    python 328_tirads_v2_gap_a_cast_fix.py            # dry-run
    python 328_tirads_v2_gap_a_cast_fix.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "328_tirads_v2_gap_a_cast_fix"


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


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
            backfilled_at TIMESTAMP, cpm_column VARCHAR,
            source_description VARCHAR, threshold VARCHAR,
            n_rows_updated BIGINT, n_distinct_rid BIGINT,
            sample_values VARCHAR, script VARCHAR
        )
    """)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 328 — TIRADS v2 Gap A cast fix "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Check source coverage
    n_nodule_rids = con.execute("""
        SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR))
        FROM main.tirads_v2_nodules_raw
        WHERE tirads_category IS NOT NULL
    """).fetchone()[0]
    log(f"  tirads_v2_nodules_raw distinct RIDs (with category): {n_nodule_rids}")

    # Current CPM coverage
    pre_worst = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE tirads_v2_worst_category IS NOT NULL
    """).fetchone()[0]
    pre_max = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE tirads_v2_max_points IS NOT NULL
    """).fetchone()[0]
    pre_scored = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE tirads_v2_n_nodules_scored IS NOT NULL
    """).fetchone()[0]
    log(f"  CPM pre: worst_category={pre_worst}, max_points={pre_max}, "
        f"n_scored={pre_scored}")

    # Rebuild rollup with VARCHAR cast
    log("  Rebuilding tirads_v2_nodule_patient_rollup_v1 with CAST...")
    con.execute("""
        CREATE OR REPLACE TABLE main.tirads_v2_nodule_patient_rollup_v1 AS
        WITH ranked AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                tirads_category,
                tirads_total_points,
                size_cm_max,
                extrathyroidal_extension_on_us,
                interval_growth_flag,
                fna_recommended_this_nodule,
                CASE tirads_category
                    WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2
                    WHEN 'TR3' THEN 3 WHEN 'TR4' THEN 4
                    WHEN 'TR5' THEN 5 ELSE 0
                END AS tr_rank
            FROM main.tirads_v2_nodules_raw
            WHERE tirads_category IS NOT NULL
        )
        SELECT
            research_id,
            COUNT(*) AS tirads_v2_n_nodules_scored,
            MAX(tirads_category) AS tirads_v2_worst_category,
            MAX(tr_rank) AS tirads_v2_worst_rank,
            MAX(tirads_total_points) AS tirads_v2_max_points,
            MAX(size_cm_max) AS tirads_v2_largest_nodule_cm,
            BOOL_OR(extrathyroidal_extension_on_us IN ('suspected', 'definite'))
                AS tirads_v2_any_ete_on_us,
            BOOL_OR(interval_growth_flag) AS tirads_v2_any_interval_growth,
            BOOL_OR(fna_recommended_this_nodule) AS tirads_v2_any_fna_recommended
        FROM ranked
        GROUP BY research_id
    """)

    rollup_rids = con.execute("""
        SELECT COUNT(DISTINCT research_id)
        FROM main.tirads_v2_nodule_patient_rollup_v1
    """).fetchone()[0]
    log(f"  Rollup rebuilt: {rollup_rids} distinct RIDs")

    if rollup_rids < 3000:
        log(f"  WARNING: expected ~3,021 RIDs, got {rollup_rids}")

    # Plan backfill
    backfill_cols = [
        ("tirads_v2_worst_category", "tirads_v2_worst_category"),
        ("tirads_v2_max_points", "tirads_v2_max_points"),
        ("tirads_v2_n_nodules_scored", "tirads_v2_n_nodules_scored"),
    ]

    for cpm_col, src_col in backfill_cols:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM.{cpm_col}: not found — skipping")
            continue

        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN main.tirads_v2_nodule_patient_rollup_v1 r
                ON CAST(c.research_id AS VARCHAR) = r.research_id
            WHERE c."{cpm_col}" IS NULL
              AND r."{src_col}" IS NOT NULL
        """).fetchone()[0]
        log(f"  CPM.{cpm_col}: {plan_n} planned backfills")

        if plan_n == 0 or not args.commit:
            continue

        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{cpm_col}" = r."{src_col}"
              FROM main.tirads_v2_nodule_patient_rollup_v1 AS r
             WHERE CAST(c.research_id AS VARCHAR) = r.research_id
               AND c."{cpm_col}" IS NULL
               AND r."{src_col}" IS NOT NULL
        """)

        post_pop = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE "{cpm_col}" IS NOT NULL
        """).fetchone()[0]
        log(f"  CPM.{cpm_col}: post-pop={post_pop}")

        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), cpm_col,
              "TIRADS v2 nodule rollup with VARCHAR cast fix",
              "v1 NULL only; pre_value=NULL", plan_n, None, None, SCRIPT])

    if not args.commit:
        log("  (dry-run — no UPDATE)")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 328 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
