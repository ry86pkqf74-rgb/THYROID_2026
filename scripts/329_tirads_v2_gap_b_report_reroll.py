"""
Script 329 — TIRADS v2 Gap B: report-level re-roll with VARCHAR cast.

tirads_v2_report_patient_rollup_v1 covers 4,073 distinct RIDs, but CPM
exposes report-level columns for far fewer (e.g. tirads_v2_any_suspicious_ln_on_us
is stuck at 1,498).  1,608–2,575 patients with report-level TIRADS signal
are not in CPM.

Fix: re-roll with VARCHAR cast, backfill CPM NULL-only.

Usage:
    python 329_tirads_v2_gap_b_report_reroll.py            # dry-run
    python 329_tirads_v2_gap_b_report_reroll.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "329_tirads_v2_gap_b_report_reroll"


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
    log(f"Script 329 — TIRADS v2 Gap B report re-roll "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Source coverage
    n_report_rids = con.execute("""
        SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR))
        FROM main.tirads_v2_reports_raw
    """).fetchone()[0]
    log(f"  tirads_v2_reports_raw distinct RIDs: {n_report_rids}")

    # Pre CPM
    pre_cols = {}
    for col in ["tirads_v2_any_fna_recommended", "tirads_v2_any_suspicious_ln_on_us",
                 "tirads_v2_any_ete_on_us", "tirads_v2_any_interval_growth",
                 "tirads_v2_shortest_followup_months"]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{col}'
        """).fetchone()[0]
        if exists:
            n = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master
                WHERE "{col}" IS NOT NULL
            """).fetchone()[0]
            pre_cols[col] = n
            log(f"  CPM.{col} pre: {n} nonnull")
        else:
            pre_cols[col] = -1
            log(f"  CPM.{col}: MISSING — will add")

    # Rebuild report rollup with VARCHAR cast
    log("  Rebuilding tirads_v2_report_patient_rollup_v1...")
    con.execute("""
        CREATE OR REPLACE TABLE main.tirads_v2_report_patient_rollup_v1 AS
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            COUNT(*) AS tirads_v2_n_reports,
            BOOL_OR(suspicious_ln_present = TRUE)
                AS tirads_v2_any_suspicious_ln_on_us,
            BOOL_OR(overall_recommendation = 'fna')
                AS tirads_v2_any_fna_recommended_report,
            MIN(follow_up_interval_months)
                AS tirads_v2_shortest_followup_months
        FROM main.tirads_v2_reports_raw
        GROUP BY CAST(research_id AS VARCHAR)
    """)

    rollup_rids = con.execute("""
        SELECT COUNT(DISTINCT research_id)
        FROM main.tirads_v2_report_patient_rollup_v1
    """).fetchone()[0]
    log(f"  Report rollup rebuilt: {rollup_rids} distinct RIDs")

    # Add missing CPM columns if needed
    for col, dtype in [
        ("tirads_v2_shortest_followup_months", "DOUBLE"),
        ("tirads_v2_any_biopsy_recommended_date", "DATE"),
        ("tirads_v2_any_biopsy_recommended_first_note_id", "VARCHAR"),
        ("tirads_v2_any_biopsy_recommended_first_evidence_text", "VARCHAR"),
    ]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{col}'
        """).fetchone()[0]
        if exists == 0 and args.commit:
            con.execute(f'ALTER TABLE main.canonical_patient_master ADD COLUMN "{col}" {dtype}')
            log(f"    Added CPM column: {col} {dtype}")

    # Backfill CPM columns
    backfill_map = [
        ("tirads_v2_any_suspicious_ln_on_us", "tirads_v2_any_suspicious_ln_on_us"),
        ("tirads_v2_shortest_followup_months", "tirads_v2_shortest_followup_months"),
    ]

    for cpm_col, src_col in backfill_map:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM.{cpm_col}: not found — skipping")
            continue

        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN main.tirads_v2_report_patient_rollup_v1 r
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
              FROM main.tirads_v2_report_patient_rollup_v1 AS r
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
              "TIRADS v2 report rollup with VARCHAR cast fix",
              "v1 NULL only", plan_n, None, None, SCRIPT])

    # Also backfill tirads_v2_any_ete_on_us and tirads_v2_any_interval_growth
    # from the NODULE rollup (rebuilt in Script 328)
    for cpm_col in ["tirads_v2_any_ete_on_us", "tirads_v2_any_interval_growth",
                     "tirads_v2_any_fna_recommended"]:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            continue

        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN main.tirads_v2_nodule_patient_rollup_v1 r
                ON CAST(c.research_id AS VARCHAR) = r.research_id
            WHERE c."{cpm_col}" IS NULL
              AND r."{cpm_col}" IS NOT NULL
        """).fetchone()[0]
        log(f"  CPM.{cpm_col} (from nodule rollup): {plan_n} planned backfills")

        if plan_n == 0 or not args.commit:
            continue

        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{cpm_col}" = r."{cpm_col}"
              FROM main.tirads_v2_nodule_patient_rollup_v1 AS r
             WHERE CAST(c.research_id AS VARCHAR) = r.research_id
               AND c."{cpm_col}" IS NULL
               AND r."{cpm_col}" IS NOT NULL
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
              "TIRADS v2 nodule rollup backfill (Gap B pass)",
              "v1 NULL only", plan_n, None, None, SCRIPT])

    if not args.commit:
        log("  (dry-run — no UPDATE)")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 329 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
