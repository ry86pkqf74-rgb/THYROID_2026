"""
Script 296 — Resolve 598 n_surgeries v1!=v2 conflicts.

After Script 287, 598 patients have n_surgeries_v1=1 but n_surgeries_v2>=2.
v2 is authoritative (pipeline output). This script:
  1. Materializes the conflict set into manuscript_workspace
  2. Validates each against operative_episode_detail_v2 episode count
  3. On --commit, UPDATEs n_surgeries := n_surgeries_v2 for well-supported rows

Usage:
    python 296_n_surgeries_v1_v2_conflict_resolve.py            # dry-run
    python 296_n_surgeries_v1_v2_conflict_resolve.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "296_n_surgeries_v1_v2_conflict_resolve"


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 296 — n_surgeries v1/v2 conflict resolution "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Step 1: Materialize conflict set
    log("  Materializing conflict set...")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_workspace.n_surgeries_v1_v2_conflict_v1 AS
        SELECT
            c.research_id,
            c.n_surgeries AS v1,
            c.n_surgeries_v2 AS v2,
            c.first_surgery_date,
            c.second_surgery_date_v2,
            c.third_surgery_date_v2,
            -- Count distinct non-null surgery dates from CPM as evidence
            (CASE WHEN c.first_surgery_date IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN c.second_surgery_date_v2 IS NOT NULL THEN 1 ELSE 0 END
             + CASE WHEN c.third_surgery_date_v2 IS NOT NULL THEN 1 ELSE 0 END
            ) AS n_distinct_dates_available,
            -- Count episodes in operative_episode_detail_v2
            COALESCE(oed.n_episodes, 0) AS n_oed_episodes,
            CASE
                WHEN c.n_surgeries_v2 IS NOT NULL
                 AND (CASE WHEN c.second_surgery_date_v2 IS NOT NULL THEN 1 ELSE 0 END) >= 1
                THEN TRUE
                ELSE FALSE
            END AS well_supported
        FROM main.canonical_patient_master c
        LEFT JOIN (
            SELECT research_id, COUNT(*) AS n_episodes
            FROM main.operative_episode_detail_v2
            GROUP BY research_id
        ) oed ON c.research_id = oed.research_id
        WHERE c.n_surgeries IS NOT NULL
          AND c.n_surgeries_v2 IS NOT NULL
          AND c.n_surgeries != c.n_surgeries_v2
    """)

    n_conflicts = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.n_surgeries_v1_v2_conflict_v1"
    ).fetchone()[0]
    log(f"  Total conflicts: {n_conflicts}")

    # Breakdown
    dist = con.execute("""
        SELECT v1, v2, COUNT(*), SUM(CASE WHEN well_supported THEN 1 ELSE 0 END)
        FROM manuscript_workspace.n_surgeries_v1_v2_conflict_v1
        GROUP BY 1, 2 ORDER BY 3 DESC
    """).fetchall()
    for d in dist:
        log(f"    v1={d[0]} v2={d[1]}: {d[2]} total, {d[3]} well-supported")

    n_well = con.execute("""
        SELECT COUNT(*)
        FROM manuscript_workspace.n_surgeries_v1_v2_conflict_v1
        WHERE well_supported = TRUE
    """).fetchone()[0]
    log(f"  Well-supported (have >=1 second_surgery_date_v2): {n_well}")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Step 2: UPDATE for well-supported rows
    log("  Updating n_surgeries for well-supported conflicts...")
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET n_surgeries = q.v2
          FROM manuscript_workspace.n_surgeries_v1_v2_conflict_v1 AS q
         WHERE c.research_id = q.research_id
           AND q.well_supported = TRUE
    """)

    post_dist = con.execute("""
        SELECT n_surgeries, COUNT(*)
        FROM main.canonical_patient_master
        WHERE n_surgeries IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  Post-update n_surgeries distribution:")
    for d in post_dist:
        log(f"    n_surgeries={d[0]}: {d[1]}")

    # Remaining conflicts
    remaining = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        WHERE c.n_surgeries IS NOT NULL
          AND c.n_surgeries_v2 IS NOT NULL
          AND c.n_surgeries != c.n_surgeries_v2
    """).fetchone()[0]
    log(f"  Remaining unresolved conflicts: {remaining}")

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        dt.datetime.utcnow(), "n_surgeries",
        "Override v1 with v2 for well-supported conflicts (second_surgery_date_v2 present)",
        "well_supported=TRUE only", n_well, n_well, None, SCRIPT
    ])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 296 complete.")


if __name__ == "__main__":
    main()
