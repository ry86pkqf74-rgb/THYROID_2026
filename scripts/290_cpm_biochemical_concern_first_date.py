"""
Script 290 — Backfill biochemical_concern_first_date from canonical_recurrence_v1
and tg_postop_surveillance_windows_v1.

After Script 288 retyped biochemical_concern_first_date from INTEGER to DATE,
this script populates it with two sources:

  Primary: canonical_recurrence_v1 where recurrence_type IN
           ('biochemical_tg_rise', 'persistent_biochemical_disease')
  Fallback: tg_postop_surveillance_windows_v1 MIN(window_first_date)
            WHERE analyte='Tg' AND value_max > 2.0
            for patients NOT covered by the primary source.

Policy: UPDATE WHERE biochemical_concern_first_date IS NULL only.

Usage:
    python 290_cpm_biochemical_concern_first_date.py            # dry-run
    python 290_cpm_biochemical_concern_first_date.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "290_cpm_biochemical_concern_first_date"


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
    log(f"Script 290 — biochemical_concern_first_date backfill "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    pre_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE biochemical_concern_first_date IS NOT NULL
    """).fetchone()[0]
    log(f"  biochemical_concern_first_date pre-pop: {pre_pop} / 10871")

    # Primary source: canonical_recurrence_v1
    con.execute("DROP TABLE IF EXISTS _bcfd_primary")
    con.execute("""
        CREATE TEMP TABLE _bcfd_primary AS
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            MIN(CAST(recurrence_date AS DATE)) AS bcfd
        FROM main.canonical_recurrence_v1
        WHERE recurrence_type IN ('biochemical_tg_rise', 'persistent_biochemical_disease')
          AND recurrence_date IS NOT NULL
        GROUP BY CAST(research_id AS VARCHAR)
    """)
    n_primary = con.execute("SELECT COUNT(*) FROM _bcfd_primary").fetchone()[0]
    log(f"  Primary source (canonical_recurrence_v1): {n_primary} patients")

    # Fallback: tg_postop_surveillance_windows_v1
    con.execute("DROP TABLE IF EXISTS _bcfd_fallback")
    con.execute("""
        CREATE TEMP TABLE _bcfd_fallback AS
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            CAST(MIN(window_first_date) AS DATE) AS bcfd
        FROM main.tg_postop_surveillance_windows_v1
        WHERE analyte = 'Tg'
          AND value_max > 2.0
          AND window_first_date IS NOT NULL
          AND CAST(research_id AS VARCHAR) NOT IN (
              SELECT research_id FROM _bcfd_primary
          )
        GROUP BY CAST(research_id AS VARCHAR)
    """)
    n_fallback = con.execute("SELECT COUNT(*) FROM _bcfd_fallback").fetchone()[0]
    log(f"  Fallback source (tg_postop_surveillance_windows): {n_fallback} patients")

    # Union both sources
    con.execute("DROP TABLE IF EXISTS _bcfd_combined")
    con.execute("""
        CREATE TEMP TABLE _bcfd_combined AS
        SELECT research_id, bcfd, 'canonical_recurrence_v1' AS source FROM _bcfd_primary
        UNION ALL
        SELECT research_id, bcfd, 'tg_postop_surveillance_windows_v1' AS source FROM _bcfd_fallback
    """)
    n_combined = con.execute("SELECT COUNT(*) FROM _bcfd_combined").fetchone()[0]
    log(f"  Combined source: {n_combined} patients")

    # Plan how many would actually be set
    plan = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        JOIN _bcfd_combined s ON c.research_id = s.research_id
        WHERE c.biochemical_concern_first_date IS NULL
          AND s.bcfd IS NOT NULL
    """).fetchone()[0]
    log(f"  Planned UPDATE: {plan} rows (CPM NULL AND source NOT NULL)")

    sample = con.execute("""
        SELECT s.bcfd, s.source
        FROM main.canonical_patient_master c
        JOIN _bcfd_combined s ON c.research_id = s.research_id
        WHERE c.biochemical_concern_first_date IS NULL
          AND s.bcfd IS NOT NULL
        LIMIT 5
    """).fetchall()
    sample_str = ", ".join(f"{s[0]}({s[1]})" for s in sample)
    log(f"  Sample values: {sample_str}")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET biochemical_concern_first_date = s.bcfd
          FROM _bcfd_combined AS s
         WHERE c.research_id = s.research_id
           AND c.biochemical_concern_first_date IS NULL
           AND s.bcfd IS NOT NULL
    """)

    post_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE biochemical_concern_first_date IS NOT NULL
    """).fetchone()[0]
    actual = post_pop - pre_pop
    log(f"  Post-fill: {post_pop} / 10871  (delta +{actual})")

    # Source breakdown
    from_primary = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        JOIN _bcfd_primary p ON c.research_id = p.research_id
        WHERE c.biochemical_concern_first_date = p.bcfd
    """).fetchone()[0]
    log(f"  From canonical_recurrence_v1: {from_primary}")
    log(f"  From tg_postop_surveillance: {actual - from_primary}")

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        dt.datetime.utcnow(), "biochemical_concern_first_date",
        "canonical_recurrence_v1 (primary) + tg_postop_surveillance_windows_v1 (fallback)",
        "recurrence_type IN biochem types OR Tg > 2.0 ng/mL",
        actual, n_combined, sample_str, SCRIPT
    ])

    cpm_invariants(con, "post")
    log("=" * 72)
    log(f"Script 290 complete. Total rows updated: {actual}")


if __name__ == "__main__":
    main()
