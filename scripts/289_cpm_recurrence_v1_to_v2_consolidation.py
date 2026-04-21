"""
Script 289 — CPM recurrence text field v1 <- v2 consolidation.

After Script 288 retyped the v1 columns to VARCHAR, backfill:
  - recurrence_histology     <- recurrence_histology_v2  (~118 expected)
  - recurrence_site_primary  <- recurrence_site_v2       (~100 expected)

Policy: mirror Script 287 — UPDATE WHERE v1 IS NULL AND v2 IS NOT NULL.
Never overwrite existing v1 values.

Usage:
    python 289_cpm_recurrence_v1_to_v2_consolidation.py            # dry-run
    python 289_cpm_recurrence_v1_to_v2_consolidation.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "289_cpm_recurrence_v1_to_v2_consolidation"

FILLS = [
    ("recurrence_histology",  "recurrence_histology_v2",
     "fill from recurrence_histology_v2 where recurrence_histology IS NULL"),
    ("recurrence_site_primary", "recurrence_site_v2",
     "fill from recurrence_site_v2 where recurrence_site_primary IS NULL"),
]


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


def v1_nonnull_count(con, v1_col):
    return con.execute(
        f'SELECT COUNT(*) FROM main.canonical_patient_master '
        f'WHERE "{v1_col}" IS NOT NULL'
    ).fetchone()[0]


def consolidate_one(con, commit, v1_col, v2_col, desc):
    log(f"--- {v1_col} ---")
    log(f"    source: {desc}")

    pre_v1_pop = v1_nonnull_count(con, v1_col)

    plan = con.execute(f"""
        SELECT COUNT(*)
          FROM main.canonical_patient_master
         WHERE "{v1_col}" IS NULL
           AND "{v2_col}" IS NOT NULL
    """).fetchone()[0]
    log(f"    pre-fill: v1 non-NULL = {pre_v1_pop} / 10871")
    log(f"    planned UPDATE: {plan} rows (v1 NULL AND v2 NOT NULL)")

    if plan == 0:
        log("    nothing to do.")
        return 0

    sample = con.execute(f"""
        SELECT "{v2_col}" AS value
          FROM main.canonical_patient_master
         WHERE "{v1_col}" IS NULL
           AND "{v2_col}" IS NOT NULL
         LIMIT 5
    """).fetchall()
    sample_str = ", ".join(str(s[0]) for s in sample)
    log(f"    sample values to write: {sample_str}")

    if not commit:
        log("    (dry-run — no UPDATE)")
        return 0

    con.execute(f"""
        UPDATE main.canonical_patient_master
           SET "{v1_col}" = "{v2_col}"
         WHERE "{v1_col}" IS NULL
           AND "{v2_col}" IS NOT NULL
    """)

    post_v1_pop = v1_nonnull_count(con, v1_col)
    actual = post_v1_pop - pre_v1_pop
    log(f"    post-fill: v1 non-NULL = {post_v1_pop} / 10871  (delta +{actual})")

    if post_v1_pop < pre_v1_pop:
        raise SystemExit(
            f"INVARIANT VIOLATION: {v1_col} non-NULL count dropped "
            f"({pre_v1_pop} -> {post_v1_pop}). Refusing to continue."
        )

    n_rid = con.execute(f"""
        SELECT COUNT(DISTINCT research_id)
          FROM main.canonical_patient_master
         WHERE "{v1_col}" IS NOT NULL
           AND "{v1_col}" = "{v2_col}"
    """).fetchone()[0]

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), v1_col, desc, "v1 NULL only; no overwrite",
          actual, n_rid, sample_str, SCRIPT])

    return actual


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 289 — CPM recurrence v1 -> v2 consolidation "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    log("Policy: fill v1 only where NULL. Never overwrite existing v1.")
    log("")

    cpm_invariants(con, "pre")

    total_updated = 0
    for v1_col, v2_col, desc in FILLS:
        total_updated += consolidate_one(con, args.commit, v1_col, v2_col, desc)
        log("")

    cpm_invariants(con, "post")
    log("=" * 72)
    log(f"Total rows updated across all columns: {total_updated}")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
