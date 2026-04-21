"""
Script 287 — CPM v1 → v2 surgery-column consolidation (CONSERVATIVE).

Background
----------
In Step 3 audit (2026-04-20) we discovered that the v1 surgery columns
in `canonical_patient_master` are stale stubs while the v2 columns hold
the correct pipeline output:

    column                                   v1 pop    v2 pop     fillable (v1 NULL, v2 populated)
    n_surgeries / _v2                         8731     10871      2140
    second_surgery_date / _v2                 2        738        737
    third_surgery_date / _v2                  0        40         40
    days_between_first_second_surgery / _v2   2        738        737

User reported patients have up to 6 surgeries — this was invisible in v1
(whose max value was 2) but correctly captured in v2:
    n_surgeries_v2: {1: 10133, 2: 698, 3: 31, 4: 7, 5: 1, 6: 1}

Orphans (no _v2 column, so derive from v2 dates):
    second_surgery_days_from_surg  ← days_between_first_second_surgery_v2
    third_surgery_days_from_surg   ← (third_surgery_date_v2 - first_surgery_date_v2)

Safety policy
-------------
**NEVER overwrite a non-NULL v1 value.** Only fills where v1 IS NULL.
This means the 598 rows where v1=1 but v2≥2 (i.e. v1 was wrong, not
merely missing) are left alone for manual review. See the Cursor prompt
file (CURSOR_PROMPT_OPERATIVE_REBUILD.md) for the conflict-resolution
and operative-detail work that is NOT safe to auto-apply.

Invariants checked pre + post:
  - CPM row count = 10871
  - distinct research_id = 10871
  - fna_path_outcome IS NOT NULL for all rows
  - v1 non-NULL values are never modified (SELECT COUNT(*) WHERE v1 IS NOT NULL
    is identical before and after)

Usage:
    python 287_cpm_surgery_v1_to_v2_consolidation.py           # dry-run
    python 287_cpm_surgery_v1_to_v2_consolidation.py --commit  # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked


SCRIPT = "287_cpm_surgery_v1_to_v2_consolidation"

# (v1_col, v2_col_or_derivation_sql, description)
# For straight v1←v2 pairs, pass the v2 column name as a plain identifier.
# For derivations, pass an SQL expression (must reference columns on main.canonical_patient_master).
FILLS = [
    ("n_surgeries",
     "n_surgeries_v2",
     "fill from n_surgeries_v2 where n_surgeries IS NULL"),
    ("second_surgery_date",
     "second_surgery_date_v2",
     "fill from second_surgery_date_v2"),
    ("third_surgery_date",
     "third_surgery_date_v2",
     "fill from third_surgery_date_v2"),
    ("days_between_first_second_surgery",
     "days_between_first_second_surgery_v2",
     "fill from days_between_first_second_surgery_v2"),
    # Orphans: derive from v2 dates
    ("second_surgery_days_from_surg",
     "days_between_first_second_surgery_v2",
     "derived: days_between_first_second_surgery_v2 (same concept)"),
    ("third_surgery_days_from_surg",
     "CAST(third_surgery_date_v2 AS DATE) - CAST(first_surgery_date_v2 AS DATE)",
     "derived: third_surgery_date_v2 - first_surgery_date_v2 (in days)"),
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
        f'WHERE "{v1_col}" IS NOT NULL').fetchone()[0]


def consolidate_one(con, commit, v1_col, v2_expr, desc):
    log(f"--- {v1_col} ---")
    log(f"    source: {desc}")

    pre_v1_pop = v1_nonnull_count(con, v1_col)

    # How many rows would be set?
    # v2_expr might be either a bare column name or an SQL expression.
    # For the planning/execute query we reference it directly as an expression
    # against the same row.
    plan = con.execute(f"""
        SELECT COUNT(*)
          FROM main.canonical_patient_master
         WHERE "{v1_col}" IS NULL
           AND ({v2_expr}) IS NOT NULL
    """).fetchone()[0]
    log(f"    pre-fill: v1 non-NULL = {pre_v1_pop} / 10871")
    log(f"    planned UPDATE: {plan} rows (v1 NULL AND source NOT NULL)")

    if plan == 0:
        log("    nothing to do.")
        return 0

    # Show a small sample of v2 values we'd write
    sample = con.execute(f"""
        SELECT ({v2_expr}) AS value
          FROM main.canonical_patient_master
         WHERE "{v1_col}" IS NULL
           AND ({v2_expr}) IS NOT NULL
         LIMIT 5
    """).fetchall()
    sample_str = ", ".join(str(s[0]) for s in sample)
    log(f"    sample values to write: {sample_str}")

    if not commit:
        log("    (dry-run — no UPDATE)")
        return 0

    # UPDATE — explicit WHERE "v1" IS NULL so we never overwrite.
    con.execute(f"""
        UPDATE main.canonical_patient_master
           SET "{v1_col}" = ({v2_expr})
         WHERE "{v1_col}" IS NULL
           AND ({v2_expr}) IS NOT NULL
    """)

    post_v1_pop = v1_nonnull_count(con, v1_col)
    log(f"    post-fill: v1 non-NULL = {post_v1_pop} / 10871  (delta +{post_v1_pop - pre_v1_pop})")

    if post_v1_pop < pre_v1_pop:
        raise SystemExit(
            f"INVARIANT VIOLATION: {v1_col} non-NULL count dropped "
            f"({pre_v1_pop} -> {post_v1_pop}). Refusing to continue."
        )

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), v1_col, desc, "v1 NULL only; no overwrite",
          post_v1_pop - pre_v1_pop, None, sample_str, SCRIPT])

    return post_v1_pop - pre_v1_pop


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 287 — CPM surgery v1 -> v2 consolidation "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    log("Policy: fill v1 only where NULL. Never overwrite existing v1.")
    log("")

    cpm_invariants(con, "pre")

    total_updated = 0
    for v1_col, v2_expr, desc in FILLS:
        total_updated += consolidate_one(con, args.commit, v1_col, v2_expr, desc)
        log("")

    cpm_invariants(con, "post")
    log("=" * 72)
    log(f"Total rows updated across all columns: {total_updated}")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
