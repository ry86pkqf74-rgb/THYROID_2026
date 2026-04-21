"""
Script 288 — Fix CPM column types (DDL bug).

Five CPM columns were declared INTEGER but are meant to hold dates/strings.
They have been 100 % NULL since creation and any backfill errors with
"Conversion Error: Unimplemented type for cast (TIMESTAMP_NS -> INTEGER)".

Also fixes canonical_recurrence_v1 which has the same DDL bug for
recurrence_date, recurrence_site, recurrence_histology.

Approach (conservative — columns are 100 % NULL):
  1. Assert each column is 100 % NULL.
  2. Try ALTER COLUMN ... SET DATA TYPE.
  3. On failure, fall back to DROP COLUMN + ADD COLUMN (same name, correct type).
  4. Log which path was taken per column.

Usage:
    python 288_fix_cpm_ddl_types.py            # dry-run
    python 288_fix_cpm_ddl_types.py --commit   # execute
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "288_fix_cpm_ddl_types"

CPM_ALTER_PLAN = [
    ("biochemical_concern_first_date", "DATE"),
    ("path_stage_raw",                 "VARCHAR"),
    ("recurrence_histology",           "VARCHAR"),
    ("recurrence_site_primary",        "VARCHAR"),
    ("rai_scan_findings_v9",           "VARCHAR"),
]

RECURRENCE_ALTER_PLAN = [
    ("recurrence_date",      "DATE"),
    ("recurrence_site",      "VARCHAR"),
    ("recurrence_histology", "VARCHAR"),
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


def retype_column(con, table_fq, col, new_type, commit):
    """Retype a column. Returns 'ALTER' or 'DROP_ADD' or 'skipped'."""
    n_nonnull = con.execute(
        f'SELECT COUNT(*) FROM {table_fq} WHERE "{col}" IS NOT NULL'
    ).fetchone()[0]
    log(f"  {col}: non-NULL count = {n_nonnull}")
    if n_nonnull != 0:
        log(f"  ERROR: {col} has {n_nonnull} non-NULL rows — refusing to retype.")
        raise SystemExit(
            f"{col} in {table_fq} has {n_nonnull} non-NULL rows. "
            f"Cannot safely retype. Aborting."
        )

    cur_type = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = '{table_fq.split('.')[-1]}'
          AND column_name = '{col}'
    """).fetchone()
    if cur_type:
        log(f"  {col}: current type = {cur_type[0]}, target = {new_type}")
        if cur_type[0].upper() == new_type.upper():
            log(f"  {col}: already correct type — skipping.")
            return "skipped"
    else:
        log(f"  WARNING: {col} not found in information_schema — skipping.")
        return "skipped"

    if not commit:
        log(f"  {col}: would retype {cur_type[0]} -> {new_type} (dry-run)")
        return "dry_run"

    method = "ALTER"
    try:
        con.execute(
            f'ALTER TABLE {table_fq} ALTER COLUMN "{col}" '
            f'SET DATA TYPE {new_type}'
        )
        log(f"  {col}: ALTER succeeded -> {new_type}")
    except Exception as e:
        log(f"  {col}: ALTER failed ({e}), falling back to DROP+ADD")
        method = "DROP_ADD"
        con.execute(f'ALTER TABLE {table_fq} DROP COLUMN "{col}"')
        con.execute(f'ALTER TABLE {table_fq} ADD COLUMN "{col}" {new_type}')
        log(f"  {col}: DROP+ADD succeeded -> {new_type}")

    verify_type = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = '{table_fq.split('.')[-1]}'
          AND column_name = '{col}'
    """).fetchone()
    log(f"  {col}: verified type = {verify_type[0] if verify_type else 'NOT FOUND'}")
    return method


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 288 — Fix CPM DDL types "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    log("")
    log("--- canonical_patient_master columns ---")
    for col, new_type in CPM_ALTER_PLAN:
        method = retype_column(
            con, "main.canonical_patient_master", col, new_type, args.commit
        )
        if args.commit and method in ("ALTER", "DROP_ADD"):
            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                dt.datetime.utcnow(), col,
                f"DDL retype INTEGER->{new_type} via {method}",
                "column was 100% NULL", 0, 0, f"method={method}", SCRIPT
            ])
        log("")

    cpm_invariants(con, "post-CPM-retype")

    log("")
    log("--- canonical_recurrence_v1 columns ---")
    for col, new_type in RECURRENCE_ALTER_PLAN:
        try:
            method = retype_column(
                con, "main.canonical_recurrence_v1", col, new_type, args.commit
            )
        except SystemExit as e:
            log(f"  STOPPING: {e}")
            log(f"  canonical_recurrence_v1.{col} is NOT 100% NULL.")
            log("  Reporting to Logan for manual review.")
            raise
        log("")

    cpm_invariants(con, "final")

    log("=" * 72)
    log("Script 288 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
