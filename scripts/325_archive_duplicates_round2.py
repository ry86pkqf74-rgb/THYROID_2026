"""
Script 325 — Archive confirmed duplicate/stale objects (Round 2).

Archives only unreferenced, Logan-approved candidates.
Referenced tables are logged to review queue but not touched.

Usage:
    python 325_archive_duplicates_round2.py            # dry-run
    python 325_archive_duplicates_round2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "325_archive_duplicates_round2"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"

AUTO_ARCHIVE = [
    ("main", "tumor_pathology",
     "Superseded by path_synoptics + synoptic_tumor_long_v1"),
    ("main", "path_size_adjudication_v241",
     "Versioned adjudication artifact (Script 241)"),
    ("main", "ret_note_entity_adjudication_v226",
     "Versioned adjudication artifact (Script 226)"),
    ("main", "ret_patient_adjudicated_v226",
     "Versioned adjudication artifact (Script 226)"),
    ("main", "tirads_v2_reports_raw",
     "Raw source superseded by tirads_v2_nodules_raw"),
    ("main", "tirads_llm_validation_v2",
     "Superseded by Phase B verify tables"),
]

CONDITIONAL_SKIP = [
    ("main", "us_nodules_tirads", "Referenced by US_Nodules_TIRADS view"),
    ("main", "imaging_nodule_master_v1", "Referenced by US_Nodules_Index, imaging_nodule_master_clean_v1"),
    ("main", "canonical_molecular_tested_v1", "Referenced by Genetics_Testing view"),
    ("main", "data_dictionary_v279", "Referenced by Data_Dictionary view"),
    ("main", "tirads_reextraction_queue_v1", "Referenced by US_TIRADS_Reextraction_Queue view"),
    ("main", "us_nodules_tirads_vs_inm_v1_discordance_v1", "Discordance queue — keep for adjudication"),
]


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


def table_exists(con, schema, table):
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchone()[0] > 0


def check_references(con, table_name):
    refs = con.execute(f"""
        SELECT DISTINCT view_name
        FROM duckdb_views()
        WHERE sql ILIKE '%{table_name}%' AND view_name != '{table_name}'
    """).fetchall()
    return [r[0] for r in refs]


def archive_one(con, schema, table_name, reason, commit):
    if not table_exists(con, schema, table_name):
        log(f"    {schema}.{table_name}: NOT FOUND — skipping")
        return False, 0

    src_fq = f'"{schema}"."{table_name}"'
    n_rows = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]

    refs = check_references(con, table_name)
    if refs:
        log(f"    {schema}.{table_name}: REFERENCED by {', '.join(refs[:5])} — SKIPPING")
        return False, n_rows

    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"{table_name}_pre325_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'

    log(f"    {schema}.{table_name}: {n_rows} rows, no refs — archiving")

    if not commit:
        log(f"    (dry-run — would archive to {archive_name})")
        return True, n_rows

    con.execute(f"CREATE TABLE {archive_fq} AS SELECT * FROM {src_fq}")

    dest_count = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if dest_count != n_rows:
        raise SystemExit(f"Archive count mismatch for {table_name}: src={n_rows}, dest={dest_count}")

    con.execute(f"DROP TABLE {src_fq}")
    log(f"    Archived + dropped: {n_rows} rows -> {archive_name}")

    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP, src_schema VARCHAR, src_table VARCHAR,
            archive_fq VARCHAR, n_rows BIGINT, reason VARCHAR, script VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), schema, table_name, archive_fq, n_rows, reason, SCRIPT])

    return True, n_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 325 — Archive duplicates round 2 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    log("")
    log("=== AUTO-ARCHIVE (unreferenced, confirmed) ===")
    n_archived = 0
    for schema, table_name, reason in AUTO_ARCHIVE:
        success, _ = archive_one(con, schema, table_name, reason, args.commit)
        if success:
            n_archived += 1

    log(f"\n  Auto-archived: {n_archived} tables")

    log("")
    log("=== SKIPPED (referenced or conditional) ===")
    for schema, table_name, reason in CONDITIONAL_SKIP:
        if table_exists(con, schema, table_name):
            n = con.execute(f"SELECT COUNT(*) FROM {schema}.\"{table_name}\"").fetchone()[0]
            log(f"    {table_name}: {n} rows — SKIPPED ({reason})")
        else:
            log(f"    {table_name}: NOT FOUND")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 325 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
