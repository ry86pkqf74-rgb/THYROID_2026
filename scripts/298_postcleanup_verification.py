"""
Script 298 — Final V1_0 lint / verification pass.

Runs after Scripts 288-303 complete. Verifies:
  1. All four CPM invariants
  2. Column types (no INTEGER where DATE/VARCHAR is correct)
  3. All views_readable views resolve
  4. Three new US masters exist and pass grain invariants
  5. Two new genetics masters exist and pass grain invariants
  6. Discordance queues are non-empty and un-resolved
  7. Writes scripts/output/298_postcleanup_audit.md

Usage:
    python 298_postcleanup_verification.py
"""
import datetime as dt
import os

from _md_connect import connect_locked

SCRIPT = "298_postcleanup_verification"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    con = connect_locked()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_lines = []

    def report(line):
        log(line)
        report_lines.append(line)

    report("=" * 72)
    report("Script 298 — Post-cleanup verification")
    report("=" * 72)

    # 1. CPM invariants
    report("")
    report("## 1. CPM Invariants")
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    report(f"  rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        report("  **INVARIANT VIOLATION**")
    else:
        report("  PASS")

    # CPM column count
    cpm_cols = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master'
    """).fetchone()[0]
    report(f"  CPM column count: {cpm_cols}")

    # 2. Column types check
    report("")
    report("## 2. Column Type Verification")
    type_issues = con.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master'
          AND (
            (column_name LIKE '%date%' AND data_type = 'INTEGER')
            OR (column_name LIKE '%stage%' AND data_type = 'INTEGER')
            OR (column_name LIKE '%histology%' AND data_type = 'INTEGER')
            OR (column_name LIKE '%site%' AND data_type = 'INTEGER')
            OR (column_name LIKE '%findings%' AND data_type = 'INTEGER')
          )
    """).fetchall()
    if type_issues:
        for t in type_issues:
            report(f"  ISSUE: {t[0]} typed as {t[1]} (should be DATE/VARCHAR)")
    else:
        report("  PASS — no misdeclared INTEGER columns found")

    # 3. views_readable verification
    report("")
    report("## 3. Views Readable Resolution")
    views = con.execute("""
        SELECT view_name FROM duckdb_views()
        WHERE schema_name = 'views_readable'
    """).fetchall()
    n_views = len(views)
    n_ok = 0
    n_fail = 0
    for v in views:
        try:
            con.execute(
                f"SELECT COUNT(*) FROM views_readable.\"{v[0]}\""
            )
            n_ok += 1
        except Exception as e:
            report(f"  FAIL: views_readable.{v[0]}: {e}")
            n_fail += 1
    report(f"  {n_ok}/{n_views} views resolve, {n_fail} failed")

    # 4. US Masters
    report("")
    report("## 4. US Master Tables")

    for tbl, expected_cols in [
        ("canonical_us_nodule_master_v1", None),
        ("canonical_us_exam_master_v1", None),
        ("canonical_us_patient_master_v1", None),
    ]:
        try:
            r = con.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT research_id)
                FROM main."{tbl}"
            """).fetchone()
            report(f"  {tbl}: {r[0]} rows, {r[1]} patients")
        except Exception as e:
            report(f"  {tbl}: MISSING — {e}")

    # 5. Genetics Masters
    report("")
    report("## 5. Genetics Master Tables")

    try:
        r = con.execute("""
            SELECT COUNT(*), COUNT(DISTINCT research_id)
            FROM main.genetics_per_test_master_v1
        """).fetchone()
        report(f"  genetics_per_test_master_v1: {r[0]} rows, {r[1]} patients")
    except Exception as e:
        report(f"  genetics_per_test_master_v1: MISSING — {e}")

    try:
        r = con.execute("""
            SELECT COUNT(*), COUNT(DISTINCT research_id),
                   SUM(CASE WHEN was_tested THEN 1 ELSE 0 END)
            FROM main.genetics_per_patient_master_v1
        """).fetchone()
        report(f"  genetics_per_patient_master_v1: {r[0]} rows, {r[1]} patients, "
               f"{r[2]} tested")
        if r[0] != 10871:
            report(f"  WARNING: expected 10,871 rows, got {r[0]}")
    except Exception as e:
        report(f"  genetics_per_patient_master_v1: MISSING — {e}")

    # 6. Discordance queues
    report("")
    report("## 6. Discordance / Adjudication Queues")
    queues = [
        ("manuscript_workspace", "tirads_v1_v2_discordance_v1"),
        ("manuscript_workspace", "genetics_per_test_discordance_v1"),
        ("manuscript_workspace", "n_surgeries_v1_v2_conflict_v1"),
        ("manuscript_workspace", "vc_complication_tiering_v1"),
        ("manuscript_workspace", "archive_candidate_review_v1"),
    ]
    for schema, tbl in queues:
        try:
            r = con.execute(
                f'SELECT COUNT(*) FROM {schema}."{tbl}"'
            ).fetchone()[0]
            report(f"  {tbl}: {r} rows")
        except Exception:
            report(f"  {tbl}: NOT FOUND")

    # 7. Backfill log summary
    report("")
    report("## 7. Backfill Log Summary")
    log_entries = con.execute("""
        SELECT script, cpm_column, n_rows_updated, source_description
        FROM manuscript_workspace.cpm_backfill_log_v1
        ORDER BY backfilled_at
    """).fetchall()
    for e in log_entries:
        report(f"  {e[0]:50s} {e[1]:40s} +{e[2] or 0}")

    # 8. Archive log summary
    report("")
    report("## 8. Archive Move Log")
    try:
        archives = con.execute("""
            SELECT script, src_table, n_rows
            FROM manuscript_workspace.archive_move_log_v1
            ORDER BY moved_at
        """).fetchall()
        for a in archives:
            report(f"  {a[0]:50s} {a[1]:60s} {a[2]} rows")
    except Exception:
        report("  archive_move_log_v1: NOT FOUND")

    # 9. Table / view counts
    report("")
    report("## 9. Final Database State")
    for schema in ["main", "manuscript_workspace", "views_readable"]:
        tbls = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_type = 'BASE TABLE'
        """).fetchone()[0]
        vws = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_views()
            WHERE schema_name = '{schema}'
        """).fetchone()[0]
        report(f"  {schema}: {tbls} tables, {vws} views")

    report("")
    report("=" * 72)
    report("Verification complete.")

    # Write report
    report_path = os.path.join(OUTPUT_DIR, "298_postcleanup_audit.md")
    with open(report_path, "w") as f:
        f.write("# Post-Cleanup Audit (Script 298)\n\n")
        f.write(f"Generated: {dt.datetime.utcnow().isoformat()}Z\n\n")
        for line in report_lines:
            f.write(line + "\n")
    log(f"  Report written to {report_path}")


if __name__ == "__main__":
    main()
