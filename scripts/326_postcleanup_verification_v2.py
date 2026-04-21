"""
Script 326 — Final post-cleanup verification (supersedes Script 298).

Checks:
  1. CPM invariants
  2. Tier 2 completeness invariant
  3. Concordance summaries from all verify tables
  4. Low-concordance flagging
  5. Main schema object classification
  6. Archive log diff
  7. Write scripts/output/326_postcleanup_audit.md

Usage:
    python 326_postcleanup_verification_v2.py
"""
from __future__ import annotations

import datetime as dt
import os
import subprocess

from _md_connect import connect_locked

SCRIPT = "326_postcleanup_verification_v2"
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
    report("Script 326 — Post-cleanup verification v2")
    report(f"Generated: {dt.datetime.utcnow().isoformat()}Z")
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
    cpm_cols = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master'
    """).fetchone()[0]
    report(f"  CPM column count: {cpm_cols}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        report("  **INVARIANT VIOLATION**")
    else:
        report("  PASS")

    # 2. Tier 2 completeness
    report("")
    report("## 2. Tier 2 Completeness")
    try:
        t2 = con.execute("""
            SELECT llm_source, expected_tier2_table, has_tier2_event_table
            FROM manuscript_workspace.tier2_completeness_v1
            ORDER BY llm_source
        """).fetchall()
        n_ok = sum(1 for _, _, ok in t2 if ok)
        report(f"  {n_ok}/{len(t2)} domains have Tier 2 tables")
        gaps = [r for r in t2 if not r[2]]
        if gaps:
            for g in gaps:
                report(f"  GAP: {g[0]} -> {g[1]}")
        else:
            report("  PASS — all domains covered")
    except Exception as e:
        report(f"  ERROR: {e}")

    # 3. Tier 2 event table row counts
    report("")
    report("## 3. Tier 2 Event Table Row Counts")
    event_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name LIKE '%_event_v1'
        ORDER BY table_name
    """).fetchall()
    for t in event_tables:
        tn = t[0]
        r = con.execute(f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main."{tn}"').fetchone()
        report(f"  {tn:50s} {r[0]:>8} rows  {r[1]:>6} pts")

    # 4. Patient-wide table row counts
    report("")
    report("## 4. Patient-wide Table Row Counts")
    wide_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name LIKE '%_patient_wide_v1'
        ORDER BY table_name
    """).fetchall()
    for t in wide_tables:
        tn = t[0]
        r = con.execute(f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main."{tn}"').fetchone()
        report(f"  {tn:50s} {r[0]:>8} rows  {r[1]:>6} pts")

    # 5. Concordance summaries
    report("")
    report("## 5. Concordance Summaries (all verify tables)")
    verify_summary_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name LIKE 'verify_%_summary_v1'
        ORDER BY table_name
    """).fetchall()

    low_concordance = []
    for t in verify_summary_tables:
        tn = t[0]
        try:
            rows = con.execute(f'SELECT * FROM main."{tn}"').fetchall()
            cols = [d[0] for d in con.execute(f'SELECT * FROM main."{tn}" LIMIT 0').description]
            pct_idx = next((i for i, c in enumerate(cols) if c == 'pct_agree'), -1)
            field_idx = next((i for i, c in enumerate(cols) if c == 'field_name'), -1)
            domain_idx = next((i for i, c in enumerate(cols) if c == 'domain'), -1)

            for row in rows:
                pct = row[pct_idx] if pct_idx >= 0 else None
                field = row[field_idx] if field_idx >= 0 else '?'
                domain = row[domain_idx] if domain_idx >= 0 else tn
                agree = row[cols.index('n_agree')] if 'n_agree' in cols else 0
                disagree = row[cols.index('n_disagree')] if 'n_disagree' in cols else 0
                report(f"  {domain:25s} {field:35s} agree={agree:>5} disagree={disagree:>5} pct={pct}")
                if pct is not None and pct < 0.80:
                    low_concordance.append((domain, field, pct))
        except Exception as e:
            report(f"  {tn}: ERROR — {e}")

    # 6. Low concordance flagging
    report("")
    report("## 6. Low Concordance Fields (pct_agree < 0.80)")
    if low_concordance:
        con.execute("""
            CREATE OR REPLACE TABLE manuscript_workspace.verification_low_concordance_v1 (
                domain VARCHAR, field_name VARCHAR, pct_agree DOUBLE, flagged_at TIMESTAMP
            )
        """)
        for domain, field, pct in low_concordance:
            report(f"  {domain:25s} {field:35s} pct={pct}")
            con.execute("""
                INSERT INTO manuscript_workspace.verification_low_concordance_v1
                VALUES (?, ?, ?, ?)
            """, [domain, field, pct, dt.datetime.utcnow()])
        report(f"  Wrote {len(low_concordance)} rows to verification_low_concordance_v1")
    else:
        report("  None — all fields >= 80% concordance")

    # 7. Archive log
    report("")
    report("## 7. Archive Move Log")
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

    # 8. Database state
    report("")
    report("## 8. Final Database State")
    for schema in ["main", "manuscript_workspace", "views_readable"]:
        tbls = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
        """).fetchone()[0]
        vws = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_views()
            WHERE schema_name = '{schema}'
        """).fetchone()[0]
        report(f"  {schema}: {tbls} tables, {vws} views")

    # 9. Domain -> verify table mapping
    report("")
    report("## 9. Domain -> Verify Table Mapping")
    verify_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name LIKE 'verify_%_v1'
          AND table_name NOT LIKE '%summary%'
        ORDER BY table_name
    """).fetchall()
    for vt in verify_tables:
        r = con.execute(f'SELECT COUNT(*) FROM main."{vt[0]}"').fetchone()[0]
        report(f"  {vt[0]:50s} {r:>8} rows")

    report("")
    report("=" * 72)
    report("Verification complete.")

    # Write report
    report_path = os.path.join(OUTPUT_DIR, "326_postcleanup_audit.md")
    with open(report_path, "w") as f:
        f.write("# Post-Cleanup Audit v2 (Script 326)\n\n")
        f.write(f"Generated: {dt.datetime.utcnow().isoformat()}Z\n\n")
        for line in report_lines:
            f.write(line + "\n")
    log(f"  Report written to {report_path}")

    # 10. Git log confirmation
    log("")
    log("## 10. Git Commit Log (304-326)")
    try:
        result = subprocess.run(
            ["git", "log", "--oneline",
             "scripts/30[4-9]*.py", "scripts/31*.py", "scripts/32*.py"],
            capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        for line in result.stdout.strip().split("\n"):
            log(f"  {line}")
    except Exception as e:
        log(f"  git log error: {e}")


if __name__ == "__main__":
    main()
