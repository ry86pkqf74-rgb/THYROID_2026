"""
Script 340 — Schema reorg final audit.

Per Prompt 4 §340:
  1. Reference sweep — scan every view DDL across all schemas in the
     publication DB for unresolved references to merged/moved
     main.<name> tables. Log to
     manuscript_workspace.schema_reorg_orphan_references_v1. (We DO NOT
     auto-rewrite view DDL — Logan reviews each.)
  2. Refresh main.__readme with the 3-schema layout.
  3. Final invariants:
       - CPM rows = distinct_rid = 10871
       - main object count ~120 (no *_event_v1, *_patient_wide_v1,
         verify_*_v1, verify_*_summary_v1)
       - tier2 = 13
       - verify = 2
       - schema_reorg_move_log_v1 has 48 rows
  4. Write scripts/output/340_schema_reorg_audit.md with pre/post counts,
     merges/moves, orphans, and quick-reference query examples.

Read-only / observational — no destructive operations except __readme
refresh.

Usage:
    python 340_schema_reorg_audit.py
"""
from __future__ import annotations

import datetime as dt
import os
import subprocess

from _md_connect import connect_locked

SCRIPT = "340_schema_reorg_audit"
DB = "thyroid_canonical_publication_v1_0"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# All names that were dropped from main during 337 / 338 / 339.
DROPPED_FROM_MAIN = []
# 12 verify_*_summary_v1 (Script 337)
for d in ["airway_invasion", "frozen_section", "genetics_per_test", "labs",
          "ln", "operative", "parathyroid", "pathology_synoptics", "rai",
          "recurrence", "us_nodule", "vascular_invasion"]:
    DROPPED_FROM_MAIN.append((f"verify_{d}_summary_v1",
                              "verify.concordance_master_v1",
                              f"WHERE domain='{d}'"))
# 12 verify_<domain>_v1 detail (Script 338)
for d in ["airway_invasion", "frozen_section", "genetics_per_test", "labs",
          "ln", "operative", "parathyroid", "pathology_synoptics", "rai",
          "recurrence", "us_nodule", "vascular_invasion"]:
    DROPPED_FROM_MAIN.append((f"verify_{d}_v1",
                              "verify.verify_long_v1",
                              f"WHERE domain='{d}' AND field_name='...'"))
# 12 *_patient_wide_v1 (Script 339A)
for t in ["airway_invasion_patient_wide_v1",
          "dynamic_risk_response_patient_wide_v1",
          "frozen_section_patient_wide_v1",
          "functional_outcomes_patient_wide_v1",
          "parathyroid_patient_wide_v1",
          "past_medical_hx_patient_wide_v1",
          "past_surgical_hx_patient_wide_v1",
          "patient_decision_adherence_patient_wide_v1",
          "physical_exam_patient_wide_v1",
          "presenting_symptoms_patient_wide_v1",
          "rad_treatment_patient_wide_v1",
          "vascular_invasion_patient_wide_v1"]:
    prefix = t[:-len("_patient_wide_v1")]
    DROPPED_FROM_MAIN.append((t, "tier2.patient_tier2_master_v1",
                              f"columns prefixed '{prefix}__'"))
# 12 *_event_v1 (Script 339B)
for t in ["airway_invasion_event_v1", "dynamic_risk_response_event_v1",
          "frozen_section_event_v1", "functional_outcomes_event_v1",
          "parathyroid_detail_event_v1", "past_medical_hx_event_v1",
          "past_surgical_hx_event_v1", "patient_decision_adherence_event_v1",
          "physical_exam_event_v1", "presenting_symptoms_event_v1",
          "rad_treatment_event_v1", "vascular_invasion_event_v1"]:
    DROPPED_FROM_MAIN.append((t, f"tier2.{t}", "(direct rename)"))


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


def ensure_logs(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.schema_reorg_orphan_references_v1 (
            detected_at TIMESTAMP,
            source_schema VARCHAR,
            source_name VARCHAR,
            ref_database VARCHAR,
            ref_schema VARCHAR,
            ref_name VARCHAR,
            ref_type VARCHAR,
            note VARCHAR,
            script VARCHAR
        )
    """)


def reference_sweep(con):
    """Find every view in the publication DB whose SQL still references a
    dropped main.<name>. Log each to schema_reorg_orphan_references_v1.
    Returns list of (source_name, dest, hint, ref_db, ref_schema, ref_name).
    """
    findings = []
    for (src_name, dest_obj, dest_hint) in DROPPED_FROM_MAIN:
        # Match `main.<name>`, `main."<name>"`, or `"main"."<name>"` in SQL.
        # Use case-insensitive SQL ILIKE; allow optional double-quotes.
        rows = con.execute(f"""
            SELECT DISTINCT database_name, schema_name, view_name, sql
              FROM duckdb_views()
             WHERE schema_name != 'archive_pub_v1_0'
               AND (sql ILIKE '%main.{src_name}%'
                    OR sql ILIKE '%main."{src_name}"%'
                    OR sql ILIKE '%"main"."{src_name}"%')
        """).fetchall()
        for (ref_db, ref_sch, ref_view, _sql) in rows:
            findings.append((src_name, dest_obj, dest_hint,
                             ref_db, ref_sch, ref_view))
    return findings


def log_orphans(con, findings):
    for (src_name, dest_obj, hint, ref_db, ref_sch, ref_view) in findings:
        con.execute("""
            INSERT INTO manuscript_workspace.schema_reorg_orphan_references_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), "main", src_name,
              ref_db, ref_sch, ref_view, "view",
              f"Rewrite to {dest_obj} ({hint})",
              SCRIPT])


def schema_object_counts(con, schema):
    n_t = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='{schema}'
    """).fetchone()[0]
    n_v = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_views()
         WHERE database_name='{DB}' AND schema_name='{schema}'
    """).fetchone()[0]
    return n_t, n_v


def list_schemas(con):
    return [r[0] for r in con.execute(f"""
        SELECT schema_name FROM duckdb_schemas()
         WHERE database_name='{DB}' ORDER BY schema_name
    """).fetchall()]


def get_git_sha():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).decode().strip()[:12]
    except Exception:
        return "unknown"


README_BODY = """thyroid_canonical_publication_v1_0 schema map (post-reorg {ts})

main ({n_main} objects)
  canonical production truth:
    canonical_patient_master, canonical_*_v1, clinical_notes_long,
    note_entities_llm_* (raw LLM source-of-truth tables),
    note_entities_* (older parsed tables), Excel source tables,
    domain masters (tirads_v2_*, imaging_*, molecular_*, complication_*,
    rai_treatment_episode_v2, operative_episode_detail_v2,
    longitudinal_lab_canonical_v1, thyroglobulin_lab_canonical_v1, tg_*,
    ln_master_rollup_v1, synoptic_tumor_long_v1,
    path_outcome_classification_v1, canonical_recurrence_v1,
    fna_episode_master_v2, tumor_episode_master_v2)

tier2 ({n_tier2} objects)
  typed per-event detail (one table per domain, 12 tables):
    airway_invasion_event_v1, dynamic_risk_response_event_v1,
    frozen_section_event_v1, functional_outcomes_event_v1,
    parathyroid_detail_event_v1, past_medical_hx_event_v1,
    past_surgical_hx_event_v1, patient_decision_adherence_event_v1,
    physical_exam_event_v1, presenting_symptoms_event_v1,
    rad_treatment_event_v1, vascular_invasion_event_v1
  per-patient wide rollup across all 12 domains (one row per research_id):
    patient_tier2_master_v1

verify ({n_verify} objects)
  long-format detail (all 12 domains, melted by field):
    verify_long_v1  — research_id, domain, field_name,
                      excel_value, llm_value, source_text,
                      source_note_ref, source_note_date,
                      concordance_status, built_at
  concordance summary (all 12 domains, one row per field):
    concordance_master_v1 — domain, field_name,
                            n_rows_evaluated, n_excel_populated,
                            n_llm_populated, n_both_populated,
                            n_concordant, n_discordant_excel_only,
                            n_discordant_llm_only, n_value_mismatch,
                            concordance_pct_both_populated,
                            concordance_pct_of_excel, notes,
                            built_at, source_table

archive_pub_v1_0 (in 'Thyroid 2026 UPdated' database)
  every pre-change snapshot with _pre<NNN>_<UTCZ> naming;
  schema-reorg snapshots use _preSCHEMAREORG_<UTCZ>

manuscript_workspace
  work queues, audits, reorg logs (archive_move_log_v1,
  schema_reorg_move_log_v1, schema_reorg_orphan_references_v1),
  extraction logs

Last updated: {ts}
Git SHA: {sha}
Script: {script}
"""


def refresh_readme(con, sha):
    n_main_t, n_main_v = schema_object_counts(con, "main")
    n_tier2_t, _ = schema_object_counts(con, "tier2")
    n_verify_t, _ = schema_object_counts(con, "verify")
    body = README_BODY.format(
        ts=dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        n_main=n_main_t + n_main_v,
        n_tier2=n_tier2_t,
        n_verify=n_verify_t,
        sha=sha, script=SCRIPT,
    )
    con.execute("""
        CREATE OR REPLACE TABLE main.__readme (
            content VARCHAR,
            updated_at TIMESTAMP,
            git_sha VARCHAR,
            script VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO main.__readme VALUES (?, ?, ?, ?)
    """, [body, dt.datetime.utcnow(), sha, SCRIPT])
    return body


def main():
    con = connect_locked()
    ensure_logs(con)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log("=" * 72)
    log("Script 340 — schema reorg final audit")
    log("=" * 72)
    cpm_invariants(con, "pre")

    report_lines = []

    def report(line):
        log(line)
        report_lines.append(line)

    # 1. Schema-level inventory
    report("")
    report("## 1. Schema inventory (publication DB)")
    schemas = list_schemas(con)
    for s in schemas:
        n_t, n_v = schema_object_counts(con, s)
        report(f"  {s}: {n_t} tables, {n_v} views ({n_t + n_v} objects)")

    # Confirm targets
    n_main_t, n_main_v = schema_object_counts(con, "main")
    n_tier2_t, _ = schema_object_counts(con, "tier2")
    n_verify_t, _ = schema_object_counts(con, "verify")
    n_main_total = n_main_t + n_main_v

    invariant_pass = True
    if n_tier2_t != 13:
        report(f"  WARNING: expected tier2=13, got {n_tier2_t}")
        invariant_pass = False
    if n_verify_t != 2:
        report(f"  WARNING: expected verify=2, got {n_verify_t}")
        invariant_pass = False
    # main ~ 120; tolerate +/- 30
    if not (90 <= n_main_total <= 150):
        report(f"  WARNING: main count {n_main_total} outside expected ~120 range")

    # Confirm no leftover Prompt-2 outputs in main
    leftover = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND (table_name LIKE 'verify_%_v1'
                OR table_name LIKE '%_event_v1'
                OR table_name LIKE '%_patient_wide_v1')
         ORDER BY table_name
    """).fetchall()
    report(f"  Leftover Prompt-2 outputs in main: {len(leftover)}")
    for (t,) in leftover:
        report(f"    {t}")

    # 2. Reference sweep
    report("")
    report("## 2. Reference sweep")
    findings = reference_sweep(con)
    report(f"  Found {len(findings)} view references to dropped main tables")
    if findings:
        report("  Logging to manuscript_workspace.schema_reorg_orphan_references_v1")
        log_orphans(con, findings)
        for (src, dest, hint, db, sch, view) in findings[:50]:
            report(f"    {db}.{sch}.{view}  ->  needs rewrite to "
                   f"{dest} ({hint}) [src was main.{src}]")
        if len(findings) > 50:
            report(f"    ... ({len(findings) - 50} more — see "
                   f"schema_reorg_orphan_references_v1)")

    # 3. schema_reorg_move_log_v1 row count
    report("")
    report("## 3. schema_reorg_move_log_v1 audit")
    try:
        log_rows = con.execute("""
            SELECT action, COUNT(*) AS n
              FROM manuscript_workspace.schema_reorg_move_log_v1
             GROUP BY action ORDER BY action
        """).fetchall()
        total = sum(n for _, n in log_rows)
        report(f"  Total rows: {total}")
        for (a, n) in log_rows:
            report(f"    {a}: {n}")
        if total != 48:
            report(f"  WARNING: expected 48 total rows, got {total}")
    except Exception as e:
        report(f"  schema_reorg_move_log_v1 ERROR: {e}")

    # 4. archive_move_log_v1 SCHEMAREORG entries
    report("")
    report("## 4. archive_move_log_v1 — schema-reorg entries")
    try:
        ar_rows = con.execute("""
            SELECT script, COUNT(*) AS n
              FROM manuscript_workspace.archive_move_log_v1
             WHERE script LIKE '337_%' OR script LIKE '338_%' OR script LIKE '339_%'
             GROUP BY script ORDER BY script
        """).fetchall()
        ar_total = sum(n for _, n in ar_rows)
        report(f"  Total archive entries from 337/338/339: {ar_total}")
        for (s, n) in ar_rows:
            report(f"    {s}: {n}")
    except Exception as e:
        report(f"  archive_move_log_v1 ERROR: {e}")

    # 5. tier2 + verify content checks
    report("")
    report("## 5. New schema content checks")
    try:
        n_master = con.execute(
            "SELECT COUNT(*) FROM tier2.patient_tier2_master_v1"
        ).fetchone()[0]
        rid_master = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM tier2.patient_tier2_master_v1"
        ).fetchone()[0]
        ncols_master = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='{DB}' AND schema_name='tier2'
               AND table_name='patient_tier2_master_v1'
        """).fetchone()[0]
        report(f"  tier2.patient_tier2_master_v1: rows={n_master}, "
               f"distinct_rid={rid_master}, ncols={ncols_master}")
        if n_master != 10871 or rid_master != 10871:
            report("  WARNING: master rowcount/distinct_rid not 10871")
            invariant_pass = False
    except Exception as e:
        report(f"  master ERROR: {e}")

    try:
        n_cm = con.execute(
            "SELECT COUNT(*) FROM verify.concordance_master_v1"
        ).fetchone()[0]
        d_cm = con.execute(
            "SELECT COUNT(DISTINCT domain) FROM verify.concordance_master_v1"
        ).fetchone()[0]
        report(f"  verify.concordance_master_v1: rows={n_cm}, "
               f"distinct_domains={d_cm}")
    except Exception as e:
        report(f"  concordance_master ERROR: {e}")

    try:
        n_long = con.execute(
            "SELECT COUNT(*) FROM verify.verify_long_v1"
        ).fetchone()[0]
        d_long = con.execute(
            "SELECT COUNT(DISTINCT domain) FROM verify.verify_long_v1"
        ).fetchone()[0]
        report(f"  verify.verify_long_v1: rows={n_long}, distinct_domains={d_long}")
    except Exception as e:
        report(f"  verify_long ERROR: {e}")

    # 6. tier2 event tables
    report("")
    report("## 6. tier2 event tables")
    try:
        ev = con.execute(f"""
            SELECT table_name FROM duckdb_tables()
             WHERE database_name='{DB}' AND schema_name='tier2'
               AND table_name LIKE '%_event_v1'
             ORDER BY table_name
        """).fetchall()
        report(f"  {len(ev)} event tables in tier2:")
        for (t,) in ev:
            n = con.execute(f'SELECT COUNT(*) FROM tier2."{t}"').fetchone()[0]
            report(f"    {t}: {n} rows")
    except Exception as e:
        report(f"  ERROR: {e}")

    # 7. Refresh __readme
    report("")
    report("## 7. __readme refresh")
    sha = get_git_sha()
    body = refresh_readme(con, sha)
    report(f"  __readme refreshed (sha={sha}, body={len(body)} chars)")

    cpm_invariants(con, "post")

    # 8. Write markdown report
    report_path = os.path.join(OUTPUT_DIR, "340_schema_reorg_audit.md")
    with open(report_path, "w") as f:
        f.write("# Schema Reorg Final Audit (Script 340)\n\n")
        f.write(f"Generated: {dt.datetime.utcnow().isoformat()}Z\n")
        f.write(f"Git SHA: {sha}\n\n")
        f.write("## Summary\n\n")
        f.write(f"- main: {n_main_total} objects ({n_main_t} tables, {n_main_v} views)\n")
        f.write(f"- tier2: {n_tier2_t} tables\n")
        f.write(f"- verify: {n_verify_t} tables\n")
        f.write(f"- Reference orphans logged: {len(findings)}\n")
        f.write(f"- Invariants: {'PASS' if invariant_pass else 'WARNINGS'}\n\n")
        f.write("## Detailed audit\n\n")
        for line in report_lines:
            f.write(line + "\n")
        f.write("\n## Quick-reference query examples\n\n")
        f.write("```sql\n")
        f.write("-- Manuscript concordance summary for pathology synoptics:\n")
        f.write("SELECT * FROM verify.concordance_master_v1\n")
        f.write(" WHERE domain='pathology_synoptics';\n\n")
        f.write("-- All discordant LN field comparisons:\n")
        f.write("SELECT * FROM verify.verify_long_v1\n")
        f.write(" WHERE domain='ln' AND concordance_status='disagree';\n\n")
        f.write("-- All Tier 2 flags for one patient:\n")
        f.write("SELECT * FROM tier2.patient_tier2_master_v1\n")
        f.write(" WHERE research_id='RID00001';\n\n")
        f.write("-- Per-event frozen section detail for one patient:\n")
        f.write("SELECT * FROM tier2.frozen_section_event_v1\n")
        f.write(" WHERE research_id='RID00001';\n")
        f.write("```\n")
    log(f"  Report written to {report_path}")

    log("=" * 72)
    log(f"Script 340 complete. Invariant status: "
        f"{'PASS' if invariant_pass else 'WARNINGS — see report'}")


if __name__ == "__main__":
    main()
