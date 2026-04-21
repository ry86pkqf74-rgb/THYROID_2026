"""
Script 336 — Final main-schema audit + __readme refresh.

Runs after Scripts 327–335 complete.  Produces:
  1. Categorical summary of main objects
  2. Orphan detection
  3. __readme refresh with categorical map + timestamp + git sha
  4. CPM invariants
  5. Tier 2 completeness invariant
  6. verify_* concordance summary
  7. scripts/output/336_postcleanup_audit_round3.md

Usage:
    python 336_final_main_audit.py
"""
import datetime as dt
import os
import subprocess

from _md_connect import connect_locked

SCRIPT = "336_final_main_audit"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

CATEGORIES = {
    "CPM": lambda n: n == "canonical_patient_master",
    "canonical_*": lambda n: n.startswith("canonical_") and n != "canonical_patient_master",
    "note_entities_llm_*": lambda n: n.startswith("note_entities_llm_"),
    "note_entities_*": lambda n: n.startswith("note_entities_") and not n.startswith("note_entities_llm_"),
    "verify_*": lambda n: n.startswith("verify_"),
    "path_*": lambda n: n.startswith("path_") and not n.startswith("path_size_"),
    "tirads_v2_*": lambda n: n.startswith("tirads_v2_"),
    "us_*": lambda n: n.startswith("us_") or n.startswith("canonical_us_"),
    "molecular_*": lambda n: n.startswith("molecular_") or n.startswith("genetics_"),
    "operative_*": lambda n: n.startswith("operative_"),
    "lab_*": lambda n: n.startswith("longitudinal_lab_") or n.startswith("lab_"),
    "complication_*": lambda n: n.startswith("complication_") or n.startswith("comp_"),
    "Tier2_event/wide": lambda n: n.endswith("_event_v1") or n.endswith("_patient_wide_v1"),
    "adjudication": lambda n: "adjudication" in n or "discordance" in n,
    "rai_*": lambda n: n.startswith("rai_"),
    "fna_*": lambda n: n.startswith("fna_"),
    "ln_*": lambda n: n.startswith("ln_"),
    "frozen_section_*": lambda n: n.startswith("frozen_section_"),
    "meta": lambda n: n in ("__readme", "data_dictionary_v279"),
    "source_excel": lambda n: n in (
        "clinical_notes_long", "path_synoptics", "ultrasound_reports",
        "ct_imaging", "mri_imaging", "nuclear_med", "fna_cytology",
        "molecular_results", "molecular_testing", "molecular_variant_long",
        "fna_history", "tumor_episode_master_v2",
    ),
    "nsqip": lambda n: n.startswith("nsqip_"),
    "synoptic_*": lambda n: n.startswith("synoptic_"),
    "survival_*": lambda n: n.startswith("survival_") or n.startswith("canonical_survival_"),
    "recurrence_*": lambda n: n.startswith("canonical_recurrence_") or n.startswith("recurrence_"),
}


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def categorize(table_name):
    for cat, predicate in CATEGORIES.items():
        if predicate(table_name):
            return cat
    return "ORPHAN"


def main():
    con = connect_locked()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_lines = []

    def report(line):
        log(line)
        report_lines.append(line)

    report("=" * 72)
    report("Script 336 — Final main-schema audit (Round 3)")
    report("=" * 72)

    # 1. Categorical summary
    report("")
    report("## 1. Main Object Inventory")
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()
    table_names = [t[0] for t in tables]

    views = con.execute("""
        SELECT view_name FROM duckdb_views()
        WHERE schema_name = 'main'
        ORDER BY view_name
    """).fetchall()
    view_names = [v[0] for v in views]

    all_objects = table_names + view_names
    report(f"  Total: {len(table_names)} tables + {len(view_names)} views = {len(all_objects)} objects")

    cat_counts = {}
    orphans = []
    for obj in all_objects:
        cat = categorize(obj)
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        if cat == "ORPHAN":
            orphans.append(obj)

    report("")
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        report(f"  {cat:30s} {count:4d}")

    # 2. Orphan listing
    report("")
    report("## 2. Orphans (uncategorized)")
    if orphans:
        for o in sorted(orphans):
            report(f"  {o}")
    else:
        report("  None")

    # 3. __readme refresh
    report("")
    report("## 3. __readme Refresh")
    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).decode().strip()[:12]
    except Exception:
        git_sha = "unknown"

    utcz = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    readme_content = f"""Thyroid 2026 Publication Database — thyroid_canonical_publication_v1_0
Last updated: {utcz}
Git SHA: {git_sha}
Script: {SCRIPT}

Object count: {len(all_objects)} ({len(table_names)} tables, {len(view_names)} views)

Categories:
"""
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        readme_content += f"  {cat}: {count}\n"

    readme_content += f"\nOrphans: {len(orphans)}\n"
    for o in sorted(orphans):
        readme_content += f"  - {o}\n"

    readme_content += "\nDO NOT DROP tables listed under 'source_excel' — these are Excel import provenance.\n"

    try:
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
        """, [readme_content, dt.datetime.utcnow(), git_sha, SCRIPT])
        report(f"  __readme refreshed (sha={git_sha})")
    except Exception as e:
        report(f"  __readme refresh failed: {e}")

    # 4. CPM invariants
    report("")
    report("## 4. CPM Invariants")
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

    cpm_cols = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master'
    """).fetchone()[0]
    report(f"  CPM column count: {cpm_cols}")

    # Gap closure deltas (Scripts 327-334)
    report("")
    report("## 5. Gap Closure Deltas (Scripts 327-334)")

    gap_checks = [
        ("operative_episode_detail_v2 distinct RIDs",
         "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2",
         10800),
        ("tirads_v2_worst_category nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE tirads_v2_worst_category IS NOT NULL",
         3021),
        ("tirads_v2_any_suspicious_ln_on_us nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE tirads_v2_any_suspicious_ln_on_us IS NOT NULL",
         4073),
        ("comp_vc_paralysis_evidence_tier nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE comp_vc_paralysis_evidence_tier IS NOT NULL",
         88),
        ("comp_vc_paresis_evidence_tier nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE comp_vc_paresis_evidence_tier IS NOT NULL",
         71),
        ("postop_calcium_min_value nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE postop_calcium_min_value IS NOT NULL",
         1000),
        ("comp_hypocalcemia_confirmed TRUE",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE comp_hypocalcemia_confirmed = TRUE",
         300),
        ("path_stage_raw nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE path_stage_raw IS NOT NULL",
         5000),
        ("rai_scan_findings_v9 nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE rai_scan_findings_v9 IS NOT NULL",
         1500),
        ("op_esophageal_inv_any nonnull",
         "SELECT COUNT(*) FROM main.canonical_patient_master WHERE op_esophageal_inv_any IS NOT NULL",
         300),
    ]

    for label, sql, threshold in gap_checks:
        try:
            val = con.execute(sql).fetchone()[0]
            status = "PASS" if val >= threshold else f"BELOW ({val} < {threshold})"
            report(f"  {label}: {val} (threshold {threshold}) — {status}")
        except Exception as e:
            report(f"  {label}: ERROR — {e}")

    # 6. Tier 2 completeness
    report("")
    report("## 6. Tier 2 Completeness")
    try:
        t2 = con.execute("""
            SELECT llm_source, expected_tier2_table, has_tier2_event_table
            FROM manuscript_workspace.tier2_completeness_v1
            ORDER BY llm_source
        """).fetchall()
        n_ok = sum(1 for _, _, ok in t2 if ok)
        report(f"  {n_ok}/{len(t2)} domains have Tier 2 tables")
        for source, expected, ok in t2:
            if not ok:
                report(f"  GAP: {source} -> {expected}")
    except Exception:
        report("  tier2_completeness_v1 not found")

    # 7. verify_* concordance summary
    report("")
    report("## 7. Verification Concordance")
    try:
        verify_tables = con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name LIKE 'verify_%_summary_v1'
            ORDER BY table_name
        """).fetchall()

        for vt in verify_tables:
            vname = vt[0]
            try:
                cols = con.execute(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{vname}'
                """).fetchall()
                col_names = {c[0] for c in cols}

                if "pct_agree" in col_names and "field_name" in col_names:
                    low = con.execute(f"""
                        SELECT field_name, pct_agree FROM main."{vname}"
                        WHERE pct_agree < 0.80
                        ORDER BY pct_agree
                    """).fetchall()
                    total = con.execute(f'SELECT COUNT(*) FROM main."{vname}"').fetchone()[0]
                    report(f"  {vname}: {total} fields, "
                           f"{len(low)} low concordance (<80%)")
                    for l in low:
                        report(f"    {l[0]}: {l[1]:.1%}")
                else:
                    n = con.execute(f'SELECT COUNT(*) FROM main."{vname}"').fetchone()[0]
                    report(f"  {vname}: {n} rows")
            except Exception as e:
                report(f"  {vname}: ERROR — {e}")
    except Exception:
        report("  No verify_*_summary_v1 tables found")

    # 8. Archive log
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

    # 9. Backfill log summary
    report("")
    report("## 9. Backfill Log Summary (Scripts 327-336)")
    try:
        log_entries = con.execute("""
            SELECT script, cpm_column, n_rows_updated
            FROM manuscript_workspace.cpm_backfill_log_v1
            WHERE script LIKE '3%'
            ORDER BY backfilled_at
        """).fetchall()
        for e in log_entries:
            report(f"  {e[0]:50s} {e[1]:40s} +{e[2] or 0}")
    except Exception:
        report("  cpm_backfill_log_v1 not found or empty for 3xx scripts")

    report("")
    report("=" * 72)
    report("Audit complete.")

    # Write report
    report_path = os.path.join(OUTPUT_DIR, "336_postcleanup_audit_round3.md")
    with open(report_path, "w") as f:
        f.write("# Post-Cleanup Audit Round 3 (Script 336)\n\n")
        f.write(f"Generated: {dt.datetime.utcnow().isoformat()}Z\n\n")
        for line in report_lines:
            f.write(line + "\n")
    log(f"  Report written to {report_path}")


if __name__ == "__main__":
    main()
