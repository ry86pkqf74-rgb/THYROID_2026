"""
Script 297 — Archive stale objects from V1_0 to archive_pub_v1_0.

Auto-archives unambiguous stale objects (broken suffix, snapshots, v1_1
finalization, versioned review tables). Writes conditional candidates to
manuscript_workspace.archive_candidate_review_v1 for Logan's review.

Usage:
    python 297_archive_stale_objects.py            # dry-run
    python 297_archive_stale_objects.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "297_archive_stale_objects"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"

# (schema, table_name, reason)
AUTO_ARCHIVE = [
    ("main", "note_entities_llm_synoptic_pathology_enrichment__march2026_broken",
     "Explicit _broken suffix"),
    ("main", "_molecular_patient_rollup_v227",
     "Leading underscore + versioned staging artifact"),
    ("manuscript_workspace", "canonical_cleanup_audit_v1_snapshot_20260417",
     "Snapshot table"),
    ("manuscript_workspace", "manuscript_dive_map_v1_pre272_snapshot",
     "Snapshot table"),
    ("manuscript_workspace", "view_definitions_snapshot_bigcleanup",
     "Snapshot table"),
    ("manuscript_workspace", "collision_resolution_v265",
     "Versioned review artifact"),
    ("manuscript_workspace", "cpm_cols_unmapped_review_v265",
     "Versioned review artifact"),
    ("manuscript_workspace", "cpm_unmapped_triage_v266a",
     "Versioned review artifact"),
    ("manuscript_workspace", "fusion_flag_unparsed_review_v265",
     "Versioned review artifact"),
    ("manuscript_workspace", "fusion_parse_error_review_v265",
     "Versioned review artifact"),
    ("manuscript_workspace", "ln_extract_noncohort_orphan_v279",
     "Versioned review artifact"),
    ("manuscript_workspace", "registry_end_to_end_validation_v273",
     "Versioned validation"),
    ("manuscript_workspace", "registry_v2_resolution_audit_v273",
     "Versioned audit"),
    ("manuscript_workspace", "registry_v2_unresolved_pointers_v273",
     "Versioned review"),
    ("manuscript_workspace", "thin_wrapper_pi_review_v273",
     "Versioned review"),
    ("manuscript_workspace", "vc_paralysis_recalibration_v236",
     "Superseded by Script 295 VC tiering"),
    # v1_1 finalization artifacts
    ("manuscript_workspace", "legacy_column_sweep_v1_1",
     "v1_1 finalization artifact"),
    ("manuscript_workspace", "nan_string_audit_v1_1",
     "v1_1 finalization artifact"),
    ("manuscript_workspace", "registry_normalization_review_v1_1",
     "v1_1 finalization artifact"),
    ("manuscript_workspace", "v1_1_finalization_audit_v1",
     "v1_1 finalization artifact"),
    ("manuscript_workspace", "v1_1_tech_debt_v1",
     "v1_1 finalization artifact"),
]

# Conditional: write to review table, do NOT auto-archive
CONDITIONAL = [
    ("main", "data_dictionary_v279",
     "Versioned — check for newer; might be current dict"),
    ("main", "path_size_adjudication_v241",
     "Versioned adjudication — check references"),
    ("main", "ret_note_entity_adjudication_v226",
     "Versioned — check references"),
    ("main", "ret_patient_adjudicated_v226",
     "Versioned — check references"),
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


def ensure_archive_log(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP,
            src_schema VARCHAR,
            src_table VARCHAR,
            archive_fq VARCHAR,
            n_rows BIGINT,
            reason VARCHAR,
            script VARCHAR
        )
    """)


def table_exists(con, schema, table):
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchone()[0] > 0


def check_references(con, table_name):
    """Check if any view references this table."""
    refs = con.execute(f"""
        SELECT DISTINCT
            database_name || '.' || schema_name || '.' ||
            COALESCE(view_name, 'unknown') AS ref
        FROM duckdb_views()
        WHERE sql ILIKE '%{table_name}%'
          AND view_name != '{table_name}'
    """).fetchall()
    return [r[0] for r in refs]


def archive_one(con, schema, table_name, reason, commit):
    """Archive one table. Returns (success, n_rows, method)."""
    if not table_exists(con, schema, table_name):
        log(f"    {schema}.{table_name}: NOT FOUND — skipping")
        return False, 0, "not_found"

    src_fq = f'"{schema}"."{table_name}"'
    n_rows = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]

    refs = check_references(con, table_name)
    if refs:
        ref_list = ", ".join(refs[:5])
        log(f"    {schema}.{table_name}: REFERENCED by {ref_list} — SKIPPING")
        return False, n_rows, "referenced"

    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"{table_name}_pre297_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'

    log(f"    {schema}.{table_name}: {n_rows} rows, no refs — archiving")

    if not commit:
        log(f"    (dry-run — would archive to {archive_name})")
        return True, n_rows, "dry_run"

    con.execute(f"CREATE TABLE {archive_fq} AS SELECT * FROM {src_fq}")

    dest_count = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if dest_count != n_rows:
        raise SystemExit(
            f"Archive count mismatch for {table_name}: "
            f"src={n_rows}, dest={dest_count}"
        )

    con.execute(f"DROP TABLE {src_fq}")
    log(f"    Archived + dropped: {n_rows} rows -> {archive_name}")

    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), schema, table_name, archive_fq,
          n_rows, reason, SCRIPT])

    return True, n_rows, "archived"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_archive_log(con)
    log("=" * 72)
    log(f"Script 297 — Archive stale objects "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Auto-archive
    log("")
    log("=== AUTO-ARCHIVE (unambiguous) ===")
    total_archived = 0
    total_skipped = 0
    for schema, table_name, reason in AUTO_ARCHIVE:
        success, n_rows, method = archive_one(
            con, schema, table_name, reason, args.commit
        )
        if success:
            total_archived += 1
        else:
            total_skipped += 1

    log(f"\n  Auto-archive: {total_archived} archived, {total_skipped} skipped")

    # Conditional candidates -> review table
    log("")
    log("=== CONDITIONAL CANDIDATES (review table) ===")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_workspace.archive_candidate_review_v1 (
            candidate_schema VARCHAR,
            candidate_name VARCHAR,
            row_count BIGINT,
            n_views_referencing INTEGER,
            referencing_objects_list VARCHAR,
            suggested_action VARCHAR,
            reason VARCHAR,
            approved BOOLEAN DEFAULT FALSE
        )
    """)

    for schema, table_name, reason in CONDITIONAL:
        if not table_exists(con, schema, table_name):
            log(f"    {schema}.{table_name}: NOT FOUND — skipping")
            continue

        src_fq = f'"{schema}"."{table_name}"'
        n_rows = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]
        refs = check_references(con, table_name)
        n_refs = len(refs)
        refs_str = ", ".join(refs[:10]) if refs else "none"

        suggested = "archive" if n_refs == 0 else "keep_referenced"

        con.execute("""
            INSERT INTO manuscript_workspace.archive_candidate_review_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)
        """, [schema, table_name, n_rows, n_refs, refs_str,
              suggested, reason])

        log(f"    {schema}.{table_name}: {n_rows} rows, "
            f"{n_refs} refs -> queued for review (suggested={suggested})")

    log("\n  Conditional candidates written to "
        "manuscript_workspace.archive_candidate_review_v1")
    log("  Set approved=TRUE and run a follow-up to archive approved rows.")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 297 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
