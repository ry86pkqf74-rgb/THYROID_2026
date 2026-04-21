"""
Script 335 — Archive round 3: redundant tables after Prompts 2 + 3.

Third archive sweep using reference-safety procedure from Script 297.
Two lists: AUTO_ARCHIVE (unreferenced → archive immediately) and
CONDITIONAL (write to archive_candidate_review_v1 for Logan's review).

DO NOT ARCHIVE: clinical_notes_long, path_synoptics, ultrasound_reports,
ct_imaging, mri_imaging, nuclear_med, fna_cytology, molecular_results,
molecular_testing, molecular_variant_long, fna_history,
fna_episode_master_v2, tumor_episode_master_v2, __readme, canonical_*,
note_entities_llm_*, *_event_v1, *_patient_wide_v1, verify_*.

Usage:
    python 335_archive_round3.py            # dry-run
    python 335_archive_round3.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "335_archive_round3"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"

AUTO_ARCHIVE = [
    ("main", "tirads_llm_extracted_v2",
     "Haiku-era methodological comparator, superseded by tirads_v2_*"),
]

CONDITIONAL = [
    ("main", "tirads_llm_validation_v2",
     "If verify_us_nodule_v1 covers, archive"),
    ("main", "extracted_tirads_validated_v1",
     "If tirads_v2_nodule_patient_rollup_v1 + canonical_us_nodule_master_v1 supersede"),
    ("main", "tumor_pathology",
     "253-col legacy, superseded by path_synoptics + synoptic_tumor_long_v1"),
    ("main", "path_size_adjudication_v241",
     "Versioned adjudication — check references"),
    ("main", "ret_note_entity_adjudication_v226",
     "Versioned — check references"),
    ("main", "ret_patient_adjudicated_v226",
     "Versioned — check references"),
    ("main", "ete_adjudication_v1",
     "If 26 low-conf rows retained in CPM, archive"),
    ("main", "extracted_ete_subgraded_v1",
     "If CPM ETE cols supersede"),
    ("main", "data_dictionary_v279",
     "Versioned — archive only if 326 produced replacement"),
    ("main", "clinical_note_ln_extracted_v1",
     "If ln_master_rollup_v1 + verify_ln_v1 supersede"),
    ("main", "extracted_rln_injury_refined_v2",
     "If complication_phenotype_v1 absorbed"),
    ("main", "extracted_braf_recovery_v1",
     "If canonical_molecular_tested_v1 or genetics_per_test_master_v1 cover"),
    ("main", "extracted_ras_patient_summary_v1",
     "If genetics_per_test_master_v1 covers"),
    ("main", "extracted_fna_bethesda_v1",
     "If fna_episode_master_v2 covers"),
    ("main", "extracted_postop_labs_expanded_v1",
     "If longitudinal_lab_canonical_v1 covers post-331"),
    ("main", "nsqip_enrichment",
     "If enriched data merged into CPM"),
    ("main", "nsqip_patient_summary",
     "If enriched data merged into CPM"),
    ("main", "patient_completion_oed_path_linkage_v1",
     "Scratch linkage table"),
    ("main", "episode_analysis_resolved_v1_dedup",
     "Scratch"),
    ("main", "lesion_analysis_resolved_v1",
     "Scratch"),
    ("main", "patient_analysis_resolved_v1",
     "Scratch"),
    ("main", "specimen_source_xref_v1",
     "If consolidated under genetics_per_test_master_v1"),
    ("main", "specimen_master_v1",
     "If consolidated under genetics_per_test_master_v1"),
    ("main", "specimen_tumor_focus_v1",
     "If consolidated under genetics_per_test_master_v1"),
    ("main", "specimen_genomic_assay_v1",
     "If consolidated under genetics_per_test_master_v1"),
    ("main", "serial_imaging_us",
     "If canonical_us_* masters cover"),
    ("main", "thyroid_sizes",
     "If canonical_us_* masters cover"),
    ("main", "thyroid_weights",
     "If canonical_us_* masters cover"),
    ("main", "survival_cohort_enriched",
     "If canonical_survival_followup_v1 covers"),
    ("main", "tumor_stage_heterogeneity_v1",
     "If CPM exposes max-stage / heterogeneity flags"),
]

DO_NOT_ARCHIVE = {
    "clinical_notes_long", "path_synoptics", "ultrasound_reports",
    "ct_imaging", "mri_imaging", "nuclear_med", "fna_cytology",
    "molecular_results", "molecular_testing", "molecular_variant_long",
    "fna_history", "fna_episode_master_v2", "tumor_episode_master_v2",
    "__readme", "canonical_patient_master",
}

DO_NOT_ARCHIVE_PREFIXES = [
    "canonical_", "note_entities_llm_", "note_entities_",
    "verify_", "complication_",
]
DO_NOT_ARCHIVE_SUFFIXES = [
    "_event_v1", "_patient_wide_v1",
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
            moved_at TIMESTAMP, src_schema VARCHAR, src_table VARCHAR,
            archive_fq VARCHAR, n_rows BIGINT, reason VARCHAR, script VARCHAR
        )
    """)


def is_protected(table_name):
    if table_name in DO_NOT_ARCHIVE:
        return True
    for prefix in DO_NOT_ARCHIVE_PREFIXES:
        if table_name.startswith(prefix):
            return True
    for suffix in DO_NOT_ARCHIVE_SUFFIXES:
        if table_name.endswith(suffix):
            return True
    return False


def table_exists(con, schema, table):
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchone()[0] > 0


def check_references(con, table_name):
    refs = con.execute(f"""
        SELECT DISTINCT
            database_name || '.' || schema_name || '.' ||
            COALESCE(view_name, 'unknown') AS ref
        FROM duckdb_views()
        WHERE sql ILIKE '%{table_name}%'
          AND view_name != '{table_name}'
          AND schema_name != 'archive_pub_v1_0'
    """).fetchall()
    return [r[0] for r in refs]


def archive_one(con, schema, table_name, reason, commit):
    if is_protected(table_name):
        log(f"    {schema}.{table_name}: PROTECTED — skipping")
        return False, 0, "protected"

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
    archive_name = f"{table_name}_pre335_{utcz}"
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
    log(f"Script 335 — Archive round 3 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state: count main objects
    pre_tables = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
    """).fetchone()[0]
    pre_views = con.execute("""
        SELECT COUNT(*) FROM duckdb_views() WHERE schema_name = 'main'
    """).fetchone()[0]
    log(f"  Pre: {pre_tables} tables + {pre_views} views = {pre_tables + pre_views} objects in main")

    # Auto-archive
    log("")
    log("=== AUTO-ARCHIVE (unambiguous) ===")
    n_auto = 0
    n_skip = 0
    for schema, table_name, reason in AUTO_ARCHIVE:
        success, n_rows, method = archive_one(
            con, schema, table_name, reason, args.commit
        )
        if success:
            n_auto += 1
        else:
            n_skip += 1
    log(f"  Auto-archive: {n_auto} archived, {n_skip} skipped")

    # Conditional candidates
    log("")
    log("=== CONDITIONAL CANDIDATES ===")
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_candidate_review_v1 (
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

    n_conditional_auto = 0
    for schema, table_name, reason in CONDITIONAL:
        if not table_exists(con, schema, table_name):
            log(f"    {schema}.{table_name}: NOT FOUND")
            continue

        if is_protected(table_name):
            log(f"    {schema}.{table_name}: PROTECTED")
            continue

        src_fq = f'"{schema}"."{table_name}"'
        n_rows = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]
        refs = check_references(con, table_name)
        n_refs = len(refs)
        refs_str = ", ".join(refs[:10]) if refs else "none"

        if n_refs == 0:
            suggested = "auto_archive"
            if args.commit:
                success, _, method = archive_one(
                    con, schema, table_name, reason, args.commit
                )
                if success:
                    n_conditional_auto += 1
                    continue
        else:
            suggested = "keep_referenced"

        con.execute("""
            INSERT INTO manuscript_workspace.archive_candidate_review_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)
        """, [schema, table_name, n_rows, n_refs, refs_str,
              suggested, reason])

        log(f"    {schema}.{table_name}: {n_rows} rows, "
            f"{n_refs} refs -> {suggested}")

    log(f"  Conditional: {n_conditional_auto} auto-archived (0 refs)")

    # Post-state
    post_tables = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
    """).fetchone()[0]
    post_views = con.execute("""
        SELECT COUNT(*) FROM duckdb_views() WHERE schema_name = 'main'
    """).fetchone()[0]
    log(f"  Post: {post_tables} tables + {post_views} views = "
        f"{post_tables + post_views} objects in main "
        f"(delta: {(pre_tables + pre_views) - (post_tables + post_views)} removed)")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 335 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
