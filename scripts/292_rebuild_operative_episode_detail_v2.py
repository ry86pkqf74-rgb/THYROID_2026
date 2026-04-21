"""
Script 292 — Rebuild operative_episode_detail_v2 from note_entities_operative_detail.

The current operative_episode_detail_v2 hardcodes operative-detail flags to
FALSE (rln_monitoring_flag, gross_ete_flag, tracheal_involvement_flag, etc.)
because the original source table 'operative_details' was archived.

The entity data in note_entities_operative_detail has the real findings.
This script:
  1. Archives the current operative_episode_detail_v2
  2. Rolls up entities per (research_id, surgery_date) to compute flags
  3. Joins flags onto the existing episode skeleton (preserving row/rid)
  4. Verifies the episode skeleton is identical pre/post

Entity-to-column mapping:
  nerve_monitoring  → rln_monitoring_flag=TRUE when present
  rln_finding       → rln_finding_raw (norm value), rln_monitoring_flag=TRUE
  gross_invasion    → gross_ete_flag=TRUE when present & norm in (gross_ete, gross_invasion)
                      local_invasion_flag=TRUE when present
  tracheal_involvement → tracheal_involvement_flag=TRUE when present
  esophageal_involvement → esophageal_involvement_flag=TRUE when present
  parathyroid_autograft → parathyroid_autograft_flag=TRUE when present
  reoperative_field → reoperative_field_flag=TRUE when present
  strap_muscle      → strap_muscle_involvement_flag=TRUE when present
  drain_placement   → drain_flag=TRUE when present
  ebl               → ebl_ml_nlp (numeric, already present; skip)
  berry_ligament    → berry_ligament_flag=TRUE when present

Usage:
    python 292_rebuild_operative_episode_detail_v2.py            # dry-run
    python 292_rebuild_operative_episode_detail_v2.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "292_rebuild_operative_episode_detail_v2"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_archive_log(con)
    log("=" * 72)
    log(f"Script 292 — Rebuild operative_episode_detail_v2 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state
    pre_rows = con.execute(
        "SELECT COUNT(*) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    pre_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    log(f"  Pre: {pre_rows} rows, {pre_rid} distinct_rid")

    # Snapshot the skeleton for comparison
    con.execute("DROP TABLE IF EXISTS _oed_skeleton_pre")
    con.execute("""
        CREATE TEMP TABLE _oed_skeleton_pre AS
        SELECT research_id, surgery_episode_id, surgery_date_native
        FROM main.operative_episode_detail_v2
        ORDER BY research_id, surgery_episode_id
    """)

    # Current flag state (all should be FALSE/NULL)
    flag_check = con.execute("""
        SELECT
            SUM(CASE WHEN rln_monitoring_flag THEN 1 ELSE 0 END) AS rln_mon,
            SUM(CASE WHEN gross_ete_flag THEN 1 ELSE 0 END) AS ete,
            SUM(CASE WHEN tracheal_involvement_flag THEN 1 ELSE 0 END) AS trach,
            SUM(CASE WHEN esophageal_involvement_flag THEN 1 ELSE 0 END) AS esoph,
            SUM(CASE WHEN parathyroid_autograft_flag THEN 1 ELSE 0 END) AS autogr,
            SUM(CASE WHEN reoperative_field_flag THEN 1 ELSE 0 END) AS reop,
            SUM(CASE WHEN strap_muscle_involvement_flag THEN 1 ELSE 0 END) AS strap,
            SUM(CASE WHEN drain_flag THEN 1 ELSE 0 END) AS drain,
            SUM(CASE WHEN local_invasion_flag THEN 1 ELSE 0 END) AS inv
        FROM main.operative_episode_detail_v2
    """).fetchone()
    log(f"  Pre flag counts: rln_mon={flag_check[0]}, ete={flag_check[1]}, "
        f"trach={flag_check[2]}, esoph={flag_check[3]}, autogr={flag_check[4]}, "
        f"reop={flag_check[5]}, strap={flag_check[6]}, drain={flag_check[7]}, "
        f"inv={flag_check[8]}")

    # Step 1: Roll up entities per (research_id, surgery_date nearest match)
    log("  Rolling up entities per (research_id)...")
    con.execute("DROP TABLE IF EXISTS _entity_rollup")
    con.execute("""
        CREATE TEMP TABLE _entity_rollup AS
        SELECT
            research_id,
            -- nerve_monitoring: TRUE if any present
            BOOL_OR(CASE WHEN entity_type = 'nerve_monitoring'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS rln_monitoring_flag_new,
            -- rln_finding: aggregate raw strings
            STRING_AGG(DISTINCT CASE WHEN entity_type = 'rln_finding'
                                     AND present_or_negated = 'present'
                                THEN entity_value_norm END, ' | ')
                AS rln_finding_raw_new,
            -- gross_ete: TRUE if gross_invasion present with ete/invasion norm
            BOOL_OR(CASE WHEN entity_type = 'gross_invasion'
                         AND present_or_negated = 'present'
                         AND entity_value_norm IN ('gross_ete', 'gross_invasion')
                    THEN TRUE END)
                AS gross_ete_flag_new,
            -- local_invasion: TRUE if any gross_invasion present
            BOOL_OR(CASE WHEN entity_type = 'gross_invasion'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS local_invasion_flag_new,
            -- tracheal
            BOOL_OR(CASE WHEN entity_type = 'tracheal_involvement'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS tracheal_involvement_flag_new,
            -- esophageal
            BOOL_OR(CASE WHEN entity_type = 'esophageal_involvement'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS esophageal_involvement_flag_new,
            -- parathyroid_autograft
            BOOL_OR(CASE WHEN entity_type = 'parathyroid_autograft'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS parathyroid_autograft_flag_new,
            -- reoperative_field
            BOOL_OR(CASE WHEN entity_type = 'reoperative_field'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS reoperative_field_flag_new,
            -- strap_muscle
            BOOL_OR(CASE WHEN entity_type = 'strap_muscle'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS strap_muscle_involvement_flag_new,
            -- drain
            BOOL_OR(CASE WHEN entity_type = 'drain_placement'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS drain_flag_new,
            -- berry_ligament
            BOOL_OR(CASE WHEN entity_type = 'berry_ligament'
                         AND present_or_negated = 'present' THEN TRUE END)
                AS berry_ligament_flag_new,
            -- parathyroid_autograft count
            COUNT(DISTINCT CASE WHEN entity_type = 'parathyroid_autograft'
                                 AND present_or_negated = 'present'
                           THEN note_row_id END)
                AS parathyroid_autograft_count_new
        FROM main.note_entities_operative_detail
        GROUP BY research_id
    """)

    n_rollup = con.execute("SELECT COUNT(*) FROM _entity_rollup").fetchone()[0]
    log(f"  Entity rollup: {n_rollup} patients with at least one entity")

    # Preview changes
    preview = con.execute("""
        SELECT
            SUM(CASE WHEN r.rln_monitoring_flag_new THEN 1 ELSE 0 END) AS rln_mon,
            SUM(CASE WHEN r.gross_ete_flag_new THEN 1 ELSE 0 END) AS ete,
            SUM(CASE WHEN r.tracheal_involvement_flag_new THEN 1 ELSE 0 END) AS trach,
            SUM(CASE WHEN r.esophageal_involvement_flag_new THEN 1 ELSE 0 END) AS esoph,
            SUM(CASE WHEN r.parathyroid_autograft_flag_new THEN 1 ELSE 0 END) AS autogr,
            SUM(CASE WHEN r.reoperative_field_flag_new THEN 1 ELSE 0 END) AS reop,
            SUM(CASE WHEN r.strap_muscle_involvement_flag_new THEN 1 ELSE 0 END) AS strap,
            SUM(CASE WHEN r.drain_flag_new THEN 1 ELSE 0 END) AS drain,
            SUM(CASE WHEN r.local_invasion_flag_new THEN 1 ELSE 0 END) AS inv,
            SUM(CASE WHEN r.berry_ligament_flag_new THEN 1 ELSE 0 END) AS berry
        FROM _entity_rollup r
    """).fetchone()
    log(f"  New flag counts (patient-level): rln_mon={preview[0]}, ete={preview[1]}, "
        f"trach={preview[2]}, esoph={preview[3]}, autogr={preview[4]}, "
        f"reop={preview[5]}, strap={preview[6]}, drain={preview[7]}, "
        f"inv={preview[8]}, berry={preview[9]}")

    if not args.commit:
        log("  (dry-run — no changes)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Step 2: Archive current table
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"operative_episode_detail_v2_pre292_{utcz}"
    log(f"  Archiving to {ARCHIVE_DB}.{ARCHIVE_SCHEMA}.{archive_name}...")
    con.execute(f"""
        CREATE TABLE {ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}" AS
        SELECT * FROM main.operative_episode_detail_v2
    """)
    archive_count = con.execute(
        f'SELECT COUNT(*) FROM {ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'
    ).fetchone()[0]
    if archive_count != pre_rows:
        raise SystemExit(
            f"Archive count mismatch: {archive_count} != {pre_rows}"
        )
    log(f"  Archived {archive_count} rows")

    archive_fq = f"{ARCHIVE_DB}.{ARCHIVE_SCHEMA}.\"{archive_name}\""
    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """, [
        dt.datetime.utcnow(), "main", "operative_episode_detail_v2",
        archive_fq,
        archive_count,
        "Pre-rebuild archive; flags were hardcoded FALSE",
        SCRIPT
    ])

    # Step 3: Rebuild with entity-derived flags
    log("  Rebuilding operative_episode_detail_v2 with entity-derived flags...")
    con.execute("""
        CREATE OR REPLACE TABLE main.operative_episode_detail_v2 AS
        SELECT
            o.research_id,
            o.surgery_episode_id,
            o.surgery_date_native,
            o.resolved_surgery_date,
            o.date_status,
            o.procedure_raw,
            o.procedure_normalized,
            o.laterality,
            o.central_neck_dissection_flag,
            o.lateral_neck_dissection_flag,
            -- Entity-derived flags (COALESCE to preserve non-NULL existing values)
            COALESCE(r.rln_monitoring_flag_new, o.rln_monitoring_flag) AS rln_monitoring_flag,
            COALESCE(r.rln_finding_raw_new, o.rln_finding_raw) AS rln_finding_raw,
            COALESCE(r.parathyroid_autograft_flag_new, o.parathyroid_autograft_flag)
                AS parathyroid_autograft_flag,
            COALESCE(r.parathyroid_autograft_count_new, o.parathyroid_autograft_count)
                AS parathyroid_autograft_count,
            o.parathyroid_autograft_site,
            o.parathyroid_resection_flag,
            COALESCE(r.gross_ete_flag_new, o.gross_ete_flag) AS gross_ete_flag,
            COALESCE(r.local_invasion_flag_new, o.local_invasion_flag) AS local_invasion_flag,
            COALESCE(r.tracheal_involvement_flag_new, o.tracheal_involvement_flag)
                AS tracheal_involvement_flag,
            COALESCE(r.esophageal_involvement_flag_new, o.esophageal_involvement_flag)
                AS esophageal_involvement_flag,
            COALESCE(r.strap_muscle_involvement_flag_new, o.strap_muscle_involvement_flag)
                AS strap_muscle_involvement_flag,
            COALESCE(r.reoperative_field_flag_new, o.reoperative_field_flag)
                AS reoperative_field_flag,
            o.ebl_ml,
            COALESCE(r.drain_flag_new, o.drain_flag) AS drain_flag,
            o.operative_findings_raw,
            o.source_tables,
            o.op_confidence,
            o.note_date_resolved,
            o.note_date_source,
            o.note_date_confidence,
            o.parathyroid_identified_count,
            COALESCE(o.frozen_section_flag, FALSE) AS frozen_section_flag,
            COALESCE(r.berry_ligament_flag_new, o.berry_ligament_flag) AS berry_ligament_flag,
            o.ebl_ml_nlp,
            o.op_enrichment_source,
            o.linked_pathology_episode_id,
            o.path_link_score_v3,
            o.linked_fna_episode_id,
            o.fna_link_score_v3
        FROM main.operative_episode_detail_v2 o
        LEFT JOIN _entity_rollup r ON o.research_id = r.research_id
    """)

    # Step 4: Verify skeleton identical
    post_rows = con.execute(
        "SELECT COUNT(*) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    post_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    log(f"  Post: {post_rows} rows, {post_rid} distinct_rid")

    if post_rows != pre_rows:
        raise SystemExit(
            f"SKELETON MISMATCH: row count changed {pre_rows} -> {post_rows}"
        )
    if post_rid != pre_rid:
        raise SystemExit(
            f"SKELETON MISMATCH: rid count changed {pre_rid} -> {post_rid}"
        )

    skeleton_diff = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT research_id, surgery_episode_id, surgery_date_native
            FROM main.operative_episode_detail_v2
            EXCEPT
            SELECT research_id, surgery_episode_id, surgery_date_native
            FROM _oed_skeleton_pre
        )
    """).fetchone()[0]
    if skeleton_diff != 0:
        raise SystemExit(
            f"SKELETON MISMATCH: {skeleton_diff} rows differ in "
            f"(research_id, surgery_episode_id, surgery_date_native)"
        )
    log("  Skeleton verified identical")

    # Post-flag counts
    post_flags = con.execute("""
        SELECT
            SUM(CASE WHEN rln_monitoring_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN gross_ete_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN tracheal_involvement_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN esophageal_involvement_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN parathyroid_autograft_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN reoperative_field_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN strap_muscle_involvement_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN drain_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN local_invasion_flag THEN 1 ELSE 0 END),
            SUM(CASE WHEN berry_ligament_flag THEN 1 ELSE 0 END)
        FROM main.operative_episode_detail_v2
    """).fetchone()
    log(f"  Post flag counts: rln_mon={post_flags[0]}, ete={post_flags[1]}, "
        f"trach={post_flags[2]}, esoph={post_flags[3]}, autogr={post_flags[4]}, "
        f"reop={post_flags[5]}, strap={post_flags[6]}, drain={post_flags[7]}, "
        f"inv={post_flags[8]}, berry={post_flags[9]}")

    # Step 5: Roll up to CPM op_*_any columns (v1-NULL-only policy)
    log("  Rolling up to CPM op_* columns (v1-NULL-only)...")

    cpm_rollups = [
        ("op_esophageal_inv_any",
         "esophageal_involvement_flag",
         "esophageal involvement from note entities"),
        ("op_tracheal_inv_any",
         "tracheal_involvement_flag",
         "tracheal involvement from note entities"),
        ("op_rln_monitoring_any",
         "rln_monitoring_flag",
         "RLN monitoring from note entities"),
    ]

    ensure_log_table(con)
    for cpm_col, oed_col, desc in cpm_rollups:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM column {cpm_col} does not exist — skipping")
            continue

        con.execute(f"DROP TABLE IF EXISTS _cpm_rollup_{oed_col}")
        con.execute(f"""
            CREATE TEMP TABLE _cpm_rollup_{oed_col} AS
            SELECT research_id, BOOL_OR("{oed_col}") AS value
            FROM main.operative_episode_detail_v2
            WHERE "{oed_col}" = TRUE
            GROUP BY research_id
        """)
        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN _cpm_rollup_{oed_col} r ON c.research_id = r.research_id
            WHERE c."{cpm_col}" IS NULL AND r.value = TRUE
        """).fetchone()[0]

        if plan_n > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master AS c
                   SET "{cpm_col}" = TRUE
                  FROM _cpm_rollup_{oed_col} AS r
                 WHERE c.research_id = r.research_id
                   AND c."{cpm_col}" IS NULL
                   AND r.value = TRUE
            """)
            log(f"  CPM.{cpm_col}: set TRUE for {plan_n} patients (v1-NULL-only)")

            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                dt.datetime.utcnow(), cpm_col, desc,
                "v1 NULL only; from operative_episode_detail_v2 rebuild",
                plan_n, None, None, SCRIPT
            ])
        else:
            log(f"  CPM.{cpm_col}: 0 new fills (all already populated or no entities)")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 292 complete.")


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


if __name__ == "__main__":
    main()
