"""Script 341 — Rebuild operative_episode_detail_v2 with real multi-episode rows.

Problem (re-verified 2026-04-21):
  - CPM has 738 RIDs with n_surgeries_v2 > 1 and authoritative surgery dates
    in first_surgery_date_v2 / second_surgery_date_v2 / third_surgery_date_v2.
  - main.operative_episode_detail_v2 has 9,371 rows / 9,368 RIDs and only
    3 RIDs with >1 row.
  - Script 327 attempted this rebuild but suppressed multi-episode rows via
    a ±7d filter against existing oed_v2 rows.

Authoritative source for episode dates (verified):
  - canonical_patient_master.{first,second,third}_surgery_date_v2 (DATE)
  - n_surgeries_v2 distribution: 1:10133, 2:698, 3:31, 4:7, 5:1, 6:1.
  - For patients with 4+ surgeries, dates beyond the third are sourced from
    note_entities_operative_detail.note_date clustering (±7d).

Strategy:
  1. Pre-state snapshot to manuscript_workspace.prompt5_remediation_log_v1.
  2. Archive existing oed_v2 to archive_pub_v1_0.
  3. Episode dates = UNION of:
       - (rid, first_surgery_date_v2, ord=1)
       - (rid, second_surgery_date_v2, ord=2)  where present
       - (rid, third_surgery_date_v2, ord=3)   where present
       - (rid, note_date) from note_entities_operative_detail
  4. Cluster within ±7d per RID, gap-and-islands. Episodes that overlap a
     CPM authoritative date are anchored on that date.
  5. Number per-RID by ascending start date → episode_rank (1..N).
  6. Aggregate detail flags per episode from note_entities_operative_detail
     joined on note_date within ±7d of cluster bounds.
  7. Build new oed_v2 = (existing-v2 episode-1 enrichment preserved) ∪
     (newly minted episodes from CPM dates and opnote clusters).
  8. Hard assertion: ≥ 700 RIDs with episode_rank > 1, else SystemExit.
  9. CREATE OR REPLACE main.operative_episode_detail_v2.
 10. Re-derive CPM rollups (op_episode_count, op_first_surgery_date,
     op_last_surgery_date, n_surgeries_from_opdetail_v2) where columns exist.
 11. Post-state snapshot + delta logging.

PHI safety: research_id only; no evidence_text or note_text in stdout.

Type notes (verified against current schema):
  research_id INTEGER, surgery_episode_id BIGINT, surgery_date_native TIMESTAMP,
  resolved_surgery_date VARCHAR (ISO date string), date_status VARCHAR,
  note_date_resolved TIMESTAMP, note_date_confidence DOUBLE,
  op_confidence DOUBLE (currently all NULL),
  op_enrichment_source INTEGER (currently all NULL — kept NULL),
  parathyroid_autograft_count BIGINT.
  surgery_episode_id is reused (only IDs 1/2/3 across 9371 rows) so we
  mint fresh sequential BIGINT ids on rebuild.

Usage:
    .venv/bin/python scripts/341_rebuild_operative_episode_multi_v2.py            # dry-run
    .venv/bin/python scripts/341_rebuild_operative_episode_multi_v2.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "341_rebuild_operative_episode_multi_v2"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"
MIN_MULTI_EPISODE_RIDS = 700


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


def ensure_log_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.prompt5_remediation_log_v1 (
            ts TIMESTAMP, script_n VARCHAR, phase VARCHAR,
            target_table VARCHAR, target_column VARCHAR,
            metric_name VARCHAR, metric_value DOUBLE,
            metric_text VARCHAR, notes VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP, src_schema VARCHAR, src_table VARCHAR,
            archive_fq VARCHAR, n_rows BIGINT, reason VARCHAR, script VARCHAR
        )
    """)


def log_metric(con, phase, target_table, target_column, metric_name,
               metric_value=None, metric_text=None, notes=None):
    con.execute("""
        INSERT INTO manuscript_workspace.prompt5_remediation_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), SCRIPT, phase, target_table, target_column,
          metric_name,
          float(metric_value) if metric_value is not None else None,
          metric_text, notes])


def snapshot_oed(con, label):
    rows = con.execute("SELECT COUNT(*) FROM main.operative_episode_detail_v2").fetchone()[0]
    rids = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    multi = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT research_id, COUNT(*) AS n
            FROM main.operative_episode_detail_v2 GROUP BY 1
        ) WHERE n > 1
    """).fetchone()[0]
    histogram = con.execute("""
        SELECT n, COUNT(*) AS n_rids FROM (
          SELECT research_id, COUNT(*) AS n
            FROM main.operative_episode_detail_v2 GROUP BY 1
        ) GROUP BY n ORDER BY n
    """).fetchall()
    hist_text = "; ".join(f"n={h[0]}:{h[1]}" for h in histogram)
    log(f"  oed_v2 [{label}]: rows={rows} rids={rids} multi_episode_rids={multi}")
    log(f"    histogram: {hist_text}")
    log_metric(con, label, "operative_episode_detail_v2", None, "rows", rows)
    log_metric(con, label, "operative_episode_detail_v2", None, "distinct_rids", rids)
    log_metric(con, label, "operative_episode_detail_v2", None,
               "multi_episode_rids", multi)
    log_metric(con, label, "operative_episode_detail_v2", None,
               "rows_per_rid_histogram", metric_text=hist_text)
    return rows, rids, multi


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_tables(con)
    log("=" * 72)
    log(f"Script 341 — REBUILD operative_episode_detail_v2 multi-episode "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")

    pre_rows, pre_rids, pre_multi = snapshot_oed(con, "pre")
    pre_multi_cpm = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master WHERE n_surgeries_v2 > 1
    """).fetchone()[0]
    log_metric(con, "pre", "canonical_patient_master", "n_surgeries_v2",
               "rids_with_n_surgeries_gt_1", pre_multi_cpm)
    log(f"  CPM n_surgeries_v2>1 = {pre_multi_cpm}")

    log("  Step 1: building authoritative episode date set from CPM v2 columns")
    # Capture the CPM cohort RID set up front; we filter every later stage by
    # this set so off-cohort orphans in note_entities_operative_detail never
    # land in the rebuilt table.
    con.execute("DROP TABLE IF EXISTS _cpm_cohort")
    con.execute("""
        CREATE TEMP TABLE _cpm_cohort AS
        SELECT CAST(research_id AS VARCHAR) AS research_id
          FROM main.canonical_patient_master
    """)
    con.execute("DROP TABLE IF EXISTS _cpm_dates")
    con.execute("""
        CREATE TEMP TABLE _cpm_dates AS
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               first_surgery_date_v2 AS op_date,
               1 AS cpm_ordinal
          FROM main.canonical_patient_master
         WHERE first_surgery_date_v2 IS NOT NULL
        UNION ALL
        SELECT CAST(research_id AS VARCHAR), second_surgery_date_v2, 2
          FROM main.canonical_patient_master
         WHERE second_surgery_date_v2 IS NOT NULL
        UNION ALL
        SELECT CAST(research_id AS VARCHAR), third_surgery_date_v2, 3
          FROM main.canonical_patient_master
         WHERE third_surgery_date_v2 IS NOT NULL
    """)
    n = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _cpm_dates
    """).fetchone()
    log(f"    CPM v2 dates: rows={n[0]} rids={n[1]}")

    log("  Step 2: adding opnote-entity dates as supplementary source (cohort-filtered)")
    con.execute("DROP TABLE IF EXISTS _opnote_dates")
    con.execute("""
        CREATE TEMP TABLE _opnote_dates AS
        SELECT DISTINCT
               CAST(ne.research_id AS VARCHAR) AS research_id,
               ne.note_row_id,
               CAST(ne.note_date AS DATE) AS op_date
          FROM main.note_entities_operative_detail ne
          JOIN _cpm_cohort c ON c.research_id = CAST(ne.research_id AS VARCHAR)
         WHERE ne.note_date IS NOT NULL
           AND CAST(ne.note_date AS DATE) >= DATE '1990-01-01'
           AND CAST(ne.note_date AS DATE) <= CURRENT_DATE
    """)
    n = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id), COUNT(DISTINCT note_row_id)
          FROM _opnote_dates
    """).fetchone()
    log(f"    opnote-entity dates: rows={n[0]} rids={n[1]} distinct_notes={n[2]}")

    log("  Step 3: union (CPM authoritative + opnote supplementary)")
    con.execute("DROP TABLE IF EXISTS _all_dates")
    con.execute("""
        CREATE TEMP TABLE _all_dates AS
        WITH u AS (
            SELECT research_id, op_date,
                   CAST(NULL AS VARCHAR) AS note_row_id,
                   CAST(cpm_ordinal AS INTEGER) AS cpm_ordinal,
                   'cpm_v2' AS src
              FROM _cpm_dates
            UNION ALL
            SELECT research_id, op_date, note_row_id,
                   CAST(NULL AS INTEGER) AS cpm_ordinal,
                   'opnote' AS src
              FROM _opnote_dates
        )
        SELECT research_id, op_date,
               LIST(DISTINCT note_row_id) FILTER (WHERE note_row_id IS NOT NULL) AS note_row_ids_on_date,
               MAX(cpm_ordinal) AS cpm_ordinal_on_date,
               BOOL_OR(src = 'cpm_v2') AS has_cpm_anchor,
               STRING_AGG(DISTINCT src, ',') AS sources
          FROM u
         GROUP BY research_id, op_date
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _all_dates").fetchone()
    log(f"    union dates rows={n[0]} rids={n[1]}")

    log("  Step 4: cluster within ±7d (gap-and-islands), prefer CPM-anchor dates")
    con.execute("DROP TABLE IF EXISTS _episodes")
    con.execute("""
        CREATE TEMP TABLE _episodes AS
        WITH ordered AS (
            SELECT research_id, op_date, note_row_ids_on_date,
                   cpm_ordinal_on_date, has_cpm_anchor, sources,
                   LAG(op_date) OVER (PARTITION BY research_id ORDER BY op_date) AS prev_date
              FROM _all_dates
        ),
        gaps AS (
            SELECT *,
                   CASE WHEN prev_date IS NULL OR (op_date - prev_date) > 7
                        THEN 1 ELSE 0 END AS is_new_cluster
              FROM ordered
        ),
        clustered AS (
            SELECT *,
                   SUM(is_new_cluster) OVER (
                       PARTITION BY research_id ORDER BY op_date
                       ROWS UNBOUNDED PRECEDING
                   ) AS cluster_id
              FROM gaps
        )
        SELECT research_id,
               cluster_id AS episode_rank,
               -- Prefer the CPM-anchor date if any in this cluster, else the
               -- earliest opnote date in the cluster.
               COALESCE(
                   MIN(CASE WHEN has_cpm_anchor THEN op_date END),
                   MIN(op_date)
               ) AS episode_anchor_date,
               MIN(op_date) AS episode_start_date,
               MAX(op_date) AS episode_end_date,
               COUNT(*) AS n_dates_in_cluster,
               BOOL_OR(has_cpm_anchor) AS has_cpm_anchor,
               MAX(cpm_ordinal_on_date) AS cpm_ordinal,
               STRING_AGG(DISTINCT sources, ',') AS source_mix,
               FLATTEN(LIST(note_row_ids_on_date)) AS note_row_ids_in_episode
          FROM clustered
         GROUP BY research_id, cluster_id
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _episodes").fetchone()
    multi_planned = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT research_id, COUNT(*) AS n FROM _episodes GROUP BY 1
        ) WHERE n > 1
    """).fetchone()[0]
    log(f"    episodes rows={n[0]} rids={n[1]} multi_episode_rids_planned={multi_planned}")

    log("  Step 5: aggregating per-episode entity flags from note_entities_operative_detail")
    con.execute("DROP TABLE IF EXISTS _episode_flags")
    con.execute("""
        CREATE TEMP TABLE _episode_flags AS
        SELECT
            ep.research_id,
            ep.episode_rank,
            ep.episode_anchor_date,
            ep.episode_start_date,
            ep.episode_end_date,
            ep.n_dates_in_cluster,
            ep.has_cpm_anchor,
            ep.cpm_ordinal,
            ep.source_mix,
            BOOL_OR(CASE WHEN ne.entity_type='nerve_monitoring'
                          AND ne.present_or_negated='present' THEN TRUE END) AS rln_monitoring_flag,
            STRING_AGG(DISTINCT CASE WHEN ne.entity_type='rln_finding'
                                          AND ne.present_or_negated='present'
                                     THEN ne.entity_value_norm END, ' | ') AS rln_finding_raw,
            BOOL_OR(CASE WHEN ne.entity_type='gross_invasion'
                          AND ne.present_or_negated='present'
                          AND ne.entity_value_norm IN ('gross_ete','gross_invasion') THEN TRUE END) AS gross_ete_flag,
            BOOL_OR(CASE WHEN ne.entity_type='gross_invasion'
                          AND ne.present_or_negated='present' THEN TRUE END) AS local_invasion_flag,
            BOOL_OR(CASE WHEN ne.entity_type='tracheal_involvement'
                          AND ne.present_or_negated='present' THEN TRUE END) AS tracheal_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type='esophageal_involvement'
                          AND ne.present_or_negated='present' THEN TRUE END) AS esophageal_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type='strap_muscle'
                          AND ne.present_or_negated='present' THEN TRUE END) AS strap_muscle_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type='reoperative_field'
                          AND ne.present_or_negated='present' THEN TRUE END) AS reoperative_field_flag,
            BOOL_OR(CASE WHEN ne.entity_type='drain_placement'
                          AND ne.present_or_negated='present' THEN TRUE END) AS drain_flag,
            BOOL_OR(CASE WHEN ne.entity_type='berry_ligament'
                          AND ne.present_or_negated='present' THEN TRUE END) AS berry_ligament_flag,
            BOOL_OR(CASE WHEN ne.entity_type='parathyroid_autograft'
                          AND ne.present_or_negated='present' THEN TRUE END) AS parathyroid_autograft_flag,
            COUNT(DISTINCT CASE WHEN ne.entity_type='parathyroid_autograft'
                                     AND ne.present_or_negated='present'
                                THEN ne.note_row_id END) AS parathyroid_autograft_count,
            MAX(CASE WHEN ne.entity_type='ebl'
                      AND ne.present_or_negated='present'
                     THEN TRY_CAST(ne.entity_value_norm AS DOUBLE) END) AS ebl_ml_nlp,
            COUNT(DISTINCT ne.note_row_id) AS n_entity_notes
          FROM _episodes ep
          LEFT JOIN main.note_entities_operative_detail ne
            ON CAST(ne.research_id AS VARCHAR) = ep.research_id
           AND CAST(ne.note_date AS DATE) BETWEEN ep.episode_start_date - 7
                                              AND ep.episode_end_date   + 7
         GROUP BY ep.research_id, ep.episode_rank, ep.episode_anchor_date,
                  ep.episode_start_date, ep.episode_end_date,
                  ep.n_dates_in_cluster, ep.has_cpm_anchor, ep.cpm_ordinal,
                  ep.source_mix
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _episode_flags").fetchone()
    log(f"    episode flags rows={n[0]} rids={n[1]}")

    log("  Step 6: matching episodes to existing oed_v2 rows for enrichment carry-over")
    con.execute("DROP TABLE IF EXISTS _existing_ranked")
    con.execute("""
        CREATE TEMP TABLE _existing_ranked AS
        SELECT *,
               COALESCE(TRY_CAST(resolved_surgery_date AS DATE),
                        CAST(surgery_date_native AS DATE)) AS _existing_op_date,
               ROW_NUMBER() OVER (
                 PARTITION BY CAST(research_id AS VARCHAR)
                 ORDER BY COALESCE(TRY_CAST(resolved_surgery_date AS DATE),
                                   CAST(surgery_date_native AS DATE)) ASC
               ) AS _existing_rank
          FROM main.operative_episode_detail_v2
    """)

    con.execute("DROP TABLE IF EXISTS _ep_to_existing")
    con.execute("""
        CREATE TEMP TABLE _ep_to_existing AS
        SELECT ef.research_id, ef.episode_rank,
               o._existing_rank AS matched_existing_rank,
               ABS(o._existing_op_date - ef.episode_anchor_date) AS day_diff,
               ROW_NUMBER() OVER (
                 PARTITION BY ef.research_id, ef.episode_rank
                 ORDER BY ABS(o._existing_op_date - ef.episode_anchor_date) ASC,
                          o._existing_rank ASC
               ) AS rn_match
          FROM _episode_flags ef
          LEFT JOIN _existing_ranked o
            ON CAST(o.research_id AS VARCHAR) = ef.research_id
           AND ABS(o._existing_op_date - ef.episode_anchor_date) <= 7
    """)

    con.execute("DROP TABLE IF EXISTS _ep_existing_unique")
    con.execute("""
        CREATE TEMP TABLE _ep_existing_unique AS
        SELECT research_id, episode_rank, matched_existing_rank
          FROM _ep_to_existing
         WHERE rn_match = 1 AND matched_existing_rank IS NOT NULL
    """)
    matched = con.execute("SELECT COUNT(*) FROM _ep_existing_unique").fetchone()[0]
    log(f"    episodes matched to an existing v2 row: {matched}")

    log("  Step 7: building rebuilt episode rows with type-preserving casts")
    con.execute("DROP TABLE IF EXISTS _oed_rebuilt")
    con.execute("""
        CREATE TEMP TABLE _oed_rebuilt AS
        SELECT
            CAST(s.research_id AS INTEGER) AS research_id,
            CAST(ROW_NUMBER() OVER (
                ORDER BY s.research_id, s.episode_rank
            ) AS BIGINT) AS surgery_episode_id,
            COALESCE(o.surgery_date_native,
                     CAST(s.episode_anchor_date AS TIMESTAMP)) AS surgery_date_native,
            COALESCE(o.resolved_surgery_date,
                     CAST(s.episode_anchor_date AS VARCHAR)) AS resolved_surgery_date,
            COALESCE(o.date_status,
                     CASE WHEN s.has_cpm_anchor THEN 'cpm_v2_anchor'
                          ELSE 'opnote_clustered' END) AS date_status,
            o.procedure_raw,
            o.procedure_normalized,
            o.laterality,
            o.central_neck_dissection_flag,
            o.lateral_neck_dissection_flag,
            COALESCE(o.rln_monitoring_flag, s.rln_monitoring_flag) AS rln_monitoring_flag,
            COALESCE(o.rln_finding_raw, s.rln_finding_raw) AS rln_finding_raw,
            COALESCE(o.parathyroid_autograft_flag, s.parathyroid_autograft_flag) AS parathyroid_autograft_flag,
            CAST(COALESCE(o.parathyroid_autograft_count, s.parathyroid_autograft_count) AS BIGINT) AS parathyroid_autograft_count,
            CAST(o.parathyroid_autograft_site AS INTEGER) AS parathyroid_autograft_site,
            o.parathyroid_resection_flag,
            COALESCE(o.gross_ete_flag, s.gross_ete_flag) AS gross_ete_flag,
            COALESCE(o.local_invasion_flag, s.local_invasion_flag) AS local_invasion_flag,
            COALESCE(o.tracheal_involvement_flag, s.tracheal_involvement_flag) AS tracheal_involvement_flag,
            COALESCE(o.esophageal_involvement_flag, s.esophageal_involvement_flag) AS esophageal_involvement_flag,
            COALESCE(o.strap_muscle_involvement_flag, s.strap_muscle_involvement_flag) AS strap_muscle_involvement_flag,
            COALESCE(o.reoperative_field_flag, s.reoperative_field_flag) AS reoperative_field_flag,
            o.ebl_ml,
            COALESCE(o.drain_flag, s.drain_flag) AS drain_flag,
            o.operative_findings_raw,
            COALESCE(o.source_tables,
                     CASE WHEN s.matched_existing_rank IS NULL
                          THEN 'note_entities_operative_detail' END) AS source_tables,
            CAST(o.op_confidence AS DOUBLE) AS op_confidence,
            COALESCE(o.note_date_resolved,
                     CAST(s.episode_anchor_date AS TIMESTAMP)) AS note_date_resolved,
            COALESCE(o.note_date_source, s.source_mix) AS note_date_source,
            CAST(COALESCE(o.note_date_confidence,
                          CASE WHEN s.has_cpm_anchor THEN 0.85
                               WHEN s.n_dates_in_cluster >= 2 THEN 0.6
                               ELSE 0.3 END) AS DOUBLE) AS note_date_confidence,
            CAST(o.parathyroid_identified_count AS INTEGER) AS parathyroid_identified_count,
            o.frozen_section_flag,
            COALESCE(o.berry_ligament_flag, s.berry_ligament_flag) AS berry_ligament_flag,
            COALESCE(o.ebl_ml_nlp, s.ebl_ml_nlp) AS ebl_ml_nlp,
            CAST(o.op_enrichment_source AS INTEGER) AS op_enrichment_source,
            CASE WHEN s.episode_rank = 1 THEN o.linked_pathology_episode_id END AS linked_pathology_episode_id,
            CASE WHEN s.episode_rank = 1 THEN o.path_link_score_v3 END AS path_link_score_v3,
            CASE WHEN s.episode_rank = 1 THEN o.linked_fna_episode_id END AS linked_fna_episode_id,
            CASE WHEN s.episode_rank = 1 THEN o.fna_link_score_v3 END AS fna_link_score_v3,
            CAST(s.episode_rank AS INTEGER) AS episode_rank,
            CAST(s.n_dates_in_cluster AS INTEGER) AS n_dates_in_cluster,
            s.source_mix AS episode_source_mix,
            CAST(s.n_entity_notes AS INTEGER) AS n_entity_notes_in_episode,
            s.has_cpm_anchor AS episode_has_cpm_anchor,
            CAST(s.cpm_ordinal AS INTEGER) AS episode_cpm_ordinal
        FROM (
            SELECT ef.*, em.matched_existing_rank
              FROM _episode_flags ef
              LEFT JOIN _ep_existing_unique em
                ON ef.research_id = em.research_id
               AND ef.episode_rank = em.episode_rank
        ) s
        LEFT JOIN _existing_ranked o
          ON CAST(o.research_id AS VARCHAR) = s.research_id
         AND o._existing_rank = s.matched_existing_rank
    """)

    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _oed_rebuilt").fetchone()
    multi_built = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT research_id, COUNT(*) AS n FROM _oed_rebuilt GROUP BY 1
        ) WHERE n > 1
    """).fetchone()[0]
    log(f"    rebuilt rows={n[0]} rids={n[1]} multi_episode_rids={multi_built}")

    hist = con.execute("""
        SELECT n, COUNT(*) AS n_rids FROM (
          SELECT research_id, COUNT(*) AS n FROM _oed_rebuilt GROUP BY 1
        ) GROUP BY n ORDER BY n
    """).fetchall()
    log("    rebuilt histogram:")
    for h in hist:
        log(f"      n_episodes={h[0]}: {h[1]} patients")

    log_metric(con, "rebuilt_preview", "operative_episode_detail_v2", None, "rows", n[0])
    log_metric(con, "rebuilt_preview", "operative_episode_detail_v2", None, "distinct_rids", n[1])
    log_metric(con, "rebuilt_preview", "operative_episode_detail_v2", None,
               "multi_episode_rids", multi_built)

    log("  Step 8: hard assertion on multi-episode rid count")
    if multi_built < MIN_MULTI_EPISODE_RIDS:
        log_metric(con, "assert_fail", "operative_episode_detail_v2", None,
                   "multi_episode_rids", multi_built,
                   notes=f"below floor of {MIN_MULTI_EPISODE_RIDS}")
        raise SystemExit(
            f"FAIL: rebuilt has only {multi_built} multi-episode RIDs "
            f"(need ≥ {MIN_MULTI_EPISODE_RIDS}). Aborting before write."
        )

    if not args.commit:
        log("  (dry-run) — no archive, no replace, no CPM update")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run) re-run with --commit to apply.")
        return

    log("  Step 9: archiving existing oed_v2")
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"operative_episode_detail_v2_preSCRIPT341_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'
    con.execute(f"""CREATE TABLE {archive_fq} AS
                    SELECT * FROM main.operative_episode_detail_v2""")
    arc_n = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if arc_n != pre_rows:
        raise SystemExit(f"Archive count mismatch: {arc_n} != {pre_rows}")
    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), "main", "operative_episode_detail_v2",
          archive_fq, arc_n,
          "Pre-Script 341 multi-episode rebuild", SCRIPT])
    log(f"    archived {arc_n} rows to {archive_name}")

    log("  Step 10: CREATE OR REPLACE main.operative_episode_detail_v2")
    con.execute("""
        CREATE OR REPLACE TABLE main.operative_episode_detail_v2 AS
        SELECT * FROM _oed_rebuilt
    """)
    snapshot_oed(con, "post")

    log("  Step 11: re-deriving CPM rollups from rebuilt table")
    rollup_cols = [
        ("op_first_surgery_date",
         "MIN(COALESCE(TRY_CAST(resolved_surgery_date AS DATE), CAST(surgery_date_native AS DATE)))"),
        ("op_last_surgery_date",
         "MAX(COALESCE(TRY_CAST(resolved_surgery_date AS DATE), CAST(surgery_date_native AS DATE)))"),
        ("op_episode_count", "COUNT(*)"),
        ("n_surgeries_from_opdetail_v2", "COUNT(*)"),
    ]
    for col, expr in rollup_cols:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE table_name='canonical_patient_master'
               AND column_name='{col}'
        """).fetchone()[0]
        if exists == 0:
            log(f"    CPM.{col}: column not found — skipping")
            continue
        con.execute(f"DROP TABLE IF EXISTS _r_{col}")
        con.execute(f"""
            CREATE TEMP TABLE _r_{col} AS
            SELECT CAST(research_id AS VARCHAR) AS research_id, {expr} AS val
              FROM main.operative_episode_detail_v2
             GROUP BY CAST(research_id AS VARCHAR)
        """)
        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{col}" = r.val
              FROM _r_{col} AS r
             WHERE CAST(c.research_id AS VARCHAR) = r.research_id
        """)
        nn = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master WHERE "{col}" IS NOT NULL
        """).fetchone()[0]
        log(f"    CPM.{col}: nonnull post={nn}")
        log_metric(con, "post", "canonical_patient_master", col, "nonnull", nn)

    cpm_invariants(con, "post")
    log("=" * 72)
    log(f"Script 341 complete. multi_episode_rids: {pre_multi} → {multi_built}")


if __name__ == "__main__":
    main()
