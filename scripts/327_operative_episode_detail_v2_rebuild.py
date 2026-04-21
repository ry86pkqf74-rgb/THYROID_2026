"""
Script 327 — True multi-episode rebuild of operative_episode_detail_v2.

Problem: CPM n_surgeries_v2 shows 738 patients with >=2 surgeries, but
operative_episode_detail_v2 only has 3 patients with >=2 rows.  ~735
re-operation episodes are missing.

Approach:
  1. Archive current v2.
  2. Derive operative episode dates per RID using hybrid strategy:
     - Pass 1: entity_type='operative_date' from note_entities_operative_detail
     - Pass 2: where episodes < n_surgeries_v2, supplement with note_date
       clusters from operative notes, ±7d, excluding already-captured dates.
     - Entity dates take priority on ±7d overlap.
  3. Cluster within ±7d per RID (gap-and-islands).
  4. Cross-validate against n_surgeries_v2; log to operative_rebuild_mismatch_v1.
  5. Per (RID, episode), roll up entity flags from note_entities_operative_detail.
  6. Merge with existing v2 data for episode 1 (preserve enrichment).
  7. Re-derive affected CPM rollups.

Usage:
    python 327_operative_episode_detail_v2_rebuild.py            # dry-run
    python 327_operative_episode_detail_v2_rebuild.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "327_operative_episode_detail_v2_rebuild"
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


def ensure_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP, src_schema VARCHAR, src_table VARCHAR,
            archive_fq VARCHAR, n_rows BIGINT, reason VARCHAR, script VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
            backfilled_at TIMESTAMP, cpm_column VARCHAR,
            source_description VARCHAR, threshold VARCHAR,
            n_rows_updated BIGINT, n_distinct_rid BIGINT,
            sample_values VARCHAR, script VARCHAR
        )
    """)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_tables(con)
    log("=" * 72)
    log(f"Script 327 — Operative episode detail v2 REBUILD "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # ── Pre-state ──
    pre_rows = con.execute(
        "SELECT COUNT(*) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    pre_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    log(f"  Pre: {pre_rows} rows, {pre_rid} distinct RIDs")

    # n_surgeries_v2 distribution
    surg_dist = con.execute("""
        SELECT n_surgeries_v2, COUNT(*) AS n
        FROM main.canonical_patient_master
        WHERE n_surgeries_v2 IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("  CPM n_surgeries_v2 distribution:")
    for s in surg_dist:
        log(f"    n={s[0]}: {s[1]} patients")

    # ── Step 1: Get existing v2 columns for schema reference ──
    v2_cols = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = 'operative_episode_detail_v2'
        ORDER BY ordinal_position
    """).fetchall()
    v2_col_names = [c[0] for c in v2_cols]
    log(f"  Existing v2 has {len(v2_col_names)} columns")

    # ── Step 2: Build episode dates via hybrid strategy ──
    log("  Step 2: Building episode dates (hybrid)...")

    # Pass 1: entity_type dates from note_entities_operative_detail
    con.execute("DROP TABLE IF EXISTS _pass1_entity_dates")
    con.execute("""
        CREATE TEMP TABLE _pass1_entity_dates AS
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            TRY_CAST(entity_value_norm AS DATE) AS op_date,
            'entity' AS date_source
        FROM main.note_entities_operative_detail
        WHERE entity_type IN ('operative_date', 'surgery_date', 'procedure_date')
          AND present_or_negated = 'present'
          AND TRY_CAST(entity_value_norm AS DATE) IS NOT NULL
    """)
    p1 = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _pass1_entity_dates").fetchone()
    log(f"    Pass 1 (entity dates): {p1[0]} date rows, {p1[1]} RIDs")

    # Pass 2: note_date from all notes in note_entities_operative_detail
    con.execute("DROP TABLE IF EXISTS _pass2_note_dates")
    con.execute("""
        CREATE TEMP TABLE _pass2_note_dates AS
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            note_date AS op_date,
            'note_date' AS date_source
        FROM main.note_entities_operative_detail
        WHERE note_date IS NOT NULL
    """)
    p2 = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _pass2_note_dates").fetchone()
    log(f"    Pass 2 (note dates): {p2[0]} date rows, {p2[1]} RIDs")

    # Pass 3: existing v2 dates (preserve known surgery dates)
    con.execute("DROP TABLE IF EXISTS _pass3_v2_dates")
    con.execute("""
        CREATE TEMP TABLE _pass3_v2_dates AS
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            COALESCE(resolved_surgery_date, surgery_date_native) AS op_date,
            'existing_v2' AS date_source
        FROM main.operative_episode_detail_v2
        WHERE COALESCE(resolved_surgery_date, surgery_date_native) IS NOT NULL
    """)
    p3 = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _pass3_v2_dates").fetchone()
    log(f"    Pass 3 (existing v2 dates): {p3[0]} date rows, {p3[1]} RIDs")

    # Merge all dates, dedup by (research_id, op_date) preferring entity > v2 > note_date
    con.execute("DROP TABLE IF EXISTS _all_dates")
    con.execute("""
        CREATE TEMP TABLE _all_dates AS
        WITH combined AS (
            SELECT *, 1 AS priority FROM _pass1_entity_dates
            UNION ALL
            SELECT *, 2 AS priority FROM _pass3_v2_dates
            UNION ALL
            SELECT *, 3 AS priority FROM _pass2_note_dates
        ),
        deduped AS (
            SELECT research_id, op_date,
                   FIRST(date_source ORDER BY priority) AS date_source
            FROM combined
            GROUP BY research_id, op_date
        )
        SELECT * FROM deduped
        WHERE op_date >= DATE '1990-01-01' AND op_date <= CURRENT_DATE
    """)
    ad = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _all_dates").fetchone()
    log(f"    All dates merged: {ad[0]} date rows, {ad[1]} RIDs")

    # ── Step 3: Cluster dates within ±7d (gap-and-islands) ──
    log("  Step 3: Clustering dates (±7d gap detection)...")
    con.execute("DROP TABLE IF EXISTS _episodes")
    con.execute("""
        CREATE TEMP TABLE _episodes AS
        WITH ordered AS (
            SELECT research_id, op_date, date_source,
                   LAG(op_date) OVER (
                       PARTITION BY research_id ORDER BY op_date
                   ) AS prev_date
            FROM _all_dates
        ),
        gaps AS (
            SELECT *,
                   CASE WHEN prev_date IS NULL
                             OR (op_date - prev_date) > 7
                        THEN 1 ELSE 0 END AS new_cluster
            FROM ordered
        ),
        clustered AS (
            SELECT *,
                   SUM(new_cluster) OVER (
                       PARTITION BY research_id
                       ORDER BY op_date
                       ROWS UNBOUNDED PRECEDING
                   ) AS cluster_id
            FROM gaps
        )
        SELECT
            research_id,
            cluster_id AS surgery_ordinal,
            MIN(op_date) AS canonical_operative_date,
            MAX(op_date) AS cluster_end_date,
            COUNT(*) AS n_dates_in_cluster,
            STRING_AGG(DISTINCT date_source, ',') AS date_sources
        FROM clustered
        GROUP BY research_id, cluster_id
    """)
    ep = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _episodes").fetchone()
    log(f"    Episodes: {ep[0]} total rows, {ep[1]} distinct RIDs")

    ep_dist = con.execute("""
        SELECT cnt, COUNT(*) AS n_patients FROM (
            SELECT research_id, COUNT(*) AS cnt FROM _episodes GROUP BY research_id
        ) GROUP BY cnt ORDER BY cnt
    """).fetchall()
    log("    Episode distribution:")
    for ed in ep_dist:
        log(f"      n_episodes={ed[0]}: {ed[1]} patients")

    # ── Step 4: Cross-validate with n_surgeries_v2 ──
    log("  Step 4: Cross-validating against n_surgeries_v2...")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_workspace.operative_rebuild_mismatch_v1 AS
        SELECT
            c.research_id,
            c.n_surgeries_v2,
            COALESCE(e.n_episodes, 0) AS n_entity_episodes,
            CASE
                WHEN e.n_episodes IS NULL THEN 'no_entity_data'
                WHEN e.n_episodes = c.n_surgeries_v2 THEN 'match'
                WHEN e.n_episodes > c.n_surgeries_v2 THEN 'entity_over'
                ELSE 'entity_under'
            END AS match_status,
            COALESCE(e.date_sources, 'none') AS date_sources
        FROM main.canonical_patient_master c
        LEFT JOIN (
            SELECT research_id,
                   COUNT(*) AS n_episodes,
                   STRING_AGG(DISTINCT date_sources, '; ') AS date_sources
            FROM _episodes GROUP BY research_id
        ) e ON CAST(c.research_id AS VARCHAR) = e.research_id
        WHERE c.n_surgeries_v2 IS NOT NULL
    """)
    mismatch_summary = con.execute("""
        SELECT match_status, COUNT(*) FROM manuscript_workspace.operative_rebuild_mismatch_v1
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    log("    Mismatch summary:")
    for m in mismatch_summary:
        log(f"      {m[0]}: {m[1]}")

    # ── Step 5: Roll up entity flags per episode ──
    log("  Step 5: Rolling up entity flags per episode...")
    con.execute("DROP TABLE IF EXISTS _episode_flags")
    con.execute("""
        CREATE TEMP TABLE _episode_flags AS
        SELECT
            ep.research_id,
            ep.surgery_ordinal,
            ep.canonical_operative_date,
            ep.cluster_end_date,
            ep.n_dates_in_cluster,
            ep.date_sources,

            BOOL_OR(CASE WHEN ne.entity_type = 'nerve_monitoring'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS rln_monitoring_flag,
            STRING_AGG(DISTINCT CASE WHEN ne.entity_type = 'rln_finding'
                                     AND ne.present_or_negated = 'present'
                                THEN ne.entity_value_norm END, ' | ')
                AS rln_finding_raw,
            BOOL_OR(CASE WHEN ne.entity_type = 'gross_invasion'
                         AND ne.present_or_negated = 'present'
                         AND ne.entity_value_norm IN ('gross_ete', 'gross_invasion')
                    THEN TRUE END)
                AS gross_ete_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'gross_invasion'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS local_invasion_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'tracheal_involvement'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS tracheal_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'esophageal_involvement'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS esophageal_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'parathyroid_autograft'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS parathyroid_autograft_flag,
            COUNT(DISTINCT CASE WHEN ne.entity_type = 'parathyroid_autograft'
                                 AND ne.present_or_negated = 'present'
                           THEN ne.note_row_id END)
                AS parathyroid_autograft_count,
            BOOL_OR(CASE WHEN ne.entity_type = 'reoperative_field'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS reoperative_field_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'strap_muscle'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS strap_muscle_involvement_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'drain_placement'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS drain_flag,
            BOOL_OR(CASE WHEN ne.entity_type = 'berry_ligament'
                         AND ne.present_or_negated = 'present' THEN TRUE END)
                AS berry_ligament_flag,
            MAX(CASE WHEN ne.entity_type = 'ebl'
                     AND ne.present_or_negated = 'present'
                THEN TRY_CAST(ne.entity_value_norm AS DOUBLE) END)
                AS ebl_ml_nlp,
            COUNT(DISTINCT ne.note_row_id) AS n_entity_rows,
            MIN(CAST(ne.note_row_id AS VARCHAR)) AS source_note_id

        FROM _episodes ep
        LEFT JOIN main.note_entities_operative_detail ne
            ON CAST(ne.research_id AS VARCHAR) = ep.research_id
           AND ne.note_date BETWEEN ep.canonical_operative_date - INTERVAL '7' DAY
                                AND ep.cluster_end_date + INTERVAL '7' DAY
        GROUP BY ep.research_id, ep.surgery_ordinal,
                 ep.canonical_operative_date, ep.cluster_end_date,
                 ep.n_dates_in_cluster, ep.date_sources
    """)
    ef = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _episode_flags").fetchone()
    log(f"    Episode flags: {ef[0]} rows, {ef[1]} RIDs")

    # ── Step 6: Build the new table ──
    log("  Step 6: Building new operative_episode_detail_v2...")

    # For episode 1 where existing v2 data exists: merge (prefer existing, enrich with entities)
    # For episodes 2+: use entity-only data
    # For RIDs without entity data but in existing v2: keep existing
    con.execute("DROP TABLE IF EXISTS _oed_rebuilt")
    con.execute("""
        CREATE TEMP TABLE _oed_rebuilt AS
        WITH existing_matched AS (
            SELECT
                CAST(o.research_id AS VARCHAR) AS research_id,
                o.surgery_episode_id,
                o.surgery_date_native,
                o.resolved_surgery_date,
                o.date_status,
                o.procedure_raw,
                o.procedure_normalized,
                o.laterality,
                o.central_neck_dissection_flag,
                o.lateral_neck_dissection_flag,
                COALESCE(ef.rln_monitoring_flag, o.rln_monitoring_flag) AS rln_monitoring_flag,
                COALESCE(ef.rln_finding_raw, o.rln_finding_raw) AS rln_finding_raw,
                COALESCE(ef.parathyroid_autograft_flag, o.parathyroid_autograft_flag)
                    AS parathyroid_autograft_flag,
                COALESCE(ef.parathyroid_autograft_count, o.parathyroid_autograft_count)
                    AS parathyroid_autograft_count,
                o.parathyroid_autograft_site,
                o.parathyroid_resection_flag,
                COALESCE(ef.gross_ete_flag, o.gross_ete_flag) AS gross_ete_flag,
                COALESCE(ef.local_invasion_flag, o.local_invasion_flag) AS local_invasion_flag,
                COALESCE(ef.tracheal_involvement_flag, o.tracheal_involvement_flag)
                    AS tracheal_involvement_flag,
                COALESCE(ef.esophageal_involvement_flag, o.esophageal_involvement_flag)
                    AS esophageal_involvement_flag,
                COALESCE(ef.strap_muscle_involvement_flag, o.strap_muscle_involvement_flag)
                    AS strap_muscle_involvement_flag,
                COALESCE(ef.reoperative_field_flag, o.reoperative_field_flag)
                    AS reoperative_field_flag,
                o.ebl_ml,
                COALESCE(ef.drain_flag, o.drain_flag) AS drain_flag,
                o.operative_findings_raw,
                o.source_tables,
                o.op_confidence,
                o.note_date_resolved,
                o.note_date_source,
                o.note_date_confidence,
                o.parathyroid_identified_count,
                o.frozen_section_flag,
                COALESCE(ef.berry_ligament_flag, o.berry_ligament_flag) AS berry_ligament_flag,
                COALESCE(ef.ebl_ml_nlp, o.ebl_ml_nlp) AS ebl_ml_nlp,
                o.op_enrichment_source,
                o.linked_pathology_episode_id,
                o.path_link_score_v3,
                o.linked_fna_episode_id,
                o.fna_link_score_v3,
                -- New columns
                COALESCE(ef.surgery_ordinal, 1) AS surgery_ordinal,
                ef.canonical_operative_date AS rebuild_canonical_date,
                COALESCE(ef.source_note_id, 'existing_v2') AS rebuild_source_note_id,
                'high' AS rebuild_confidence,
                TRUE AS from_existing_v2
            FROM main.operative_episode_detail_v2 o
            LEFT JOIN _episode_flags ef
                ON CAST(o.research_id AS VARCHAR) = ef.research_id
               AND ef.surgery_ordinal = 1
               AND ABS(COALESCE(o.resolved_surgery_date, o.surgery_date_native)
                       - ef.canonical_operative_date) <= 7
        ),
        new_episodes AS (
            SELECT
                ef.research_id,
                MD5(ef.research_id || ':' || CAST(ef.surgery_ordinal AS VARCHAR)
                    || ':' || CAST(ef.canonical_operative_date AS VARCHAR))
                    AS surgery_episode_id,
                ef.canonical_operative_date AS surgery_date_native,
                ef.canonical_operative_date AS resolved_surgery_date,
                'entity_derived' AS date_status,
                NULL AS procedure_raw,
                NULL AS procedure_normalized,
                NULL AS laterality,
                FALSE AS central_neck_dissection_flag,
                FALSE AS lateral_neck_dissection_flag,
                COALESCE(ef.rln_monitoring_flag, FALSE) AS rln_monitoring_flag,
                ef.rln_finding_raw,
                COALESCE(ef.parathyroid_autograft_flag, FALSE) AS parathyroid_autograft_flag,
                COALESCE(ef.parathyroid_autograft_count, 0) AS parathyroid_autograft_count,
                NULL AS parathyroid_autograft_site,
                FALSE AS parathyroid_resection_flag,
                COALESCE(ef.gross_ete_flag, FALSE) AS gross_ete_flag,
                COALESCE(ef.local_invasion_flag, FALSE) AS local_invasion_flag,
                COALESCE(ef.tracheal_involvement_flag, FALSE) AS tracheal_involvement_flag,
                COALESCE(ef.esophageal_involvement_flag, FALSE) AS esophageal_involvement_flag,
                COALESCE(ef.strap_muscle_involvement_flag, FALSE) AS strap_muscle_involvement_flag,
                COALESCE(ef.reoperative_field_flag, FALSE) AS reoperative_field_flag,
                NULL AS ebl_ml,
                COALESCE(ef.drain_flag, FALSE) AS drain_flag,
                NULL AS operative_findings_raw,
                'note_entities_operative_detail' AS source_tables,
                'entity_derived' AS op_confidence,
                ef.canonical_operative_date AS note_date_resolved,
                ef.date_sources AS note_date_source,
                CASE WHEN ef.date_sources LIKE '%entity%' THEN 'high'
                     ELSE 'medium' END AS note_date_confidence,
                CAST(NULL AS INTEGER) AS parathyroid_identified_count,
                FALSE AS frozen_section_flag,
                COALESCE(ef.berry_ligament_flag, FALSE) AS berry_ligament_flag,
                ef.ebl_ml_nlp,
                'script_327_rebuild' AS op_enrichment_source,
                NULL AS linked_pathology_episode_id,
                CAST(NULL AS DOUBLE) AS path_link_score_v3,
                NULL AS linked_fna_episode_id,
                CAST(NULL AS DOUBLE) AS fna_link_score_v3,
                ef.surgery_ordinal,
                ef.canonical_operative_date AS rebuild_canonical_date,
                ef.source_note_id AS rebuild_source_note_id,
                CASE WHEN ef.n_entity_rows > 3 THEN 'high'
                     WHEN ef.n_entity_rows > 0 THEN 'medium'
                     ELSE 'low' END AS rebuild_confidence,
                FALSE AS from_existing_v2
            FROM _episode_flags ef
            WHERE NOT EXISTS (
                SELECT 1 FROM main.operative_episode_detail_v2 o
                WHERE CAST(o.research_id AS VARCHAR) = ef.research_id
                  AND ABS(COALESCE(o.resolved_surgery_date, o.surgery_date_native)
                          - ef.canonical_operative_date) <= 7
            )
            AND ef.surgery_ordinal >= 1
        )
        SELECT * FROM existing_matched
        UNION ALL
        SELECT * FROM new_episodes
    """)

    rebuilt_rows = con.execute("SELECT COUNT(*) FROM _oed_rebuilt").fetchone()[0]
    rebuilt_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _oed_rebuilt").fetchone()[0]
    log(f"    Rebuilt: {rebuilt_rows} rows, {rebuilt_rids} RIDs")

    # Episode distribution in rebuilt table
    rebuilt_dist = con.execute("""
        SELECT cnt, COUNT(*) FROM (
            SELECT research_id, COUNT(*) AS cnt FROM _oed_rebuilt GROUP BY research_id
        ) GROUP BY cnt ORDER BY cnt
    """).fetchall()
    log("    Rebuilt distribution:")
    for rd in rebuilt_dist:
        log(f"      n_episodes={rd[0]}: {rd[1]} patients")

    # Invariant check: distinct RIDs >= 10,800
    if rebuilt_rids < 10800:
        log(f"  WARNING: rebuilt RID count {rebuilt_rids} < 10,800 target")

    if not args.commit:
        log("  (dry-run — no changes)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # ── Step 7: Archive and replace ──
    log("  Step 7: Archiving and replacing...")
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"operative_episode_detail_v2_pre327_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'

    con.execute(f"""
        CREATE TABLE {archive_fq} AS
        SELECT * FROM main.operative_episode_detail_v2
    """)
    arc_count = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if arc_count != pre_rows:
        raise SystemExit(f"Archive count mismatch: {arc_count} != {pre_rows}")
    log(f"    Archived {arc_count} rows to {archive_name}")

    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), "main", "operative_episode_detail_v2",
          archive_fq, arc_count,
          "Pre-327 multi-episode rebuild", SCRIPT])

    con.execute("""
        CREATE OR REPLACE TABLE main.operative_episode_detail_v2 AS
        SELECT * FROM _oed_rebuilt
    """)
    post_rows = con.execute("SELECT COUNT(*) FROM main.operative_episode_detail_v2").fetchone()[0]
    post_rids = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    log(f"    Post-rebuild: {post_rows} rows, {post_rids} RIDs")

    # ── Step 8: Re-derive CPM rollups ──
    log("  Step 8: Re-deriving CPM rollups...")

    rollups = [
        ("op_esophageal_inv_any", "esophageal_involvement_flag",
         "esophageal involvement from 327 rebuild"),
        ("op_tracheal_inv_any", "tracheal_involvement_flag",
         "tracheal involvement from 327 rebuild"),
        ("op_rln_monitoring_any", "rln_monitoring_flag",
         "RLN monitoring from 327 rebuild"),
    ]

    for cpm_col, oed_col, desc in rollups:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"    CPM.{cpm_col}: column not found — skipping")
            continue

        con.execute(f"DROP TABLE IF EXISTS _rollup_{oed_col}")
        con.execute(f"""
            CREATE TEMP TABLE _rollup_{oed_col} AS
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   BOOL_OR("{oed_col}") AS val
            FROM main.operative_episode_detail_v2
            WHERE "{oed_col}" = TRUE
            GROUP BY CAST(research_id AS VARCHAR)
        """)
        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN _rollup_{oed_col} r ON CAST(c.research_id AS VARCHAR) = r.research_id
            WHERE c."{cpm_col}" IS NULL AND r.val = TRUE
        """).fetchone()[0]

        if plan_n > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master AS c
                   SET "{cpm_col}" = TRUE
                  FROM _rollup_{oed_col} AS r
                 WHERE CAST(c.research_id AS VARCHAR) = r.research_id
                   AND c."{cpm_col}" IS NULL
                   AND r.val = TRUE
            """)
            log(f"    CPM.{cpm_col}: backfilled {plan_n} (NULL->TRUE)")
            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), cpm_col, desc,
                  "v1 NULL only", plan_n, None, None, SCRIPT])
        else:
            log(f"    CPM.{cpm_col}: 0 new fills")

    # revision_surgery_flag / any_lateral_neck_dissection from multi-episode data
    for cpm_col, sql_expr, desc in [
        ("revision_surgery_flag",
         "BOOL_OR(surgery_ordinal > 1 OR reoperative_field_flag = TRUE)",
         "revision flag from multi-episode rebuild"),
        ("any_lateral_neck_dissection",
         "BOOL_OR(lateral_neck_dissection_flag = TRUE)",
         "lateral neck dissection from 327 rebuild"),
    ]:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"    CPM.{cpm_col}: column not found — skipping")
            continue

        con.execute(f"DROP TABLE IF EXISTS _rollup_{cpm_col}")
        con.execute(f"""
            CREATE TEMP TABLE _rollup_{cpm_col} AS
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   {sql_expr} AS val
            FROM main.operative_episode_detail_v2
            GROUP BY CAST(research_id AS VARCHAR)
            HAVING {sql_expr}
        """)
        plan_n = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN _rollup_{cpm_col} r ON CAST(c.research_id AS VARCHAR) = r.research_id
            WHERE c."{cpm_col}" IS NULL AND r.val = TRUE
        """).fetchone()[0]

        if plan_n > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master AS c
                   SET "{cpm_col}" = TRUE
                  FROM _rollup_{cpm_col} AS r
                 WHERE CAST(c.research_id AS VARCHAR) = r.research_id
                   AND c."{cpm_col}" IS NULL
                   AND r.val = TRUE
            """)
            log(f"    CPM.{cpm_col}: backfilled {plan_n}")
            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), cpm_col, desc,
                  "v1 NULL only", plan_n, None, None, SCRIPT])
        else:
            log(f"    CPM.{cpm_col}: 0 new fills")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 327 complete.")


if __name__ == "__main__":
    main()
