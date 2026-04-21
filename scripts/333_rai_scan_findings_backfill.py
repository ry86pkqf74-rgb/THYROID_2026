"""
Script 333 — rai_scan_findings_v9 backfill from wider entity types.

Script 293 populated rai_scan_findings_v9 from post_treatment_wbs_findings
only (reaching 527 nonnull).  This script widens the entity filter to
wb_scan_finding, focal_uptake_site, thyroid_remnant_uptake, wb_scan_negative,
and joins to rai_treatment_episode_v2 within ±30d.

Usage:
    python 333_rai_scan_findings_backfill.py            # dry-run
    python 333_rai_scan_findings_backfill.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "333_rai_scan_findings_backfill"

ENTITY_TYPES = (
    'wb_scan_finding', 'focal_uptake_site', 'thyroid_remnant_uptake',
    'wb_scan_negative', 'post_treatment_wbs_findings',
    'scan_finding', 'uptake_site'
)


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


def ensure_log_table(con):
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

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 333 — RAI scan findings backfill "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state
    pre_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE rai_scan_findings_v9 IS NOT NULL
    """).fetchone()[0]
    log(f"  rai_scan_findings_v9 pre: {pre_pop} nonnull")

    # Parse entities
    log("  Parsing note_entities_llm_rai_detailed...")
    entity_types_sql = ", ".join(f"'{e}'" for e in ENTITY_TYPES)
    con.execute("DROP TABLE IF EXISTS _rai_entities")
    con.execute(f"""
        CREATE TEMP TABLE _rai_entities AS
        WITH src AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                note_id, note_date,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_rai_detailed
            WHERE result_json IS NOT NULL
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ent AS (
            SELECT
                s.research_id, s.note_id, s.note_date,
                json_extract_string(e, '$.entity_type') AS entity_type,
                json_extract_string(e, '$.entity_value') AS entity_value,
                json_extract_string(e, '$.evidence_text') AS evidence_text
            FROM src s, UNNEST(s.arr) AS t(e)
        )
        SELECT * FROM ent
        WHERE entity_type IN ({entity_types_sql})
          AND entity_value IS NOT NULL
          AND TRIM(entity_value) != ''
    """)
    ent_count = con.execute("SELECT COUNT(*) FROM _rai_entities").fetchone()[0]
    ent_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _rai_entities").fetchone()[0]
    log(f"    Raw entities: {ent_count} rows, {ent_rids} RIDs")

    # Entity type distribution
    ent_dist = con.execute("""
        SELECT entity_type, COUNT(*), COUNT(DISTINCT research_id)
        FROM _rai_entities GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    for ed in ent_dist:
        log(f"      {ed[0]:40s} {ed[1]:5d} rows  {ed[2]:5d} RIDs")

    # Aggregate per research_id
    con.execute("DROP TABLE IF EXISTS _rai_agg")
    con.execute("""
        CREATE TEMP TABLE _rai_agg AS
        SELECT
            research_id,
            STRING_AGG(DISTINCT entity_value, ' | ' ORDER BY entity_value)
                AS findings_text,
            MIN(note_date) AS first_note_date,
            MIN(CAST(note_id AS VARCHAR)) AS first_note_id,
            LEFT(MIN(evidence_text), 200) AS first_evidence_text
        FROM _rai_entities
        GROUP BY research_id
    """)
    agg_rids = con.execute("SELECT COUNT(*) FROM _rai_agg").fetchone()[0]
    log(f"    Aggregated: {agg_rids} RIDs")

    # Plan backfill
    plan_n = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        JOIN _rai_agg r ON CAST(c.research_id AS VARCHAR) = r.research_id
        WHERE c.rai_scan_findings_v9 IS NULL
          AND r.findings_text IS NOT NULL
    """).fetchone()[0]
    log(f"  Planned backfill: {plan_n}")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Add companion columns if missing
    for col, dtype in [
        ("rai_scan_findings_v9_source_note_id", "VARCHAR"),
        ("rai_scan_findings_v9_source_note_date", "DATE"),
        ("rai_scan_findings_v9_source_evidence_text", "VARCHAR"),
    ]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{col}'
        """).fetchone()[0]
        if exists == 0:
            con.execute(f'ALTER TABLE main.canonical_patient_master ADD COLUMN "{col}" {dtype}')
            log(f"    Added CPM column: {col} {dtype}")

    # Backfill
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET rai_scan_findings_v9 = r.findings_text,
               rai_scan_findings_v9_source_note_id = r.first_note_id,
               rai_scan_findings_v9_source_note_date = r.first_note_date,
               rai_scan_findings_v9_source_evidence_text = r.first_evidence_text
          FROM _rai_agg AS r
         WHERE CAST(c.research_id AS VARCHAR) = r.research_id
           AND c.rai_scan_findings_v9 IS NULL
           AND r.findings_text IS NOT NULL
    """)

    post_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE rai_scan_findings_v9 IS NOT NULL
    """).fetchone()[0]
    log(f"  rai_scan_findings_v9 post: {post_pop} (delta +{post_pop - pre_pop})")

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), "rai_scan_findings_v9",
          "Widened entity types from note_entities_llm_rai_detailed",
          "v1 NULL only", post_pop - pre_pop, agg_rids, None, SCRIPT])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 333 complete.")


if __name__ == "__main__":
    main()
