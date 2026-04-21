"""
Script 293 — Integrate RAI scan findings into rai_scan_findings_v9.

note_entities_llm_rai_detailed has 559 post_treatment_wbs_findings entities.
rai_scan_findings_v9 (retyped VARCHAR in Script 288) is 100% NULL.
This script STRING_AGGs the findings per research_id and writes to CPM.

Also proposes (prints only, does NOT auto-add) columns for:
  pre_rai_tsh, pre_rai_tg, rai_dose_mci_per_episode.

Usage:
    python 293_rai_findings_integration.py            # dry-run
    python 293_rai_findings_integration.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "293_rai_findings_integration"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 293 — RAI findings integration "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Check result_json structure
    n_total = con.execute(
        "SELECT COUNT(*) FROM main.note_entities_llm_rai_detailed"
    ).fetchone()[0]
    log(f"  note_entities_llm_rai_detailed total: {n_total}")

    # Extract post_treatment_wbs_findings
    con.execute("DROP TABLE IF EXISTS _rai_findings")
    con.execute("""
        CREATE TEMP TABLE _rai_findings AS
        WITH src AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_rai_detailed
            WHERE result_json IS NOT NULL
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ents AS (
            SELECT
                s.research_id,
                json_extract_string(e, '$.entity_type') AS entity_type,
                json_extract_string(e, '$.entity_value') AS entity_value
            FROM src s, UNNEST(s.arr) AS t(e)
        )
        SELECT
            research_id,
            STRING_AGG(DISTINCT entity_value, ' | ' ORDER BY entity_value) AS findings
        FROM ents
        WHERE entity_type = 'post_treatment_wbs_findings'
          AND entity_value IS NOT NULL
          AND TRIM(entity_value) != ''
        GROUP BY research_id
    """)

    n_findings = con.execute("SELECT COUNT(*) FROM _rai_findings").fetchone()[0]
    n_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM _rai_findings"
    ).fetchone()[0]
    log(f"  RAI findings extracted: {n_findings} patients")

    # Plan
    plan = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        JOIN _rai_findings f ON c.research_id = f.research_id
        WHERE c.rai_scan_findings_v9 IS NULL
          AND f.findings IS NOT NULL
    """).fetchone()[0]
    log(f"  Planned UPDATE: {plan} rows (CPM NULL AND source NOT NULL)")

    sample = con.execute("""
        SELECT f.findings
        FROM main.canonical_patient_master c
        JOIN _rai_findings f ON c.research_id = f.research_id
        WHERE c.rai_scan_findings_v9 IS NULL
        LIMIT 3
    """).fetchall()
    for i, s in enumerate(sample):
        log(f"  Sample {i+1}: {str(s[0])[:120]}")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
    else:
        con.execute("""
            UPDATE main.canonical_patient_master AS c
               SET rai_scan_findings_v9 = f.findings
              FROM _rai_findings AS f
             WHERE c.research_id = f.research_id
               AND c.rai_scan_findings_v9 IS NULL
               AND f.findings IS NOT NULL
        """)

        post_pop = con.execute("""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE rai_scan_findings_v9 IS NOT NULL
        """).fetchone()[0]
        log(f"  rai_scan_findings_v9 post-pop: {post_pop}")

        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dt.datetime.utcnow(), "rai_scan_findings_v9",
            "STRING_AGG(post_treatment_wbs_findings) from note_entities_llm_rai_detailed",
            "v1 NULL only", plan, n_rid, None, SCRIPT
        ])

    # Propose additional columns
    log("")
    log("  === PROPOSED additional CPM columns (NOT auto-added) ===")
    proposals = con.execute("""
        WITH src AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_rai_detailed
            WHERE result_json IS NOT NULL
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ents AS (
            SELECT
                json_extract_string(e, '$.entity_type') AS entity_type,
                COUNT(*) AS n,
                COUNT(DISTINCT s.research_id) AS n_rid
            FROM src s, UNNEST(s.arr) AS t(e)
            GROUP BY 1
        )
        SELECT entity_type, n, n_rid
        FROM ents
        WHERE entity_type NOT IN ('post_treatment_wbs_findings')
        ORDER BY n DESC
    """).fetchall()
    for p in proposals:
        log(f"  {p[0]:40s} {p[1]:5d} rows  {p[2]:5d} patients")
    log("  (Logan: consider adding pre_rai_tsh, pre_rai_tg, rai_dose_mci)")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 293 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
