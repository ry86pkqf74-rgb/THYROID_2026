"""
Script 334 — Derive op_esophageal_inv_any from airway invasion entities.

cpm.op_esophageal_inv_any is 0 nonnull.  note_entities_llm_airway_invasion
contains 'esophag' mentions across 381 distinct RIDs.  This is an interim
fix harvesting esophageal invasion signal from the airway-invasion
extraction.

Usage:
    python 334_op_esophageal_inv_any_from_airway.py            # dry-run
    python 334_op_esophageal_inv_any_from_airway.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "334_op_esophageal_inv_any_from_airway"


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
    log(f"Script 334 — op_esophageal_inv_any from airway "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state
    pre_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE op_esophageal_inv_any IS NOT NULL
    """).fetchone()[0]
    log(f"  op_esophageal_inv_any pre: {pre_pop} nonnull")

    # Parse airway invasion entities for esophageal mentions
    log("  Parsing note_entities_llm_airway_invasion...")
    con.execute("DROP TABLE IF EXISTS _esoph_raw")
    con.execute("""
        CREATE TEMP TABLE _esoph_raw AS
        WITH src AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                note_id, note_date,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_airway_invasion
            WHERE result_json IS NOT NULL
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ent AS (
            SELECT
                s.research_id, s.note_id, s.note_date,
                json_extract_string(e, '$.entity_type') AS entity_type,
                json_extract_string(e, '$.entity_value') AS entity_value,
                json_extract_string(e, '$.present_or_negated') AS present_or_negated,
                json_extract_string(e, '$.evidence_text') AS evidence_text,
                TRY_CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence
            FROM src s, UNNEST(s.arr) AS t(e)
        )
        SELECT * FROM ent
        WHERE entity_type = 'esophageal_invasion'
           OR entity_value ILIKE '%esophag%'
           OR evidence_text ILIKE '%esophag%'
    """)
    raw_count = con.execute("SELECT COUNT(*) FROM _esoph_raw").fetchone()[0]
    raw_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _esoph_raw").fetchone()[0]
    log(f"    Esophageal mentions: {raw_count} rows, {raw_rids} RIDs")

    # Classify per patient
    con.execute("DROP TABLE IF EXISTS _esoph_classified")
    con.execute("""
        CREATE TEMP TABLE _esoph_classified AS
        SELECT
            research_id,

            BOOL_OR(
                present_or_negated IN ('present', 'invading')
                AND COALESCE(confidence, 1.0) >= 0.5
            ) AS has_positive,
            BOOL_OR(
                present_or_negated IN ('negated', 'absent')
            ) AS has_negative,

            MIN(CASE WHEN present_or_negated IN ('present', 'invading')
                          AND COALESCE(confidence, 1.0) >= 0.5
                     THEN note_date END)
                AS first_positive_date,
            MIN(CASE WHEN present_or_negated IN ('present', 'invading')
                          AND COALESCE(confidence, 1.0) >= 0.5
                     THEN CAST(note_id AS VARCHAR) END)
                AS first_positive_note_id,
            LEFT(MIN(CASE WHEN present_or_negated IN ('present', 'invading')
                              AND COALESCE(confidence, 1.0) >= 0.5
                         THEN evidence_text END), 200)
                AS first_positive_evidence_text,

            -- Extent classification
            CASE
                WHEN BOOL_OR(entity_value ILIKE '%full%thickness%'
                             OR entity_value ILIKE '%transmural%') THEN 'full_thickness'
                WHEN BOOL_OR(entity_value ILIKE '%partial%'
                             OR entity_value ILIKE '%invad%') THEN 'invading_partial'
                WHEN BOOL_OR(entity_value ILIKE '%abut%'
                             OR entity_value ILIKE '%contact%') THEN 'abutting'
                ELSE NULL
            END AS extent,

            COUNT(*) AS n_notes_documenting

        FROM _esoph_raw
        GROUP BY research_id
    """)
    classified_rids = con.execute("SELECT COUNT(*) FROM _esoph_classified").fetchone()[0]
    log(f"    Classified: {classified_rids} RIDs")

    # Derive op_esophageal_inv_any
    con.execute("DROP TABLE IF EXISTS _esoph_final")
    con.execute("""
        CREATE TEMP TABLE _esoph_final AS
        SELECT
            research_id,
            CASE
                WHEN has_positive THEN TRUE
                WHEN has_negative AND NOT has_positive THEN FALSE
                ELSE NULL
            END AS op_esophageal_inv_any,
            first_positive_date,
            first_positive_note_id,
            first_positive_evidence_text,
            extent,
            n_notes_documenting
        FROM _esoph_classified
    """)

    final_true = con.execute(
        "SELECT COUNT(*) FROM _esoph_final WHERE op_esophageal_inv_any = TRUE"
    ).fetchone()[0]
    final_false = con.execute(
        "SELECT COUNT(*) FROM _esoph_final WHERE op_esophageal_inv_any = FALSE"
    ).fetchone()[0]
    final_null = con.execute(
        "SELECT COUNT(*) FROM _esoph_final WHERE op_esophageal_inv_any IS NULL"
    ).fetchone()[0]
    log(f"    Classification: TRUE={final_true}, FALSE={final_false}, NULL={final_null}")

    # Plan
    plan_n = con.execute("""
        SELECT COUNT(*)
        FROM main.canonical_patient_master c
        JOIN _esoph_final f ON CAST(c.research_id AS VARCHAR) = f.research_id
        WHERE c.op_esophageal_inv_any IS NULL
          AND f.op_esophageal_inv_any IS NOT NULL
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
        ("op_esophageal_inv_first_date", "DATE"),
        ("op_esophageal_inv_first_note_id", "VARCHAR"),
        ("op_esophageal_inv_first_evidence_text", "VARCHAR"),
        ("op_esophageal_inv_extent", "VARCHAR"),
        ("op_esophageal_inv_n_notes_documenting", "INTEGER"),
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
           SET op_esophageal_inv_any = f.op_esophageal_inv_any,
               op_esophageal_inv_first_date = f.first_positive_date,
               op_esophageal_inv_first_note_id = f.first_positive_note_id,
               op_esophageal_inv_first_evidence_text = f.first_positive_evidence_text,
               op_esophageal_inv_extent = f.extent,
               op_esophageal_inv_n_notes_documenting = f.n_notes_documenting
          FROM _esoph_final AS f
         WHERE CAST(c.research_id AS VARCHAR) = f.research_id
           AND c.op_esophageal_inv_any IS NULL
           AND f.op_esophageal_inv_any IS NOT NULL
    """)

    post_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE op_esophageal_inv_any IS NOT NULL
    """).fetchone()[0]
    log(f"  op_esophageal_inv_any post: {post_pop} (delta +{post_pop - pre_pop})")

    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), "op_esophageal_inv_any",
          "Derived from note_entities_llm_airway_invasion esophageal mentions",
          "v1 NULL only", post_pop - pre_pop, raw_rids, None, SCRIPT])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 334 complete.")


if __name__ == "__main__":
    main()
