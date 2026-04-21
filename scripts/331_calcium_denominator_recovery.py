"""
Script 331 — Calcium denominator recovery from LLM labs.

postop_calcium_min_value nonnull = 544.  longitudinal_lab_canonical_v1 has
only 188 calcium rows across 166 RIDs.  note_entities_llm_labs.result_json
contains ~300 distinct RIDs with calcium/PTH mentions.

Approach:
  1. Parse result_json for calcium/PTH entities via UNNEST.
  2. Normalize to mg/dL (calcium) or ng/dL (PTH).
  3. Plausibility filter (calcium 4.0–14.8 mg/dL).
  4. ADDITIVE insert into longitudinal_lab_canonical_v1.
  5. Re-derive postop_calcium_min_value, has_low_calcium_flag,
     comp_hypocalcemia_confirmed on CPM from widened input.

Usage:
    python 331_calcium_denominator_recovery.py            # dry-run
    python 331_calcium_denominator_recovery.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "331_calcium_denominator_recovery"


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
    log(f"Script 331 — Calcium denominator recovery "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state
    pre_lab_ca = con.execute("""
        SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1
        WHERE lab_name_standardized IN ('calcium', 'total_calcium',
              'corrected_calcium', 'ionized_calcium')
    """).fetchone()[0]
    pre_cpm_ca = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE postop_calcium_min_value IS NOT NULL
    """).fetchone()[0]
    pre_hypo = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE comp_hypocalcemia_confirmed = TRUE
    """).fetchone()[0]
    log(f"  Pre: lab calcium rows={pre_lab_ca}, CPM postop_ca={pre_cpm_ca}, "
        f"hypocalcemia confirmed={pre_hypo}")

    # Step 1: Parse LLM labs for calcium/PTH
    log("  Step 1: Parsing note_entities_llm_labs...")
    con.execute("DROP TABLE IF EXISTS _llm_ca_raw")
    con.execute("""
        CREATE TEMP TABLE _llm_ca_raw AS
        WITH src AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                note_id, note_date, note_type, extracted_at,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_labs
            WHERE result_json IS NOT NULL
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ent AS (
            SELECT
                s.research_id, s.note_id, s.note_date, s.note_type,
                json_extract_string(e, '$.entity_type') AS entity_type,
                json_extract_string(e, '$.entity_value') AS entity_value,
                json_extract_string(e, '$.entity_date') AS entity_date,
                json_extract_string(e, '$.evidence_text') AS evidence_text,
                TRY_CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence
            FROM src s, UNNEST(s.arr) AS t(e)
        )
        SELECT * FROM ent
        WHERE entity_type IN ('calcium', 'pth', 'parathyroid_hormone',
                              'corrected_calcium', 'ionized_calcium',
                              'total_calcium')
    """)
    raw_count = con.execute("SELECT COUNT(*) FROM _llm_ca_raw").fetchone()[0]
    raw_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _llm_ca_raw").fetchone()[0]
    log(f"    Raw calcium/PTH entities: {raw_count} rows, {raw_rids} RIDs")

    # Step 2: Normalize values
    log("  Step 2: Normalizing values...")
    con.execute("DROP TABLE IF EXISTS _llm_ca_norm")
    con.execute("""
        CREATE TEMP TABLE _llm_ca_norm AS
        SELECT
            research_id, note_id, note_date, note_type,
            entity_type, entity_value, entity_date,
            evidence_text, confidence,

            -- Extract numeric value
            TRY_CAST(regexp_extract(entity_value, '([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
                AS raw_numeric,

            -- Detect unit
            CASE
                WHEN entity_value ILIKE '%mmol%' THEN 'mmol/L'
                WHEN entity_value ILIKE '%pmol%' THEN 'pmol/L'
                WHEN entity_value ILIKE '%ng/dl%' OR entity_value ILIKE '%ng/dL%' THEN 'ng/dL'
                WHEN entity_value ILIKE '%pg/ml%' OR entity_value ILIKE '%pg/mL%' THEN 'pg/mL'
                WHEN entity_value ILIKE '%mg/dl%' OR entity_value ILIKE '%mg/dL%' THEN 'mg/dL'
                WHEN entity_value ILIKE '%meq%' THEN 'mEq/L'
                ELSE 'unknown'
            END AS detected_unit,

            -- Standardized name
            CASE
                WHEN entity_type IN ('calcium', 'total_calcium') THEN 'calcium'
                WHEN entity_type = 'corrected_calcium' THEN 'corrected_calcium'
                WHEN entity_type = 'ionized_calcium' THEN 'ionized_calcium'
                WHEN entity_type IN ('pth', 'parathyroid_hormone') THEN 'pth'
                ELSE entity_type
            END AS lab_name_std
        FROM _llm_ca_raw
        WHERE TRY_CAST(regexp_extract(entity_value, '([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
              IS NOT NULL
    """)

    # Convert to mg/dL
    con.execute("DROP TABLE IF EXISTS _llm_ca_converted")
    con.execute("""
        CREATE TEMP TABLE _llm_ca_converted AS
        SELECT *,
            CASE
                WHEN lab_name_std IN ('calcium', 'corrected_calcium', 'ionized_calcium')
                    THEN CASE
                        WHEN detected_unit = 'mmol/L' AND raw_numeric BETWEEN 1.0 AND 3.5
                            THEN raw_numeric * 4.008
                        WHEN detected_unit = 'mg/dL' THEN raw_numeric
                        WHEN detected_unit = 'unknown' AND raw_numeric BETWEEN 4.0 AND 14.8
                            THEN raw_numeric
                        WHEN detected_unit = 'unknown' AND raw_numeric >= 100
                            THEN raw_numeric / 100.0
                        WHEN detected_unit = 'unknown' AND raw_numeric >= 20
                            THEN raw_numeric / 10.0
                        ELSE NULL
                    END
                WHEN lab_name_std = 'pth'
                    THEN CASE
                        WHEN detected_unit = 'pmol/L' THEN raw_numeric * 9.43
                        WHEN detected_unit IN ('pg/mL', 'ng/dL', 'mg/dL', 'unknown')
                            THEN raw_numeric
                        ELSE raw_numeric
                    END
                ELSE NULL
            END AS value_mg_dl
        FROM _llm_ca_norm
    """)

    # Step 3: Plausibility filter for calcium
    con.execute("DROP TABLE IF EXISTS _llm_ca_plausible")
    con.execute("""
        CREATE TEMP TABLE _llm_ca_plausible AS
        SELECT * FROM _llm_ca_converted
        WHERE (lab_name_std != 'pth' AND value_mg_dl BETWEEN 4.0 AND 14.8)
           OR (lab_name_std = 'pth' AND value_mg_dl BETWEEN 1.0 AND 2000.0)
    """)
    plaus_count = con.execute("SELECT COUNT(*) FROM _llm_ca_plausible").fetchone()[0]
    plaus_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _llm_ca_plausible").fetchone()[0]
    log(f"    Plausible rows: {plaus_count}, {plaus_rids} RIDs")

    # Out-of-range → orphan review
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.lab_orphan_cohort_review_v1 (
            research_id VARCHAR, note_id VARCHAR, note_date DATE,
            lab_name VARCHAR, raw_value VARCHAR, converted_value DOUBLE,
            reason VARCHAR, script VARCHAR, reviewed_at TIMESTAMP
        )
    """)
    oor_count = con.execute("""
        SELECT COUNT(*) FROM _llm_ca_converted
        WHERE value_mg_dl IS NOT NULL
          AND NOT (
            (lab_name_std != 'pth' AND value_mg_dl BETWEEN 4.0 AND 14.8)
            OR (lab_name_std = 'pth' AND value_mg_dl BETWEEN 1.0 AND 2000.0)
          )
    """).fetchone()[0]
    log(f"    Out-of-range rows → orphan review: {oor_count}")

    if args.commit and oor_count > 0:
        con.execute("""
            INSERT INTO manuscript_workspace.lab_orphan_cohort_review_v1
            SELECT research_id, CAST(note_id AS VARCHAR), note_date,
                   lab_name_std, entity_value, value_mg_dl,
                   'calcium_out_of_range_llm', ?, CURRENT_TIMESTAMP
            FROM _llm_ca_converted
            WHERE value_mg_dl IS NOT NULL
              AND NOT (
                (lab_name_std != 'pth' AND value_mg_dl BETWEEN 4.0 AND 14.8)
                OR (lab_name_std = 'pth' AND value_mg_dl BETWEEN 1.0 AND 2000.0)
              )
        """, [SCRIPT])

    # Step 4: ADDITIVE insert into longitudinal_lab_canonical_v1
    log("  Step 4: Inserting into longitudinal_lab_canonical_v1...")

    # Check existing schema columns
    lab_cols = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'longitudinal_lab_canonical_v1'
        ORDER BY ordinal_position
    """).fetchall()
    lab_col_names = [c[0] for c in lab_cols]
    log(f"    longitudinal_lab_canonical_v1 has {len(lab_col_names)} columns")

    if not args.commit:
        log("  (dry-run — no INSERT)")
    else:
        # Deduplicate: skip rows where (research_id, note_date, lab_name_std, value)
        # already exists in longitudinal_lab_canonical_v1
        con.execute("""
            INSERT INTO main.longitudinal_lab_canonical_v1
            (research_id, lab_date, lab_name_raw, lab_name_standardized,
             value_raw, value_numeric, unit_raw, unit_standardized,
             source_table, source_script, ingestion_wave,
             data_completeness_tier, provenance_note)
            SELECT
                p.research_id,
                COALESCE(TRY_CAST(p.entity_date AS DATE), p.note_date) AS lab_date,
                p.entity_value AS lab_name_raw,
                p.lab_name_std AS lab_name_standardized,
                p.entity_value AS value_raw,
                p.value_mg_dl AS value_numeric,
                p.detected_unit AS unit_raw,
                CASE WHEN p.lab_name_std = 'pth' THEN 'pg/mL' ELSE 'mg/dL' END
                    AS unit_standardized,
                'note_entities_llm_labs' AS source_table,
                '331_calcium_from_llm_labs' AS source_script,
                'v1_0_llm_recovery' AS ingestion_wave,
                'medium' AS data_completeness_tier,
                LEFT(p.evidence_text, 200) || ':' || COALESCE(CAST(p.note_id AS VARCHAR), '')
                    AS provenance_note
            FROM _llm_ca_plausible p
            WHERE NOT EXISTS (
                SELECT 1 FROM main.longitudinal_lab_canonical_v1 l
                WHERE l.research_id = p.research_id
                  AND l.lab_date = COALESCE(TRY_CAST(p.entity_date AS DATE), p.note_date)
                  AND l.lab_name_standardized = p.lab_name_std
                  AND l.source_table = 'note_entities_llm_labs'
            )
        """)

        post_lab_ca = con.execute("""
            SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1
            WHERE lab_name_standardized IN ('calcium', 'total_calcium',
                  'corrected_calcium', 'ionized_calcium')
        """).fetchone()[0]
        log(f"    Calcium rows post-insert: {post_lab_ca} (was {pre_lab_ca})")

    # Step 5: Re-derive CPM calcium columns
    log("  Step 5: Re-deriving CPM calcium columns...")
    if args.commit:
        # Build per-patient calcium rollup
        con.execute("DROP TABLE IF EXISTS _ca_rollup")
        con.execute("""
            CREATE TEMP TABLE _ca_rollup AS
            SELECT
                research_id,
                MIN(value_numeric) AS ca_min_all,
                MIN(CASE WHEN lab_name_standardized IN ('calcium', 'total_calcium',
                              'corrected_calcium', 'ionized_calcium')
                    THEN value_numeric END) AS ca_min_calcium
            FROM main.longitudinal_lab_canonical_v1
            WHERE value_numeric IS NOT NULL
              AND lab_name_standardized IN ('calcium', 'total_calcium',
                  'corrected_calcium', 'ionized_calcium')
              AND value_numeric BETWEEN 4.0 AND 14.8
            GROUP BY research_id
        """)
        rollup_rids = con.execute("SELECT COUNT(DISTINCT research_id) FROM _ca_rollup").fetchone()[0]
        log(f"    Calcium rollup: {rollup_rids} RIDs")

        # Backfill postop_calcium_min_value where NULL
        plan_ca = con.execute("""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN _ca_rollup r ON CAST(c.research_id AS VARCHAR) = r.research_id
            WHERE c.postop_calcium_min_value IS NULL
              AND r.ca_min_calcium IS NOT NULL
        """).fetchone()[0]
        log(f"    postop_calcium_min_value planned backfill: {plan_ca}")

        if plan_ca > 0:
            con.execute("""
                UPDATE main.canonical_patient_master AS c
                   SET postop_calcium_min_value = r.ca_min_calcium
                  FROM _ca_rollup AS r
                 WHERE CAST(c.research_id AS VARCHAR) = r.research_id
                   AND c.postop_calcium_min_value IS NULL
                   AND r.ca_min_calcium IS NOT NULL
            """)
            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), "postop_calcium_min_value",
                  "MIN calcium from longitudinal_lab_canonical_v1 (post-LLM recovery)",
                  "v1 NULL only", plan_ca, None, None, SCRIPT])

        # Update has_low_calcium_flag
        con.execute("""
            UPDATE main.canonical_patient_master
               SET has_low_calcium_flag = TRUE
             WHERE postop_calcium_min_value < 8.0
               AND has_low_calcium_flag IS NOT TRUE
        """)
        low_ca_n = con.execute("""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE has_low_calcium_flag = TRUE
        """).fetchone()[0]
        log(f"    has_low_calcium_flag = TRUE: {low_ca_n}")

        # Update comp_hypocalcemia_confirmed where calcium < 8.0
        con.execute("""
            UPDATE main.canonical_patient_master
               SET comp_hypocalcemia_confirmed = TRUE
             WHERE postop_calcium_min_value < 8.0
               AND comp_hypocalcemia_confirmed IS NOT TRUE
        """)

        post_cpm_ca = con.execute("""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE postop_calcium_min_value IS NOT NULL
        """).fetchone()[0]
        post_hypo = con.execute("""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE comp_hypocalcemia_confirmed = TRUE
        """).fetchone()[0]
        log(f"    Post: postop_ca={post_cpm_ca}, hypocalcemia={post_hypo}")

        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), "comp_hypocalcemia_confirmed",
              "Set TRUE where postop_calcium_min_value < 8.0 (post-LLM recovery)",
              "v1 NULL only; threshold 8.0 mg/dL",
              post_hypo - pre_hypo, None, None, SCRIPT])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 331 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
