"""
Script 291 — Integrate TSH from note_entities_llm_labs into
longitudinal_lab_canonical_v1.

note_entities_llm_labs has 886 rows (861 patients) with TSH values in
result_json that were parsed by the LLM but never loaded into the
canonical lab table (which has only 515 TSH rows / 413 patients).

Approach:
  1. Extract TSH entities from result_json (UNNEST + filter entity_type='tsh').
  2. Parse numeric value from entity_value ("0.96 mIU/L" -> 0.96).
  3. INSERT into longitudinal_lab_canonical_v1 with source_table='llm_notes',
     deduplicating against existing rows on (research_id, lab_date, lab_name).
  4. If parse-failure rate > 5%, STOP and write queue table instead.
  5. Re-run tsh_suppressed_ever backfill on CPM.

Usage:
    python 291_tsh_llm_integration.py            # dry-run
    python 291_tsh_llm_integration.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "291_tsh_llm_integration"
TSH_SUPPRESSED_THRESHOLD = 0.1


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
    log(f"Script 291 — TSH LLM integration "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Pre-state
    pre = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.longitudinal_lab_canonical_v1
        WHERE lab_name_standardized = 'tsh'
    """).fetchone()
    log(f"  TSH rows pre: {pre[0]}, distinct_rid: {pre[1]}")

    # Step 1: Extract TSH entities from LLM JSON
    log("  Step 1: Extracting TSH entities from note_entities_llm_labs...")
    con.execute("DROP TABLE IF EXISTS _tsh_llm_raw")
    con.execute("""
        CREATE TEMP TABLE _tsh_llm_raw AS
        WITH src AS (
            SELECT
                CAST(research_id AS BIGINT) AS research_id,
                note_row_id,
                TRY_CAST(note_date AS DATE) AS note_date,
                CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_labs
            WHERE result_json IS NOT NULL
              AND LOWER(result_json) LIKE '%tsh%'
              AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        )
        SELECT
            s.research_id,
            s.note_row_id,
            s.note_date,
            json_extract_string(e, '$.entity_type') AS entity_type,
            json_extract_string(e, '$.entity_value') AS entity_value,
            TRY_CAST(json_extract_string(e, '$.entity_date') AS DATE) AS entity_date,
            TRY_CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence,
            json_extract_string(e, '$.present_or_negated') AS present_or_negated
        FROM src s, UNNEST(s.arr) AS t(e)
        WHERE LOWER(json_extract_string(e, '$.entity_type')) = 'tsh'
          AND json_extract_string(e, '$.present_or_negated') = 'present'
    """)

    n_raw = con.execute("SELECT COUNT(*) FROM _tsh_llm_raw").fetchone()[0]
    n_raw_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM _tsh_llm_raw"
    ).fetchone()[0]
    log(f"  Extracted TSH entities: {n_raw} rows, {n_raw_rid} patients")

    # Step 2: Parse numeric value
    log("  Step 2: Parsing numeric values...")
    con.execute("DROP TABLE IF EXISTS _tsh_llm_parsed")
    con.execute("""
        CREATE TEMP TABLE _tsh_llm_parsed AS
        SELECT
            research_id,
            note_row_id,
            COALESCE(entity_date, note_date) AS lab_date,
            entity_value AS value_raw,
            TRY_CAST(
                regexp_extract(entity_value, '([0-9]+\\.?[0-9]*)', 1)
                AS DOUBLE
            ) AS value_numeric,
            CASE
                WHEN LOWER(entity_value) LIKE '%miu%' THEN 'mIU/L'
                WHEN LOWER(entity_value) LIKE '%uiu%' THEN 'uIU/mL'
                ELSE 'mIU/L'
            END AS unit_standardized,
            confidence
        FROM _tsh_llm_raw
    """)

    n_parsed = con.execute(
        "SELECT COUNT(*) FROM _tsh_llm_parsed WHERE value_numeric IS NOT NULL"
    ).fetchone()[0]
    n_failed = con.execute(
        "SELECT COUNT(*) FROM _tsh_llm_parsed WHERE value_numeric IS NULL"
    ).fetchone()[0]
    total = n_parsed + n_failed
    fail_rate = n_failed / total if total > 0 else 0
    log(f"  Parsed: {n_parsed}, Failed: {n_failed}, Fail rate: {fail_rate:.1%}")

    if fail_rate > 0.05:
        log("  STOP: Parse failure rate > 5%. Writing queue table for review.")
        con.execute("""
            CREATE OR REPLACE TABLE manuscript_workspace.llm_tsh_parse_queue_v1 AS
            SELECT * FROM _tsh_llm_parsed WHERE value_numeric IS NULL
        """)
        n_q = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.llm_tsh_parse_queue_v1"
        ).fetchone()[0]
        log(f"  Queue table written: {n_q} rows")
        raise SystemExit(
            f"Parse failure rate {fail_rate:.1%} > 5%. "
            f"Review manuscript_workspace.llm_tsh_parse_queue_v1"
        )

    # Step 3: Deduplicate against existing rows
    log("  Step 3: Deduplicating against existing longitudinal_lab_canonical_v1...")
    con.execute("DROP TABLE IF EXISTS _tsh_to_insert")
    con.execute("""
        CREATE TEMP TABLE _tsh_to_insert AS
        SELECT
            p.research_id,
            p.lab_date,
            'resolved' AS lab_date_status,
            p.value_raw AS lab_name_raw,
            'tsh' AS lab_name_standardized,
            'thyroid_function' AS analyte_group,
            p.value_raw,
            p.value_numeric,
            NULL::INTEGER AS unit_raw,
            p.unit_standardized,
            NULL::INTEGER AS reference_range,
            NULL::INTEGER AS abnormal_flag,
            FALSE AS is_censored,
            'note_entities_llm_labs' AS source_table,
            '291_tsh_llm_integration' AS source_script,
            'llm_notes' AS ingestion_wave,
            'tier_3_llm' AS data_completeness_tier,
            'LLM-extracted from clinical notes' AS provenance_note,
            p.value_numeric AS value_corrected,
            NULL::VARCHAR AS calcium_correction_applied,
            TRUE AS is_in_canonical_cancer_cohort
        FROM _tsh_llm_parsed p
        WHERE p.value_numeric IS NOT NULL
          AND p.lab_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM main.longitudinal_lab_canonical_v1 l
              WHERE l.research_id = p.research_id
                AND l.lab_date = p.lab_date
                AND l.lab_name_standardized = 'tsh'
          )
    """)

    n_insert = con.execute("SELECT COUNT(*) FROM _tsh_to_insert").fetchone()[0]
    n_insert_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM _tsh_to_insert"
    ).fetchone()[0]
    log(f"  New rows to insert (deduped): {n_insert}, from {n_insert_rid} patients")

    # Also count rows excluded by dedup
    n_deduped = n_parsed - n_insert - n_failed
    log(f"  Excluded by dedup (already in canonical): ~{max(0, n_deduped)}")

    # Sample
    sample = con.execute("""
        SELECT value_numeric FROM _tsh_to_insert LIMIT 5
    """).fetchall()
    sample_str = ", ".join(str(s[0]) for s in sample)
    log(f"  Sample values: {sample_str}")

    if not args.commit:
        log("  (dry-run — no INSERT)")
    else:
        # Insert
        con.execute("""
            INSERT INTO main.longitudinal_lab_canonical_v1
            SELECT * FROM _tsh_to_insert
        """)
        post = con.execute("""
            SELECT COUNT(*), COUNT(DISTINCT research_id)
            FROM main.longitudinal_lab_canonical_v1
            WHERE lab_name_standardized = 'tsh'
        """).fetchone()
        log(f"  TSH rows post: {post[0]}, distinct_rid: {post[1]}")
        log(f"  Delta: +{post[0] - pre[0]} rows, +{post[1] - pre[1]} patients")

    # Step 4: Re-run tsh_suppressed_ever backfill
    log("")
    log("  Step 4: Re-running tsh_suppressed_ever backfill...")

    pre_tsh_pop = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE tsh_suppressed_ever IS NOT NULL
    """).fetchone()[0]
    log(f"  tsh_suppressed_ever pre-pop: {pre_tsh_pop}")

    if not args.commit:
        plan_tsh = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            WHERE c.tsh_suppressed_ever IS NULL
              AND EXISTS (
                  SELECT 1 FROM main.longitudinal_lab_canonical_v1 l
                  WHERE l.research_id = c.research_id
                    AND l.lab_name_standardized = 'tsh'
                    AND l.value_numeric < {TSH_SUPPRESSED_THRESHOLD}
              )
        """).fetchone()[0]
        log(f"  Would set tsh_suppressed_ever=TRUE for {plan_tsh} additional patients (dry-run)")
    else:
        con.execute("DROP TABLE IF EXISTS _tsh_suppressed")
        con.execute(f"""
            CREATE TEMP TABLE _tsh_suppressed AS
            SELECT research_id,
                   MAX(CASE WHEN value_numeric < {TSH_SUPPRESSED_THRESHOLD}
                       THEN TRUE ELSE FALSE END) AS value
            FROM main.longitudinal_lab_canonical_v1
            WHERE lab_name_standardized = 'tsh'
              AND value_numeric IS NOT NULL
            GROUP BY research_id
        """)

        con.execute("""
            UPDATE main.canonical_patient_master AS c
               SET tsh_suppressed_ever = s.value
              FROM _tsh_suppressed AS s
             WHERE c.research_id = s.research_id
               AND c.tsh_suppressed_ever IS NULL
               AND s.value IS NOT NULL
        """)

        post_tsh_pop = con.execute("""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE tsh_suppressed_ever IS NOT NULL
        """).fetchone()[0]
        actual_tsh = post_tsh_pop - pre_tsh_pop
        log(f"  tsh_suppressed_ever post-pop: {post_tsh_pop} (delta +{actual_tsh})")

        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dt.datetime.utcnow(), "tsh_suppressed_ever",
            "re-run after LLM TSH integration (291)",
            f"TSH < {TSH_SUPPRESSED_THRESHOLD} mIU/L",
            actual_tsh, None, sample_str, SCRIPT
        ])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 291 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
