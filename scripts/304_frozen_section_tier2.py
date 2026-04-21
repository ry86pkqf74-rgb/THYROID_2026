"""
Script 304 — Build frozen_section_event_v1 + frozen_section_patient_wide_v1.

Source: note_entities_llm_frozen_section_detail (32,408 notes, ~8,640 entities).
Grain (event): one row per (research_id, note_index, entity_index).
Grain (wide): one row per research_id.

Also backfills CPM nlp_frozensec_n_events where NULL.

Usage:
    python 304_frozen_section_tier2.py            # dry-run
    python 304_frozen_section_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "304_frozen_section_tier2"


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


EVENT_SQL = """
WITH ent AS (
    SELECT research_id,
           note_index,
           note_date, note_type, extracted_at,
           UNNEST(CAST(json_extract(result_json, '$.entities') AS JSON[])) AS ent_json
      FROM main.note_entities_llm_frozen_section_detail
     WHERE result_json IS NOT NULL
       AND json_array_length(json_extract(result_json, '$.entities')) > 0
),
numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, note_index
               ORDER BY CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) NULLS LAST
           ) AS event_index
      FROM ent
)
SELECT
    research_id,
    note_index,
    note_date,
    note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    json_extract_string(ent_json, '$.entity_type')   AS entity_type,
    json_extract_string(ent_json, '$.entity_value')   AS entity_value,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_type')) = 'site'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%left lobe%' THEN 'left_lobe'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%right lobe%' THEN 'right_lobe'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%isthmus%' THEN 'isthmus'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%central neck%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%level vi%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%level 6%' THEN 'central_neck'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lateral%' THEN 'lateral_neck'
        ELSE NULL
    END AS frozen_section_site,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%benign%' THEN 'benign'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%papillary%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ptc%' THEN 'PTC'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%malignant%' THEN 'malignant'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%follicular lesion%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%follicular neoplasm%' THEN 'follicular_lesion'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%atypia%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%aus%' THEN 'atypia'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%indeterminate%' THEN 'indeterminate'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%defer%' THEN 'deferred'
        ELSE NULL
    END AS frozen_section_result,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_type')) = 'indication'
          OR LOWER(json_extract_string(ent_json, '$.entity_type')) ILIKE '%reason%'
        THEN json_extract_string(ent_json, '$.entity_value')
        ELSE NULL
    END AS frozen_section_indication,

    json_extract_string(ent_json, '$.entity_date')   AS frozen_section_date,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%defer%' THEN TRUE ELSE FALSE END AS was_deferred_flag,
    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_type')) ILIKE '%final%' THEN TRUE ELSE FALSE END AS was_final_diagnosis_flag,

    json_extract_string(ent_json, '$.present_or_negated')  AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text')       AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""


WIDE_SQL = """
WITH evt AS (
    SELECT * FROM main.frozen_section_event_v1
),
per_patient AS (
    SELECT
        research_id,
        COUNT(*) AS n_frozen_events,

        -- any_frozen_performed
        TRUE AS any_frozen_performed_flag,
        MIN(note_date) AS first_frozen_performed_date,
        MAX(note_date) AS last_frozen_performed_date,

        -- any_frozen_malignant_result
        BOOL_OR(frozen_section_result IN ('malignant', 'PTC')) AS any_frozen_malignant_result_flag,
        MIN(CASE WHEN frozen_section_result IN ('malignant', 'PTC') THEN note_date END)
            AS first_malignant_frozen_date,

        -- any_frozen_deferred
        BOOL_OR(was_deferred_flag) AS any_frozen_deferred_flag,
        MIN(CASE WHEN was_deferred_flag THEN note_date END) AS first_deferred_frozen_date,

        COUNT(DISTINCT note_index) AS n_notes_documenting_frozen_performed

    FROM evt
    GROUP BY research_id
),
first_refs AS (
    SELECT DISTINCT ON (e.research_id)
        e.research_id,
        e.source_note_ref AS first_frozen_performed_source_note_ref,
        e.evidence_text   AS first_frozen_performed_evidence_text
    FROM evt e
    JOIN per_patient pp ON pp.research_id = e.research_id AND e.note_date = pp.first_frozen_performed_date
    ORDER BY e.research_id, e.event_index
),
malignant_refs AS (
    SELECT DISTINCT ON (e.research_id)
        e.research_id,
        e.source_note_ref AS first_malignant_frozen_source_note_ref,
        e.evidence_text   AS first_malignant_frozen_evidence_text,
        COUNT(*) OVER (PARTITION BY e.research_id) AS n_notes_documenting_malignant_frozen
    FROM evt e
    JOIN per_patient pp ON pp.research_id = e.research_id
    WHERE e.frozen_section_result IN ('malignant', 'PTC')
    ORDER BY e.research_id, e.note_date, e.event_index
),
deferred_refs AS (
    SELECT DISTINCT ON (e.research_id)
        e.research_id,
        e.source_note_ref AS first_deferred_frozen_source_note_ref,
        e.evidence_text   AS first_deferred_frozen_evidence_text
    FROM evt e
    JOIN per_patient pp ON pp.research_id = e.research_id
    WHERE e.was_deferred_flag = TRUE
    ORDER BY e.research_id, e.note_date, e.event_index
),
slot_events AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY note_date, event_index) AS slot
    FROM evt
)
SELECT
    pp.*,

    fr.first_frozen_performed_source_note_ref,
    fr.first_frozen_performed_evidence_text,

    mr.first_malignant_frozen_source_note_ref,
    mr.first_malignant_frozen_evidence_text,
    mr.n_notes_documenting_malignant_frozen,

    dr.first_deferred_frozen_source_note_ref,
    dr.first_deferred_frozen_evidence_text,

    -- Slot columns 1..6
    MAX(CASE WHEN s.slot = 1 THEN s.frozen_section_site END) AS frozen_1_site,
    MAX(CASE WHEN s.slot = 1 THEN s.frozen_section_result END) AS frozen_1_result,
    MAX(CASE WHEN s.slot = 1 THEN s.note_date END) AS frozen_1_date,
    MAX(CASE WHEN s.slot = 1 THEN s.source_note_ref END) AS frozen_1_source_note_ref,
    MAX(CASE WHEN s.slot = 1 THEN s.evidence_text END) AS frozen_1_evidence_text,

    MAX(CASE WHEN s.slot = 2 THEN s.frozen_section_site END) AS frozen_2_site,
    MAX(CASE WHEN s.slot = 2 THEN s.frozen_section_result END) AS frozen_2_result,
    MAX(CASE WHEN s.slot = 2 THEN s.note_date END) AS frozen_2_date,
    MAX(CASE WHEN s.slot = 2 THEN s.source_note_ref END) AS frozen_2_source_note_ref,
    MAX(CASE WHEN s.slot = 2 THEN s.evidence_text END) AS frozen_2_evidence_text,

    MAX(CASE WHEN s.slot = 3 THEN s.frozen_section_site END) AS frozen_3_site,
    MAX(CASE WHEN s.slot = 3 THEN s.frozen_section_result END) AS frozen_3_result,
    MAX(CASE WHEN s.slot = 3 THEN s.note_date END) AS frozen_3_date,
    MAX(CASE WHEN s.slot = 3 THEN s.source_note_ref END) AS frozen_3_source_note_ref,
    MAX(CASE WHEN s.slot = 3 THEN s.evidence_text END) AS frozen_3_evidence_text,

    MAX(CASE WHEN s.slot = 4 THEN s.frozen_section_site END) AS frozen_4_site,
    MAX(CASE WHEN s.slot = 4 THEN s.frozen_section_result END) AS frozen_4_result,
    MAX(CASE WHEN s.slot = 4 THEN s.note_date END) AS frozen_4_date,
    MAX(CASE WHEN s.slot = 4 THEN s.source_note_ref END) AS frozen_4_source_note_ref,
    MAX(CASE WHEN s.slot = 4 THEN s.evidence_text END) AS frozen_4_evidence_text,

    MAX(CASE WHEN s.slot = 5 THEN s.frozen_section_site END) AS frozen_5_site,
    MAX(CASE WHEN s.slot = 5 THEN s.frozen_section_result END) AS frozen_5_result,
    MAX(CASE WHEN s.slot = 5 THEN s.note_date END) AS frozen_5_date,
    MAX(CASE WHEN s.slot = 5 THEN s.source_note_ref END) AS frozen_5_source_note_ref,
    MAX(CASE WHEN s.slot = 5 THEN s.evidence_text END) AS frozen_5_evidence_text,

    MAX(CASE WHEN s.slot = 6 THEN s.frozen_section_site END) AS frozen_6_site,
    MAX(CASE WHEN s.slot = 6 THEN s.frozen_section_result END) AS frozen_6_result,
    MAX(CASE WHEN s.slot = 6 THEN s.note_date END) AS frozen_6_date,
    MAX(CASE WHEN s.slot = 6 THEN s.source_note_ref END) AS frozen_6_source_note_ref,
    MAX(CASE WHEN s.slot = 6 THEN s.evidence_text END) AS frozen_6_evidence_text

FROM per_patient pp
LEFT JOIN first_refs fr ON fr.research_id = pp.research_id
LEFT JOIN malignant_refs mr ON mr.research_id = pp.research_id
LEFT JOIN deferred_refs dr ON dr.research_id = pp.research_id
LEFT JOIN slot_events s ON s.research_id = pp.research_id AND s.slot <= 6
GROUP BY
    pp.research_id, pp.n_frozen_events,
    pp.any_frozen_performed_flag, pp.first_frozen_performed_date, pp.last_frozen_performed_date,
    pp.any_frozen_malignant_result_flag, pp.first_malignant_frozen_date,
    pp.any_frozen_deferred_flag, pp.first_deferred_frozen_date,
    pp.n_notes_documenting_frozen_performed,
    fr.first_frozen_performed_source_note_ref, fr.first_frozen_performed_evidence_text,
    mr.first_malignant_frozen_source_note_ref, mr.first_malignant_frozen_evidence_text,
    mr.n_notes_documenting_malignant_frozen,
    dr.first_deferred_frozen_source_note_ref, dr.first_deferred_frozen_evidence_text
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 304 — frozen_section_event_v1 + patient_wide "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Preview event table
    log("Step 1: Building frozen_section_event_v1...")
    preview = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM ({EVENT_SQL}) t
    """).fetchone()
    log(f"  Event preview: {preview[0]} rows, {preview[1]} patients")

    if not args.commit:
        log("  (dry-run — no tables created)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Create event table
    con.execute(f"CREATE OR REPLACE TABLE main.frozen_section_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.frozen_section_event_v1").fetchone()
    log(f"  Created frozen_section_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    # Create patient-wide table
    log("Step 2: Building frozen_section_patient_wide_v1...")
    con.execute(f"CREATE OR REPLACE TABLE main.frozen_section_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.frozen_section_patient_wide_v1").fetchone()
    log(f"  Created frozen_section_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    # CPM backfill: nlp_frozensec_n_entities where NULL
    log("Step 3: CPM backfill nlp_frozensec_n_entities...")
    n_before = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE nlp_frozensec_n_entities IS NULL
    """).fetchone()[0]
    log(f"  NULLs before: {n_before}")

    con.execute("""
        UPDATE main.canonical_patient_master cpm
        SET nlp_frozensec_n_entities = w.n_frozen_events
        FROM main.frozen_section_patient_wide_v1 w
        WHERE cpm.research_id = w.research_id
          AND cpm.nlp_frozensec_n_entities IS NULL
    """)

    n_after = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE nlp_frozensec_n_entities IS NULL
    """).fetchone()[0]
    n_filled = n_before - n_after
    log(f"  Filled {n_filled} NULLs (remaining: {n_after})")

    if n_filled > 0:
        con.execute("""
            CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
                backfilled_at TIMESTAMP,
                script VARCHAR,
                cpm_column VARCHAR,
                n_rows_updated BIGINT,
                source_description VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), SCRIPT, "nlp_frozensec_n_entities",
              n_filled, "frozen_section_patient_wide_v1.n_frozen_events"])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 304 complete.")


if __name__ == "__main__":
    main()
