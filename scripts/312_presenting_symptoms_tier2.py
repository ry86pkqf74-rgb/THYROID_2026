"""
Script 312 — Build presenting_symptoms_event_v1 + presenting_symptoms_patient_wide_v1.

Source: note_entities_llm_presenting_symptoms (11,037 notes, ~280 entities — low volume).

Usage:
    python 312_presenting_symptoms_tier2.py            # dry-run
    python 312_presenting_symptoms_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "312_presenting_symptoms_tier2"


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
    SELECT research_id, note_index, note_date, note_type, extracted_at,
           UNNEST(CAST(json_extract(result_json, '$.entities') AS JSON[])) AS ent_json
      FROM main.note_entities_llm_presenting_symptoms
     WHERE result_json IS NOT NULL
       AND json_array_length(json_extract(result_json, '$.entities')) > 0
),
numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, note_index
               ORDER BY TRY_CAST(json_extract_string(ent_json, '48source_line') AS BIGINT) NULLS LAST
           ) AS event_index
      FROM ent
)
SELECT
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    json_extract_string(ent_json, '$.entity_value') AS ps_symptom,
    json_extract_string(ent_json, '$.entity_date') AS ps_onset_duration_raw,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%severe%' THEN 'severe'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%moderate%' THEN 'moderate'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%mild%' THEN 'mild'
        ELSE NULL
    END AS ps_severity,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_type')) ILIKE '%trigger%'
           OR LOWER(json_extract_string(ent_json, '$.entity_type')) ILIKE '%reason%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%incidental%' = FALSE
         THEN TRUE ELSE FALSE END AS ps_was_trigger_for_workup_flag,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    TRY_CAST(json_extract_string(ent_json, '76source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (SELECT * FROM main.presenting_symptoms_event_v1),
first_symptom AS (
    SELECT DISTINCT ON (research_id)
        research_id,
        ps_symptom AS presenting_symptom_primary,
        source_note_ref AS presenting_symptom_primary_source_note_ref,
        note_date AS presenting_symptom_primary_note_date,
        evidence_text AS presenting_symptom_primary_evidence_text
    FROM evt
    WHERE present_or_negated != 'negated'
    ORDER BY research_id, note_date, event_index
)
SELECT
    e.research_id,

    fs.presenting_symptom_primary,
    fs.presenting_symptom_primary_source_note_ref,
    fs.presenting_symptom_primary_note_date,
    fs.presenting_symptom_primary_evidence_text,

    BOOL_OR(e.present_or_negated != 'negated') AS was_symptomatic_at_presentation_flag,
    MIN(CASE WHEN e.present_or_negated != 'negated' THEN e.note_date END) AS symptomatic_first_date,
    MIN(CASE WHEN e.present_or_negated != 'negated' THEN e.source_note_ref END) AS symptomatic_first_source_note_ref,
    MIN(CASE WHEN e.present_or_negated != 'negated' THEN e.evidence_text END) AS symptomatic_first_evidence_text

FROM evt e
LEFT JOIN first_symptom fs ON fs.research_id = e.research_id
GROUP BY e.research_id,
    fs.presenting_symptom_primary, fs.presenting_symptom_primary_source_note_ref,
    fs.presenting_symptom_primary_note_date, fs.presenting_symptom_primary_evidence_text
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 312 — presenting_symptoms tier2 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    preview = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM ({EVENT_SQL}) t
    """).fetchone()
    log(f"  Event preview: {preview[0]} rows, {preview[1]} patients")

    if not args.commit:
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(f"CREATE OR REPLACE TABLE main.presenting_symptoms_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.presenting_symptoms_event_v1").fetchone()
    log(f"  Created presenting_symptoms_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    con.execute(f"CREATE OR REPLACE TABLE main.presenting_symptoms_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.presenting_symptoms_patient_wide_v1").fetchone()
    log(f"  Created presenting_symptoms_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 312 complete.")


if __name__ == "__main__":
    main()
