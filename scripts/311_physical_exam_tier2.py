"""
Script 311 — Build physical_exam_event_v1 + physical_exam_patient_wide_v1.

Source: note_entities_llm_physical_exam (11,037 notes, ~2,025 entities).

Usage:
    python 311_physical_exam_tier2.py            # dry-run
    python 311_physical_exam_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "311_physical_exam_tier2"


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
      FROM main.note_entities_llm_physical_exam
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
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%mass%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%nodule%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%palpable%thyroid%' THEN 'thyroid_mass'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymph%node%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymphadenop%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%neck%mass%' THEN 'neck_ln'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%voice%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%hoarse%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%dysphon%' THEN 'voice'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%scar%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%incision%' THEN 'scar'
        ELSE 'other'
    END AS pe_finding_category,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%left%' THEN 'left'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%right%' THEN 'right'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%bilateral%' THEN 'bilateral'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%midline%' THEN 'midline'
        ELSE NULL
    END AS pe_laterality,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%tender%' THEN TRUE ELSE FALSE END AS pe_tenderness_flag,
    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%fixed%' THEN TRUE ELSE FALSE END AS pe_fixed_flag,
    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%mobile%' THEN TRUE ELSE FALSE END AS pe_mobile_flag,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.entity_value') AS entity_value,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (SELECT * FROM main.physical_exam_event_v1 WHERE present_or_negated != 'negated')
SELECT
    research_id,

    BOOL_OR(pe_finding_category = 'thyroid_mass') AS pe_palpable_thyroid_mass_flag,
    MIN(CASE WHEN pe_finding_category = 'thyroid_mass' THEN note_date END) AS pe_palpable_thyroid_mass_first_date,
    MIN(CASE WHEN pe_finding_category = 'thyroid_mass' THEN source_note_ref END) AS pe_palpable_thyroid_mass_first_source_note_ref,
    MIN(CASE WHEN pe_finding_category = 'thyroid_mass' THEN evidence_text END) AS pe_palpable_thyroid_mass_first_evidence_text,
    MAX(CASE WHEN pe_finding_category = 'thyroid_mass' THEN note_date END) AS pe_palpable_thyroid_mass_last_date,
    MAX(CASE WHEN pe_finding_category = 'thyroid_mass' THEN source_note_ref END) AS pe_palpable_thyroid_mass_last_source_note_ref,
    COUNT(DISTINCT CASE WHEN pe_finding_category = 'thyroid_mass' THEN note_index END) AS pe_palpable_thyroid_mass_n_notes_documenting,

    BOOL_OR(pe_finding_category = 'neck_ln') AS pe_palpable_ln_flag,
    MIN(CASE WHEN pe_finding_category = 'neck_ln' THEN note_date END) AS pe_palpable_ln_first_date,
    MIN(CASE WHEN pe_finding_category = 'neck_ln' THEN source_note_ref END) AS pe_palpable_ln_first_source_note_ref,
    MIN(CASE WHEN pe_finding_category = 'neck_ln' THEN evidence_text END) AS pe_palpable_ln_first_evidence_text,
    MAX(CASE WHEN pe_finding_category = 'neck_ln' THEN note_date END) AS pe_palpable_ln_last_date,
    MAX(CASE WHEN pe_finding_category = 'neck_ln' THEN source_note_ref END) AS pe_palpable_ln_last_source_note_ref,
    COUNT(DISTINCT CASE WHEN pe_finding_category = 'neck_ln' THEN note_index END) AS pe_palpable_ln_n_notes_documenting,

    BOOL_OR(pe_finding_category = 'voice') AS pe_documented_voice_abnormality_flag,
    MIN(CASE WHEN pe_finding_category = 'voice' THEN note_date END) AS pe_documented_voice_abnormality_first_date,
    MIN(CASE WHEN pe_finding_category = 'voice' THEN source_note_ref END) AS pe_documented_voice_abnormality_first_source_note_ref,
    MIN(CASE WHEN pe_finding_category = 'voice' THEN evidence_text END) AS pe_documented_voice_abnormality_first_evidence_text,
    MAX(CASE WHEN pe_finding_category = 'voice' THEN note_date END) AS pe_documented_voice_abnormality_last_date,
    MAX(CASE WHEN pe_finding_category = 'voice' THEN source_note_ref END) AS pe_documented_voice_abnormality_last_source_note_ref,
    COUNT(DISTINCT CASE WHEN pe_finding_category = 'voice' THEN note_index END) AS pe_documented_voice_abnormality_n_notes_documenting

FROM evt
GROUP BY research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 311 — physical_exam tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.physical_exam_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.physical_exam_event_v1").fetchone()
    log(f"  Created physical_exam_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    con.execute(f"CREATE OR REPLACE TABLE main.physical_exam_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.physical_exam_patient_wide_v1").fetchone()
    log(f"  Created physical_exam_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 311 complete.")


if __name__ == "__main__":
    main()
