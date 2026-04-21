"""
Script 310 — Build functional_outcomes_event_v1 + functional_outcomes_patient_wide_v1.

Source: note_entities_llm_functional_outcomes (11,037 notes, ~3,322 entities).

"persistent" cut-point: >=1 documentation at >180 days (6 months) after
the patient's first documented surgery date from operative_episode_detail_v2.

Usage:
    python 310_functional_outcomes_tier2.py            # dry-run
    python 310_functional_outcomes_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "310_functional_outcomes_tier2"


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
      FROM main.note_entities_llm_functional_outcomes
     WHERE result_json IS NOT NULL
       AND json_array_length(json_extract(result_json, '$.entities')) > 0
),
numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, note_index
               ORDER BY TRY_CAST(json_extract_string(ent_json, '51source_line') AS BIGINT) NULLS LAST
           ) AS event_index
      FROM ent
)
SELECT
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_type')) ILIKE '%voice%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%voice%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%hoarse%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%dysphon%' THEN 'voice'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%swallow%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%dysphag%' THEN 'swallowing'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%scar%' THEN 'scar'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%quality of life%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%qol%' THEN 'qol'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%hypocalcemia%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%tingling%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%tetany%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%paresthesia%' THEN 'hypocalcemia_sx'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%shoulder%' THEN 'shoulder_mobility'
        ELSE NULL
    END AS fo_domain,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%severe%' THEN 'severe'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%moderate%' THEN 'moderate'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%mild%' THEN 'mild'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%none%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%no %complaint%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%normal%' THEN 'none'
        ELSE NULL
    END AS fo_severity,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%resolved%' THEN 'resolved'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%improv%' THEN 'improving'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%persist%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%permanent%' THEN 'persistent'
        ELSE NULL
    END AS fo_resolution,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.entity_value') AS entity_value,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    TRY_CAST(json_extract_string(ent_json, '101source_line') AS BIGINT) AS source_line

FROM numbered
"""

# "persistent" defined as: >=1 symptom documented >180 days after first surgery
WIDE_SQL = """
WITH evt AS (SELECT * FROM main.functional_outcomes_event_v1),
first_surgery AS (
    SELECT research_id, MIN(TRY_CAST(surgery_date_native AS DATE)) AS first_surgery_date
    FROM main.operative_episode_detail_v2
    GROUP BY research_id
)
SELECT
    e.research_id,

    BOOL_OR(fo_domain = 'voice' AND present_or_negated != 'negated') AS voice_symptom_postop_flag,
    MIN(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.note_date END) AS voice_symptom_postop_first_date,
    MIN(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.source_note_ref END) AS voice_symptom_postop_first_source_note_ref,
    MIN(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.evidence_text END) AS voice_symptom_postop_first_evidence_text,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.note_date END) AS voice_symptom_postop_last_date,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.source_note_ref END) AS voice_symptom_postop_last_source_note_ref,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.evidence_text END) AS voice_symptom_postop_last_evidence_text,
    COUNT(DISTINCT CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated' THEN e.note_index END) AS voice_symptom_postop_n_notes_documenting,

    BOOL_OR(fo_domain = 'swallowing' AND present_or_negated != 'negated') AS swallowing_symptom_postop_flag,
    MIN(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.note_date END) AS swallowing_symptom_postop_first_date,
    MIN(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.source_note_ref END) AS swallowing_symptom_postop_first_source_note_ref,
    MIN(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.evidence_text END) AS swallowing_symptom_postop_first_evidence_text,
    MAX(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.note_date END) AS swallowing_symptom_postop_last_date,
    MAX(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.source_note_ref END) AS swallowing_symptom_postop_last_source_note_ref,
    MAX(CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.evidence_text END) AS swallowing_symptom_postop_last_evidence_text,
    COUNT(DISTINCT CASE WHEN fo_domain = 'swallowing' AND present_or_negated != 'negated' THEN e.note_index END) AS swallowing_symptom_postop_n_notes_documenting,

    -- persistent_voice_change: >=1 voice symptom documented >180 days after first surgery
    BOOL_OR(fo_domain = 'voice' AND present_or_negated != 'negated'
            AND fs.first_surgery_date IS NOT NULL
            AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY) AS persistent_voice_change_flag,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.note_date END) AS persistent_voice_change_last_documented_date,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.source_note_ref END) AS persistent_voice_change_last_source_note_ref,
    MAX(CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.evidence_text END) AS persistent_voice_change_last_evidence_text,
    COUNT(DISTINCT CASE WHEN fo_domain = 'voice' AND present_or_negated != 'negated'
                        AND fs.first_surgery_date IS NOT NULL
                        AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
                        THEN e.note_index END) AS persistent_voice_change_n_notes_beyond_6mo,

    -- persistent_hypocalcemia_sx: >=1 hypocalcemia symptom documented >180 days after first surgery
    BOOL_OR(fo_domain = 'hypocalcemia_sx' AND present_or_negated != 'negated'
            AND fs.first_surgery_date IS NOT NULL
            AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY) AS persistent_hypocalcemia_sx_flag,
    MAX(CASE WHEN fo_domain = 'hypocalcemia_sx' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.note_date END) AS persistent_hypocalcemia_sx_last_documented_date,
    MAX(CASE WHEN fo_domain = 'hypocalcemia_sx' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.source_note_ref END) AS persistent_hypocalcemia_sx_last_source_note_ref,
    MAX(CASE WHEN fo_domain = 'hypocalcemia_sx' AND present_or_negated != 'negated'
             AND fs.first_surgery_date IS NOT NULL
             AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
             THEN e.evidence_text END) AS persistent_hypocalcemia_sx_last_evidence_text,
    COUNT(DISTINCT CASE WHEN fo_domain = 'hypocalcemia_sx' AND present_or_negated != 'negated'
                        AND fs.first_surgery_date IS NOT NULL
                        AND TRY_CAST(e.note_date AS DATE) > fs.first_surgery_date + INTERVAL '180' DAY
                        THEN e.note_index END) AS persistent_hypocalcemia_sx_n_notes_beyond_6mo

FROM evt e
LEFT JOIN first_surgery fs ON fs.research_id = e.research_id
GROUP BY e.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 310 — functional_outcomes tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.functional_outcomes_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.functional_outcomes_event_v1").fetchone()
    log(f"  Created functional_outcomes_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    con.execute(f"CREATE OR REPLACE TABLE main.functional_outcomes_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.functional_outcomes_patient_wide_v1").fetchone()
    log(f"  Created functional_outcomes_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 310 complete.")


if __name__ == "__main__":
    main()
