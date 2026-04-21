"""
Script 309 — Build past_medical_hx_event_v1 + past_medical_hx_patient_wide_v1.

Source: note_entities_llm_past_medical_hx (11,037 notes, ~865 entities — low volume).

Usage:
    python 309_past_medical_hx_tier2.py            # dry-run
    python 309_past_medical_hx_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "309_past_medical_hx_tier2"


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
      FROM main.note_entities_llm_past_medical_hx
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

    json_extract_string(ent_json, '$.entity_value') AS pmh_condition,
    json_extract_string(ent_json, '$.entity_date') AS pmh_onset_date_raw,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.present_or_negated')) = 'present' THEN 'active'
        WHEN LOWER(json_extract_string(ent_json, '$.present_or_negated')) = 'negated' THEN 'resolved'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%chronic%' THEN 'chronic'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%history%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%prior%' THEN 'historical'
        ELSE NULL
    END AS pmh_status,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%goiter%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%graves%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%hashimoto%' THEN TRUE ELSE FALSE END AS pmh_is_thyroid_related_flag,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%radiation%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%xrt%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%irradiat%' THEN TRUE ELSE FALSE END AS pmh_is_radiation_exposure_flag,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%cancer%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%carcinoma%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%malign%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymphoma%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%melanoma%' THEN TRUE ELSE FALSE END AS pmh_is_cancer_history_flag,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%hashimoto%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%graves%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%autoimmune%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lupus%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%rheumatoid%' THEN TRUE ELSE FALSE END AS pmh_is_autoimmune_flag,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (SELECT * FROM main.past_medical_hx_event_v1)
SELECT
    research_id,

    BOOL_OR(pmh_is_radiation_exposure_flag) AS had_prior_neck_radiation_flag,
    MIN(CASE WHEN pmh_is_radiation_exposure_flag THEN note_date END) AS prior_neck_radiation_first_date,
    MIN(CASE WHEN pmh_is_radiation_exposure_flag THEN source_note_ref END) AS prior_neck_radiation_first_source_note_ref,
    MIN(CASE WHEN pmh_is_radiation_exposure_flag THEN evidence_text END) AS prior_neck_radiation_first_evidence_text,
    COUNT(DISTINCT CASE WHEN pmh_is_radiation_exposure_flag THEN note_index END) AS prior_neck_radiation_n_notes_documenting,

    BOOL_OR(pmh_is_cancer_history_flag) AS had_prior_cancer_flag,
    MIN(CASE WHEN pmh_is_cancer_history_flag THEN note_date END) AS prior_cancer_first_date,
    MIN(CASE WHEN pmh_is_cancer_history_flag THEN source_note_ref END) AS prior_cancer_first_source_note_ref,
    MIN(CASE WHEN pmh_is_cancer_history_flag THEN evidence_text END) AS prior_cancer_first_evidence_text,
    COUNT(DISTINCT CASE WHEN pmh_is_cancer_history_flag THEN note_index END) AS prior_cancer_n_notes_documenting,

    BOOL_OR(pmh_is_autoimmune_flag) AS has_autoimmune_thyroid_disease_flag,
    MIN(CASE WHEN pmh_is_autoimmune_flag THEN note_date END) AS autoimmune_first_date,
    MIN(CASE WHEN pmh_is_autoimmune_flag THEN source_note_ref END) AS autoimmune_first_source_note_ref,
    MIN(CASE WHEN pmh_is_autoimmune_flag THEN evidence_text END) AS autoimmune_first_evidence_text,
    COUNT(DISTINCT CASE WHEN pmh_is_autoimmune_flag THEN note_index END) AS autoimmune_n_notes_documenting

FROM evt
GROUP BY research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 309 — past_medical_hx tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.past_medical_hx_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.past_medical_hx_event_v1").fetchone()
    log(f"  Created past_medical_hx_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    con.execute(f"CREATE OR REPLACE TABLE main.past_medical_hx_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.past_medical_hx_patient_wide_v1").fetchone()
    log(f"  Created past_medical_hx_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 309 complete.")


if __name__ == "__main__":
    main()
