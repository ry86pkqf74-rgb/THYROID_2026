"""
Script 308 — Build past_surgical_hx_event_v1 + past_surgical_hx_patient_wide_v1.

Source: note_entities_llm_past_surgical_hx (11,037 notes, ~3,919 entities).

Usage:
    python 308_past_surgical_hx_tier2.py            # dry-run
    python 308_past_surgical_hx_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "308_past_surgical_hx_tier2"


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
      FROM main.note_entities_llm_past_surgical_hx
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

    json_extract_string(ent_json, '$.entity_value') AS psh_procedure,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%' THEN 'thyroid'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%neck%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%parathyroid%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%laryn%' THEN 'neck'
        ELSE NULL
    END AS psh_anatomic_site,

    json_extract_string(ent_json, '$.entity_date') AS psh_date_raw,
    TRY_CAST(json_extract_string(ent_json, '$.entity_date') AS DATE) AS psh_date_parsed,

    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%' THEN TRUE ELSE FALSE END AS psh_was_thyroid_related_flag,
    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%neck%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%parathyroid%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%laryn%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%thyroid%' THEN TRUE ELSE FALSE END AS psh_was_neck_related_flag,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (SELECT * FROM main.past_surgical_hx_event_v1)
SELECT
    research_id,

    BOOL_OR(psh_was_thyroid_related_flag) AS had_prior_thyroid_surgery_flag,
    MIN(CASE WHEN psh_was_thyroid_related_flag THEN note_date END) AS prior_thyroid_surgery_first_date,
    MIN(CASE WHEN psh_was_thyroid_related_flag THEN source_note_ref END) AS prior_thyroid_surgery_first_documentation_source_note_ref,
    MIN(CASE WHEN psh_was_thyroid_related_flag THEN evidence_text END) AS prior_thyroid_surgery_first_evidence_text,
    COUNT(DISTINCT CASE WHEN psh_was_thyroid_related_flag THEN note_index END) AS prior_thyroid_surgery_n_notes_documenting,

    MAX(CASE WHEN psh_was_thyroid_related_flag THEN psh_procedure END) AS prior_thyroid_surgery_type_primary,
    MIN(CASE WHEN psh_was_thyroid_related_flag THEN source_note_ref END) AS prior_thyroid_surgery_type_primary_source_note_ref,
    MIN(CASE WHEN psh_was_thyroid_related_flag THEN evidence_text END) AS prior_thyroid_surgery_type_primary_evidence_text,

    BOOL_OR(psh_was_neck_related_flag AND NOT psh_was_thyroid_related_flag) AS had_prior_neck_surgery_flag,
    MIN(CASE WHEN psh_was_neck_related_flag AND NOT psh_was_thyroid_related_flag THEN note_date END) AS prior_neck_surgery_first_date,
    MIN(CASE WHEN psh_was_neck_related_flag AND NOT psh_was_thyroid_related_flag THEN source_note_ref END) AS prior_neck_surgery_first_source_note_ref,
    MIN(CASE WHEN psh_was_neck_related_flag AND NOT psh_was_thyroid_related_flag THEN evidence_text END) AS prior_neck_surgery_first_evidence_text

FROM evt
GROUP BY research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 308 — past_surgical_hx tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.past_surgical_hx_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.past_surgical_hx_event_v1").fetchone()
    log(f"  Created past_surgical_hx_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    con.execute(f"CREATE OR REPLACE TABLE main.past_surgical_hx_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.past_surgical_hx_patient_wide_v1").fetchone()
    log(f"  Created past_surgical_hx_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 308 complete.")


if __name__ == "__main__":
    main()
