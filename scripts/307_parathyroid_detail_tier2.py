"""
Script 307 — Build parathyroid_detail_event_v1 + parathyroid_patient_wide_v1.

Source: note_entities_llm_parathyroid_detail (17,321 notes, ~10,130 entities).
Grain (event): one row per (research_id, note_index, entity_index).
Grain (wide): one row per research_id.

Usage:
    python 307_parathyroid_detail_tier2.py            # dry-run
    python 307_parathyroid_detail_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "307_parathyroid_detail_tier2"


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
           UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS ent_json
      FROM main.note_entities_llm_parathyroid_detail
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
    json_extract_string(ent_json, '$.entity_type')   AS entity_type,
    json_extract_string(ent_json, '$.entity_value')   AS entity_value,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%identified%' THEN 'identified'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%preserved%' THEN 'preserved'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%autotransplant%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%reimplant%' THEN 'autotransplanted'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%inadvertent%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%removed%' THEN 'inadvertently_removed'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ischemi%' THEN 'ischemic'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%biops%' THEN 'biopsied'
        ELSE NULL
    END AS pt_action,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%left upper%' OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lu%parathyroid%' THEN 'LU'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%left lower%' OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ll%parathyroid%' THEN 'LL'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%right upper%' OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ru%parathyroid%' THEN 'RU'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%right lower%' OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%rl%parathyroid%' THEN 'RL'
        ELSE 'unknown'
    END AS pt_gland_position,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%scm%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%sternocleidomast%' THEN 'SCM'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%forearm%' THEN 'forearm'
        ELSE NULL
    END AS pt_implant_site,

    json_extract_string(ent_json, '$.present_or_negated')  AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text')       AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (
    SELECT * FROM main.parathyroid_detail_event_v1
)
SELECT
    research_id,

    COUNT(CASE WHEN pt_action = 'identified' THEN 1 END) AS n_pt_identified,
    COUNT(CASE WHEN pt_action = 'preserved' THEN 1 END) AS n_pt_preserved,
    COUNT(CASE WHEN pt_action = 'autotransplanted' THEN 1 END) AS n_pt_autotransplanted,
    COUNT(CASE WHEN pt_action = 'inadvertently_removed' THEN 1 END) AS n_pt_removed,

    BOOL_OR(pt_action = 'ischemic') AS any_ischemic_change_flag,
    MIN(CASE WHEN pt_action = 'ischemic' THEN note_date END) AS first_ischemic_date,
    MIN(CASE WHEN pt_action = 'ischemic' THEN source_note_ref END) AS first_ischemic_source_note_ref,
    MIN(CASE WHEN pt_action = 'ischemic' THEN evidence_text END) AS first_ischemic_evidence_text,

    MAX(CASE WHEN pt_action = 'autotransplanted' THEN pt_implant_site END) AS autotransplant_site_primary,
    MIN(CASE WHEN pt_action = 'autotransplanted' THEN note_date END) AS autotransplant_site_first_date,
    MIN(CASE WHEN pt_action = 'autotransplanted' THEN source_note_ref END) AS autotransplant_site_first_source_note_ref,
    MIN(CASE WHEN pt_action = 'autotransplanted' THEN evidence_text END) AS autotransplant_site_first_evidence_text,

    MIN(CASE WHEN pt_action = 'identified' THEN source_note_ref END) AS n_pt_identified_first_source_note_ref,
    MIN(CASE WHEN pt_action = 'identified' THEN evidence_text END) AS n_pt_identified_first_evidence_text,
    MIN(CASE WHEN pt_action = 'preserved' THEN source_note_ref END) AS n_pt_preserved_first_source_note_ref,
    MIN(CASE WHEN pt_action = 'preserved' THEN evidence_text END) AS n_pt_preserved_first_evidence_text,
    MIN(CASE WHEN pt_action = 'autotransplanted' THEN source_note_ref END) AS n_pt_autotransplanted_first_source_note_ref,
    MIN(CASE WHEN pt_action = 'autotransplanted' THEN evidence_text END) AS n_pt_autotransplanted_first_evidence_text,
    MIN(CASE WHEN pt_action = 'inadvertently_removed' THEN source_note_ref END) AS n_pt_removed_first_source_note_ref,
    MIN(CASE WHEN pt_action = 'inadvertently_removed' THEN evidence_text END) AS n_pt_removed_first_evidence_text

FROM evt
GROUP BY research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 307 — parathyroid_detail tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.parathyroid_detail_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.parathyroid_detail_event_v1").fetchone()
    log(f"  Created parathyroid_detail_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    log("Step 2: Building parathyroid_patient_wide_v1...")
    con.execute(f"CREATE OR REPLACE TABLE main.parathyroid_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.parathyroid_patient_wide_v1").fetchone()
    log(f"  Created parathyroid_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 307 complete.")


if __name__ == "__main__":
    main()
