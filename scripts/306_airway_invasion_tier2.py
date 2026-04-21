"""
Script 306 — Build airway_invasion_event_v1 + airway_invasion_patient_wide_v1.

Source: note_entities_llm_airway_invasion (48,169 notes, ~11,601 entities).
Grain (event): one row per (research_id, note_index, entity_index).
Grain (wide): one row per research_id.

Backfills CPM ai_any_llm, ai_max_extent_llm where NULL.

Usage:
    python 306_airway_invasion_tier2.py            # dry-run
    python 306_airway_invasion_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "306_airway_invasion_tier2"


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
      FROM main.note_entities_llm_airway_invasion
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
        WHEN LOWER(json_extract_string(ent_json, '$.present_or_negated')) IN ('present', 'positive') THEN 'Y'
        WHEN LOWER(json_extract_string(ent_json, '$.present_or_negated')) IN ('negated', 'negative', 'absent') THEN 'N'
        WHEN LOWER(json_extract_string(ent_json, '$.present_or_negated')) = 'uncertain' THEN 'uncertain'
        ELSE NULL
    END AS ai_present,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%full%thickness%' THEN 'full_thickness'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%invad%'
         AND LOWER(json_extract_string(ent_json, '$.entity_value')) NOT ILIKE '%full%' THEN 'invading_no_full_thickness'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%abut%' THEN 'abutting'
        ELSE NULL
    END AS ai_extent,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%trachea%' THEN 'trachea'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%esophag%' THEN 'esophagus'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%rln%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%recurrent laryngeal%' THEN 'RLN'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%strap%' THEN 'strap_muscle'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%prevertebral%' THEN 'prevertebral'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%carotid%' THEN 'carotid'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ijv%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%jugular%' THEN 'IJV'
        ELSE NULL
    END AS ai_structure,

    json_extract_string(ent_json, '$.present_or_negated')  AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text')       AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""

WIDE_SQL = """
WITH evt AS (
    SELECT * FROM main.airway_invasion_event_v1
),
per_patient AS (
    SELECT
        research_id,
        BOOL_OR(ai_present = 'Y') AS ai_any_positive_flag,
        MIN(CASE WHEN ai_present = 'Y' THEN note_date END) AS ai_first_positive_date,
        MAX(CASE WHEN ai_present = 'Y' THEN note_date END) AS ai_last_positive_date,
        COUNT(DISTINCT CASE WHEN ai_present = 'Y' THEN note_index END) AS ai_n_notes_positive,

        MAX(CASE
            WHEN ai_extent = 'full_thickness' THEN 3
            WHEN ai_extent = 'invading_no_full_thickness' THEN 2
            WHEN ai_extent = 'abutting' THEN 1
            ELSE 0
        END) AS ai_max_extent_rank

    FROM evt GROUP BY research_id
)
SELECT
    pp.*,
    CASE pp.ai_max_extent_rank
        WHEN 3 THEN 'full_thickness'
        WHEN 2 THEN 'invading_no_full_thickness'
        WHEN 1 THEN 'abutting'
        ELSE NULL
    END AS ai_max_extent,

    fp.ai_first_positive_source_note_ref,
    fp.ai_first_positive_evidence_text,

    me.ai_max_extent_date,
    me.ai_max_extent_source_note_ref,
    me.ai_max_extent_evidence_text

FROM per_patient pp

LEFT JOIN (
    SELECT DISTINCT ON (research_id)
        research_id,
        source_note_ref AS ai_first_positive_source_note_ref,
        evidence_text AS ai_first_positive_evidence_text
    FROM evt WHERE ai_present = 'Y'
    ORDER BY research_id, note_date, event_index
) fp ON fp.research_id = pp.research_id

LEFT JOIN (
    SELECT DISTINCT ON (e.research_id)
        e.research_id,
        e.note_date AS ai_max_extent_date,
        e.source_note_ref AS ai_max_extent_source_note_ref,
        e.evidence_text AS ai_max_extent_evidence_text
    FROM evt e
    JOIN per_patient pp2 ON pp2.research_id = e.research_id
    WHERE (CASE
        WHEN e.ai_extent = 'full_thickness' THEN 3
        WHEN e.ai_extent = 'invading_no_full_thickness' THEN 2
        WHEN e.ai_extent = 'abutting' THEN 1 ELSE 0 END) = pp2.ai_max_extent_rank
    ORDER BY e.research_id, e.note_date DESC, e.event_index
) me ON me.research_id = pp.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 306 — airway_invasion tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.airway_invasion_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.airway_invasion_event_v1").fetchone()
    log(f"  Created airway_invasion_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    log("Step 2: Building airway_invasion_patient_wide_v1...")
    con.execute(f"CREATE OR REPLACE TABLE main.airway_invasion_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.airway_invasion_patient_wide_v1").fetchone()
    log(f"  Created airway_invasion_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    # CPM backfill
    log("Step 3: CPM backfill...")
    for col, src_col in [("nlp_airway_any_invasion", "ai_any_positive_flag")]:
        try:
            n_null = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master WHERE {col} IS NULL
            """).fetchone()[0]
        except Exception:
            log(f"  Column {col} not found on CPM — skipping")
            continue
        if n_null > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master cpm
                SET {col} = w.{src_col}
                FROM main.airway_invasion_patient_wide_v1 w
                WHERE cpm.research_id = w.research_id AND cpm.{col} IS NULL
            """)
            n_after = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master WHERE {col} IS NULL
            """).fetchone()[0]
            n_filled = n_null - n_after
            log(f"  {col}: filled {n_filled} NULLs")
            if n_filled > 0:
                con.execute("""
                    INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES (?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), SCRIPT, col, n_filled,
                      f"airway_invasion_patient_wide_v1.{src_col}"])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 306 complete.")


if __name__ == "__main__":
    main()
