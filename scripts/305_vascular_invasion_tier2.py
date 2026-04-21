"""
Script 305 — Build vascular_invasion_event_v1 + vascular_invasion_patient_wide_v1.

Source: note_entities_llm_vascular_invasion (39,210 notes, ~22,800 entities).
Grain (event): one row per (research_id, note_index, entity_index).
Grain (wide): one row per research_id.

Backfills CPM vi_any_llm, vi_extensive_llm where NULL.

Usage:
    python 305_vascular_invasion_tier2.py            # dry-run
    python 305_vascular_invasion_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "305_vascular_invasion_tier2"


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
      FROM main.note_entities_llm_vascular_invasion
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
    END AS vi_present,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%extensive%' THEN 'extensive'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%focal%' THEN 'focal'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%angioinvas%' THEN 'angioinvasion'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymphovascular%' THEN 'lymphovascular'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%vascular%'
         AND LOWER(json_extract_string(ent_json, '$.entity_value')) NOT ILIKE '%lymph%' THEN 'vascular_only'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymphatic%' THEN 'lymphatic_only'
        ELSE NULL
    END AS vi_extent,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%arteri%' THEN 'artery'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%vein%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%venous%' THEN 'vein'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lymphatic%' THEN 'lymphatic'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%capsul%' THEN 'capsular'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%extrathyroid%' THEN 'extrathyroidal'
        ELSE NULL
    END AS vi_vessel_type,

    json_extract_string(ent_json, '$.present_or_negated')  AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text')       AS evidence_text,
    CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) AS source_line

FROM numbered
"""


WIDE_SQL = """
WITH evt AS (
    SELECT * FROM main.vascular_invasion_event_v1
),
per_patient AS (
    SELECT
        research_id,

        BOOL_OR(vi_present = 'Y') AS vi_any_positive_flag,
        MIN(CASE WHEN vi_present = 'Y' THEN note_date END) AS vi_first_positive_date,
        MAX(CASE WHEN vi_present = 'Y' THEN note_date END) AS vi_last_positive_date,
        COUNT(DISTINCT CASE WHEN vi_present = 'Y' THEN note_index END) AS vi_n_notes_positive,

        BOOL_OR(vi_present = 'N') AS vi_any_negative_flag,
        MIN(CASE WHEN vi_present = 'N' THEN note_date END) AS vi_first_negative_date,

        BOOL_OR(vi_extent = 'extensive') AS vi_extensive_flag,
        MIN(CASE WHEN vi_extent = 'extensive' THEN note_date END) AS vi_first_extensive_date,

        MAX(CASE
            WHEN vi_extent = 'extensive' THEN 4
            WHEN vi_extent = 'angioinvasion' THEN 3
            WHEN vi_extent = 'lymphovascular' THEN 2
            WHEN vi_extent = 'focal' THEN 1
            ELSE 0
        END) AS vi_max_extent_rank

    FROM evt
    GROUP BY research_id
)
SELECT
    pp.*,

    CASE pp.vi_max_extent_rank
        WHEN 4 THEN 'extensive'
        WHEN 3 THEN 'angioinvasion'
        WHEN 2 THEN 'lymphovascular'
        WHEN 1 THEN 'focal'
        ELSE NULL
    END AS vi_max_extent,

    fp.vi_first_positive_source_note_ref,
    fp.vi_first_positive_evidence_text,

    fn.vi_first_negative_source_note_ref,
    fn.vi_first_negative_evidence_text,

    fe.vi_first_extensive_source_note_ref,
    fe.vi_first_extensive_evidence_text,

    me.vi_max_extent_date,
    me.vi_max_extent_source_note_ref,
    me.vi_max_extent_evidence_text

FROM per_patient pp

LEFT JOIN (
    SELECT DISTINCT ON (research_id)
        research_id, source_note_ref AS vi_first_positive_source_note_ref,
        evidence_text AS vi_first_positive_evidence_text
    FROM evt WHERE vi_present = 'Y'
    ORDER BY research_id, note_date, event_index
) fp ON fp.research_id = pp.research_id

LEFT JOIN (
    SELECT DISTINCT ON (research_id)
        research_id, source_note_ref AS vi_first_negative_source_note_ref,
        evidence_text AS vi_first_negative_evidence_text
    FROM evt WHERE vi_present = 'N'
    ORDER BY research_id, note_date, event_index
) fn ON fn.research_id = pp.research_id

LEFT JOIN (
    SELECT DISTINCT ON (research_id)
        research_id, source_note_ref AS vi_first_extensive_source_note_ref,
        evidence_text AS vi_first_extensive_evidence_text
    FROM evt WHERE vi_extent = 'extensive'
    ORDER BY research_id, note_date, event_index
) fe ON fe.research_id = pp.research_id

LEFT JOIN (
    SELECT DISTINCT ON (e.research_id)
        e.research_id,
        e.note_date AS vi_max_extent_date,
        e.source_note_ref AS vi_max_extent_source_note_ref,
        e.evidence_text AS vi_max_extent_evidence_text
    FROM evt e
    JOIN per_patient pp2 ON pp2.research_id = e.research_id
    WHERE (CASE
        WHEN e.vi_extent = 'extensive' THEN 4
        WHEN e.vi_extent = 'angioinvasion' THEN 3
        WHEN e.vi_extent = 'lymphovascular' THEN 2
        WHEN e.vi_extent = 'focal' THEN 1 ELSE 0 END) = pp2.vi_max_extent_rank
    ORDER BY e.research_id, e.note_date DESC, e.event_index
) me ON me.research_id = pp.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 305 — vascular_invasion tier2 "
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

    con.execute(f"CREATE OR REPLACE TABLE main.vascular_invasion_event_v1 AS {EVENT_SQL}")
    n_evt = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.vascular_invasion_event_v1").fetchone()
    log(f"  Created vascular_invasion_event_v1: {n_evt[0]} rows, {n_evt[1]} patients")

    log("Step 2: Building vascular_invasion_patient_wide_v1...")
    con.execute(f"CREATE OR REPLACE TABLE main.vascular_invasion_patient_wide_v1 AS {WIDE_SQL}")
    n_wide = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.vascular_invasion_patient_wide_v1").fetchone()
    log(f"  Created vascular_invasion_patient_wide_v1: {n_wide[0]} rows, {n_wide[1]} patients")

    # CPM backfill
    log("Step 3: CPM backfill vi_any_llm, vi_extensive_llm...")
    for col, src_col in [("nlp_vasc_any_vi", "vi_any_positive_flag"),
                          ("nlp_vasc_extensive_vi", "vi_extensive_flag")]:
        try:
            n_null = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master WHERE {col} IS NULL
            """).fetchone()[0]
        except Exception:
            log(f"  Column {col} not found on CPM — skipping backfill")
            continue

        if n_null > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master cpm
                SET {col} = w.{src_col}
                FROM main.vascular_invasion_patient_wide_v1 w
                WHERE cpm.research_id = w.research_id
                  AND cpm.{col} IS NULL
            """)
            n_after = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master WHERE {col} IS NULL
            """).fetchone()[0]
            n_filled = n_null - n_after
            log(f"  {col}: filled {n_filled} NULLs (remaining {n_after})")
            if n_filled > 0:
                con.execute("""
                    INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES (?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), SCRIPT, col,
                      n_filled, f"vascular_invasion_patient_wide_v1.{src_col}"])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 305 complete.")


if __name__ == "__main__":
    main()
