"""
Script 313 — Build event + patient_wide tables for three small-volume domains:
  - rad_treatment (~580 entities)
  - patient_decision_adherence (~641 entities)
  - dynamic_risk_response (~53 entities)

One script, six tables (intentional exception to one-table-per-script rule).

Usage:
    python 313_small_volume_domains_tier2.py            # dry-run
    python 313_small_volume_domains_tier2.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "313_small_volume_domains_tier2"

ENTITY_CTE = """
WITH ent AS (
    SELECT research_id, note_index, note_date, note_type, extracted_at,
           UNNEST(CAST(json_extract(result_json, '$.entities') AS JSON[])) AS ent_json
      FROM main.note_entities_llm_{domain}
     WHERE result_json IS NOT NULL
       AND json_array_length(json_extract(result_json, '$.entities')) > 0
),
numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, note_index
               ORDER BY TRY_CAST(json_extract_string(ent_json, '$.source_line') AS BIGINT) NULLS LAST
           ) AS event_index
      FROM ent
)
"""


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


# ── Rad treatment ─────────────────────────────────────────────────────────

RT_EVENT_SQL = ENTITY_CTE.format(domain="rad_treatment") + """
SELECT
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%ebrt%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%external beam%' THEN 'EBRT'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%proton%' THEN 'proton'
        ELSE 'other'
    END AS rt_modality,

    json_extract_string(ent_json, '$.entity_value') AS entity_value,
    json_extract_string(ent_json, '$.entity_date') AS rt_date_range_raw,
    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    TRY_CAST(json_extract_string(ent_json, '78source_line') AS BIGINT) AS source_line
FROM numbered
"""

RT_WIDE_SQL = """
WITH evt AS (SELECT * FROM main.rad_treatment_event_v1)
SELECT
    research_id,
    BOOL_OR(rt_modality = 'EBRT') AS had_external_beam_radiation_flag,
    MIN(CASE WHEN rt_modality = 'EBRT' THEN note_date END) AS ebrt_first_treatment_date,
    MIN(CASE WHEN rt_modality = 'EBRT' THEN source_note_ref END) AS ebrt_first_treatment_source_note_ref,
    MIN(CASE WHEN rt_modality = 'EBRT' THEN evidence_text END) AS ebrt_first_treatment_evidence_text,
    MAX(CASE WHEN rt_modality = 'EBRT' THEN note_date END) AS ebrt_last_treatment_date,
    COUNT(DISTINCT CASE WHEN rt_modality = 'EBRT' THEN note_index END) AS ebrt_n_notes_documenting,

    MAX(rt_modality) AS rt_modality_primary,
    MIN(note_date) AS rt_modality_primary_first_date,
    MIN(source_note_ref) AS rt_modality_primary_source_note_ref,
    MIN(evidence_text) AS rt_modality_primary_evidence_text
FROM evt
GROUP BY research_id
"""


# ── Patient decision adherence ────────────────────────────────────────────

PDA_EVENT_SQL = ENTITY_CTE.format(domain="patient_decision_adherence") + """
SELECT
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%declined%surgery%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%refused%surgery%' THEN 'declined_surgery'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%declined%rai%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%refused%rai%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%refused%radioactive%' THEN 'declined_RAI'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%declined%follow%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%refused%follow%' THEN 'declined_followup'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%refused%biopsy%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%declined%biopsy%' THEN 'refused_biopsy'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%lost to%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%no show%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%no-show%' THEN 'lost_to_followup'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%compli%'
          OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%adher%' THEN 'compliance_positive'
        ELSE NULL
    END AS pda_category,

    json_extract_string(ent_json, '$.entity_date') AS pda_date_raw,
    CASE WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%concern%'
           OR LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%risk%'
         THEN TRUE ELSE FALSE END AS pda_clinician_concern_flag,

    json_extract_string(ent_json, '$.entity_value') AS entity_value,
    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    TRY_CAST(json_extract_string(ent_json, '138source_line') AS BIGINT) AS source_line
FROM numbered
"""

PDA_WIDE_SQL = """
WITH evt AS (SELECT * FROM main.patient_decision_adherence_event_v1)
SELECT
    research_id,
    BOOL_OR(pda_category = 'declined_surgery') AS any_declined_surgery_flag,
    MIN(CASE WHEN pda_category = 'declined_surgery' THEN note_date END) AS declined_surgery_first_date,
    MIN(CASE WHEN pda_category = 'declined_surgery' THEN source_note_ref END) AS declined_surgery_first_source_note_ref,
    MIN(CASE WHEN pda_category = 'declined_surgery' THEN evidence_text END) AS declined_surgery_first_evidence_text,
    COUNT(DISTINCT CASE WHEN pda_category = 'declined_surgery' THEN note_index END) AS declined_surgery_n_notes,

    BOOL_OR(pda_category = 'declined_RAI') AS any_declined_rai_flag,
    MIN(CASE WHEN pda_category = 'declined_RAI' THEN note_date END) AS declined_rai_first_date,
    MIN(CASE WHEN pda_category = 'declined_RAI' THEN source_note_ref END) AS declined_rai_first_source_note_ref,
    MIN(CASE WHEN pda_category = 'declined_RAI' THEN evidence_text END) AS declined_rai_first_evidence_text,
    COUNT(DISTINCT CASE WHEN pda_category = 'declined_RAI' THEN note_index END) AS declined_rai_n_notes,

    BOOL_OR(pda_category = 'lost_to_followup') AS any_lost_to_followup_concern_flag,
    MIN(CASE WHEN pda_category = 'lost_to_followup' THEN note_date END) AS lost_to_followup_first_date,
    MIN(CASE WHEN pda_category = 'lost_to_followup' THEN source_note_ref END) AS lost_to_followup_first_source_note_ref,
    MIN(CASE WHEN pda_category = 'lost_to_followup' THEN evidence_text END) AS lost_to_followup_first_evidence_text,
    MAX(CASE WHEN pda_category = 'lost_to_followup' THEN note_date END) AS last_lost_to_followup_concern_date,
    COUNT(DISTINCT CASE WHEN pda_category = 'lost_to_followup' THEN note_index END) AS lost_to_followup_n_notes
FROM evt
GROUP BY research_id
"""


# ── Dynamic risk response ────────────────────────────────────────────────

DRR_EVENT_SQL = ENTITY_CTE.format(domain="dynamic_risk_response") + """
SELECT
    research_id, note_index, note_date, note_type,
    CAST(research_id AS VARCHAR) || ':' || CAST(note_index AS VARCHAR) AS source_note_ref,
    event_index,

    CASE
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%excellent%' THEN 'excellent'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%indeterminate%' THEN 'indeterminate'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%biochemical%incomplete%' THEN 'biochemical_incomplete'
        WHEN LOWER(json_extract_string(ent_json, '$.entity_value')) ILIKE '%structural%incomplete%' THEN 'structural_incomplete'
        ELSE NULL
    END AS drr_category,

    json_extract_string(ent_json, '$.entity_value') AS drr_criteria_basis_raw,
    json_extract_string(ent_json, '$.entity_date') AS drr_date_raw,

    json_extract_string(ent_json, '$.entity_type') AS entity_type,
    json_extract_string(ent_json, '$.present_or_negated') AS present_or_negated,
    CAST(json_extract_string(ent_json, '$.confidence') AS DOUBLE) AS confidence,
    json_extract_string(ent_json, '$.evidence_text') AS evidence_text,
    TRY_CAST(json_extract_string(ent_json, '192source_line') AS BIGINT) AS source_line
FROM numbered
"""

DRR_WIDE_SQL = """
WITH evt AS (SELECT * FROM main.dynamic_risk_response_event_v1),
latest AS (
    SELECT DISTINCT ON (research_id)
        research_id,
        drr_category AS drr_latest_category,
        source_note_ref AS drr_latest_source_note_ref,
        note_date AS drr_latest_note_date,
        evidence_text AS drr_latest_evidence_text
    FROM evt
    ORDER BY research_id, note_date DESC, event_index DESC
),
worst AS (
    SELECT DISTINCT ON (research_id)
        research_id,
        drr_category AS drr_worst_category_ever,
        note_date AS drr_worst_category_date,
        source_note_ref AS drr_worst_category_source_note_ref,
        evidence_text AS drr_worst_category_evidence_text
    FROM evt
    ORDER BY research_id,
        CASE drr_category
            WHEN 'structural_incomplete' THEN 4
            WHEN 'biochemical_incomplete' THEN 3
            WHEN 'indeterminate' THEN 2
            WHEN 'excellent' THEN 1
            ELSE 0
        END DESC,
        note_date DESC
)
SELECT
    l.research_id,
    l.drr_latest_category, l.drr_latest_source_note_ref,
    l.drr_latest_note_date, l.drr_latest_evidence_text,
    w.drr_worst_category_ever, w.drr_worst_category_date,
    w.drr_worst_category_source_note_ref, w.drr_worst_category_evidence_text
FROM latest l
LEFT JOIN worst w ON w.research_id = l.research_id
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    log("=" * 72)
    log(f"Script 313 — small-volume domains tier2 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    domains = [
        ("rad_treatment", RT_EVENT_SQL, RT_WIDE_SQL,
         "rad_treatment_event_v1", "rad_treatment_patient_wide_v1"),
        ("patient_decision_adherence", PDA_EVENT_SQL, PDA_WIDE_SQL,
         "patient_decision_adherence_event_v1", "patient_decision_adherence_patient_wide_v1"),
        ("dynamic_risk_response", DRR_EVENT_SQL, DRR_WIDE_SQL,
         "dynamic_risk_response_event_v1", "dynamic_risk_response_patient_wide_v1"),
    ]

    for domain_name, evt_sql, wide_sql, evt_tbl, wide_tbl in domains:
        log(f"  --- {domain_name} ---")
        preview = con.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT research_id) FROM ({evt_sql}) t
        """).fetchone()
        log(f"  Event preview: {preview[0]} rows, {preview[1]} patients")

        if args.commit:
            con.execute(f"CREATE OR REPLACE TABLE main.{evt_tbl} AS {evt_sql}")
            n_evt = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{evt_tbl}").fetchone()
            log(f"  Created {evt_tbl}: {n_evt[0]} rows, {n_evt[1]} patients")

            con.execute(f"CREATE OR REPLACE TABLE main.{wide_tbl} AS {wide_sql}")
            n_wide = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{wide_tbl}").fetchone()
            log(f"  Created {wide_tbl}: {n_wide[0]} rows, {n_wide[1]} patients")

    if not args.commit:
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 313 complete.")


if __name__ == "__main__":
    main()
