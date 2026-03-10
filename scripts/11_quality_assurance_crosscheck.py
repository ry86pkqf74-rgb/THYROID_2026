#!/usr/bin/env python3
"""
11_quality_assurance_crosscheck.py — Complete Validation & Final Extraction

Last heavy-compute step while MotherDuck Business trial is free.

Phases:
  1. Connect to MotherDuck, print compute tier info
  2. Build master_timeline (one row per patient-surgery, handles multi-surgery)
  3. Upgrade extracted_clinical_events_v3 → v4 with relative-day anchoring
  4. Domain-specific consistency checks → qa_issues table
  5. Final extraction sweep on raw notes (RAI, recurrence, meds, PMH)
  6. Outputs: Parquet exports, qa_summary_report.md, advanced_features_v3
  7. Safety: EXPLAIN ANALYZE, compute-tier advisory, trial summary

Usage:
  python scripts/11_quality_assurance_crosscheck.py
  python scripts/11_quality_assurance_crosscheck.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("qa_crosscheck")

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports"
STUDIES = ROOT / "studies"
QA_DIR = STUDIES / "qa_crosscheck"
MD_DATABASE = "thyroid_research_2026"

PHASE_TIMES: dict[str, float] = {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 2 — Master Timeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MASTER_TIMELINE_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE master_timeline AS
    WITH surgery_sources AS (
        -- Source 1: master_cohort (one surgery_date per patient)
        SELECT
            CAST(research_id AS INT) AS research_id,
            TRY_CAST(surgery_date AS DATE) AS surgery_date,
            'master_cohort' AS source
        FROM master_cohort
        WHERE surgery_date IS NOT NULL
          AND TRY_CAST(surgery_date AS DATE) IS NOT NULL

        UNION ALL

        -- Source 2: operative_details (may have multiple rows per patient)
        SELECT
            CAST(research_id AS INT) AS research_id,
            TRY_CAST(surg_date AS DATE) AS surgery_date,
            'operative_details' AS source
        FROM operative_details
        WHERE surg_date IS NOT NULL
          AND TRY_CAST(surg_date AS DATE) IS NOT NULL

        UNION ALL

        -- Source 3: path_synoptics — duplicate research_ids signal re-operations
        SELECT
            CAST(research_id AS INT) AS research_id,
            TRY_CAST(surgery_date AS DATE) AS surgery_date,
            'path_synoptics' AS source
        FROM path_synoptics
        WHERE surgery_date IS NOT NULL
          AND TRY_CAST(surgery_date AS DATE) IS NOT NULL
    ),
    deduplicated AS (
        SELECT DISTINCT
            research_id,
            surgery_date
        FROM surgery_sources
        WHERE surgery_date >= '1990-01-01'
          AND surgery_date <= CURRENT_DATE + INTERVAL '365' DAY
    ),
    numbered AS (
        SELECT
            research_id,
            surgery_date,
            ROW_NUMBER() OVER (
                PARTITION BY research_id ORDER BY surgery_date
            ) AS surgery_number,
            COUNT(*) OVER (PARTITION BY research_id) AS total_surgeries
        FROM deduplicated
    )
    SELECT
        research_id,
        surgery_number,
        surgery_date,
        total_surgeries,
        CASE
            WHEN surgery_number = 1 THEN 'initial'
            ELSE 'reoperation'
        END AS surgery_type,
        CASE
            WHEN surgery_number > 1 THEN DATEDIFF('day',
                LAG(surgery_date) OVER (
                    PARTITION BY research_id ORDER BY surgery_date
                ),
                surgery_date
            )
            ELSE NULL
        END AS days_since_prior_surgery
    FROM numbered
    ORDER BY research_id, surgery_number
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 3 — extracted_clinical_events_v4
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EVENTS_V4_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_clinical_events_v4 AS
    WITH events AS (
        SELECT
            e.*,
            e.followup_date AS event_dt
        FROM extracted_clinical_events_v3 e
    ),
    nearest_surgery AS (
        SELECT
            ev.research_id AS ev_rid,
            ev.event_date   AS ev_date,
            mt.surgery_number,
            mt.surgery_date,
            ABS(DATEDIFF('day', mt.surgery_date, ev.event_dt)) AS abs_days,
            DATEDIFF('day', mt.surgery_date, ev.event_dt) AS signed_days,
            ROW_NUMBER() OVER (
                PARTITION BY ev.research_id, ev.event_date
                ORDER BY ABS(DATEDIFF('day', mt.surgery_date, ev.event_dt))
            ) AS rn
        FROM events ev
        JOIN master_timeline mt
            ON CAST(ev.research_id AS INT) = mt.research_id
        WHERE ev.event_dt IS NOT NULL
    )
    SELECT
        e.research_id,
        e.event_type,
        e.event_subtype,
        e.event_value,
        e.event_unit,
        e.event_date,
        e.event_text,
        e.source_column,
        e.followup_date,
        COALESCE(ns.signed_days, e.event_relative_days)
            AS days_since_nearest_surgery,
        ns.surgery_number AS nearest_surgery_number,
        e.confidence_score
    FROM extracted_clinical_events_v3 e
    LEFT JOIN nearest_surgery ns
        ON CAST(e.research_id AS INT) = ns.ev_rid
       AND e.event_date = ns.ev_date
       AND ns.rn = 1
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 4 — QA Consistency Checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QA_ISSUES_SETUP = """\
    CREATE OR REPLACE TABLE qa_issues (
        check_id        VARCHAR,
        severity        VARCHAR,
        research_id     INT,
        description     VARCHAR,
        detail          VARCHAR,
        checked_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

QA_CHECKS: list[tuple[str, str, str, str]] = [
    # (check_id, severity, description, insert SQL)

    (
        "tg_rise_before_recurrence",
        "warning",
        "Tg or TSH rising within 90 days before a recurrence event",
        textwrap.dedent("""\
            INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
            SELECT
                'tg_rise_before_recurrence',
                'warning',
                CAST(lab.research_id AS INT),
                'Tg/TSH rising within 90d pre-recurrence',
                'lab=' || lab.event_subtype
                    || ' val=' || COALESCE(CAST(lab.event_value AS VARCHAR), '?')
                    || ' date=' || COALESCE(lab.event_date, '?')
                    || ' recurrence=' || COALESCE(rec.event_date, '?')
            FROM extracted_clinical_events_v4 lab
            JOIN extracted_clinical_events_v4 rec
                ON CAST(lab.research_id AS INT) = CAST(rec.research_id AS INT)
               AND rec.event_subtype = 'recurrence'
               AND rec.followup_date IS NOT NULL
            WHERE lab.event_type = 'lab'
              AND lab.event_subtype IN ('thyroglobulin', 'tsh')
              AND lab.event_value IS NOT NULL
              AND lab.followup_date IS NOT NULL
              AND lab.followup_date BETWEEN rec.followup_date - INTERVAL '90' DAY
                                        AND rec.followup_date
              AND lab.event_value > 2.0
        """),
    ),

    (
        "imaging_vs_path_size",
        "info",
        "Pre-op nodule size vs path tumor size difference >20%",
        textwrap.dedent("""\
            INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
            WITH preop_us AS (
                SELECT
                    CAST(research_id AS INT) AS research_id,
                    MAX(TRY_CAST(nodule_1_largest_dim_cm AS DOUBLE)) AS us_size_cm
                FROM us_nodules_tirads
                GROUP BY research_id
            )
            SELECT
                'imaging_vs_path_size',
                'info',
                tp.research_id,
                'US nodule size vs path tumor size >20% discrepancy',
                'us=' || ROUND(u.us_size_cm, 2) || 'cm'
                    || ' path=' || ROUND(TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE), 2) || 'cm'
                    || ' diff=' || ROUND(ABS(u.us_size_cm
                        - TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE))
                        / GREATEST(TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE), 0.1) * 100, 1) || '%'
            FROM tumor_pathology tp
            JOIN preop_us u ON tp.research_id = CAST(u.research_id AS VARCHAR)
            WHERE TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) > 0
              AND u.us_size_cm > 0
              AND ABS(u.us_size_cm - TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE))
                  / GREATEST(TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE), 0.1) > 0.20
        """),
    ),

    (
        "complication_vs_opnote",
        "warning",
        "RLN injury flagged but no nerve monitoring recorded (or vice versa)",
        textwrap.dedent("""\
            INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
            SELECT
                'complication_vs_opnote',
                'warning',
                CAST(comp.research_id AS INT),
                'RLN injury vs op-note mismatch',
                'rln=' || COALESCE(CAST(comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR), 'NULL')
                    || ' vocal_cord=' || COALESCE(CAST(comp.vocal_cord_status AS VARCHAR), 'NULL')
                    || ' ebl=' || COALESCE(CAST(od.ebl AS VARCHAR), 'NULL')
            FROM complications comp
            LEFT JOIN operative_details od
                ON comp.research_id = od.research_id
            WHERE (
                LOWER(COALESCE(CAST(comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR), ''))
                    NOT IN ('', 'no', 'none', 'n/a', 'na')
                AND od.research_id IS NULL
            )
            OR (
                LOWER(COALESCE(CAST(comp.vocal_cord_status AS VARCHAR), ''))
                    LIKE '%paralysis%'
                AND LOWER(COALESCE(CAST(comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR), ''))
                    IN ('', 'no', 'none', 'n/a', 'na')
            )
        """),
    ),

    (
        "date_sanity",
        "error",
        "Event date before first surgery or >10 years after last surgery",
        textwrap.dedent("""\
            INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
            WITH patient_bounds AS (
                SELECT
                    research_id,
                    MIN(surgery_date) AS first_surgery,
                    MAX(surgery_date) AS last_surgery
                FROM master_timeline
                GROUP BY research_id
            )
            SELECT
                'date_sanity',
                'error',
                CAST(e.research_id AS INT),
                CASE
                    WHEN e.followup_date < pb.first_surgery
                    THEN 'Event before first surgery'
                    ELSE 'Event >10yr after last surgery'
                END,
                'event_date=' || COALESCE(e.event_date, '?')
                    || ' type=' || COALESCE(e.event_subtype, '?')
                    || ' first_surg=' || CAST(pb.first_surgery AS VARCHAR)
                    || ' last_surg=' || CAST(pb.last_surgery AS VARCHAR)
            FROM extracted_clinical_events_v4 e
            JOIN patient_bounds pb
                ON CAST(e.research_id AS INT) = pb.research_id
            WHERE e.followup_date IS NOT NULL
              AND (
                  e.followup_date < pb.first_surgery
                  OR e.followup_date > pb.last_surgery + INTERVAL '3652' DAY
              )
        """),
    ),

    (
        "multi_surgery_recurrence_gap",
        "warning",
        "Multi-surgery patient without prior recurrence event before re-op",
        textwrap.dedent("""\
            INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
            WITH reops AS (
                SELECT research_id, surgery_number, surgery_date
                FROM master_timeline
                WHERE surgery_number > 1
            ),
            prior_recurrence AS (
                SELECT DISTINCT
                    CAST(research_id AS INT) AS research_id
                FROM extracted_clinical_events_v4
                WHERE event_subtype = 'recurrence'
            )
            SELECT
                'multi_surgery_recurrence_gap',
                'warning',
                r.research_id,
                'Re-operation (surgery #' || r.surgery_number
                    || ') without documented recurrence event',
                'reop_date=' || CAST(r.surgery_date AS VARCHAR)
            FROM reops r
            LEFT JOIN prior_recurrence pr ON r.research_id = pr.research_id
            WHERE pr.research_id IS NULL
        """),
    ),
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 5 — Final Extraction Sweep
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

REMAINING_EVENTS_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_remaining_events AS
    WITH notes_flat AS (
        SELECT
            CAST("Research ID number" AS INT) AS research_id,
            col_name,
            CAST(col_text AS VARCHAR) AS col_text
        FROM raw_clinical_notes
        UNPIVOT (col_text FOR col_name IN (
            COLUMNS(* EXCLUDE "Research ID number")
        ))
        WHERE col_text IS NOT NULL
          AND LENGTH(CAST(col_text AS VARCHAR)) > 10
    ),
    comp_flat AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            '_complications' AS col_name,
            COALESCE(
                CAST("_raw_laryngoscopy_note" AS VARCHAR), ''
            ) AS col_text
        FROM complications
        WHERE "_raw_laryngoscopy_note" IS NOT NULL
    ),
    all_text AS (
        SELECT * FROM notes_flat
        UNION ALL
        SELECT * FROM comp_flat
    ),

    -- RAI dose / number of treatments
    rai_events AS (
        SELECT
            research_id,
            'treatment' AS event_type,
            'rai_dose' AS event_subtype,
            TRY_CAST(regexp_extract(col_text,
                '(?i)(\\d+\\.?\\d*)\\s*(?:mCi|millicurie)', 1) AS DOUBLE
            ) AS event_value,
            'mCi' AS event_unit,
            NULL AS event_date,
            regexp_extract(col_text,
                '(?i)(.{0,60}\\d+\\.?\\d*\\s*(?:mCi|millicurie).{0,40})', 1
            ) AS event_text,
            'v5_rai_' || col_name AS source_column
        FROM all_text
        WHERE regexp_matches(col_text,
            '(?i)\\d+\\.?\\d*\\s*(?:mCi|millicurie)')
    ),

    -- Exact recurrence wording
    recurrence_events AS (
        SELECT
            research_id,
            'treatment' AS event_type,
            'recurrence' AS event_subtype,
            NULL AS event_value,
            NULL AS event_unit,
            NULL AS event_date,
            regexp_extract(col_text,
                '(?i)(.{0,40}(?:recurren\\w+|persistent\\s+(?:disease|structural)|'
                || 'structural\\s+(?:disease|recurrence)).{0,40})', 1
            ) AS event_text,
            'v5_recurrence_' || col_name AS source_column
        FROM all_text
        WHERE regexp_matches(col_text,
            '(?i)(?:recurren\\w+|persistent\\s+(?:disease|structural)|'
            || 'structural\\s+(?:disease|recurrence))')
    ),

    -- Additional medications (calcitriol, calcium carbonate, etc.)
    med_events AS (
        SELECT
            research_id,
            'medication' AS event_type,
            CASE
                WHEN regexp_matches(col_text, '(?i)calcitriol') THEN 'calcitriol'
                WHEN regexp_matches(col_text, '(?i)calcium\\s+carbonate') THEN 'calcium_carbonate'
                WHEN regexp_matches(col_text, '(?i)ergocalciferol|vitamin\\s+D2') THEN 'ergocalciferol'
                WHEN regexp_matches(col_text, '(?i)cholecalciferol|vitamin\\s+D3') THEN 'cholecalciferol'
                WHEN regexp_matches(col_text, '(?i)tums') THEN 'calcium_supplement'
                ELSE 'other_med'
            END AS event_subtype,
            TRY_CAST(regexp_extract(col_text,
                '(?i)(?:calcitriol|calcium|ergocalciferol|cholecalciferol)\\s+'
                || '(\\d+\\.?\\d*)\\s*(?:mcg|mg|iu|units)', 1) AS DOUBLE
            ) AS event_value,
            regexp_extract(col_text,
                '(?i)(?:calcitriol|calcium|ergocalciferol|cholecalciferol)\\s+'
                || '\\d+\\.?\\d*\\s*(mcg|mg|iu|units)', 1
            ) AS event_unit,
            NULL AS event_date,
            regexp_extract(col_text,
                '(?i)(.{0,30}(?:calcitriol|calcium\\s+carbonate|ergocalciferol|'
                || 'cholecalciferol|vitamin\\s+D[23]?|tums).{0,40})', 1
            ) AS event_text,
            'v5_med_' || col_name AS source_column
        FROM all_text
        WHERE regexp_matches(col_text,
            '(?i)(?:calcitriol|calcium\\s+carbonate|ergocalciferol|'
            || 'cholecalciferol|vitamin\\s+D[23]?|tums)')
    ),

    -- PMH flags not yet captured
    pmh_events AS (
        SELECT
            research_id,
            'comorbidity' AS event_type,
            CASE
                WHEN regexp_matches(col_text, '(?i)\\b(?:chronic\\s+kidney|CKD|renal\\s+insufficiency)\\b')
                    THEN 'ckd'
                WHEN regexp_matches(col_text, '(?i)\\bosteoporosis\\b')
                    THEN 'osteoporosis'
                WHEN regexp_matches(col_text, '(?i)\\b(?:atrial\\s+fib|AFib|a[\\s-]*fib)\\b')
                    THEN 'atrial_fibrillation'
                WHEN regexp_matches(col_text, '(?i)\\b(?:coronary\\s+artery|CAD|MI|myocardial)\\b')
                    THEN 'cad'
                WHEN regexp_matches(col_text, '(?i)\\b(?:heart\\s+failure|CHF|HFrEF|HFpEF)\\b')
                    THEN 'chf'
                WHEN regexp_matches(col_text, '(?i)\\bstroke\\b')
                    THEN 'stroke'
                WHEN regexp_matches(col_text, '(?i)\\b(?:depression|MDD)\\b')
                    THEN 'depression'
                WHEN regexp_matches(col_text, '(?i)\\basthma\\b')
                    THEN 'asthma'
                WHEN regexp_matches(col_text, '(?i)\\b(?:COPD|chronic\\s+obstructive)\\b')
                    THEN 'copd'
                ELSE 'other_pmh'
            END AS event_subtype,
            NULL AS event_value,
            NULL AS event_unit,
            NULL AS event_date,
            SUBSTRING(col_text, 1, 200) AS event_text,
            'v5_pmh_' || col_name AS source_column
        FROM all_text
        WHERE regexp_matches(col_text,
            '(?i)(?:CKD|osteoporosis|atrial\\s+fib|AFib|coronary\\s+artery|CAD|'
            || 'heart\\s+failure|CHF|stroke|depression|MDD|asthma|COPD|'
            || 'chronic\\s+kidney|chronic\\s+obstructive)')
    ),

    combined AS (
        SELECT * FROM rai_events
        UNION ALL
        SELECT * FROM recurrence_events
        UNION ALL
        SELECT * FROM med_events
        UNION ALL
        SELECT * FROM pmh_events
    ),
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, event_type, event_subtype,
                             COALESCE(CAST(event_value AS VARCHAR), '')
                ORDER BY LENGTH(COALESCE(event_text, '')) DESC
            ) AS rn
        FROM combined
    )
    SELECT
        research_id,
        event_type,
        event_subtype,
        event_value,
        event_unit,
        event_date,
        event_text,
        source_column
    FROM deduped
    WHERE rn = 1
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 6 — advanced_features_v3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADVANCED_V3_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE advanced_features_v3 AS
    SELECT
        mc.research_id,
        mc.age_at_surgery,
        mc.sex,
        mc.surgery_date,

        -- Pathology
        tp.histology_1_type,
        tp.variant_standardized,
        tp.surgery_type_normalized,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
        TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
        TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
        TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
        tp.tumor_1_extrathyroidal_ext,
        tp.tumor_1_gross_ete,
        tp.braf_mutation_mentioned,
        tp.ras_mutation_mentioned,
        tp.ret_mutation_mentioned,
        tp.tert_mutation_mentioned,

        -- Complications
        comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS rln_injury,
        comp.vocal_cord_status,
        comp.seroma,
        comp.hematoma,
        comp.hypocalcemia,
        comp.hypoparathyroidism,

        -- Operative
        od.ebl,
        od.skin_skin_time_min AS operative_time_min,

        -- Timeline (multi-surgery)
        mt.total_surgeries,
        mt.surgery_number AS primary_surgery_number,

        -- Relative-day lab anchoring (latest values relative to nearest surgery)
        latest_tg.days_since_nearest_surgery AS latest_tg_days_from_surgery,
        latest_tg.event_value AS latest_tg_value,
        latest_tsh.days_since_nearest_surgery AS latest_tsh_days_from_surgery,
        latest_tsh.event_value AS latest_tsh_value,

        -- QA flags
        COALESCE(qi.n_issues, 0) AS qa_issue_count,

        -- Data availability
        CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
        CASE WHEN od.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_operative_details,
        mc.has_tumor_pathology,
        mc.has_benign_pathology,
        mc.has_thyroglobulin_labs,
        mc.has_nuclear_med

    FROM master_cohort mc
    LEFT JOIN tumor_pathology tp
        ON mc.research_id = tp.research_id
    LEFT JOIN complications comp
        ON mc.research_id = CAST(comp.research_id AS VARCHAR)
    LEFT JOIN operative_details od
        ON mc.research_id = CAST(od.research_id AS VARCHAR)
    LEFT JOIN (
        SELECT research_id, surgery_number, total_surgeries
        FROM master_timeline WHERE surgery_number = 1
    ) mt ON CAST(mc.research_id AS VARCHAR) = CAST(mt.research_id AS VARCHAR)
    LEFT JOIN (
        SELECT research_id, event_value, days_since_nearest_surgery,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY followup_date DESC NULLS LAST
            ) AS rn
        FROM extracted_clinical_events_v4
        WHERE event_subtype = 'thyroglobulin' AND event_value IS NOT NULL
    ) latest_tg ON mc.research_id = CAST(latest_tg.research_id AS VARCHAR)
                AND latest_tg.rn = 1
    LEFT JOIN (
        SELECT research_id, event_value, days_since_nearest_surgery,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY followup_date DESC NULLS LAST
            ) AS rn
        FROM extracted_clinical_events_v4
        WHERE event_subtype = 'tsh' AND event_value IS NOT NULL
    ) latest_tsh ON mc.research_id = CAST(latest_tsh.research_id AS VARCHAR)
                 AND latest_tsh.rn = 1
    LEFT JOIN (
        SELECT research_id, COUNT(*) AS n_issues
        FROM qa_issues
        GROUP BY research_id
    ) qi ON CAST(mc.research_id AS VARCHAR) = CAST(qi.research_id AS VARCHAR)
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_connection():
    import duckdb
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN not set. Export it before running this script."
        )
    return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")


def _table_exists(con, name: str) -> bool:
    n = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{name}'"
    ).fetchone()[0]
    return n > 0


def _safe_count(con, name: str) -> int | None:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        return None


def _safe_distinct(con, name: str, col: str = "research_id") -> int | None:
    try:
        return con.execute(
            f"SELECT COUNT(DISTINCT CAST({col} AS VARCHAR)) FROM {name}"
        ).fetchone()[0]
    except Exception:
        return None


def _timed_execute(con, sql: str, label: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql)
    elapsed = time.perf_counter() - t0
    log.info(f"  {label}: {elapsed:.2f}s")
    return elapsed


def _fmt_size(n_bytes: int | None) -> str:
    if n_bytes is None:
        return "N/A"
    if n_bytes > 1_048_576:
        return f"{n_bytes / 1_048_576:.1f} MB"
    if n_bytes > 1024:
        return f"{n_bytes / 1024:.1f} KB"
    return f"{n_bytes} B"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 1: Connect & Compute Tier
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase1_connect(con) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 1: Connection & Compute Tier")
    log.info("=" * 72)
    report: list[str] = []

    db_name = con.execute("SELECT current_database()").fetchone()[0]
    version = con.execute("SELECT version()").fetchone()[0]
    log.info(f"  Database: {db_name}")
    log.info(f"  DuckDB version: {version}")
    report.append(f"- Database: `{db_name}`")
    report.append(f"- DuckDB version: {version}")

    tier = "Unknown (check MotherDuck UI)"
    for probe_sql in [
        "SELECT current_setting('motherduck_attached_database_type')",
        "CALL pragma_database_list()",
    ]:
        try:
            result = con.execute(probe_sql).fetchone()
            if result:
                tier = str(result)
                break
        except Exception:
            continue
    log.info(f"  Compute tier probe: {tier}")
    report.append(f"- Compute tier: {tier}")

    log.info("")
    log.info("  Compute advisory:")
    log.info("    Business trial active — using large compute for heavy QA.")
    log.info("    If Jumbo/Mega available, switch via MotherDuck UI for faster joins.")
    report.append("- Advisory: Business trial active — large compute enabled")

    try:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()
        log.info(f"  Tables in database: {len(tables)}")
        report.append(f"- Tables: {len(tables)}")
    except Exception:
        pass

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 2: Master Timeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase2_timeline(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 2: Build Master Surgery Timeline")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        log.info("  [DRY RUN] Would create master_timeline")
        report.append("- DRY RUN: skipped master_timeline creation")
        return report

    # Check that path_synoptics has surgery_date column
    has_ps_date = False
    try:
        cols = [r[0] for r in con.execute("DESCRIBE path_synoptics").fetchall()]
        has_ps_date = "surgery_date" in cols
    except Exception:
        pass

    sql = MASTER_TIMELINE_SQL
    if not has_ps_date:
        log.info("  path_synoptics lacks surgery_date — excluding from timeline sources")
        sql = sql.replace(
            "UNION ALL\n\n        -- Source 3: path_synoptics",
            "-- (path_synoptics excluded: no surgery_date column)\n        -- Source 3: path_synoptics (disabled)\n        -- UNION ALL\n\n        -- Source 3: path_synoptics",
        )
        sql = sql.replace(
            "SELECT\n            CAST(research_id AS INT) AS research_id,\n"
            "            TRY_CAST(surgery_date AS DATE) AS surgery_date,\n"
            "            'path_synoptics' AS source\n"
            "        FROM path_synoptics\n"
            "        WHERE surgery_date IS NOT NULL\n"
            "          AND TRY_CAST(surgery_date AS DATE) IS NOT NULL",
            "-- (path_synoptics block removed)",
        )

    elapsed = _timed_execute(con, sql, "master_timeline creation")

    total = _safe_count(con, "master_timeline") or 0
    patients = _safe_distinct(con, "master_timeline") or 0
    multi = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM master_timeline "
        "WHERE total_surgeries > 1"
    ).fetchone()[0]

    log.info(f"  master_timeline: {total:,} rows, {patients:,} patients")
    log.info(f"  Multi-surgery patients: {multi:,}")
    report.append(f"- **master_timeline**: {total:,} rows, {patients:,} patients")
    report.append(f"- Multi-surgery patients: {multi:,}")
    report.append(f"- Build time: {elapsed:.1f}s")

    if multi > 0:
        dist = con.execute(
            "SELECT total_surgeries, COUNT(DISTINCT research_id) AS n "
            "FROM master_timeline GROUP BY 1 ORDER BY 1"
        ).fetchall()
        for nsurg, n in dist:
            log.info(f"    {nsurg} surgery(ies): {n:,} patients")

    PHASE_TIMES["phase2"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 3: Events V4 with relative days
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase3_events_v4(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 3: Build extracted_clinical_events_v4")
    log.info("=" * 72)
    report: list[str] = []

    v3_count = _safe_count(con, "extracted_clinical_events_v3") or 0
    log.info(f"  Source: extracted_clinical_events_v3 ({v3_count:,} events)")

    if dry_run:
        log.info("  [DRY RUN] Would create extracted_clinical_events_v4")
        report.append("- DRY RUN: skipped v4 creation")
        return report

    elapsed = _timed_execute(con, EVENTS_V4_SQL, "events_v4 creation")
    v4_count = _safe_count(con, "extracted_clinical_events_v4") or 0
    v4_patients = _safe_distinct(con, "extracted_clinical_events_v4") or 0
    anchored = con.execute(
        "SELECT COUNT(*) FROM extracted_clinical_events_v4 "
        "WHERE days_since_nearest_surgery IS NOT NULL"
    ).fetchone()[0]
    pct = (anchored / max(v4_count, 1)) * 100

    log.info(f"  v4: {v4_count:,} events, {v4_patients:,} patients")
    log.info(f"  Surgery-anchored events: {anchored:,} ({pct:.1f}%)")
    report.append(f"- **events_v4**: {v4_count:,} events, {v4_patients:,} patients")
    report.append(f"- Surgery-anchored: {anchored:,} ({pct:.1f}%)")
    report.append(f"- Build time: {elapsed:.1f}s")

    PHASE_TIMES["phase3"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 4: QA Consistency Checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase4_qa_checks(con, dry_run: bool) -> tuple[list[str], list[dict]]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 4: Domain-Specific QA Consistency Checks")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        log.info("  [DRY RUN] Would create qa_issues and run 5 checks")
        report.append("- DRY RUN: skipped QA checks")
        return report, []

    _timed_execute(con, QA_ISSUES_SETUP, "qa_issues table setup")

    check_results: list[dict] = []
    for check_id, severity, description, sql in QA_CHECKS:
        log.info(f"\n  Check: {check_id}")
        log.info(f"    {description}")
        try:
            ct0 = time.perf_counter()
            con.execute(sql)
            ct_elapsed = time.perf_counter() - ct0
            n_issues = con.execute(
                f"SELECT COUNT(*) FROM qa_issues WHERE check_id = '{check_id}'"
            ).fetchone()[0]
            n_patients = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM qa_issues "
                f"WHERE check_id = '{check_id}'"
            ).fetchone()[0]
            status = "PASS" if n_issues == 0 else f"{n_issues} issues"
            log.info(f"    Result: {status} ({n_patients} patients, {ct_elapsed:.2f}s)")
            check_results.append({
                "check_id": check_id,
                "severity": severity,
                "description": description,
                "n_issues": n_issues,
                "n_patients": n_patients,
                "elapsed": ct_elapsed,
                "status": "PASS" if n_issues == 0 else "FAIL",
            })
        except Exception as exc:
            log.error(f"    Check FAILED: {exc}")
            check_results.append({
                "check_id": check_id,
                "severity": severity,
                "description": description,
                "n_issues": -1,
                "n_patients": 0,
                "elapsed": 0,
                "status": f"ERROR: {exc}",
            })

    total_issues = _safe_count(con, "qa_issues") or 0
    total_patients = _safe_distinct(con, "qa_issues") or 0
    log.info(f"\n  Total QA issues: {total_issues:,} across {total_patients:,} patients")

    report.append("")
    report.append("| Check | Severity | Issues | Patients | Time | Status |")
    report.append("|-------|----------|--------|----------|------|--------|")
    for cr in check_results:
        report.append(
            f"| `{cr['check_id']}` | {cr['severity']} | {cr['n_issues']} "
            f"| {cr['n_patients']} | {cr['elapsed']:.2f}s | {cr['status']} |"
        )
    report.append(f"\n- **Total QA issues: {total_issues:,}** ({total_patients:,} patients)")

    PHASE_TIMES["phase4"] = time.perf_counter() - t0
    return report, check_results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 5: Final Extraction Sweep
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase5_extraction(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 5: Final Extraction Sweep (RAI, recurrence, meds, PMH)")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        log.info("  [DRY RUN] Would create extracted_remaining_events")
        report.append("- DRY RUN: skipped extraction sweep")
        return report

    if not _table_exists(con, "raw_clinical_notes"):
        log.warning("  raw_clinical_notes not found — skipping extraction sweep")
        report.append("- Skipped: raw_clinical_notes not found")
        PHASE_TIMES["phase5"] = time.perf_counter() - t0
        return report

    try:
        elapsed = _timed_execute(con, REMAINING_EVENTS_SQL,
                                 "extracted_remaining_events creation")
        total = _safe_count(con, "extracted_remaining_events") or 0
        patients = _safe_distinct(con, "extracted_remaining_events") or 0

        type_dist = con.execute(
            "SELECT event_type, event_subtype, COUNT(*) AS n "
            "FROM extracted_remaining_events "
            "GROUP BY 1, 2 ORDER BY 3 DESC"
        ).fetchall()

        log.info(f"  extracted_remaining_events: {total:,} rows, {patients:,} patients")
        for etype, esub, n in type_dist:
            log.info(f"    {etype}/{esub}: {n:,}")

        report.append(f"- **extracted_remaining_events**: {total:,} rows, {patients:,} patients")
        report.append(f"- Build time: {elapsed:.1f}s")
        for etype, esub, n in type_dist:
            report.append(f"  - {etype}/{esub}: {n:,}")
    except Exception as exc:
        log.error(f"  Extraction sweep FAILED: {exc}")
        report.append(f"- Extraction sweep FAILED: {exc}")

    PHASE_TIMES["phase5"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 6: Outputs (Parquet, Report, advanced_features_v3)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXPORT_TABLES = [
    "extracted_clinical_events_v4",
    "qa_issues",
    "master_timeline",
    "extracted_remaining_events",
    "advanced_features_v3",
]


def phase6_outputs(con, dry_run: bool, check_results: list[dict]) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 6: Outputs — Parquet, Report, advanced_features_v3")
    log.info("=" * 72)
    report: list[str] = []

    # 6a: Build advanced_features_v3
    if not dry_run:
        log.info("  Building advanced_features_v3 ...")
        try:
            elapsed = _timed_execute(con, ADVANCED_V3_SQL, "advanced_features_v3")
            af_count = _safe_count(con, "advanced_features_v3") or 0
            af_patients = _safe_distinct(con, "advanced_features_v3") or 0
            log.info(f"  advanced_features_v3: {af_count:,} rows, {af_patients:,} patients")
            report.append(f"- **advanced_features_v3**: {af_count:,} rows ({elapsed:.1f}s)")
        except Exception as exc:
            log.error(f"  advanced_features_v3 FAILED: {exc}")
            report.append(f"- advanced_features_v3 FAILED: {exc}")
    else:
        report.append("- DRY RUN: skipped advanced_features_v3")

    # 6b: Parquet exports
    EXPORTS.mkdir(exist_ok=True)
    QA_DIR.mkdir(parents=True, exist_ok=True)

    for table_name in EXPORT_TABLES:
        if dry_run:
            log.info(f"  [DRY RUN] Would export {table_name}")
            report.append(f"- `{table_name}` → DRY RUN")
            continue

        if not _table_exists(con, table_name):
            log.warning(f"  {table_name} not found — skipping export")
            report.append(f"- `{table_name}` → skipped (not found)")
            continue

        out_path = EXPORTS / f"{table_name}.parquet"
        try:
            et0 = time.perf_counter()
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            df.to_parquet(str(out_path), index=False)
            elapsed = time.perf_counter() - et0
            size_mb = out_path.stat().st_size / 1_048_576
            log.info(f"  {out_path.name}: {len(df):,} rows, {size_mb:.2f} MB ({elapsed:.1f}s)")
            report.append(
                f"- `{table_name}` → `{out_path.name}` ({len(df):,} rows, {size_mb:.2f} MB)"
            )
        except Exception as exc:
            log.error(f"  Export failed for {table_name}: {exc}")
            report.append(f"- `{table_name}` → FAILED: {exc}")

    # Also export qa_issues to QA_DIR
    if not dry_run and _table_exists(con, "qa_issues"):
        qa_path = QA_DIR / "qa_issues.parquet"
        try:
            df = con.execute("SELECT * FROM qa_issues").fetchdf()
            df.to_parquet(str(qa_path), index=False)
            log.info(f"  {qa_path}: {len(df):,} rows")
        except Exception:
            pass

    # 6c: QA summary report
    _write_qa_report(check_results)
    report.append(f"- QA report → `{QA_DIR / 'qa_summary_report.md'}`")

    PHASE_TIMES["phase6"] = time.perf_counter() - t0
    return report


def _write_qa_report(check_results: list[dict]) -> None:
    QA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# QA Summary Report — THYROID_2026 Lakehouse\n",
        f"**Generated:** {timestamp}\n",
        "## Check Results\n",
        "| Check | Severity | Issues | Patients | Time | Status |",
        "|-------|----------|--------|----------|------|--------|",
    ]
    total_issues = 0
    total_pass = 0
    for cr in check_results:
        lines.append(
            f"| `{cr['check_id']}` | {cr['severity']} | {cr['n_issues']} "
            f"| {cr['n_patients']} | {cr['elapsed']:.2f}s | {cr['status']} |"
        )
        if cr["n_issues"] >= 0:
            total_issues += cr["n_issues"]
        if cr["status"] == "PASS":
            total_pass += 1

    n_checks = len(check_results)
    lines.append(f"\n**Summary:** {total_pass}/{n_checks} checks passed, "
                 f"{total_issues} total issues flagged\n")

    lines.append("## Phase Compute Times\n")
    lines.append("| Phase | Time |")
    lines.append("|-------|------|")
    for phase, elapsed in PHASE_TIMES.items():
        lines.append(f"| {phase} | {elapsed:.1f}s |")
    total_time = sum(PHASE_TIMES.values())
    lines.append(f"| **Total** | **{total_time:.1f}s** |")

    lines.append("\n## Outputs\n")
    lines.append("- `exports/extracted_clinical_events_v4.parquet`")
    lines.append("- `exports/qa_issues.parquet`")
    lines.append("- `exports/master_timeline.parquet`")
    lines.append("- `exports/extracted_remaining_events.parquet`")
    lines.append("- `exports/advanced_features_v3.parquet`")
    lines.append("- `studies/qa_crosscheck/qa_issues.parquet`")
    lines.append("- `studies/qa_crosscheck/qa_summary_report.md` (this file)")

    lines.append(f"\n---\n*Generated by `11_quality_assurance_crosscheck.py` "
                 f"at {timestamp}*\n")

    report_path = QA_DIR / "qa_summary_report.md"
    report_path.write_text("\n".join(lines))
    log.info(f"  QA report written to {report_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 7: Safety — EXPLAIN ANALYZE, trial summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase7_safety(con, dry_run: bool) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 7: Safety — EXPLAIN ANALYZE & Trial Summary")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        report.append("- DRY RUN: skipped EXPLAIN ANALYZE")
    else:
        log.info("  EXPLAIN ANALYZE: heaviest query (events_v4 + timeline join) ...")
        try:
            t0 = time.perf_counter()
            explain_result = con.execute(
                "EXPLAIN ANALYZE "
                "SELECT mt.surgery_number, COUNT(*) AS n_events, "
                "  AVG(e.days_since_nearest_surgery) AS avg_days "
                "FROM extracted_clinical_events_v4 e "
                "JOIN master_timeline mt "
                "  ON CAST(e.research_id AS INT) = mt.research_id "
                "  AND e.nearest_surgery_number = mt.surgery_number "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
            elapsed = time.perf_counter() - t0
            log.info(f"    Completed in {elapsed:.2f}s")
            for row in explain_result[:8]:
                log.info(f"    {row[0] if isinstance(row, tuple) else row}")
            report.append(f"- EXPLAIN ANALYZE (v4 × timeline join): {elapsed:.2f}s")
        except Exception as exc:
            log.warning(f"    EXPLAIN ANALYZE failed: {exc}")
            report.append(f"- EXPLAIN ANALYZE: skipped ({exc})")

    # Table sizes summary
    log.info("\n  Final table sizes:")
    report.append("")
    report.append("| Table | Rows | Patients | Est. Size |")
    report.append("|-------|------|----------|-----------|")

    for tbl in [
        "master_timeline", "extracted_clinical_events_v4",
        "qa_issues", "extracted_remaining_events", "advanced_features_v3",
    ]:
        row_count = _safe_count(con, tbl)
        patient_count = _safe_distinct(con, tbl)
        est_size = None
        try:
            result = con.execute(
                f"SELECT estimated_size FROM duckdb_tables() "
                f"WHERE table_name = '{tbl}'"
            ).fetchone()
            if result:
                est_size = result[0]
        except Exception:
            pass

        if row_count is not None:
            size_str = _fmt_size(est_size)
            log.info(f"    {tbl:45s} {row_count:>10,} rows  {size_str:>10s}")
            report.append(
                f"| `{tbl}` | {row_count:,} | {patient_count or 0:,} | {size_str} |"
            )

    # Trial utilization summary
    log.info("\n" + "-" * 72)
    total_time = sum(PHASE_TIMES.values())
    log.info(f"  Total compute time this run: {total_time:.1f}s")
    log.info("")
    log.info("  MotherDuck trial utilization summary:")
    log.info("    - All QA checks persisted as tables (survive downgrade)")
    log.info("    - extracted_clinical_events_v4: surgery-anchored relative days")
    log.info("    - master_timeline: multi-surgery patients properly tracked")
    log.info("    - advanced_features_v3: includes timeline + QA columns")
    log.info("    - Ready for dashboard connection or downgrade")
    log.info("-" * 72)

    report.append(f"\n**Total compute: {total_time:.1f}s**")
    report.append("\nMotherDuck trial utilization summary — "
                  "ready for dashboard connection or downgrade")

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Quality Assurance Cross-Check: validate, anchor dates "
        "to surgeries, extract remaining data, and export"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print SQL plans without executing CREATE/DROP statements",
    )
    args = parser.parse_args()

    log.info("=" * 72)
    log.info("  THYROID LAKEHOUSE — QA CROSS-CHECK & FINAL EXTRACTION")
    log.info("=" * 72)
    if args.dry_run:
        log.info("*** DRY RUN MODE — no tables will be created ***\n")

    try:
        con = _get_connection()
    except Exception as exc:
        log.error(f"Connection failed: {exc}")
        sys.exit(1)

    try:
        phase1_connect(con)
        phase2_timeline(con, args.dry_run)
        phase3_events_v4(con, args.dry_run)

        _info_qa, check_results = phase4_qa_checks(con, args.dry_run)

        phase5_extraction(con, args.dry_run)
        phase6_outputs(con, args.dry_run, check_results)
        phase7_safety(con, args.dry_run)
    finally:
        con.close()

    log.info("\n" + "=" * 72)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 72)
    log.info(f"  QA report: {QA_DIR / 'qa_summary_report.md'}")
    log.info(f"  Exports:   {EXPORTS}")
    log.info("")
    log.info("  MotherDuck trial utilization summary — "
             "ready for dashboard connection or downgrade")
    log.info("=" * 72)


if __name__ == "__main__":
    main()
