#!/usr/bin/env python3
"""
10_maximize_motherduck_trial.py — Maximize MotherDuck Business Trial Compute

Before Business tier expires (~18 days left), create richly derived tables
that remain queryable on free tier Pulse compute.

Phases:
  1. Connect to MotherDuck, print database + compute tier info
  2. Improved date/temporal extraction → extracted_clinical_events_v3
     (target: 10x more follow-up dates vs v2's ~140)
  3. Create 7 pre-computed analytics tables (DuckDB lacks MATERIALIZED VIEW;
     CREATE TABLE AS serves the same purpose and persists on free tier)
  4. Performance & size validation
  5. Export key tables to local Parquet
  6. Write trial_utilization_log.md

Usage:
  python scripts/10_maximize_motherduck_trial.py
  python scripts/10_maximize_motherduck_trial.py --dry-run
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
log = logging.getLogger("trial_maximize")

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports"
LOG_FILE = ROOT / "trial_utilization_log.md"

MD_DATABASE = "thyroid_research_2026"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 2 — Date Extraction V3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Step 1: Extract ALL dates from raw_clinical_notes into staging table
EXTRACTION_STEP1_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE _extracted_dates_raw AS
    WITH notes_flat AS (
        SELECT
            CAST("Research ID number" AS VARCHAR) AS research_id,
            col_name,
            CAST(col_text AS VARCHAR) AS col_text
        FROM raw_clinical_notes
        UNPIVOT (col_text FOR col_name IN (
            COLUMNS(* EXCLUDE "Research ID number")
        ))
        WHERE col_text IS NOT NULL
          AND LENGTH(CAST(col_text AS VARCHAR)) > 20
    ),
    date_matches AS (
        SELECT
            research_id,
            col_name,
            col_text,
            UNNEST(regexp_extract_all(
                col_text, '\\d{1,2}/\\d{1,2}/\\d{2,4}'
            )) AS raw_date
        FROM notes_flat
    ),
    parsed AS (
        SELECT
            research_id,
            col_name,
            raw_date,
            col_text,
            COALESCE(
                try_strptime(raw_date, '%m/%d/%Y'),
                try_strptime(raw_date, '%m/%d/%y')
            ) AS parsed_dt
        FROM date_matches
    ),
    with_context AS (
        SELECT
            research_id,
            col_name AS source_column,
            raw_date,
            CAST(parsed_dt AS DATE) AS extracted_date,
            CASE
                WHEN regexp_matches(col_text,
                    '(?i)(?:follow[\\s\\-]*up|f/u|seen\\s+on|visit\\s+on|'
                    'return\\s+|next\\s+|appoint|RTC|clinic\\s+visit|'
                    'schedule|post[\\s\\-]*op).*?'
                    || replace(raw_date, '/', '\\/'))
                THEN 0.90
                WHEN regexp_matches(col_text,
                    '(?i)(?:date|on|at|seen).*?'
                    || replace(raw_date, '/', '\\/'))
                THEN 0.75
                ELSE 0.60
            END AS confidence
        FROM parsed
        WHERE parsed_dt IS NOT NULL
          AND CAST(parsed_dt AS DATE) >= '1990-01-01'
          AND CAST(parsed_dt AS DATE) <= CURRENT_DATE + INTERVAL '365' DAY
    )
    SELECT DISTINCT
        research_id,
        source_column,
        raw_date,
        extracted_date,
        MAX(confidence) AS confidence
    FROM with_context
    GROUP BY research_id, source_column, raw_date, extracted_date
""")

# Step 2: Also extract dates from laryngoscopy notes in complications
EXTRACTION_STEP1B_SQL = textwrap.dedent("""\
    INSERT INTO _extracted_dates_raw
    WITH laryng_dates AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            '_raw_laryngoscopy_note' AS source_column,
            UNNEST(regexp_extract_all(
                CAST("_raw_laryngoscopy_note" AS VARCHAR),
                '\\d{1,2}/\\d{1,2}/\\d{2,4}'
            )) AS raw_date
        FROM complications
        WHERE "_raw_laryngoscopy_note" IS NOT NULL
          AND LENGTH(CAST("_raw_laryngoscopy_note" AS VARCHAR)) > 10
    ),
    parsed AS (
        SELECT
            research_id,
            source_column,
            raw_date,
            COALESCE(
                try_strptime(raw_date, '%m/%d/%Y'),
                try_strptime(raw_date, '%m/%d/%y')
            ) AS parsed_dt
        FROM laryng_dates
    )
    SELECT DISTINCT
        research_id,
        source_column,
        raw_date,
        CAST(parsed_dt AS DATE) AS extracted_date,
        0.85 AS confidence
    FROM parsed
    WHERE parsed_dt IS NOT NULL
      AND CAST(parsed_dt AS DATE) >= '1990-01-01'
      AND CAST(parsed_dt AS DATE) <= CURRENT_DATE + INTERVAL '365' DAY
""")

# Step 3: Build V3 = V2 (or V1) base + new dates + laryngoscopy struct dates + relative days
EXTRACTION_STEP2_SQL_TEMPLATE = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_clinical_events_v3 AS
    WITH v_base AS (
        SELECT
            research_id,
            event_type,
            event_subtype,
            event_value,
            event_unit,
            event_date,
            event_text,
            source_column,
            TRY_CAST(event_date AS DATE) AS followup_date,
            NULL::INT AS event_relative_days,
            CASE event_type
                WHEN 'lab' THEN 0.95
                WHEN 'medication' THEN 0.90
                WHEN 'treatment' THEN 0.92
                WHEN 'follow_up' THEN 0.88
                WHEN 'comorbidity' THEN 0.80
                ELSE 0.70
            END::FLOAT AS confidence_score
        FROM {source_table}
    ),
    new_dates AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            'follow_up' AS event_type,
            'extracted_date' AS event_subtype,
            NULL::DOUBLE AS event_value,
            NULL::VARCHAR AS event_unit,
            strftime(extracted_date, '%Y-%m-%d') AS event_date,
            raw_date AS event_text,
            'v3_' || source_column AS source_column,
            extracted_date AS followup_date,
            NULL::INT AS event_relative_days,
            confidence::FLOAT AS confidence_score
        FROM _extracted_dates_raw
    ),
    new_dates_dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, event_date
                ORDER BY confidence_score DESC
            ) AS rn
        FROM new_dates
    ),
    new_dates_filtered AS (
        SELECT
            research_id, event_type, event_subtype, event_value,
            event_unit, event_date, event_text, source_column,
            followup_date, event_relative_days, confidence_score
        FROM new_dates_dedup
        WHERE rn = 1
          AND NOT EXISTS (
              SELECT 1 FROM v_base vb
              WHERE vb.research_id = new_dates_dedup.research_id
                AND vb.event_date = new_dates_dedup.event_date
          )
    ),
    laryngoscopy_struct AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            'follow_up' AS event_type,
            'laryngoscopy_date' AS event_subtype,
            NULL::DOUBLE AS event_value,
            NULL::VARCHAR AS event_unit,
            strftime(TRY_CAST(laryngoscopy_date AS DATE), '%Y-%m-%d') AS event_date,
            'Laryngoscopy: ' || COALESCE(CAST(vocal_cord_status AS VARCHAR), '')
                AS event_text,
            'complications.laryngoscopy_date' AS source_column,
            TRY_CAST(laryngoscopy_date AS DATE) AS followup_date,
            NULL::INT AS event_relative_days,
            0.95::FLOAT AS confidence_score
        FROM complications
        WHERE TRY_CAST(laryngoscopy_date AS DATE) IS NOT NULL
    ),
    combined AS (
        SELECT * FROM v_base
        UNION ALL
        SELECT * FROM new_dates_filtered
        UNION ALL
        SELECT * FROM laryngoscopy_struct ls
        WHERE NOT EXISTS (
            SELECT 1 FROM v_base vb
            WHERE vb.research_id = ls.research_id
              AND vb.event_date = ls.event_date
        )
    )
    SELECT
        c.research_id,
        c.event_type,
        c.event_subtype,
        c.event_value,
        c.event_unit,
        c.event_date,
        c.event_text,
        c.source_column,
        c.followup_date,
        CASE
            WHEN c.followup_date IS NOT NULL
                 AND TRY_CAST(mc.surgery_date AS DATE) IS NOT NULL
            THEN DATEDIFF('day',
                TRY_CAST(mc.surgery_date AS DATE), c.followup_date)
            ELSE c.event_relative_days
        END AS event_relative_days,
        c.confidence_score
    FROM combined c
    LEFT JOIN master_cohort mc
        ON CAST(c.research_id AS VARCHAR) = mc.research_id
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Phase 3 — Materialized Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PATIENT_SUMMARY_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE patient_level_summary_mv AS
    WITH latest_labs AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            event_subtype,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, event_subtype
                ORDER BY TRY_CAST(event_date AS DATE) DESC NULLS LAST
            ) AS rn
        FROM extracted_clinical_events_v3
        WHERE event_type = 'lab' AND event_value IS NOT NULL
    ),
    pivoted_labs AS (
        SELECT
            research_id,
            MAX(CASE WHEN event_subtype = 'tsh' THEN event_value END)
                AS latest_tsh,
            MAX(CASE WHEN event_subtype = 'thyroglobulin' THEN event_value END)
                AS latest_tg,
            MAX(CASE WHEN event_subtype = 'calcium' THEN event_value END)
                AS latest_calcium,
            MAX(CASE WHEN event_subtype = 'pth' THEN event_value END)
                AS latest_pth
        FROM latest_labs
        WHERE rn = 1
        GROUP BY research_id
    ),
    levo AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            event_value AS levothyroxine_dose,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY TRY_CAST(event_date AS DATE) DESC NULLS LAST
            ) AS rn
        FROM extracted_clinical_events_v3
        WHERE event_subtype = 'levothyroxine' AND event_value IS NOT NULL
    ),
    comorbidities AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            COUNT(DISTINCT event_subtype) AS n_comorbidities
        FROM extracted_clinical_events_v3
        WHERE event_type = 'comorbidity'
        GROUP BY research_id
    ),
    recurrence AS (
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            TRUE AS recurrence_flag
        FROM extracted_clinical_events_v3
        WHERE event_subtype = 'recurrence'
    ),
    last_fu AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MAX(followup_date) AS last_followup_date
        FROM extracted_clinical_events_v3
        WHERE followup_date IS NOT NULL
        GROUP BY research_id
    )
    SELECT
        mc.research_id,
        mc.age_at_surgery,
        mc.sex,
        mc.surgery_date,
        pl.latest_tsh,
        pl.latest_tg,
        pl.latest_calcium,
        pl.latest_pth,
        lv.levothyroxine_dose,
        COALESCE(cm.n_comorbidities, 0) AS n_comorbidities,
        COALESCE(r.recurrence_flag, FALSE) AS recurrence_flag,
        lfu.last_followup_date,
        tp.histology_1_type,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
        TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm
    FROM master_cohort mc
    LEFT JOIN pivoted_labs pl ON mc.research_id = pl.research_id
    LEFT JOIN (SELECT * FROM levo WHERE rn = 1) lv
        ON mc.research_id = lv.research_id
    LEFT JOIN comorbidities cm ON mc.research_id = cm.research_id
    LEFT JOIN recurrence r ON mc.research_id = r.research_id
    LEFT JOIN last_fu lfu ON mc.research_id = lfu.research_id
    LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
""")

TG_TREND_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE tg_trend_long_mv AS
    WITH parsed AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            TRY_CAST(specimen_collect_dt AS DATE) AS lab_date,
            TRY_CAST(
                regexp_extract(CAST(result AS VARCHAR),
                    '([0-9]*\\.?[0-9]+)', 1)
                AS DOUBLE
            ) AS tg_value,
            lab_index
        FROM thyroglobulin_labs
        WHERE result IS NOT NULL
    ),
    with_baseline AS (
        SELECT
            *,
            FIRST_VALUE(tg_value) OVER (
                PARTITION BY research_id
                ORDER BY COALESCE(lab_date, '1900-01-01'::DATE), lab_index
            ) AS baseline_tg,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY COALESCE(lab_date, '1900-01-01'::DATE), lab_index
            ) AS measurement_num
        FROM parsed
        WHERE tg_value IS NOT NULL
    )
    SELECT
        research_id,
        lab_date,
        tg_value AS thyroglobulin_value,
        baseline_tg,
        tg_value - baseline_tg AS change_from_baseline,
        CASE WHEN baseline_tg > 0
             THEN ROUND((tg_value - baseline_tg) / baseline_tg * 100, 1)
             ELSE NULL
        END AS pct_change_from_baseline,
        measurement_num
    FROM with_baseline
    ORDER BY research_id, lab_date, lab_index
""")

RECURRENCE_RISK_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE recurrence_risk_features_mv AS
    WITH tg_summary AS (
        SELECT
            research_id,
            MIN(thyroglobulin_value) AS tg_min,
            MAX(thyroglobulin_value) AS tg_max,
            AVG(thyroglobulin_value) AS tg_mean,
            arg_min(thyroglobulin_value, measurement_num) AS tg_first,
            arg_max(thyroglobulin_value, measurement_num) AS tg_last,
            COUNT(*) AS tg_measurement_count,
            CASE
                WHEN COUNT(*) >= 2
                     AND MIN(lab_date) IS NOT NULL
                     AND MAX(lab_date) IS NOT NULL
                     AND MIN(lab_date) != MAX(lab_date)
                THEN (LN(GREATEST(
                        arg_max(thyroglobulin_value, measurement_num), 0.01))
                      - LN(GREATEST(
                        arg_min(thyroglobulin_value, measurement_num), 0.01)))
                     / GREATEST(DATEDIFF('day',
                        MIN(lab_date), MAX(lab_date)), 1) * 365.25
                ELSE NULL
            END AS tg_annual_log_slope
        FROM tg_trend_long_mv
        GROUP BY research_id
    ),
    recurrence_events AS (
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            TRUE AS has_recurrence_event,
            MIN(TRY_CAST(event_date AS DATE)) AS first_recurrence_date
        FROM extracted_clinical_events_v3
        WHERE event_subtype = 'recurrence'
        GROUP BY research_id
    )
    SELECT
        mc.research_id,
        mc.surgery_date,
        tp.histology_1_type,
        tp.histology_1_t_stage_ajcc8 AS pt_stage,
        tp.histology_1_n_stage_ajcc8 AS pn_stage,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage,
        tp.tumor_1_extrathyroidal_ext AS ete,
        tp.tumor_1_gross_ete AS gross_ete,
        TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS tumor_size_cm,
        TRY_CAST(tp.histology_1_ln_positive AS INT) AS ln_positive,
        TRY_CAST(tp.histology_1_ln_examined AS INT) AS ln_examined,
        TRY_CAST(tp.histology_1_ln_ratio AS DOUBLE) AS ln_ratio,
        tp.braf_mutation_mentioned AS braf_positive,
        tp.ras_mutation_mentioned AS ras_positive,
        tp.ret_mutation_mentioned AS ret_positive,
        tp.tert_mutation_mentioned AS tert_positive,
        ts.tg_first,
        ts.tg_last,
        ts.tg_max,
        ts.tg_mean,
        ts.tg_measurement_count,
        ts.tg_annual_log_slope,
        COALESCE(re.has_recurrence_event, FALSE) AS recurrence_flag,
        re.first_recurrence_date,
        rrc.recurrence_risk_band
    FROM master_cohort mc
    LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
    LEFT JOIN tg_summary ts ON mc.research_id = ts.research_id
    LEFT JOIN recurrence_events re ON mc.research_id = re.research_id
    LEFT JOIN recurrence_risk_cohort rrc ON mc.research_id = rrc.research_id
    WHERE tp.research_id IS NOT NULL
""")

SERIAL_NODULE_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE serial_nodule_tracking_mv AS
    WITH us_timeline AS (
        SELECT
            research_id,
            TRY_CAST(ultrasound_date AS DATE) AS exam_date,
            TRY_CAST(number_of_nodules AS INT) AS num_nodules,
            TRY_CAST(right_lobe_volume_ml AS DOUBLE) AS right_lobe_vol,
            TRY_CAST(left_lobe_volume_ml AS DOUBLE) AS left_lobe_vol,
            TRY_CAST(total_thyroid_volume_ml AS DOUBLE) AS total_vol,
            lymph_node_assessment,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY TRY_CAST(ultrasound_date AS DATE)
            ) AS exam_seq
        FROM ultrasound_reports
        WHERE ultrasound_date IS NOT NULL
    )
    SELECT
        *,
        LAG(num_nodules) OVER w AS prev_num_nodules,
        num_nodules - LAG(num_nodules) OVER w AS nodule_count_change,
        LAG(total_vol) OVER w AS prev_total_vol,
        total_vol - LAG(total_vol) OVER w AS vol_change_ml,
        DATEDIFF('day',
            LAG(exam_date) OVER w, exam_date) AS days_between_exams
    FROM us_timeline
    WINDOW w AS (PARTITION BY research_id ORDER BY exam_date)
    ORDER BY research_id, exam_date
""")

SURVIVAL_COHORT_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE survival_cohort_ready_mv AS
    WITH surgery_dates AS (
        SELECT
            research_id,
            TRY_CAST(surgery_date AS DATE) AS surgery_dt
        FROM master_cohort
        WHERE surgery_date IS NOT NULL
    ),
    recurrence_dates AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MIN(TRY_CAST(event_date AS DATE)) AS recurrence_dt
        FROM extracted_clinical_events_v3
        WHERE event_subtype = 'recurrence' AND event_date IS NOT NULL
        GROUP BY research_id
    ),
    last_contact AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MAX(followup_date) AS last_contact_dt
        FROM extracted_clinical_events_v3
        WHERE followup_date IS NOT NULL
        GROUP BY research_id
    )
    SELECT
        sd.research_id,
        sd.surgery_dt AS surgery_date,
        rd.recurrence_dt AS recurrence_date,
        COALESCE(rd.recurrence_dt, lc.last_contact_dt, CURRENT_DATE)
            AS censor_date,
        CASE WHEN rd.recurrence_dt IS NOT NULL THEN 1 ELSE 0 END
            AS event_occurred,
        DATEDIFF('day', sd.surgery_dt,
            COALESCE(rd.recurrence_dt, lc.last_contact_dt, CURRENT_DATE))
            AS time_to_event_days,
        tp.histology_1_type,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
        mc.age_at_surgery,
        mc.sex
    FROM surgery_dates sd
    JOIN master_cohort mc ON sd.research_id = mc.research_id
    LEFT JOIN tumor_pathology tp ON sd.research_id = tp.research_id
    LEFT JOIN recurrence_dates rd ON sd.research_id = rd.research_id
    LEFT JOIN last_contact lc ON sd.research_id = lc.research_id
    WHERE sd.surgery_dt IS NOT NULL
""")

MOLECULAR_PATH_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE molecular_path_risk_mv AS
    WITH mol AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MAX(CASE WHEN CAST(mutation AS VARCHAR) ILIKE '%BRAF%'
                     THEN CAST(result AS VARCHAR) END) AS braf_result,
            MAX(CASE WHEN CAST(mutation AS VARCHAR) ILIKE '%RAS%'
                      OR CAST(mutation AS VARCHAR) ILIKE '%NRAS%'
                      OR CAST(mutation AS VARCHAR) ILIKE '%HRAS%'
                     THEN CAST(result AS VARCHAR) END) AS ras_result,
            MAX(CASE WHEN CAST(mutation AS VARCHAR) ILIKE '%RET%'
                     THEN CAST(result AS VARCHAR) END) AS ret_result,
            MAX(CASE WHEN CAST(mutation AS VARCHAR) ILIKE '%TERT%'
                     THEN CAST(result AS VARCHAR) END) AS tert_result,
            STRING_AGG(DISTINCT CAST(mutation AS VARCHAR), '; ')
                AS all_mutations_detected
        FROM molecular_testing
        WHERE result IS NOT NULL
        GROUP BY research_id
    ),
    bethesda AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            MAX(TRY_CAST(bethesda AS INT)) AS max_bethesda
        FROM fna_history
        GROUP BY research_id
    ),
    recurrence AS (
        SELECT DISTINCT
            CAST(research_id AS VARCHAR) AS research_id,
            TRUE AS recurrence_flag
        FROM extracted_clinical_events_v3
        WHERE event_subtype = 'recurrence'
    )
    SELECT
        tp.research_id,
        tp.histology_1_type,
        tp.variant_standardized,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage,
        tp.braf_mutation_mentioned,
        tp.ras_mutation_mentioned,
        tp.ret_mutation_mentioned,
        tp.tert_mutation_mentioned,
        mol.braf_result,
        mol.ras_result,
        mol.ret_result,
        mol.tert_result,
        mol.all_mutations_detected,
        b.max_bethesda AS bethesda_max,
        TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS tumor_size_cm,
        TRY_CAST(tp.histology_1_ln_positive AS INT) AS ln_positive,
        tp.tumor_1_extrathyroidal_ext AS ete,
        COALESCE(r.recurrence_flag, FALSE) AS recurrence_flag
    FROM tumor_pathology tp
    LEFT JOIN mol ON tp.research_id = mol.research_id
    LEFT JOIN bethesda b ON tp.research_id = b.research_id
    LEFT JOIN recurrence r ON tp.research_id = r.research_id
""")

COMPLICATION_SEVERITY_MV_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE complication_severity_mv AS
    SELECT
        CAST(comp.research_id AS VARCHAR) AS research_id,
        mc.age_at_surgery,
        mc.sex,
        mc.surgery_date,
        comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy
            AS rln_injury,
        comp.vocal_cord_status,
        comp.affected_side,
        comp.seroma,
        comp.hematoma,
        comp.hypocalcemia,
        comp.hypoparathyroidism,
        comp.laryngoscopy_date,
        od.ebl,
        od.skin_skin_time_min AS operative_time_min,
        od.bmi,
        tp.surgery_type_normalized,
        tp.histology_1_type,
        (CASE WHEN LOWER(COALESCE(
              CAST(comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy
                   AS VARCHAR), ''))
              NOT IN ('', 'no', 'none', 'n/a')
              THEN 3 ELSE 0 END
         + CASE WHEN LOWER(COALESCE(CAST(comp.hematoma AS VARCHAR), ''))
              NOT IN ('', 'no', 'none', 'n/a')
              THEN 2 ELSE 0 END
         + CASE WHEN LOWER(COALESCE(CAST(comp.seroma AS VARCHAR), ''))
              NOT IN ('', 'no', 'none', 'n/a')
              THEN 1 ELSE 0 END
         + CASE WHEN LOWER(COALESCE(CAST(comp.hypocalcemia AS VARCHAR), ''))
              NOT IN ('', 'no', 'none', 'n/a')
              THEN 1 ELSE 0 END
         + CASE WHEN LOWER(COALESCE(
              CAST(comp.hypoparathyroidism AS VARCHAR), ''))
              NOT IN ('', 'no', 'none', 'n/a')
              THEN 2 ELSE 0 END
        ) AS complication_severity_score
    FROM complications comp
    LEFT JOIN master_cohort mc
        ON mc.research_id = CAST(comp.research_id AS VARCHAR)
    LEFT JOIN operative_details od
        ON comp.research_id = od.research_id
    LEFT JOIN tumor_pathology tp
        ON mc.research_id = tp.research_id
""")

MATERIALIZED_TABLES = [
    ("patient_level_summary_mv", PATIENT_SUMMARY_MV_SQL,
     "One row per patient: latest labs, comorbidities, recurrence, follow-up"),
    ("tg_trend_long_mv", TG_TREND_MV_SQL,
     "Long-format thyroglobulin trajectory with baseline delta"),
    ("recurrence_risk_features_mv", RECURRENCE_RISK_MV_SQL,
     "Combined mutation + staging + Tg trend + recurrence features"),
    ("serial_nodule_tracking_mv", SERIAL_NODULE_MV_SQL,
     "Serial US: nodule count and volume change over time"),
    ("survival_cohort_ready_mv", SURVIVAL_COHORT_MV_SQL,
     "Time-to-event format for survival / recurrence analysis"),
    ("molecular_path_risk_mv", MOLECULAR_PATH_MV_SQL,
     "Molecular testing + pathology + Bethesda + recurrence"),
    ("complication_severity_mv", COMPLICATION_SEVERITY_MV_SQL,
     "Surgical complications with severity scoring"),
]

EXPORT_TABLES = [
    "extracted_clinical_events_v3",
    "patient_level_summary_mv",
    "survival_cohort_ready_mv",
    "recurrence_risk_features_mv",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_connection():
    import duckdb

    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN not set. Export it before running this script."
        )
    return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")


def _table_exists(con, table_name: str) -> bool:
    n = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{table_name}'"
    ).fetchone()[0]
    return n > 0


def _safe_count(con, table_name: str) -> int | None:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except Exception:
        return None


def _safe_distinct(con, table_name: str, col: str = "research_id") -> int | None:
    try:
        return con.execute(
            f"SELECT COUNT(DISTINCT CAST({col} AS VARCHAR)) FROM {table_name}"
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 1: Connection & Database Info
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase1_connect(con) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 1: Connection & Database Info")
    log.info("=" * 72)

    report: list[str] = []

    db_name = con.execute("SELECT current_database()").fetchone()[0]
    log.info(f"  Database: {db_name}")
    report.append(f"- Database: `{db_name}`")

    version = con.execute("SELECT version()").fetchone()[0]
    log.info(f"  DuckDB version: {version}")
    report.append(f"- DuckDB version: {version}")

    # Attempt to identify MotherDuck compute tier
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

    # Table inventory
    try:
        tables = con.execute(
            "SELECT table_name, estimated_size "
            "FROM duckdb_tables() "
            "ORDER BY estimated_size DESC"
        ).fetchall()
        log.info(f"  Tables in database: {len(tables)}")
        report.append(f"- Tables: {len(tables)}")
        for name, size in tables[:15]:
            log.info(f"    {name:40s}  {_fmt_size(size)}")
    except Exception:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()
        log.info(f"  Tables in database: {len(tables)}")
        report.append(f"- Tables: {len(tables)}")
        for (name,) in tables:
            log.info(f"    {name}")

    # View inventory
    try:
        views = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_type = 'VIEW'"
        ).fetchall()
        log.info(f"  Views in database: {len(views)}")
        report.append(f"- Views: {len(views)}")
    except Exception:
        pass

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 2: Date Extraction V3
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase2_extraction(con, dry_run: bool) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 2: Improved Date/Temporal Extraction → V3")
    log.info("=" * 72)

    report: list[str] = []

    # Baseline counts
    v1_total = _safe_count(con, "extracted_clinical_events") or 0
    v2_exists = _table_exists(con, "extracted_clinical_events_v2")
    source_table = "extracted_clinical_events_v2" if v2_exists else "extracted_clinical_events"
    source_total = _safe_count(con, source_table) or v1_total

    source_fu = 0
    try:
        source_fu = con.execute(
            f"SELECT COUNT(*) FROM {source_table} WHERE event_type = 'follow_up'"
        ).fetchone()[0]
    except Exception:
        pass

    log.info(f"  Source: {source_table} ({source_total:,} events, {source_fu:,} follow-up)")
    report.append(f"- Source table: `{source_table}` ({source_total:,} events)")
    report.append(f"- Baseline follow-up events: {source_fu:,}")

    if dry_run:
        log.info("  [DRY RUN] Would create _extracted_dates_raw + extracted_clinical_events_v3")
        report.append("- DRY RUN: skipped creation")
        return report

    # Step 1: Extract dates from raw_clinical_notes
    has_raw_notes = _table_exists(con, "raw_clinical_notes")
    has_complications = _table_exists(con, "complications")

    if has_raw_notes:
        try:
            log.info("  Step 1a: Extracting dates from raw_clinical_notes ...")
            elapsed = _timed_execute(con, EXTRACTION_STEP1_SQL, "raw_notes date extraction")
            raw_date_count = _safe_count(con, "_extracted_dates_raw") or 0
            raw_patients = _safe_distinct(con, "_extracted_dates_raw") or 0
            log.info(f"  → {raw_date_count:,} date instances from {raw_patients:,} patients")
            report.append(
                f"- Dates extracted from raw_clinical_notes: "
                f"{raw_date_count:,} ({raw_patients:,} patients)"
            )
        except Exception as exc:
            log.error(f"  Step 1a failed: {exc}")
            report.append(f"- raw_notes extraction FAILED: {exc}")
            # Create empty staging table so step 2 can proceed
            con.execute(
                "CREATE OR REPLACE TABLE _extracted_dates_raw ("
                "research_id VARCHAR, source_column VARCHAR, "
                "raw_date VARCHAR, extracted_date DATE, confidence FLOAT)"
            )
    else:
        log.warning("  raw_clinical_notes not found — creating empty staging table")
        con.execute(
            "CREATE OR REPLACE TABLE _extracted_dates_raw ("
            "research_id VARCHAR, source_column VARCHAR, "
            "raw_date VARCHAR, extracted_date DATE, confidence FLOAT)"
        )
        report.append("- raw_clinical_notes not found (skipped)")

    # Step 1b: Extract dates from laryngoscopy notes
    if has_complications:
        try:
            log.info("  Step 1b: Extracting dates from laryngoscopy notes ...")
            _timed_execute(con, EXTRACTION_STEP1B_SQL, "laryngoscopy date extraction")
            laryng_count = con.execute(
                "SELECT COUNT(*) FROM _extracted_dates_raw "
                "WHERE source_column = '_raw_laryngoscopy_note'"
            ).fetchone()[0]
            log.info(f"  → {laryng_count:,} additional dates from laryngoscopy notes")
            report.append(f"- Dates from laryngoscopy notes: {laryng_count:,}")
        except Exception as exc:
            log.warning(f"  Step 1b failed (non-critical): {exc}")
            report.append(f"- Laryngoscopy extraction skipped: {exc}")

    # Step 2: Build V3
    log.info("  Step 2: Building extracted_clinical_events_v3 ...")
    v3_sql = EXTRACTION_STEP2_SQL_TEMPLATE.format(source_table=source_table)
    try:
        elapsed = _timed_execute(con, v3_sql, "v3 table creation")
        v3_total = _safe_count(con, "extracted_clinical_events_v3") or 0
        v3_fu = con.execute(
            "SELECT COUNT(*) FROM extracted_clinical_events_v3 "
            "WHERE event_type = 'follow_up'"
        ).fetchone()[0]
        v3_patients = _safe_distinct(con, "extracted_clinical_events_v3") or 0

        improvement = v3_fu - source_fu
        pct = (improvement / max(source_fu, 1)) * 100

        log.info(f"  ✓ extracted_clinical_events_v3 created")
        log.info(f"    Total events: {v3_total:,}")
        log.info(f"    Follow-up events: {v3_fu:,} (was {source_fu:,}, +{improvement:,} = +{pct:.0f}%)")
        log.info(f"    Unique patients: {v3_patients:,}")

        report.append(f"- **V3 total events: {v3_total:,}**")
        report.append(f"- **V3 follow-up events: {v3_fu:,}** (+{improvement:,}, +{pct:.0f}%)")
        report.append(f"- V3 unique patients: {v3_patients:,}")
        report.append(f"- V3 build time: {elapsed:.1f}s")

        # Show confidence distribution
        try:
            conf_dist = con.execute(
                "SELECT ROUND(confidence_score, 1) AS conf, COUNT(*) AS n "
                "FROM extracted_clinical_events_v3 "
                "WHERE event_type = 'follow_up' "
                "GROUP BY 1 ORDER BY 1 DESC"
            ).fetchall()
            log.info("    Follow-up confidence distribution:")
            for conf, n in conf_dist:
                log.info(f"      {conf}: {n:,}")
        except Exception:
            pass

    except Exception as exc:
        log.error(f"  V3 creation FAILED: {exc}")
        report.append(f"- V3 creation FAILED: {exc}")

    # Cleanup staging table
    try:
        con.execute("DROP TABLE IF EXISTS _extracted_dates_raw")
    except Exception:
        pass

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 3: Materialized Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase3_materialized(con, dry_run: bool) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 3: Create Pre-Computed Analytics Tables")
    log.info("=" * 72)

    report: list[str] = []

    for table_name, sql, description in MATERIALIZED_TABLES:
        log.info(f"\n  Creating {table_name} ...")
        log.info(f"    {description}")

        if dry_run:
            log.info(f"    [DRY RUN] Would execute CREATE OR REPLACE TABLE {table_name}")
            report.append(f"- `{table_name}`: DRY RUN (skipped)")
            continue

        try:
            elapsed = _timed_execute(con, sql, table_name)
            row_count = _safe_count(con, table_name)
            patient_count = _safe_distinct(con, table_name)

            log.info(
                f"    ✓ {table_name}: {row_count:,} rows, "
                f"{patient_count:,} patients, {elapsed:.1f}s"
            )
            report.append(
                f"- `{table_name}`: **{row_count:,} rows**, "
                f"{patient_count:,} patients ({elapsed:.1f}s)"
            )

        except Exception as exc:
            log.error(f"    ✗ {table_name} FAILED: {exc}")
            report.append(f"- `{table_name}`: FAILED — {exc}")

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 4: Performance & Size Checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase4_performance(con) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 4: Performance & Size Validation")
    log.info("=" * 72)

    report: list[str] = []
    all_tables = (
        ["extracted_clinical_events_v3"]
        + [t for t, _, _ in MATERIALIZED_TABLES]
    )

    # Size + count summary
    log.info("\n  Table sizes:")
    report.append("")
    report.append("| Table | Rows | Patients | Est. Size |")
    report.append("|-------|------|----------|-----------|")

    for table_name in all_tables:
        row_count = _safe_count(con, table_name)
        patient_count = _safe_distinct(con, table_name)
        est_size = None
        try:
            result = con.execute(
                f"SELECT estimated_size FROM duckdb_tables() "
                f"WHERE table_name = '{table_name}'"
            ).fetchone()
            if result:
                est_size = result[0]
        except Exception:
            pass

        if row_count is not None:
            size_str = _fmt_size(est_size)
            log.info(
                f"    {table_name:40s}  "
                f"{row_count:>10,} rows  "
                f"{patient_count or 0:>8,} pts  "
                f"{size_str:>10s}"
            )
            report.append(
                f"| `{table_name}` | {row_count:,} | "
                f"{patient_count or 0:,} | {size_str} |"
            )
        else:
            log.warning(f"    {table_name}: not found")

    # EXPLAIN ANALYZE on one heavy query
    log.info("\n  EXPLAIN ANALYZE: patient_level_summary_mv rebuild ...")
    try:
        t0 = time.perf_counter()
        explain_result = con.execute(
            "EXPLAIN ANALYZE "
            "SELECT COUNT(*), COUNT(DISTINCT research_id) "
            "FROM patient_level_summary_mv "
            "WHERE recurrence_flag = TRUE"
        ).fetchall()
        elapsed = time.perf_counter() - t0
        log.info(f"    EXPLAIN ANALYZE completed in {elapsed:.2f}s")
        for row in explain_result[:5]:
            log.info(f"    {row[0] if isinstance(row, tuple) else row}")
        report.append(f"- EXPLAIN ANALYZE (recurrence query): {elapsed:.2f}s")
    except Exception as exc:
        log.warning(f"    EXPLAIN ANALYZE failed: {exc}")
        report.append(f"- EXPLAIN ANALYZE: skipped ({exc})")

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 5: Export to Parquet
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase5_export(con, dry_run: bool) -> list[str]:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 5: Export Key Tables to Parquet")
    log.info("=" * 72)

    EXPORTS.mkdir(exist_ok=True)
    report: list[str] = []

    for table_name in EXPORT_TABLES:
        out_path = EXPORTS / f"{table_name}.parquet"
        log.info(f"  Exporting {table_name} → {out_path.name} ...")

        if dry_run:
            log.info(f"    [DRY RUN] Would export {table_name}")
            report.append(f"- `{table_name}` → DRY RUN")
            continue

        if not _table_exists(con, table_name):
            log.warning(f"    Table {table_name} not found — skipping export")
            report.append(f"- `{table_name}` → skipped (not found)")
            continue

        try:
            t0 = time.perf_counter()
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            df.to_parquet(str(out_path), index=False)
            elapsed = time.perf_counter() - t0
            size_mb = out_path.stat().st_size / 1_048_576

            log.info(
                f"    ✓ {out_path.name}: {len(df):,} rows, "
                f"{size_mb:.2f} MB, {elapsed:.1f}s"
            )
            report.append(
                f"- `{table_name}` → `{out_path.name}` "
                f"({len(df):,} rows, {size_mb:.2f} MB)"
            )
        except Exception as exc:
            log.error(f"    ✗ Export failed for {table_name}: {exc}")
            report.append(f"- `{table_name}` → FAILED: {exc}")

    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 6: Trial Utilization Log
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase6_log(
    connection_info: list[str],
    extraction_info: list[str],
    mv_info: list[str],
    perf_info: list[str],
    export_info: list[str],
) -> None:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 6: Writing Trial Utilization Log")
    log.info("=" * 72)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sections = [
        f"# Trial Utilization Log — MotherDuck Business Tier\n",
        f"**Run:** {timestamp}\n",
        "## 1. Connection & Database Info\n",
        "\n".join(connection_info),
        "\n## 2. Date Extraction Improvement\n",
        "\n".join(extraction_info),
        "\n## 3. New Pre-Computed Analytics Tables\n",
        "\n".join(mv_info),
        "\n## 4. Performance & Size\n",
        "\n".join(perf_info),
        "\n## 5. Exports\n",
        "\n".join(export_info),
        "\n## 6. Recommendations\n",
        textwrap.dedent("""\
        - **Keep Business tier** for remaining ~18 days to run any additional
          heavy aggregations, EXPLAIN ANALYZE profiling, or bulk exports.
        - All 7 pre-computed tables + extracted_clinical_events_v3 are
          **persisted as regular tables** and remain fully queryable on
          free-tier Pulse compute after downgrade.
        - The Parquet exports in `exports/` serve as local backups independent
          of MotherDuck tier.
        - **Before downgrade:** verify Streamlit dashboard works against the
          new tables, and export any additional views you may need.
        - Consider creating a MotherDuck read-only share for collaborators
          while still on Business tier (share creation may require paid tier).
        """),
        f"\n---\n*Generated by `10_maximize_motherduck_trial.py` at {timestamp}*\n",
    ]

    content = "\n".join(sections)

    if LOG_FILE.exists():
        existing = LOG_FILE.read_text()
        content = existing.rstrip() + "\n\n---\n\n" + content

    LOG_FILE.write_text(content)
    log.info(f"  ✓ Log written to {LOG_FILE}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Maximize MotherDuck Business trial: "
        "enriched extraction + materialized analytics tables"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print SQL without executing CREATE/DROP statements"
    )
    args = parser.parse_args()

    log.info("=" * 72)
    log.info("  THYROID LAKEHOUSE — MAXIMIZE MOTHERDUCK BUSINESS TRIAL")
    log.info("=" * 72)
    if args.dry_run:
        log.info("*** DRY RUN MODE — no tables will be created ***\n")

    try:
        con = _get_connection()
    except Exception as exc:
        log.error(f"Connection failed: {exc}")
        sys.exit(1)

    try:
        info_conn = phase1_connect(con)
        info_extract = phase2_extraction(con, args.dry_run)
        info_mv = phase3_materialized(con, args.dry_run)
        info_perf = phase4_performance(con) if not args.dry_run else ["- DRY RUN"]
        info_export = phase5_export(con, args.dry_run)
        phase6_log(info_conn, info_extract, info_mv, info_perf, info_export)
    finally:
        con.close()

    log.info("\n" + "=" * 72)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 72)
    log.info(f"  Log: {LOG_FILE}")
    log.info("")
    log.info("  Next: open MotherDuck UI → query new views → connect Streamlit if ready")
    log.info("=" * 72)


if __name__ == "__main__":
    main()
