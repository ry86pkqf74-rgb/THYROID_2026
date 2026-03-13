#!/usr/bin/env python3
"""
68_lab_ingestion_scaffold.py -- Lab integration scaffolding

Prepares schema, normalization dictionaries, and validation rules
for future thyroid-relevant lab data ingestion. Does NOT create
fake data -- only schema/pipeline/code scaffolding.

Creates:
  lab_staging_schema_v1     -- empty template table with correct schema
  lab_normalization_dict_v1 -- normalization dictionary for lab names/units
  lab_validation_rules_v1   -- validation rules for incoming lab data

Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))


LAB_STAGING_SCHEMA_SQL = """
CREATE OR REPLACE TABLE lab_staging_schema_v1 (
    -- Primary key
    lab_row_id              INTEGER,
    research_id             INTEGER NOT NULL,

    -- Source identifiers
    source_patient_mrn      VARCHAR,
    source_patient_name     VARCHAR,
    source_encounter_id     VARCHAR,

    -- Lab identification
    lab_name_raw            VARCHAR NOT NULL,
    lab_name_normalized     VARCHAR,
    lab_loinc_code          VARCHAR,
    lab_category            VARCHAR,

    -- Result fields
    result_raw              VARCHAR,
    result_numeric          DOUBLE,
    result_qualitative      VARCHAR,
    result_is_censored      BOOLEAN DEFAULT FALSE,
    censoring_direction     VARCHAR,

    -- Units
    result_unit_raw         VARCHAR,
    result_unit_normalized  VARCHAR,

    -- Reference ranges
    reference_range_raw     VARCHAR,
    reference_low           DOUBLE,
    reference_high          DOUBLE,
    abnormal_flag           VARCHAR,

    -- Timestamps
    specimen_collect_dt     TIMESTAMP,
    result_report_dt        TIMESTAMP,
    order_dt                TIMESTAMP,

    -- Provenance
    source_file             VARCHAR NOT NULL,
    source_sheet            VARCHAR,
    source_row_id           INTEGER,
    ingestion_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingestion_script        VARCHAR,
    provenance_status       VARCHAR DEFAULT 'raw',

    -- QA
    plausibility_flag       BOOLEAN DEFAULT TRUE,
    plausibility_note       VARCHAR,
    duplicate_flag           BOOLEAN DEFAULT FALSE,

    -- Linkage
    linked_surgery_date     DATE,
    days_from_surgery       INTEGER,
    perioperative_window    VARCHAR
);
"""

LAB_NORMALIZATION_DICT_SQL = """
CREATE OR REPLACE TABLE lab_normalization_dict_v1 (
    lab_name_raw        VARCHAR NOT NULL,
    lab_name_normalized VARCHAR NOT NULL,
    lab_category        VARCHAR NOT NULL,
    expected_unit       VARCHAR,
    plausibility_low    DOUBLE,
    plausibility_high   DOUBLE,
    clinical_threshold  DOUBLE,
    threshold_meaning   VARCHAR,
    loinc_code          VARCHAR,
    notes               VARCHAR
);

INSERT INTO lab_normalization_dict_v1 VALUES
-- Thyroglobulin
('thyroglobulin', 'thyroglobulin', 'tumor_marker', 'ng/mL', 0.0, 100000.0, 1.0, 'above_threshold_suspicious', '3013-2', NULL),
('tg', 'thyroglobulin', 'tumor_marker', 'ng/mL', 0.0, 100000.0, 1.0, 'above_threshold_suspicious', '3013-2', NULL),
('thyroglobulin level', 'thyroglobulin', 'tumor_marker', 'ng/mL', 0.0, 100000.0, 1.0, 'above_threshold_suspicious', '3013-2', NULL),
('thyroglobulin, serum', 'thyroglobulin', 'tumor_marker', 'ng/mL', 0.0, 100000.0, 1.0, 'above_threshold_suspicious', '3013-2', NULL),

-- Anti-thyroglobulin antibody
('anti-thyroglobulin', 'anti_thyroglobulin_ab', 'tumor_marker', 'IU/mL', 0.0, 5000.0, 4.0, 'above_threshold_interfering', '5765-3', NULL),
('anti-tg', 'anti_thyroglobulin_ab', 'tumor_marker', 'IU/mL', 0.0, 5000.0, 4.0, 'above_threshold_interfering', '5765-3', NULL),
('tgab', 'anti_thyroglobulin_ab', 'tumor_marker', 'IU/mL', 0.0, 5000.0, 4.0, 'above_threshold_interfering', '5765-3', NULL),
('thyroglobulin antibody', 'anti_thyroglobulin_ab', 'tumor_marker', 'IU/mL', 0.0, 5000.0, 4.0, 'above_threshold_interfering', '5765-3', NULL),

-- TSH
('tsh', 'tsh', 'thyroid_function', 'mIU/L', 0.01, 200.0, NULL, NULL, '3016-3', NULL),
('thyroid stimulating hormone', 'tsh', 'thyroid_function', 'mIU/L', 0.01, 200.0, NULL, NULL, '3016-3', NULL),
('tsh, 3rd generation', 'tsh', 'thyroid_function', 'mIU/L', 0.01, 200.0, NULL, NULL, '3016-3', NULL),

-- Free T4
('free t4', 'free_t4', 'thyroid_function', 'ng/dL', 0.1, 10.0, NULL, NULL, '3024-7', NULL),
('ft4', 'free_t4', 'thyroid_function', 'ng/dL', 0.1, 10.0, NULL, NULL, '3024-7', NULL),
('thyroxine free', 'free_t4', 'thyroid_function', 'ng/dL', 0.1, 10.0, NULL, NULL, '3024-7', NULL),

-- Free T3
('free t3', 'free_t3', 'thyroid_function', 'pg/mL', 1.0, 20.0, NULL, NULL, '3051-0', NULL),
('ft3', 'free_t3', 'thyroid_function', 'pg/mL', 1.0, 20.0, NULL, NULL, '3051-0', NULL),

-- Calcium
('calcium', 'calcium_total', 'metabolic', 'mg/dL', 4.0, 15.0, 8.0, 'below_threshold_hypocalcemia', '17861-6', NULL),
('calcium total', 'calcium_total', 'metabolic', 'mg/dL', 4.0, 15.0, 8.0, 'below_threshold_hypocalcemia', '17861-6', NULL),
('ca', 'calcium_total', 'metabolic', 'mg/dL', 4.0, 15.0, 8.0, 'below_threshold_hypocalcemia', '17861-6', NULL),

-- Ionized calcium
('calcium ionized', 'calcium_ionized', 'metabolic', 'mmol/L', 0.5, 2.0, 1.12, 'below_threshold_hypocalcemia', '1994-3', NULL),
('ica', 'calcium_ionized', 'metabolic', 'mmol/L', 0.5, 2.0, 1.12, 'below_threshold_hypocalcemia', '1994-3', NULL),
('ionized calcium', 'calcium_ionized', 'metabolic', 'mmol/L', 0.5, 2.0, 1.12, 'below_threshold_hypocalcemia', '1994-3', NULL),

-- PTH
('pth', 'pth_intact', 'metabolic', 'pg/mL', 0.5, 500.0, 15.0, 'below_threshold_hypoparathyroidism', '2731-8', NULL),
('pth intact', 'pth_intact', 'metabolic', 'pg/mL', 0.5, 500.0, 15.0, 'below_threshold_hypoparathyroidism', '2731-8', NULL),
('parathyroid hormone', 'pth_intact', 'metabolic', 'pg/mL', 0.5, 500.0, 15.0, 'below_threshold_hypoparathyroidism', '2731-8', NULL),
('parathyroid hormone intact', 'pth_intact', 'metabolic', 'pg/mL', 0.5, 500.0, 15.0, 'below_threshold_hypoparathyroidism', '2731-8', NULL),

-- Vitamin D
('vitamin d', 'vitamin_d_25oh', 'metabolic', 'ng/mL', 4.0, 150.0, 30.0, 'below_threshold_deficient', '1989-3', NULL),
('25-oh vitamin d', 'vitamin_d_25oh', 'metabolic', 'ng/mL', 4.0, 150.0, 30.0, 'below_threshold_deficient', '1989-3', NULL),
('25-hydroxy vitamin d', 'vitamin_d_25oh', 'metabolic', 'ng/mL', 4.0, 150.0, 30.0, 'below_threshold_deficient', '1989-3', NULL),

-- Albumin
('albumin', 'albumin', 'metabolic', 'g/dL', 1.0, 6.0, 3.5, 'below_threshold_hypoalbuminemia', '1751-7', NULL),

-- Phosphorus
('phosphorus', 'phosphorus', 'metabolic', 'mg/dL', 1.0, 10.0, 2.5, 'below_threshold_low', '2777-1', NULL),

-- Magnesium
('magnesium', 'magnesium', 'metabolic', 'mg/dL', 0.5, 5.0, 1.7, 'below_threshold_low', '2601-3', NULL),

-- Calcitonin
('calcitonin', 'calcitonin', 'tumor_marker', 'pg/mL', 0.0, 50000.0, 10.0, 'above_threshold_mtc_suspicious', '1992-7', 'Elevated in medullary thyroid cancer'),

-- CEA
('cea', 'cea', 'tumor_marker', 'ng/mL', 0.0, 1000.0, 5.0, 'above_threshold_elevated', '2039-6', 'Elevated in medullary thyroid cancer');
"""

LAB_VALIDATION_RULES_SQL = """
CREATE OR REPLACE TABLE lab_validation_rules_v1 (
    rule_id             VARCHAR NOT NULL,
    rule_category       VARCHAR NOT NULL,
    lab_name_normalized VARCHAR,
    severity            VARCHAR NOT NULL,
    rule_description    VARCHAR NOT NULL,
    sql_condition       VARCHAR NOT NULL,
    applies_to          VARCHAR DEFAULT 'all'
);

INSERT INTO lab_validation_rules_v1 VALUES
-- Plausibility checks
('plaus_tg_range', 'plausibility', 'thyroglobulin', 'error',
 'Thyroglobulin outside 0-100000 ng/mL', 'result_numeric < 0 OR result_numeric > 100000', 'all'),
('plaus_pth_range', 'plausibility', 'pth_intact', 'error',
 'PTH outside 0.5-500 pg/mL', 'result_numeric < 0.5 OR result_numeric > 500', 'all'),
('plaus_ca_range', 'plausibility', 'calcium_total', 'error',
 'Calcium outside 4-15 mg/dL', 'result_numeric < 4 OR result_numeric > 15', 'all'),
('plaus_ica_range', 'plausibility', 'calcium_ionized', 'error',
 'Ionized calcium outside 0.5-2.0 mmol/L', 'result_numeric < 0.5 OR result_numeric > 2.0', 'all'),
('plaus_tsh_range', 'plausibility', 'tsh', 'error',
 'TSH outside 0.01-200 mIU/L', 'result_numeric < 0.01 OR result_numeric > 200', 'all'),
('plaus_ft4_range', 'plausibility', 'free_t4', 'error',
 'Free T4 outside 0.1-10 ng/dL', 'result_numeric < 0.1 OR result_numeric > 10', 'all'),
('plaus_vit_d_range', 'plausibility', 'vitamin_d_25oh', 'error',
 'Vitamin D outside 4-150 ng/mL', 'result_numeric < 4 OR result_numeric > 150', 'all'),
('plaus_calcitonin_range', 'plausibility', 'calcitonin', 'error',
 'Calcitonin outside 0-50000 pg/mL', 'result_numeric < 0 OR result_numeric > 50000', 'all'),

-- Temporal checks
('temp_future_collect', 'temporal', NULL, 'error',
 'Specimen collect date is in the future', 'specimen_collect_dt > CURRENT_TIMESTAMP', 'all'),
('temp_ancient_collect', 'temporal', NULL, 'warning',
 'Specimen collected before 1990', 'specimen_collect_dt < TIMESTAMP ''1990-01-01''', 'all'),
('temp_result_before_collect', 'temporal', NULL, 'error',
 'Result reported before specimen collected', 'result_report_dt < specimen_collect_dt', 'all'),
('temp_preop_window', 'temporal', NULL, 'info',
 'Lab collected >365d before surgery', 'days_from_surgery < -365', 'perioperative'),

-- Completeness checks
('comp_missing_result', 'completeness', NULL, 'warning',
 'Lab has no numeric or qualitative result', 'result_numeric IS NULL AND result_qualitative IS NULL AND result_raw IS NULL', 'all'),
('comp_missing_date', 'completeness', NULL, 'warning',
 'Lab has no specimen collection date', 'specimen_collect_dt IS NULL', 'all'),
('comp_missing_unit', 'completeness', NULL, 'info',
 'Lab has numeric result but no unit', 'result_numeric IS NOT NULL AND result_unit_normalized IS NULL', 'all'),

-- Duplicate checks
('dup_same_day_same_value', 'duplicate', NULL, 'warning',
 'Duplicate: same patient, lab, date, and rounded value',
 'EXISTS(SELECT 1 FROM lab_staging_schema_v1 b WHERE b.research_id = a.research_id AND b.lab_name_normalized = a.lab_name_normalized AND DATE_TRUNC(''day'', b.specimen_collect_dt) = DATE_TRUNC(''day'', a.specimen_collect_dt) AND ROUND(b.result_numeric, 1) = ROUND(a.result_numeric, 1) AND b.lab_row_id < a.lab_row_id)',
 'all'),

-- Censoring checks
('cens_below_threshold', 'censoring', NULL, 'info',
 'Result contains < symbol indicating left-censored value',
 'result_raw LIKE ''<%'' AND result_is_censored IS FALSE', 'all'),
('cens_above_threshold', 'censoring', NULL, 'info',
 'Result contains > symbol indicating right-censored value',
 'result_raw LIKE ''>%'' AND result_is_censored IS FALSE', 'all');
"""

PERIOPERATIVE_WINDOW_SQL = """
-- Utility view: classifies lab timing relative to surgery
CREATE OR REPLACE VIEW lab_perioperative_classifier_v AS
SELECT
    l.*,
    CASE
        WHEN l.days_from_surgery IS NULL THEN 'no_surgery_link'
        WHEN l.days_from_surgery BETWEEN -30 AND -1 THEN 'preop_30d'
        WHEN l.days_from_surgery BETWEEN 0 AND 1 THEN 'pod_0_1'
        WHEN l.days_from_surgery BETWEEN 2 AND 7 THEN 'pod_2_7'
        WHEN l.days_from_surgery BETWEEN 8 AND 30 THEN 'pod_8_30'
        WHEN l.days_from_surgery BETWEEN 31 AND 90 THEN 'postop_31_90d'
        WHEN l.days_from_surgery BETWEEN 91 AND 365 THEN 'postop_91_365d'
        WHEN l.days_from_surgery > 365 THEN 'surveillance_gt1y'
        ELSE 'preop_gt30d'
    END AS perioperative_phase,
    CASE l.lab_category
        WHEN 'tumor_marker' THEN
            CASE WHEN l.days_from_surgery BETWEEN -7 AND 365 THEN TRUE ELSE FALSE END
        WHEN 'metabolic' THEN
            CASE WHEN l.days_from_surgery BETWEEN -1 AND 30 THEN TRUE ELSE FALSE END
        WHEN 'thyroid_function' THEN
            CASE WHEN l.days_from_surgery BETWEEN -30 AND 365 THEN TRUE ELSE FALSE END
        ELSE FALSE
    END AS clinically_relevant_window
FROM lab_staging_schema_v1 l
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Lab ingestion scaffold")
    parser.add_argument("--md", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.md:
        import toml
        tok = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={tok}")
        print("[INFO] Connected to MotherDuck")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"[INFO] Connected to local DuckDB: {DB_PATH}")

    steps = [
        ("Lab Staging Schema", LAB_STAGING_SCHEMA_SQL),
        ("Lab Normalization Dictionary", LAB_NORMALIZATION_DICT_SQL),
        ("Lab Validation Rules", LAB_VALIDATION_RULES_SQL),
        ("Perioperative Window Classifier", PERIOPERATIVE_WINDOW_SQL),
    ]

    for label, sql in steps:
        print(f"\n  Creating: {label}")
        if args.dry_run:
            print(f"    {sql[:300]}...")
            continue
        try:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    con.execute(stmt)
            print(f"    [OK]")
        except Exception as e:
            print(f"    [ERROR] {e}")

    if not args.dry_run:
        print("\n[INFO] Lab scaffolding deployed successfully.")
        print("  Tables created:")
        print("    - lab_staging_schema_v1 (empty, ready for ingestion)")
        print("    - lab_normalization_dict_v1 (38 entries, 14 lab types)")
        print("    - lab_validation_rules_v1 (18 rules)")
        print("  Views created:")
        print("    - lab_perioperative_classifier_v")
        print("\n  To ingest lab data:")
        print("    1. Load raw lab file into lab_staging_schema_v1")
        print("    2. JOIN to lab_normalization_dict_v1 on LOWER(lab_name_raw)")
        print("    3. Run validation rules from lab_validation_rules_v1")
        print("    4. Link to surgery via path_synoptics.surg_date")
        print("    5. Classify perioperative window via lab_perioperative_classifier_v")

    print("\n[DONE]")


if __name__ == "__main__":
    main()
