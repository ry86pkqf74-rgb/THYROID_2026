#!/usr/bin/env python3
"""
77_lab_canonical_layer.py -- Forward-compatible canonical lab scaffold

Creates:
  - longitudinal_lab_canonical_v1: unified long-format lab table
  - val_lab_completeness_v1: per-analyte completeness summary
  
Design:
  Current partial data populates the table now via ingestion waves.
  Future institutional lab extract appends/rebuilds with minimal rewrite.
  Each row carries data_completeness_tier and ingestion_wave metadata.

Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")


def connect(args) -> duckdb.DuckDBPyConnection:
    if args.md:
        import toml
        token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(DB_PATH))


def table_exists(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


CREATE_CANONICAL_SQL = """
CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1 AS

-- Wave 1: Thyroglobulin (structured, gold standard)
SELECT
    research_id,
    TRY_CAST(specimen_collect_dt AS DATE) AS lab_date,
    CASE
        WHEN TRY_CAST(specimen_collect_dt AS DATE) IS NOT NULL THEN 'exact_collection_date'
        ELSE 'unresolved_date'
    END AS lab_date_status,
    'thyroglobulin' AS lab_name_raw,
    'thyroglobulin' AS lab_name_standardized,
    'thyroid_tumor_markers' AS analyte_group,
    result AS value_raw,
    TRY_CAST(
        CASE
            WHEN result LIKE '<%' THEN REPLACE(REPLACE(result, '<', ''), ' ', '')
            WHEN regexp_matches(result, '^[0-9]') THEN regexp_extract(result, '([0-9]+\\.?[0-9]*)', 1)
            ELSE NULL
        END AS DOUBLE
    ) AS value_numeric,
    units AS unit_raw,
    'ng/mL' AS unit_standardized,
    NULL::VARCHAR AS reference_range,
    NULL::VARCHAR AS abnormal_flag,
    CASE WHEN result LIKE '<%' THEN TRUE ELSE FALSE END AS is_censored,
    'thyroglobulin_labs' AS source_table,
    '77_lab_canonical_layer' AS source_script,
    'wave_1_structured_tg' AS ingestion_wave,
    'current_structured' AS data_completeness_tier,
    NULL::VARCHAR AS provenance_note
FROM thyroglobulin_labs
WHERE research_id IS NOT NULL

UNION ALL

-- Wave 2: Anti-thyroglobulin (structured)
SELECT
    research_id,
    TRY_CAST(specimen_collect_dt AS DATE) AS lab_date,
    CASE
        WHEN TRY_CAST(specimen_collect_dt AS DATE) IS NOT NULL THEN 'exact_collection_date'
        ELSE 'unresolved_date'
    END AS lab_date_status,
    'anti_thyroglobulin' AS lab_name_raw,
    'anti_thyroglobulin' AS lab_name_standardized,
    'thyroid_tumor_markers' AS analyte_group,
    result AS value_raw,
    TRY_CAST(
        CASE
            WHEN result LIKE '<%' THEN REPLACE(REPLACE(result, '<', ''), ' ', '')
            WHEN regexp_matches(result, '^[0-9]') THEN regexp_extract(result, '([0-9]+\\.?[0-9]*)', 1)
            ELSE NULL
        END AS DOUBLE
    ) AS value_numeric,
    units AS unit_raw,
    'IU/mL' AS unit_standardized,
    NULL::VARCHAR AS reference_range,
    NULL::VARCHAR AS abnormal_flag,
    CASE WHEN result LIKE '<%' THEN TRUE ELSE FALSE END AS is_censored,
    'anti_thyroglobulin_labs' AS source_table,
    '77_lab_canonical_layer' AS source_script,
    'wave_2_structured_anti_tg' AS ingestion_wave,
    'current_structured' AS data_completeness_tier,
    NULL::VARCHAR AS provenance_note
FROM anti_thyroglobulin_labs
WHERE research_id IS NOT NULL

UNION ALL

-- Wave 3: PTH / Calcium / Ionized Ca (NLP-extracted, partial)
SELECT
    research_id,
    TRY_CAST(lab_date AS DATE) AS lab_date,
    CASE
        WHEN TRY_CAST(lab_date AS DATE) IS NOT NULL THEN 'extracted_date'
        ELSE 'unresolved_date'
    END AS lab_date_status,
    lab_type AS lab_name_raw,
    CASE
        WHEN lab_type = 'pth' THEN 'parathyroid_hormone'
        WHEN lab_type = 'total_calcium' THEN 'calcium_total'
        WHEN lab_type = 'ionized_calcium' THEN 'calcium_ionized'
        ELSE lab_type
    END AS lab_name_standardized,
    CASE
        WHEN lab_type = 'pth' THEN 'parathyroid'
        WHEN lab_type IN ('total_calcium', 'ionized_calcium') THEN 'calcium_metabolism'
        ELSE 'other'
    END AS analyte_group,
    CAST(value AS VARCHAR) AS value_raw,
    value AS value_numeric,
    unit AS unit_raw,
    CASE
        WHEN lab_type = 'pth' THEN 'pg/mL'
        WHEN lab_type = 'total_calcium' THEN 'mg/dL'
        WHEN lab_type = 'ionized_calcium' THEN 'mmol/L'
        ELSE unit
    END AS unit_standardized,
    NULL::VARCHAR AS reference_range,
    NULL::VARCHAR AS abnormal_flag,
    FALSE AS is_censored,
    'extracted_postop_labs_expanded_v1' AS source_table,
    '77_lab_canonical_layer' AS source_script,
    'wave_3_nlp_postop_labs' AS ingestion_wave,
    'current_nlp_partial' AS data_completeness_tier,
    COALESCE(extraction_method, 'nlp') AS provenance_note
FROM extracted_postop_labs_expanded_v1
WHERE research_id IS NOT NULL
  AND value IS NOT NULL
"""

CREATE_COMPLETENESS_SQL = """
CREATE OR REPLACE TABLE val_lab_completeness_v1 AS
WITH patient_total AS (
    SELECT COUNT(DISTINCT research_id) AS n_total FROM path_synoptics
),
lab_stats AS (
    SELECT
        lab_name_standardized,
        analyte_group,
        data_completeness_tier,
        ingestion_wave,
        COUNT(*) AS n_measurements,
        COUNT(DISTINCT research_id) AS n_patients,
        ROUND(100.0 * SUM(CASE WHEN lab_date IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS date_coverage_pct,
        ROUND(100.0 * SUM(CASE WHEN value_numeric IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS numeric_parse_pct,
        source_table
    FROM longitudinal_lab_canonical_v1
    GROUP BY lab_name_standardized, analyte_group, data_completeness_tier, ingestion_wave, source_table
)
SELECT
    ls.*,
    ROUND(100.0 * ls.n_patients / pt.n_total, 1) AS patient_coverage_pct,
    CASE
        WHEN ls.data_completeness_tier = 'current_structured' THEN 'Available now - structured lab feed'
        WHEN ls.data_completeness_tier = 'current_nlp_partial' THEN 'Partial - NLP-extracted, limited coverage'
        WHEN ls.data_completeness_tier = 'future_institutional_required' THEN 'Not yet available - awaiting institutional extract'
        ELSE 'Unknown'
    END AS tier_description
FROM lab_stats ls
CROSS JOIN patient_total pt
ORDER BY ls.analyte_group, ls.lab_name_standardized
"""

FUTURE_ANALYTES_SQL = """
INSERT INTO val_lab_completeness_v1
SELECT
    analyte AS lab_name_standardized,
    grp AS analyte_group,
    'future_institutional_required' AS data_completeness_tier,
    'future_wave' AS ingestion_wave,
    0 AS n_measurements,
    0 AS n_patients,
    0.0 AS date_coverage_pct,
    0.0 AS numeric_parse_pct,
    NULL AS source_table,
    0.0 AS patient_coverage_pct,
    'Not yet available - awaiting institutional extract' AS tier_description
FROM (VALUES
    ('tsh', 'thyroid_function'),
    ('free_t4', 'thyroid_function'),
    ('free_t3', 'thyroid_function'),
    ('vitamin_d', 'bone_metabolism'),
    ('albumin', 'nutritional'),
    ('phosphorus', 'electrolytes'),
    ('magnesium', 'electrolytes'),
    ('calcitonin', 'thyroid_tumor_markers'),
    ('cea', 'thyroid_tumor_markers')
) AS t(analyte, grp)
WHERE analyte NOT IN (SELECT DISTINCT lab_name_standardized FROM val_lab_completeness_v1)
"""


def main():
    parser = argparse.ArgumentParser(description="Lab canonical layer scaffold")
    parser.add_argument("--md", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.md and not args.local:
        args.local = True

    con = connect(args)
    dry = args.dry_run

    print(f"Target: {'MotherDuck' if args.md else 'local'}")
    print(f"Timestamp: {TIMESTAMP}")

    # Build canonical table
    print("\n--- Building longitudinal_lab_canonical_v1 ---")
    if not dry:
        con.execute(CREATE_CANONICAL_SQL)
        r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1").fetchone()
        print(f"  Created: {r[0]} rows, {r[1]} patients")

        by_type = con.execute("""
            SELECT lab_name_standardized, data_completeness_tier, COUNT(*) as n, COUNT(DISTINCT research_id) as pts
            FROM longitudinal_lab_canonical_v1 GROUP BY 1, 2 ORDER BY 3 DESC
        """).fetchall()
        for row in by_type:
            print(f"    {row[0]} ({row[1]}): {row[2]} rows, {row[3]} pts")
    else:
        print("  [DRY RUN] skipped")

    # Build completeness summary
    print("\n--- Building val_lab_completeness_v1 ---")
    if not dry:
        con.execute(CREATE_COMPLETENESS_SQL)
        con.execute(FUTURE_ANALYTES_SQL)
        r = con.execute("SELECT COUNT(*) FROM val_lab_completeness_v1").fetchone()
        print(f"  Created: {r[0]} rows")

        summary = con.execute("SELECT * FROM val_lab_completeness_v1 ORDER BY analyte_group, lab_name_standardized").fetchall()
        desc = con.execute("DESCRIBE val_lab_completeness_v1").fetchall()
        cols = [d[0] for d in desc]
        for row in summary:
            d = dict(zip(cols, row))
            print(f"    {d['lab_name_standardized']}: {d['n_patients']} pts, {d['n_measurements']} meas, tier={d['data_completeness_tier']}")
    else:
        print("  [DRY RUN] skipped")

    # ANALYZE
    if not dry and args.md:
        print("\n--- Running ANALYZE ---")
        for tbl in ["longitudinal_lab_canonical_v1", "val_lab_completeness_v1"]:
            try:
                con.execute(f"ANALYZE {tbl}")
                print(f"  ANALYZE {tbl}: done")
            except Exception as e:
                print(f"  ANALYZE {tbl}: {e}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
