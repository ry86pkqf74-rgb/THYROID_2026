#!/usr/bin/env python3
"""Build analyst-style institutional chemistry CSV from live MotherDuck sources.

Rows are **not** copied from raw clinical note text.  Sources:

1. ``main.extracted_postop_labs_expanded_v1`` — structured post-op PTH / calcium
   (only rows with a resolved calendar ``lab_date``).
2. ``main.canonical_extracted_fact_long_v2`` — promoted NLP lab entities for
   TSH / PTH / calcium / vitamin D with ``present_or_negated = 'present'`` and a
   parsable ``entity_date``.

Use when a flat analyst deliverable file is not yet checked into
``exports/incoming/``.  ``source_table`` and ``source_lineage_key`` keep provenance
joinable without exporting note bodies.

Usage (from repo root)::

  .venv/bin/python studies/20260407_institutional_lab_wave_append/assemble_institutional_lab_csv_from_md.py \\
      --output exports/incoming/final_institutional_chemistry_20260407.csv

Requires MotherDuck RW token (prefer ``MD_SA_TOKEN`` + ``--md-sa``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

ASSEMBLY_SQL = r"""
WITH postop AS (
  SELECT
    research_id,
    TRY_CAST(lab_date AS DATE) AS lab_d,
    CASE LOWER(TRIM(lab_type))
      WHEN 'pth' THEN 'pth'
      WHEN 'total_calcium' THEN 'calcium'
      WHEN 'ionized_calcium' THEN 'ionized_calcium'
      ELSE NULL
    END AS lab_name_standardized,
    CAST(value AS DOUBLE) AS value_numeric,
    TRIM(COALESCE(CAST(value AS VARCHAR), '')) AS value_raw,
    unit AS unit_raw,
    'metabolic_panel_postop_structured' AS analyte_group,
    'extracted_postop_labs_expanded_v1' AS source_table,
    concat_ws('|',
      'join_keys', 'table=extracted_postop_labs_expanded_v1',
      concat('research_id=', CAST(research_id AS VARCHAR)),
      concat('lab_type=', lab_type)
    ) AS provenance_note,
    md5(concat_ws('|', 'postop_v1', CAST(research_id AS VARCHAR),
         strftime(CAST(TRY_CAST(lab_date AS DATE) AS TIMESTAMP), '%Y-%m-%d'),
         lab_type,
         CAST(ROUND(COALESCE(value, 0.0), 6) AS VARCHAR))) AS source_lineage_key
  FROM main.extracted_postop_labs_expanded_v1
  WHERE research_id IS NOT NULL
    AND TRY_CAST(lab_date AS DATE) IS NOT NULL
    AND EXTRACT(YEAR FROM TRY_CAST(lab_date AS DATE)) BETWEEN 1980 AND 2035
    AND value IS NOT NULL
),
canon AS (
  SELECT
    research_id,
    TRY_CAST(entity_date AS DATE) AS lab_d,
    CASE LOWER(TRIM(entity_type))
      WHEN 'tsh' THEN 'tsh'
      WHEN 'thyroid_stimulating_hormone' THEN 'tsh'
      WHEN 'pth' THEN 'pth'
      WHEN 'parathyroid_hormone' THEN 'pth'
      WHEN 'calcium' THEN 'calcium'
      WHEN 'vitamin_d' THEN 'vitamin_d'
      ELSE NULL
    END AS lab_name_standardized,
    TRY_CAST(
      regexp_replace(
        trim(COALESCE(NULLIF(TRIM(entity_value_raw), ''), entity_value_norm)),
        '^[<>]=?\s*', ''
      ) AS DOUBLE
    ) AS value_numeric,
    trim(COALESCE(NULLIF(TRIM(entity_value_raw), ''), entity_value_norm)) AS value_raw,
    CAST(NULL AS VARCHAR) AS unit_raw,
    CASE LOWER(TRIM(entity_type))
      WHEN 'tsh' THEN 'thyroid_function'
      WHEN 'thyroid_stimulating_hormone' THEN 'thyroid_function'
      ELSE 'metabolic_panel_nlp_canonical'
    END AS analyte_group,
    'canonical_extracted_fact_long_v2' AS source_table,
    md5(concat_ws('|', 'cfact_v2', COALESCE(note_row_id, ''),
         COALESCE(extraction_run_id, ''), LOWER(TRIM(entity_type)),
         COALESCE(entity_date, ''),
         COALESCE(entity_value_raw, entity_value_norm, ''))) AS source_lineage_key,
    concat_ws('|',
      'join_keys', 'table=canonical_extracted_fact_long_v2',
      concat('note_row_id=', COALESCE(note_row_id, '')),
      concat('extraction_run_id=', COALESCE(extraction_run_id, '')),
      concat('entity_type=', LOWER(TRIM(entity_type)))
    ) AS provenance_note
  FROM main.canonical_extracted_fact_long_v2
  WHERE fact_domain = 'labs'
    AND research_id IS NOT NULL
    AND present_or_negated = 'present'
    AND TRY_CAST(entity_date AS DATE) IS NOT NULL
    AND EXTRACT(YEAR FROM TRY_CAST(entity_date AS DATE)) BETWEEN 1980 AND 2035
),
u AS (
  SELECT
    research_id,
    CAST(lab_d AS VARCHAR) AS lab_date,
    lab_name_standardized,
    lab_name_standardized AS lab_name_raw,
    value_raw,
    value_numeric,
    unit_raw,
    CAST(NULL AS VARCHAR) AS unit_standardized,
    analyte_group,
    source_table,
    provenance_note,
    source_lineage_key,
    'exact_collection_date' AS lab_date_status
  FROM postop
  WHERE lab_name_standardized IS NOT NULL

  UNION ALL BY NAME

  SELECT
    research_id,
    CAST(lab_d AS VARCHAR) AS lab_date,
    lab_name_standardized,
    lab_name_standardized AS lab_name_raw,
    value_raw,
    value_numeric,
    unit_raw,
    CAST(NULL AS VARCHAR) AS unit_standardized,
    analyte_group,
    source_table,
    provenance_note,
    source_lineage_key,
    'exact_collection_date' AS lab_date_status
  FROM canon
  WHERE lab_name_standardized IS NOT NULL
    AND value_raw IS NOT NULL
    AND TRIM(value_raw) <> ''
)
SELECT * FROM u
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_lineage_key ORDER BY research_id) = 1
ORDER BY research_id, lab_date, lab_name_standardized
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--output",
        type=Path,
        default=ROOT / "exports/incoming/final_institutional_chemistry_20260407.csv",
        help="Path for CSV output.",
    )
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    return p.parse_args()


def main() -> None:
    from utils.md_connect import connect_md_or_file

    args = parse_args()
    out = args.output.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    con = connect_md_or_file(
        ROOT / "thyroid_master.duckdb",
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
    )
    try:
        df = con.execute(ASSEMBLY_SQL).fetchdf()
    finally:
        con.close()

    cols = [
        "research_id",
        "lab_date",
        "lab_name_raw",
        "lab_name_standardized",
        "value_raw",
        "value_numeric",
        "unit_raw",
        "unit_standardized",
        "analyte_group",
        "lab_date_status",
        "source_table",
        "provenance_note",
        "source_lineage_key",
    ]
    df[cols].to_csv(out, index=False)
    print(f"  Wrote {len(df):,} row(s) → {out}")
    print("  By lab_name_standardized:")
    print(df.groupby("lab_name_standardized").size().sort_values(ascending=False).to_string())


if __name__ == "__main__":
    main()
