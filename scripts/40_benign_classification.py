#!/usr/bin/env python3
"""
40_benign_classification.py — Classify no-histology path_synoptics records.

Creates `benign_procedure_classification_v` to categorize path_synoptics rows
with missing/blank tumor histology into likely benign or completion-procedure
buckets for downstream manuscript accounting.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


BENIGN_CLASSIFICATION_SQL = """
CREATE OR REPLACE VIEW benign_procedure_classification_v AS
WITH ps_norm AS (
    SELECT
        ps.*,
        LOWER(TRIM(COALESCE(CAST(ps.tumor_1_histologic_type AS VARCHAR), ''))) AS hist_norm,
        LOWER(TRIM(COALESCE(CAST(ps.reop AS VARCHAR), ''))) AS reop_norm
    FROM path_synoptics ps
),
patient_cancer_flags AS (
    SELECT
        research_id,
        MAX(
            CASE
                WHEN hist_norm NOT IN ('', 'x', 'none', 'na', 'n/a', 'unknown', 'null')
                     AND hist_norm IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS has_any_histology_row
    FROM ps_norm
    GROUP BY research_id
),
no_histology_rows AS (
    SELECT
        p.*,
        f.has_any_histology_row,
        CASE
            WHEN p.hist_norm IN ('', 'x', 'none', 'na', 'n/a', 'unknown', 'null') THEN TRUE
            ELSE FALSE
        END AS is_missing_histology
    FROM ps_norm p
    LEFT JOIN patient_cancer_flags f USING (research_id)
)
SELECT
    research_id,
    TRY_CAST(surg_date AS DATE) AS surg_date,
    reop,
    surgery,
    thyroid_procedure,
    path_diagnosis_summary,
    path_diagnosis_comment,
    synoptic_diagnosis,
    tumor_1_histologic_type,
    has_any_histology_row,
    CASE
        WHEN has_any_histology_row = 1 AND reop_norm IN ('yes', 'true', '1', 'y')
            THEN 'completion_or_reoperation_after_cancer'
        WHEN LOWER(TRIM(COALESCE(multinodular_goiter, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(substernal_multinodular_goiter, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(adenomatous_hyperplasia, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(hyperplastic_nodules, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(adenomatoid_nodules, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(colloid_nodule, ''))) IN ('x', 'yes', 'present', 'true')
            THEN 'multinodular_goiter_or_hyperplasia'
        WHEN LOWER(TRIM(COALESCE(follicular_adenoma, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(hurthle_cell_oncocytic_adenoma, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(hyalinizing_trabecular_tumor_adenoma, ''))) IN ('x', 'yes', 'present', 'true')
            THEN 'benign_adenoma_pattern'
        WHEN LOWER(TRIM(COALESCE(hashimoto_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(chronic_lymphocytic_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(lymphocytic_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(chronic_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(autoimmune_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
            THEN 'thyroiditis_pattern'
        WHEN LOWER(TRIM(COALESCE(graves, ''))) IN ('x', 'yes', 'present', 'true')
            THEN 'graves_pattern'
        WHEN LOWER(TRIM(COALESCE(parathyroid_glands, ''))) NOT IN ('', 'x', 'none', 'na', 'n/a', 'unknown', 'null')
             OR LOWER(TRIM(COALESCE(parathyroid_gland_findings, ''))) NOT IN ('', 'x', 'none', 'na', 'n/a', 'unknown', 'null')
            THEN 'parathyroid_focused_specimen'
        WHEN has_any_histology_row = 1
            THEN 'likely_non_index_specimen'
        ELSE 'unclassified_no_histology'
    END AS benign_classification,
    CASE
        WHEN has_any_histology_row = 1 AND reop_norm IN ('yes', 'true', '1', 'y') THEN 90
        WHEN LOWER(TRIM(COALESCE(multinodular_goiter, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(follicular_adenoma, ''))) IN ('x', 'yes', 'present', 'true')
             OR LOWER(TRIM(COALESCE(hashimoto_thyroiditis, ''))) IN ('x', 'yes', 'present', 'true')
            THEN 80
        WHEN has_any_histology_row = 1 THEN 70
        ELSE 40
    END AS classification_confidence
FROM no_histology_rows
WHERE is_missing_histology = TRUE;
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Benign no-histology classification")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck RW database")
    args = parser.parse_args()

    print("=" * 80)
    print("  BENIGN CLASSIFICATION — No-Histology path_synoptics")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient

        con = MotherDuckClient().connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))
        pq = PROCESSED / "path_synoptics.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE path_synoptics AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )

    section("CREATE VIEW")
    con.execute(BENIGN_CLASSIFICATION_SQL)
    total = con.execute(
        "SELECT COUNT(*) FROM benign_procedure_classification_v"
    ).fetchone()[0]
    print(f"  benign_procedure_classification_v: {total:,} rows")

    section("CLASSIFICATION SUMMARY")
    rows = con.execute(
        """
        SELECT benign_classification, COUNT(*) AS rows,
               COUNT(DISTINCT research_id) AS patients,
               ROUND(AVG(classification_confidence), 1) AS avg_conf
        FROM benign_procedure_classification_v
        GROUP BY 1
        ORDER BY rows DESC
        """
    ).fetchall()
    print(f"  {'classification':<45} {'rows':>8} {'patients':>10} {'avg_conf':>10}")
    print("  " + "-" * 77)
    for r in rows:
        print(f"  {str(r[0]):<45} {r[1]:>8} {r[2]:>10} {r[3]:>10.1f}")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
