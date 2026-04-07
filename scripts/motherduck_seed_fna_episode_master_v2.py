#!/usr/bin/env python3
"""
motherduck_seed_fna_episode_master_v2.py

Creates main.fna_episode_master_v2 on MotherDuck when raw fna_history parquets
are not available for script 22.

Source: patient_refined_master_clinical_v12 (aggregated first/last FNA dates and
worst Bethesda). Produces one row per distinct episode date per patient (up to two
rows when first_fna_date and last_fna_date differ).

This is a operational bootstrap — full multi-FNA granularity still requires
materializing fna_history via script 22 when processed/*.parquet exists.

Usage (from repo root so .streamlit/secrets.toml resolves):
  .venv/bin/python scripts/motherduck_seed_fna_episode_master_v2.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

SEED_SQL = """
CREATE OR REPLACE TABLE fna_episode_master_v2 AS
WITH raw AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        CAST(worst_bethesda_num AS INTEGER) AS bethesda_category,
        CAST(CAST(bethesda_final AS VARCHAR) AS VARCHAR) AS bethesda_raw,
        fna_path_outcome AS pathology_diagnosis,
        CAST(NULL AS VARCHAR) AS pathology_extended,
        CAST(NULL AS VARCHAR) AS specimen_site_raw,
        CAST(NULL AS VARCHAR) AS laterality,
        CAST(first_fna_date AS DATE) AS d_first,
        CAST(last_fna_date AS DATE) AS d_last
    FROM patient_refined_master_clinical_v12
    WHERE COALESCE(n_fna_episodes, 0) >= 1
      AND first_fna_date IS NOT NULL
),
expanded AS (
    SELECT
        research_id,
        bethesda_category,
        bethesda_raw,
        pathology_diagnosis,
        pathology_extended,
        specimen_site_raw,
        laterality,
        d_first AS episode_date
    FROM raw
    UNION ALL
    SELECT
        research_id,
        bethesda_category,
        bethesda_raw,
        pathology_diagnosis,
        pathology_extended,
        specimen_site_raw,
        laterality,
        d_last
    FROM raw
    WHERE d_last IS NOT NULL
      AND d_last IS DISTINCT FROM d_first
),
dedup AS (
    SELECT DISTINCT
        research_id,
        episode_date,
        bethesda_category,
        bethesda_raw,
        pathology_diagnosis,
        pathology_extended,
        specimen_site_raw,
        laterality
    FROM expanded
    WHERE episode_date IS NOT NULL
)
SELECT
    research_id,
    CAST(
        ROW_NUMBER() OVER (
            PARTITION BY research_id ORDER BY episode_date ASC, bethesda_category ASC
        ) AS INTEGER
    ) AS fna_episode_id,
    episode_date AS fna_date_native,
    episode_date AS resolved_fna_date,
    'exact_source_date'::VARCHAR AS date_status,
    100::INTEGER AS date_confidence,
    bethesda_raw,
    bethesda_category,
    pathology_diagnosis,
    pathology_extended,
    specimen_site_raw,
    laterality,
    NULL::VARCHAR AS linked_molecular_episode_id,
    NULL::VARCHAR AS linked_imaging_nodule_id,
    NULL::VARCHAR AS linked_surgery_episode_id,
    'patient_refined_master_clinical_v12'::VARCHAR AS source_table,
    NULL::DOUBLE AS fna_confidence
FROM dedup
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL only",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    from motherduck_client import token_mode
    from utils.md_connect import connect_md_fail_closed

    print(f"  MotherDuck token source: {token_mode()}")
    if args.dry_run:
        print(SEED_SQL)
        return

    con = connect_md_fail_closed(DB_PATH)
    try:
        con.execute("SELECT 1 FROM patient_refined_master_clinical_v12 LIMIT 1")
    except Exception as e:
        print("  FATAL: patient_refined_master_clinical_v12 not found on MotherDuck:", e)
        sys.exit(1)

    con.execute(SEED_SQL.strip())
    n = con.execute("SELECT COUNT(*) FROM fna_episode_master_v2").fetchone()[0]
    p = con.execute("SELECT COUNT(DISTINCT research_id) FROM fna_episode_master_v2").fetchone()[0]
    print(f"  fna_episode_master_v2: {n:,} rows, {p:,} patients")
    con.close()


if __name__ == "__main__":
    main()
