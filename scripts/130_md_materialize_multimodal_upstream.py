#!/usr/bin/env python3
"""
130_md_materialize_multimodal_upstream.py

Materialize multimodal contract upstream tables on MotherDuck when the cloud
catalog has canonical v2 facts + imaging_nodule_master_v1 but is missing:

  - imaging_nodule_long_v2       (derived here from imaging_nodule_master_v1)
  - event_date_audit_v2         (script 22 SQL; needs long_v2)
  - patient_cross_domain_timeline_v2 (script 22 SQL)
  - linkage_master_v1            (identity spine; no raw MRN sources in MD)
  - mrn_crosswalk_v1             (zero-row table with contract columns)

Why not script 47?
  47_mrn_crosswalk_demographics_v3 requires raw_path_synoptics / raw_clinical_notes /
  raw_complications / raw_operative_details, which are typically not loaded to MD.

Why not full script 22?
  Full 22 rebuilds all canonical tables from local parquets + register_parquets;
  MotherDuck is usually already hydrated from pipeline imports. This script only
  fills the gap tables safely.

Run:
  .venv/bin/python scripts/130_md_materialize_multimodal_upstream.py --md
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402


def _load_22_module():
    path = ROOT / "scripts" / "22_canonical_episodes_v2.py"
    spec = importlib.util.spec_from_file_location("canonical_episodes_v2", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


IMAGING_LONG_V2_FROM_MASTER_V1_SQL = """
CREATE OR REPLACE TABLE imaging_nodule_long_v2 AS
WITH base AS (
    SELECT
        CAST(m.research_id AS INTEGER) AS research_id,
        TRY_CAST(m.exam_date AS DATE) AS exam_date_native,
        TRY_CAST(m.exam_date AS DATE) AS resolved_exam_date,
        CASE
            WHEN TRY_CAST(m.exam_date AS DATE) IS NOT NULL
            THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(m.exam_date AS DATE) IS NOT NULL THEN 100
            ELSE 0
        END AS date_confidence,
        'US'::VARCHAR AS modality,
        'imaging_nodule_master_v1'::VARCHAR AS report_source_table,
        m.exam_id,
        CAST(m.nodule_id AS VARCHAR) AS nodule_id,
        m.nodule_number,
        m.laterality,
        TRY_CAST(m.max_dimension_cm AS DOUBLE) AS max_dim_cm,
        COALESCE(m.suspicious_flag, FALSE) AS suspicious_flag
    FROM imaging_nodule_master_v1 m
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY research_id
            ORDER BY exam_id NULLS LAST, exam_date_native NULLS LAST
        )::INTEGER AS imaging_exam_id,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, exam_id
            ORDER BY nodule_number NULLS LAST, nodule_id NULLS LAST
        )::INTEGER AS nodule_index_within_exam
    FROM base
)
SELECT
    research_id,
    modality,
    exam_date_native,
    date_status,
    date_confidence,
    imaging_exam_id,
    nodule_index_within_exam,
    CAST(NULL AS VARCHAR) AS composition,
    CAST(NULL AS VARCHAR) AS echogenicity,
    CAST(NULL AS VARCHAR) AS shape,
    CAST(NULL AS VARCHAR) AS margins,
    CAST(NULL AS VARCHAR) AS calcifications,
    CAST(NULL AS INTEGER) AS tirads_score,
    CAST(NULL AS VARCHAR) AS tirads_category,
    max_dim_cm AS size_cm_max,
    CAST(NULL AS DOUBLE) AS size_cm_x,
    CAST(NULL AS DOUBLE) AS size_cm_y,
    CAST(NULL AS DOUBLE) AS size_cm_z,
    laterality AS laterality,
    CAST(NULL AS VARCHAR) AS location_detail,
    report_source_table,
    CAST(NULL AS VARCHAR) AS exam_impression_raw,
    suspicious_flag AS suspicious_node_flag,
    CAST(NULL AS VARCHAR) AS suspicious_node_details,
    FALSE AS growth_flag,
    TRUE AS dominant_nodule_flag,
    CAST(research_id AS VARCHAR) || '-' || modality || '-' ||
        CAST(imaging_exam_id AS VARCHAR) || '-' ||
        CAST(nodule_index_within_exam AS VARCHAR) AS nodule_id,
    resolved_exam_date,
    CAST(NULL AS INTEGER) AS nodule_count_in_exam,
    CAST(NULL AS DOUBLE) AS imaging_confidence,
    CAST(NULL AS VARCHAR) AS linked_fna_episode_id,
    CAST(NULL AS VARCHAR) AS linked_molecular_episode_id,
    CAST(NULL AS VARCHAR) AS linked_pathology_tumor_id
FROM ranked
"""

LINKAGE_MASTER_IDENTITY_SQL = """
CREATE OR REPLACE TABLE linkage_master_v1 AS
SELECT DISTINCT
    CAST(u.research_id AS BIGINT) AS research_id,
    CAST(u.research_id AS BIGINT) AS canonical_research_id,
    CAST(NULL AS VARCHAR) AS euh_mrn,
    'identity'::VARCHAR AS linkage_method,
    1.0::DOUBLE AS confidence,
    FALSE AS has_mrn
FROM (
    SELECT research_id FROM operative_episode_detail_v2
    UNION
    SELECT research_id FROM tumor_episode_master_v2
    UNION
    SELECT research_id FROM molecular_test_episode_v2
    UNION
    SELECT research_id FROM imaging_nodule_master_v1
) u
WHERE u.research_id IS NOT NULL
"""

MRN_CROSSWALK_EMPTY_SQL = """
CREATE OR REPLACE TABLE mrn_crosswalk_v1 AS
SELECT
    CAST(NULL AS INTEGER) AS research_id,
    CAST(NULL AS VARCHAR) AS euh_mrn,
    CAST(NULL AS VARCHAR) AS tec_mrn,
    CAST(NULL AS INTEGER) AS canonical_research_id,
    CAST(NULL AS VARCHAR) AS linkage_method,
    CAST(NULL AS DOUBLE) AS confidence
WHERE FALSE
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--md",
        action="store_true",
        help="Target MotherDuck (required; fail-closed).",
    )
    args = ap.parse_args()
    if not args.md:
        ap.error("Only --md is supported (local file path is out of scope for this script).")

    c22 = _load_22_module()

    con = connect_md_fail_closed(DB_PATH, env="prod")
    try:
        section("imaging_nodule_long_v2 ← imaging_nodule_master_v1")
        con.execute(IMAGING_LONG_V2_FROM_MASTER_V1_SQL)
        n_long = con.execute("SELECT COUNT(*) FROM imaging_nodule_long_v2").fetchone()[0]
        print(f"  rows: {n_long:,}")

        section("event_date_audit_v2 (script 22)")
        con.execute(c22.EVENT_DATE_AUDIT_V2_SQL)
        n_eda = con.execute("SELECT COUNT(*) FROM event_date_audit_v2").fetchone()[0]
        print(f"  rows: {n_eda:,}")

        section("patient_cross_domain_timeline_v2 (script 22)")
        con.execute(c22.PATIENT_CROSS_DOMAIN_TIMELINE_V2_SQL)
        n_tl = con.execute("SELECT COUNT(*) FROM patient_cross_domain_timeline_v2").fetchone()[0]
        print(f"  rows: {n_tl:,}")

        section("linkage_master_v1 (identity)")
        con.execute(LINKAGE_MASTER_IDENTITY_SQL)
        n_lm = con.execute("SELECT COUNT(*) FROM linkage_master_v1").fetchone()[0]
        print(f"  rows: {n_lm:,}")

        section("mrn_crosswalk_v1 (empty stub)")
        con.execute(MRN_CROSSWALK_EMPTY_SQL)
        n_xw = con.execute("SELECT COUNT(*) FROM mrn_crosswalk_v1").fetchone()[0]
        print(f"  rows: {n_xw:,}")

        print("\n  Done. Re-run 128 with --md (omit --allow-bootstrap-dev if all upstreams exist).\n")
    finally:
        con.close()


if __name__ == "__main__":
    main()
