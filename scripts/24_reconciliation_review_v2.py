#!/usr/bin/env python3
"""
24_reconciliation_review_v2.py -- Cross-source reconciliation review views

Creates review-queue views that surface disagreements and mismatches across
clinical domains for manual adjudication.

Views created:
  1. pathology_reconciliation_review_v2   -- histology mismatches
  2. molecular_linkage_review_v2          -- molecular linkage issues
  3. rai_adjudication_review_v2           -- RAI chronology/dose issues
  4. imaging_pathology_concordance_review_v2 -- laterality/size mismatches
  5. operative_pathology_reconciliation_review_v2 -- op vs path mismatches

Run after scripts 22 and 23.
Supports --md flag for MotherDuck deployment.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 1. Pathology reconciliation review
# ---------------------------------------------------------------------------
PATHOLOGY_RECON_SQL = """
CREATE OR REPLACE TABLE pathology_reconciliation_review_v2 AS
SELECT
    t.research_id,
    t.surgery_episode_id,
    t.tumor_ordinal,
    t.surgery_date,
    t.primary_histology,
    t.histology_variant,
    t.histology_source,
    t.t_stage,
    t.n_stage,
    t.extrathyroidal_extension,
    t.vascular_invasion,
    t.capsular_invasion,
    t.perineural_invasion,
    t.margin_status,
    t.consult_diagnosis,
    t.consult_precedence_flag,
    t.histology_discordance_flag,
    t.t_stage_discordance_flag,
    t.confidence_rank,
    CASE
        WHEN t.histology_discordance_flag THEN 'histology_mismatch'
        WHEN t.t_stage_discordance_flag THEN 'staging_mismatch'
        WHEN t.consult_precedence_flag AND t.confidence_rank > 1
             THEN 'consult_precedence_needed'
        ELSE 'concordant'
    END AS review_reason,
    CASE
        WHEN t.histology_discordance_flag THEN 'error'
        WHEN t.t_stage_discordance_flag THEN 'warning'
        WHEN t.consult_precedence_flag THEN 'info'
        ELSE 'ok'
    END AS review_severity,
    'pending' AS review_status
FROM tumor_episode_master_v2 t
WHERE t.histology_discordance_flag
   OR t.t_stage_discordance_flag
   OR (t.consult_precedence_flag AND t.confidence_rank > 1)
ORDER BY
    CASE WHEN t.histology_discordance_flag THEN 0 ELSE 1 END,
    t.research_id
"""

# ---------------------------------------------------------------------------
# 2. Molecular linkage review
# ---------------------------------------------------------------------------
MOLECULAR_LINKAGE_REVIEW_SQL = """
CREATE OR REPLACE TABLE molecular_linkage_review_v2 AS
WITH mol AS (
    SELECT
        m.research_id,
        m.molecular_episode_id,
        m.platform,
        m.overall_result_class,
        m.test_date_native,
        m.linked_fna_episode_id,
        m.linked_surgery_episode_id,
        m.braf_flag,
        m.ras_flag,
        m.ret_flag,
        m.tert_flag,
        m.high_risk_marker_flag,
        m.inadequate_flag,
        m.cancelled_flag,
        m.date_status
    FROM molecular_test_episode_v2 m
),
issues AS (
    SELECT
        mol.*,
        CASE
            WHEN mol.linked_fna_episode_id IS NULL
                 AND mol.linked_surgery_episode_id IS NULL
                 THEN 'unlinked_test'
            WHEN mol.inadequate_flag THEN 'inadequate_specimen'
            WHEN mol.cancelled_flag THEN 'cancelled_test'
            WHEN mol.date_status = 'unresolved_date' THEN 'missing_date'
            WHEN mol.linked_surgery_episode_id IS NOT NULL
                 AND mol.test_date_native IS NOT NULL
                 AND EXISTS (
                     SELECT 1 FROM operative_episode_detail_v2 o
                     WHERE o.research_id = mol.research_id
                       AND CAST(o.surgery_episode_id AS VARCHAR) = mol.linked_surgery_episode_id
                       AND o.surgery_date_native < mol.test_date_native
                 ) THEN 'post_surgery_test'
            ELSE NULL
        END AS review_reason
    FROM mol
)
SELECT
    i.*,
    CASE
        WHEN i.review_reason = 'unlinked_test' THEN 'warning'
        WHEN i.review_reason = 'post_surgery_test' THEN 'warning'
        WHEN i.review_reason = 'inadequate_specimen' THEN 'info'
        WHEN i.review_reason = 'cancelled_test' THEN 'info'
        WHEN i.review_reason = 'missing_date' THEN 'warning'
        ELSE 'ok'
    END AS review_severity,
    'pending' AS review_status
FROM issues i
WHERE i.review_reason IS NOT NULL
ORDER BY
    CASE i.review_reason
        WHEN 'unlinked_test' THEN 0
        WHEN 'post_surgery_test' THEN 1
        WHEN 'missing_date' THEN 2
        ELSE 3
    END,
    i.research_id
"""

# ---------------------------------------------------------------------------
# 3. RAI adjudication review
# ---------------------------------------------------------------------------
RAI_ADJUDICATION_REVIEW_SQL = """
CREATE OR REPLACE TABLE rai_adjudication_review_v2 AS
WITH rai AS (
    SELECT
        r.research_id,
        r.rai_episode_id,
        r.resolved_rai_date,
        r.dose_mci,
        r.rai_assertion_status,
        r.rai_intent,
        r.completion_status,
        r.date_status,
        r.linked_surgery_episode_id
    FROM rai_treatment_episode_v2 r
),
issues AS (
    SELECT
        rai.*,
        CASE
            WHEN rai.rai_assertion_status = 'ambiguous' THEN 'ambiguous_assertion'
            WHEN rai.completion_status = 'recommended'
                 AND NOT EXISTS (
                     SELECT 1 FROM rai_treatment_episode_v2 r2
                     WHERE r2.research_id = rai.research_id
                       AND r2.rai_episode_id != rai.rai_episode_id
                       AND r2.completion_status = 'completed'
                 ) THEN 'recommended_no_completion'
            WHEN rai.dose_mci IS NOT NULL AND rai.dose_mci > 250
                 THEN 'high_dose_review'
            WHEN rai.dose_mci IS NOT NULL AND rai.dose_mci < 10
                 THEN 'implausible_low_dose'
            WHEN rai.resolved_rai_date IS NOT NULL
                 AND rai.linked_surgery_episode_id IS NOT NULL
                 AND EXISTS (
                     SELECT 1 FROM operative_episode_detail_v2 o
                     WHERE o.research_id = rai.research_id
                       AND CAST(o.surgery_episode_id AS VARCHAR) = rai.linked_surgery_episode_id
                       AND o.surgery_date_native > rai.resolved_rai_date
                       AND rai.rai_assertion_status NOT IN ('historical', 'negated')
                 ) THEN 'rai_before_surgery'
            WHEN rai.date_status = 'unresolved_date' THEN 'missing_date'
            ELSE NULL
        END AS review_reason
    FROM rai
)
SELECT
    i.*,
    CASE
        WHEN i.review_reason = 'rai_before_surgery' THEN 'error'
        WHEN i.review_reason = 'implausible_low_dose' THEN 'error'
        WHEN i.review_reason = 'high_dose_review' THEN 'warning'
        WHEN i.review_reason = 'ambiguous_assertion' THEN 'warning'
        WHEN i.review_reason = 'recommended_no_completion' THEN 'info'
        WHEN i.review_reason = 'missing_date' THEN 'warning'
        ELSE 'ok'
    END AS review_severity,
    'pending' AS review_status
FROM issues i
WHERE i.review_reason IS NOT NULL
ORDER BY
    CASE i.review_reason
        WHEN 'rai_before_surgery' THEN 0
        WHEN 'implausible_low_dose' THEN 1
        WHEN 'high_dose_review' THEN 2
        ELSE 3
    END,
    i.research_id
"""

# ---------------------------------------------------------------------------
# 4. Imaging-pathology concordance review
# ---------------------------------------------------------------------------
IMAGING_PATH_CONCORDANCE_SQL = """
CREATE OR REPLACE TABLE imaging_pathology_concordance_review_v2 AS
WITH img AS (
    SELECT research_id, nodule_id, laterality AS img_lat,
           size_cm_max AS img_size, modality, exam_date_native
    FROM imaging_nodule_long_v2
    WHERE size_cm_max IS NOT NULL
),
tumor AS (
    SELECT research_id, surgery_episode_id, laterality AS path_lat,
           tumor_size_cm AS path_size, surgery_date
    FROM tumor_episode_master_v2
    WHERE tumor_size_cm IS NOT NULL
),
compared AS (
    SELECT
        img.research_id,
        img.nodule_id,
        tumor.surgery_episode_id,
        img.modality,
        img.img_lat,
        tumor.path_lat,
        img.img_size,
        tumor.path_size,
        img.exam_date_native,
        tumor.surgery_date,
        CASE
            WHEN img.img_lat IS NOT NULL AND tumor.path_lat IS NOT NULL
                 AND img.img_lat != tumor.path_lat
                 THEN TRUE ELSE FALSE
        END AS laterality_mismatch,
        CASE
            WHEN ABS(img.img_size - tumor.path_size) > 1.0 THEN TRUE
            WHEN img.img_size > 0 AND tumor.path_size > 0
                 AND GREATEST(img.img_size, tumor.path_size)
                     / LEAST(img.img_size, tumor.path_size) > 2.0
                 THEN TRUE
            ELSE FALSE
        END AS size_mismatch,
        ABS(img.img_size - tumor.path_size) AS size_diff_cm
    FROM img
    JOIN tumor ON img.research_id = tumor.research_id
)
SELECT
    c.*,
    CASE
        WHEN c.laterality_mismatch AND c.size_mismatch THEN 'laterality_and_size'
        WHEN c.laterality_mismatch THEN 'laterality_only'
        WHEN c.size_mismatch THEN 'size_only'
        ELSE 'concordant'
    END AS review_reason,
    CASE
        WHEN c.laterality_mismatch THEN 'error'
        WHEN c.size_mismatch THEN 'warning'
        ELSE 'ok'
    END AS review_severity,
    'pending' AS review_status
FROM compared c
WHERE c.laterality_mismatch OR c.size_mismatch
ORDER BY
    CASE WHEN c.laterality_mismatch THEN 0 ELSE 1 END,
    c.size_diff_cm DESC
"""

# ---------------------------------------------------------------------------
# 5. Operative-pathology reconciliation review
# ---------------------------------------------------------------------------
OP_PATH_RECON_SQL = """
CREATE OR REPLACE TABLE operative_pathology_reconciliation_review_v2 AS
WITH combined AS (
    SELECT
        o.research_id,
        o.surgery_episode_id,
        o.procedure_normalized,
        o.laterality AS op_lat,
        o.central_neck_dissection_flag AS op_cnd,
        o.lateral_neck_dissection_flag AS op_lnd,
        t.laterality AS path_lat,
        t.nodal_disease_positive_count,
        t.nodal_disease_total_count,
        t.primary_histology,
        CASE
            WHEN o.procedure_normalized IN ('total_thyroidectomy', 'completion_thyroidectomy')
                 AND t.laterality IS NOT NULL
                 AND t.laterality NOT IN ('bilateral', o.laterality)
                 AND o.laterality IS NOT NULL
                 THEN TRUE ELSE FALSE
        END AS laterality_mismatch,
        CASE
            WHEN o.central_neck_dissection_flag
                 AND (t.nodal_disease_total_count IS NULL OR t.nodal_disease_total_count = 0)
                 THEN TRUE ELSE FALSE
        END AS cnd_no_nodes,
        CASE
            WHEN NOT o.central_neck_dissection_flag
                 AND t.nodal_disease_total_count IS NOT NULL
                 AND t.nodal_disease_total_count > 0
                 THEN TRUE ELSE FALSE
        END AS nodes_no_cnd,
        CASE
            WHEN o.procedure_normalized = 'hemithyroidectomy'
                 AND t.multifocality_flag
                 AND t.laterality = 'bilateral'
                 THEN TRUE ELSE FALSE
        END AS bilateral_disease_lobectomy
    FROM operative_episode_detail_v2 o
    LEFT JOIN tumor_episode_master_v2 t
        ON o.research_id = t.research_id
        AND (o.surgery_date_native = t.surgery_date
             OR o.surgery_date_native IS NULL
             OR t.surgery_date IS NULL)
)
SELECT
    c.*,
    CASE
        WHEN c.laterality_mismatch THEN 'op_path_laterality'
        WHEN c.cnd_no_nodes THEN 'cnd_no_nodal_path'
        WHEN c.nodes_no_cnd THEN 'nodal_path_no_cnd'
        WHEN c.bilateral_disease_lobectomy THEN 'bilateral_in_lobectomy'
        ELSE 'concordant'
    END AS review_reason,
    CASE
        WHEN c.laterality_mismatch THEN 'error'
        WHEN c.bilateral_disease_lobectomy THEN 'warning'
        WHEN c.cnd_no_nodes OR c.nodes_no_cnd THEN 'warning'
        ELSE 'ok'
    END AS review_severity,
    'pending' AS review_status
FROM combined c
WHERE c.laterality_mismatch
   OR c.cnd_no_nodes
   OR c.nodes_no_cnd
   OR c.bilateral_disease_lobectomy
ORDER BY
    CASE WHEN c.laterality_mismatch THEN 0 ELSE 1 END,
    c.research_id
"""


ALL_RECON_SQL = [
    ("pathology_reconciliation_review_v2", PATHOLOGY_RECON_SQL),
    ("molecular_linkage_review_v2", MOLECULAR_LINKAGE_REVIEW_SQL),
    ("rai_adjudication_review_v2", RAI_ADJUDICATION_REVIEW_SQL),
    ("imaging_pathology_concordance_review_v2", IMAGING_PATH_CONCORDANCE_SQL),
    ("operative_pathology_reconciliation_review_v2", OP_PATH_RECON_SQL),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck")
    args = parser.parse_args()

    section("24 -- Reconciliation Review v2")

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            con = duckdb.connect(str(DB_PATH))
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Using local DuckDB: {DB_PATH}")

    for name, sql in ALL_RECON_SQL:
        section(name)
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN: {name} skipped -- {e}")

    section("Review Queue Summary")
    for name, _ in ALL_RECON_SQL:
        try:
            total = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            errors = con.execute(
                f"SELECT COUNT(*) FROM {name} WHERE review_severity = 'error'"
            ).fetchone()[0]
            warnings = con.execute(
                f"SELECT COUNT(*) FROM {name} WHERE review_severity = 'warning'"
            ).fetchone()[0]
            print(f"  {name:<50} total={total:>5,}  errors={errors:>5,}  warnings={warnings:>5,}")
        except Exception:
            print(f"  {name:<50} (not available)")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
