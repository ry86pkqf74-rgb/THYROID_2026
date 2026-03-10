#!/usr/bin/env python3
"""
26_motherduck_materialize_v2.py -- MotherDuck materialization of v2 tables

Materializes canonical episode tables, granular linkage tables,
reconciliation review views, QA outputs, Streamlit-critical views,
upstream heavy views, adjudication views, manual review queues,
and date rescue KPIs into MotherDuck for downstream Streamlit
consumption.  Skips any source view that does not yet exist.

Run after scripts 15-25, 27.
Supports --md flag (required for actual MotherDuck deployment).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

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


MATERIALIZATION_MAP: list[tuple[str, str]] = [
    # ── V2 canonical episode tables (script 22) ─────────────────────
    ("md_tumor_episode_master_v2", "tumor_episode_master_v2"),
    ("md_molecular_test_episode_v2", "molecular_test_episode_v2"),
    ("md_rai_treatment_episode_v2", "rai_treatment_episode_v2"),
    ("md_imaging_nodule_long_v2", "imaging_nodule_long_v2"),
    ("md_imaging_exam_summary_v2", "imaging_exam_summary_v2"),
    ("md_oper_episode_detail_v2", "operative_episode_detail_v2"),
    ("md_fna_episode_master_v2", "fna_episode_master_v2"),
    ("md_event_date_audit_v2", "event_date_audit_v2"),
    ("md_patient_cross_domain_timeline_v2", "patient_cross_domain_timeline_v2"),
    # ── V2 granular linkage tables (script 23) ──────────────────────
    ("md_imaging_fna_linkage_v2", "imaging_fna_linkage_v2"),
    ("md_fna_molecular_linkage_v2", "fna_molecular_linkage_v2"),
    ("md_preop_surgery_linkage_v2", "preop_surgery_linkage_v2"),
    ("md_surgery_pathology_linkage_v2", "surgery_pathology_linkage_v2"),
    ("md_pathology_rai_linkage_v2", "pathology_rai_linkage_v2"),
    ("md_linkage_summary_v2", "linkage_summary_v2"),
    # ── V2 QA tables (script 25) ───────────────────────────────────
    ("md_date_quality_summary_v2", "qa_date_completeness_v2"),
    ("md_qa_issues_v2", "qa_issues_v2"),
    ("md_qa_summary_v2", "qa_summary_by_domain_v2"),
    ("md_qa_high_priority_v2", "qa_high_priority_review_v2"),
    # ── V2 reconciliation review views (script 24) ─────────────────
    ("md_pathology_recon_review_v2", "pathology_reconciliation_review_v2"),
    ("md_molecular_linkage_review_v2", "molecular_linkage_review_v2"),
    ("md_rai_adjudication_review_v2", "rai_adjudication_review_v2"),
    ("md_imaging_path_concordance_v2", "imaging_pathology_concordance_review_v2"),
    ("md_op_path_recon_review_v2", "operative_pathology_reconciliation_review_v2"),
    # ── P0: Streamlit-critical views (script 18) ───────────────────
    ("md_streamlit_cohort_qc_summary", "streamlit_cohort_qc_summary_v"),
    ("md_patient_reconciliation_summary", "patient_reconciliation_summary_v"),
    ("md_streamlit_patient_header", "streamlit_patient_header_v"),
    ("md_streamlit_patient_timeline", "streamlit_patient_timeline_v"),
    ("md_streamlit_patient_conflicts", "streamlit_patient_conflicts_v"),
    ("md_streamlit_patient_manual_review", "streamlit_patient_manual_review_v"),
    # ── P1: Upstream heavy views (scripts 16-18) ───────────────────
    ("md_histology_reconciliation_v2", "histology_reconciliation_v2"),
    ("md_validation_failures_v3", "validation_failures_v3"),
    ("md_molecular_episode_v2", "molecular_episode_v2"),
    ("md_rai_episode_v2", "rai_episode_v2"),
    ("md_patient_master_timeline_v2", "patient_master_timeline_v2"),
    ("md_patient_manual_review_summary", "patient_manual_review_summary_v"),
    # ── P1: Adjudication views (scripts 18-19) ─────────────────────
    ("md_histology_analysis_cohort", "histology_analysis_cohort_v"),
    ("md_molecular_episode_v3", "molecular_episode_v3"),
    ("md_rai_episode_v3", "rai_episode_v3"),
    ("md_adjudication_progress_summary", "adjudication_progress_summary_v"),
    # ── P1: Manual review queues (script 18) ───────────────────────
    ("md_histology_manual_review_queue", "histology_manual_review_queue_v"),
    ("md_molecular_manual_review_queue", "molecular_manual_review_queue_v"),
    ("md_rai_manual_review_queue", "rai_manual_review_queue_v"),
    ("md_timeline_manual_review_queue", "timeline_manual_review_queue_v"),
    ("md_unresolved_high_value_cases", "unresolved_high_value_cases_v"),
    # ── Date rescue KPI (script 27) ────────────────────────────────
    ("md_date_rescue_rate_summary", "date_rescue_rate_summary"),
]

MANUAL_REVIEW_QUEUE_SUMMARY_SQL = """
CREATE OR REPLACE TABLE md_manual_review_queue_summary_v2 AS
SELECT 'pathology' AS domain,
       COUNT(*) AS total_issues,
       COUNT(*) FILTER (WHERE review_severity = 'error') AS error_count,
       COUNT(*) FILTER (WHERE review_severity = 'warning') AS warning_count,
       COUNT(DISTINCT research_id) AS patients_affected
FROM pathology_reconciliation_review_v2
UNION ALL
SELECT 'molecular',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM molecular_linkage_review_v2
UNION ALL
SELECT 'rai',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM rai_adjudication_review_v2
UNION ALL
SELECT 'imaging_pathology',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM imaging_pathology_concordance_review_v2
UNION ALL
SELECT 'operative_pathology',
       COUNT(*), COUNT(*) FILTER (WHERE review_severity = 'error'),
       COUNT(*) FILTER (WHERE review_severity = 'warning'),
       COUNT(DISTINCT research_id)
FROM operative_pathology_reconciliation_review_v2
"""


def materialize_all(
    source_con: duckdb.DuckDBPyConnection,
    target_con: duckdb.DuckDBPyConnection,
    same_connection: bool = False,
) -> None:
    """Materialize all v2 tables from source to target."""

    section("Materializing v2 tables")

    if same_connection:
        for md_name, src_name in MATERIALIZATION_MAP:
            if not table_available(source_con, src_name):
                print(f"  SKIP {src_name:<50} (not found in source)")
                continue
            try:
                source_con.execute(
                    f"CREATE OR REPLACE TABLE {md_name} AS "
                    f"SELECT * FROM {src_name}"
                )
                cnt = source_con.execute(
                    f"SELECT COUNT(*) FROM {md_name}"
                ).fetchone()[0]
                print(f"  OK   {md_name:<50} {cnt:>8,} rows")
            except Exception as e:
                print(f"  WARN {md_name:<50} {e}")

        # Manual review queue summary
        try:
            source_con.execute(MANUAL_REVIEW_QUEUE_SUMMARY_SQL)
            cnt = source_con.execute(
                "SELECT COUNT(*) FROM md_manual_review_queue_summary_v2"
            ).fetchone()[0]
            print(f"  OK   {'md_manual_review_queue_summary_v2':<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN md_manual_review_queue_summary_v2: {e}")

    else:
        for md_name, src_name in MATERIALIZATION_MAP:
            if not table_available(source_con, src_name):
                print(f"  SKIP {src_name:<50} (not found in source)")
                continue
            try:
                df = source_con.execute(f"SELECT * FROM {src_name}").fetchdf()
                target_con.execute(f"DROP TABLE IF EXISTS {md_name}")
                target_con.register("_tmp_df", df)
                target_con.execute(
                    f"CREATE TABLE {md_name} AS SELECT * FROM _tmp_df"
                )
                target_con.unregister("_tmp_df")
                print(f"  OK   {md_name:<50} {len(df):>8,} rows")
            except Exception as e:
                print(f"  WARN {md_name:<50} {e}")

        # Manual review queue summary (build from materialized tables)
        try:
            target_con.execute(
                MANUAL_REVIEW_QUEUE_SUMMARY_SQL.replace(
                    "pathology_reconciliation_review_v2",
                    "md_pathology_recon_review_v2",
                ).replace(
                    "molecular_linkage_review_v2",
                    "md_molecular_linkage_review_v2",
                ).replace(
                    "rai_adjudication_review_v2",
                    "md_rai_adjudication_review_v2",
                ).replace(
                    "imaging_pathology_concordance_review_v2",
                    "md_imaging_path_concordance_v2",
                ).replace(
                    "operative_pathology_reconciliation_review_v2",
                    "md_op_path_recon_review_v2",
                )
            )
            cnt = target_con.execute(
                "SELECT COUNT(*) FROM md_manual_review_queue_summary_v2"
            ).fetchone()[0]
            print(f"  OK   {'md_manual_review_queue_summary_v2':<50} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN md_manual_review_queue_summary_v2: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck (required for production)")
    args = parser.parse_args()

    section("26 -- MotherDuck Materialization v2")

    local_con = duckdb.connect(str(DB_PATH))
    print(f"  Source: {DB_PATH}")

    required = [
        "tumor_episode_master_v2", "molecular_test_episode_v2",
        "rai_treatment_episode_v2", "imaging_nodule_long_v2",
        "operative_episode_detail_v2",
    ]
    missing = [t for t in required if not table_available(local_con, t)]
    if missing:
        print(f"\n  ERROR: Missing required tables: {missing}")
        print("  Run scripts 22-25 first.")
        local_con.close()
        sys.exit(1)

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            md_con = client.connect_rw()
            print("  Target: MotherDuck (RW)")
            materialize_all(local_con, md_con, same_connection=False)
            md_con.close()
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            print("  Materializing locally instead")
            materialize_all(local_con, local_con, same_connection=True)
    else:
        print("  Target: local DuckDB (use --md for MotherDuck)")
        materialize_all(local_con, local_con, same_connection=True)

    section("Materialization Summary")
    for md_name, _ in MATERIALIZATION_MAP:
        try:
            cnt = local_con.execute(
                f"SELECT COUNT(*) FROM {md_name}"
            ).fetchone()[0]
            print(f"  {md_name:<50} {cnt:>8,} rows")
        except Exception:
            pass

    local_con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
