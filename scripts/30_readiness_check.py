#!/usr/bin/env python3
"""
30_readiness_check.py -- MotherDuck / local DuckDB readiness report

Checks the presence and row counts of all critical v3 tables and views,
prints a readiness report, and exits non-zero if any critical tables are
missing.

Supports --md flag for MotherDuck.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

CRITICAL_TABLES = [
    ("molecular_episode_v3", "v3 adjudication"),
    ("rai_episode_v3", "v3 adjudication"),
    ("validation_failures_v3", "v3 validation"),
    ("adjudication_decisions", "reviewer persistence"),
    ("tumor_episode_master_v2", "v2 canonical"),
    ("molecular_test_episode_v2", "v2 canonical"),
    ("rai_treatment_episode_v2", "v2 canonical"),
    ("imaging_nodule_long_v2", "v2 canonical"),
    ("operative_episode_detail_v2", "v2 canonical"),
    ("fna_episode_master_v2", "v2 canonical"),
    ("linkage_summary_v2", "cross-domain linkage"),
    ("qa_issues_v2", "QA validation"),
    ("date_rescue_rate_summary", "date provenance"),
    ("patient_reconciliation_summary_v", "patient spine"),
]

OPTIONAL_TABLES = [
    ("histology_reconciliation_v2", "upstream"),
    ("patient_master_timeline_v2", "upstream"),
    ("patient_validation_rollup_v2_mv", "validation rollup"),
    ("histology_analysis_cohort_v", "adjudication"),
    ("molecular_analysis_cohort_v", "adjudication"),
    ("rai_analysis_cohort_v", "adjudication"),
    ("histology_post_review_v", "post-review"),
    ("molecular_post_review_v", "post-review"),
    ("rai_post_review_v", "post-review"),
    ("manuscript_histology_cohort_v", "manuscript"),
    ("manuscript_molecular_cohort_v", "manuscript"),
    ("manuscript_rai_cohort_v", "manuscript"),
    ("manuscript_patient_summary_v", "manuscript"),
    ("streamlit_cohort_qc_summary_v", "streamlit"),
    ("streamlit_patient_header_v", "streamlit"),
    ("streamlit_patient_timeline_v", "streamlit"),
    ("streamlit_patient_conflicts_v", "streamlit"),
    ("streamlit_patient_manual_review_v", "streamlit"),
    ("imaging_fna_linkage_v2", "linkage"),
    ("fna_molecular_linkage_v2", "linkage"),
    ("preop_surgery_linkage_v2", "linkage"),
    ("surgery_pathology_linkage_v2", "linkage"),
    ("pathology_rai_linkage_v2", "linkage"),
    ("val_histology_confirmation", "validation engine"),
    ("val_molecular_confirmation", "validation engine"),
    ("val_rai_confirmation", "validation engine"),
    ("val_chronology_anomalies", "validation engine"),
    ("val_completeness_scorecard", "validation engine"),
    ("val_review_queue_combined", "validation engine"),
]


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def tbl_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def row_count(con: duckdb.DuckDBPyConnection, name: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        return 0


def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
            return con
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            print("  Falling back to local DuckDB")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    print(f"  Using local DuckDB: {DB_PATH}")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Check MotherDuck instead of local DuckDB")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    section("30 — Readiness Check")

    con = get_connection(args.md)

    report: dict = {
        "checked_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": "motherduck" if args.md else str(DB_PATH),
        "critical": {},
        "optional": {},
        "critical_missing": [],
        "ready": True,
    }

    section("Critical Tables")
    crit_ok = 0
    crit_miss = 0
    for tbl, category in CRITICAL_TABLES:
        exists = tbl_exists(con, tbl)
        cnt = row_count(con, tbl) if exists else 0
        status = "OK" if exists and cnt > 0 else ("EMPTY" if exists else "MISSING")
        report["critical"][tbl] = {"status": status, "rows": cnt, "category": category}
        icon = "OK  " if status == "OK" else "MISS"
        print(f"  {icon} {tbl:<50} {cnt:>8,} rows  [{category}]")
        if status == "OK":
            crit_ok += 1
        else:
            crit_miss += 1
            report["critical_missing"].append(tbl)

    section("Optional Tables")
    opt_ok = 0
    opt_miss = 0
    for tbl, category in OPTIONAL_TABLES:
        exists = tbl_exists(con, tbl)
        cnt = row_count(con, tbl) if exists else 0
        status = "OK" if exists and cnt > 0 else ("EMPTY" if exists else "MISSING")
        report["optional"][tbl] = {"status": status, "rows": cnt, "category": category}
        icon = "OK  " if status == "OK" else "----"
        print(f"  {icon} {tbl:<50} {cnt:>8,} rows  [{category}]")
        if status == "OK":
            opt_ok += 1
        else:
            opt_miss += 1

    report["ready"] = crit_miss == 0

    section("Summary")
    print(f"  Critical: {crit_ok}/{len(CRITICAL_TABLES)} present")
    print(f"  Optional: {opt_ok}/{len(OPTIONAL_TABLES)} present")
    if report["critical_missing"]:
        print(f"\n  MISSING CRITICAL TABLES:")
        for t in report["critical_missing"]:
            print(f"    - {t}")
        print(f"\n  Run the following to fix:")
        print(f"    python scripts/22_canonical_episodes_v2.py [--md]")
        print(f"    python scripts/23_cross_domain_linkage_v2.py [--md]")
        print(f"    python scripts/26_motherduck_materialize_v2.py [--md]")
    else:
        print(f"\n  ALL CRITICAL TABLES PRESENT — system is ready.")

    if args.json:
        print(json.dumps(report, indent=2, default=str))

    con.close()
    sys.exit(0 if report["ready"] else 1)


if __name__ == "__main__":
    main()
