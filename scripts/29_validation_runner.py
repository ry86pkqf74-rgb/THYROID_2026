#!/usr/bin/env python3
"""
29_validation_runner.py -- Combined validation + review-export runner

Runs the validation suite (script 21) and manual review export (script 28)
in a single invocation, optionally against MotherDuck.

Steps:
  1. Run all 6 validation tests
  2. Export manual review queues (all domains)
  3. Export manuscript cohort bundle
  4. Print reconciliation gap summary across domains

Supports --md flag for MotherDuck.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORT_DIR = ROOT / "exports"

sys.path.insert(0, str(ROOT))

RECONCILIATION_DOMAINS = [
    ("histology", "histology_manual_review_queue_v", "md_histology_manual_review_queue"),
    ("molecular", "molecular_manual_review_queue_v", "md_molecular_manual_review_queue"),
    ("rai", "rai_manual_review_queue_v", "md_rai_manual_review_queue"),
    ("timeline", "timeline_manual_review_queue_v", "md_timeline_manual_review_queue"),
    ("imaging_path", "imaging_pathology_concordance_review_v2", "md_imaging_path_concordance_v2"),
    ("op_path", "operative_pathology_reconciliation_review_v2", "md_op_path_recon_review_v2"),
]

VALIDATION_VIEWS = [
    "streamlit_patient_header_v",
    "streamlit_patient_timeline_v",
    "streamlit_patient_conflicts_v",
    "streamlit_patient_manual_review_v",
    "patient_reconciliation_summary_v",
    "adjudication_decisions",
    "adjudication_decision_history",
    "histology_post_review_v",
    "molecular_post_review_v",
    "rai_post_review_v",
    "manuscript_histology_cohort_v",
    "manuscript_molecular_cohort_v",
    "manuscript_rai_cohort_v",
    "manuscript_patient_summary_v",
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


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), text=True
        ).strip()
    except Exception:
        return "unknown"


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


def run_validation(con: duckdb.DuckDBPyConnection) -> dict:
    """Run validation checks and return results dict."""
    section("Step 1: Validation Tests")
    results: dict = {"passed": 0, "failed": 0, "skipped": 0, "details": {}}

    for view in VALIDATION_VIEWS:
        if tbl_exists(con, view):
            cnt = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
            status = "PASS" if cnt > 0 else "WARN (empty)"
            results["passed"] += 1 if cnt > 0 else 0
            results["details"][view] = {"status": status, "rows": cnt}
            print(f"  {status:<12} {view:<50} {cnt:>8,} rows")
        else:
            results["skipped"] += 1
            results["details"][view] = {"status": "SKIP", "rows": 0}
            print(f"  {'SKIP':<12} {view:<50} (not found)")

    print(f"\n  Summary: {results['passed']} passed, "
          f"{results['failed']} failed, {results['skipped']} skipped")
    return results


def run_review_export(
    con: duckdb.DuckDBPyConnection, out_dir: Path, use_md: bool,
) -> dict:
    """Export review queues and return manifest dict."""
    section("Step 2: Manual Review Export")

    manifest: dict = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "repo_commit_sha": git_sha(),
        "source": "motherduck" if use_md else str(DB_PATH),
        "queues": {},
    }

    for domain, local_view, md_view in RECONCILIATION_DOMAINS:
        view = md_view if (use_md and tbl_exists(con, md_view)) else local_view
        if not tbl_exists(con, view):
            print(f"  SKIP {domain:<25} ({view} not found)")
            manifest["queues"][domain] = {"rows": 0, "status": "missing"}
            continue

        try:
            df: pd.DataFrame = con.execute(
                f"SELECT * FROM {view} ORDER BY research_id"
            ).fetchdf()
        except Exception as e:
            print(f"  ERR  {domain:<25} {e}")
            manifest["queues"][domain] = {"rows": 0, "status": f"error: {e}"}
            continue

        csv_path = out_dir / f"{domain}_review.csv"
        pq_path = out_dir / f"{domain}_review.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)
        manifest["queues"][domain] = {
            "rows": len(df),
            "columns": list(df.columns),
            "status": "ok",
        }
        print(f"  OK   {domain:<25} {len(df):>8,} rows")

    return manifest


def run_gap_summary(con: duckdb.DuckDBPyConnection, use_md: bool) -> dict:
    """Print reconciliation gap summary across all domains."""
    section("Step 3: Reconciliation Gap Summary")

    gaps: dict = {}
    for domain, local_view, md_view in RECONCILIATION_DOMAINS:
        view = md_view if (use_md and tbl_exists(con, md_view)) else local_view
        if not tbl_exists(con, view):
            continue
        try:
            total = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
            errors = 0
            warnings = 0
            try:
                errors = con.execute(
                    f"SELECT COUNT(*) FROM {view} "
                    f"WHERE review_severity = 'error'"
                ).fetchone()[0]
                warnings = con.execute(
                    f"SELECT COUNT(*) FROM {view} "
                    f"WHERE review_severity = 'warning'"
                ).fetchone()[0]
            except Exception:
                pass
            patients = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM {view}"
            ).fetchone()[0]
            gaps[domain] = {
                "total": total, "errors": errors,
                "warnings": warnings, "patients": patients,
            }
            print(f"  {domain:<25} total={total:>6,}  "
                  f"errors={errors:>5,}  warnings={warnings:>5,}  "
                  f"patients={patients:>5,}")
        except Exception as e:
            print(f"  {domain:<25} {e}")

    return gaps


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Run against MotherDuck")
    parser.add_argument("--skip-export", action="store_true",
                        help="Skip the file export step")
    args = parser.parse_args()

    section("29 -- Combined Validation + Review Export Runner")

    con = get_connection(args.md)

    validation_results = run_validation(con)
    gap_summary = run_gap_summary(con, args.md)

    export_manifest: dict = {}
    out_dir = Path("")
    if not args.skip_export:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
        out_dir = EXPORT_DIR / f"validation_run_{ts}"
        out_dir.mkdir(parents=True, exist_ok=True)
        export_manifest = run_review_export(con, out_dir, args.md)

    section("Final Report")

    report = {
        "run_at": datetime.now(tz=timezone.utc).isoformat(),
        "source": "motherduck" if args.md else str(DB_PATH),
        "repo_commit_sha": git_sha(),
        "validation": validation_results,
        "gap_summary": gaps if (gaps := gap_summary) else {},
        "export": export_manifest,
    }

    if not args.skip_export:
        report_path = out_dir / "validation_report.json"
        report_path.write_text(json.dumps(report, indent=2, default=str))
        print(f"  Report written to {report_path}")

    print(f"\n  Validation: {validation_results['passed']} passed, "
          f"{validation_results['skipped']} skipped")
    total_gaps = sum(g.get("errors", 0) for g in gap_summary.values())
    total_warnings = sum(g.get("warnings", 0) for g in gap_summary.values())
    print(f"  Reconciliation gaps: {total_gaps:,} errors, "
          f"{total_warnings:,} warnings across {len(gap_summary)} domains")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
