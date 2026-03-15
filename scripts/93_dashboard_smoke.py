#!/usr/bin/env python3
"""
93_dashboard_smoke.py  —  Dashboard critical-path smoke test suite

Verifies that every major dashboard section can retrieve its data from
MotherDuck without errors and within acceptable row-count thresholds.

Covers all 6 workflow sections:
  1. Overview
  2. Patient Explorer
  3. Data Quality  (QA Workbench, Validation Engine)
  4. Linkage & Episodes
  5. Outcomes & Analytics  (Survival, Scoring, Complications)
  6. Manuscript & Export

Usage
─────
  # Standard smoke test against prod share (RO — mirrors production)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/93_dashboard_smoke.py --share

  # Test against RW prod database (pre-deployment check)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/93_dashboard_smoke.py --md

  # Test against QA database
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/93_dashboard_smoke.py --md --env qa

Exit codes
──────────
  0  All checks PASS
  1  One or more checks FAIL
  2  Connectivity error
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


@dataclass
class SmokeResult:
    section: str
    label: str
    status: str   # PASS | FAIL | WARN
    detail: str = ""
    elapsed_ms: float = 0.0


# ── Smoke checks ──────────────────────────────────────────────────────────
# (section, label, sql, min_rows, max_ms)
SMOKE_CHECKS: list[tuple[str, str, str, int, int]] = [
    # ── Section 1: Overview ──────────────────────────────────────────────
    ("Overview", "master_cohort_count",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort", 10500, 2000),
    ("Overview", "date_rescue_kpi",
     "SELECT COUNT(*) FROM date_rescue_rate_summary", 1, 2000),
    ("Overview", "scoring_stage_dist",
     "SELECT COUNT(*) FROM thyroid_scoring_py_v1 WHERE ajcc_stage_group IS NOT NULL", 1000, 2000),

    # ── Section 2: Patient Explorer ──────────────────────────────────────
    ("Patient Explorer", "patient_header_accessible",
     "SELECT COUNT(*) FROM streamlit_patient_header_v", 10000, 3000),
    ("Patient Explorer", "patient_timeline_accessible",
     "SELECT COUNT(*) FROM streamlit_patient_timeline_v LIMIT 1", 1, 3000),
    ("Patient Explorer", "patient_conflicts_accessible",
     "SELECT COUNT(*) FROM streamlit_patient_conflicts_v LIMIT 1", 0, 3000),

    # ── Section 3: Data Quality ──────────────────────────────────────────
    ("Data Quality", "val_integrity_summary",
     "SELECT COUNT(*) FROM val_dataset_integrity_summary_v1", 1, 3000),
    ("Data Quality", "val_provenance_completeness",
     "SELECT COUNT(*) FROM val_provenance_completeness_v2", 1, 3000),
    ("Data Quality", "val_episode_linkage",
     "SELECT COUNT(*) FROM val_episode_linkage_completeness_v1", 1, 3000),
    ("Data Quality", "hardening_summary",
     "SELECT COUNT(*) FROM val_hardening_summary", 1, 3000),
    ("Data Quality", "qa_issues_v2_count",
     "SELECT COUNT(*) FROM qa_issues_v2", 0, 5000),

    # ── Section 4: Linkage & Episodes ───────────────────────────────────
    ("Linkage", "linkage_summary_v3",
     "SELECT COUNT(*) FROM linkage_summary_v3", 3, 3000),
    ("Linkage", "imaging_exam_master",
     "SELECT COUNT(*) FROM imaging_exam_master_v1", 100, 5000),
    ("Linkage", "fna_episode_count",
     "SELECT COUNT(*) FROM fna_episode_master_v2", 100, 5000),
    ("Linkage", "operative_episode_count",
     "SELECT COUNT(*) FROM operative_episode_detail_v2", 8000, 5000),

    # ── Section 5: Outcomes & Analytics ─────────────────────────────────
    ("Outcomes", "survival_cohort",
     "SELECT COUNT(*) FROM survival_cohort_enriched", 40000, 5000),
    ("Outcomes", "cancer_cohort",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1", 3900, 3000),
    ("Outcomes", "complication_phenotype",
     "SELECT COUNT(*) FROM complication_patient_summary_v1", 1000, 5000),
    ("Outcomes", "lab_canonical_count",
     "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1", 30000, 3000),
    ("Outcomes", "recurrence_event_count",
     "SELECT COUNT(*) FROM recurrence_event_clean_v1", 100, 5000),
    ("Outcomes", "cure_kpis",
     "SELECT n_total FROM cure_kpis LIMIT 1", 1, 2000),

    # ── Section 6: Manuscript & Export ──────────────────────────────────
    ("Manuscript", "manuscript_cohort_v1",
     "SELECT COUNT(*) FROM manuscript_cohort_v1", 10500, 3000),
    ("Manuscript", "manuscript_tables",
     "SELECT COUNT(*) FROM manuscript_tables_v3_mv", 1, 5000),
    ("Manuscript", "episode_dedup",
     "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup", 9000, 3000),
    ("Manuscript", "scoring_patients",
     "SELECT COUNT(*) FROM thyroid_scoring_py_v1", 10500, 3000),
]

# Column-presence spot checks: (table, [required_cols])
COLUMN_CHECKS: list[tuple[str, list[str]]] = [
    ("manuscript_cohort_v1", ["research_id", "demo_sex_final", "demo_race_final",
                               "ajcc_t_stage", "braf_positive_final"]),
    ("thyroid_scoring_py_v1", ["research_id", "ajcc_t_stage", "ajcc_n_stage",
                                "ajcc_stage_group", "ata_initial_risk"]),
    ("survival_cohort_enriched", ["research_id", "time_days", "event",
                                   "ajcc_stage_8", "ete_type"]),
    ("patient_analysis_resolved_v1", ["research_id", "analysis_eligible_flag",
                                       "molecular_eligible_flag"]),
]


def run_smoke(con, checks: list[tuple]) -> list[SmokeResult]:
    results: list[SmokeResult] = []
    current_section = ""

    for section, label, sql, min_rows, max_ms in checks:
        if section != current_section:
            print(f"\n  ── {section}")
            current_section = section

        try:
            t0 = time.perf_counter()
            row = con.execute(sql).fetchone()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            val = row[0] if row else 0
            val_int = int(val) if val is not None else 0

            if val_int < min_rows:
                status = "FAIL"
                detail = f"{val_int:,} rows < minimum {min_rows:,}"
            elif elapsed_ms > max_ms:
                status = "WARN"
                detail = f"{val_int:,} rows in {elapsed_ms:.0f}ms (SLA {max_ms}ms)"
            else:
                status = "PASS"
                detail = f"{val_int:,} rows in {elapsed_ms:.0f}ms"
        except Exception as e:
            elapsed_ms = 0.0
            status = "FAIL"
            detail = f"ERROR: {e!s:.100}"

        icon = "✓" if status == "PASS" else ("⚠" if status == "WARN" else "✗")
        print(f"    {icon} {label:<45} {detail}")
        results.append(SmokeResult(section=section, label=label, status=status,
                                    detail=detail, elapsed_ms=elapsed_ms))
    return results


def run_column_checks(con) -> list[SmokeResult]:
    results: list[SmokeResult] = []
    print(f"\n  ── Column presence spot checks")
    for tbl, required_cols in COLUMN_CHECKS:
        try:
            actual_cols = {
                r[0] for r in con.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name='{tbl}' AND table_schema='main'"
                ).fetchall()
            }
            missing = [c for c in required_cols if c not in actual_cols]
            status = "FAIL" if missing else "PASS"
            detail = f"Missing: {missing}" if missing else f"All {len(required_cols)} columns present"
        except Exception as e:
            status = "FAIL"
            detail = f"ERROR: {e!s:.80}"
        icon = "✓" if status == "PASS" else "✗"
        print(f"    {icon} {tbl:<45} {detail}")
        results.append(SmokeResult(section="Column checks", label=tbl, status=status, detail=detail))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--share", action="store_true",
                       help="Test via RO share (mirrors production dashboard path)")
    group.add_argument("--md",   action="store_true",
                       help="Test via RW prod database")
    parser.add_argument("--env", default="prod", choices=["dev", "qa", "prod"])
    parser.add_argument("--sa",  action="store_true", help="Use service-account token")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  THYROID_2026  —  Dashboard Smoke Tests")
    mode = "RO share" if args.share else f"RW ({args.env})"
    print(f"  Mode: {mode}  |  {len(SMOKE_CHECKS)} queries / {len(COLUMN_CHECKS)} column checks")
    print(f"{'='*70}")

    try:
        client = MotherDuckClient.for_env(args.env, use_service_account=args.sa)
        if args.share:
            con = client.connect_ro_share()
            # Set catalog so plain table names resolve correctly
            try:
                con.execute("USE thyroid_share;")
            except Exception:
                pass
            print(f"  Connected: RO share (thyroid_share)\n")
        else:
            con = client.connect_rw()
            print(f"  Connected: {client.config.database} (RW)\n")
    except Exception as e:
        print(f"  ERROR: Cannot connect — {e}")
        sys.exit(2)

    t0 = time.time()
    results = run_smoke(con, SMOKE_CHECKS)
    results += run_column_checks(con)
    total_elapsed = time.time() - t0
    con.close()

    n_pass = sum(1 for r in results if r.status == "PASS")
    n_warn = sum(1 for r in results if r.status == "WARN")
    n_fail = sum(1 for r in results if r.status == "FAIL")

    print(f"\n  {'─'*50}")
    print(f"  SUMMARY: {n_pass} PASS  |  {n_warn} WARN  |  {n_fail} FAIL")
    print(f"  Total time: {total_elapsed:.1f}s")

    if n_fail > 0:
        print(f"\n  ✗ SMOKE TESTS FAILED — dashboard may be degraded\n")
        sys.exit(1)
    elif n_warn > 0:
        print(f"\n  ⚠ SMOKE TESTS PASSED with {n_warn} SLA warnings\n")
        sys.exit(0)
    else:
        print(f"\n  ✓ ALL SMOKE TESTS PASS\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
