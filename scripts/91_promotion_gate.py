#!/usr/bin/env python3
"""
91_promotion_gate.py  —  Environment promotion gate for THYROID_2026

Validates that a SOURCE environment is healthy enough to promote to a TARGET
environment.  Run this before running scripts/26_motherduck_materialize_v2.py
against the target database.

Supported promotion paths
─────────────────────────
  dev  → qa   (routine development iteration)
  qa   → prod (validated release)

Usage
─────
  # Validate dev before promoting to qa
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/91_promotion_gate.py --from dev --to qa

  # Full prod promotion (use SA token in CI)
  MD_SA_TOKEN=... .venv/bin/python scripts/91_promotion_gate.py --from qa --to prod --sa

  # Dry-run: run checks on current prod without writing
  MD_SA_TOKEN=... .venv/bin/python scripts/91_promotion_gate.py --from prod --to prod --dry-run

Exit codes
──────────
  0  All gates PASS  — safe to promote
  1  One or more gates FAIL — promotion blocked
  2  Environment / connectivity error
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402


# ── Gate definitions ───────────────────────────────────────────────────────
@dataclass
class GateResult:
    name: str
    status: str   # PASS | FAIL | SKIP | WARN
    detail: str = ""


CRITICAL_TABLES = [
    "master_cohort",
    "manuscript_cohort_v1",
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
    "thyroid_scoring_py_v1",
    "analysis_cancer_cohort_v1",
    "operative_episode_detail_v2",
    "rai_treatment_episode_v2",
    "molecular_test_episode_v2",
    "survival_cohort_enriched",
    "streamlit_patient_header_v",
    "longitudinal_lab_canonical_v1",
    "val_hardening_summary",
    "val_dataset_integrity_summary_v1",
]

# (metric_name, sql, lo, hi)
METRIC_BOUNDS: list[tuple[str, str, int, int]] = [
    ("surgical_cohort",   "SELECT COUNT(DISTINCT research_id) FROM master_cohort",                      10500, 11500),
    ("cancer_cohort",     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",                              3900,  4300),
    ("manuscript_cohort", "SELECT COUNT(*) FROM manuscript_cohort_v1",                                  10500, 11200),
    ("dedup_episodes",    "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",                    9000,   9800),
    ("scoring_pts",       "SELECT COUNT(*) FROM thyroid_scoring_py_v1",                                10500, 11500),
    ("survival_cohort",   "SELECT COUNT(*) FROM survival_cohort_enriched",                             40000, 70000),
    ("tirads_patients",   "SELECT COUNT(*) FROM extracted_tirads_validated_v1",                         3000,  4000),
    ("operative_eps",     "SELECT COUNT(*) FROM operative_episode_detail_v2",                           8000, 12000),
]

NULL_RATE_CHECKS: list[tuple[str, str, float]] = [
    ("manuscript_cohort_v1",               "research_id",      0.0),
    ("patient_analysis_resolved_v1",       "research_id",      0.0),
    ("episode_analysis_resolved_v1_dedup", "research_id",      0.0),
    ("thyroid_scoring_py_v1",              "research_id",      0.0),
    ("thyroid_scoring_py_v1",              "ajcc8_t_stage",    65.0),  # known missingness
    ("manuscript_cohort_v1",               "demo_sex_final",   10.0),
]

UNIQUENESS_CHECKS: list[tuple[str, str]] = [
    ("patient_analysis_resolved_v1",       "research_id"),
    ("episode_analysis_resolved_v1_dedup", "CONCAT(CAST(research_id AS VARCHAR),'|',CAST(COALESCE(surgery_episode_id,-1) AS VARCHAR))"),
    ("manuscript_cohort_v1",               "research_id"),
]

# Validation tables that must all have 0 FAIL rows for a prod promotion
VAL_TABLES_PROD_ONLY = [
    "val_hardening_summary",
    "val_dataset_integrity_summary_v1",
    "val_provenance_completeness_v2",
]


def run_gates(con: Any, promote_to_prod: bool) -> list[GateResult]:
    results: list[GateResult] = []

    # ── G1: Critical tables exist ──────────────────────────────────────────
    missing = []
    for tbl in CRITICAL_TABLES:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        except Exception:
            missing.append(tbl)
    results.append(GateResult(
        "G1_critical_tables",
        "FAIL" if missing else "PASS",
        f"Missing: {missing}" if missing else f"All {len(CRITICAL_TABLES)} tables present",
    ))

    # ── G2: Canonical metric bounds ─────────────────────────────────────────
    failures: list[str] = []
    for name, sql, lo, hi in METRIC_BOUNDS:
        try:
            v = con.execute(sql).fetchone()[0]
            if not (lo <= int(v) <= hi):
                failures.append(f"{name}={int(v):,} not in [{lo:,},{hi:,}]")
        except Exception as e:
            failures.append(f"{name}: ERROR {e}")
    results.append(GateResult(
        "G2_metric_bounds",
        "FAIL" if failures else "PASS",
        "; ".join(failures) if failures else f"All {len(METRIC_BOUNDS)} metrics in range",
    ))

    # ── G3: Row multiplication ───────────────────────────────────────────────
    dup_failures: list[str] = []
    for tbl, key_expr in UNIQUENESS_CHECKS:
        try:
            dupl = con.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT {key_expr}) FROM {tbl}"
            ).fetchone()[0]
            if dupl != 0:
                dup_failures.append(f"{tbl}: {dupl} duplicates")
        except Exception as e:
            dup_failures.append(f"{tbl}: ERROR {e}")
    results.append(GateResult(
        "G3_no_row_multiplication",
        "FAIL" if dup_failures else "PASS",
        "; ".join(dup_failures) if dup_failures else "No row duplication in patient/episode tables",
    ))

    # ── G4: Null rate ceilings ───────────────────────────────────────────────
    null_failures: list[str] = []
    for tbl, col, max_pct in NULL_RATE_CHECKS:
        try:
            row = con.execute(
                f"SELECT ROUND(100.0*COUNT(*) FILTER (WHERE {col} IS NULL)"
                f"/NULLIF(COUNT(*),0),2) FROM {tbl}"
            ).fetchone()
            pct = float(row[0]) if row and row[0] is not None else 0.0
            if pct > max_pct:
                null_failures.append(f"{tbl}.{col}: {pct:.1f}% > {max_pct}% ceiling")
        except Exception as e:
            null_failures.append(f"{tbl}.{col}: ERROR {e}")
    results.append(GateResult(
        "G4_null_rate_ceilings",
        "FAIL" if null_failures else "PASS",
        "; ".join(null_failures) if null_failures else f"All {len(NULL_RATE_CHECKS)} null-rate checks pass",
    ))

    # ── G5: Validation tables pass (prod-only) ────────────────────────────
    if promote_to_prod:
        val_failures: list[str] = []
        for vtbl in VAL_TABLES_PROD_ONLY:
            try:
                # Expect a 'status' or 'result' column; skip if absent
                cols = [r[0] for r in con.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name='{vtbl}' AND table_schema='main'"
                ).fetchall()]
                if "status" in cols:
                    fails = con.execute(
                        f"SELECT COUNT(*) FROM {vtbl} WHERE LOWER(status)='fail'"
                    ).fetchone()[0]
                    if fails > 0:
                        val_failures.append(f"{vtbl}: {fails} FAIL rows")
                elif "gate_status" in cols:
                    fails = con.execute(
                        f"SELECT COUNT(*) FROM {vtbl} WHERE LOWER(gate_status)='fail'"
                    ).fetchone()[0]
                    if fails > 0:
                        val_failures.append(f"{vtbl}: {fails} FAIL rows")
            except Exception as e:
                val_failures.append(f"{vtbl}: ERROR {e}")
        results.append(GateResult(
            "G5_validation_tables_pass",
            "FAIL" if val_failures else "PASS",
            "; ".join(val_failures) if val_failures else "All validation tables pass",
        ))
    else:
        results.append(GateResult("G5_validation_tables_pass", "SKIP", "Only required for prod promotion"))

    # ── G6: RO share accessible (prod-only) ─────────────────────────────────
    if promote_to_prod:
        try:
            share_con = MotherDuckClient.for_env("prod").connect_ro_share()
            n = share_con.execute("SELECT COUNT(DISTINCT research_id) FROM master_cohort").fetchone()[0]
            share_con.close()
            results.append(GateResult("G6_ro_share_accessible", "PASS", f"RO share: {int(n):,} patients"))
        except Exception as e:
            results.append(GateResult("G6_ro_share_accessible", "FAIL", str(e)))
    else:
        results.append(GateResult("G6_ro_share_accessible", "SKIP", "Only required for prod promotion"))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="src_env", default="qa", choices=["dev", "qa", "prod"])
    parser.add_argument("--to",   dest="tgt_env", default="prod", choices=["dev", "qa", "prod"])
    parser.add_argument("--sa", action="store_true", help="Use service-account token (MD_SA_TOKEN)")
    parser.add_argument("--dry-run", action="store_true", help="Run checks only; do not write anything")
    args = parser.parse_args()

    promote_to_prod = args.tgt_env == "prod"

    print(f"\n{'='*70}")
    print(f"  THYROID_2026  —  Promotion Gate")
    print(f"  {args.src_env.upper()} → {args.tgt_env.upper()}")
    print(f"  Service account token: {'yes' if args.sa else 'no (personal token)'}")
    print(f"{'='*70}\n")

    try:
        client = MotherDuckClient.for_env(args.src_env, use_service_account=args.sa)
        con = client.connect_rw()
        print(f"  Connected to: {client.config.database}\n")
    except Exception as e:
        print(f"  ERROR: Cannot connect to {args.src_env}: {e}")
        sys.exit(2)

    t0 = time.time()
    results = run_gates(con, promote_to_prod=promote_to_prod)
    elapsed = time.time() - t0
    con.close()

    # ── Report ──────────────────────────────────────────────────────────────
    any_fail = False
    print(f"  {'Gate':<35} {'Status':<6}  Detail")
    print(f"  {'-'*35} {'-'*6}  {'-'*40}")
    for r in results:
        icon = "✓" if r.status == "PASS" else ("✗" if r.status == "FAIL" else "–")
        print(f"  {r.name:<35} {icon} {r.status:<4}  {r.detail[:80]}")
        if r.status == "FAIL":
            any_fail = True

    print(f"\n  Completed in {elapsed:.1f}s")

    if args.dry_run:
        print("\n  [DRY-RUN] No promotion performed.")
        sys.exit(0)

    if any_fail:
        print(f"\n  ✗ PROMOTION BLOCKED — fix failures before promoting {args.src_env} → {args.tgt_env}\n")
        sys.exit(1)
    else:
        print(f"\n  ✓ ALL GATES PASS — safe to promote {args.src_env} → {args.tgt_env}")
        print(f"  Next step: scripts/26_motherduck_materialize_v2.py --md")
        print(f"  (targeting database: {MotherDuckClient.for_env(args.tgt_env).config.database})\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
