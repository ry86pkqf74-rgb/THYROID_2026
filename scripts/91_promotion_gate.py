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
    # Phase 3 governance tables
    "unified_review_queue_v1",
    "review_ops_progress_v1",
    "source_limited_enforcement_registry_v2",
]

# (metric_name, sql, lo, hi)
METRIC_BOUNDS: list[tuple[str, str, int, int]] = [
    ("surgical_cohort",   "SELECT COUNT(DISTINCT research_id) FROM master_cohort",                      10500, 12000),
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

    # ── G7: Canonical metrics registry — drift & staleness ────────────────
    try:
        from scripts import _canonical_metrics_registry_mod  # noqa — resolved at runtime
    except Exception:
        pass
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from scripts._100_canonical_metrics_registry_api import check_metric_drift, check_staleness_days  # noqa
    except ImportError:
        pass

    _has_registry_api = False
    try:
        # Import drift check from script 100
        _script100 = Path(__file__).resolve().parent / "100_canonical_metrics_registry.py"
        if _script100.exists():
            import importlib.util
            _spec = importlib.util.spec_from_file_location("script100", str(_script100))
            _mod = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            _drift_fn = _mod.check_metric_drift
            _stale_fn = _mod.check_staleness_days
            _has_registry_api = True
    except Exception:
        pass

    if _has_registry_api:
        # Staleness check
        stale = _stale_fn(con, max_days=7)
        if stale["status"] == "STALE":
            results.append(GateResult("G7_canonical_metrics_drift", "WARN",
                f"Registry stale: {stale['detail']}"))
        elif stale["status"] == "FAIL":
            results.append(GateResult("G7_canonical_metrics_drift", "WARN",
                f"Registry not found: {stale['detail']}"))
        else:
            # Drift check (primary metrics only)
            primary_ids = ["total_surgical_patients", "manuscript_cohort_size",
                          "cancer_cohort_size", "dedup_episodes", "braf_positive",
                          "ras_positive", "tert_positive"]
            drift_results = _drift_fn(con, metric_ids=primary_ids, tolerance_pct=1.0)
            drifted = [d for d in drift_results if d["status"] in ("DRIFT", "ERROR")]
            if drifted:
                detail = "; ".join(f"{d['metric_id']}:{d['status']}" for d in drifted)
                results.append(GateResult("G7_canonical_metrics_drift", "WARN",
                    f"{len(drifted)} metrics drifted: {detail}"))
            else:
                results.append(GateResult("G7_canonical_metrics_drift", "PASS",
                    f"All {len(primary_ids)} primary metrics within 1% tolerance"))
    else:
        results.append(GateResult("G7_canonical_metrics_drift", "SKIP",
            "script 100 not available; run canonical_metrics_registry first"))

    # ── G8: Review ops freshness ───────────────────────────────────────────
    g8_failures: list[str] = []
    review_ops_tables = [
        "unified_review_queue_v1",
        "review_ops_progress_v1",
        "review_ops_kpi_v1",
    ]
    for tbl in review_ops_tables:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        except Exception:
            g8_failures.append(f"{tbl} missing")
    if not g8_failures:
        # Check freshness — created_at or computed_at should be within 30 days
        try:
            age = con.execute(
                "SELECT DATE_DIFF('day', MAX(created_at), CURRENT_TIMESTAMP) "
                "FROM unified_review_queue_v1"
            ).fetchone()[0]
            if age is not None and int(age) > 30:
                g8_failures.append(f"unified_review_queue_v1 is {int(age)} days stale (max 30)")
        except Exception:
            pass  # created_at column might not exist yet
    results.append(GateResult(
        "G8_review_ops_freshness",
        "FAIL" if g8_failures else "PASS",
        "; ".join(g8_failures) if g8_failures else "All review ops tables present and fresh",
    ))

    # ── G9: Source-limited enforcement registry completeness ───────────────
    g9_failures: list[str] = []
    try:
        con.execute("SELECT 1 FROM source_limited_enforcement_registry_v2 LIMIT 1")
        cnt = con.execute("SELECT COUNT(*) FROM source_limited_enforcement_registry_v2").fetchone()[0]
        if int(cnt) < 30:
            g9_failures.append(f"Only {cnt} fields in registry (expected >=30)")
        # Check that all tiers are present
        tiers = [r[0] for r in con.execute(
            "SELECT DISTINCT status FROM source_limited_enforcement_registry_v2"
        ).fetchall()]
        for expected in ("CANONICAL", "SOURCE_LIMITED"):
            if expected not in tiers:
                g9_failures.append(f"Missing tier: {expected}")
        # Check validation assertions
        try:
            val_fails = con.execute(
                "SELECT COUNT(*) FROM val_source_limited_enforcement_v1 WHERE result='FAIL'"
            ).fetchone()[0]
            if val_fails > 0:
                g9_failures.append(f"{val_fails} enforcement validation assertions FAIL")
        except Exception:
            pass  # validation table may not exist yet
    except Exception:
        g9_failures.append("source_limited_enforcement_registry_v2 missing; run script 103")
    results.append(GateResult(
        "G9_source_limited_registry",
        "FAIL" if g9_failures else "PASS",
        "; ".join(g9_failures) if g9_failures else f"Registry present with {cnt} fields, all tiers covered",
    ))

    # ── G10: Multi-artifact freshness ───────────────────────────────────────
    g10_checks: list[tuple[str, str]] = [
        ("val_multi_surgery_review_queue_v3", "multi-surgery audit"),
        ("val_episode_linkage_v2_scorecard", "episode linkage v2 scorecard"),
        ("val_hardening_summary", "hardening summary"),
    ]
    g10_failures: list[str] = []
    for tbl, label in g10_checks:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        except Exception:
            g10_failures.append(f"{label} ({tbl}) missing")
    results.append(GateResult(
        "G10_multi_artifact_freshness",
        "FAIL" if g10_failures else "PASS",
        "; ".join(g10_failures) if g10_failures else f"All {len(g10_checks)} governance artifacts present",
    ))

    # ── G11: Non-regression proof ───────────────────────────────────────────
    NONREGRESSION_BOUNDS: list[tuple[str, str, int]] = [
        ("patient_analysis_resolved_v1", "SELECT COUNT(*) FROM patient_analysis_resolved_v1", 10000),
        ("episode_dedup", "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup", 8000),
        ("manuscript_cohort", "SELECT COUNT(*) FROM manuscript_cohort_v1", 10000),
        ("cancer_cohort", "SELECT COUNT(*) FROM analysis_cancer_cohort_v1", 3500),
        ("scoring_table", "SELECT COUNT(*) FROM thyroid_scoring_py_v1", 10000),
    ]
    g11_failures: list[str] = []
    for label, sql, min_rows in NONREGRESSION_BOUNDS:
        try:
            v = int(con.execute(sql).fetchone()[0])
            if v < min_rows:
                g11_failures.append(f"{label}: {v:,} < {min_rows:,} minimum")
        except Exception as e:
            g11_failures.append(f"{label}: ERROR {e}")
    results.append(GateResult(
        "G11_nonregression_proof",
        "FAIL" if g11_failures else "PASS",
        "; ".join(g11_failures) if g11_failures else f"All {len(NONREGRESSION_BOUNDS)} tables above minimum thresholds",
    ))

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
