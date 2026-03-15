#!/usr/bin/env python3
"""
84_query_observability.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
MotherDuck query performance observability for dashboard and ETL paths.

Benchmarks:
  1. Dashboard hot-path queries (what Streamlit reads on every page load)
  2. Top materialization queries from script 26
  3. Canonical metric reproducibility (11 manuscript metrics)

Captures per-query:
  - latency (ms)
  - row count
  - pass/fail status
  - warning if latency exceeds threshold

Output:
  - exports/final_md_optimization_20260314/query_benchmark_<timestamp>.csv
  - exports/final_md_optimization_20260314/query_benchmark_<timestamp>.json
  - val_query_benchmark_v1 table in MotherDuck

Usage:
    .venv/bin/python scripts/84_query_observability.py [--md] [--local]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / "final_md_optimization_20260314"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Query registry ────────────────────────────────────────────────────────────
# (label, category, sql, expected_min_rows, latency_warn_ms)
BENCHMARK_QUERIES = [
    # ── Dashboard: Overview tab ──────────────────────────────────────────
    ("dash_patient_count",
     "dashboard_overview",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
     10_000, 3_000),
    ("dash_cancer_cohort",
     "dashboard_overview",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
     4_000, 3_000),
    ("dash_date_rescue_rate",
     "dashboard_overview",
     "SELECT AVG(rescue_rate_pct) AS overall_rescue_rate_pct FROM date_rescue_rate_summary",
     1, 3_000),
    ("dash_manuscript_cohort",
     "dashboard_overview",
     "SELECT COUNT(*) FROM manuscript_cohort_v1",
     10_000, 5_000),
    # ── Dashboard: Patient header ─────────────────────────────────────────
    ("dash_streamlit_patient_header",
     "dashboard_patient",
     "SELECT COUNT(*) FROM streamlit_patient_header_v",
     10_000, 8_000),
    # ── Dashboard: Scoring systems ────────────────────────────────────────
    ("dash_scoring_systems",
     "dashboard_scoring",
     "SELECT COUNT(*) FROM thyroid_scoring_py_v1",
     10_000, 5_000),
    # ── Dashboard: Operative detail ───────────────────────────────────────
    ("dash_operative_detail",
     "dashboard_operative",
     "SELECT COUNT(*) FROM operative_episode_detail_v2",
     8_000, 5_000),
    # ── Materialization: critical tables ──────────────────────────────────
    ("mat_episode_resolved_dedup",
     "materialization",
     "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
     9_000, 5_000),
    ("mat_linkage_summary_v3",
     "materialization",
     "SELECT COUNT(*) FROM linkage_summary_v3",
     1, 5_000),
    ("mat_survival_cohort_enriched",
     "materialization",
     "SELECT COUNT(*) FROM survival_cohort_enriched",
     40_000, 10_000),
    # ── Canonical manuscript metrics ──────────────────────────────────────
    ("metric_01_surgical_cohort",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
     10_000, 3_000),
    ("metric_02_cancer_eligible",
     "manuscript_metric",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1 WHERE analysis_eligible_flag IS TRUE",
     3_500, 5_000),
    ("metric_03_molecular_tested",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2 WHERE cancelled_flag IS NOT TRUE",
     700, 5_000),
    ("metric_04_rai_received",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM rai_treatment_episode_v2",
     800, 5_000),
    ("metric_05_tirads_patients",
     "manuscript_metric",
     "SELECT COUNT(*) FROM extracted_tirads_validated_v1",
     3_000, 5_000),
    ("metric_06_braf_positive",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM extracted_braf_recovery_v1 WHERE braf_status='positive'",
     300, 8_000),
    ("metric_07_ete_graded",
     "manuscript_metric",
     "SELECT COUNT(*) FROM extracted_ete_ene_tert_refined_v1 WHERE ete_grade_v9 IS NOT NULL",
     3_500, 8_000),
    ("metric_08_recurrence_any",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM extracted_recurrence_refined_v1 WHERE recurrence_any IS TRUE",
     1_000, 8_000),
    ("metric_09_ln_yield",
     "manuscript_metric",
     "SELECT COUNT(DISTINCT research_id) FROM extracted_ln_yield_v1",
     5_000, 8_000),
    ("metric_10_survival_cohort",
     "manuscript_metric",
     "SELECT COUNT(*) FROM survival_cohort_enriched",
     50_000, 10_000),
    ("metric_11_manuscript_patients",
     "manuscript_metric",
     "SELECT COUNT(*) FROM manuscript_cohort_v1",
     10_000, 5_000),
    # ── Provenance validation tables ──────────────────────────────────────
    ("val_provenance_completeness",
     "validation",
     "SELECT COUNT(*) FROM val_provenance_completeness_v2",
     1, 5_000),
    ("val_operative_nlp",
     "validation",
     "SELECT COUNT(*) FROM val_operative_nlp_propagation_v1",
     1, 5_000),
]


def get_connection(use_md: bool):
    import duckdb
    if use_md:
        try:
            import toml
            token = os.environ.get("MOTHERDUCK_TOKEN") or toml.load(
                str(ROOT / ".streamlit" / "secrets.toml")
            )["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.environ["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    db = ROOT / "thyroid_master.duckdb"
    return duckdb.connect(str(db))


def run_benchmark(con, queries: list) -> list[dict]:
    results = []
    for label, cat, sql, min_rows, warn_ms in queries:
        t0 = time.monotonic()
        try:
            row = con.execute(sql).fetchone()
            latency_ms = int((time.monotonic() - t0) * 1000)
            value = int(row[0]) if row else 0
            ok = value >= min_rows
            warn = latency_ms > warn_ms
            status = "PASS" if ok else "FAIL"
            if warn:
                status_detail = f"{status}_SLOW"
            else:
                status_detail = status
        except Exception as e:
            latency_ms = int((time.monotonic() - t0) * 1000)
            value = -1
            ok = False
            status = "ERROR"
            status_detail = f"ERROR: {str(e)[:80]}"

        results.append({
            "label": label,
            "category": cat,
            "latency_ms": latency_ms,
            "row_count": value,
            "min_rows": min_rows,
            "latency_warn_ms": warn_ms,
            "status": status_detail,
            "pass": ok,
            "sql": sql[:120],
            "run_at": datetime.utcnow().isoformat(),
        })

        icon = "✓" if ok else "✗"
        slow_note = f"  ⚠ SLOW" if latency_ms > warn_ms and ok else ""
        print(f"  {icon}  [{cat}] {label}: {value:>8,} rows  {latency_ms:>5}ms{slow_note}")

    return results


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    use_md = args.md or not args.local
    con = get_connection(use_md)

    print("\n" + "=" * 72)
    print("  84 — Query Observability Benchmark")
    print("=" * 72)
    print(f"  Running {len(BENCHMARK_QUERIES)} queries...\n")

    results = run_benchmark(con, BENCHMARK_QUERIES)

    # ── Summary ─────────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r["pass"])
    failed = [r for r in results if not r["pass"]]
    slow   = [r for r in results if r["latency_ms"] > r["latency_warn_ms"] and r["pass"]]
    avg_ms = int(sum(r["latency_ms"] for r in results) / len(results))

    print(f"\n  Summary: {passed}/{len(results)} passed "
          f"| {len(failed)} failed | {len(slow)} slow | avg {avg_ms}ms")

    if failed:
        print("\n  FAILURES:")
        for f in failed:
            print(f"    ✗  {f['label']}: {f['status']} | {f['row_count']:,} rows")

    if slow:
        print("\n  SLOW QUERIES (>warn threshold):")
        for s in slow:
            print(f"    ⚠  {s['label']}: {s['latency_ms']}ms > {s['latency_warn_ms']}ms")

    # ── Write val_* table ─────────────────────────────────────────────────
    import pandas as pd, tempfile
    df = pd.DataFrame(results)
    tmp = tempfile.mktemp(suffix=".parquet")
    try:
        df.to_parquet(tmp, index=False)
        con.execute(
            f"CREATE OR REPLACE TABLE val_query_benchmark_v1 AS "
            f"SELECT * FROM read_parquet('{tmp}')"
        )
        print("\n  ✓  val_query_benchmark_v1 written")
    except Exception as e:
        print(f"  WARNING: Could not write val_query_benchmark_v1: {e}")
    finally:
        import os; os.unlink(tmp)

    # ── Export ────────────────────────────────────────────────────────────
    csv_out = EXPORTS_DIR / f"query_benchmark_{TIMESTAMP}.csv"
    json_out = EXPORTS_DIR / f"query_benchmark_{TIMESTAMP}.json"
    df.to_csv(csv_out, index=False)
    json_out.write_text(json.dumps(results, indent=2))

    print(f"\n  Exported: {csv_out.relative_to(ROOT)}")
    print(f"  Exported: {json_out.relative_to(ROOT)}")

    con.close()
    print("\n  Done.\n")
    return 0 if len(failed) == 0 else 1


if __name__ == "__main__":
    sys.exit(main() or 0)
