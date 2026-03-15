#!/usr/bin/env python3
"""
92_query_benchmark.py  —  MotherDuck query performance benchmark suite

Runs a suite of representative critical-path queries, records timing, and
compares against a saved baseline CSV.  Flags any query that regresses
beyond the configured multiplier threshold (default 2×).

Usage
─────
  # Run benchmarks against prod, save to exports/md_benchmark_<date>/
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/92_query_benchmark.py --md

  # Run against qa; compare to prod baseline
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/92_query_benchmark.py --env qa \\
      --baseline exports/md_benchmark_20260314/benchmark_baseline.csv

  # Update the stored baseline from the latest results
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/92_query_benchmark.py --md --update-baseline

  # Dry-run (print queries, skip execution)
  .venv/bin/python scripts/92_query_benchmark.py --dry-run

Regression threshold
────────────────────
A query is REGRESSED if current_ms > baseline_ms * REGRESSION_MULTIPLIER.
Default multiplier: 2.0 (queries that take >2× baseline are flagged).

Records saved
─────────────
  exports/md_benchmark_<date>/benchmark_results_<timestamp>.csv
  exports/md_benchmark_<date>/benchmark_baseline.csv  (if --update-baseline)
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402

REGRESSION_MULTIPLIER = 2.0

# ── Benchmark queries ─────────────────────────────────────────────────────
# (label, sql, expected_min_rows, tier)
# tier: "hot" = materialized table (SLA <500ms), "warm" = view/join (SLA <5000ms)
BENCHMARKS: list[tuple[str, str, int, str]] = [
    # P0: Streamlit hot-path (materialized tables, should be sub-100ms on Pulse)
    ("overview_patient_count",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
     10500, "hot"),
    ("streamlit_header_count",
     "SELECT COUNT(*) FROM streamlit_patient_header_v",
     10000, "hot"),
    ("scoring_ajcc_dist",
     "SELECT ajcc8_stage_group, COUNT(*) FROM thyroid_scoring_py_v1 GROUP BY 1 ORDER BY 1",
     3, "hot"),
    ("manuscript_cohort_sample",
     "SELECT research_id, demo_sex_final, ajcc8_t_stage FROM manuscript_cohort_v1 LIMIT 100",
     100, "hot"),
    ("cancer_cohort_full",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
     3900, "hot"),
    ("dedup_episode_count",
     "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
     9000, "hot"),
    ("survival_cohort_count",
     "SELECT COUNT(*) FROM survival_cohort_enriched",
     40000, "hot"),
    ("lab_canonical_count",
     "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1",
     30000, "hot"),
    ("tirads_validated_dist",
     "SELECT tirads_best_category, COUNT(*) FROM extracted_tirads_validated_v1 GROUP BY 1",
     3, "hot"),
    ("operative_episode_count",
     "SELECT COUNT(*) FROM operative_episode_detail_v2",
     8000, "hot"),
    # P1: Warm queries (views, lightweight joins on materialized tables)
    ("braf_ras_positivity",
     """SELECT
        SUM(CASE WHEN LOWER(CAST(braf_positive_final AS VARCHAR))='true' THEN 1 ELSE 0 END) AS braf_positive,
        SUM(CASE WHEN LOWER(CAST(ras_positive_v11 AS VARCHAR))='true' THEN 1 ELSE 0 END)  AS ras_positive,
        COUNT(*) AS total
        FROM patient_refined_master_clinical_v12""",
     1, "warm"),
    ("date_rescue_kpi",
     "SELECT * FROM date_rescue_rate_summary LIMIT 20",
     1, "warm"),
    ("val_integrity_summary",
     "SELECT COUNT(*) FROM val_dataset_integrity_summary_v1",
     1, "warm"),
    ("linkage_summary_v3",
     "SELECT COUNT(*) FROM linkage_summary_v3",
     5, "warm"),
    ("rai_episode_dist",
     "SELECT rai_assertion_status, COUNT(*) FROM rai_treatment_episode_v2 GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
     1, "warm"),
    ("molecular_platform_dist",
     "SELECT platform, COUNT(*) FROM molecular_test_episode_v2 WHERE platform IS NOT NULL GROUP BY 1",
     1, "warm"),
    # P2: Complex aggregations (should remain under 10s)
    ("survival_by_stage",
     """SELECT ajcc_stage_8, COUNT(*) AS n, AVG(time_days)/365.25 AS mean_followup_y
        FROM survival_cohort_enriched GROUP BY 1 ORDER BY 1""",
     3, "warm"),
    ("ete_grade_cross_stage",
     """SELECT s.ajcc8_t_stage, m.ete_grade_v9, COUNT(*) AS n
        FROM thyroid_scoring_py_v1 s
        JOIN patient_refined_master_clinical_v9 m
          ON CAST(s.research_id AS VARCHAR)=CAST(m.research_id AS VARCHAR)
        GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20""",
     3, "warm"),
    ("complication_summary",
     """SELECT entity_name, COUNT(DISTINCT research_id) AS patients
        FROM extracted_complications_refined_v5
        WHERE entity_is_confirmed = TRUE
        GROUP BY 1 ORDER BY 2 DESC""",
     3, "warm"),
]


@dataclass
class BenchmarkResult:
    label: str
    elapsed_ms: float
    rows: int
    tier: str
    status: str = "OK"       # OK | SLOW | REGRESSED | ERROR
    baseline_ms: float | None = None
    error: str = ""


def load_baseline(path: Path) -> dict[str, float]:
    """Load label→median_ms map from a baseline CSV."""
    if not path.exists():
        return {}
    baseline: dict[str, float] = {}
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            label = row.get("label", "") or ""
            if not label or label.startswith("#"):
                continue
            try:
                baseline[label] = float(row["median_ms"])
            except (KeyError, ValueError, TypeError):
                pass
    return baseline


def run_benchmarks(
    con: Any,
    baseline: dict[str, float],
    n_runs: int = 3,
) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    print(f"\n  {'Label':<40} {'Tier':<5} {'Runs':>4}  {'Median ms':>10}  {'Rows':>8}  Status")
    print(f"  {'-'*40} {'-'*5} {'-'*4}  {'-'*10}  {'-'*8}  {'-'*12}")

    for label, sql, min_rows, tier in BENCHMARKS:
        timings: list[float] = []
        last_rows = 0
        err = ""
        for _ in range(n_runs):
            try:
                t0 = time.perf_counter()
                df = con.execute(sql).fetchdf()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                timings.append(elapsed_ms)
                last_rows = len(df)
            except Exception as e:
                err = str(e)[:80]
                break

        if err:
            r = BenchmarkResult(label=label, elapsed_ms=-1, rows=0, tier=tier, status="ERROR", error=err)
        else:
            median_ms = sorted(timings)[len(timings) // 2]
            base = baseline.get(label)
            sla_ms = 500 if tier == "hot" else 10_000
            if base is not None and median_ms > base * REGRESSION_MULTIPLIER:
                status = "REGRESSED"
            elif median_ms > sla_ms * 2:
                status = "SLOW"
            else:
                status = "OK"
            r = BenchmarkResult(label=label, elapsed_ms=median_ms, rows=last_rows,
                                tier=tier, status=status, baseline_ms=base, error=err)

        results.append(r)
        icon = "✓" if r.status == "OK" else ("⚠" if r.status == "SLOW" else ("✗" if r.status in ("ERROR","REGRESSED") else ""))
        base_info = f"(base {r.baseline_ms:.0f}ms)" if r.baseline_ms else ""
        print(f"  {label:<40} {tier:<5} {n_runs:>4}  {r.elapsed_ms:>10.1f}  {r.rows:>8,}  {icon} {r.status} {base_info}")

    return results


def save_results(results: list[BenchmarkResult], out_dir: Path, as_baseline: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_path = out_dir / f"benchmark_results_{ts}.csv"
    fields = ["label", "tier", "elapsed_ms", "rows", "status", "baseline_ms", "error"]
    with results_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({
                "label": r.label, "tier": r.tier,
                "elapsed_ms": f"{r.elapsed_ms:.2f}" if r.elapsed_ms >= 0 else "",
                "rows": r.rows, "status": r.status,
                "baseline_ms": f"{r.baseline_ms:.2f}" if r.baseline_ms else "",
                "error": r.error,
            })
    print(f"\n  Results saved → {results_path}")

    if as_baseline:
        baseline_path = out_dir / "benchmark_baseline.csv"
        with baseline_path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["label", "tier", "median_ms"])
            w.writeheader()
            for r in results:
                if r.status not in ("ERROR",):
                    w.writerow({"label": r.label, "tier": r.tier, "median_ms": f"{r.elapsed_ms:.2f}"})
        print(f"  Baseline updated → {baseline_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--md",  action="store_true", help="Use MotherDuck (default: local)")
    parser.add_argument("--env", default="prod", choices=["dev", "qa", "prod"])
    parser.add_argument("--sa",  action="store_true", help="Use service-account token (MD_SA_TOKEN)")
    parser.add_argument("--baseline", type=Path,
                        default=ROOT / "exports" / "md_benchmark_20260314" / "benchmark_baseline.csv",
                        help="Path to baseline CSV for regression comparison")
    parser.add_argument("--update-baseline", action="store_true", help="Overwrite baseline with current results")
    parser.add_argument("--runs", type=int, default=3, help="Number of timed runs per query")
    parser.add_argument("--dry-run", action="store_true", help="Print queries without executing")
    args = parser.parse_args()

    out_dir = ROOT / "exports" / f"md_benchmark_{datetime.now().strftime('%Y%m%d')}"

    print(f"\n{'='*70}")
    print(f"  THYROID_2026  —  Query Benchmark Suite")
    print(f"  Environment: {args.env}  |  Runs/query: {args.runs}")
    print(f"  Baseline: {args.baseline}")
    print(f"{'='*70}")

    if args.dry_run:
        for label, sql, min_rows, tier in BENCHMARKS:
            print(f"\n  [{tier}] {label}")
            print(f"    {sql[:120]}")
        print(f"\n  {len(BENCHMARKS)} queries defined.  (dry-run — skipping execution)")
        sys.exit(0)

    baseline = load_baseline(args.baseline) if args.baseline.exists() else {}
    if baseline:
        print(f"  Loaded {len(baseline)} baseline entries from {args.baseline}")
    else:
        print("  No baseline found — first run will not flag regressions")

    # Connect
    if args.md:
        try:
            client = MotherDuckClient.for_env(args.env, use_service_account=args.sa)
            con = client.connect_rw()
            print(f"  Connected: {client.config.database} (MotherDuck)\n")
        except Exception as e:
            print(f"  ERROR: {e}")
            sys.exit(2)
    else:
        import duckdb
        local_path = ROOT / "thyroid_master_local.duckdb"
        con = duckdb.connect(str(local_path))
        print(f"  Connected: {local_path} (local)\n")

    t_total = time.time()
    results = run_benchmarks(con, baseline, n_runs=args.runs)
    total_elapsed = time.time() - t_total
    con.close()

    # Summary
    n_ok       = sum(1 for r in results if r.status == "OK")
    n_slow     = sum(1 for r in results if r.status == "SLOW")
    n_regressed = sum(1 for r in results if r.status == "REGRESSED")
    n_error    = sum(1 for r in results if r.status == "ERROR")

    print(f"\n  ──────────────────────────────────────────────────")
    print(f"  SUMMARY: {n_ok} OK  |  {n_slow} SLOW  |  {n_regressed} REGRESSED  |  {n_error} ERROR")
    print(f"  Total benchmark time: {total_elapsed:.1f}s")

    save_results(results, out_dir, as_baseline=args.update_baseline)

    if n_regressed > 0 or n_error > 0:
        print(f"\n  ✗ Benchmark FAILED — {n_regressed} regressions, {n_error} errors\n")
        sys.exit(1)
    else:
        print(f"\n  ✓ All benchmarks within acceptable range\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
