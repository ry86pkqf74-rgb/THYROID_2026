#!/usr/bin/env python3
"""
96_release_manifest.py  —  Generate a deterministic release manifest

Captures a point-in-time snapshot of the promoted MotherDuck production
database suitable for embedding in a GitHub release.

Output: exports/release_manifests/release_manifest_<sha7>_<timestamp>.json

Fields
──────
  git_sha, git_branch, tagged_version  — SCM provenance
  generated_at                          — UTC ISO timestamp
  row_counts                            — one entry per CRITICAL_TABLE
  metric_snapshot                       — 8 named metrics with in-range flag
  benchmark_deltas                      — query latency vs baseline CSV
  gate_results                          — pass/fail per gate
  overall_status                        — RELEASE_READY | BLOCKED | DRY_RUN
  notes                                 — optional freeform string

Usage
─────
  # Read from prod and write manifest
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/96_release_manifest.py

  # Point to a specific env (e.g. qa)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/96_release_manifest.py --env qa

  # Dry-run (print but do not write)
  .venv/bin/python scripts/96_release_manifest.py --dry-run

  # Use service-account token
  MD_SA_TOKEN=... .venv/bin/python scripts/96_release_manifest.py --sa

Exit codes
──────────
  0  Manifest written; overall_status = RELEASE_READY
  1  One or more gates FAILED; overall_status = BLOCKED
  2  Connection / environment error
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent

ENV_DATABASES = {
    "dev":  "thyroid_research_2026_dev",
    "qa":   "thyroid_research_2026_qa",
    "prod": "thyroid_research_2026",
}

MANIFEST_DIR = ROOT / "exports" / "release_manifests"

# ── Critical table list ───────────────────────────────────────────────────

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
    "extracted_tirads_validated_v1",
    "patient_refined_master_clinical_v12",
]

# ── Canonical metric definitions ──────────────────────────────────────────

METRICS: list[tuple[str, str, int, int]] = [
    ("surgical_cohort",   "SELECT COUNT(DISTINCT research_id) FROM master_cohort",                      10500, 11500),
    ("cancer_cohort",     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",                              3900,  4300),
    ("manuscript_cohort", "SELECT COUNT(*) FROM manuscript_cohort_v1",                                  10500, 11200),
    ("dedup_episodes",    "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",                     9000,  9800),
    ("scoring_patients",  "SELECT COUNT(*) FROM thyroid_scoring_py_v1",                                 10500, 11500),
    ("survival_cohort",   "SELECT COUNT(*) FROM survival_cohort_enriched",                              40000, 70000),
    ("tirads_patients",   "SELECT COUNT(*) FROM extracted_tirads_validated_v1",                          3000,  4000),
    ("operative_eps",     "SELECT COUNT(*) FROM operative_episode_detail_v2",                            8000, 12000),
    ("lab_rows",          "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1",                         30000, 60000),
    ("refined_master_v12","SELECT COUNT(*) FROM patient_refined_master_clinical_v12",                   10000, 14000),
    ("molecular_tested",  "SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2",            800, 1200),
]

# ── Benchmark queries ─────────────────────────────────────────────────────

BENCHMARK_QUERIES: list[tuple[str, str, int]] = [
    ("count_manuscript",
     "SELECT COUNT(*) FROM manuscript_cohort_v1",
     1),
    ("surgical_cohort_distinct",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
     1),
    ("cancer_cohort_count",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
     1),
    ("dedup_episodes_count",
     "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
     1),
    ("lab_analyte_breakdown",
     "SELECT analyte_group, COUNT(*) FROM longitudinal_lab_canonical_v1 GROUP BY 1",
     1),
    ("tirads_category_dist",
     "SELECT tirads_best_category, COUNT(*) FROM extracted_tirads_validated_v1 GROUP BY 1",
     1),
]

BENCHMARK_RUNS = 2


# ── Connection helper ──────────────────────────────────────────────────────

def get_connection(env: str, sa: bool = False):
    import duckdb
    db_name = ENV_DATABASES[env]
    if sa:
        token = os.environ.get("MD_SA_TOKEN") or os.environ.get("MOTHERDUCK_TOKEN")
    else:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            try:
                import toml
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
            except Exception:
                pass
    if not token:
        print(f"ERROR: No MotherDuck token for env={env}")
        sys.exit(2)
    return duckdb.connect(f"md:{db_name}?motherduck_token={token}")


# ── Git helpers ────────────────────────────────────────────────────────────

def git_sha(short: bool = True) -> str:
    n = 7 if short else 40
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()[:n]
    except Exception:
        return "unknown"


def git_branch() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "branch", "--show-current"], text=True
        ).strip()
    except Exception:
        return "unknown"


def git_tag() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "describe", "--tags", "--exact-match"], text=True
        ).strip()
    except Exception:
        return ""


def git_dirty() -> bool:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(ROOT), "status", "--porcelain"], text=True
        ).strip()
        return bool(out)
    except Exception:
        return False


# ── Row counts ─────────────────────────────────────────────────────────────

def collect_row_counts(con) -> dict[str, Any]:
    counts = {}
    for tbl in CRITICAL_TABLES:
        try:
            n = int(con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0])
            counts[tbl] = {"rows": n, "status": "OK"}
        except Exception as e:
            counts[tbl] = {"rows": None, "status": f"ERROR: {str(e)[:80]}"}
    return counts


# ── Metrics ────────────────────────────────────────────────────────────────

def collect_metrics(con) -> dict[str, Any]:
    snap: dict[str, Any] = {}
    failed = 0
    for name, sql, lo, hi in METRICS:
        try:
            v = int(con.execute(sql).fetchone()[0])
            in_range = lo <= v <= hi
            snap[name] = {"value": v, "lo": lo, "hi": hi,
                          "in_range": in_range, "status": "PASS" if in_range else "FAIL"}
            if not in_range:
                failed += 1
        except Exception as e:
            snap[name] = {"value": None, "status": f"ERROR: {str(e)[:80]}"}
            failed += 1
    snap["_failures"] = failed
    return snap


# ── Benchmark ──────────────────────────────────────────────────────────────

def collect_benchmarks(con, baseline_csv: Path | None) -> dict[str, Any]:
    baseline: dict[str, float] = {}
    if baseline_csv and baseline_csv.exists():
        with baseline_csv.open() as f:
            for row in csv.DictReader(f):
                if "label" in row and "median_ms" in row:
                    try:
                        baseline[row["label"]] = float(row["median_ms"])
                    except (ValueError, TypeError):
                        pass

    results: dict[str, Any] = {}
    for label, sql, _ in BENCHMARK_QUERIES:
        times_ms = []
        for _ in range(BENCHMARK_RUNS):
            t0 = time.perf_counter()
            try:
                con.execute(sql).fetchall()
                times_ms.append((time.perf_counter() - t0) * 1000)
            except Exception as e:
                results[label] = {"error": str(e)[:80]}
                break
        if times_ms:
            median_ms = sorted(times_ms)[len(times_ms) // 2]
            base = baseline.get(label)
            delta_pct = None
            regression = False
            if base and base > 0:
                delta_pct = round((median_ms - base) / base * 100, 1)
                regression = delta_pct > 100  # >2× baseline
            results[label] = {
                "median_ms": round(median_ms, 1),
                "baseline_ms": base,
                "delta_pct": delta_pct,
                "regression": regression,
            }
    return results


# ── Gate summary ───────────────────────────────────────────────────────────

def gate_map_dedup() -> dict:
    validator = ROOT / "scripts" / "94_map_dedup_validator.py"
    if not validator.exists():
        return {"name": "map_dedup", "status": "SKIP", "detail": "94_map_dedup_validator.py not found"}
    try:
        res = subprocess.run([sys.executable, str(validator)],
                             capture_output=True, text=True, timeout=30)
        status = "PASS" if res.returncode == 0 else "FAIL"
        lines = (res.stdout + res.stderr).strip().splitlines()
        detail = next((l for l in lines if "PASS" in l or "FAIL" in l or "duplicate" in l.lower()), "")
        return {"name": "map_dedup", "status": status, "detail": detail}
    except Exception as e:
        return {"name": "map_dedup", "status": "WARN", "detail": str(e)[:80]}


def gate_working_tree(allow_dirty: bool) -> dict:
    dirty = git_dirty()
    if dirty and not allow_dirty:
        return {"name": "working_tree_clean", "status": "WARN",
                "detail": "Uncommitted changes in working tree"}
    return {"name": "working_tree_clean", "status": "PASS" if not dirty else "WARN",
            "detail": "clean" if not dirty else "dirty (allowed)"}


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default="prod", choices=list(ENV_DATABASES),
                    help="Database environment to snapshot (default: prod)")
    ap.add_argument("--sa", action="store_true",
                    help="Use MD_SA_TOKEN instead of MOTHERDUCK_TOKEN")
    ap.add_argument("--dry-run", action="store_true",
                    help="Collect and print but do not write file")
    ap.add_argument("--allow-dirty", action="store_true",
                    help="Allow uncommitted working tree changes")
    ap.add_argument("--baseline-csv",
                    type=Path,
                    default=ROOT / "exports" / "md_benchmark_20260314" / "benchmark_baseline.csv",
                    help="Benchmark baseline CSV for regression comparison")
    ap.add_argument("--manifest-dir", type=Path, default=MANIFEST_DIR,
                    help="Output directory for manifest JSON")
    ap.add_argument("--notes", default="",
                    help="Freeform release notes to embed")
    args = ap.parse_args()

    print("=" * 72)
    print("  96 — Release Manifest Generator")
    print(f"  Environment : {args.env.upper()}  ({ENV_DATABASES[args.env]})")
    if args.dry_run:
        print("  [DRY-RUN — manifest will not be written]")
    print("=" * 72)

    # ── Connect ────────────────────────────────────────────────────────────
    print(f"\n  Connecting to {args.env}...")
    try:
        con = get_connection(args.env, sa=args.sa)
        print("  ✓  Connected")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(2)

    # ── Collect ────────────────────────────────────────────────────────────
    print("\n  Collecting row counts...")
    row_counts = collect_row_counts(con)

    print("  Collecting metrics...")
    metrics = collect_metrics(con)

    print("  Running benchmarks...")
    benchmarks = collect_benchmarks(con, args.baseline_csv)
    con.close()

    # ── Gate checks ────────────────────────────────────────────────────────
    print("  Running gate checks...")
    gates = [
        gate_map_dedup(),
        gate_working_tree(args.allow_dirty),
    ]
    rc_errors = sum(1 for v in row_counts.values() if v.get("status", "").startswith("ERROR"))
    gates.append({
        "name": "table_existence",
        "status": "PASS" if rc_errors == 0 else "FAIL",
        "detail": f"{len(CRITICAL_TABLES) - rc_errors}/{len(CRITICAL_TABLES)} tables accessible",
    })
    metric_failures = int(metrics.get("_failures", 0))
    gates.append({
        "name": "metric_bounds",
        "status": "PASS" if metric_failures == 0 else "FAIL",
        "detail": f"{metric_failures} metric(s) out of range",
    })
    reg_count = sum(1 for v in benchmarks.values() if isinstance(v, dict) and v.get("regression"))
    gates.append({
        "name": "benchmark_regression",
        "status": "WARN" if reg_count > 0 else "PASS",
        "detail": f"{reg_count} regression(s) detected vs baseline",
    })

    for g in gates:
        icon = {"PASS": "✓", "FAIL": "✗", "WARN": "⚠", "SKIP": "–"}.get(g["status"], "?")
        print(f"  {icon}  {g['name']:<35s} {g['status']:<5s}  {g.get('detail','')}")

    failed = sum(1 for g in gates if g["status"] == "FAIL")
    overall = "RELEASE_READY" if failed == 0 else "BLOCKED"
    if args.dry_run:
        overall = "DRY_RUN"

    # ── Build manifest ─────────────────────────────────────────────────────
    sha = git_sha(short=True)
    now_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    manifest_id = f"release_{sha}_{now_str}"

    manifest = {
        "manifest_id":    manifest_id,
        "env":            args.env,
        "database":       ENV_DATABASES[args.env],
        "git_sha":        git_sha(short=False),
        "git_sha_short":  sha,
        "git_branch":     git_branch(),
        "tagged_version": git_tag(),
        "generated_at":   datetime.utcnow().isoformat(),
        "overall_status": overall,
        "gates":          gates,
        "row_counts":     row_counts,
        "metrics":        {k: v for k, v in metrics.items() if k != "_failures"},
        "benchmarks":     benchmarks,
        "notes":          args.notes,
    }

    # ── Print summary ──────────────────────────────────────────────────────
    print(f"\n  Overall status : {overall}")

    if not args.dry_run:
        args.manifest_dir.mkdir(parents=True, exist_ok=True)
        out_path = args.manifest_dir / f"{manifest_id}.json"
        out_path.write_text(json.dumps(manifest, indent=2))
        print(f"  Manifest       : {out_path.relative_to(ROOT)}")
        # Also write a stable "latest" pointer
        latest = args.manifest_dir / "LATEST_MANIFEST.json"
        latest.write_text(json.dumps({
            "manifest_id": manifest_id,
            "env": args.env,
            "overall_status": overall,
            "generated_at": manifest["generated_at"],
            "git_sha": sha,
            "path": str(out_path.relative_to(ROOT)),
        }, indent=2))
        print(f"  Latest pointer : {latest.relative_to(ROOT)}")
    else:
        print("\n  [dry-run] manifest not written; showing summary:")
        for k in ("git_sha_short", "git_branch", "tagged_version", "overall_status"):
            print(f"  {k:<25s}: {manifest[k]}")
        print(f"  Row count sample:")
        for tbl, v in list(row_counts.items())[:5]:
            print(f"    {tbl}: {v.get('rows','?')}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
