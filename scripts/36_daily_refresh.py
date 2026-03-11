#!/usr/bin/env python3
"""
36_daily_refresh.py — Full Pipeline Refresh Orchestrator

Runs the complete THYROID_2026 deployment chain in order:
  Phase 6 adjudication:  15 → 16 → 17 → 18 → 19
  V2 canonical:          22 → 23 → 24 → 25 → 26 → 27
  Research views:        03
  Validation:            29 → 30
  Analytics:             31 → 37

Designed for cron / GitHub Actions nightly refresh.

Usage:
    .venv/bin/python scripts/36_daily_refresh.py
    .venv/bin/python scripts/36_daily_refresh.py --md
    .venv/bin/python scripts/36_daily_refresh.py --skip-analytics
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
PYTHON = str(ROOT / ".venv" / "bin" / "python")

PHASE6_CHAIN = [
    "15_date_association_audit.py",
    "16_reconciliation_v2.py",
    "17_semantic_cleanup_v3.py",
    "18_adjudication_framework.py",
    "19_reviewer_persistence.py",
]

V2_CHAIN = [
    "22_canonical_episodes_v2.py",
    "23_cross_domain_linkage_v2.py",
    "24_reconciliation_review_v2.py",
    "25_qa_engine_v2.py",
    "26_motherduck_materialize_v2.py",
    "27_date_provenance.py",
]

REFRESH_CHAIN = [
    "03_research_views.py",
    "29_validation_runner.py",
    "30_readiness_check.py",
]

ANALYTICS_CHAIN = [
    "31_analytic_models.py",
    "37_publication_export.py",
]


def _find_script(name: str) -> Path | None:
    """Resolve script path, handling multiple prefix variants (e.g. 16_sync, 16_reconciliation)."""
    candidates = sorted(SCRIPTS.glob(name))
    if candidates:
        return candidates[0]
    prefix = name.split("_")[0]
    candidates = sorted(SCRIPTS.glob(f"{prefix}_*.py"))
    return candidates[0] if candidates else None


def _run(script_name: str, extra_args: list[str], dry_run: bool) -> bool:
    path = _find_script(script_name)
    if not path:
        print(f"  SKIP  {script_name} (not found)")
        return True

    cmd = [PYTHON, str(path)] + extra_args
    label = path.name
    print(f"\n{'─' * 60}")
    print(f"  RUN   {label}  {' '.join(extra_args)}")

    if dry_run:
        print(f"  DRY   would execute: {' '.join(cmd)}")
        return True

    t0 = time.time()
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=False)
    elapsed = time.time() - t0

    if result.returncode == 0:
        print(f"  OK    {label}  ({elapsed:.1f}s)")
        return True
    else:
        print(f"  FAIL  {label}  exit={result.returncode}  ({elapsed:.1f}s)")
        return False


def main():
    parser = argparse.ArgumentParser(description="Full pipeline refresh")
    parser.add_argument("--md", action="store_true", help="Pass --md to each script")
    parser.add_argument("--skip-analytics", action="store_true")
    parser.add_argument("--skip-v2", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    extra = ["--md"] if args.md else []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{'=' * 60}")
    print(f"THYROID_2026 Daily Refresh — {ts}")
    print(f"{'=' * 60}")

    t_start = time.time()
    failed = []

    print("\n▶ PHASE 6 — Adjudication Chain (15→19)")
    for s in PHASE6_CHAIN:
        if not _run(s, extra, args.dry_run):
            failed.append(s)

    if not args.skip_v2:
        print("\n▶ V2 CANONICAL — Episodes & Linkage (22→27)")
        for s in V2_CHAIN:
            if not _run(s, extra, args.dry_run):
                failed.append(s)

    print("\n▶ REFRESH — Views + Validation (03, 29, 30)")
    for s in REFRESH_CHAIN:
        if not _run(s, extra, args.dry_run):
            failed.append(s)

    if not args.skip_analytics:
        print("\n▶ ANALYTICS — Models + Publication (31, 37)")
        for s in ANALYTICS_CHAIN:
            local_extra = ["--local"] if not args.md else extra
            if not _run(s, local_extra, args.dry_run):
                failed.append(s)

    elapsed_total = time.time() - t_start
    print(f"\n{'=' * 60}")
    if failed:
        print(f"COMPLETED WITH ERRORS ({len(failed)} failures) — {elapsed_total:.0f}s")
        for f in failed:
            print(f"  ✗  {f}")
        sys.exit(1)
    else:
        print(f"ALL STEPS COMPLETE — {elapsed_total:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
