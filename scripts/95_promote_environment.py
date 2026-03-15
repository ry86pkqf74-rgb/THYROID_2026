#!/usr/bin/env python3
"""
95_promote_environment.py  —  MotherDuck DEV → QA → PROD promotion workflow

Implements a deterministic, gate-driven promotion pipeline:

  dev  → qa   routine iteration (personal token)
  qa   → prod validated release  (service-account token required)

The script:
  1. Runs the promotion gate (91_promotion_gate.py) on the SOURCE env
  2. Runs script-96 to generate a release manifest for the SOURCE
  3. If --materialize, runs script-26 to copy tables to the TARGET env
  4. Writes a promotion record (JSON) to exports/promotions/

Usage
─────
  # Validate dev before promoting to qa (dry-run: no write)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_promote_environment.py \\
      --from dev --to qa --dry-run

  # Full dev-to-qa promotion (materialize + record)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_promote_environment.py \\
      --from dev --to qa --materialize

  # Full qa-to-prod promotion (requires SA token)
  MD_SA_TOKEN=... .venv/bin/python scripts/95_promote_environment.py \\
      --from qa --to prod --materialize --sa

  # Print rollback checklist only
  python scripts/95_promote_environment.py --rollback-checklist

Promotion environments
──────────────────────
  dev   thyroid_research_2026_dev   personal dev DB
  qa    thyroid_research_2026_qa    validation DB
  prod  thyroid_research_2026       production DB (RO share backed)

Promotion record
────────────────
  Written to exports/promotions/promo_<from>_to_<to>_<timestamp>.json
  Contains: git_sha, run_ts, source_env, target_env, gate_results,
            canonical_metrics, benchmark_delta, manifest_path, status.

Exit codes
──────────
  0  Promotion PASS (or dry-run complete with all gates passing)
  1  One or more promotion gates FAIL — promotion blocked
  2  Environment / connectivity / auth error
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PROMOTIONS_DIR = ROOT / "exports" / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)

ENV_DB_MAP = {
    "dev":  "thyroid_research_2026_dev",
    "qa":   "thyroid_research_2026_qa",
    "prod": "thyroid_research_2026",
}

VALID_PATHS = [
    ("dev", "qa"),
    ("qa", "prod"),
    # Allow self-check without promotion
    ("dev", "dev"),
    ("qa",  "qa"),
    ("prod", "prod"),
]

# ── Promotion checklist ────────────────────────────────────────────────────

PROMOTION_CHECKLIST = """
DEV → QA PROMOTION CHECKLIST
═════════════════════════════
Pre-conditions:
  □  All script-91 gates pass on DEV
  □  No schema-breaking migrations pending (check RELEASE_NOTES.md)
  □  Benchmark baseline exists (exports/md_benchmark_<date>/benchmark_baseline.csv)
  □  No open P0 review-queue items in hardening_review_queue

Steps:
  □  1.  python scripts/95_promote_environment.py --from dev --to qa --dry-run
  □  2.  Review dry-run gates (all must be PASS)
  □  3.  python scripts/95_promote_environment.py --from dev --to qa --materialize
  □  4.  Confirm promotion record written to exports/promotions/
  □  5.  python scripts/96_release_manifest.py --env qa --baseline <path>

QA → PROD PROMOTION CHECKLIST
══════════════════════════════
Pre-conditions:
  □  DEV → QA promotion completed and recorded
  □  All script-91 gates pass on QA
  □  Release manifest (script-96) generated for QA
  □  Share publication check (script-94 or CI job 5) passes on QA
  □  Benchmark regression acceptable (no blocker-severity regressions)
  □  RELEASE_NOTES.md updated with this release

Steps:
  □  1.  MD_SA_TOKEN=... python scripts/91_promotion_gate.py --from qa --to prod --sa
  □  2.  Review all 12 gates (all must be PASS)
  □  3.  MD_SA_TOKEN=... python scripts/95_promote_environment.py --from qa --to prod --materialize --sa
  □  4.  Confirm promotion record written
  □  5.  python scripts/96_release_manifest.py --env prod --tag <version>
  □  6.  Verify RO share reflects new counts: python scripts/93_dashboard_smoke.py --md
  □  7.  git tag -a v<version> -m "Release <version>" && git push origin v<version>
  □  8.  Update RELEASE_NOTES.md and commit

ROLLBACK CHECKLIST (prod rollback)
════════════════════════════════════
  □  1.  Identify the previous promotion record:
           ls -lt exports/promotions/ | head
  □  2.  Note the previous manifest_path from that record
  □  3.  Restore tables from the backup snapshot (if taken before promotion):
           MD_SA_TOKEN=... python scripts/26_motherduck_materialize_v2.py --md
           (ensure source env is pointing at the prior QA snapshot)
  □  4.  Verify restored counts match previous manifest row counts
  □  5.  Re-run script-91 on prod to confirm restored state is healthy
  □  6.  Update RELEASE_NOTES.md with rollback notice and commit/push
  □  7.  Notify downstream consumers (dashboard, manuscript packaging)
"""


# ── Git helpers ────────────────────────────────────────────────────────────

def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def git_dirty() -> bool:
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=ROOT, text=True
        ).strip()
        return bool(out)
    except Exception:
        return False


# ── Gate runner ────────────────────────────────────────────────────────────

def run_promotion_gate(
    from_env: str,
    to_env: str,
    sa: bool = False,
    dry_run: bool = False,
) -> tuple[bool, dict]:
    """Run script-91 and return (passed, summary_dict)."""
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "91_promotion_gate.py"),
        "--from", from_env,
        "--to", to_env,
    ]
    if sa:
        cmd.append("--sa")
    if dry_run:
        cmd.append("--dry-run")

    print(f"\n  Running promotion gate: {' '.join(cmd)}\n")
    t0 = time.monotonic()
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = round(time.monotonic() - t0, 1)

    passed = result.returncode == 0
    return passed, {
        "returncode": result.returncode,
        "elapsed_s": elapsed,
        "cmd": " ".join(cmd),
        "status": "PASS" if passed else "FAIL",
    }


# ── Manifest runner ────────────────────────────────────────────────────────

def run_release_manifest(
    env: str,
    sa: bool = False,
    baseline: Optional[Path] = None,
) -> Optional[Path]:
    """Run script-96 and return the path to the generated manifest."""
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "96_release_manifest.py"),
        "--env", env,
    ]
    if sa:
        cmd.append("--sa")
    if baseline and baseline.exists():
        cmd.extend(["--baseline", str(baseline)])

    print(f"\n  Generating release manifest: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Extract manifest path from stdout
    for line in result.stdout.splitlines():
        if "manifest written:" in line.lower():
            path = Path(line.split(":", 1)[-1].strip())
            if path.exists():
                return path

    # Fallback: check most recent manifest in exports/manifests/
    manifests = sorted(
        (ROOT / "exports" / "manifests").glob(f"manifest_{env}_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return manifests[0] if manifests else None


# ── Promotion record ───────────────────────────────────────────────────────

def write_promotion_record(
    from_env: str,
    to_env: str,
    gate_result: dict,
    manifest_path: Optional[Path],
    dry_run: bool,
    status: str,
) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    record = {
        "promotion_ts": datetime.utcnow().isoformat(),
        "git_sha": git_sha(),
        "git_dirty": git_dirty(),
        "from_env": from_env,
        "to_env": to_env,
        "source_db": ENV_DB_MAP.get(from_env, from_env),
        "target_db": ENV_DB_MAP.get(to_env, to_env),
        "dry_run": dry_run,
        "gate_result": gate_result,
        "manifest_path": str(manifest_path) if manifest_path else None,
        "status": status,
    }
    out_path = PROMOTIONS_DIR / f"promo_{from_env}_to_{to_env}_{ts}.json"
    out_path.write_text(json.dumps(record, indent=2))
    return out_path


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--from", dest="from_env", choices=list(ENV_DB_MAP), required=False)
    ap.add_argument("--to",   dest="to_env",   choices=list(ENV_DB_MAP), required=False)
    ap.add_argument("--sa",          action="store_true", help="Use service-account token")
    ap.add_argument("--dry-run",     action="store_true", help="Run gates only; do not materialize")
    ap.add_argument("--materialize", action="store_true", help="Run script-26 after gates pass")
    ap.add_argument("--rollback-checklist", action="store_true",
                    help="Print the rollback checklist and exit")
    ap.add_argument("--promotion-checklist", action="store_true",
                    help="Print both promotion checklists and exit")
    ap.add_argument("--baseline", type=Path, default=None,
                    help="Benchmark baseline CSV for delta in manifest")
    args = ap.parse_args()

    if args.rollback_checklist or args.promotion_checklist:
        print(PROMOTION_CHECKLIST)
        sys.exit(0)

    if not args.from_env or not args.to_env:
        ap.print_help()
        sys.exit(2)

    from_env = args.from_env
    to_env   = args.to_env

    if (from_env, to_env) not in VALID_PATHS:
        print(f"ERROR: Unsupported promotion path: {from_env} → {to_env}")
        print(f"       Valid paths: {VALID_PATHS}")
        sys.exit(2)

    print("\n" + "=" * 72)
    print(f"  PROMOTION:  {from_env.upper()} → {to_env.upper()}")
    print(f"  Source DB:  {ENV_DB_MAP[from_env]}")
    print(f"  Target DB:  {ENV_DB_MAP[to_env]}")
    print(f"  Mode:       {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"  Git SHA:    {git_sha()}")
    print(f"  Timestamp:  {datetime.utcnow().isoformat()}")
    if git_dirty():
        print("  ⚠  WARNING: working tree is dirty (uncommitted changes)")
    print("=" * 72)

    # ── Step 1: Run promotion gate ─────────────────────────────────────────
    gate_passed, gate_result = run_promotion_gate(
        from_env, to_env, sa=args.sa, dry_run=args.dry_run,
    )
    if not gate_passed:
        print(f"\n  ✗  Promotion gate FAILED for {from_env} → {to_env}")
        record_path = write_promotion_record(
            from_env, to_env, gate_result, None, args.dry_run, "GATE_FAIL",
        )
        print(f"  Promotion record: {record_path.relative_to(ROOT)}")
        sys.exit(1)
    print(f"\n  ✓  Promotion gate PASSED ({gate_result['elapsed_s']}s)")

    # ── Step 2: Generate release manifest ─────────────────────────────────
    manifest_path: Optional[Path] = None
    if not args.dry_run:
        manifest_path = run_release_manifest(from_env, sa=args.sa, baseline=args.baseline)
        if manifest_path:
            print(f"  ✓  Release manifest: {manifest_path.name}")
        else:
            print("  ⚠  Release manifest generation did not produce a file")

    # ── Step 3: Materialize (optional) ────────────────────────────────────
    if args.materialize and not args.dry_run and from_env != to_env:
        print(f"\n  Materializing {from_env} tables to {to_env}...")
        env = dict(os.environ)
        if args.sa:
            env["MOTHERDUCK_TARGET_DB"] = ENV_DB_MAP[to_env]
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "26_motherduck_materialize_v2.py"), "--md"],
            env=env,
        )
        if result.returncode != 0:
            print("  ✗  Materialization failed")
            record_path = write_promotion_record(
                from_env, to_env, gate_result, manifest_path, args.dry_run, "MATERIALIZE_FAIL",
            )
            print(f"  Promotion record: {record_path.relative_to(ROOT)}")
            sys.exit(1)
        print("  ✓  Materialization complete")

    # ── Write promotion record ─────────────────────────────────────────────
    status = "DRY_RUN_PASS" if args.dry_run else "PROMOTED"
    record_path = write_promotion_record(
        from_env, to_env, gate_result, manifest_path, args.dry_run, status,
    )
    print(f"\n  ✓  Promotion record: {record_path.relative_to(ROOT)}")
    print("\n  " + "=" * 68)
    print(f"  STATUS: {status}")
    print("  " + "=" * 68 + "\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
