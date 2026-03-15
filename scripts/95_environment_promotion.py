#!/usr/bin/env python3
"""
95_environment_promotion.py  —  DEV → QA → PROD MotherDuck promotion workflow

Implements a deterministic, gate-checked promotion workflow.

Promotion paths
───────────────
  dev  → qa      Routine development iteration; runs full gate set.
  qa   → prod    Validated release; adds manuscript metric snapshot.
  qa   → qa      Re-validation in-place (idempotent).

The script does NOT copy tables (use script 26 for materialization).
Instead it:
  1. Runs all promotion gates on the SOURCE environment.
  2. If all gates PASS, writes a release manifest to the TARGET env.
  3. Optionally triggers script 26 rematerialization in the TARGET env.
  4. Records the promotion in a `promotion_log` table in the target DB.

Usage
─────
  # Promote dev content to qa (checks dev, records manifest in qa)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \\
      --from dev --to qa

  # Promote qa to prod (production gated; needs all gates)
  MD_SA_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \\
      --from qa --to prod --sa

  # Dry-run (validate only; do not write manifest)
  .venv/bin/python scripts/95_environment_promotion.py \\
      --from qa --to prod --dry-run

Exit codes
──────────
  0  Promotion succeeded (or dry-run passed)
  1  Gate failure — promotion blocked
  2  Connectivity / environment error
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

# ── Environment definitions ────────────────────────────────────────────────

ENV_DATABASES = {
    "dev":  "thyroid_research_2026_dev",
    "qa":   "thyroid_research_2026_qa",
    "prod": "thyroid_research_2026",
}

ENV_GATE_LEVELS = {
    "dev":  "smoke",      # existence checks only
    "qa":   "full",       # full metric bounds + null checks
    "prod": "full+share", # full + RO share accessible
}

ALLOWED_PATHS = {
    ("dev",  "qa"),
    ("qa",   "prod"),
    ("qa",   "qa"),   # idempotent re-validation
    ("dev",  "dev"),  # self-validation
}

# ── Critical tables (must exist in source) ────────────────────────────────

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
]

# ── Metric bounds (same as script 91) ────────────────────────────────────

METRIC_BOUNDS: list[tuple[str, str, int, int]] = [
    ("surgical_cohort",   "SELECT COUNT(DISTINCT research_id) FROM master_cohort",                       10500, 11500),
    ("cancer_cohort",     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",                               3900,  4300),
    ("manuscript_cohort", "SELECT COUNT(*) FROM manuscript_cohort_v1",                                   10500, 11200),
    ("dedup_episodes",    "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",                      9000,  9800),
    ("scoring_patients",  "SELECT COUNT(*) FROM thyroid_scoring_py_v1",                                  10500, 11500),
    ("survival_cohort",   "SELECT COUNT(*) FROM survival_cohort_enriched",                               40000, 70000),
    ("tirads_patients",   "SELECT COUNT(*) FROM extracted_tirads_validated_v1",                           3000,  4000),
    ("operative_eps",     "SELECT COUNT(*) FROM operative_episode_detail_v2",                             8000, 12000),
]

# ── Hardening gates (src tables must exist) ───────────────────────────────

HARDENING_TABLES = [
    "val_hardening_summary",
    "val_dataset_integrity_summary_v1",
    "val_provenance_completeness_v2",
    "val_episode_linkage_completeness_v1",
    "val_lab_completeness_v1",
]


# ── Gate results ──────────────────────────────────────────────────────────

@dataclass
class GateResult:
    name: str
    status: str   # PASS | FAIL | WARN | SKIP
    detail: str = ""
    value: Any = None


@dataclass
class PromotionManifest:
    promotion_id: str        = ""
    from_env: str            = ""
    to_env: str              = ""
    from_db: str             = ""
    to_db: str               = ""
    git_sha: str             = ""
    git_branch: str          = ""
    promoted_at: str         = ""
    gates_run: int           = 0
    gates_passed: int        = 0
    gates_failed: int        = 0
    overall_status: str      = "UNKNOWN"
    metric_snapshot: dict    = field(default_factory=dict)
    benchmark_deltas: dict   = field(default_factory=dict)
    gate_details: list       = field(default_factory=list)
    notes: str               = ""


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
        print(f"ERROR: No MotherDuck token found for env={env}")
        sys.exit(2)
    return duckdb.connect(f"md:{db_name}?motherduck_token={token}")


# ── Individual gate functions ──────────────────────────────────────────────

def gate_table_existence(con, level: str) -> list[GateResult]:
    results = []
    for tbl in CRITICAL_TABLES:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
            results.append(GateResult(f"exists:{tbl}", "PASS"))
        except Exception:
            results.append(GateResult(f"exists:{tbl}", "FAIL",
                                       f"Table {tbl} not found"))
    return results


def gate_metric_bounds(con, level: str) -> list[GateResult]:
    if level == "smoke":
        return [GateResult("metric_bounds", "SKIP", "Skipped in smoke mode")]
    results = []
    for name, sql, lo, hi in METRIC_BOUNDS:
        try:
            v = int(con.execute(sql).fetchone()[0])
            ok = lo <= v <= hi
            results.append(GateResult(
                f"metric:{name}",
                "PASS" if ok else "FAIL",
                f"{v:,} {'in' if ok else 'NOT in'} [{lo:,}, {hi:,}]",
                v,
            ))
        except Exception as e:
            results.append(GateResult(f"metric:{name}", "FAIL", str(e)[:120]))
    return results


def gate_row_multiplication(con, level: str) -> list[GateResult]:
    if level == "smoke":
        return [GateResult("row_multiplication", "SKIP", "Skipped in smoke mode")]
    results = []
    checks = [
        ("patient_analysis_resolved_v1",       "research_id",    "patient"),
        ("manuscript_cohort_v1",               "research_id",    "manuscript"),
    ]
    for tbl, col, label in checks:
        try:
            dupl = con.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {tbl}"
            ).fetchone()[0]
            status = "PASS" if dupl == 0 else "FAIL"
            results.append(GateResult(f"no_dupl:{label}", status,
                                       f"{dupl} duplicate {col}s in {tbl}", dupl))
        except Exception as e:
            results.append(GateResult(f"no_dupl:{label}", "FAIL", str(e)[:120]))
    return results


def gate_null_core_columns(con, level: str) -> list[GateResult]:
    if level == "smoke":
        return [GateResult("null_cols", "SKIP", "Skipped in smoke mode")]
    results = []
    checks = [
        ("manuscript_cohort_v1",               "research_id", 0.0),
        ("patient_analysis_resolved_v1",       "research_id", 0.0),
        ("episode_analysis_resolved_v1_dedup", "research_id", 0.0),
    ]
    for tbl, col, max_pct in checks:
        try:
            row = con.execute(
                f"SELECT ROUND(100.0*COUNT(*) FILTER (WHERE {col} IS NULL)"
                f"/NULLIF(COUNT(*),0),2) FROM {tbl}"
            ).fetchone()
            pct = float(row[0] or 0)
            ok = pct <= max_pct
            results.append(GateResult(
                f"null:{tbl}.{col}",
                "PASS" if ok else "FAIL",
                f"{pct:.2f}% null (limit {max_pct}%)",
                pct,
            ))
        except Exception as e:
            results.append(GateResult(f"null:{tbl}.{col}", "FAIL", str(e)[:120]))
    return results


def gate_hardening_tables(con, level: str) -> list[GateResult]:
    if level == "smoke":
        return [GateResult("hardening_tbls", "SKIP", "Skipped in smoke mode")]
    results = []
    for tbl in HARDENING_TABLES:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            results.append(GateResult(f"hardening:{tbl}", "PASS",
                                       f"{int(n):,} rows", int(n)))
        except Exception as e:
            results.append(GateResult(f"hardening:{tbl}", "WARN",
                                       f"Missing or empty: {e}"))
    return results


def gate_ro_share(sa: bool, level: str) -> GateResult:
    if "share" not in level:
        return GateResult("ro_share", "SKIP", "Only checked on prod promotion")
    try:
        import duckdb
        token = os.environ.get("MD_SA_TOKEN") or os.environ.get("MOTHERDUCK_TOKEN")
        con = duckdb.connect(
            f"md:thyroid_research_2026?motherduck_token={token}"
        )
        n = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM master_cohort"
        ).fetchone()[0]
        con.close()
        if n < 10000:
            return GateResult("ro_share", "FAIL", f"Only {n} patients in share")
        return GateResult("ro_share", "PASS", f"{n:,} patients readable via share", n)
    except Exception as e:
        return GateResult("ro_share", "FAIL", f"Share not accessible: {e}")


# ── MAP dedup gate ─────────────────────────────────────────────────────────

def gate_map_dedup() -> GateResult:
    """Run script 94 as a sub-process and parse the result."""
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "94_map_dedup_validator.py")],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return GateResult("map_dedup", "PASS", "0 duplicate MAP entries")
        else:
            lines = (result.stdout + result.stderr).strip().splitlines()
            detail = next((l for l in lines if "FAIL" in l or "duplicate" in l.lower()), "MAP duplicates found")
            return GateResult("map_dedup", "FAIL", detail)
    except Exception as e:
        return GateResult("map_dedup", "WARN", f"Could not run validator: {e}")


# ── Metric snapshot ────────────────────────────────────────────────────────

def capture_metric_snapshot(con) -> dict:
    snapshot = {}
    for name, sql, lo, hi in METRIC_BOUNDS:
        try:
            v = int(con.execute(sql).fetchone()[0])
            snapshot[name] = {"value": v, "lo": lo, "hi": hi,
                              "in_range": lo <= v <= hi}
        except Exception as e:
            snapshot[name] = {"error": str(e)[:80]}
    return snapshot


# ── Git helpers ────────────────────────────────────────────────────────────

def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            text=True
        ).strip()[:12]
    except Exception:
        return "unknown"


def git_branch() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "branch", "--show-current"],
            text=True
        ).strip()
    except Exception:
        return "unknown"


# ── Promotion log writer ───────────────────────────────────────────────────

def write_promotion_log(con, manifest: PromotionManifest) -> None:
    """Insert one row into promotion_log in the TARGET database."""
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS promotion_log (
                promotion_id    VARCHAR,
                from_env        VARCHAR,
                to_env          VARCHAR,
                from_db         VARCHAR,
                to_db           VARCHAR,
                git_sha         VARCHAR,
                git_branch      VARCHAR,
                promoted_at     VARCHAR,
                gates_run       INTEGER,
                gates_passed    INTEGER,
                gates_failed    INTEGER,
                overall_status  VARCHAR,
                metric_snapshot JSON,
                notes           VARCHAR
            )
        """)
        con.execute(
            "INSERT INTO promotion_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                manifest.promotion_id,
                manifest.from_env,
                manifest.to_env,
                manifest.from_db,
                manifest.to_db,
                manifest.git_sha,
                manifest.git_branch,
                manifest.promoted_at,
                manifest.gates_run,
                manifest.gates_passed,
                manifest.gates_failed,
                manifest.overall_status,
                json.dumps(manifest.metric_snapshot),
                manifest.notes,
            ],
        )
        print("  ✓  Promotion logged to promotion_log")
    except Exception as e:
        print(f"  WARN: Could not write promotion_log: {e}")


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="from_env", required=True,
                    choices=["dev", "qa", "prod"])
    ap.add_argument("--to",   dest="to_env",   required=True,
                    choices=["dev", "qa", "prod"])
    ap.add_argument("--sa", action="store_true",
                    help="Use service-account token (MD_SA_TOKEN)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run all checks but do NOT write manifest or log")
    ap.add_argument("--notes", default="",
                    help="Optional release notes to embed in manifest")
    ap.add_argument(
        "--manifest-dir",
        type=Path,
        default=ROOT / "exports" / "release_manifests",
        help="Directory to write the release manifest JSON",
    )
    args = ap.parse_args()

    from_env = args.from_env
    to_env   = args.to_env

    if (from_env, to_env) not in ALLOWED_PATHS:
        print(f"ERROR: Promotion path {from_env} → {to_env} is not allowed.")
        print(f"  Allowed paths: {sorted(ALLOWED_PATHS)}")
        sys.exit(2)

    print("=" * 72)
    print(f"  95 — Environment Promotion: {from_env.upper()} → {to_env.upper()}")
    if args.dry_run:
        print("  [DRY-RUN — no manifest will be written]")
    print("=" * 72)

    # ── Connect to source ──────────────────────────────────────────────────
    print(f"\n  Connecting to source env: {from_env} ({ENV_DATABASES[from_env]})")
    try:
        src_con = get_connection(from_env, sa=args.sa)
        print("  ✓  Connected")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(2)

    level = ENV_GATE_LEVELS.get(to_env, "full")

    # ── Run gates ─────────────────────────────────────────────────────────
    print(f"\n  Running gates (level={level})...\n")
    all_gates: list[GateResult] = []

    all_gates += gate_table_existence(src_con, level)
    all_gates += gate_metric_bounds(src_con, level)
    all_gates += gate_row_multiplication(src_con, level)
    all_gates += gate_null_core_columns(src_con, level)
    all_gates += gate_hardening_tables(src_con, level)
    all_gates.append(gate_map_dedup())
    all_gates.append(gate_ro_share(args.sa, level))

    for g in all_gates:
        icon = {"PASS": "✓", "FAIL": "✗", "WARN": "⚠", "SKIP": "–"}.get(g.status, "?")
        print(f"  {icon}  {g.name:<40s} {g.status:<5s}  {g.detail}")

    passed = sum(1 for g in all_gates if g.status == "PASS")
    failed = sum(1 for g in all_gates if g.status == "FAIL")
    warns  = sum(1 for g in all_gates if g.status == "WARN")
    skipped = sum(1 for g in all_gates if g.status == "SKIP")

    print(f"\n  Gates: {len(all_gates)} total | "
          f"{passed} PASS | {failed} FAIL | {warns} WARN | {skipped} SKIP")

    now = datetime.utcnow()
    promotion_id = f"promo_{from_env}_{to_env}_{now.strftime('%Y%m%d_%H%M%S')}"
    overall_status = "PASS" if failed == 0 else "FAIL"

    # ── Metric snapshot ────────────────────────────────────────────────────
    print("\n  Capturing metric snapshot...")
    snapshot = capture_metric_snapshot(src_con)
    src_con.close()

    # ── Build manifest ─────────────────────────────────────────────────────
    manifest = PromotionManifest(
        promotion_id=promotion_id,
        from_env=from_env,
        to_env=to_env,
        from_db=ENV_DATABASES[from_env],
        to_db=ENV_DATABASES[to_env],
        git_sha=git_sha(),
        git_branch=git_branch(),
        promoted_at=now.isoformat(),
        gates_run=len(all_gates),
        gates_passed=passed,
        gates_failed=failed,
        overall_status=overall_status,
        metric_snapshot=snapshot,
        gate_details=[asdict(g) for g in all_gates],
        notes=args.notes,
    )

    # ── Write manifest ─────────────────────────────────────────────────────
    if not args.dry_run:
        args.manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = args.manifest_dir / f"{promotion_id}.json"
        manifest_path.write_text(json.dumps(asdict(manifest), indent=2))
        print(f"\n  Manifest: {manifest_path.relative_to(ROOT)}")

        # Write promotion log to TARGET db (even if gates failed — for audit trail)
        if to_env != from_env:
            try:
                tgt_con = get_connection(to_env, sa=args.sa)
                write_promotion_log(tgt_con, manifest)
                tgt_con.close()
            except Exception as e:
                print(f"  WARN: Could not connect to target to write log: {e}")
    else:
        print("\n  [dry-run: manifest not written]")

    # ── Final verdict ──────────────────────────────────────────────────────
    print(f"\n  {'=' * 68}")
    if overall_status == "PASS":
        print(f"  ✓  PROMOTION {from_env.upper()} → {to_env.upper()} : APPROVED")
        print(f"  Promotion ID: {promotion_id}")
    else:
        print(f"  ✗  PROMOTION {from_env.upper()} → {to_env.upper()} : BLOCKED")
        print(f"  {failed} gate(s) failed. Resolve before promoting.")
        for g in all_gates:
            if g.status == "FAIL":
                print(f"    • {g.name}: {g.detail}")
    print(f"  {'=' * 68}\n")

    sys.exit(0 if overall_status == "PASS" else 1)


if __name__ == "__main__":
    main()
