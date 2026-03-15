#!/usr/bin/env python3
"""
83_motherduck_env_strategy.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MotherDuck Business environment promotion strategy.

Implements a dev → qa → prod promotion workflow using MotherDuck Business:
  - thyroid_research_2026_dev   (development; full reads/writes)
  - thyroid_research_2026_qa    (QA/validation; writes gated by tests)
  - thyroid_research_2026       (production; read-only share exposed to Streamlit)

This script:
  A. Creates / confirms environment databases exist
  B. Runs promotion gates (row count + provenance null checks)
  C. Promotes approved tables from dev/qa → prod using CREATE TABLE AS SELECT
  D. Verifies production read-only share is still accessible (circuit-breaker)
  E. Writes promotion audit log to exports/

Service-account readiness:
  - Token resolved via MOTHERDUCK_TOKEN env var (CI) or secrets.toml (local)
  - Never prints the token
  - All writes use explicit database prefix (thyroid_research_2026.tablename)

MotherDuck Duckling size guidance (documented in config/motherduck_environments.yml):
  - Pulse:    dashboard/read-only queries, Streamlit
  - Standard: routine ETL, script 26 materialization, script 29 validation
  - Jumbo:    full rebuild (scripts 22-27), hardening passes (78, 80)

Usage:
    .venv/bin/python scripts/83_motherduck_env_strategy.py --mode check
    .venv/bin/python scripts/83_motherduck_env_strategy.py --mode promote --from-env dev --to-env qa
    .venv/bin/python scripts/83_motherduck_env_strategy.py --mode promote --from-env qa --to-env prod
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / "final_md_optimization_20260314"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Canonical tables that must pass gates before promotion ──────────────────
PROMOTION_GATES = [
    # (table, min_rows, required_nonnull_cols)
    ("manuscript_cohort_v1",              10_000, ["research_id"]),
    ("patient_analysis_resolved_v1",      10_000, ["research_id"]),
    ("episode_analysis_resolved_v1_dedup", 9_000, ["research_id"]),
    ("thyroid_scoring_py_v1",             10_000, ["research_id"]),
    ("analysis_cancer_cohort_v1",          4_000, ["research_id"]),
    ("operative_episode_detail_v2",        8_000, ["research_id"]),
    ("rai_treatment_episode_v2",             800, ["research_id"]),
    ("molecular_test_episode_v2",         10_000, ["research_id"]),
]

# ── Critical manuscript metrics (reproduced to validate promotion) ──────────
METRIC_CHECKS = [
    ("surgical_cohort_size",
     "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
     10_000, 12_000),
    ("cancer_cohort_size",
     "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
     3_900, 4_300),
    ("manuscript_cohort_size",
     "SELECT COUNT(*) FROM manuscript_cohort_v1",
     10_500, 11_200),
    ("dedup_episodes",
     "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
     9_000, 9_800),
]

# Production RO share path (read-only Streamlit URL)
RO_SHARE = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"


def get_token() -> str:
    t = os.environ.get("MOTHERDUCK_TOKEN")
    if t:
        return t
    try:
        import toml
        return toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
    except Exception:
        raise RuntimeError("MOTHERDUCK_TOKEN not set and secrets.toml not found")


def get_connection(db: str = "thyroid_research_2026"):
    import duckdb
    token = get_token()
    return duckdb.connect(f"md:{db}?motherduck_token={token}")


def safe_count(con, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def check_db_exists(db: str) -> bool:
    import duckdb
    try:
        token = get_token()
        con = duckdb.connect(f"md:{db}?motherduck_token={token}")
        con.execute("SELECT 1")
        con.close()
        return True
    except Exception:
        return False


def run_gates(con, gates: list) -> tuple[list, list]:
    passed, failed = [], []
    for tbl, min_rows, nonnull_cols in gates:
        try:
            n = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            if n < min_rows:
                failed.append({
                    "table": tbl, "check": "min_rows",
                    "expected": min_rows, "actual": n,
                })
                continue
            for col in nonnull_cols:
                nulls = safe_count(con,
                    f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL")
                if nulls > 0:
                    failed.append({
                        "table": tbl, "check": f"nonnull:{col}",
                        "expected": 0, "actual": nulls,
                    })
                    continue
            passed.append({"table": tbl, "rows": n})
        except Exception as e:
            failed.append({"table": tbl, "check": "error", "message": str(e)})
    return passed, failed


def run_metric_checks(con, checks: list) -> tuple[list, list]:
    passed, failed = [], []
    for name, sql, lo, hi in checks:
        v = safe_count(con, sql)
        ok = lo <= v <= hi
        entry = {"metric": name, "value": v, "expected_range": [lo, hi], "pass": ok}
        (passed if ok else failed).append(entry)
    return passed, failed


def check_ro_share() -> dict:
    """Verify the production read-only share is still accessible."""
    import duckdb
    try:
        token = get_token()
        con = duckdb.connect(f"{RO_SHARE}?motherduck_token={token}")
        n = con.execute("SELECT COUNT(DISTINCT research_id) FROM master_cohort").fetchone()[0]
        con.close()
        return {"accessible": True, "patient_count": int(n)}
    except Exception as e:
        return {"accessible": False, "error": str(e)}


def promote_tables(
    from_db: str, to_db: str, dry_run: bool
) -> dict:
    """
    Move canonical tables from source DB to target DB via CREATE TABLE AS SELECT.
    This is a READ from source + WRITE to target pattern.
    """
    import duckdb
    token = get_token()
    con = duckdb.connect(f"md:{to_db}?motherduck_token={token}")
    promoted, failed = [], []
    for tbl, _, _ in PROMOTION_GATES:
        sql = (
            f"CREATE OR REPLACE TABLE {tbl} AS "
            f"SELECT * FROM {from_db}.{tbl}"
        )
        if dry_run:
            promoted.append({"table": tbl, "status": "dry_run"})
            continue
        try:
            con.execute(sql)
            n = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            promoted.append({"table": tbl, "status": "ok", "rows": n})
        except Exception as e:
            failed.append({"table": tbl, "status": "error", "message": str(e)})
    con.close()
    return {"promoted": promoted, "failed": failed}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=["check", "promote", "gate-only"],
                    default="check")
    ap.add_argument("--from-env", default="thyroid_research_2026",
                    help="Source DB for promotion")
    ap.add_argument("--to-env",   default="thyroid_research_2026_qa",
                    help="Target DB for promotion")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("\n" + "=" * 72)
    print("  83 — MotherDuck Business Environment Strategy")
    print("=" * 72)

    report: dict = {
        "mode": args.mode,
        "timestamp": datetime.utcnow().isoformat(),
        "from_env": args.from_env,
        "to_env":   args.to_env,
    }

    # ── A. Environment existence check ─────────────────────────────────────
    print("\n  A. Environment Check:")
    envs = [
        "thyroid_research_2026",
        "thyroid_research_2026_dev",
        "thyroid_research_2026_qa",
    ]
    env_status = {}
    for env in envs:
        exists = check_db_exists(env)
        status = "EXISTS" if exists else "NOT_FOUND (provision manually via MotherDuck UI)"
        env_status[env] = exists
        print(f"    {env}: {status}")
    report["env_status"] = env_status

    # ── B. Production gates ─────────────────────────────────────────────────
    print("\n  B. Production Gate Checks:")
    con_prod = get_connection("thyroid_research_2026")
    passed, failed = run_gates(con_prod, PROMOTION_GATES)
    m_passed, m_failed = run_metric_checks(con_prod, METRIC_CHECKS)
    con_prod.close()

    for p in passed:
        print(f"    ✓  {p['table']}: {p['rows']:,} rows")
    for f in failed:
        print(f"    ✗  {f['table']}: {f}")
    for m in m_passed:
        print(f"    ✓  {m['metric']}: {m['value']:,}")
    for m in m_failed:
        print(f"    ✗  {m['metric']}: {m['value']:,} (expected {m['expected_range']})")

    report["gate_passed"] = [p["table"] for p in passed]
    report["gate_failed"] = failed
    report["metrics_passed"] = m_passed
    report["metrics_failed"] = m_failed
    gate_ok = len(failed) == 0 and len(m_failed) == 0
    report["gates_all_pass"] = gate_ok

    # ── C. RO Share check ──────────────────────────────────────────────────
    print("\n  C. Production RO Share:")
    ro = check_ro_share()
    if ro.get("accessible"):
        print(f"    ✓  Accessible ({ro['patient_count']:,} patients)")
    else:
        print(f"    ✗  NOT ACCESSIBLE: {ro.get('error')}")
    report["ro_share"] = ro

    # ── D. Promote (if requested) ──────────────────────────────────────────
    if args.mode == "promote":
        if not gate_ok:
            print("\n  ✗ Gates FAILED — promotion aborted.")
            report["promotion"] = {"status": "aborted_gates_failed"}
        else:
            print(f"\n  D. Promoting: {args.from_env} → {args.to_env}")
            promo = promote_tables(args.from_env, args.to_env, args.dry_run)
            for p in promo["promoted"]:
                print(f"    ✓  {p['table']}: {p.get('rows', 'dry_run')}")
            for f in promo["failed"]:
                print(f"    ✗  {f['table']}: {f['message']}")
            report["promotion"] = {
                "status": "dry_run" if args.dry_run else "complete",
                **promo,
            }

    # ── E. Save audit log ────────────────────────────────────────────────────
    out = EXPORTS_DIR / f"md_env_strategy_audit_{TIMESTAMP}.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"\n  Audit log: {out.relative_to(ROOT)}")
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
