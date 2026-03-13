#!/usr/bin/env python3
"""
Smoke test for Thyroid Cohort Explorer dashboard.

Verifies that the MotherDuck connection works, critical tables exist,
health monitoring tables are populated, and key queries succeed.

Usage:
    # Against MotherDuck RO share (default):
    .venv/bin/python scripts/smoke_test_dashboard.py

    # Against local DuckDB:
    USE_LOCAL_DUCKDB=1 .venv/bin/python scripts/smoke_test_dashboard.py

    # Quiet mode (exit code only):
    .venv/bin/python scripts/smoke_test_dashboard.py --quiet

Exit codes: 0 = all pass, 1 = critical failure, 2 = warnings only
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

SHARE_PATH = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
DATABASE = "thyroid_research_2026"
SHARE_CATALOG = "thyroid_share"


@dataclass
class SmokeResult:
    name: str
    passed: bool
    message: str = ""
    elapsed_ms: float = 0.0
    critical: bool = True


@dataclass
class SmokeReport:
    results: list[SmokeResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def critical_passed(self) -> bool:
        return all(r.passed for r in self.results if r.critical)

    @property
    def n_pass(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def n_fail(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def add(self, result: SmokeResult) -> None:
        self.results.append(result)

    def summary(self) -> str:
        lines = [f"\n{'='*60}", "SMOKE TEST REPORT", f"{'='*60}"]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            crit = " [CRITICAL]" if r.critical and not r.passed else ""
            lines.append(f"  [{status}] {r.name}{crit} ({r.elapsed_ms:.0f}ms) {r.message}")
        lines.append(f"{'='*60}")
        lines.append(f"Total: {self.n_pass} pass, {self.n_fail} fail")
        if self.critical_passed:
            lines.append("STATUS: READY" if self.all_passed else "STATUS: WARNINGS (non-critical failures)")
        else:
            lines.append("STATUS: CRITICAL FAILURES — dashboard may not load correctly")
        return "\n".join(lines)


def _timed(fn, name: str, critical: bool = True) -> SmokeResult:
    t0 = time.perf_counter()
    try:
        msg = fn()
        elapsed = (time.perf_counter() - t0) * 1000
        return SmokeResult(name=name, passed=True, message=msg or "", elapsed_ms=elapsed, critical=critical)
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return SmokeResult(name=name, passed=False, message=str(e)[:200], elapsed_ms=elapsed, critical=critical)


def _ensure_token() -> None:
    if os.getenv("MOTHERDUCK_TOKEN"):
        return
    secrets_path = Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml
            secrets = toml.load(str(secrets_path))
            token = secrets.get("MOTHERDUCK_TOKEN", "")
            if token:
                os.environ["MOTHERDUCK_TOKEN"] = token
                return
        except Exception:
            pass
    raise RuntimeError("MOTHERDUCK_TOKEN not found in env or .streamlit/secrets.toml")


def run_smoke_tests(quiet: bool = False) -> SmokeReport:
    report = SmokeReport()

    # 0. Token availability
    def check_token():
        _ensure_token()
        return "token resolved"
    report.add(_timed(check_token, "Token available"))
    if not report.results[-1].passed:
        return report

    from motherduck_client import MotherDuckClient, MotherDuckConfig
    import duckdb

    # 1. RO share connection
    con = None
    catalog = DATABASE

    def check_ro_connect():
        nonlocal con, catalog
        if os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
            con = duckdb.connect(os.getenv("LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb"))
            catalog = DATABASE
            return "local DuckDB"
        cfg = MotherDuckConfig(database=DATABASE, share_path=SHARE_PATH)
        cli = MotherDuckClient(cfg)
        con = cli.connect_ro_share()
        con.execute(f"USE {SHARE_CATALOG};")
        catalog = SHARE_CATALOG
        return f"connected to {SHARE_CATALOG}"
    report.add(_timed(check_ro_connect, "RO share connection"))
    if con is None:
        return report

    # 2. Critical tables exist
    CRITICAL_TABLES = [
        "master_cohort",
        "path_synoptics",
        "tumor_episode_master_v2",
        "molecular_episode_v3",
        "rai_episode_v3",
        "manuscript_cohort_v1",
        "patient_analysis_resolved_v1",
        "episode_analysis_resolved_v1_dedup",
        "streamlit_patient_header_v",
        "advanced_features_sorted",
    ]
    for tbl in CRITICAL_TABLES:
        def check_tbl(t=tbl):
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            return f"{n:,} rows"
        report.add(_timed(check_tbl, f"Table: {tbl}"))

    # 3. Health/validation tables exist
    HEALTH_TABLES = [
        "val_dataset_integrity_summary_v1",
        "val_provenance_completeness_v2",
        "val_episode_linkage_completeness_v1",
        "val_lab_completeness_v1",
        "val_hardening_summary",
    ]
    for tbl in HEALTH_TABLES:
        def check_health(t=tbl):
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            if n == 0:
                raise ValueError("table exists but is empty")
            return f"{n} rows"
        report.add(_timed(check_health, f"Health: {tbl}", critical=False))

    # 4. Patient timeline query
    def check_timeline():
        df = con.execute("""
            SELECT research_id, event_type, event_date
            FROM streamlit_patient_timeline_v
            WHERE research_id = 1
            LIMIT 10
        """).fetchdf()
        return f"{len(df)} timeline rows for patient 1"
    report.add(_timed(check_timeline, "Patient timeline query"))

    # 5. Overview KPI query
    def check_overview():
        n = con.execute("SELECT COUNT(DISTINCT research_id) FROM master_cohort").fetchone()[0]
        if n < 10000:
            raise ValueError(f"Expected 10k+ patients, got {n}")
        return f"{n:,} patients"
    report.add(_timed(check_overview, "Overview patient count"))

    # 6. Advanced features load
    def check_features():
        cols = con.execute("SELECT COUNT(*) FROM advanced_features_sorted").fetchone()[0]
        return f"{cols:,} rows"
    report.add(_timed(check_features, "Advanced features load"))

    # 7. Manuscript cohort
    def check_manuscript():
        n = con.execute("SELECT COUNT(*) FROM manuscript_cohort_v1").fetchone()[0]
        if n < 10000:
            raise ValueError(f"Expected 10k+ rows, got {n}")
        return f"{n:,} rows"
    report.add(_timed(check_manuscript, "Manuscript cohort"))

    # 8. Scoring systems
    def check_scoring():
        n = con.execute("""
            SELECT COUNT(*) FROM thyroid_scoring_py_v1
            WHERE ajcc8_stage_group IS NOT NULL
        """).fetchone()[0]
        return f"{n:,} scored patients"
    report.add(_timed(check_scoring, "Scoring systems", critical=False))

    con.close()
    return report


def main():
    parser = argparse.ArgumentParser(description="Dashboard smoke test")
    parser.add_argument("--quiet", action="store_true", help="Suppress output, exit code only")
    args = parser.parse_args()

    report = run_smoke_tests(quiet=args.quiet)

    if not args.quiet:
        print(report.summary())

    if not report.critical_passed:
        sys.exit(1)
    elif not report.all_passed:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
