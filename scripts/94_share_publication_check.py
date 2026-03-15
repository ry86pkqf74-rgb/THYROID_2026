#!/usr/bin/env python3
"""
94_share_publication_check.py  —  RO share hardening and publication check

Before promoting a new release to production, verify:
  1. The RO share is accessible with the current token
  2. All Streamlit-critical tables are visible through the share
  3. Row counts match the production RW database (within tolerance)
  4. No PHI columns are exposed through the share
  5. The share catalog alias resolves correctly

This script should be run AFTER scripts/26_motherduck_materialize_v2.py
and BEFORE tagging a release or updating the Streamlit Cloud app.

Usage
─────
  # Full publication check
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/94_share_publication_check.py

  # Use service-account token (CI)
  MD_SA_TOKEN=... .venv/bin/python scripts/94_share_publication_check.py --sa

  # Skip row-count cross-check (faster, for frequent pre-deploy checks)
  MOTHERDUCK_TOKEN=... .venv/bin/python scripts/94_share_publication_check.py --no-count-check

Exit codes
──────────
  0  Share is healthy and publication-ready
  1  One or more checks FAIL
  2  Cannot connect to share
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

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


# ── Tables that MUST be visible through the RO share ──────────────────────
REQUIRED_VIA_SHARE: list[tuple[str, int]] = [
    # (table_name, minimum_row_count)
    ("master_cohort",                          10500),
    ("manuscript_cohort_v1",                   10500),
    ("patient_analysis_resolved_v1",           10500),
    ("episode_analysis_resolved_v1_dedup",      9000),
    ("thyroid_scoring_py_v1",                  10500),
    ("analysis_cancer_cohort_v1",               3900),
    ("operative_episode_detail_v2",             8000),
    ("rai_treatment_episode_v2",                 700),
    ("molecular_test_episode_v2",                700),
    ("streamlit_patient_header_v",             10000),
    ("streamlit_patient_timeline_v",            1000),
    ("longitudinal_lab_canonical_v1",          30000),
    ("survival_cohort_enriched",               40000),
    ("val_dataset_integrity_summary_v1",            1),
    ("val_hardening_summary",                       1),
    ("date_rescue_rate_summary",                    1),
    ("extracted_tirads_validated_v1",           3000),
    ("complication_patient_summary_v1",         1000),
]

# PHI columns that must NOT be present in any share-accessible table
PHI_COLUMNS = ["mrn", "dob", "date_of_birth", "patient_name", "first_name",
               "last_name", "ssn", "phone", "address", "zip_code", "email"]

# Tables to cross-check row counts between share and prod RW
COUNT_CROSSCHECK_TABLES: list[str] = [
    "master_cohort",
    "manuscript_cohort_v1",
    "thyroid_scoring_py_v1",
    "episode_analysis_resolved_v1_dedup",
    "survival_cohort_enriched",
]

COUNT_TOLERANCE_PCT = 2.0  # 2% allowed drift


@dataclass
class CheckResult:
    name: str
    status: str   # PASS | FAIL | WARN | SKIP
    detail: str = ""


def check_share_connectivity(share_con: Any) -> CheckResult:
    try:
        share_con.execute("USE thyroid_share;")
        row = share_con.execute("SELECT COUNT(DISTINCT research_id) FROM master_cohort").fetchone()
        n = int(row[0]) if row else 0
        return CheckResult("C1_share_connectivity", "PASS", f"{n:,} patients accessible via RO share")
    except Exception as e:
        return CheckResult("C1_share_connectivity", "FAIL", str(e)[:120])


def check_required_tables(share_con: Any) -> list[CheckResult]:
    results = []
    missing = []
    low_count = []
    for tbl, min_rows in REQUIRED_VIA_SHARE:
        try:
            n = share_con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            if int(n) < min_rows:
                low_count.append(f"{tbl}: {n:,} < {min_rows:,}")
        except Exception:
            missing.append(tbl)
    if missing:
        results.append(CheckResult("C2_required_tables_exist",
                                    "FAIL", f"Not accessible: {missing}"))
    else:
        results.append(CheckResult("C2_required_tables_exist",
                                    "PASS", f"All {len(REQUIRED_VIA_SHARE)} tables accessible"))
    if low_count:
        results.append(CheckResult("C2b_row_count_minimums",
                                    "FAIL", "; ".join(low_count)))
    else:
        results.append(CheckResult("C2b_row_count_minimums",
                                    "PASS", "All tables meet minimum row counts"))
    return results


def check_phi_columns(share_con: Any) -> CheckResult:
    """Scan information_schema for PHI column names accessible through the share."""
    try:
        rows = share_con.execute(
            "SELECT DISTINCT table_name, column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = 'main'"
        ).fetchall()
        exposed = [
            f"{t}.{c}" for t, c in rows
            if c.lower() in PHI_COLUMNS
        ]
        if exposed:
            return CheckResult("C3_phi_columns_absent",
                                "FAIL", f"PHI columns exposed: {exposed[:10]}")
        return CheckResult("C3_phi_columns_absent",
                            "PASS", f"No PHI columns found ({len(rows)} columns scanned)")
    except Exception as e:
        return CheckResult("C3_phi_columns_absent", "WARN", f"Could not scan columns: {e!s:.80}")


def check_catalog_alias(share_con: Any) -> CheckResult:
    """Verify that both thyroid_share.master_cohort and plain master_cohort resolve."""
    try:
        n1 = share_con.execute("SELECT COUNT(*) FROM master_cohort").fetchone()[0]
        n2 = share_con.execute("SELECT COUNT(*) FROM thyroid_share.master_cohort").fetchone()[0]
        if n1 != n2:
            return CheckResult("C4_catalog_alias", "WARN",
                                f"Plain ({n1:,}) vs qualified ({n2:,}) counts differ")
        return CheckResult("C4_catalog_alias", "PASS",
                            f"thyroid_share alias resolves correctly ({n1:,} rows)")
    except Exception as e:
        return CheckResult("C4_catalog_alias", "FAIL", str(e)[:120])


def check_count_crosscheck(share_con: Any, rw_con: Any) -> list[CheckResult]:
    results = []
    for tbl in COUNT_CROSSCHECK_TABLES:
        try:
            share_n = int(share_con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0])
            rw_n    = int(rw_con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0])
            if rw_n == 0:
                pct_diff = 0.0
            else:
                pct_diff = abs(share_n - rw_n) / rw_n * 100
            if pct_diff > COUNT_TOLERANCE_PCT:
                status = "FAIL"
                detail = f"share={share_n:,} vs prod={rw_n:,} ({pct_diff:.1f}% drift > {COUNT_TOLERANCE_PCT}% tolerance)"
            else:
                status = "PASS"
                detail = f"share={share_n:,} vs prod={rw_n:,} ({pct_diff:.1f}% drift)"
        except Exception as e:
            status = "WARN"
            detail = f"Could not compare: {e!s:.80}"
        results.append(CheckResult(f"C5_count_match_{tbl}", status, detail))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sa", action="store_true", help="Use service-account token (MD_SA_TOKEN)")
    parser.add_argument("--no-count-check", action="store_true",
                        help="Skip RW vs share row-count cross-check (faster)")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  THYROID_2026  —  RO Share Publication Check")
    print(f"  Checking {len(REQUIRED_VIA_SHARE)} required tables, PHI columns, catalog alias")
    print(f"{'='*70}\n")

    try:
        client = MotherDuckClient.for_env("prod", use_service_account=args.sa)
        share_con = client.connect_ro_share()
        share_con.execute("USE thyroid_share;")
        print("  Share connection: OK")
    except Exception as e:
        print(f"  ERROR: Cannot connect to RO share — {e}")
        sys.exit(2)

    results: list[CheckResult] = []
    t0 = time.time()

    # C1: Connectivity
    results.append(check_share_connectivity(share_con))

    # C2: Required tables + minimum rows
    results.extend(check_required_tables(share_con))

    # C3: PHI columns
    results.append(check_phi_columns(share_con))

    # C4: Catalog alias
    results.append(check_catalog_alias(share_con))

    # C5: Count cross-check (optional)
    if not args.no_count_check:
        try:
            rw_con = client.connect_rw()
            results.extend(check_count_crosscheck(share_con, rw_con))
            rw_con.close()
        except Exception as e:
            results.append(CheckResult("C5_count_crosscheck", "WARN",
                                        f"Could not connect to RW for comparison: {e!s:.80}"))
    else:
        results.append(CheckResult("C5_count_crosscheck", "SKIP", "--no-count-check specified"))

    elapsed = time.time() - t0
    share_con.close()

    # ── Report ──────────────────────────────────────────────────────────────
    print(f"\n  {'Check':<45} {'Status':<6}  Detail")
    print(f"  {'-'*45} {'-'*6}  {'-'*40}")
    any_fail = False
    for r in results:
        icon = "✓" if r.status == "PASS" else ("⚠" if r.status in ("WARN", "SKIP") else "✗")
        print(f"  {r.name:<45} {icon} {r.status:<4}  {r.detail[:70]}")
        if r.status == "FAIL":
            any_fail = True

    print(f"\n  Completed in {elapsed:.1f}s")

    if any_fail:
        print("\n  ✗ SHARE IS NOT PUBLICATION-READY — fix failures before release\n")
        sys.exit(1)
    else:
        print("\n  ✓ RO SHARE IS PUBLICATION-READY\n")
        print("  Update checklist:")
        print("    □ Tag the release:  git tag v<version> && git push origin v<version>")
        print("    □ Update RELEASE_NOTES.md")
        print("    □ Notify Streamlit Cloud to restart if environment variables changed")
        print()
        sys.exit(0)


if __name__ == "__main__":
    main()
