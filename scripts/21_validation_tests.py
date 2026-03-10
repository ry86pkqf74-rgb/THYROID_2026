#!/usr/bin/env python3
"""
21_validation_tests.py — Phase N: Validation / Smoke Tests

Validates the full adjudication + export pipeline:
  1. All streamlit_* views return rows
  2. patient_reconciliation_summary_v is non-empty
  3. Reviewer tables are accessible
  4. Post-review overlays prefer adjudicated values when decisions exist
  5. Manuscript views only include intended rows
  6. Export generates files with manifest

Run after scripts 15-20.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def test_pass(name: str) -> None:
    print(f"  PASS  {name}")


def test_fail(name: str, detail: str = "") -> None:
    msg = f"  FAIL  {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase N: Validation Tests")
    parser.add_argument("--md", action="store_true",
                        help="Use MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    print("=" * 80)
    print("  VALIDATION TESTS — Phase N")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    passed = 0
    failed = 0

    # ── Test 1: Streamlit support views ──────────────────────────────────
    section("TEST 1: Streamlit Support Views")
    streamlit_views = [
        "streamlit_patient_header_v",
        "streamlit_patient_timeline_v",
        "streamlit_patient_conflicts_v",
        "streamlit_patient_manual_review_v",
        "streamlit_cohort_qc_summary_v",
    ]
    for v in streamlit_views:
        if not table_available(con, v):
            test_fail(f"{v} exists")
            failed += 1
            continue
        cnt = con.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0]
        if cnt > 0:
            test_pass(f"{v} has {cnt:,} rows")
            passed += 1
        else:
            test_fail(f"{v} is empty")
            failed += 1

    # ── Test 2: patient_reconciliation_summary_v ─────────────────────────
    section("TEST 2: Patient Reconciliation Summary")
    if table_available(con, "patient_reconciliation_summary_v"):
        cnt = con.execute("SELECT COUNT(*) FROM patient_reconciliation_summary_v").fetchone()[0]
        if cnt > 0:
            test_pass(f"patient_reconciliation_summary_v has {cnt:,} rows")
            passed += 1
        else:
            test_fail("patient_reconciliation_summary_v is empty")
            failed += 1
    else:
        test_fail("patient_reconciliation_summary_v missing")
        failed += 1

    # ── Test 3: Reviewer tables accessible ───────────────────────────────
    section("TEST 3: Reviewer Tables")
    for tbl in ["adjudication_decisions", "adjudication_decision_history"]:
        if table_available(con, tbl):
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            test_pass(f"{tbl} accessible ({cnt:,} rows)")
            passed += 1
        else:
            test_fail(f"{tbl} not accessible — run script 19")
            failed += 1

    # ── Test 4: Post-review overlay logic ────────────────────────────────
    section("TEST 4: Post-Review Overlay Logic")

    if table_available(con, "adjudication_decisions") and table_available(con, "histology_post_review_v"):
        try:
            # Get a real research_id + op_seq from histology_analysis_cohort_v
            sample = con.execute("""
                SELECT CAST(research_id AS BIGINT), CAST(op_seq AS VARCHAR)
                FROM histology_analysis_cohort_v
                WHERE analysis_eligible_flag = TRUE
                LIMIT 1
            """).fetchone()

            if sample:
                test_rid, test_op = sample

                # Insert test decision
                con.execute("""
                    INSERT INTO adjudication_decisions (
                        research_id, review_domain, linked_episode_id,
                        conflict_type, reviewer_action, reviewer_resolution_status,
                        final_value_selected, reviewer_name, source_view, active_flag
                    ) VALUES (?, 'histology', ?, 'test', 'test_override', 'resolved',
                              'TEST_VALUE', 'validation_test', 'test', TRUE)
                """, [test_rid, test_op])

                # Verify overlay picks up the decision
                result = con.execute(f"""
                    SELECT effective_histology, adjudication_applied_flag, value_source
                    FROM histology_post_review_v
                    WHERE CAST(research_id AS BIGINT) = {test_rid}
                      AND CAST(op_seq AS VARCHAR) = '{test_op}'
                """).fetchone()

                if result:
                    eff_val, adj_flag, src = result
                    if eff_val == "TEST_VALUE" and adj_flag is True and src == "reviewer":
                        test_pass("Post-review overlay prefers adjudicated value")
                        passed += 1
                    else:
                        test_fail(f"Overlay returned {eff_val}, adj={adj_flag}, src={src}")
                        failed += 1
                else:
                    test_fail("Could not find overlaid row after inserting decision")
                    failed += 1

                # Clean up test decision
                con.execute(f"""
                    DELETE FROM adjudication_decisions
                    WHERE research_id = {test_rid}
                      AND reviewer_name = 'validation_test'
                      AND final_value_selected = 'TEST_VALUE'
                """)
                test_pass("Test decision cleaned up")
                passed += 1
            else:
                test_fail("No sample row available in histology_analysis_cohort_v")
                failed += 1

        except Exception as e:
            test_fail(f"Post-review overlay test error: {e}")
            failed += 1
    else:
        test_fail("Prerequisites missing for overlay test (adjudication_decisions or histology_post_review_v)")
        failed += 1

    # ── Test 5: Manuscript views ─────────────────────────────────────────
    section("TEST 5: Manuscript Views")
    manuscript_views = [
        "manuscript_histology_cohort_v",
        "manuscript_molecular_cohort_v",
        "manuscript_rai_cohort_v",
        "manuscript_patient_summary_v",
    ]
    for v in manuscript_views:
        if not table_available(con, v):
            test_fail(f"{v} missing — run script 20")
            failed += 1
            continue
        cnt = con.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0]
        if cnt > 0:
            test_pass(f"{v} has {cnt:,} rows")
            passed += 1
        else:
            test_fail(f"{v} is empty")
            failed += 1

    # Verify analysis_inclusion_flag is all TRUE in manuscript views
    for v in ["manuscript_histology_cohort_v", "manuscript_molecular_cohort_v", "manuscript_rai_cohort_v"]:
        if not table_available(con, v):
            continue
        try:
            non_included = con.execute(f"""
                SELECT COUNT(*) FROM {v}
                WHERE analysis_inclusion_flag = FALSE OR analysis_inclusion_flag IS NULL
            """).fetchone()[0]
            if non_included == 0:
                test_pass(f"{v} contains only analysis_inclusion_flag = TRUE")
                passed += 1
            else:
                test_fail(f"{v} has {non_included} rows where analysis_inclusion_flag != TRUE")
                failed += 1
        except Exception as e:
            test_fail(f"{v} inclusion check error: {e}")
            failed += 1

    # ── Test 6: Export file verification ──────────────────────────────────
    section("TEST 6: Export Bundle")
    export_dirs = sorted(ROOT.glob("exports/manuscript_cohort_*"))
    if export_dirs:
        latest = export_dirs[-1]
        manifest_path = latest / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
            if "cohorts" in manifest and len(manifest["cohorts"]) > 0:
                test_pass(f"Manifest exists with {len(manifest['cohorts'])} cohorts")
                passed += 1
            else:
                test_fail("Manifest has no cohorts")
                failed += 1

            for prefix in manifest.get("cohorts", {}):
                csv_exists = (latest / f"{prefix}.csv").exists()
                pq_exists = (latest / f"{prefix}.parquet").exists()
                if csv_exists and pq_exists:
                    test_pass(f"{prefix}: CSV + Parquet exist")
                    passed += 1
                else:
                    test_fail(f"{prefix}: missing CSV={not csv_exists} Parquet={not pq_exists}")
                    failed += 1
        else:
            test_fail("No manifest.json in latest export bundle")
            failed += 1
    else:
        test_fail("No export bundle found — run script 20 with --export")
        failed += 1

    # ── Summary ──────────────────────────────────────────────────────────
    section("SUMMARY")
    total = passed + failed
    print(f"  {passed}/{total} tests passed, {failed} failed")
    if failed == 0:
        print("  All validation tests PASSED")
    else:
        print(f"  {failed} test(s) FAILED — review above")

    con.close()
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
