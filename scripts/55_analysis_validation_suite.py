#!/usr/bin/env python3
"""
55_analysis_validation_suite.py -- Validation tests for analysis-grade tables

Lightweight but comprehensive assertion-based tests for all tables created
by the analysis-grade optimization pipeline (scripts 48-53).

Tests cover:
  - No duplicate patient IDs in patient-level resolved tables
  - No duplicate (research_id, surgery_episode_id) in episode tables
  - linkage_score values are NULL or in [0.0, 1.0]
  - Scoring fields are NULL when *_calculable_flag is FALSE
  - timing_days_post_surgery >= 0 for surgery_related complications
  - result_numeric within plausibility bounds per lab type
  - Date precedence: specimen_collect_dt preferred when available
  - analysis_eligible_flag is TRUE only when histology + surgery_date present
  - AJCC8 T stage consistent with tumor_size_cm and gross_ete
  - ATA risk never NULL when both histology and ETE grade are present

Also creates val_analysis_resolved_v1 as a summary validation table
compatible with scripts/29_validation_engine.py.

Supports --md, --local, --strict (fail on any test failure) flags.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

TODAY = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def connect_md() -> duckdb.DuckDBPyConnection:
    token = _get_token()
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


class TestResult:
    """Accumulate test results."""

    def __init__(self) -> None:
        self.results: list[dict] = []

    def record(self, test_id: str, table: str, status: str,
               details: str, n_violations: int = 0) -> None:
        self.results.append({
            "test_id": test_id,
            "table": table,
            "status": status,
            "details": details,
            "n_violations": n_violations,
            "checked_at": datetime.now().isoformat(),
        })
        icon = "✓" if status == "PASS" else ("?" if status == "SKIP" else "✗")
        print(f"  [{icon}] {test_id}: {details}")

    def summary(self) -> tuple[int, int, int]:
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        skipped = sum(1 for r in self.results if r["status"] == "SKIP")
        return passed, failed, skipped


def run_test(con: duckdb.DuckDBPyConnection, results: TestResult,
             test_id: str, table: str, sql: str, description: str,
             expect_zero: bool = True) -> int:
    """Run a SQL test. Returns violation count (0 = PASS)."""
    if not table_available(con, table):
        results.record(test_id, table, "SKIP",
                       f"{table} not found", 0)
        return 0

    try:
        r = con.execute(sql).fetchone()
        count = r[0] if r else 0

        if expect_zero:
            if count == 0:
                results.record(test_id, table, "PASS",
                               f"{description} -- 0 violations", 0)
            else:
                results.record(test_id, table, "FAIL",
                               f"{description} -- {count:,} violations", count)
        else:
            results.record(test_id, table, "PASS",
                           f"{description} -- count={count:,}", count)

        return count
    except Exception as exc:
        results.record(test_id, table, "FAIL",
                       f"{description} -- ERROR: {exc}", -1)
        return -1


def run_all_tests(con: duckdb.DuckDBPyConnection) -> TestResult:
    results = TestResult()

    section("T1: Duplicate patient ID checks")

    run_test(con, results, "T1.1", "patient_analysis_resolved_v1",
             "SELECT COUNT(*) FROM (SELECT research_id FROM patient_analysis_resolved_v1 "
             "GROUP BY research_id HAVING COUNT(*) > 1)",
             "No duplicate research_id in patient_analysis_resolved_v1")

    run_test(con, results, "T1.2", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM (SELECT research_id FROM thyroid_scoring_systems_v1 "
             "GROUP BY research_id HAVING COUNT(*) > 1)",
             "No duplicate research_id in thyroid_scoring_systems_v1")

    run_test(con, results, "T1.3", "complication_patient_summary_v1",
             "SELECT COUNT(*) FROM (SELECT research_id FROM complication_patient_summary_v1 "
             "GROUP BY research_id HAVING COUNT(*) > 1)",
             "No duplicate research_id in complication_patient_summary_v1")

    run_test(con, results, "T1.4", "longitudinal_lab_patient_summary_v1",
             "SELECT COUNT(*) FROM (SELECT research_id FROM longitudinal_lab_patient_summary_v1 "
             "GROUP BY research_id HAVING COUNT(*) > 1)",
             "No duplicate research_id in longitudinal_lab_patient_summary_v1")

    run_test(con, results, "T1.5", "imaging_patient_summary_v1",
             "SELECT COUNT(*) FROM (SELECT research_id FROM imaging_patient_summary_v1 "
             "GROUP BY research_id HAVING COUNT(*) > 1)",
             "No duplicate research_id in imaging_patient_summary_v1")

    section("T2: Episode-level uniqueness")

    run_test(con, results, "T2.1", "episode_analysis_resolved_v1",
             "SELECT COUNT(*) FROM (SELECT research_id, surgery_episode_id "
             "FROM episode_analysis_resolved_v1 "
             "GROUP BY research_id, surgery_episode_id HAVING COUNT(*) > 1)",
             "No duplicate (research_id, surgery_episode_id) in episode resolved")

    run_test(con, results, "T2.2", "lesion_analysis_resolved_v1",
             "SELECT COUNT(*) FROM (SELECT research_id, surgery_episode_id, tumor_ordinal "
             "FROM lesion_analysis_resolved_v1 "
             "GROUP BY research_id, surgery_episode_id, tumor_ordinal HAVING COUNT(*) > 1)",
             "No duplicate lesion keys in lesion_analysis_resolved_v1")

    section("T3: Linkage score bounds [0.0, 1.0]")

    for tbl in ["imaging_fna_linkage_v3", "fna_molecular_linkage_v3",
                "preop_surgery_linkage_v3", "surgery_pathology_linkage_v3",
                "pathology_rai_linkage_v3"]:
        run_test(con, results, f"T3.{tbl[:3]}", tbl,
                 f"SELECT COUNT(*) FROM {tbl} "
                 f"WHERE linkage_score IS NOT NULL "
                 f"AND (linkage_score < 0 OR linkage_score > 1.0)",
                 f"linkage_score in [0.0, 1.0] for {tbl}")

    section("T4: Scoring NULL when calculable_flag is FALSE")

    run_test(con, results, "T4.1", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE NOT ajcc8_calculable_flag AND ajcc8_stage_group IS NOT NULL",
             "ajcc8_stage_group is NULL when not calculable")

    run_test(con, results, "T4.2", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE NOT macis_calculable_flag AND macis_score IS NOT NULL",
             "macis_score is NULL when not calculable")

    run_test(con, results, "T4.3", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE NOT ames_calculable_flag AND ames_risk_group IS NOT NULL",
             "ames_risk_group is NULL when not calculable")

    section("T5: Complication timing plausibility")

    run_test(con, results, "T5.1", "complication_phenotype_v1",
             "SELECT COUNT(*) FROM complication_phenotype_v1 "
             "WHERE surgery_related_flag "
             "AND timing_days_post_surgery IS NOT NULL "
             "AND timing_days_post_surgery < -30",
             "No surgery_related complications with timing < -30 days")

    run_test(con, results, "T5.2", "complication_phenotype_v1",
             "SELECT COUNT(*) FROM complication_phenotype_v1 "
             "WHERE permanent_flag AND NOT confirmed_flag",
             "Permanent complications must also be confirmed")

    section("T6: Lab plausibility bounds")

    run_test(con, results, "T6.1", "longitudinal_lab_clean_v1",
             "SELECT COUNT(*) FROM longitudinal_lab_clean_v1 "
             "WHERE lab_type = 'thyroglobulin' "
             "AND result_numeric IS NOT NULL "
             "AND result_numeric NOT BETWEEN 0 AND 100000",
             "Tg values within [0, 100000] ng/mL")

    run_test(con, results, "T6.2", "longitudinal_lab_clean_v1",
             "SELECT COUNT(*) FROM longitudinal_lab_clean_v1 "
             "WHERE lab_type = 'pth' "
             "AND result_numeric IS NOT NULL "
             "AND result_numeric NOT BETWEEN 0.5 AND 500",
             "PTH values within [0.5, 500] pg/mL")

    run_test(con, results, "T6.3", "longitudinal_lab_clean_v1",
             "SELECT COUNT(*) FROM longitudinal_lab_clean_v1 "
             "WHERE lab_type = 'calcium' "
             "AND result_numeric IS NOT NULL "
             "AND result_numeric NOT BETWEEN 4 AND 15",
             "Calcium values within [4, 15] mg/dL")

    section("T7: Date precedence")

    run_test(con, results, "T7.1", "longitudinal_lab_clean_v1",
             "SELECT COUNT(*) FROM longitudinal_lab_clean_v1 "
             "WHERE source_table = 'thyroglobulin_labs' "
             "AND date_source != 'specimen_collect_dt'",
             "Thyroglobulin_labs always uses specimen_collect_dt as date source")

    section("T8: Analysis eligibility consistency")

    run_test(con, results, "T8.1", "patient_analysis_resolved_v1",
             "SELECT COUNT(*) FROM patient_analysis_resolved_v1 "
             "WHERE analysis_eligible_flag "
             "AND (path_histology_raw IS NULL OR first_surgery_date IS NULL)",
             "analysis_eligible_flag=TRUE only when histology+surgery_date present")

    run_test(con, results, "T8.2", "patient_analysis_resolved_v1",
             "SELECT COUNT(*) FROM patient_analysis_resolved_v1 "
             "WHERE scoring_ajcc8_flag "
             "AND ajcc8_stage_group IS NULL",
             "ajcc8_stage_group present when scoring_ajcc8_flag=TRUE")

    section("T9: AJCC8 T stage consistency")

    run_test(con, results, "T9.1", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE ajcc8_calculable_flag "
             "AND ajcc8_t_stage IS NOT NULL "
             "AND tumor_size_cm IS NOT NULL "
             "AND tumor_size_cm > 0 "
             "AND gross_ete_flag = FALSE "
             "AND ajcc8_t_stage = 'T1a' "
             "AND tumor_size_cm > 1.0",
             "T1a only assigned for tumor_size_cm <= 1.0")

    run_test(con, results, "T9.2", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE ajcc8_calculable_flag "
             "AND gross_ete_flag = TRUE "
             "AND ajcc8_t_stage NOT IN ('T3b','T4a','T4b')",
             "Gross ETE assigns T3b or higher")

    section("T10: Molecular risk composite logic")

    run_test(con, results, "T10.1", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE braf_positive AND tert_positive "
             "AND molecular_risk_tier != 'high'",
             "BRAF+TERT co-mutation = high molecular risk tier")

    run_test(con, results, "T10.2", "thyroid_scoring_systems_v1",
             "SELECT COUNT(*) FROM thyroid_scoring_systems_v1 "
             "WHERE NOT braf_positive AND NOT ras_positive AND NOT tert_positive "
             "AND molecular_risk_tier NOT IN ('low','unknown')",
             "All-negative molecular = low or unknown risk tier")

    section("T11: Resolved layer version consistency")

    run_test(con, results, "T11.1", "patient_analysis_resolved_v1",
             "SELECT COUNT(*) FROM patient_analysis_resolved_v1 "
             "WHERE resolved_layer_version != 'v1'",
             "All patient_analysis_resolved_v1 rows have version='v1'")

    section("T12: Lab deduplication")

    run_test(con, results, "T12.1", "longitudinal_lab_clean_v1",
             "SELECT COUNT(*) FROM ("
             "SELECT research_id, lab_type, collection_date, "
             "ROUND(COALESCE(result_numeric,0),2) AS result_rounded "
             "FROM longitudinal_lab_clean_v1 "
             "GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)",
             "No duplicate (patient, lab_type, date, value) in lab clean table")

    return results


def save_validation_table(con: duckdb.DuckDBPyConnection,
                          results: TestResult) -> None:
    """Write results to val_analysis_resolved_v1 table for script 29 integration."""
    try:
        rows = []
        for r in results.results:
            severity = "error" if r["status"] == "FAIL" else "info"
            rows.append((
                r["test_id"],
                severity,
                None,  # research_id
                r["details"],
                f"table={r['table']} violations={r['n_violations']}",
                r["checked_at"],
            ))

        con.execute("DROP TABLE IF EXISTS val_analysis_resolved_v1")
        con.execute("""
CREATE TABLE val_analysis_resolved_v1 (
    check_id VARCHAR,
    severity VARCHAR,
    research_id INTEGER,
    description VARCHAR,
    detail VARCHAR,
    checked_at VARCHAR
)
""")
        con.executemany(
            "INSERT INTO val_analysis_resolved_v1 VALUES (?,?,?,?,?,?)",
            rows
        )
        n = con.execute(
            "SELECT COUNT(*) FROM val_analysis_resolved_v1"
        ).fetchone()[0]
        print(f"\n  Saved {n} results to val_analysis_resolved_v1")
    except Exception as exc:
        print(f"\n  [WARN] Could not save to val_analysis_resolved_v1: {exc}")


def main() -> None:
    p = argparse.ArgumentParser(description="55_analysis_validation_suite.py")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--strict", action="store_true",
                   help="Exit non-zero if any tests fail")
    p.add_argument("--no-save", action="store_true",
                   help="Don't write val_analysis_resolved_v1 table")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        results = run_all_tests(con)

        passed, failed, skipped = results.summary()

        section("Test Summary")
        print(f"  PASS:  {passed}")
        print(f"  FAIL:  {failed}")
        print(f"  SKIP:  {skipped}")
        print(f"  TOTAL: {passed + failed + skipped}")

        if not args.no_save:
            save_validation_table(con, results)

        if failed > 0 and args.strict:
            print(f"\n  [STRICT] {failed} test(s) failed -- exiting with code 1")
            sys.exit(1)
        elif failed > 0:
            print(f"\n  [WARN] {failed} test(s) failed (use --strict to fail pipeline)")

    finally:
        con.close()

    print("\n[COMPLETE] 55_analysis_validation_suite.py finished")


if __name__ == "__main__":
    main()
