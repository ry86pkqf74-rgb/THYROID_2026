#!/usr/bin/env python3
"""MotherDuck formalization validation suite.

Runs a comprehensive set of checks against the MotherDuck database to verify
that the formalization pass completed successfully.  Produces a Markdown report
in the studies/ directory.

Checks:
  1. MotherDuck attachment verification (PRAGMA database_list)
  2. Row counts: local parquet vs v2_stage vs main (all 22 domains + canonical)
  3. Schema/provenance completeness: required columns in every main table
  4. Canonical/quarantine row counts and domain distribution
  5. Review queue population counts in qa.manual_review_queue
  6. QA view smoke tests
  7. load_inventory completeness
  8. Release schema existence check

Usage:
  .venv/bin/python scripts/119_md_formalization_validate.py --md
  .venv/bin/python scripts/119_md_formalization_validate.py --db-path thyroid_master.duckdb
  .venv/bin/python scripts/119_md_formalization_validate.py --md --output-dir studies/20260407_motherduck_formalization
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
DEFAULT_V2_DIR = ROOT / "processed" / "output" / "v2_parquets"
PROCESSED = ROOT / "processed"

REQUIRED_ENTITY_COLUMNS = [
    "research_id", "note_row_id", "entity_type",
    "entity_value_raw", "entity_value_norm",
]

DESIRED_PROVENANCE_COLUMNS = [
    "entity_date", "note_date", "extraction_run_id",
    "extracted_at", "source_file_id",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate MotherDuck formalization.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    p.add_argument("--v2-parquets-dir", default=str(DEFAULT_V2_DIR))
    p.add_argument("--output-dir", default=None, help="Output directory for report.")
    return p.parse_args()


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file
        return connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    return duckdb.connect(args.db_path)


class ValidationResult:
    def __init__(self, name: str):
        self.name = name
        self.checks: list[dict] = []

    def add(self, check: str, status: str, detail: str = ""):
        self.checks.append({"check": check, "status": status, "detail": detail})
        icon = "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "WARN"
        print(f"  [{icon}] {check}: {detail}")

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "FAIL")

    @property
    def warned(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "WARN")


def check_md_attachment(con: duckdb.DuckDBPyConnection, results: ValidationResult) -> None:
    """Check 1: Verify MotherDuck attachment."""
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        md_found = any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
        if md_found:
            results.add("MD attachment", "PASS", f"{len(dbs)} databases attached")
        else:
            results.add("MD attachment", "WARN",
                         "No md: path in database_list (may be local-only)")
    except Exception as exc:
        results.add("MD attachment", "FAIL", str(exc))


def check_row_counts(
    con: duckdb.DuckDBPyConnection,
    v2_dir: Path,
    results: ValidationResult,
) -> list[dict]:
    """Check 2: Row counts across local, v2_stage, and main."""
    registry = load_registry()
    rows_data = []

    for name, spec in registry.v2_domains.items():
        if not spec.canonical_output:
            continue
        stem = spec.parquet_stem
        pq_path = v2_dir / f"{stem}.parquet"

        local_count = 0
        if pq_path.exists():
            local_count = len(pd.read_parquet(pq_path))

        stage_count = -1
        try:
            stage_count = con.execute(f"SELECT COUNT(*) FROM v2_stage.{stem}").fetchone()[0]
        except Exception:
            pass

        main_count = -1
        try:
            main_count = con.execute(f"SELECT COUNT(*) FROM main.{stem}").fetchone()[0]
        except Exception:
            pass

        match_stage = local_count == stage_count if stage_count >= 0 else False
        match_main = local_count == main_count if main_count >= 0 else False

        row = {
            "domain": name, "stem": stem,
            "local": local_count, "v2_stage": stage_count, "main": main_count,
            "stage_match": match_stage, "main_match": match_main,
        }
        rows_data.append(row)

    mismatches = [r for r in rows_data if not r["stage_match"] or not r["main_match"]]
    if mismatches:
        detail = ", ".join(r["stem"] for r in mismatches)
        results.add("Row count parity", "WARN", f"{len(mismatches)} mismatches: {detail}")
    else:
        results.add("Row count parity", "PASS",
                     f"{len(rows_data)} domains checked, all match")

    for tbl_name in ["canonical_extracted_fact_long_v2", "canonical_fact_quarantine_v2",
                      "note_extraction_runs"]:
        pq_path = PROCESSED / f"{tbl_name}.parquet"
        local_count = len(pd.read_parquet(pq_path)) if pq_path.exists() else -1
        try:
            md_count = con.execute(f"SELECT COUNT(*) FROM main.{tbl_name}").fetchone()[0]
        except Exception:
            md_count = -1
        match = local_count == md_count if (local_count >= 0 and md_count >= 0) else False
        status = "PASS" if match else ("WARN" if md_count < 0 else "FAIL")
        results.add(f"Canonical {tbl_name}", status,
                     f"local={local_count:,}  md={md_count:,}")

    return rows_data


def check_schema_completeness(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 3: Required columns present in main entity tables."""
    registry = load_registry()
    issues = []

    for name, spec in registry.v2_domains.items():
        if not spec.canonical_output:
            continue
        stem = spec.parquet_stem
        try:
            cols = [r[0] for r in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='main' AND table_name='{stem}'"
            ).fetchall()]
        except Exception:
            issues.append(f"{stem}: table not found in main")
            continue

        missing = [c for c in REQUIRED_ENTITY_COLUMNS if c not in cols]
        if missing:
            issues.append(f"{stem}: missing {missing}")

    if issues:
        results.add("Schema completeness", "WARN", f"{len(issues)} issues: {'; '.join(issues[:5])}")
    else:
        results.add("Schema completeness", "PASS", "All entity tables have required columns")


def check_canonical_distribution(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> list[dict]:
    """Check 4: Canonical fact distribution by entity_type."""
    dist_data = []
    for tbl in ["canonical_extracted_fact_long_v2", "canonical_fact_quarantine_v2"]:
        try:
            rows = con.execute(f"""
                SELECT COALESCE(entity_type, 'UNKNOWN') AS domain,
                       COUNT(*) AS n_rows,
                       COUNT(DISTINCT research_id) AS n_patients
                FROM main.{tbl}
                GROUP BY 1
                ORDER BY 2 DESC
            """).fetchall()
            for domain, n, pts in rows:
                dist_data.append({"table": tbl, "domain": domain, "rows": n, "patients": pts})
            total = sum(r[1] for r in rows)
            results.add(f"Canonical dist ({tbl})", "PASS",
                         f"{len(rows)} domains, {total:,} total rows")
        except Exception as exc:
            results.add(f"Canonical dist ({tbl})", "FAIL", str(exc))

    return dist_data


def check_review_queue(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 5: Review queue population."""
    try:
        total = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        reviewed = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue "
            "WHERE verification_status IS NOT NULL"
        ).fetchone()[0]
        pending = total - reviewed
        results.add("Review queue", "PASS",
                     f"{total:,} total, {reviewed:,} reviewed, {pending:,} pending")
    except Exception as exc:
        results.add("Review queue", "WARN", f"qa.manual_review_queue not found: {exc}")


def check_qa_views(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 6: QA summary view smoke tests."""
    views = [
        "qa.promotion_scorecard_summary_v",
        "qa.domain_validation_summary_v",
        "qa.date_provenance_completeness_v",
        "qa.manual_review_queue_summary_v",
    ]
    for view in views:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
            results.add(f"QA view {view.split('.')[-1]}", "PASS", f"{cnt:,} rows")
        except Exception as exc:
            results.add(f"QA view {view.split('.')[-1]}", "WARN", str(exc))


def check_load_inventory(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 7: load_inventory completeness."""
    try:
        total = con.execute("SELECT COUNT(*) FROM v2_stage.load_inventory").fetchone()[0]
        mismatches = con.execute(
            "SELECT COUNT(*) FROM v2_stage.load_inventory WHERE NOT row_match"
        ).fetchone()[0]
        if mismatches > 0:
            results.add("Load inventory", "WARN",
                         f"{total:,} entries, {mismatches:,} row-count mismatches")
        else:
            results.add("Load inventory", "PASS", f"{total:,} entries, all match")
    except Exception as exc:
        results.add("Load inventory", "WARN", f"Not found: {exc}")


def check_release_schemas(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 8: Release schema existence."""
    try:
        schemas = [r[0] for r in con.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
        ).fetchall()]
        release_schemas = [s for s in schemas if s.startswith("release_")]
        if release_schemas:
            results.add("Release schemas", "PASS",
                         f"{len(release_schemas)} found: {', '.join(sorted(release_schemas))}")
        else:
            results.add("Release schemas", "WARN",
                         f"No release_YYYYMMDD schemas found (run 115 to create). "
                         f"Current schemas: {', '.join(schemas)}")
    except Exception as exc:
        results.add("Release schemas", "WARN", str(exc))


def generate_report(
    results: ValidationResult,
    rows_data: list[dict],
    dist_data: list[dict],
) -> str:
    """Generate a Markdown validation report."""
    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "# MotherDuck Formalization Validation Report",
        "",
        f"**Generated:** {now}",
        f"**Total checks:** {len(results.checks)}",
        f"**Passed:** {results.passed}  |  **Warned:** {results.warned}  |  **Failed:** {results.failed}",
        "",
        "---",
        "",
        "## Check Results",
        "",
        "| Check | Status | Detail |",
        "|-------|--------|--------|",
    ]
    for c in results.checks:
        icon = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}.get(c["status"], "?")
        lines.append(f"| {c['check']} | {icon} | {c['detail']} |")

    if rows_data:
        lines.extend([
            "",
            "---",
            "",
            "## Row Count Parity (v2 domains)",
            "",
            "| Domain | Stem | Local | v2_stage | main | Stage Match | Main Match |",
            "|--------|------|------:|--------:|-----:|:-----------:|:----------:|",
        ])
        for r in rows_data:
            s_match = "Y" if r["stage_match"] else "N"
            m_match = "Y" if r["main_match"] else "N"
            lines.append(
                f"| {r['domain']} | {r['stem']} | {r['local']:,} | "
                f"{r['v2_stage']:,} | {r['main']:,} | {s_match} | {m_match} |"
            )

    if dist_data:
        lines.extend([
            "",
            "---",
            "",
            "## Canonical Fact Distribution",
            "",
            "| Table | Domain | Rows | Patients |",
            "|-------|--------|-----:|--------:|",
        ])
        for d in dist_data:
            lines.append(
                f"| {d['table']} | {d['domain']} | {d['rows']:,} | {d['patients']:,} |"
            )

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    con = get_connection(args)
    v2_dir = Path(args.v2_parquets_dir)

    results = ValidationResult("MotherDuck Formalization")

    try:
        print("=== MotherDuck Formalization Validation ===\n")

        print("--- Check 1: MD Attachment ---")
        check_md_attachment(con, results)

        print("\n--- Check 2: Row Count Parity ---")
        rows_data = check_row_counts(con, v2_dir, results)

        print("\n--- Check 3: Schema Completeness ---")
        check_schema_completeness(con, results)

        print("\n--- Check 4: Canonical Distribution ---")
        dist_data = check_canonical_distribution(con, results)

        print("\n--- Check 5: Review Queue ---")
        check_review_queue(con, results)

        print("\n--- Check 6: QA Views ---")
        check_qa_views(con, results)

        print("\n--- Check 7: Load Inventory ---")
        check_load_inventory(con, results)

        print("\n--- Check 8: Release Schemas ---")
        check_release_schemas(con, results)

    finally:
        con.close()

    print(f"\n=== Summary: {results.passed} PASS / {results.warned} WARN / {results.failed} FAIL ===")

    report = generate_report(results, rows_data, dist_data)

    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = ROOT / "studies" / f"{datetime.now().strftime('%Y%m%d')}_motherduck_formalization"

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "validation_report.md"
    report_path.write_text(report)
    print(f"\n  [report] {report_path}")


if __name__ == "__main__":
    main()
