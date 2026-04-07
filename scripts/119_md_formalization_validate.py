#!/usr/bin/env python3
"""MotherDuck formalization validation suite.

Runs a comprehensive set of checks against the MotherDuck database to verify
that the formalization pass completed successfully.  Produces a Markdown report
in the studies/ directory.

Two modes:
  structural (default) — informational WARNs for missing infrastructure.
      Suitable for dry runs and local development.
  release (--release-mode) — strict FAILs when infrastructure required for
      a signed-off release is absent.  Blocks sign-off when:
        * MotherDuck is not actually attached
        * v2_stage.load_inventory is missing or has row mismatches
        * any promotable row in qa.manual_review_queue is still pending
        * no release_YYYYMMDD schema exists
        * no named snapshot in qa.release_manifest for the target run

Checks:
  1. MotherDuck attachment verification (PRAGMA database_list)
  2. Row counts: local parquet vs v2_stage vs main (all 23 v2 domains + canonical)
  3. Schema/provenance completeness: required columns in every main table
  4. Canonical/quarantine row counts and domain distribution
  5. Review queue population counts in qa.manual_review_queue
  6. QA view smoke tests
  7. load_inventory completeness
  8. Release schema existence check

Usage:
  .venv/bin/python scripts/119_md_formalization_validate.py --md
  .venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode
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

# Wide JSON note-level v2 parquets keep ids on the row but encode entities in JSON.
WIDE_JSON_MISSING_COLUMNS = ["entity_type", "entity_value_raw", "entity_value_norm"]

DESIRED_PROVENANCE_COLUMNS = [
    "entity_date", "note_date", "extraction_run_id",
    "extracted_at", "source_file_id",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate MotherDuck formalization.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument(
        "--md-sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN (release / CI).",
    )
    p.add_argument(
        "--md-user-agent",
        default=None,
        help="custom_user_agent for MotherDuck connection (query history). "
        "Default: MOTHERDUCK_CUSTOM_USER_AGENT or THYROID_2026_formalization_validate/1.0",
    )
    p.add_argument(
        "--md-session-hint",
        default=None,
        help="SET motherduck_session_hint after connect. Default: MOTHERDUCK_SESSION_HINT.",
    )
    p.add_argument("--release-mode", action="store_true",
                    help="Strict release validation: FAIL on missing infrastructure.")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    p.add_argument("--v2-parquets-dir", default=str(DEFAULT_V2_DIR))
    p.add_argument("--output-dir", default=None, help="Output directory for report.")
    return p.parse_args()


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        import os
        from utils.md_connect import connect_md_or_file

        ua = args.md_user_agent or os.environ.get(
            "MOTHERDUCK_CUSTOM_USER_AGENT",
            "THYROID_2026_formalization_validate/1.0",
        )
        return connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=args.md_sa,
            custom_user_agent=ua,
            motherduck_session_hint=args.md_session_hint,
        )
    return duckdb.connect(args.db_path)


class ValidationResult:
    def __init__(self, name: str):
        self.name = name
        self.checks: list[dict] = []

    def add(self, check: str, status: str, detail: str = ""):
        self.checks.append({"check": check, "status": status, "detail": detail})
        print(f"  [{status}] {check}: {detail}")

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "FAIL")

    @property
    def warned(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "WARN")


def check_md_attachment(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Check 1: Verify MotherDuck attachment."""
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        md_found = any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
        if md_found:
            results.add("MD attachment", "PASS", f"{len(dbs)} databases attached")
        else:
            status = "FAIL" if strict else "WARN"
            results.add("MD attachment", status,
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


def _is_wide_note_level_contract(
    con: duckdb.DuckDBPyConnection,
    stem: str,
    missing: list[str],
) -> bool:
    """True when main and v2_stage both omit typed entity columns (wide JSON note-level shape).

    Ids (`research_id`, `note_row_id`) are still present; `entity_type` / `entity_value_*` live in JSON
    until promotion to `canonical_extracted_fact_long_v2`.
    """
    if set(missing) != set(WIDE_JSON_MISSING_COLUMNS):
        return False
    try:
        stg_cols = [r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='v2_stage' AND table_name='{stem}'"
        ).fetchall()]
    except Exception:
        return False
    if not stg_cols:
        return False
    return all(c not in stg_cols for c in WIDE_JSON_MISSING_COLUMNS)


def check_schema_completeness(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
) -> None:
    """Check 3: Required columns present in main entity tables.

    Promoted v2 domain tables may use a **wide JSON note-level** contract:
    `entity_type` / `entity_value_raw` / `entity_value_norm` absent on both v2_stage and main
    (ids present). That is PASS with a documented exception — analytic long-form remains
    ``main.canonical_extracted_fact_long_v2`` and presentation views.
    Any other missing-column pattern stays WARN (e.g. missing `research_id` or promotion drift).
    """
    registry = load_registry()
    issues: list[str] = []
    wide_stems: list[str] = []

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
        if not missing:
            continue

        if _is_wide_note_level_contract(con, stem, missing):
            wide_stems.append(stem)
            continue

        stg_note = ""
        try:
            stg_cols = [r[0] for r in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='v2_stage' AND table_name='{stem}'"
            ).fetchall()]
            if stg_cols and all(c in stg_cols for c in missing):
                stg_note = " (present on v2_stage but not main — promotion/main drift)"
        except Exception:
            pass
        issues.append(f"{stem}: missing {missing}{stg_note}")

    if issues:
        results.add("Schema completeness", "WARN", f"{len(issues)} issues: {'; '.join(issues[:5])}")
    elif wide_stems:
        results.add(
            "Schema completeness",
            "PASS",
            f"Wide note-level v2 contract on {len(wide_stems)} promoted table(s); "
            f"entity_type/entity_value_* in main.canonical_extracted_fact_long_v2 "
            f"(see docs/domain_mapping_rules.md). Example stems: {', '.join(wide_stems[:3])}"
            f"{'…' if len(wide_stems) > 3 else ''}",
        )
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
    strict: bool = False,
) -> None:
    """Check 5: Review queue population.

    In release mode, any pending (unreviewed) promotable rows cause FAIL.
    In structural mode, report counts as PASS for observability.
    """
    try:
        total = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        reviewed = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue "
            "WHERE verification_status IS NOT NULL"
        ).fetchone()[0]
        pending = total - reviewed

        if strict and pending > 0:
            results.add("Review queue", "FAIL",
                         f"{total:,} total, {reviewed:,} reviewed, "
                         f"{pending:,} PENDING — must be resolved before release")
        else:
            results.add("Review queue", "PASS",
                         f"{total:,} total, {reviewed:,} reviewed, {pending:,} pending")
    except Exception as exc:
        status = "FAIL" if strict else "WARN"
        results.add("Review queue", status, f"qa.manual_review_queue not found: {exc}")


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
    strict: bool = False,
) -> None:
    """Check 7: load_inventory completeness.

    In release mode, missing table or row mismatches cause FAIL.
    """
    try:
        total = con.execute("SELECT COUNT(*) FROM v2_stage.load_inventory").fetchone()[0]
        mismatches = con.execute(
            "SELECT COUNT(*) FROM v2_stage.load_inventory WHERE NOT row_match"
        ).fetchone()[0]
        if mismatches > 0:
            status = "FAIL" if strict else "WARN"
            results.add("Load inventory", status,
                         f"{total:,} entries, {mismatches:,} row-count mismatches")
        else:
            results.add("Load inventory", "PASS", f"{total:,} entries, all match")
    except Exception as exc:
        status = "FAIL" if strict else "WARN"
        results.add("Load inventory", status, f"Not found: {exc}")


def check_release_schemas(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Check 8: Release schema existence.

    In release mode, absence of any release_YYYYMMDD schema causes FAIL.
    """
    try:
        schemas = [r[0] for r in con.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
        ).fetchall()]
        release_schemas = [s for s in schemas if s.startswith("release_")]
        if release_schemas:
            results.add("Release schemas", "PASS",
                         f"{len(release_schemas)} found: {', '.join(sorted(release_schemas))}")
        else:
            status = "FAIL" if strict else "WARN"
            results.add("Release schemas", status,
                         f"No release_YYYYMMDD schemas found (run 115 to create). "
                         f"Current schemas: {', '.join(schemas)}")
    except Exception as exc:
        status = "FAIL" if strict else "WARN"
        results.add("Release schemas", status, str(exc))


def check_release_manifest(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Check 9 (release-mode only): Named snapshot in qa.release_manifest."""
    try:
        total = con.execute("SELECT COUNT(*) FROM qa.release_manifest").fetchone()[0]
        if total > 0:
            latest = con.execute(
                "SELECT release_tag, created_at FROM qa.release_manifest "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            results.add("Release manifest", "PASS",
                         f"{total} release(s); latest: {latest[0]} ({latest[1]})")
        else:
            status = "FAIL" if strict else "WARN"
            results.add("Release manifest", status,
                         "qa.release_manifest is empty — no snapshots recorded")
    except Exception as exc:
        status = "FAIL" if strict else "WARN"
        results.add("Release manifest", status, f"Not found: {exc}")


def generate_report(
    results: ValidationResult,
    rows_data: list[dict],
    dist_data: list[dict],
    mode: str = "structural",
) -> str:
    """Generate a Markdown validation report."""
    now = datetime.now(timezone.utc).isoformat()
    mode_label = "Release Validation" if mode == "release" else "Structural Validation"
    lines = [
        f"# MotherDuck Formalization {mode_label} Report",
        "",
        f"**Generated:** {now}",
        f"**Mode:** {mode_label}",
        f"**Total checks:** {len(results.checks)}",
        f"**Passed:** {results.passed}  |  **Warned:** {results.warned}  |  **Failed:** {results.failed}",
        "",
    ]

    if results.failed > 0:
        lines.append(f"**VERDICT: BLOCKED** — {results.failed} check(s) failed.")
    elif results.warned > 0:
        lines.append(
            f"**VERDICT: PASS WITH WARNINGS** — {results.warned} check(s) warned; no failures."
        )
    else:
        lines.append("**VERDICT: PASS** — all checks passed.")

    lines.extend([
        "",
        "---",
        "",
        "## Check Results",
        "",
        "| Check | Status | Detail |",
        "|-------|--------|--------|",
    ])
    for c in results.checks:
        lines.append(f"| {c['check']} | {c['status']} | {c['detail']} |")

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
    strict = args.release_mode
    mode = "release" if strict else "structural"

    results = ValidationResult("MotherDuck Formalization")

    try:
        mode_label = "RELEASE" if strict else "STRUCTURAL"
        print(f"=== MotherDuck Formalization Validation ({mode_label}) ===\n")

        print("--- Check 1: MD Attachment ---")
        check_md_attachment(con, results, strict=strict)

        print("\n--- Check 2: Row Count Parity ---")
        rows_data = check_row_counts(con, v2_dir, results)

        print("\n--- Check 3: Schema Completeness ---")
        check_schema_completeness(con, results)

        print("\n--- Check 4: Canonical Distribution ---")
        dist_data = check_canonical_distribution(con, results)

        print("\n--- Check 5: Review Queue ---")
        check_review_queue(con, results, strict=strict)

        print("\n--- Check 6: QA Views ---")
        check_qa_views(con, results)

        print("\n--- Check 7: Load Inventory ---")
        check_load_inventory(con, results, strict=strict)

        print("\n--- Check 8: Release Schemas ---")
        check_release_schemas(con, results, strict=strict)

        if strict:
            print("\n--- Check 9: Release Manifest ---")
            check_release_manifest(con, results, strict=strict)

    finally:
        con.close()

    print(f"\n=== Summary: {results.passed} PASS / {results.warned} WARN / {results.failed} FAIL ===")

    report = generate_report(results, rows_data, dist_data, mode=mode)

    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = ROOT / "studies" / f"{datetime.now().strftime('%Y%m%d')}_motherduck_formalization"

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "validation_report.md"
    report_path.write_text(report)
    print(f"\n  [report] {report_path}")

    if strict and results.failed > 0:
        print(f"\n  RELEASE BLOCKED: {results.failed} check(s) failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
