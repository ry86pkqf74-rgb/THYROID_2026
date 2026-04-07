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
  9. qa.release_manifest (release-mode)
 10. Non-null extraction_run_id on main.canonical_extracted_fact_long_v2 (release-mode, contract §3)
 11. Analyst presentation views (release-mode): main.master_fact_long_verified_v1,
     main.master_patient_rollup_verified_v1, main.master_source_lineage_v1 — existence,
     required traceability columns, and non-null core fields (see check_presentation_layer)
 12. Molecular normalized contract views (scripts/sql/133_molecular_contract_views_ddl.sql):
     required columns, live-row parity vs main.molecular_results, payload_checksum uniqueness,
     allele_fraction bounds, variant_class enum, provenance columns, assay/panel pairing
 13. Specimen + analytic FHIR layer (scripts/138_md_specimen_fhir_layer.py): table presence when
     synoptic_tumor_long_v1 exists; fingerprint uniqueness; qa.val_specimen_contract_v1 FAIL rows

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

# Analyst presentation layer (scripts/125_master_verified_views.py, runbook §5).
PRESENTATION_LONG_VIEWS = (
    "master_fact_long_verified_v1",
    "master_source_lineage_v1",
)
PRESENTATION_ROLLUP_VIEW = "master_patient_rollup_verified_v1"
# Long-form rows: identity + run + release; reviewer_status column required (may be NULL when no queue match).
PRESENTATION_LONG_TRACE_COLS = (
    "research_id",
    "source_domain",
    "extraction_run_id",
    "reviewer_status",
    "release_tag",
)
# Exactly one of these identifies the source note row.
PRESENTATION_SOURCE_ID_COLS = ("source_object_id", "note_row_id")
# Rollup is aggregated: no per-fact source_domain; traceability via patient + release + review metrics.
PRESENTATION_ROLLUP_TRACE_COLS = (
    "research_id",
    "release_tag",
    "reviewed_facts",
    "pct_reviewed",
)

# Molecular normalized layer (scripts/sql/133_molecular_contract_views_ddl.sql)
MOLECULAR_RESULTS_TABLE = "molecular_results"
MOLECULAR_CONTRACT_VIEWS: dict[str, tuple[str, ...]] = {
    "molecular_results_contract_v": (
        "molecular_result_id",
        "research_id",
        "assay_name",
        "panel_version",
        "payload_checksum",
        "parse_status",
        "normalization_status",
        "lineage_id",
        "ingestion_ts",
        "ingestion_run_id",
        "source_table",
        "source_row_fingerprint",
    ),
    "molecular_variant_contract_v": (
        "molecular_variant_id",
        "molecular_result_id",
        "research_id",
        "variant_class",
        "allele_fraction",
        "lineage_id",
        "ingestion_ts",
    ),
    "molecular_qc_summary_v": (
        "source_table",
        "parse_status",
        "normalization_status",
        "n_results",
        "n_patients",
    ),
    "molecular_patient_rollup_v": (
        "research_id",
        "n_molecular_results",
        "n_variant_calls",
        "latest_test_date_parsed",
    ),
}


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
        "Default: MOTHERDUCK_CUSTOM_USER_AGENT or THYROID_2026_molecular/119_md_formalization_validate;kind=validate",
    )
    p.add_argument(
        "--md-session-hint",
        default=None,
        help="SET motherduck_session_hint after connect. Default: MOTHERDUCK_SESSION_HINT.",
    )
    p.add_argument("--release-mode", action="store_true",
                    help="Strict release validation: FAIL on missing infrastructure.")
    p.add_argument(
        "--md-env",
        default=None,
        help="MotherDuck environment (dev|qa|prod) when using --md.",
    )
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    p.add_argument("--v2-parquets-dir", default=str(DEFAULT_V2_DIR))
    p.add_argument("--output-dir", default=None, help="Output directory for report.")
    return p.parse_args()


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        import os
        from utils.md_connect import connect_md_or_file

        if args.md_env and not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get(
            "MOTHERDUCK_DB"
        ):
            from motherduck_client import resolve_database_for_env

            os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(args.md_env)

        from utils.md_pipeline_attribution import (
            molecular_custom_user_agent,
            molecular_session_hint,
        )

        ua = args.md_user_agent or molecular_custom_user_agent(
            "119_md_formalization_validate", "validate"
        )
        if args.md_session_hint is not None and str(args.md_session_hint).strip():
            hint = str(args.md_session_hint).strip()
        else:
            hint = molecular_session_hint("validate")
        return connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=args.md_sa,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
            env=args.md_env,
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


def check_canonical_extraction_run_id(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Release-mode check: main.canonical_extracted_fact_long_v2 must carry extraction_run_id.

    Matches docs/motherduck_database_contract_v1.md §3 (required for entity provenance).
    """
    try:
        row = con.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE extraction_run_id IS NULL
                    OR trim(cast(extraction_run_id AS VARCHAR)) = ''
                ) AS blank
            FROM main.canonical_extracted_fact_long_v2
            """
        ).fetchone()
        total, blank = int(row[0]), int(row[1])
        if blank == 0:
            results.add(
                "Canonical extraction_run_id",
                "PASS",
                f"{total:,} rows; 0 blank (contract §3)",
            )
        else:
            status = "FAIL" if strict else "WARN"
            results.add(
                "Canonical extraction_run_id",
                status,
                f"{blank:,} / {total:,} rows have NULL/blank extraction_run_id",
            )
    except Exception as exc:
        status = "FAIL" if strict else "WARN"
        results.add("Canonical extraction_run_id", status, str(exc))


def _main_view_columns(con: duckdb.DuckDBPyConnection, view_name: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [view_name],
    ).fetchall()
    return {r[0] for r in rows}


def check_presentation_layer(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Release / structural: analyst-facing verified views exist with traceability columns.

    Long-form views must expose research_id, source_domain, source_object_id or note_row_id,
    extraction_run_id, reviewer_status (column present; values may be NULL), and release_tag.
    Rollup view exposes patient-level release_tag and review-coverage metrics instead of per-note ids.
    """
    status_fail = "FAIL" if strict else "WARN"
    for view in PRESENTATION_LONG_VIEWS:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{view}").fetchone()[0]
        except Exception as exc:
            results.add(f"Presentation {view}", status_fail, f"not queryable: {exc}")
            continue
        if n == 0:
            results.add(f"Presentation {view}", status_fail, "0 rows (run 125 after 103/114)")
            continue
        cols = _main_view_columns(con, view)
        missing = [c for c in PRESENTATION_LONG_TRACE_COLS if c not in cols]
        sid_ok = any(c in cols for c in PRESENTATION_SOURCE_ID_COLS)
        detail_parts: list[str] = []
        if missing:
            detail_parts.append(f"missing columns: {missing}")
        if not sid_ok:
            detail_parts.append(
                f"need one of {PRESENTATION_SOURCE_ID_COLS} for note-level traceability"
            )
        if detail_parts:
            results.add(f"Presentation {view}", status_fail, "; ".join(detail_parts))
            continue

        id_col = "source_object_id" if "source_object_id" in cols else "note_row_id"
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (WHERE research_id IS NULL) AS n_rid,
                COUNT(*) FILTER (
                    WHERE source_domain IS NULL
                    OR trim(cast(source_domain AS VARCHAR)) = ''
                ) AS n_dom,
                COUNT(*) FILTER (WHERE "{id_col}" IS NULL) AS n_sid,
                COUNT(*) FILTER (
                    WHERE extraction_run_id IS NULL
                    OR trim(cast(extraction_run_id AS VARCHAR)) = ''
                ) AS n_run,
                COUNT(*) FILTER (
                    WHERE release_tag IS NULL
                    OR trim(cast(release_tag AS VARCHAR)) = ''
                ) AS n_tag
            FROM main.{view}
            """
        ).fetchone()
        ntot = int(row[0])
        n_rid, n_dom, n_sid, n_run, n_tag = (int(row[i]) for i in range(1, 6))
        problems: list[str] = []
        if n_rid:
            problems.append(f"research_id NULL: {n_rid:,}")
        if n_dom:
            problems.append(f"source_domain blank: {n_dom:,}")
        if n_sid:
            problems.append(f"{id_col} NULL: {n_sid:,}")
        if n_run:
            problems.append(f"extraction_run_id blank: {n_run:,}")
        if n_tag:
            problems.append(f"release_tag blank: {n_tag:,}")
        if problems:
            results.add(
                f"Presentation {view}",
                status_fail,
                f"{ntot:,} rows; " + "; ".join(problems),
            )
        else:
            results.add(
                f"Presentation {view}",
                "PASS",
                f"{ntot:,} rows; core traceability non-null (reviewer_status may be NULL)",
            )

    view = PRESENTATION_ROLLUP_VIEW
    try:
        n = con.execute(f"SELECT COUNT(*) FROM main.{view}").fetchone()[0]
    except Exception as exc:
        results.add(f"Presentation {view}", status_fail, f"not queryable: {exc}")
        return
    if n == 0:
        results.add(f"Presentation {view}", status_fail, "0 rows (run 125 after 103/114)")
        return
    cols = _main_view_columns(con, view)
    missing = [c for c in PRESENTATION_ROLLUP_TRACE_COLS if c not in cols]
    if missing:
        results.add(f"Presentation {view}", status_fail, f"missing columns: {missing}")
        return
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE research_id IS NULL) AS n_rid,
            COUNT(*) FILTER (
                WHERE release_tag IS NULL
                OR trim(cast(release_tag AS VARCHAR)) = ''
            ) AS n_tag
        FROM main.{view}
        """
    ).fetchone()
    ntot, n_rid, n_tag = int(row[0]), int(row[1]), int(row[2])
    if n_rid or n_tag:
        results.add(
            f"Presentation {view}",
            status_fail,
            f"{ntot:,} rows; research_id NULL={n_rid:,}, release_tag blank={n_tag:,}",
        )
    else:
        results.add(
            f"Presentation {view}",
            "PASS",
            f"{ntot:,} patient rows; research_id + release_tag + review metrics present",
        )


def _main_object_exists(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _view_or_table_columns(con: duckdb.DuckDBPyConnection, name: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [name],
    ).fetchall()
    return {r[0] for r in rows}


def check_molecular_normalized_contract(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Validate molecular normalized tables + 133 contract views (checksums, enums, provenance)."""
    status_fail = "FAIL" if strict else "WARN"

    if not _main_object_exists(con, MOLECULAR_RESULTS_TABLE):
        results.add(
            "Molecular layer present",
            "PASS",
            "main.molecular_results not found — molecular contract checks skipped",
        )
        return

    try:
        n_all = int(
            con.execute(
                f"SELECT COUNT(*) FROM main.{MOLECULAR_RESULTS_TABLE}"
            ).fetchone()[0]
        )
        n_live = int(
            con.execute(
                f"SELECT COUNT(*) FROM main.{MOLECULAR_RESULTS_TABLE} "
                f"WHERE superseded_by_molecular_result_id IS NULL"
            ).fetchone()[0]
        )
    except Exception as exc:
        results.add("Molecular base table", status_fail, str(exc))
        return

    if n_all == 0:
        results.add(
            "Molecular row counts",
            "PASS",
            "main.molecular_results is empty — contract view checks skipped",
        )
        return

    # Required contract views + columns
    col_failures = 0
    for view_name, required in MOLECULAR_CONTRACT_VIEWS.items():
        if not _main_object_exists(con, view_name):
            results.add(
                f"Molecular view {view_name}",
                status_fail,
                "view/table missing — run scripts/117_md_contract_views.py",
            )
            col_failures += 1
            continue
        cols = _view_or_table_columns(con, view_name)
        missing = [c for c in required if c not in cols]
        if missing:
            col_failures += 1
            results.add(
                f"Molecular view {view_name}",
                status_fail,
                f"missing columns: {missing}",
            )
    if col_failures == 0:
        results.add(
            "Molecular contract required columns",
            "PASS",
            f"all {len(MOLECULAR_CONTRACT_VIEWS)} views expose required fields",
        )

    # Row counts: live results must match contract slice when views exist
    if _main_object_exists(con, "molecular_results_contract_v"):
        try:
            n_contract = int(
                con.execute(
                    "SELECT COUNT(*) FROM main.molecular_results_contract_v"
                ).fetchone()[0]
            )
            if n_contract != n_live:
                results.add(
                    "Molecular results contract row parity",
                    status_fail,
                    f"molecular_results live={n_live:,} vs "
                    f"molecular_results_contract_v={n_contract:,}",
                )
            else:
                results.add(
                    "Molecular results contract row parity",
                    "PASS",
                    f"{n_contract:,} live rows in contract view",
                )
        except Exception as exc:
            results.add("Molecular results contract row parity", status_fail, str(exc))

        # Primary key uniqueness on result id in contract slice
        try:
            row = con.execute(
                """
                SELECT COUNT(*) AS n,
                       COUNT(DISTINCT molecular_result_id) AS n_id
                FROM main.molecular_results_contract_v
                """
            ).fetchone()
            ntot, nid = int(row[0]), int(row[1])
            if ntot != nid:
                results.add(
                    "Molecular result_id uniqueness",
                    status_fail,
                    f"{ntot:,} rows but {nid:,} distinct molecular_result_id",
                )
            else:
                results.add(
                    "Molecular result_id uniqueness",
                    "PASS",
                    f"{ntot:,} rows; distinct molecular_result_id",
                )
        except Exception as exc:
            results.add("Molecular result_id uniqueness", status_fail, str(exc))

    # Non-zero contract when source has live rows (release signal)
    if n_live > 0 and _main_object_exists(con, "molecular_results_contract_v"):
        try:
            nc = int(
                con.execute(
                    "SELECT COUNT(*) FROM main.molecular_results_contract_v"
                ).fetchone()[0]
            )
            if nc == 0:
                results.add(
                    "Molecular contract non-empty",
                    status_fail,
                    "molecular_results has live rows but contract view is empty",
                )
            else:
                results.add(
                    "Molecular contract non-empty",
                    "PASS",
                    f"{nc:,} contract rows",
                )
        except Exception as exc:
            results.add("Molecular contract non-empty", status_fail, str(exc))

    # Payload checksum uniqueness (non-null only)
    if _main_object_exists(con, "molecular_results_contract_v"):
        try:
            row = con.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE payload_checksum IS NOT NULL) AS nn,
                    COUNT(DISTINCT payload_checksum) FILTER (
                        WHERE payload_checksum IS NOT NULL
                    ) AS nd
                FROM main.molecular_results_contract_v
                """
            ).fetchone()
            nn, nd = int(row[0]), int(row[1])
            if nn > 0 and nn != nd:
                results.add(
                    "Molecular payload_checksum uniqueness",
                    status_fail,
                    f"{nn:,} non-null checksums but {nd:,} distinct (duplicate fingerprints)",
                )
            elif nn == 0:
                results.add(
                    "Molecular payload_checksum uniqueness",
                    "WARN" if strict else "PASS",
                    "no non-null payload_checksum rows to verify uniqueness",
                )
            else:
                results.add(
                    "Molecular payload_checksum uniqueness",
                    "PASS",
                    f"{nn:,} non-null checksums; all distinct",
                )
        except Exception as exc:
            results.add("Molecular payload_checksum uniqueness", status_fail, str(exc))

    # Provenance: lineage + ingestion on results contract
    if _main_object_exists(con, "molecular_results_contract_v"):
        try:
            row = con.execute(
                """
                SELECT
                    COUNT(*) AS n,
                    COUNT(*) FILTER (
                        WHERE lineage_id IS NULL
                        OR trim(cast(lineage_id AS VARCHAR)) = ''
                    ) AS n_bad_lineage,
                    COUNT(*) FILTER (WHERE ingestion_ts IS NULL) AS n_bad_ts
                FROM main.molecular_results_contract_v
                """
            ).fetchone()
            n, bad_l, bad_t = int(row[0]), int(row[1]), int(row[2])
            if bad_l or bad_t:
                results.add(
                    "Molecular provenance (results)",
                    status_fail,
                    f"{n:,} rows; blank lineage={bad_l:,}; null ingestion_ts={bad_t:,}",
                )
            else:
                results.add(
                    "Molecular provenance (results)",
                    "PASS",
                    f"{n:,} rows; lineage_id + ingestion_ts present",
                )
        except Exception as exc:
            results.add("Molecular provenance (results)", status_fail, str(exc))

    # Allele fraction bounds on variant contract
    if _main_object_exists(con, "molecular_variant_contract_v"):
        try:
            bad_af = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM main.molecular_variant_contract_v
                    WHERE allele_fraction IS NOT NULL
                      AND (
                          allele_fraction < 0
                          OR allele_fraction > 1
                          OR allele_fraction != allele_fraction
                      )
                    """
                ).fetchone()[0]
            )
            n_var = int(
                con.execute(
                    "SELECT COUNT(*) FROM main.molecular_variant_contract_v"
                ).fetchone()[0]
            )
            if bad_af:
                results.add(
                    "Molecular allele_fraction bounds",
                    status_fail,
                    f"{bad_af:,} / {n_var:,} rows outside [0,1] or non-finite",
                )
            else:
                results.add(
                    "Molecular allele_fraction bounds",
                    "PASS",
                    f"{n_var:,} variant rows; AF in [0,1] or NULL",
                )
        except Exception as exc:
            results.add("Molecular allele_fraction bounds", status_fail, str(exc))

    # Variant class enum (canonical bucket set)
    if _main_object_exists(con, "molecular_variant_contract_v"):
        try:
            bad_vc = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM main.molecular_variant_contract_v
                    WHERE upper(trim(cast(variant_class AS VARCHAR))) NOT IN (
                        'SNV', 'INDEL', 'FUSION', 'CNV', 'OTHER'
                    )
                    OR variant_class IS NULL
                    OR trim(cast(variant_class AS VARCHAR)) = ''
                    """
                ).fetchone()[0]
            )
            if bad_vc:
                results.add(
                    "Molecular variant_class enum",
                    status_fail,
                    f"{bad_vc:,} rows with NULL/blank/non-canonical variant_class",
                )
            else:
                results.add(
                    "Molecular variant_class enum",
                    "PASS",
                    "all variant_class values in {SNV,INDEL,FUSION,CNV,OTHER}",
                )
        except Exception as exc:
            results.add("Molecular variant_class enum", status_fail, str(exc))

    # Assay / panel_version: panel should not be blank when assay_name is populated
    if _main_object_exists(con, "molecular_results_contract_v"):
        try:
            bad_panel = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM main.molecular_results_contract_v
                    WHERE assay_name IS NOT NULL
                      AND trim(cast(assay_name AS VARCHAR)) != ''
                      AND (
                          panel_version IS NULL
                          OR trim(cast(panel_version AS VARCHAR)) = ''
                      )
                    """
                ).fetchone()[0]
            )
            if bad_panel:
                results.add(
                    "Molecular assay/panel_version pairing",
                    "WARN",
                    f"{bad_panel:,} rows with assay_name but empty panel_version",
                )
            else:
                results.add(
                    "Molecular assay/panel_version pairing",
                    "PASS",
                    "no assay rows with blank panel_version",
                )
        except Exception as exc:
            results.add("Molecular assay/panel_version pairing", status_fail, str(exc))

    # Optional: cross-check assay_key targets from dictionary for known Afirma assay_name tokens
    if (
        _main_object_exists(con, "molecular_assay_dictionary")
        and _main_object_exists(con, "molecular_results_contract_v")
    ):
        try:
            outsiders = int(
                con.execute(
                    """
                    SELECT COUNT(DISTINCT r.assay_name)
                    FROM main.molecular_results_contract_v r
                    WHERE r.assay_name IS NOT NULL
                      AND trim(cast(r.assay_name AS VARCHAR)) != ''
                      AND NOT EXISTS (
                          SELECT 1
                          FROM main.molecular_assay_dictionary d
                          WHERE d.assay_name = r.assay_name
                      )
                    """
                ).fetchone()[0]
            )
            if outsiders:
                results.add(
                    "Molecular assay_name dictionary match",
                    "WARN",
                    f"{outsiders} distinct assay_name value(s) not in molecular_assay_dictionary "
                    f"(expected for non-afirma panels such as ThyroSeq)",
                )
            else:
                results.add(
                    "Molecular assay_name dictionary match",
                    "PASS",
                    "all assay_name values appear in molecular_assay_dictionary",
                )
        except Exception as exc:
            results.add("Molecular assay_name dictionary match", "WARN", str(exc))


SPECIMEN_FHIR_OBJECTS = (
    "specimen_master_v1",
    "specimen_tumor_focus_v1",
    "specimen_genomic_assay_v1",
    "specimen_source_xref_v1",
    "fhir_patient_deid_map_v1",
    "fhir_specimen_v1",
    "fhir_procedure_collection_v1",
    "fhir_encounter_v1",
    "fhir_episode_of_care_v1",
    "fhir_bundle_specimen_export_v1",
)


def check_specimen_fhir_layer(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Validate specimen identity + analytic FHIR tables when materialized (Check 13)."""
    status_skip = "WARN" if not strict else "FAIL"
    anchor = "synoptic_tumor_long_v1"
    if not _main_object_exists(con, anchor):
        results.add(
            "Specimen layer prerequisites",
            "PASS",
            f"main.{anchor} absent — specimen/FHIR checks skipped (run 108 + 109 + 138)",
        )
        return
    missing = [t for t in SPECIMEN_FHIR_OBJECTS if not _main_object_exists(con, t)]
    if missing:
        results.add(
            "Specimen/FHIR tables present",
            status_skip,
            f"missing: {', '.join(missing)} — run scripts/138_md_specimen_fhir_layer.py",
        )
        if strict:
            return
    else:
        results.add(
            "Specimen/FHIR tables present",
            "PASS",
            f"{len(SPECIMEN_FHIR_OBJECTS)} objects found",
        )

    if not _main_object_exists(con, "specimen_master_v1"):
        return

    try:
        ok_fp = con.execute(
            "SELECT COUNT(*) = COUNT(DISTINCT specimen_fingerprint_sha256) "
            "FROM main.specimen_master_v1"
        ).fetchone()[0]
        results.add(
            "Specimen master fingerprint uniqueness",
            "PASS" if ok_fp else status_skip,
            "distinct fingerprints" if ok_fp else "duplicate specimen_fingerprint_sha256",
        )
    except Exception as exc:
        results.add("Specimen master fingerprint uniqueness", status_skip, str(exc))

    if _main_object_exists(con, "qa.val_specimen_contract_v1"):
        try:
            nfail = int(
                con.execute(
                    "SELECT COUNT(*) FROM qa.val_specimen_contract_v1 WHERE UPPER(status) = 'FAIL'"
                ).fetchone()[0]
            )
            if nfail > 0:
                results.add(
                    "qa.val_specimen_contract_v1",
                    status_skip,
                    f"{nfail} failing row(s) — inspect after scripts/138",
                )
            else:
                results.add("qa.val_specimen_contract_v1", "PASS", "no FAIL rows recorded")
        except Exception as exc:
            results.add("qa.val_specimen_contract_v1", status_skip, str(exc))


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
                "ORDER BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, "
                "created_at DESC LIMIT 1"
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

            print("\n--- Check 10: Canonical extraction_run_id (contract) ---")
            check_canonical_extraction_run_id(con, results, strict=strict)

        print("\n--- Check 11: Analyst presentation layer (master_*_verified_v1) ---")
        check_presentation_layer(con, results, strict=strict)

        print("\n--- Check 12: Molecular normalized contract views ---")
        check_molecular_normalized_contract(con, results, strict=strict)

        print("\n--- Check 13: Specimen + analytic FHIR layer ---")
        check_specimen_fhir_layer(con, results, strict=strict)

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
