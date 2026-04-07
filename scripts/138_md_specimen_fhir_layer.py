#!/usr/bin/env python3
"""Materialize canonical specimen identity + analytic FHIR export on MotherDuck.

Prereqs (main schema): synoptic_tumor_long_v1, path_synoptics_encounter_qc_v1,
surgery_pathology_linkage_v3, fna_molecular_linkage_v3, preop_surgery_linkage_v3,
molecular_test_episode_v2

Pipeline: scripts/sql/139_specimen_identity_layer_ddl.sql (identity) then
scripts/sql/138_specimen_fhir_tail_ddl.sql (FHIR), then
scripts/140_md_specimen_genomics_binding.apply_specimen_genomics_binding (genomics).

Operational rules (MotherDuck):
  * connect_md_or_file(..., fail_closed=True, custom_user_agent='specimen_fhir_export_v1')
  * Second connection after build deploys `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` with
    custom_user_agent='specimen_fhir_release_ops_v1'
  * RW token only (MOTHERDUCK_TOKEN / MD_SA_TOKEN)
  * Attempt named CREATE SNAPSHOT before DDL; on DuckLake, logs skip and continues

Usage:
  .venv/bin/python scripts/138_md_specimen_fhir_layer.py --md [--dry-run] [--skip-snapshot]
  .venv/bin/python scripts/138_md_specimen_fhir_layer.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
from typing import Any
import importlib.util
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_IDENTITY_PATH = ROOT / "scripts" / "sql" / "139_specimen_identity_layer_ddl.sql"
DDL_FHIR_TAIL_PATH = ROOT / "scripts" / "sql" / "138_specimen_fhir_tail_ddl.sql"
DDL_QA_DIAG_PATH = ROOT / "scripts" / "sql" / "142_specimen_fhir_qa_diagnostics_ddl.sql"
UA = "specimen_fhir_export_v1"
UA_QA_DEPLOY = "specimen_fhir_release_ops_v1"

# Required on the target catalog before DDL (see docs/motherduck_database_contract_v1.md).
PREREQ_MAIN_TABLES: tuple[str, ...] = (
    "synoptic_tumor_long_v1",
    "path_synoptics_encounter_qc_v1",
    "surgery_pathology_linkage_v3",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "molecular_test_episode_v2",
    "tumor_episode_master_v2",
)


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _prod_database_name() -> str:
    return (
        os.environ.get("MOTHERDUCK_DATABASE")
        or os.environ.get("MOTHERDUCK_DB")
        or "Thyroid 2026"
    ).strip()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def try_named_snapshot(con, *, snapshot_name: str, prod: str) -> tuple[str, str]:
    """Return (status, detail). status one of ok|skipped|failed."""
    sql = f"CREATE SNAPSHOT {_quote_ident(snapshot_name)} OF {_quote_ident(prod)};"
    try:
        con.execute(sql)
        return ("ok", sql)
    except Exception as e:
        msg = str(e).lower()
        if (
            "ducklake" in msg
            or ("snapshot" in msg and "not supported" in msg)
            or "does not have snapshots" in msg
            or "not a native duckdb" in msg
        ):
            return ("skipped", f"{e!r} — {sql}")
        return ("failed", f"{e!r} — {sql}")


def missing_prereq_tables(con) -> list[str]:
    """Return main.* table names that are required but absent."""
    return [t for t in PREREQ_MAIN_TABLES if not _table_exists(con, "main", t)]


def _table_exists(con, schema: str, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
        return r is not None
    except Exception:
        return False


def run_validation(con) -> list[tuple[str, str, str]]:
    """Return list of (check_name, status, detail)."""
    out: list[tuple[str, str, str]] = []

    def run(name: str, sql: str, expect_true: bool) -> None:
        try:
            val = con.execute(sql).fetchone()
            ok = bool(val and val[0] is True)
            if not expect_true:
                ok = bool(val and val[0] is not None)
            out.append((name, "PASS" if ok else "FAIL", f"{val}"))
        except Exception as e:
            out.append((name, "FAIL", str(e)))

    run(
        "specimen_master_fingerprint_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT specimen_fingerprint_sha256), FALSE)"
        " FROM main.specimen_master_v1",
        True,
    )
    run(
        "specimen_master_id_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT specimen_id), FALSE) FROM main.specimen_master_v1",
        True,
    )
    run(
        "specimen_focus_fingerprint_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT focus_fingerprint_sha256), FALSE)"
        " FROM main.specimen_tumor_focus_v1",
        True,
    )
    run(
        "specimen_focus_orphan_guard",
        "SELECT NOT EXISTS ("
        " SELECT 1 FROM main.specimen_tumor_focus_v1 f"
        " LEFT JOIN main.specimen_master_v1 m ON f.specimen_id = m.specimen_id"
        " WHERE m.specimen_id IS NULL)",
        True,
    )
    run(
        "multi_synoptic_fp_isolation",
        "SELECT NOT EXISTS ("
        " SELECT 1 FROM main.specimen_master_v1 a"
        " JOIN main.specimen_master_v1 b"
        " ON a.research_id = b.research_id"
        " AND COALESCE(a.procedure_date_day, '') = COALESCE(b.procedure_date_day, '')"
        " AND COALESCE(CAST(a.surgery_episode_id AS VARCHAR), '')"
        "   = COALESCE(CAST(b.surgery_episode_id AS VARCHAR), '')"
        " AND COALESCE(CAST(a.synoptic_row_ix AS VARCHAR), '')"
        "   <> COALESCE(CAST(b.synoptic_row_ix AS VARCHAR), '')"
        " AND a.source_system = 'pathology_synoptic_encounter'"
        " AND b.source_system = 'pathology_synoptic_encounter'"
        " AND a.specimen_fingerprint_sha256 = b.specimen_fingerprint_sha256)",
        True,
    )
    run(
        "genomic_assay_id_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT genomic_assay_id), FALSE)"
        " FROM main.specimen_genomic_assay_v1",
        True,
    )
    run(
        "fhir_specimen_subject_ref",
        """SELECT COALESCE(BOOL_AND(
          json_extract_string(resource_json, '$.subject.reference') IS NOT NULL
          AND starts_with(json_extract_string(resource_json, '$.subject.reference'), 'Patient/')
          AND json_extract_string(resource_json, '$.subject.reference') NOT LIKE 'Patient/Patient/%'
        ), TRUE) FROM main.fhir_specimen_v1""",
        True,
    )
    run(
        "fhir_specimen_collection_procedure_ref",
        """SELECT COALESCE(BOOL_AND(
          json_extract_string(resource_json, '$.collection.procedure.reference')
            = 'Procedure/' || procedure_fhir_id
        ), TRUE) FROM main.fhir_specimen_v1""",
        True,
    )
    run(
        "fhir_procedure_encounter_ref",
        """SELECT COALESCE(BOOL_AND(
          json_extract_string(resource_json, '$.encounter.reference')
            = 'Encounter/' || encounter_fhir_id
        ), TRUE) FROM main.fhir_procedure_collection_v1""",
        True,
    )
    run(
        "fhir_encounter_episode_ref",
        """SELECT COALESCE(BOOL_AND(
          json_extract_string(resource_json, '$.episodeOfCare[0].reference')
            = 'EpisodeOfCare/' || episode_fhir_id
        ), TRUE) FROM main.fhir_encounter_v1""",
        True,
    )
    run(
        "fhir_episode_id_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT episode_fhir_id), FALSE)"
        " FROM main.fhir_episode_of_care_v1",
        True,
    )
    run(
        "fhir_procedure_orphan_guard",
        """SELECT NOT EXISTS (
          SELECT 1 FROM main.fhir_procedure_collection_v1 p
          LEFT JOIN main.fhir_encounter_v1 e
            ON p.specimen_id = e.specimen_id
          WHERE e.specimen_id IS NULL
        )""",
        True,
    )
    return out


def persist_validation(con, rows: list[tuple[str, str, str]]) -> None:
    con.execute("DELETE FROM qa.val_specimen_contract_v1 WHERE 1=1")
    con.executemany(
        "INSERT INTO qa.val_specimen_contract_v1 (check_name, status, detail, measured_at) "
        "VALUES (?, ?, ?, current_timestamp)",
        [(a, b, c) for a, b, c in rows],
    )


def deploy_specimen_fhir_qa_diagnostics(
    con_primary: Any,
    args: argparse.Namespace,
) -> str:
    """Apply qa.v_diag_* views. MotherDuck uses fail-closed UA ``specimen_fhir_release_ops_v1``."""
    ddl = DDL_QA_DIAG_PATH.read_text(encoding="utf-8")
    if args.md:
        hint = (
            os.environ.get("MOTHERDUCK_SESSION_HINT")
            or f"thyroid2026:specimen_fhir_qa_deploy:{_git_sha()[:7]}"
        )
        from utils.md_connect import connect_md_or_file

        con2 = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=True,
            custom_user_agent=UA_QA_DEPLOY,
            motherduck_session_hint=hint,
        )
        try:
            con2.execute(ddl)
        finally:
            con2.close()
        return f"qa diagnostics deployed (UA={UA_QA_DEPLOY})"
    con_primary.execute(ddl)
    return "qa diagnostics deployed (local)"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Specimen + FHIR layer on MotherDuck.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB fallback path.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip CREATE SNAPSHOT preamble.")
    p.add_argument(
        "--study-dir",
        default=None,
        help="Write audit memo here (default: studies/specimen_fhir_export_<UTCtimestamp>/)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    study_slug = os.environ.get("SPECIMEN_FHIR_STUDY_TAG") or (
        f"specimen_fhir_export_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    )
    study_dir = Path(args.study_dir) if args.study_dir else ROOT / "studies" / study_slug
    study_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        print(
            f"[dry-run] Would apply {DDL_IDENTITY_PATH} + {DDL_FHIR_TAIL_PATH} via MotherDuck={args.md}"
        )
        print(f"[dry-run] Study dir {study_dir}")
        return

    from utils.md_connect import connect_md_or_file

    spec139 = importlib.util.spec_from_file_location(
        "_specimen_identity139", ROOT / "scripts" / "139_md_specimen_identity_layer.py"
    )
    mod139 = importlib.util.module_from_spec(spec139)
    assert spec139.loader
    spec139.loader.exec_module(mod139)

    spec140 = importlib.util.spec_from_file_location(
        "_specimen_genomics140", ROOT / "scripts" / "140_md_specimen_genomics_binding.py"
    )
    mod140 = importlib.util.module_from_spec(spec140)
    assert spec140.loader
    spec140.loader.exec_module(mod140)

    hint = os.environ.get("MOTHERDUCK_SESSION_HINT") or "thyroid2026:specimen_fhir:" + _git_sha()[:7]
    con = connect_md_or_file(
        Path(args.db_path),
        md=args.md,
        fail_closed=args.md,
        prefer_service_account=True,
        custom_user_agent=UA,
        motherduck_session_hint=hint,
    )

    identity_run_id = f"specimen_identity_build_v1_{uuid.uuid4().hex[:12]}"

    snap_name = f"specimen_fhir_pre_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    snap_detail = "not_attempted"
    if args.md and not args.skip_snapshot:
        prod = _prod_database_name()
        st, detail = try_named_snapshot(con, snapshot_name=snap_name, prod=prod)
        print(f"  Snapshot {snap_name}: {st} — {detail[:200]}")
        snap_detail = detail
    elif args.skip_snapshot:
        snap_detail = "skipped_flag"

    missing = missing_prereq_tables(con)
    if missing:
        sha = _git_sha()
        fail_lines = [
            "# Specimen + FHIR hardening — blocked (prerequisites)",
            f"Generated: {datetime.now(timezone.utc).isoformat()}Z",
            f"Git SHA: {sha}",
            f"custom_user_agent: {UA}",
            "",
            "## MotherDuck snapshot",
            f"- Attempt: `{snap_name}`",
            f"- Result detail: {snap_detail}",
            "",
            "## Prerequisite tables missing on catalog",
            "The following `main.*` objects must exist before DDL:",
            *(f"- `main.{t}`" for t in missing),
            "",
            "## Remediation (typical)",
            "- `synoptic_tumor_long_v1`: run `scripts/108_synoptic_tumor_long_v1.py --md` (needs `processed/path_synoptics.parquet`).",
            "- `path_synoptics_encounter_qc_v1`: run `scripts/109_synoptic_encounter_qc.py --md` (needs `path_synoptics`).",
            "- `surgery_pathology_linkage_v3`, `fna_molecular_linkage_v3`, `preop_surgery_linkage_v3`, `molecular_test_episode_v2`:",
            "  load analysis/episode contract assets (e.g. `scripts/117_md_contract_views.py --md` + manuscript freeze parquets, or your org’s linkage materialization).",
            "- `tumor_episode_master_v2`: episode contract parquet / `117_md_contract_views.py` (FHIR EpisodeOfCare period enrichment).",
            "",
            "No DDL was applied; fix prerequisites and re-run.",
            "",
        ]
        study_dir.mkdir(parents=True, exist_ok=True)
        (study_dir / "audit_memo.md").write_text("\n".join(fail_lines), encoding="utf-8")
        (study_dir / "prereq_failure.txt").write_text("\n".join(missing), encoding="utf-8")
        print(
            "FATAL: missing prerequisite tables on MotherDuck:\n  - "
            + "\n  - ".join(missing)
            + "\nSee study audit_memo.md for remediation."
        )
        con.close()
        sys.exit(1)

    try:
        con.execute("BEGIN TRANSACTION")
        mod139.apply_specimen_identity_layer(con, identity_run_id, include_specimen_detail=True)
        con.execute(DDL_FHIR_TAIL_PATH.read_text(encoding="utf-8"))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    mod140.apply_specimen_genomics_binding(con)

    val_rows = run_validation(con)
    persist_validation(con, val_rows)
    for name, st, det in val_rows:
        print(f"  [{st}] {name}: {det[:120]}")

    qa_deploy_msg = deploy_specimen_fhir_qa_diagnostics(con, args)
    print(f"  {qa_deploy_msg}")

    sha = _git_sha()
    memo = [
        "# Specimen + FHIR hardening — machine audit memo",
        f"Generated: {datetime.now(timezone.utc).isoformat()}Z",
        f"Git SHA: {sha}",
        f"Identity build_run_id: {identity_run_id}",
        f"custom_user_agent (pipeline): {UA}",
        f"QA diagnostics deploy UA: `{UA_QA_DEPLOY}` (see `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`)",
        "",
        "## MotherDuck snapshot",
        f"- Attempt: `{snap_name}`",
        f"- Result detail: {snap_detail}",
        "",
        "## README vs sign-off vs live",
        "- README (2026-04-07): states MotherDuck formalized; release-mode may still fail on manual_review_queue.",
        "- studies/20260407_signoff_memo/signoff_memo.md: NOT READY (v2_stage/provenance blockers per that memo).",
        "- Checked-in validation artifacts may be stale vs live catalog; this run reflects DB state at execution time.",
        "",
        "## Stale vs current artifacts",
        "- Any checked-in `studies/*/validation_report.md` older than this run's DB time is potentially stale.",
        "- `docs/motherduck_database_contract_v1.md` documents specimen/FHIR surfaces (commit with this change).",
        "",
        "## Validation rows (qa.val_specimen_contract_v1)",
        *[f"- {a}: **{b}** {c}" for a, b, c in val_rows],
        "",
    ]
    (study_dir / "audit_memo.md").write_text("\n".join(memo), encoding="utf-8")

    telemetry_sql = """
SELECT user_agent, session_name, count_star AS approx_queries
FROM (
  SELECT
    coalesce(user_agent, '') AS user_agent,
    coalesce(session_name, '') AS session_name,
    COUNT(*) AS count_star
  FROM md_information_schema.query_history
  WHERE user_agent = 'specimen_fhir_export_v1'
  GROUP BY 1, 2
) q
LIMIT 20
"""
    tele_lines = [
        f"MD_INFORMATION_SCHEMA.QUERY_HISTORY filter user_agent={UA} (if permission denied, empty)."
    ]
    if args.md:
        try:
            df = con.execute(telemetry_sql).fetchdf()
            tele_lines.append(df.to_markdown(index=False))
        except Exception as e:
            tele_lines.append(f"(telemetry unavailable: {e})")
    (study_dir / "query_history_telemetry.md").write_text("\n".join(tele_lines), encoding="utf-8")

    impl = [
        "# Implementation report — specimen + FHIR hardening",
        f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}",
        f"**Commit SHA:** `{sha}`",
        "",
        "## Source inventory",
        "- `scripts/sql/139_specimen_identity_layer_ddl.sql` — identity DDL",
        "- `scripts/sql/138_specimen_fhir_tail_ddl.sql` — genomic + FHIR DDL",
        "- `scripts/139_md_specimen_identity_layer.py` — standalone identity runner",
        "- `scripts/138_md_specimen_fhir_layer.py` — orchestrator",
        "- `utils/specimen_fingerprint.py` — fingerprint test helpers",
        "",
        "## Table contract",
        "- `main.specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `specimen_source_xref_v1`",
        "- `qa.specimen_merge_review_queue_v1`, `qa.val_specimen_contract_v1`",
        "- `main.fhir_*_v1` + `main.fhir_bundle_specimen_export_v1`",
        "",
        "## Matching policy",
        "- Auto-merge: exact `specimen_fingerprint_sha256` only (full rebuild replaces derived tables).",
        "- Near-duplicate pairs → `qa.specimen_merge_review_queue_v1` (same patient/day/surgery_episode, distinct FP).",
        "- Genomics: scripts/140 — v3 linkage chain, optional genetic_testing + ThyroSeq JSON explosion.",
        "",
        "## Unresolved review burden",
        "- See row count `SELECT COUNT(*) FROM qa.specimen_merge_review_queue_v1` on target DB.",
        "",
        "## Test / lint",
        "- Run `pytest tests/test_specimen_fhir_layer.py` and `ruff` / `mypy` per CI.",
        "",
        "## MotherDuck snapshot / share",
        f"- Snapshot attempt recorded in audit_memo.md for this run (`{snap_name}`).",
        "- Optional read-only share: attach promoted DB in MotherDuck UI; document token path per org policy.",
        "",
    ]
    (study_dir / "implementation_report.md").write_text("\n".join(impl), encoding="utf-8")

    con.close()
    print(f"Done. Study artifacts under {study_dir}")


if __name__ == "__main__":
    main()
