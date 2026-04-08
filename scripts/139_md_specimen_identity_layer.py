#!/usr/bin/env python3
"""Materialize canonical specimen identity on MotherDuck (no FHIR genomic tail).

Rules:
  * fail_closed MotherDuck, RW token only (MOTHERDUCK_TOKEN / MD_SA_TOKEN)
  * :func:`specimen_fhir_release_writer_attribution` for query-history UA / session hint
  * CREATE SNAPSHOT before DDL when --md (same pattern as 138)
  * Optional main.specimen_detail seed — identifiers only (no note text)

Usage:
  .venv/bin/python scripts/139_md_specimen_identity_layer.py --md [--skip-snapshot]
  .venv/bin/python scripts/139_md_specimen_identity_layer.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_PATH = ROOT / "scripts" / "sql" / "139_specimen_identity_layer_ddl.sql"

PREREQ_MAIN_TABLES: tuple[str, ...] = (
    "synoptic_tumor_long_v1",
    "path_synoptics_encounter_qc_v1",
    "surgery_pathology_linkage_v3",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "molecular_test_episode_v2",
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


def missing_prereq_tables(con) -> list[str]:
    return [t for t in PREREQ_MAIN_TABLES if not _table_exists(con, "main", t)]


def load_identity_sql(run_id: str) -> str:
    return DDL_PATH.read_text(encoding="utf-8").replace("__BUILD_RUN_ID__", run_id)


def _split_sql_statements(sql: str) -> list[str]:
    """Split on ``;`` outside single-quoted strings and outside ``--`` line comments.

    ``139_specimen_identity_layer_ddl.sql`` embeds semicolons inside string literals
    (e.g. concat-based evidence fields) and inside ``-- ...`` comment lines
    (e.g. ``DuckLake: no indexes; idempotent ...``). MotherDuck may also fail a
    single giant multi-statement execute; running one statement at a time is reliable.
    """
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    in_line_comment = False
    while i < n:
        c = sql[i]
        if in_line_comment:
            buf.append(c)
            if c in "\n":
                in_line_comment = False
            i += 1
            continue
        if in_single:
            buf.append(c)
            if c == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    buf.append(sql[i + 1])
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            in_line_comment = True
            buf.append(c)
            buf.append(sql[i + 1])
            i += 2
            continue
        if c == "'":
            in_single = True
            buf.append(c)
            i += 1
            continue
        if c == ";":
            buf.append(c)
            stmt = "".join(buf).strip()
            if stmt:
                parts.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def run_identity_validation(con) -> list[tuple[str, str, str]]:
    """Identity-only checks after ``139`` DDL.

    Focus-grain duplicate / orphan / provenance predicates mirror ``142`` list views
    but are expressed inline here because standalone ``139`` runs do not deploy
    ``142`` (genomic + FHIR tables required for full QA DDL).
    """
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
        "specimen_master_provenance_build_id",
        "SELECT NOT EXISTS ("
        " SELECT 1 FROM main.specimen_master_v1"
        " WHERE identity_build_run_id IS NULL OR trim(CAST(identity_build_run_id AS VARCHAR)) = '')",
        True,
    )
    run(
        "specimen_focus_provenance_build_id",
        "SELECT NOT EXISTS ("
        " SELECT 1 FROM main.specimen_tumor_focus_v1"
        " WHERE identity_build_run_id IS NULL OR trim(CAST(identity_build_run_id AS VARCHAR)) = '')",
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


def maybe_specimen_detail_seed(con, run_id: str) -> None:
    if not _table_exists(con, "main", "specimen_detail"):
        return
    if not _table_exists(con, "main", "master_cohort"):
        return
    try:
        con.execute(
            "DELETE FROM main.specimen_master_v1 WHERE source_system = 'specimen_detail_aggregate'"
        )
    except Exception as exc:
        print(f"  [warn] specimen_detail pre-delete: {exc}")
    sql = """
INSERT INTO main.specimen_master_v1 BY NAME
WITH sd0 AS (
  SELECT DISTINCT
    CAST(sd.research_id AS BIGINT) AS research_id,
    strftime(
      COALESCE(
        TRY_CAST(sd.date_of_surgery AS DATE),
        TRY_CAST(mc.surgery_date AS DATE)
      ),
      '%Y-%m-%d'
    ) AS procedure_date_day
  FROM main.specimen_detail sd
  INNER JOIN main.master_cohort mc
    ON CAST(sd.research_id AS BIGINT) = mc.research_id
),
fp AS (
  SELECT
    research_id,
    COALESCE(NULLIF(trim(CAST(procedure_date_day AS VARCHAR)), ''), '') AS procedure_date_day,
    'specimen_detail_aggregate'::VARCHAR AS source_system,
    ''::VARCHAR AS accession_or_source_id,
    'gross_aggregate'::VARCHAR AS specimen_role,
    'thyroid'::VARCHAR AS anatomic_site,
    ''::VARCHAR AS laterality,
    CAST(NULL AS BIGINT) AS surgery_episode_id,
    CAST(0 AS BIGINT) AS encounter_synoptic_row_ix,
    CAST(0 AS BIGINT) AS synoptic_row_ix,
    concat_ws(
      '|',
      LOWER(TRIM(CAST(research_id AS VARCHAR))),
      LOWER(TRIM('specimen_detail_aggregate')),
      LOWER(TRIM(COALESCE(procedure_date_day, ''))),
      LOWER(TRIM('')),
      LOWER(TRIM('gross_aggregate')),
      LOWER(TRIM('thyroid')),
      LOWER(TRIM('')),
      LOWER(TRIM('')),
      LOWER(TRIM(CAST(0 AS VARCHAR))),
      LOWER(TRIM(CAST(0 AS VARCHAR)))
    ) AS fingerprint_input_canonical,
    sha256(concat_ws(
      '|',
      LOWER(TRIM(CAST(research_id AS VARCHAR))),
      LOWER(TRIM('specimen_detail_aggregate')),
      LOWER(TRIM(COALESCE(procedure_date_day, ''))),
      LOWER(TRIM('')),
      LOWER(TRIM('gross_aggregate')),
      LOWER(TRIM('thyroid')),
      LOWER(TRIM('')),
      LOWER(TRIM('')),
      LOWER(TRIM(CAST(0 AS VARCHAR))),
      LOWER(TRIM(CAST(0 AS VARCHAR)))
    )) AS specimen_fingerprint_sha256
  FROM sd0
  WHERE research_id IS NOT NULL
)
SELECT
  ('spm_' || specimen_fingerprint_sha256) AS specimen_id,
  specimen_fingerprint_sha256,
  fingerprint_input_canonical,
  research_id,
  source_system,
  procedure_date_day,
  accession_or_source_id,
  specimen_role,
  anatomic_site,
  laterality,
  surgery_episode_id,
  encounter_synoptic_row_ix,
  synoptic_row_ix,
  'specimen_detail_gross'::VARCHAR AS source_candidate_kind,
  CAST(? AS VARCHAR) AS identity_build_run_id,
  current_timestamp AS identity_built_at,
  current_timestamp AS materialized_at
FROM fp;
"""
    try:
        con.execute(sql, [run_id])
    except Exception as exc:
        print(f"  [warn] specimen_detail seed skipped: {exc}")


def apply_specimen_identity_layer(con, run_id: str, *, include_specimen_detail: bool = True) -> None:
    sql = load_identity_sql(run_id)
    for idx, stmt in enumerate(_split_sql_statements(sql)):
        if not stmt.strip():
            continue
        try:
            con.execute(stmt)
        except Exception as exc:
            head = stmt[:480].replace("\n", " ")
            tail = stmt[-120:].replace("\n", " ") if len(stmt) > 480 else ""
            raise RuntimeError(
                f"specimen identity DDL failed at batched statement index {idx} "
                f"(len={len(stmt)} chars): {exc}\n"
                f"statement_head: {head!s}\n"
                f"statement_tail: {tail!s}"
            ) from exc
    if include_specimen_detail:
        maybe_specimen_detail_seed(con, run_id)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Canonical specimen identity layer on MotherDuck.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB fallback path.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip CREATE SNAPSHOT preamble.")
    p.add_argument(
        "--no-specimen-detail",
        action="store_true",
        help="Skip optional main.specimen_detail seed.",
    )
    p.add_argument(
        "--study-dir",
        default=None,
        help="Write validation report here (default: studies/specimen_identity_build_<utc>/)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    utc_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    study_dir = (
        Path(args.study_dir)
        if args.study_dir
        else ROOT / "studies" / f"specimen_identity_build_{utc_stamp}"
    )
    study_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        print(f"[dry-run] Would apply {DDL_PATH} via MotherDuck={args.md}")
        print(f"[dry-run] Study dir {study_dir}")
        return

    from utils.md_connect import connect_md_or_file
    from utils.md_pipeline_attribution import specimen_fhir_release_writer_attribution

    ua, hint = specimen_fhir_release_writer_attribution()
    run_id = f"specimen_identity_{uuid.uuid4().hex[:12]}"
    con = connect_md_or_file(
        Path(args.db_path),
        md=args.md,
        fail_closed=args.md,
        prefer_service_account=True,
        custom_user_agent=ua,
        motherduck_session_hint=hint,
    )

    snap_name = f"specimen_identity_pre_{utc_stamp}"
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
            "# Specimen identity — blocked (prerequisites)",
            f"Generated: {datetime.now(timezone.utc).isoformat()}Z",
            f"Git SHA: {sha}",
            f"custom_user_agent: {ua}",
            "",
            "## MotherDuck snapshot",
            f"- Attempt: `{snap_name}`",
            f"- Result detail: {snap_detail}",
            "",
            "## Missing `main.*` tables",
            *(f"- `main.{t}`" for t in missing),
            "",
        ]
        (study_dir / "audit_memo.md").write_text("\n".join(fail_lines), encoding="utf-8")
        (study_dir / "prereq_failure.txt").write_text("\n".join(missing), encoding="utf-8")
        print(
            "FATAL: missing prerequisite tables:\n  - "
            + "\n  - ".join(missing)
            + f"\nSee {study_dir / 'audit_memo.md'}"
        )
        con.close()
        sys.exit(1)

    try:
        con.execute("BEGIN TRANSACTION")
        apply_specimen_identity_layer(
            con,
            run_id,
            include_specimen_detail=not args.no_specimen_detail,
        )
        val_rows = run_identity_validation(con)
        persist_validation(con, val_rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    for name, st, det in val_rows:
        print(f"  [{st}] {name}: {det[:140]}")

    sha = _git_sha()
    report = [
        "# Specimen identity — validation report",
        f"Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
        f"Git SHA: `{sha}`",
        f"custom_user_agent: `{ua}`",
        f"identity_build_run_id: `{run_id}`",
        "",
        "## Snapshot",
        f"- Name: `{snap_name}`",
        f"- Detail: {snap_detail}",
        "",
        "## qa.val_specimen_contract_v1",
        *[f"- **{b}** `{a}` — {c}" for a, b, c in val_rows],
        "",
        "## Row counts (informational)",
    ]
    try:
        mc = con.execute("SELECT COUNT(*) FROM main.specimen_master_v1").fetchone()[0]
        fc = con.execute("SELECT COUNT(*) FROM main.specimen_tumor_focus_v1").fetchone()[0]
        xc = con.execute("SELECT COUNT(*) FROM main.specimen_source_xref_v1").fetchone()[0]
        qc = con.execute("SELECT COUNT(*) FROM qa.specimen_merge_review_queue_v1").fetchone()[0]
        report.extend(
            [
                f"- specimen_master_v1: {mc}",
                f"- specimen_tumor_focus_v1: {fc}",
                f"- specimen_source_xref_v1: {xc}",
                f"- specimen_merge_review_queue_v1: {qc}",
            ]
        )
    except Exception as e:
        report.append(f"(counts unavailable: {e})")

    (study_dir / "validation_report.md").write_text("\n".join(report), encoding="utf-8")
    con.close()
    print(f"Done. Artifacts: {study_dir}")


if __name__ == "__main__":
    main()
