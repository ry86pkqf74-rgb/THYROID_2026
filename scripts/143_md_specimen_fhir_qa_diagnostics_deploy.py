#!/usr/bin/env python3
"""Deploy qa.v_diag_* specimen/FHIR diagnostic views only (no full 138 rebuild).

Applies ``scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`` in full, including
focus-grain surfaces used as the sole authority for Check 13 when the specimen/FHIR
layer is complete: duplicate focus fingerprints, orphan focus→master, genomic→focus
orphans, provenance summary + ``v_diag_specimen_provenance_focus_gaps_v1``, broken
FHIR refs, **bundle entry.url drift** (``v_diag_specimen_fhir_bundle_entry_drift_v1``),
**genomics contract list views** (tier / Thyroseq slice / ordinality), review burden,
and ``qa.t_diag_specimen_focus_qa_metrics_v1``.

Uses fail-closed MotherDuck RW token; default ``custom_user_agent`` is
``specimen_fhir_ref_integrity_v2`` (override via ``MOTHERDUCK_CUSTOM_USER_AGENT``).
Session hint still comes from :func:`specimen_fhir_release_writer_attribution`.
Attempts CREATE SNAPSHOT before DDL when ``--md`` (skipped on DuckLake / unsupported — logged).

Usage:
  .venv/bin/python scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md
  .venv/bin/python scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_PATH = ROOT / "scripts" / "sql" / "142_specimen_fhir_qa_diagnostics_ddl.sql"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _prod_database_name() -> str:
    return (
        os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or "Thyroid 2026"
    ).strip()


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy specimen/FHIR QA diagnostic views.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB when not --md.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip CREATE SNAPSHOT preamble on --md.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ddl = DDL_PATH.read_text(encoding="utf-8")
    if args.md:
        from utils.md_connect import connect_md_or_file
        from utils.md_pipeline_attribution import specimen_fhir_release_writer_attribution

        _, hint = specimen_fhir_release_writer_attribution()
        ua = os.environ.get(
            "MOTHERDUCK_CUSTOM_USER_AGENT", "specimen_fhir_ref_integrity_v2"
        )
        con = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=True,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
        )
    else:
        import duckdb

        con = duckdb.connect(str(args.db_path))
        ua = "local"
    snap_line = "not_attempted"
    try:
        if args.md and not args.skip_snapshot:
            prod = _prod_database_name()
            snap_name = f"specimen_fhir_qa_diag_pre_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
            st, detail = try_named_snapshot(con, snapshot_name=snap_name, prod=prod)
            snap_line = f"{st}: {detail[:300]}"
            print(f"  Snapshot {snap_name}: {snap_line}")
        con.execute(ddl)
    finally:
        con.close()
    print(f"OK — applied {DDL_PATH.name} (UA={ua if args.md else 'local'})")


if __name__ == "__main__":
    main()
