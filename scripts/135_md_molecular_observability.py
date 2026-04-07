#!/usr/bin/env python3
"""MotherDuck observability + molecular pipeline governance for THYROID_2026.

Subcommands
-----------
  diagnostics      — Print databases, snapshots, query history samples (requires MD admin views).
  recent-molecular — Filter QUERY_HISTORY for molecular / ingest / lineage-related SQL.
  init-audit       — Create qa.molecular_pipeline_run_audit (idempotent DDL).
  record-run       — Append markdown audit log + optional INSERT into qa.molecular_pipeline_run_audit.

Usage
-----
  .venv/bin/python scripts/135_md_molecular_observability.py diagnostics --md
  .venv/bin/python scripts/135_md_molecular_observability.py recent-molecular --md --md-env prod
  .venv/bin/python scripts/135_md_molecular_observability.py init-audit --md
  .venv/bin/python scripts/135_md_molecular_observability.py record-run --md \\
      --pipeline 42_ingest_afirma --git-sha abc1234 --rows-json '{\"molecular_results\":120}' \\
      --validation pass --runtime 42.5

Credentials: same as other scripts (MOTHERDUCK_TOKEN / MD_SA_TOKEN / .streamlit/secrets.toml).
Never prints token values.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "thyroid_master.duckdb"

from motherduck_client import (  # noqa: E402
    read_scaling_token_mode,
    token_mode,
)
from utils.md_observability import (  # noqa: E402
    append_audit_markdown,
    apply_audit_ddl,
    format_console_block,
    insert_audit_row,
    run_diagnostics,
    run_recent_molecular,
)
from utils.md_pipeline_attribution import (  # noqa: E402
    connect_attribution,
    git_sha_short,
)


def _connect_md(args: argparse.Namespace):
    from utils.md_connect import connect_md_or_file

    if args.md_env and not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get(
        "MOTHERDUCK_DB"
    ):
        from motherduck_client import resolve_database_for_env

        os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(args.md_env)

    ua, hint = connect_attribution(
        component="135_md_molecular_observability",
        run_kind="validate",
    )
    return connect_md_or_file(
        DB_PATH,
        md=True,
        fail_closed=True,
        env=args.md_env,
        prefer_service_account=bool(getattr(args, "md_sa", False)),
        custom_user_agent=ua,
        motherduck_session_hint=hint,
    )


def cmd_diagnostics(args: argparse.Namespace) -> None:
    print("Token source (read/write):", token_mode())
    print("Read-scaling token source:", read_scaling_token_mode())
    t0 = time.perf_counter()
    con = _connect_md(args)
    try:
        print(f"\nDiagnostics (connected in {time.perf_counter() - t0:.2f}s)\n")
        for res in run_diagnostics(con):
            print(
                format_console_block(
                    res.name,
                    res.dataframe,
                    res.error,
                )
            )
    finally:
        con.close()


def cmd_recent_molecular(args: argparse.Namespace) -> None:
    con = _connect_md(args)
    try:
        res = run_recent_molecular(con)
        print(
            format_console_block(
                res.name,
                res.dataframe,
                res.error,
            )
        )
    finally:
        con.close()


def cmd_init_audit(args: argparse.Namespace) -> None:
    con = _connect_md(args)
    try:
        apply_audit_ddl(con)
        print("  OK: qa.molecular_pipeline_run_audit ready (DDL applied).")
    finally:
        con.close()


def cmd_record_run(args: argparse.Namespace) -> None:
    row_counts = json.loads(args.rows_json)
    git_sha = args.git_sha or git_sha_short(ROOT)
    db_name = args.database or ""
    schema = (args.schema or "main").strip() or "main"
    if not db_name.strip():
        con0 = _connect_md(args)
        try:
            row = con0.execute("SELECT current_database()").fetchone()
            db_name = row[0] if row else "unknown"
        finally:
            con0.close()

    ua, hint = connect_attribution(component=args.pipeline, run_kind=args.run_kind)

    audit_id: str | None = None
    if args.persist_db:
        con = _connect_md(args)
        try:
            apply_audit_ddl(con)
            audit_id = insert_audit_row(
                con,
                pipeline_name=args.pipeline,
                git_sha=git_sha,
                database_name=db_name,
                schema_name=schema,
                row_counts=row_counts,
                runtime_seconds=args.runtime,
                validation_status=args.validation,
                custom_user_agent=ua,
                session_hint=hint,
                notes=args.notes,
            )
            print(f"  Inserted audit_id={audit_id}")
        finally:
            con.close()

    log_path = append_audit_markdown(
        ROOT,
        pipeline_name=args.pipeline,
        git_sha=git_sha,
        database_name=db_name,
        schema_name=schema,
        row_counts=row_counts,
        runtime_seconds=args.runtime,
        validation_status=args.validation,
        audit_id=audit_id,
    )
    print(f"  Markdown audit: {log_path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--md", action="store_true", help="Connect to MotherDuck (required).")
    common.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    common.add_argument("--md-env", default=None, help="dev|qa|prod catalog selector.")

    s1 = sub.add_parser("diagnostics", parents=[common], help="Run MD information_schema suite.")
    s1.set_defaults(func=cmd_diagnostics)

    s2 = sub.add_parser(
        "recent-molecular",
        parents=[common],
        help="Recent molecular-related QUERY_HISTORY rows.",
    )
    s2.set_defaults(func=cmd_recent_molecular)

    s3 = sub.add_parser("init-audit", parents=[common], help="Apply qa.molecular_pipeline_run_audit DDL.")
    s3.set_defaults(func=cmd_init_audit)

    s4 = sub.add_parser("record-run", parents=[common], help="Record a run to markdown + optional DB.")
    s4.add_argument("--pipeline", required=True, help="Pipeline name, e.g. 42_ingest_afirma")
    s4.add_argument("--git-sha", default=None, help="Override git SHA (default: git rev-parse).")
    s4.add_argument("--database", default=None, help="Catalog name (default: current_database()).")
    s4.add_argument("--schema", default="main", help="Primary schema touched.")
    s4.add_argument("--rows-json", required=True, help='JSON object of table counts, e.g. {"molecular_results":1}')
    s4.add_argument("--runtime", type=float, default=None, help="Wall runtime seconds.")
    s4.add_argument(
        "--validation",
        required=True,
        choices=("pass", "fail", "warn", "skipped"),
        help="Validation outcome label.",
    )
    s4.add_argument(
        "--run-kind",
        default="ingest",
        help="Attribution kind segment (ingest|release|validate|…); stored only in user-agent helper context.",
    )
    s4.add_argument("--persist-db", action="store_true", help="Also INSERT into qa.molecular_pipeline_run_audit.")
    s4.add_argument("--notes", default=None, help="Optional free-text notes (no secrets).")
    s4.set_defaults(func=cmd_record_run)

    return p


def main() -> None:
    args = build_parser().parse_args()
    if not args.md:
        print("  FATAL: these commands require --md (MotherDuck).", file=sys.stderr)
        sys.exit(2)
    args.func(args)


if __name__ == "__main__":
    main()
