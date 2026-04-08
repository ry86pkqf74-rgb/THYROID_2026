#!/usr/bin/env python3
"""
150_ingest_dicom_headers.py — Flattened DICOM header export ingest (CSV/XLSX/JSON/Parquet).

Additive layer: does not modify imaging_nodule_master_v1, script 128, or 129 outputs.
Optional ``optional_attach_dicom_to_imaging_nodule_frame`` in utils/dicom_header_helpers.py
is a no-op unless ``dicom_study_header_v1`` exists.

Linkage (deterministic):
  1) Explicit ``research_id`` column in source (see config/dicom_header_aliases.yml)
  2) Else exact normalized accession match against optional candidate spine (no fuzzy / no MRN+date)

Default: export-only to ``exports/dicom_header_ingest_<ts>/`` — no database writes.
Database DDL / inserts require ``--write-db`` (operator confirmation on prod environments).

Candidate spine (read-only UNION): see ``fetch_linkage_candidates_union`` in utils/dicom_header_helpers.py

Usage:
  .venv/bin/python scripts/150_ingest_dicom_headers.py \\
      --input tests/fixtures/dicom_headers/study_series_synthetic.csv

  .venv/bin/python scripts/150_ingest_dicom_headers.py --input ./headers.csv \\
      --md --link-candidates-from-db

  .venv/bin/python scripts/150_ingest_dicom_headers.py --input ./headers.csv \\
      --db-path ./thyroid_master.duckdb --link-candidates-from-db --write-db
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = ROOT / "scripts" / "sql" / "150_dicom_header_layer_ddl.sql"

sys.path.insert(0, str(ROOT))

from utils.dicom_header_helpers import (  # noqa: E402
    build_column_lookup,
    build_enriched_rows,
    fetch_linkage_candidates_union,
    load_alias_config,
    read_input_files,
    resolve_exact_links,
    rows_to_study_series,
)

LOCAL_DB_DEFAULT = ROOT / os.getenv("LOCAL_DUCKDB_PATH", "thyroid_master.duckdb")


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _strip_full_line_comments(ddl: str) -> str:
    lines = [ln for ln in ddl.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(lines)


def _split_sql(sql_text: str) -> list[str]:
    parts: list[str] = []
    for stmt in sql_text.split(";"):
        s = stmt.strip()
        if s:
            parts.append(s)
    return parts


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file

        pref_sa = bool(getattr(args, "md_sa", False))
        override_db = (args.database or "").strip()
        if override_db:
            os.environ["MOTHERDUCK_DATABASE"] = override_db
        elif not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get("MOTHERDUCK_DB"):
            env_name = (args.md_env or os.getenv("MOTHERDUCK_ENV") or "prod").strip()
            from motherduck_client import resolve_database_for_env

            os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(env_name)
        return connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=pref_sa,
            custom_user_agent=os.getenv(
                "MOTHERDUCK_CUSTOM_USER_AGENT",
                "THYROID_2026_scripts/150_ingest_dicom_headers",
            ),
        )
    return duckdb.connect(str(Path(args.db_path)))


def merge_table(
    con: duckdb.DuckDBPyConnection,
    name: str,
    new_df: pd.DataFrame,
    dedup_subset: list[str],
    *,
    reg_name: str,
) -> None:
    try:
        old = con.execute(f"SELECT * FROM {name}").df()
    except Exception:
        old = pd.DataFrame()
    if old.empty:
        merged = new_df
    elif new_df.empty:
        merged = old
    else:
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=dedup_subset, keep="last")
    con.register(reg_name, merged)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM {reg_name}")


def apply_ddl(con: duckdb.DuckDBPyConnection, *, dry_run: bool) -> None:
    if not DDL_PATH.is_file():
        raise SystemExit(f"DDL missing: {DDL_PATH}")
    body = _strip_full_line_comments(DDL_PATH.read_text(encoding="utf-8"))
    stmts = _split_sql(body)
    if dry_run:
        print(f"  [dry-run] {len(stmts)} DDL statement(s) from {DDL_PATH.name}")
        return
    for st in stmts:
        con.execute(st)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", action="append", required=True, help="Input file path (repeatable).")
    p.add_argument(
        "--format",
        default="auto",
        choices=("auto", "csv", "xlsx", "json", "parquet"),
        help="Input format (default: infer from extension).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Export directory (default: exports/dicom_header_ingest_<utc_ts>).",
    )
    p.add_argument(
        "--alias-yml",
        default=None,
        help="Override path to dicom_header_aliases.yml",
    )
    p.add_argument(
        "--ingestion-run-id",
        default=None,
        help="Stable run id (default: random uuid fragment).",
    )
    p.add_argument(
        "--link-candidates-from-db",
        action="store_true",
        help="Pull imaging/specimen/FNA accession candidates via read-only SQL (--md or --db-path).",
    )
    p.add_argument(
        "--candidate-parquet",
        default=None,
        help="Optional path to a candidate-frame parquet (columns per fetch_linkage_candidates_union).",
    )
    p.add_argument(
        "--date-skew-days-max",
        type=int,
        default=14,
        help="Max |study_date - exam_date| days for exact_accession approval when both dates exist.",
    )
    p.add_argument("--md", action="store_true", help="Use MotherDuck for --link-candidates-from-db / --write-db.")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN when using --md.")
    p.add_argument("--md-env", default=None, help="MOTHERDUCK_ENV selector (dev|qa|prod).")
    p.add_argument("--database", default=None, help="Override MotherDuck database name.")
    p.add_argument("--db-path", default=str(LOCAL_DB_DEFAULT), help="Local DuckDB path when not --md.")
    p.add_argument(
        "--write-db",
        action="store_true",
        help="Apply DDL (if needed) and merge into dicom_*_v1 tables (operator confirmation in prod).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_id = (args.ingestion_run_id or "").strip() or str(uuid.uuid4())[:26]
    ts = datetime.now(timezone.utc)
    ts_tag = ts.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir) if args.output_dir else ROOT / "exports" / f"dicom_header_ingest_{ts_tag}"
    out_dir.mkdir(parents=True, exist_ok=True)

    yml = Path(args.alias_yml) if args.alias_yml else None
    cfg = load_alias_config(yml)
    col_lookup = build_column_lookup(cfg["canonical_fields"])

    paths = [Path(x) for x in args.input]
    raw_df = read_input_files(paths, args.format)
    enriched = build_enriched_rows(raw_df, col_lookup)
    prov_df, study_df, series_df = rows_to_study_series(enriched, ingestion_run_id=run_id, ingestion_ts=ts)

    candidates: pd.DataFrame | None = None
    if args.candidate_parquet:
        candidates = pd.read_parquet(args.candidate_parquet)
    con: duckdb.DuckDBPyConnection | None = None
    if args.link_candidates_from_db or args.write_db:
        con = connect(args)
    try:
        if args.link_candidates_from_db and con is not None:
            candidates = fetch_linkage_candidates_union(con)

        links_df, review_df = resolve_exact_links(
            study_df,
            candidates,
            ingestion_run_id=run_id,
            date_skew_days_max=args.date_skew_days_max,
            ingestion_ts=ts,
        )

        prov_path = out_dir / "dicom_header_ingestion_provenance_v1.parquet"
        study_path = out_dir / "dicom_study_header_v1.parquet"
        series_path = out_dir / "dicom_series_header_v1.parquet"
        link_path = out_dir / "dicom_imaging_link_exact_v1.parquet"
        rev_path = out_dir / "dicom_link_review_queue_v1.parquet"
        prov_df.to_parquet(prov_path, index=False)
        study_df.to_parquet(study_path, index=False)
        series_df.to_parquet(series_path, index=False)
        links_df.to_parquet(link_path, index=False)
        review_df.to_parquet(rev_path, index=False)

        manifest = {
            "ingestion_run_id": run_id,
            "ingestion_ts_utc": ts.isoformat(),
            "git_sha": _git_sha(),
            "inputs": [str(p.resolve()) for p in paths],
            "row_counts": {
                "provenance": int(len(prov_df)),
                "study": int(len(study_df)),
                "series": int(len(series_df)),
                "link_exact": int(len(links_df)),
                "review_queue": int(len(review_df)),
            },
            "link_candidates_from_db": bool(args.link_candidates_from_db),
            "candidate_rows": int(len(candidates)) if candidates is not None else 0,
            "write_db": bool(args.write_db),
        }
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        if args.write_db and con is not None:
            apply_ddl(con, dry_run=False)
            merge_table(
                con,
                "dicom_header_ingestion_provenance_v1",
                prov_df,
                ["row_fingerprint_sha256", "ingestion_run_id"],
                reg_name="_mrgp",
            )
            merge_table(con, "dicom_study_header_v1", study_df, ["study_instance_uid"], reg_name="_mrgs")
            merge_table(con, "dicom_series_header_v1", series_df, ["series_instance_uid"], reg_name="_mrge")
            merge_table(con, "dicom_imaging_link_exact_v1", links_df, ["link_id"], reg_name="_mrgl")
            merge_table(con, "dicom_link_review_queue_v1", review_df, ["review_id"], reg_name="_mrgr")
            print(f"  Wrote merged tables to database (run_id={run_id}).")
        elif args.write_db and con is None:
            raise SystemExit("--write-db requires a connection (--md or local --db-path).")
        print(f"  Exports: {out_dir}")
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()
