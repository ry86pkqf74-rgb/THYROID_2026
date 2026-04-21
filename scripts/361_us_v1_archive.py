#!/usr/bin/env python3
"""Script 361 — Archive US/TIRADS v1 tables to "Thyroid 2026 UPdated".us_legacy_20260421.

Phase 1 of US v2 consolidation (cursor prompt 2026-04-21).

Non-destructive:
  - CREATE SCHEMA IF NOT EXISTS in archive DB.
  - CREATE TABLE AS SELECT * for each v1 source.
  - DOES NOT drop any source table. CPM continues to read from main.* until
    the v2 cutover and a separate explicit drop script.

Mirrors the molecular_legacy_20260421 archive precedent
(/Users/ros/THyroid 2026/archive/molecular_legacy_20260421/).
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "us_legacy_20260421"
SCRIPT_TAG = "Script 361"

MAIN_TABLES = [
    "canonical_us_nodule_master_v1",
    "canonical_us_nodule_characteristics_v1",
    "imaging_nodule_master_v1",
    "tirads_llm_extracted_v2",
    "serial_imaging_us",
    "canonical_us_exam_master_v1",
    "canonical_us_patient_master_v1",
]

WS_TABLES = [
    "tirads_granular_parsed_v1",
    "us_nodule_dynamics_parsed_v1",
    # imaging_nodule_master_clean_v1 does not exist in workspace — verified by probe.
]

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"361_archive_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def table_exists(con, db: str, schema: str, table: str) -> bool:
    rows = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [db, schema, table],
    ).fetchone()
    return bool(rows and rows[0] > 0)


def archive_one(con, src_schema: str, src_table: str, *, commit: bool) -> dict:
    fq_src = f'{PUBLICATION_DB}.{src_schema}."{src_table}"'
    fq_dst = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{src_table}"'

    if not table_exists(con, PUBLICATION_DB, src_schema, src_table):
        log(f"  SKIP {src_schema}.{src_table} (not found)")
        return {"table": src_table, "status": "missing"}

    src_n = con.execute(f"SELECT COUNT(*) FROM {fq_src}").fetchone()[0]

    dst_exists = table_exists(con, ARCHIVE_DB, ARCHIVE_SCHEMA, src_table)
    if dst_exists:
        dst_n = con.execute(f"SELECT COUNT(*) FROM {fq_dst}").fetchone()[0]
        log(f"  EXISTS {src_table}: src={src_n} dst={dst_n} (no-op)")
        return {
            "table": src_table,
            "status": "exists",
            "src_rows": src_n,
            "dst_rows": dst_n,
        }

    if not commit:
        log(f"  DRY  {src_table}: src={src_n} would CTAS to archive")
        return {"table": src_table, "status": "dry", "src_rows": src_n}

    con.execute(f"CREATE TABLE {fq_dst} AS SELECT * FROM {fq_src}")
    dst_n = con.execute(f"SELECT COUNT(*) FROM {fq_dst}").fetchone()[0]
    if dst_n != src_n:
        raise SystemExit(
            f"COUNT MISMATCH for {src_table}: src={src_n} dst={dst_n}"
        )
    log(f"  OK   {src_table}: archived {src_n} rows")
    return {
        "table": src_table,
        "status": "archived",
        "src_rows": src_n,
        "dst_rows": dst_n,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--commit",
        action="store_true",
        help="Apply CREATE SCHEMA / CREATE TABLE statements. "
             "Default is dry-run (probe only).",
    )
    args = ap.parse_args()

    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    log(f"  ensure schema exists: \"{ARCHIVE_DB}\".\"{ARCHIVE_SCHEMA}\"")
    if args.commit:
        con.execute(
            f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
        )

    results: list[dict] = []
    for t in MAIN_TABLES:
        results.append(archive_one(con, "main", t, commit=args.commit))
    for t in WS_TABLES:
        results.append(
            archive_one(con, "manuscript_workspace", t, commit=args.commit)
        )

    summary = {
        "script": SCRIPT_TAG,
        "run_ts_utc": RUN_TS,
        "commit": args.commit,
        "archive_db": ARCHIVE_DB,
        "archive_schema": ARCHIVE_SCHEMA,
        "results": results,
    }
    DECISION_LOG.write_text(json.dumps(summary, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")

    archived = sum(1 for r in results if r["status"] in ("archived", "exists"))
    skipped = sum(1 for r in results if r["status"] == "missing")
    dryd = sum(1 for r in results if r["status"] == "dry")
    log(f"summary: archived/exists={archived}  dry={dryd}  missing={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
