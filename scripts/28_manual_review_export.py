#!/usr/bin/env python3
"""
28_manual_review_export.py -- Export high-priority manual review tables

Exports all five manual review queues plus the unresolved high-value
cases view to CSV and Parquet for offline adjudication. Produces a
manifest with row counts and generation timestamp.

Run after scripts 15-20 (adjudication views must exist).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORT_DIR = ROOT / "exports"

REVIEW_QUEUES = [
    ("histology_manual_review",     "histology_manual_review_queue_v"),
    ("molecular_manual_review",     "molecular_manual_review_queue_v"),
    ("rai_manual_review",           "rai_manual_review_queue_v"),
    ("timeline_manual_review",      "timeline_manual_review_queue_v"),
    ("unresolved_high_value_cases", "unresolved_high_value_cases_v"),
    ("patient_manual_review_summary", "patient_manual_review_summary_v"),
]


def tbl_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(ROOT),
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--priority-min", type=int, default=0,
        help="Only export rows with priority_score >= this value (default: 0 = all)",
    )
    parser.add_argument(
        "--md", action="store_true",
        help="Read from MotherDuck instead of local DuckDB",
    )
    args = parser.parse_args()

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    out_dir = EXPORT_DIR / f"manual_review_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.md:
        try:
            sys.path.insert(0, str(ROOT))
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("Source: MotherDuck (RW)")
        except Exception as e:
            print(f"MotherDuck unavailable: {e}")
            print("Falling back to local DuckDB")
            con = duckdb.connect(str(DB_PATH), read_only=True)
            print(f"Source: {DB_PATH}")
    else:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        print(f"Source: {DB_PATH}")
    print(f"Output: {out_dir}")
    if args.priority_min > 0:
        print(f"Filter: priority_score >= {args.priority_min}")

    manifest: dict = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "repo_commit_sha": git_sha(),
        "source_scripts": ["15", "16", "17", "18", "19"],
        "export_directory": str(out_dir),
        "priority_min_filter": args.priority_min,
        "queues": {},
    }

    for prefix, view_name in REVIEW_QUEUES:
        if not tbl_exists(con, view_name):
            print(f"  SKIP {view_name:<50} (not found)")
            manifest["queues"][prefix] = {"rows": 0, "status": "missing"}
            continue

        sql = f"SELECT * FROM {view_name}"
        if args.priority_min > 0 and prefix != "patient_manual_review_summary":
            sql += f" WHERE priority_score >= {args.priority_min}"
        sql += " ORDER BY research_id"

        try:
            df: pd.DataFrame = con.execute(sql).fetchdf()
        except Exception as e:
            print(f"  ERR  {view_name:<50} {e}")
            manifest["queues"][prefix] = {"rows": 0, "status": f"error: {e}"}
            continue

        csv_path = out_dir / f"{prefix}.csv"
        pq_path = out_dir / f"{prefix}.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)

        manifest["queues"][prefix] = {
            "rows": len(df),
            "columns": list(df.columns),
            "status": "ok",
        }
        print(f"  OK   {prefix:<50} {len(df):>8,} rows")

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(f"\n  Manifest: {manifest_path}")

    con.close()
    print("  Done.\n")


if __name__ == "__main__":
    main()
