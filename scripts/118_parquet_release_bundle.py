#!/usr/bin/env python3
"""Export a curated, partitioned Parquet release bundle from MotherDuck.

Reads from MotherDuck main and qa schemas (fail-closed) and writes a
directory-partitioned Parquet bundle suitable for Fabric/OneLake consumption.

Output structure:
  exports/parquet_release_YYYYMMDD/
    main/
      canonical_extracted_fact_long_v2.parquet
      canonical_fact_quarantine_v2.parquet
      note_extraction_runs.parquet
      note_entities_llm_<domain>.parquet  (22 files)
      tumor_episode_master_v2.parquet
      molecular_test_episode_v2.parquet
      rai_treatment_episode_v2.parquet
      operative_episode_detail_v2.parquet
    qa/
      promotion_scorecard.parquet
      domain_validation.parquet
      manual_review_queue.parquet
    manifest.json

Usage:
  .venv/bin/python scripts/118_parquet_release_bundle.py --md
  .venv/bin/python scripts/118_parquet_release_bundle.py --md --dry-run
  .venv/bin/python scripts/118_parquet_release_bundle.py --db-path thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry

DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
EXPORTS_DIR = ROOT / "exports"

MAIN_TABLES = [
    "canonical_extracted_fact_long_v2",
    "canonical_fact_quarantine_v2",
    "note_extraction_runs",
    "tumor_episode_master_v2",
    "molecular_test_episode_v2",
    "rai_treatment_episode_v2",
    "operative_episode_detail_v2",
]

QA_TABLES = [
    "promotion_scorecard",
    "domain_validation",
    "manual_review_queue",
]

# Curated bundle for manuscript stats (no raw note bodies; facts + lineage + labs).
FINAL_MASTER_MAIN = [
    "canonical_extracted_fact_long_v2",
    "canonical_fact_quarantine_v2",
    "note_extraction_runs",
    "longitudinal_lab_canonical_v1",
    "longitudinal_lab_deduped_v",
    "master_fact_long_verified_v1",
    "master_patient_rollup_verified_v1",
    "master_source_lineage_v1",
]

FINAL_MASTER_QA = [
    "promotion_scorecard",
    "domain_validation",
    "manual_review_queue",
    "promotion_review_decisions",
    "release_manifest",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export Parquet release bundle from MotherDuck.")
    p.add_argument("--md", action="store_true", help="Read from MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Local DuckDB path.")
    p.add_argument("--dry-run", action="store_true", help="Show what would be exported.")
    p.add_argument("--tag", default=None, help="Release tag (default: YYYYMMDD).")
    p.add_argument("--output-dir", default=None, help="Override output directory.")
    p.add_argument(
        "--final-master",
        action="store_true",
        help="Export FINAL_MASTER_MAIN + FINAL_MASTER_QA to exports/final_master_release_<tag>/",
    )
    return p.parse_args()


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file
        return connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    return duckdb.connect(args.db_path)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def export_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    out_dir: Path,
    dry_run: bool = False,
) -> dict | None:
    """Export a single table to Parquet. Returns metadata dict or None."""
    try:
        df = con.execute(f"SELECT * FROM {schema}.{table}").fetchdf()
    except Exception as exc:
        print(f"  [skip] {schema}.{table}: {exc}")
        return None

    out_path = out_dir / f"{table}.parquet"

    if dry_run:
        print(f"  [dry-run] {schema}.{table}: {len(df):,} rows -> {out_path.name}")
        return {"table": f"{schema}.{table}", "rows": len(df), "file": out_path.name}

    df.to_parquet(out_path, index=False)
    checksum = file_sha256(out_path)
    print(f"  [export] {schema}.{table}: {len(df):,} rows -> {out_path.name} ({checksum})")

    return {
        "table": f"{schema}.{table}",
        "rows": len(df),
        "file": out_path.name,
        "size_bytes": out_path.stat().st_size,
        "sha256_16": checksum,
    }


def main() -> None:
    args = parse_args()
    registry = load_registry()
    tag = args.tag or datetime.now().strftime("%Y%m%d")
    sha = git_sha()
    now = datetime.now(timezone.utc).isoformat()

    if args.output_dir:
        bundle_dir = Path(args.output_dir)
    elif args.final_master:
        bundle_dir = EXPORTS_DIR / f"final_master_release_{tag}"
    else:
        bundle_dir = EXPORTS_DIR / f"parquet_release_{tag}"

    main_dir = bundle_dir / "main"
    qa_dir = bundle_dir / "qa"

    if not args.dry_run:
        main_dir.mkdir(parents=True, exist_ok=True)
        qa_dir.mkdir(parents=True, exist_ok=True)

    con = get_connection(args)
    manifest_entries: list[dict] = []

    try:
        print(f"=== Parquet Release Bundle: {tag} ===")
        print(f"  Output: {bundle_dir}")
        if args.final_master:
            print("  Mode: final-master (curated facts + labs + lineage; no note text)")

        print("\n--- main schema ---")
        main_list = FINAL_MASTER_MAIN if args.final_master else MAIN_TABLES
        for table in main_list:
            entry = export_table(con, "main", table, main_dir, args.dry_run)
            if entry:
                manifest_entries.append(entry)

        if not args.final_master:
            v2_domains = {
                name: spec for name, spec in registry.v2_domains.items()
                if spec.canonical_output
            }
            for name, spec in v2_domains.items():
                entry = export_table(con, "main", spec.parquet_stem, main_dir, args.dry_run)
                if entry:
                    manifest_entries.append(entry)

        print("\n--- qa schema ---")
        qa_list = FINAL_MASTER_QA if args.final_master else QA_TABLES
        for table in qa_list:
            entry = export_table(con, "qa", table, qa_dir, args.dry_run)
            if entry:
                manifest_entries.append(entry)

    finally:
        con.close()

    manifest = {
        "release_tag": tag,
        "git_sha": sha,
        "registry_version": registry.schema_version,
        "created_at": now,
        "tables": manifest_entries,
        "total_files": len(manifest_entries),
        "total_rows": sum(e.get("rows", 0) for e in manifest_entries),
    }

    if not args.dry_run:
        manifest_path = bundle_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"\n  [manifest] {manifest_path}")

    print(f"\n  [summary] {len(manifest_entries)} files, "
          f"{manifest['total_rows']:,} total rows")
    print(f"  [done] Release bundle {'(dry-run) ' if args.dry_run else ''}created at {bundle_dir}")


if __name__ == "__main__":
    main()
