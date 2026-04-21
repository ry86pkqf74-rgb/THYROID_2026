"""Step 3 - Archive 13 legacy molecular tables to Parquet.

Strategy: pull each table to a pandas DataFrame, then COPY to Parquet via a
local in-memory DuckDB. The molecular_results table stores `raw_payload_json`
as JSON; we coerce JSON-typed columns to VARCHAR before export so the local
DuckDB can serialize them.
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)

import duckdb
from motherduck_client import get_token  # type: ignore

ARCHIVE_DIR = "/Users/ros/THyroid 2026/archive/molecular_legacy_20260421"

LEGACY = [
    "genetics_per_patient_master_v1",
    "molecular_results",
    "thyroseq_molecular_enrichment",
    "molecular_testing",
    "molecular_test_episode_v2",
    "analysis_molecular_subset_v1",
    "genetics_per_test_master_v1",
    "note_entities_genetics",
    "molecular_variant_long",
    "canonical_molecular_tested_v1",
    "molecular_code_crosswalk",
    "molecular_assay_dictionary",
    "molecular_ingestion_runs",
]


def main() -> None:
    os.environ["motherduck_token"] = get_token() or ""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    md = duckdb.connect("md:thyroid_canonical_publication_v1_0")

    manifest = {
        "archived_on": "2026-04-21",
        "source_database": "thyroid_canonical_publication_v1_0",
        "archive_dir": ARCHIVE_DIR,
        "tables": [],
    }

    for tbl in LEGACY:
        cols = md.execute(f"DESCRIBE {tbl}").fetchdf()
        select_parts = []
        for _, row in cols.iterrows():
            col = row["column_name"]
            ctype = row["column_type"].upper()
            if "JSON" in ctype:
                select_parts.append(f'CAST("{col}" AS VARCHAR) AS "{col}"')
            else:
                select_parts.append(f'"{col}"')
        select_sql = ", ".join(select_parts)

        df = md.execute(f"SELECT {select_sql} FROM {tbl}").fetchdf()
        path = os.path.join(ARCHIVE_DIR, f"{tbl}.parquet")

        local = duckdb.connect(":memory:")
        local.register("t", df)
        local.execute(f"COPY t TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        local.close()

        n = len(df)
        size_mb = round(os.path.getsize(path) / (1024 * 1024), 3)
        manifest["tables"].append({"name": tbl, "rows": int(n), "parquet_mb": size_mb})
        print(f"  {tbl:40s} rows={n:>9,} -> {size_mb} MB")

    manifest_path = os.path.join(ARCHIVE_DIR, "MANIFEST.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nmanifest -> {manifest_path}")

    print("\n--- verifying parquet round-trip row counts ---")
    local = duckdb.connect(":memory:")
    for tbl in LEGACY:
        path = os.path.join(ARCHIVE_DIR, f"{tbl}.parquet")
        n = local.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        original = next(t["rows"] for t in manifest["tables"] if t["name"] == tbl)
        ok = "OK" if n == original else "MISMATCH"
        print(f"  {tbl:40s} parquet={n:>9,} original={original:>9,} {ok}")


if __name__ == "__main__":
    main()
