#!/usr/bin/env python3
"""121_backfill_provenance.py — Backfill provenance columns into v2 domain parquets.

The v2 fleet parquets contain per-note rows with result_json. Some downstream
consumers (the promotion gate, the contract validator) expect entity-level
provenance columns. This script reads each v2 parquet and ensures the three
contract provenance columns are present:

  - preprocess_batch_id (already present in fleet output)
  - preprocess_script_version (already present in fleet output)
  - preprocessed_at_utc (already present in fleet output)

Additionally, it ensures extraction_run_id and source_file_id are present
in the raw note-level parquets so that script 103 can propagate them during
entity expansion.

Usage:
  .venv/bin/python scripts/121_backfill_provenance.py
  .venv/bin/python scripts/121_backfill_provenance.py --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from utils.text_helpers import save_parquet  # noqa: E402

V2_DIR = ROOT / "processed" / "output" / "v2_parquets"

REQUIRED_PROVENANCE = [
    "preprocess_batch_id",
    "preprocess_script_version",
    "preprocessed_at_utc",
]

ADDITIONAL_PROVENANCE = [
    "extraction_run_id",
    "source_file_id",
]


def compute_extraction_run_id(df: pd.DataFrame) -> str:
    """Deterministic run ID from the parquet's extracted_at range and model."""
    earliest = ""
    if "extracted_at" in df.columns:
        vals = df["extracted_at"].dropna()
        if len(vals) > 0:
            earliest = str(vals.iloc[0])
    model = ""
    if "llm_model" in df.columns:
        vals = df["llm_model"].dropna()
        if len(vals) > 0:
            model = str(vals.iloc[0])
    key = f"{earliest}|{model}|{len(df)}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("  121 — backfill provenance columns in v2 parquets")
    print("=" * 70)

    if not V2_DIR.exists():
        print(f"  ERROR: {V2_DIR} not found")
        sys.exit(1)

    parquets = sorted(V2_DIR.glob("note_entities_llm_*.parquet"))
    combined = [p for p in V2_DIR.glob("*.parquet") if "combined" in p.name]
    parquets = [p for p in parquets if "combined" not in p.name]

    print(f"  found {len(parquets)} v2 domain parquets")
    ts = datetime.now(timezone.utc).isoformat()
    modified = 0

    for pq in parquets:
        df = pd.read_parquet(pq)
        changed = False

        for col in REQUIRED_PROVENANCE:
            if col not in df.columns:
                if col == "preprocess_batch_id":
                    df[col] = "backfill_121"
                elif col == "preprocess_script_version":
                    df[col] = "v2_fleet_backfill"
                elif col == "preprocessed_at_utc":
                    df[col] = ts
                changed = True
                print(f"  {pq.name}: added {col}")

        if "extraction_run_id" not in df.columns:
            df["extraction_run_id"] = compute_extraction_run_id(df)
            changed = True
            print(f"  {pq.name}: added extraction_run_id")

        if "source_file_id" not in df.columns:
            if "source_workbook" in df.columns:
                df["source_file_id"] = df["source_workbook"].apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest()[:12]
                    if pd.notna(x) else None
                )
            else:
                df["source_file_id"] = None
            changed = True
            print(f"  {pq.name}: added source_file_id")

        if changed:
            modified += 1
            if not args.dry_run:
                save_parquet(df, pq)

    print(f"\n  modified: {modified} / {len(parquets)} parquets")
    if args.dry_run:
        print("  dry-run: no files changed")

    print("=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
