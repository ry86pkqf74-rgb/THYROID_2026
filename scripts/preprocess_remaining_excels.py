#!/usr/bin/env python3
"""
preprocess_remaining_excels.py — Unpivot “remaining” clinical-note workbooks

Same processing model as build_clinical_notes_long.py (column map + long unpivot),
but reads every *.xlsx under a configurable input directory and writes outputs to:

  processed/remaining/clinical_notes_long.parquet
  processed/remaining/clinical_notes_long.csv
  processed/remaining/clinical_notes_long_qa.csv

Provenance columns (added to each row):
  source_workbook, preprocess_batch_id, preprocessed_at_utc, preprocess_script_version

Usage (from repo root):
  python scripts/preprocess_remaining_excels.py
  python scripts/preprocess_remaining_excels.py --input-dir raw/remaining
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.text_helpers import save_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("preprocess_remaining")

PROCESSED_REMAINING = ROOT / "processed" / "remaining"
DEFAULT_INPUT_DIR = ROOT / "raw" / "remaining"
CONFIG_PATH = ROOT / "config" / "notes_column_map.csv"
SCRIPT_VERSION = "preprocess_remaining_excels_v1"


def _load_build_clinical_notes_long():
    """Load build_clinical_notes_long as a module without package imports."""
    path = ROOT / "scripts" / "build_clinical_notes_long.py"
    spec = importlib.util.spec_from_file_location("_build_clinical_notes_long", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module spec from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def write_qa_report(df: pd.DataFrame, path: Path) -> None:
    """Row counts by note_type / source (same shape as build_clinical_notes_long)."""
    qa = (
        df.groupby(["note_type", "source_sheet", "source_column"])
        .agg(
            row_count=("note_row_id", "count"),
            unique_patients=("research_id", "nunique"),
            avg_char_count=("char_count", "mean"),
            max_char_count=("char_count", "max"),
            pct_with_date=("note_date", lambda s: round(100 * s.notna().mean(), 1)),
        )
        .reset_index()
    )
    qa["avg_char_count"] = qa["avg_char_count"].round(0).astype(int)
    qa.to_csv(path, index=False)
    log.info(f"  QA report: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preprocess remaining Excel workbooks into processed/remaining/"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing .xlsx files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--column-map",
        type=Path,
        default=CONFIG_PATH,
        help="notes_column_map.csv path",
    )
    args = parser.parse_args()

    input_dir: Path = args.input_dir.resolve()
    col_map_path: Path = args.column_map.resolve()

    log.info("=" * 70)
    log.info("  PREPROCESS REMAINING EXCELS → processed/remaining/")
    log.info("=" * 70)

    if not col_map_path.exists():
        log.error(f"Column map not found: {col_map_path}")
        sys.exit(1)
    if not input_dir.is_dir():
        log.error(f"Input directory not found: {input_dir}")
        sys.exit(1)

    try:
        bnl = _load_build_clinical_notes_long()
        load_column_map = bnl.load_column_map
        build_long = bnl.build_long
    except Exception as exc:
        log.error(f"Failed to load build_clinical_notes_long helpers: {exc}")
        sys.exit(1)

    col_map = load_column_map(col_map_path)
    log.info(
        f"  Column map: {len(col_map)} entries, "
        f"{int(col_map['is_note_like'].sum())} note-like"
    )

    xlsx_files = sorted(input_dir.glob("*.xlsx"))
    if not xlsx_files:
        log.error(f"No .xlsx files under {input_dir}")
        sys.exit(1)

    batch_id = str(uuid.uuid4())
    preprocessed_at = datetime.now(timezone.utc).isoformat()

    parts: list[pd.DataFrame] = []
    for xlsx in xlsx_files:
        log.info(f"  Workbook: {xlsx.name}")
        try:
            df = build_long(xlsx, col_map)
        except Exception as exc:
            log.error(f"    Failed to build long table for {xlsx.name}: {exc}")
            continue
        if df.empty:
            log.warning(f"    No rows from {xlsx.name} — skipping")
            continue
        df = df.copy()
        df["source_workbook"] = xlsx.name
        df["preprocess_batch_id"] = batch_id
        df["preprocessed_at_utc"] = preprocessed_at
        df["preprocess_script_version"] = SCRIPT_VERSION
        parts.append(df)

    if not parts:
        log.error("No data produced from any workbook.")
        sys.exit(1)

    result = pd.concat(parts, ignore_index=True)
    log.info(f"  Combined rows: {len(result):,}")
    log.info(f"  Unique patients: {result['research_id'].nunique():,}")
    log.info(f"  preprocess_batch_id: {batch_id}")

    PROCESSED_REMAINING.mkdir(parents=True, exist_ok=True)

    out_parquet = PROCESSED_REMAINING / "clinical_notes_long.parquet"
    try:
        save_parquet(result, out_parquet)
    except Exception as exc:
        log.error(f"Failed to write Parquet: {exc}")
        sys.exit(1)

    out_csv = PROCESSED_REMAINING / "clinical_notes_long.csv"
    try:
        result.to_csv(out_csv, index=False)
        log.info(f"  CSV export: {out_csv}")
    except Exception as exc:
        log.error(f"Failed to write CSV: {exc}")
        sys.exit(1)

    write_qa_report(result, PROCESSED_REMAINING / "clinical_notes_long_qa.csv")

    log.info("=" * 70)
    log.info("  DONE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
