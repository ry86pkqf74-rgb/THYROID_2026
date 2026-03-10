#!/usr/bin/env python3
"""
build_clinical_notes_long.py — Unpivot clinical notes to long format

Reads config/notes_column_map.csv and raw/Notes 12_1_25.xlsx, producing:
  processed/clinical_notes_long.parquet
  processed/clinical_notes_long.csv       (optional flat export)
  processed/clinical_notes_long_qa.csv    (row counts by note_type)
"""

from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.text_helpers import (
    standardize_columns,
    clean_research_id,
    strip_phi,
    make_note_row_id,
    save_parquet,
    extract_note_date,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_notes_long")

RAW_PATH = ROOT / "raw" / "Notes 12_1_25.xlsx"
CONFIG_PATH = ROOT / "config" / "notes_column_map.csv"
PROCESSED = ROOT / "processed"
PROCESSED.mkdir(exist_ok=True)


def load_column_map(path: Path) -> pd.DataFrame:
    """Load the canonical notes column mapping."""
    df = pd.read_csv(path)
    required = {"sheet", "source_column_snake", "is_note_like", "proposed_note_type", "proposed_note_index"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Column map missing columns: {missing}")
    return df


def build_long(raw_path: Path, col_map: pd.DataFrame) -> pd.DataFrame:
    """Unpivot notes from wide Excel sheets into one long DataFrame."""
    xl = pd.ExcelFile(raw_path, engine="openpyxl")
    note_rows = col_map[col_map["is_note_like"] == True]

    sheets_needed = note_rows["sheet"].unique()
    all_records: list[dict] = []

    for sheet_name in sheets_needed:
        if sheet_name not in xl.sheet_names:
            log.warning(f"  Sheet '{sheet_name}' not in workbook — skipping")
            continue

        log.info(f"  Loading sheet: {sheet_name}")
        df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
        df = standardize_columns(df)
        df = clean_research_id(df)
        df = strip_phi(df)

        sheet_notes = note_rows[note_rows["sheet"] == sheet_name]

        for _, mapping_row in sheet_notes.iterrows():
            snake_col = mapping_row["source_column_snake"]
            note_type = mapping_row["proposed_note_type"]
            note_index = mapping_row["proposed_note_index"]

            if snake_col not in df.columns:
                log.warning(f"    Column '{snake_col}' not found in sheet '{sheet_name}' — skipping")
                continue

            if pd.isna(note_type) or str(note_type).strip() == "":
                continue

            note_index = int(note_index) if pd.notna(note_index) and str(note_index).strip() else 1

            for _, row in df.iterrows():
                rid = row.get("research_id")
                text = row.get(snake_col)

                if pd.isna(rid):
                    continue
                if pd.isna(text) or str(text).strip() == "":
                    continue

                text_str = str(text).strip()
                all_records.append({
                    "note_row_id": make_note_row_id(rid, sheet_name, snake_col),
                    "research_id": int(rid),
                    "note_type": str(note_type),
                    "note_index": int(note_index),
                    "note_date": extract_note_date(text_str),
                    "note_text": text_str,
                    "source_sheet": sheet_name,
                    "source_column": snake_col,
                    "char_count": len(text_str),
                })

    if not all_records:
        log.warning("  No note records produced!")
        return pd.DataFrame(columns=[
            "note_row_id", "research_id", "note_type", "note_index",
            "note_date", "note_text", "source_sheet", "source_column", "char_count",
        ])

    result = pd.DataFrame(all_records)
    n_dated = result["note_date"].notna().sum()
    log.info(f"  Total note rows: {len(result):,}")
    log.info(f"  Unique patients: {result['research_id'].nunique():,}")
    log.info(f"  Note types: {sorted(result['note_type'].unique())}")
    log.info(f"  Notes with date: {n_dated:,} ({100*n_dated/len(result):.1f}%)")
    return result


def write_qa_report(df: pd.DataFrame, path: Path) -> None:
    """Write a QA summary CSV with row counts by note_type and source."""
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
    log.info("=" * 70)
    log.info("  BUILD CLINICAL_NOTES_LONG")
    log.info("=" * 70)

    if not RAW_PATH.exists():
        log.error(f"Raw file not found: {RAW_PATH}")
        sys.exit(1)
    if not CONFIG_PATH.exists():
        log.error(f"Column map not found: {CONFIG_PATH}")
        sys.exit(1)

    col_map = load_column_map(CONFIG_PATH)
    log.info(f"  Column map: {len(col_map)} entries, {col_map['is_note_like'].sum()} note-like")

    df = build_long(RAW_PATH, col_map)

    if df.empty:
        log.warning("  Empty result — nothing to write")
        sys.exit(1)

    out_parquet = PROCESSED / "clinical_notes_long.parquet"
    save_parquet(df, out_parquet)

    out_csv = PROCESSED / "clinical_notes_long.csv"
    df.to_csv(out_csv, index=False)
    log.info(f"  CSV export: {out_csv}")

    write_qa_report(df, PROCESSED / "clinical_notes_long_qa.csv")

    log.info("=" * 70)
    log.info("  DONE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
