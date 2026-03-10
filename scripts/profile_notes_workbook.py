#!/usr/bin/env python3
"""
profile_notes_workbook.py — Profile every sheet in Notes 12_1_25.xlsx

Outputs:
  processed/notes_workbook_profile.md       (human-readable)
  processed/notes_column_map_proposed.csv   (machine-readable mapping)
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.text_helpers import RESEARCH_ID_ALIASES, to_snake_case

RAW_PATH = ROOT / "raw" / "Notes 12_1_25.xlsx"
PROCESSED = ROOT / "processed"
PROCESSED.mkdir(exist_ok=True)

NOTE_TYPE_PATTERNS: list[tuple[re.Pattern, str, int | None]] = [
    (re.compile(r"^h_p_(\d+)$"), "h_p", None),
    (re.compile(r"^op_note_(\d+)$"), "op_note", None),
    (re.compile(r"^dc_sum_(\d+)$"), "dc_sum", None),
    (re.compile(r"^ed_note_(\d+)$"), "ed_note", None),
    (re.compile(r"^thyroid_cx_history_summary$"), "history_summary", 1),
    (re.compile(r"^last_endocrine_fm_note$"), "endocrine_note", 1),
    (re.compile(r"^last_endocrine_note$"), "endocrine_note", 1),
    (re.compile(r"^consult_note_(\d+)$"), "consult_note", None),
    (re.compile(r"^progress_note_(\d+)$"), "progress_note", None),
    (re.compile(r"^path_note_(\d+)$"), "path_note", None),
    (re.compile(r"^rai_note_(\d+)$"), "rai_note", None),
    (re.compile(r"^rad_onc_note_(\d+)$"), "rad_onc_note", None),
]

AVG_LEN_NOTE_THRESHOLD = 100


def infer_note_type(snake_col: str) -> tuple[str | None, int | None]:
    """Try to assign a note_type and note_index from the snake_case column name."""
    for pat, ntype, fixed_idx in NOTE_TYPE_PATTERNS:
        m = pat.match(snake_col)
        if m:
            idx = fixed_idx if fixed_idx is not None else int(m.group(1))
            return ntype, idx
    return None, None


def profile_column(series: pd.Series) -> dict:
    """Compute profiling stats for a single column."""
    total = len(series)
    non_null = series.notna().sum()
    pct_non_null = round(100 * non_null / total, 1) if total else 0.0

    if series.dtype == object:
        str_vals = series.dropna().astype(str)
        empty_count = (str_vals.str.strip() == "").sum()
        pct_empty = round(100 * empty_count / total, 1) if total else 0.0
        lengths = str_vals.str.len()
        avg_len = round(lengths.mean(), 1) if len(lengths) else 0.0
        max_len = int(lengths.max()) if len(lengths) else 0
    else:
        pct_empty = 0.0
        avg_len = 0.0
        max_len = 0

    return {
        "pct_non_null": pct_non_null,
        "pct_empty_string": pct_empty,
        "avg_length": avg_len,
        "max_length": max_len,
    }


def main() -> None:
    if not RAW_PATH.exists():
        print(f"ERROR: {RAW_PATH} not found")
        sys.exit(1)

    xl = pd.ExcelFile(RAW_PATH, engine="openpyxl")
    print(f"Workbook: {RAW_PATH.name}")
    print(f"Sheets:   {xl.sheet_names}\n")

    md_lines: list[str] = [
        "# Notes Workbook Profile",
        f"\nSource: `{RAW_PATH.name}`\n",
        f"Sheets: {len(xl.sheet_names)}\n",
    ]
    csv_rows: list[dict] = []

    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        n_rows, n_cols = df.shape
        print(f"--- Sheet: {sheet} ({n_rows} rows, {n_cols} cols) ---")

        md_lines.append(f"## Sheet: `{sheet}`")
        md_lines.append(f"\n- Rows: {n_rows:,}")
        md_lines.append(f"- Columns: {n_cols}")

        id_candidates = []
        for col in df.columns:
            snake = to_snake_case(str(col))
            if snake in RESEARCH_ID_ALIASES or snake == "research_id":
                id_candidates.append(col)

        if id_candidates:
            md_lines.append(f"- Research ID column(s): {id_candidates}")
        else:
            md_lines.append("- Research ID column(s): **NONE FOUND**")

        md_lines.append("\n| Column | Snake | %NonNull | %Empty | AvgLen | MaxLen | NoteLike |")
        md_lines.append("|--------|-------|----------|--------|--------|--------|----------|")

        for col in df.columns:
            snake = to_snake_case(str(col))
            is_id = snake in RESEARCH_ID_ALIASES or snake == "research_id"

            stats = profile_column(df[col])
            is_note_like = (
                not is_id
                and df[col].dtype == object
                and stats["avg_length"] >= AVG_LEN_NOTE_THRESHOLD
            )

            note_type, note_index = (None, None)
            if is_note_like:
                note_type, note_index = infer_note_type(snake)
                if note_type is None:
                    if "note" in snake or "summary" in snake:
                        note_type = snake
                        note_index = 1

            comments = ""
            if is_id:
                comments = "research_id candidate"
            elif is_note_like and note_type is None:
                comments = "note-like but no type pattern matched"

            md_lines.append(
                f"| {col} | {snake} | {stats['pct_non_null']}% | "
                f"{stats['pct_empty_string']}% | {stats['avg_length']} | "
                f"{stats['max_length']} | {'YES' if is_note_like else ''} |"
            )

            csv_rows.append({
                "sheet": sheet,
                "source_column_original": col,
                "source_column_snake": snake,
                "is_note_like": is_note_like,
                "proposed_note_type": note_type or "",
                "proposed_note_index": note_index if note_index is not None else "",
                "comments": comments,
            })

            flag = " [NOTE-LIKE]" if is_note_like else ""
            print(
                f"  {snake:40s}  {stats['pct_non_null']:5.1f}% non-null  "
                f"avg={stats['avg_length']:7.1f}  max={stats['max_length']:6d}{flag}"
            )

        md_lines.append("")

    profile_path = PROCESSED / "notes_workbook_profile.md"
    profile_path.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"\nProfile written to {profile_path}")

    csv_path = PROCESSED / "notes_column_map_proposed.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "sheet", "source_column_original", "source_column_snake",
            "is_note_like", "proposed_note_type", "proposed_note_index", "comments",
        ])
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"Column map written to {csv_path}")


if __name__ == "__main__":
    main()
