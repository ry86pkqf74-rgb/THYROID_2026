#!/usr/bin/env python3
"""build_clinical_notes_long.py — Build long-format clinical notes from raw/Notes 12_1_25.xlsx

Outputs:
- processed/clinical_notes_long.parquet (preferred if parquet engine available)
- processed/clinical_notes_long.csv (fallback)

Schema:
- research_id (string)
- note_type (string)
- note_index (Int64 nullable)
- note_text (string)
- source_sheet (string)
- source_column (string)  # standardized snake_case column name
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
PROCESSED = ROOT / "processed"
PROCESSED.mkdir(exist_ok=True)

NOTES_XLSX = RAW / "Notes 12_1_25.xlsx"


def _norm_rid(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = s.strip()
    return s or None


def _snake(col: str) -> str:
    clean = re.sub(r"[^\w]+", "_", str(col).strip()).lower().strip("_")
    clean = re.sub(r"_+", "_", clean)
    return clean


def main() -> int:
    if not NOTES_XLSX.exists():
        raise FileNotFoundError(f"Missing {NOTES_XLSX}")

    # Read sheets
    s1 = pd.read_excel(NOTES_XLSX, sheet_name="Sheet1")
    s2 = pd.read_excel(NOTES_XLSX, sheet_name="Sheet2")

    rows: list[dict] = []

    # Sheet1
    sheet1_note_col = "Thyroid Cx History/summary"
    if sheet1_note_col in s1.columns:
        for _, r in s1[["Research ID number", sheet1_note_col]].iterrows():
            rid = _norm_rid(r["Research ID number"])
            txt = r[sheet1_note_col]
            if rid is None or pd.isna(txt):
                continue
            t = str(txt)
            if not t.strip():
                continue
            rows.append({
                "research_id": rid,
                "note_type": "THYROID_CX_HISTORY",
                "note_index": pd.NA,
                "note_text": t,
                "source_sheet": "Sheet1",
                "source_column": _snake(sheet1_note_col),
            })

    # Sheet2
    col_map: dict[str, tuple[str, int | None]] = {
        "Other History": ("OTHER_HISTORY", None),
        "Last Endocrine/FM note": ("ENDOCRINE_FM", None),
        "Other  notes": ("OTHER_NOTES", None),
        "DEATH": ("DEATH", None),
        "ED note 1": ("ED_NOTE", 1),
        "ED note 2": ("ED_NOTE", 2),
    }
    for i in range(1, 5):
        col_map[f"H&P-{i}"] = ("HP", i)
        col_map[f"OPNote-{i}"] = ("OPNOTE", i)
        col_map[f"DC_sum_{i}"] = ("DC_SUM", i)

    use_cols = ["Research ID number"] + [c for c in col_map if c in s2.columns]
    sub = s2[use_cols]
    for _, r in sub.iterrows():
        rid = _norm_rid(r["Research ID number"])
        if rid is None:
            continue
        for c, (nt, ni) in col_map.items():
            if c not in sub.columns:
                continue
            txt = r[c]
            if pd.isna(txt):
                continue
            t = str(txt)
            if not t.strip():
                continue
            rows.append({
                "research_id": rid,
                "note_type": nt,
                "note_index": ni if ni is not None else pd.NA,
                "note_text": t,
                "source_sheet": "Sheet2",
                "source_column": _snake(c),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print("No notes extracted.")
        return 0

    df["note_index"] = df["note_index"].astype("Int64")

    # QA
    print(f"clinical_notes_long: {len(df):,} notes across {df['research_id'].nunique():,} patients")
    print(df["note_type"].value_counts().to_string())

    # Write parquet preferred
    pq_path = PROCESSED / "clinical_notes_long.parquet"
    csv_path = PROCESSED / "clinical_notes_long.csv"

    wrote = False
    try:
        df.to_parquet(pq_path, index=False)
        wrote = True
        print(f"Wrote {pq_path}")
    except Exception as exc:
        print(f"Parquet write failed ({exc}); falling back to CSV")

    if not wrote:
        df.to_csv(csv_path, index=False)
        print(f"Wrote {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
