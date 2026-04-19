"""Flatten us_nodules_tirads.parquet → one row per (research_id, us_N) exam.

Input: processed/us_nodules_tirads.parquet (wide, ~8.8k patient rows, us_1..us_14 columns)
Output: processed/remaining/tirads_us_reports.parquet (long, ~15-20k exam rows)

Schema matches run_extraction_concurrent.py expectations.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE = Path("/Users/loganglosser/THYROID_2026")
IN_PATH = BASE / "processed/us_nodules_tirads.parquet"
OUT_PATH = BASE / "processed/remaining/tirads_us_reports.parquet"
SOURCE_WORKBOOK = "US Nodules TIRADS 12_1_25.xlsx"
SOURCE_SHEET = "us_nodules_tirads"  # logical name; original had 14 sheets
SCRIPT_VERSION = "flatten_tirads_us.v1"


NODULE_COLS = [f"nodule_{i}" for i in range(1, 11)] + [f"n{i}" for i in range(11, 15)]
TR_COLS = [f"n{i}_tr" for i in range(1, 15)]

MAX_EXAMS = 14


def _clean(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "null"):
        return ""
    return s


def _fmt_date(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def build_note_text(row: pd.Series, exam_idx: int) -> str:
    body = _clean(row.get(f"us_{exam_idx}"))
    impression = _clean(row.get(f"us_{exam_idx}_impression"))
    date_str = _fmt_date(row.get(f"us_{exam_idx}_date")) or ""
    if not body and not impression:
        return ""

    parts = []
    if date_str:
        parts.append(f"ULTRASOUND DATE: {date_str}")
    if body:
        parts.append("REPORT:\n" + body)
    if impression:
        parts.append("IMPRESSION:\n" + impression)

    # Nodule breakdown columns live on the patient row and describe the baseline
    # ultrasound (us_1). Include them ONLY for exam_idx == 1 so we don't
    # mis-attribute baseline nodule descriptions to follow-up exams.
    if exam_idx == 1:
        nodule_lines = []
        for nc, tc in zip(NODULE_COLS, TR_COLS):
            n = _clean(row.get(nc))
            t = _clean(row.get(tc))
            if n:
                line = f"- {nc}: {n}"
                if t:
                    line += f"  [TR:{t}]"
                nodule_lines.append(line)
        if nodule_lines:
            parts.append("NODULE BREAKDOWN:\n" + "\n".join(nodule_lines))

    return "\n\n".join(parts)


def main() -> None:
    df = pd.read_parquet(IN_PATH)
    print(f"Input: {IN_PATH}  rows={len(df):,}")

    batch_id = f"tirads-flatten-{uuid.uuid4().hex[:12]}"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    records = []
    for _, row in df.iterrows():
        research_id = _clean(row.get("research_id"))
        if not research_id:
            continue
        for k in range(1, MAX_EXAMS + 1):
            note_text = build_note_text(row, k)
            if not note_text:
                continue
            note_row_id = f"{research_id}_us{k}"
            source_column = f"us_{k}+impression" + ("+nodules" if k == 1 else "")
            records.append(
                {
                    "note_row_id": note_row_id,
                    "research_id": research_id,
                    "note_text": note_text,
                    "note_type": "ultrasound_report",
                    "note_date": _fmt_date(row.get(f"us_{k}_date")),
                    "source_workbook": SOURCE_WORKBOOK,
                    "source_sheet": SOURCE_SHEET,
                    "source_column": source_column,
                    "note_index": k,
                    "preprocess_batch_id": batch_id,
                    "preprocessed_at_utc": now_utc,
                    "preprocess_script_version": SCRIPT_VERSION,
                }
            )

    out = pd.DataFrame.from_records(records)
    print(f"Output rows: {len(out):,}")
    print(f"note_text len: mean={out['note_text'].str.len().mean():.0f}  p95={out['note_text'].str.len().quantile(0.95):.0f}  max={out['note_text'].str.len().max():.0f}")
    print("exam index distribution:")
    print(out["note_index"].value_counts().sort_index().to_string())

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
