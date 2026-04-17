"""
Wave-3 notes preprocessor: ingest surgical pathology / synoptic / tumor-path
free-text columns.

Wave-1 covered Notes (op_notes, H&P, discharge, endocrine).
Wave-2 covered Imaging (Thyroid US, LN US) + FNA cytology.
Wave-3 (this) covers surgical PATH narratives, which is where final LN yield
and ENE language live:

    raw/All Diagnoses & synoptic 12_1_2025.xlsx :: synoptics + Dx merged
        - path extended (Gross path)              ~11,624
        - Synoptic diagnosis                       ~3,643
        - PATH DIAGNOSIS COMMENT                   ~2,758
        - Microscopic description                  ~1,669
        - Path Diagnosis Summary                  ~11,685
        - Path Special Studies                       ~818
        - FS Pathology (Frozen Section)            ~4,166
        - Tumor_{1..5}_LN examined comment
        - Tumor_{1..5}_margin & angiolymphatic invasion comment
        - Atypical adenomas
    raw/FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx
        - pathology_excerpt                        ~4,290
        - primary_ln_ln_comment                      ~82
        - tumor_{1..5}_angioinvasion_detail         ~3,800

Output schema matches wave-1/wave-2 long-notes parquet.

Usage:
    python scripts/preprocess_path_wave3.py
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import uuid
from pathlib import Path

import pandas as pd

SCRIPT_VERSION = "preprocess_path_wave3_v1"
RAW = Path("raw")
OUT = Path("processed/remaining/clinical_notes_long_wave3.parquet")

SYNOPTIC_WB = RAW / "All Diagnoses & synoptic 12_1_2025.xlsx"
SYNOPTIC_SHEET = "synoptics + Dx merged"
TUMORPATH_WB = RAW / "FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx"

MIN_CHARS = 25


def _norm_rid(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return s


def _norm_date(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, (pd.Timestamp, _dt.datetime, _dt.date)):
        ts = pd.Timestamp(v)
        if pd.isna(ts):
            return ""
        return ts.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return ""
    try:
        return pd.to_datetime(s, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return s


def _row_id(rid: str, sheet: str, col: str, idx: int, text: str) -> str:
    h = hashlib.sha1()
    h.update(f"{rid}|{sheet}|{col}|{idx}|{text}".encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _emit(out, rid, note_type, note_index, note_date, text, sheet, col,
          workbook, batch_id, ts):
    text = (text or "").strip()
    if len(text) < MIN_CHARS:
        return
    out.append({
        "note_row_id": _row_id(rid, sheet, col, note_index, text),
        "research_id": rid,
        "note_type": note_type,
        "note_index": int(note_index),
        "note_date": note_date,
        "note_text": text,
        "source_sheet": sheet,
        "source_column": col,
        "char_count": len(text),
        "source_workbook": workbook,
        "preprocess_batch_id": batch_id,
        "preprocessed_at_utc": ts,
        "preprocess_script_version": SCRIPT_VERSION,
    })


# ───────────────────────────────────────────────────────────
# synoptics + Dx merged
# ───────────────────────────────────────────────────────────
def _pick_date_col(cols):
    for candidate in ("surgery_date", "Surgery Date", "Surgery date",
                      "specimen_date", "Accession Date", "Collection Date"):
        if candidate in cols:
            return candidate
    # fallback: any column containing "date"
    for c in cols:
        if "date" in str(c).lower():
            return c
    return None


def ingest_synoptics(out, batch_id, ts):
    if not SYNOPTIC_WB.exists():
        print(f"  [skip] {SYNOPTIC_WB} not found")
        return 0
    df = pd.read_excel(SYNOPTIC_WB, sheet_name=SYNOPTIC_SHEET)
    workbook = SYNOPTIC_WB.name
    sheet = SYNOPTIC_SHEET
    cols = list(df.columns)

    # find rid column
    rid_col = None
    for c in ("research_id", "Research ID#", "Research ID number",
              "Research_ID#", "Research ID Number"):
        if c in cols:
            rid_col = c
            break
    if rid_col is None:
        for c in cols:
            if "research" in str(c).lower() and "id" in str(c).lower():
                rid_col = c
                break
    if rid_col is None:
        raise RuntimeError("no research_id col found in synoptics sheet")

    date_col = _pick_date_col(cols)
    print(f"  synoptics rid_col={rid_col!r}  date_col={date_col!r}")

    # narrative text columns to ingest
    NARRATIVE_COLS = [
        ("path extended (Gross path)", "gross_pathology"),
        ("Synoptic diagnosis", "synoptic_diagnosis"),
        ("PATH DIAGNOSIS COMMENT", "path_diagnosis_comment"),
        ("Microscopic description", "microscopic_description"),
        ("Path Diagnosis Summary", "path_diagnosis_summary"),
        ("Path Special Studies", "path_special_studies"),
        ("FS Pathology (Frozen Section)", "frozen_section_narrative"),
        ("Atypical  adenomas", "atypical_adenomas_comment"),
    ]
    # LN / angiolymphatic narrative per tumor (1..5)
    for n in range(1, 6):
        for pat, label in [
            (f"Tumor_{n}_LN examined comment", f"tumor{n}_ln_narrative"),
            (f"Tumor_{n}_margin & angiolymphatic invasion comment",
             f"tumor{n}_angiolymphatic_narrative"),
            (f"Tumor_{n}_additional finding 1", f"tumor{n}_additional_finding1"),
            (f"Tumor_{n}_additional finding 2", f"tumor{n}_additional_finding2"),
            (f"Tumor_{n}_additional finding 3", f"tumor{n}_additional_finding3"),
        ]:
            # match case-insensitive with leading/trailing spaces tolerated
            for c in cols:
                if str(c).strip().lower() == pat.lower():
                    NARRATIVE_COLS.append((c, label))
                    break

    emitted = 0
    for _, row in df.iterrows():
        rid = _norm_rid(row.get(rid_col))
        if not rid:
            continue
        d = _norm_date(row.get(date_col)) if date_col else ""
        for idx, (col, note_type) in enumerate(NARRATIVE_COLS, start=1):
            if col not in df.columns:
                continue
            v = row.get(col)
            if pd.isna(v):
                continue
            _emit(out, rid, note_type, idx, d, str(v), sheet, col,
                  workbook, batch_id, ts)
            emitted += 1
    return emitted


# ───────────────────────────────────────────────────────────
# TumorPath
# ───────────────────────────────────────────────────────────
def ingest_tumorpath(out, batch_id, ts):
    if not TUMORPATH_WB.exists():
        print(f"  [skip] {TUMORPATH_WB} not found")
        return 0
    df = pd.read_excel(TUMORPATH_WB)
    workbook = TUMORPATH_WB.name
    sheet = "Sheet1"
    cols = list(df.columns)

    rid_col = "research_id" if "research_id" in cols else None
    if rid_col is None:
        raise RuntimeError("no research_id in TumorPath")

    date_col = "surgery_date" if "surgery_date" in cols else None

    NARRATIVE_COLS = [
        ("pathology_excerpt", "tumorpath_excerpt"),
        ("ln_parsed_data_json", "tumorpath_ln_parsed_json"),
        ("ln_parsed_locations_json", "tumorpath_ln_locations_json"),
        ("primary_ln_ln_comment", "tumorpath_primary_ln_comment"),
    ]
    for n in range(1, 6):
        for pat, label in [
            (f"tumor_{n}_angioinvasion_detail", f"tumorpath_tumor{n}_angio_detail"),
            (f"tumor_{n}_ln_comment", f"tumorpath_tumor{n}_ln_comment"),
        ]:
            if pat in cols:
                NARRATIVE_COLS.append((pat, label))

    emitted = 0
    for _, row in df.iterrows():
        rid = _norm_rid(row.get(rid_col))
        if not rid:
            continue
        d = _norm_date(row.get(date_col)) if date_col else ""
        for idx, (col, note_type) in enumerate(NARRATIVE_COLS, start=1):
            if col not in cols:
                continue
            v = row.get(col)
            if pd.isna(v):
                continue
            _emit(out, rid, note_type, idx, d, str(v), sheet, col,
                  workbook, batch_id, ts)
            emitted += 1
    return emitted


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(OUT))
    args = ap.parse_args()

    batch_id = str(uuid.uuid4())
    ts = pd.Timestamp.utcnow().isoformat()
    rows: list[dict] = []

    print(f"==> wave-3 batch_id={batch_id}")
    n1 = ingest_synoptics(rows, batch_id, ts)
    print(f"  synoptics text notes: {n1:,}")
    n2 = ingest_tumorpath(rows, batch_id, ts)
    print(f"  tumorpath text notes: {n2:,}")
    print(f"  total wave-3 notes: {len(rows):,}")

    df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    print()
    print(f"unique research_ids: {df.research_id.nunique():,}")
    print("by note_type (top 20):")
    for k, v in df.note_type.value_counts().head(20).items():
        print(f"  {v:>6,}  {k}")
    print(f"\nwrote {len(df):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
