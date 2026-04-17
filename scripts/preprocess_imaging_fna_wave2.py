"""
Wave-2 notes preprocessor: ingest free-text from Imaging_12_1_25.xlsx and
FNAs 12_5_2025.xlsx into the same long-notes schema used by
clinical_notes_long.parquet.

Wave-1 (`preprocess_remaining_excels_v1`, batch b29a01dc...) only ingested
`Notes 12_1_25.xlsx` (op_notes, H&P, discharge summaries, ED notes,
endocrine notes, history summaries). That parquet has zero coverage of:

  * Direct radiology report text (Thyroid US sheet, 14 US episodes per RID,
    `US-N findings`, `US-N nodule_details`, `US-N Impression`)
  * LN US findings (LN_US1..LN_US4)
  * Head/neck US, scintigraphy, preop laryngoscopy free text
  * FNA cytology text (`FNA#N_path_extended`, `FNA#N History`, `Bethesda #N`)

These are the primary sources for the TI-RADS / cervical-LN / Bethesda
domains. Wave-2 fixes this so the LLM extractor can see them.

Output schema matches `processed/remaining/clinical_notes_long.parquet`:
    note_row_id (sha1), research_id, note_type, note_index, note_date,
    note_text, source_sheet, source_column, char_count, source_workbook,
    preprocess_batch_id, preprocessed_at_utc, preprocess_script_version

Usage:
    python scripts/preprocess_imaging_fna_wave2.py
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import re
import uuid
from pathlib import Path

import pandas as pd

SCRIPT_VERSION = "preprocess_imaging_fna_wave2_v1"
RAW = Path("raw")
OUT = Path("processed/remaining/clinical_notes_long_wave2.parquet")


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
        return s  # keep partial dates as-is; downstream parser handles them


def _row_id(rid: str, sheet: str, col: str, idx: int, text: str) -> str:
    h = hashlib.sha1()
    h.update(f"{rid}|{sheet}|{col}|{idx}|{text}".encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _emit(out, rid, note_type, note_index, note_date, text, sheet, col,
          workbook, batch_id, ts):
    text = (text or "").strip()
    if len(text) < 25:  # skip near-empty
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


# ──────────────────────────────────────────────────────────────────────────
# Imaging_12_1_25.xlsx ingest
# ──────────────────────────────────────────────────────────────────────────
def ingest_thyroid_us(out, batch_id, ts):
    """14 US episodes per RID. Per US: date, clinical hx, findings,
    nodule_details, impression. We emit ONE concatenated note per US episode
    so the LLM sees indication + findings + impression together."""
    path = RAW / "Imaging_12_1_25.xlsx"
    df = pd.read_excel(path, sheet_name="Thyroid US")
    rid_col = "Research ID number"
    workbook = path.name
    sheet = "Thyroid US"

    # Map US episode index → (date_col, hx_col, findings_col, nodules_col, impression_col)
    # Column names vary in case/spacing across episodes; we resolve by regex.
    cols = list(df.columns)

    def _find(idx, *needles):
        """Find column for episode `idx` matching ANY needle (case-insensitive)."""
        rx = re.compile(rf"\bus[-_\s]*0*{idx}\b", re.IGNORECASE)
        for c in cols:
            if not rx.search(str(c)):
                continue
            cl = str(c).lower()
            if any(n in cl for n in needles):
                return c
        return None

    episode_emitted = 0
    for idx in range(1, 15):
        date_col = _find(idx, "date")
        hx_col = _find(idx, "clinical", "hx", "history", "indication")
        findings_col = _find(idx, "finding")
        nodules_col = _find(idx, "nodule")
        impression_col = _find(idx, "impression")

        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            d = _norm_date(row.get(date_col)) if date_col else ""
            parts = []
            if hx_col and pd.notna(row.get(hx_col)):
                parts.append(f"[CLINICAL HISTORY/INDICATION]\n{str(row[hx_col]).strip()}")
            if findings_col and pd.notna(row.get(findings_col)):
                parts.append(f"[FINDINGS]\n{str(row[findings_col]).strip()}")
            if nodules_col and pd.notna(row.get(nodules_col)):
                parts.append(f"[NODULE DETAILS]\n{str(row[nodules_col]).strip()}")
            if impression_col and pd.notna(row.get(impression_col)):
                parts.append(f"[IMPRESSION]\n{str(row[impression_col]).strip()}")
            text = "\n\n".join(parts)
            _emit(out, rid, "thyroid_us_report", idx, d, text, sheet,
                  f"US{idx}_concatenated", workbook, batch_id, ts)
            episode_emitted += 1

    # Standalone supplementary fields
    for col, note_type in [
        ("Cervical Lymph Node US performed", "cervical_ln_us_freetext"),
        ("Head/neck US findings", "head_neck_us_freetext"),
        ("Thyroid scintigraphy", "thyroid_scintigraphy"),
        ("Preop Laryngoscopy", "preop_laryngoscopy"),
    ]:
        if col not in df.columns:
            continue
        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            v = row.get(col)
            if pd.isna(v):
                continue
            _emit(out, rid, note_type, 1, "", str(v), sheet, col,
                  workbook, batch_id, ts)
    return episode_emitted


def ingest_ln_us(out, batch_id, ts):
    path = RAW / "Imaging_12_1_25.xlsx"
    df = pd.read_excel(path, sheet_name="LN US")
    rid_col = "Research ID number"
    workbook = path.name
    sheet = "LN US"
    n = 0
    for col_idx, col in enumerate(["LN_US1", "LN_US2", "LN_US3", "LN_US4"], start=1):
        if col not in df.columns:
            continue
        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            v = row.get(col)
            if pd.isna(v):
                continue
            _emit(out, rid, "cervical_ln_us_report", col_idx, "", str(v),
                  sheet, col, workbook, batch_id, ts)
            n += 1
    return n


# ──────────────────────────────────────────────────────────────────────────
# FNAs 12_5_2025.xlsx ingest
# ──────────────────────────────────────────────────────────────────────────
def ingest_fna_workbook(out, batch_id, ts):
    """12 FNA episodes per RID. Per episode: date, specimen received,
    path_extended, history, Bethesda. Emit one concatenated note per FNA."""
    path = RAW / "FNAs 12_5_2025.xlsx"
    df = pd.read_excel(path)
    workbook = path.name
    sheet = "Sheet1"
    rid_col = "Research_ID#"

    cols = list(df.columns)

    def _match(idx, *needles):
        rx = re.compile(rf"fna\s*[#_]?\s*0*{idx}\b", re.IGNORECASE)
        for c in cols:
            if not rx.search(str(c)):
                continue
            cl = str(c).lower()
            if any(n in cl for n in needles):
                return c
        return None

    n_emitted = 0
    for idx in range(1, 13):
        # Episode 1 has slightly different column names ("#1_Preop_FNA_Date",
        # "Preop_Specimen_received_FNA_location", "FNA1_path_extended",
        # "Preop_FNA_history", "Bethesda*")
        if idx == 1:
            date_col = "#1_Preop_FNA_Date" if "#1_Preop_FNA_Date" in cols else _match(1, "date")
            spec_col = "Preop_Specimen_received_FNA_location" if "Preop_Specimen_received_FNA_location" in cols else _match(1, "specimen")
            path_col = "FNA1_path_extended" if "FNA1_path_extended" in cols else _match(1, "path")
            hx_col = "Preop_FNA_history" if "Preop_FNA_history" in cols else _match(1, "history")
            bet_col = "Bethesda*" if "Bethesda*" in cols else _match(1, "bethesda")
        else:
            date_col = _match(idx, "date")
            spec_col = _match(idx, "specimen")
            path_col = _match(idx, "path")
            hx_col = _match(idx, "history")
            bet_col = _match(idx, "bethesda")

        for _, row in df.iterrows():
            rid = _norm_rid(row.get(rid_col))
            if not rid:
                continue
            d = _norm_date(row.get(date_col)) if date_col else ""
            parts = []
            if spec_col and pd.notna(row.get(spec_col)):
                parts.append(f"[SPECIMEN/LOCATION]\n{str(row[spec_col]).strip()}")
            if hx_col and pd.notna(row.get(hx_col)):
                parts.append(f"[CLINICAL HISTORY]\n{str(row[hx_col]).strip()}")
            if path_col and pd.notna(row.get(path_col)):
                parts.append(f"[CYTOLOGY/PATH]\n{str(row[path_col]).strip()}")
            if bet_col and pd.notna(row.get(bet_col)):
                parts.append(f"[BETHESDA]\n{str(row[bet_col]).strip()}")
            text = "\n\n".join(parts)
            _emit(out, rid, "fna_episode", idx, d, text, sheet,
                  f"FNA{idx}_concatenated", workbook, batch_id, ts)
            n_emitted += 1
    return n_emitted


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(OUT))
    args = ap.parse_args()

    batch_id = str(uuid.uuid4())
    ts = pd.Timestamp.utcnow().isoformat()
    rows: list[dict] = []

    print(f"==> wave-2 batch_id={batch_id}")
    n_us = ingest_thyroid_us(rows, batch_id, ts)
    print(f"  Thyroid US episodes scanned: {n_us:,}")
    n_ln = ingest_ln_us(rows, batch_id, ts)
    print(f"  LN US episodes scanned: {n_ln:,}")
    n_fna = ingest_fna_workbook(rows, batch_id, ts)
    print(f"  FNA episodes scanned: {n_fna:,}")
    print(f"  total non-empty notes emitted: {len(rows):,}")

    df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    print()
    print(f"unique research_ids: {df.research_id.nunique():,}")
    print("by note_type:")
    for k, v in df.note_type.value_counts().items():
        print(f"  {v:>6,}  {k}")
    print(f"\nwrote {len(df):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
