#!/usr/bin/env python3
"""
integrate_missing_sources.py — Phase 6: Integrate 8 Missing High-Value Sources

Thyroid Cancer Research Lakehouse (THYROID_2026)

Creates 9 new Parquet tables from raw Excel sources:
  1. complications.parquet
  2. molecular_testing.parquet
  3. operative_details.parquet
  4. fna_history.parquet
  5. us_nodules_tirads.parquet
  6. serial_imaging_us.parquet
  7. path_synoptics.parquet
  8. clinical_notes.parquet
  9. extracted_clinical_events.parquet   (NLP-extracted labs, meds, comorbidities)

Also produces:
  - integration_report.csv
  - Updated data_dictionary.csv / data_dictionary.md
  - DuckDB advanced_features_v2 view
  - ETL stub in scripts/08_integrate_missing_sources.py
"""

from __future__ import annotations

import csv
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil import parser as dateutil_parser

# ── Logging ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("integrate")

# ── Paths ────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw"
PROCESSED = ROOT / "processed"
PROCESSED.mkdir(exist_ok=True)

ALT_SOURCE = Path.home() / "Downloads" / "Active Master Files"

SOURCE_FILES = {
    "complications": "Thyroid all_Complications 12_1_25.xlsx",
    "molecular":     "THYROSEQ_AFIRMA_12_5.xlsx",
    "operative":     "Thyroid OP Sheet data.xlsx",
    "fna":           "FNAs 12_5_2025.xlsx",
    "us_tirads":     "US Nodules TIRADS 12_1_25.xlsx",
    "imaging":       "Imaging_12_1_25.xlsx",
    "synoptic":      "All Diagnoses & synoptic 12_1_2025.xlsx",
    "notes":         "Notes 12_1_25.xlsx",
}

PHI_COLUMNS = {
    "patient_first_nm", "patient_last_nm", "patient_id",
    "empi_nbr", "euh_mrn", "tec_mrn", "dob", "date_of_birth",
    "surgeon", "death", "patient_first_name", "patient_last_name",
}

RESEARCH_ID_ALIASES = {
    "research_id", "research_id_number", "researchid", "record_id",
}


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def resolve_source(key: str) -> Path:
    """Locate the raw Excel file in /raw/ or the alternate downloads folder."""
    fname = SOURCE_FILES[key]
    p = RAW / fname
    if p.exists():
        return p
    alt = ALT_SOURCE / fname
    if alt.exists():
        log.info(f"  Using alt source: {alt}")
        return alt
    raise FileNotFoundError(f"Cannot find {fname} in {RAW} or {ALT_SOURCE}")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase snake_case all columns; unify research_id variants."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        clean = re.sub(r"[^\w]+", "_", str(col).strip()).lower().strip("_")
        clean = re.sub(r"_+", "_", clean)
        if clean in RESEARCH_ID_ALIASES:
            clean = "research_id"
        rename_map[col] = clean

    seen: dict[str, int] = {}
    final: dict[str, str] = {}
    for old, new in rename_map.items():
        if new in seen:
            seen[new] += 1
            final[old] = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
            final[old] = new
    return df.rename(columns=final)


def clean_research_id(df: pd.DataFrame) -> pd.DataFrame:
    """Cast research_id to int, dropping rows with invalid/missing IDs."""
    if "research_id" not in df.columns:
        log.warning("  No research_id column found")
        return df
    n_before = len(df)
    df["research_id"] = (
        df["research_id"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    mask = df["research_id"].str.fullmatch(r"\d+", na=False)
    df = df.loc[mask].copy()
    df["research_id"] = df["research_id"].astype(int)
    n_dropped = n_before - len(df)
    if n_dropped:
        log.info(f"  Dropped {n_dropped:,} rows with invalid/missing research_id")
    return df


def strip_phi(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns whose standardized name matches the PHI denylist."""
    to_drop = [c for c in df.columns if c in PHI_COLUMNS]
    if to_drop:
        log.info(f"  PHI stripped: {to_drop}")
        df = df.drop(columns=to_drop)
    return df


def safe_parse_date(val) -> str | None:
    """Robustly parse dates from Excel serial numbers, timestamps, or text."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (datetime, pd.Timestamp)):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    try:
        n = float(s)
        if 1 < n < 100_000:
            return (datetime(1899, 12, 30) + timedelta(days=int(n))).strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        pass
    try:
        return dateutil_parser.parse(s, dayfirst=False).strftime("%Y-%m-%d")
    except (ValueError, OverflowError, TypeError):
        return None


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """Write DataFrame to Parquet in /processed/. Coerces mixed-type columns to string."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)
    out = PROCESSED / f"{name}.parquet"
    df.to_parquet(out, engine="pyarrow", index=False)
    size_mb = out.stat().st_size / (1024 * 1024)
    log.info(f"  => {name}.parquet  {len(df):>8,} rows x {len(df.columns):>3} cols  ({size_mb:.2f} MB)")
    return out


# ═══════════════════════════════════════════════════════════════════
#  LARYNGOSCOPY NOTE PARSING
# ═══════════════════════════════════════════════════════════════════

_VC_STATUS = [
    (re.compile(r"(?:vocal\s*cord|VC)\s*(?:paraly[sz]\w+|palsy)", re.I), "paralysis"),
    (re.compile(r"(?:vocal\s*cord|VC)\s*(?:pares\w+|weakness|hypomobil\w+|immobil\w+)", re.I), "paresis"),
    (re.compile(r"normal\s+(?:vocal\s*cord|VC|cord)\s+(?:function|mobility|movement)", re.I), "normal"),
    (re.compile(r"(?:vocal\s*cord|VC)s?\s+(?:are|were|appear)\s+normal", re.I), "normal"),
    (re.compile(r"(?:symmetric|bilateral)\s+(?:vocal\s*cord|VC)\s+(?:mobility|movement)", re.I), "normal"),
    (re.compile(r"\bmobile\s+(?:vocal\s*cord|VC)", re.I), "normal"),
]

_SIDE = [
    (re.compile(r"\bleft\s+(?:vocal|VC|RLN|recurrent)", re.I), "left"),
    (re.compile(r"\bright\s+(?:vocal|VC|RLN|recurrent)", re.I), "right"),
    (re.compile(r"\bbilateral\s+(?:vocal|VC|RLN|recurrent)", re.I), "bilateral"),
]

DATE_RE = re.compile(r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\b")


def _extract_vc_status(text) -> str | None:
    if pd.isna(text):
        return None
    s = str(text)
    for pat, status in _VC_STATUS:
        if pat.search(s):
            return status
    return "unknown" if len(s.strip()) > 10 else None


def _extract_side(text) -> str | None:
    if pd.isna(text):
        return None
    s = str(text)
    for pat, side in _SIDE:
        if pat.search(s):
            return side
    return None


def _extract_palsy_detail(text) -> str | None:
    if pd.isna(text):
        return None
    s = str(text).strip()
    if not s:
        return None
    parts = []
    status = _extract_vc_status(s)
    side = _extract_side(s)
    if status:
        parts.append(status)
    if side:
        parts.append(side)
    return "; ".join(parts) if parts else None


def _extract_date_from_note(text) -> str | None:
    if pd.isna(text):
        return None
    m = DATE_RE.search(str(text))
    return safe_parse_date(m.group(1)) if m else None


# ═══════════════════════════════════════════════════════════════════
#  TABLE PROCESSORS
# ═══════════════════════════════════════════════════════════════════

def process_complications() -> pd.DataFrame:
    """File 1: complications + parsed laryngoscopy notes."""
    log.info("Processing complications...")
    path = resolve_source("complications")
    df = pd.read_excel(path, sheet_name="Complications", engine="openpyxl")
    df = standardize_columns(df)
    df = clean_research_id(df)
    df = strip_phi(df)

    laryng_cols = [c for c in df.columns if "laryngoscopy" in c and "note" in c]
    vc_cols = [c for c in df.columns if "vocal" in c and "note" in c.lower()]
    note_col = laryng_cols[0] if laryng_cols else None
    vc_note_col = vc_cols[0] if vc_cols else None

    raw_text = df[note_col] if note_col else (df[vc_note_col] if vc_note_col else pd.Series(dtype=str))

    if note_col or vc_note_col:
        src = note_col or vc_note_col
        df["_raw_laryngoscopy_note"] = df[src]
        combined = df[note_col].fillna("") if note_col else ""
        if vc_note_col and vc_note_col != note_col:
            combined = combined.astype(str) + " " + df[vc_note_col].fillna("").astype(str)
        df["vocal_cord_status"] = combined.apply(_extract_vc_status)
        df["affected_side"] = combined.apply(_extract_side)
        df["laryngoscopy_date"] = combined.apply(_extract_date_from_note)
        df["vocal_cord_palsy_detail"] = combined.apply(_extract_palsy_detail)

    return df


def process_molecular_testing() -> pd.DataFrame:
    """File 2: ThyroSeq/Afirma → long format (one row per test per patient)."""
    log.info("Processing molecular testing...")
    path = resolve_source("molecular")
    df = pd.read_excel(path, sheet_name="Thyroseq and AFIRMA", engine="openpyxl")
    df = standardize_columns(df)
    df = clean_research_id(df)
    df = strip_phi(df)

    pat = re.compile(r"^(.+?)_(\d+)$")
    groups: dict[int, dict[str, str]] = {}
    for col in df.columns:
        if col == "research_id":
            continue
        m = pat.match(col)
        if m:
            field, idx = m.group(1), int(m.group(2))
            groups.setdefault(idx, {})[col] = field

    if not groups:
        log.warning("  No numbered test groups detected — returning wide format")
        return df

    static = [c for c in df.columns if c == "research_id" or not pat.match(c)]
    frames = []
    for idx in sorted(groups):
        rename = groups[idx]
        avail = [c for c in static + list(rename.keys()) if c in df.columns]
        sub = df[avail].rename(columns=rename).copy()
        sub["test_index"] = idx
        melted = list(rename.values())
        sub = sub.dropna(subset=melted, how="all")
        frames.append(sub)

    result = pd.concat(frames, ignore_index=True)
    log.info(f"  Molecular testing melted: {len(result):,} rows across {len(groups)} test slots")
    return result


def process_operative_details() -> pd.DataFrame:
    """File 3: OP sheet — wide format (one row per patient)."""
    log.info("Processing operative details...")
    path = resolve_source("operative")
    df = pd.read_excel(path, sheet_name="Physical OP sheet data", engine="openpyxl")
    df = standardize_columns(df)
    df = clean_research_id(df)
    df = strip_phi(df)
    return df


def process_fna_history() -> pd.DataFrame:
    """File 4: FNA Bethesda → long format (one row per FNA per patient)."""
    log.info("Processing FNA history...")
    path = resolve_source("fna")
    df = pd.read_excel(path, sheet_name="FNA Bethesda", engine="openpyxl")
    df = standardize_columns(df)
    # Drop duplicate column names (Excel files with embedded newlines can cause collisions)
    df = df.loc[:, ~df.columns.duplicated()]
    df = clean_research_id(df)
    df = strip_phi(df)

    pat = re.compile(r"^fna_(\d+)_(.+)$")
    groups: dict[int, dict[str, str]] = {}
    for col in df.columns:
        m = pat.match(col)
        if m:
            idx, field = int(m.group(1)), m.group(2)
            groups.setdefault(idx, {})[col] = field

    if not groups:
        log.warning("  No FNA groups detected — returning wide format")
        return df

    static = [c for c in df.columns if c == "research_id" or not pat.match(c)]
    frames = []

    # FNA#1 uses non-standard naming (e.g. "1_preop_fna_date", "bethesda")
    # Synthesize an FNA#1 row from the static columns if detectable
    fna1_map: dict[str, str] = {}
    for col in static:
        if col == "research_id":
            continue
        cl = col.lower()
        if "1_preop_fna_date" in cl or (cl.startswith("1_") and "date" in cl):
            fna1_map[col] = "date"
        elif cl == "bethesda" or cl == "preop_bethesda":
            fna1_map[col] = "bethesda"
        elif "fna1_path" in cl or "fna_1_path" in cl:
            fna1_map[col] = "path_extended"
        elif cl == "preop_fna_history" or cl == "fna_history":
            fna1_map[col] = "history"
        elif cl == "preop_specimen_received_fna_location":
            fna1_map[col] = "specimen_received"

    if fna1_map:
        fna1_cols = ["research_id"] + list(fna1_map.keys())
        fna1 = df[fna1_cols].rename(columns=fna1_map).copy()
        fna1 = fna1.loc[:, ~fna1.columns.duplicated()]
        fna1["fna_index"] = 1
        fna1_data = [c for c in fna1_map.values() if c in fna1.columns]
        fna1 = fna1.dropna(subset=fna1_data, how="all")
        if len(fna1):
            frames.append(fna1)
            log.info(f"  Synthesized FNA#1 from static columns: {len(fna1):,} rows")

    for idx in sorted(groups):
        rename = groups[idx]
        avail = [c for c in static + list(rename.keys()) if c in df.columns]
        sub = df[avail].rename(columns=rename).copy()
        sub = sub.loc[:, ~sub.columns.duplicated()]
        sub["fna_index"] = idx
        melted = [c for c in rename.values() if c in sub.columns]
        sub = sub.dropna(subset=melted, how="all")
        frames.append(sub)

    result = pd.concat(frames, ignore_index=True, sort=False)

    if "date" in result.columns:
        result["fna_date_parsed"] = result["date"].apply(safe_parse_date)

    log.info(f"  FNA history melted: {len(result):,} rows across {len(set(result['fna_index']))} FNA slots")
    return result


def process_us_nodules_tirads() -> pd.DataFrame:
    """File 5: US Nodules TIRADS — 14 sheets stacked (one row per exam per patient)."""
    log.info("Processing US Nodules TIRADS (14 sheets)...")
    path = resolve_source("us_tirads")
    xl = pd.ExcelFile(path, engine="openpyxl")

    frames = []
    for sheet in xl.sheet_names:
        m = re.search(r"US[- ]*(\d+)", sheet, re.I)
        if not m:
            log.info(f"  Skipping non-US sheet: {sheet}")
            continue
        exam_idx = int(m.group(1))
        log.info(f"  Sheet {sheet} → exam_index={exam_idx}")
        sdf = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        sdf = standardize_columns(sdf)
        sdf = clean_research_id(sdf)
        sdf = strip_phi(sdf)
        sdf["us_exam_index"] = exam_idx

        data_cols = [c for c in sdf.columns if c not in ("research_id", "us_exam_index")]
        date_cols = [c for c in data_cols if "date" in c]
        if date_cols:
            sdf = sdf.dropna(subset=date_cols, how="all")
        else:
            sdf = sdf.dropna(subset=data_cols, how="all")

        if len(sdf):
            frames.append(sdf)

    result = pd.concat(frames, ignore_index=True)
    log.info(f"  US Nodules TIRADS: {len(result):,} total exam rows")
    return result


def process_serial_imaging() -> pd.DataFrame:
    """File 6: Imaging → long-format serial reports across modalities."""
    log.info("Processing serial imaging (8 sheets)...")
    path = resolve_source("imaging")
    xl = pd.ExcelFile(path, engine="openpyxl")

    modality_map = {
        "Thyroid US": "thyroid_us",
        "LN US": "ln_us",
        "US FNA": "us_fna",
        "CT & PETCT": "ct_petct",
        "Nuclear Med Scans": "nuclear_med",
        "MRI": "mri",
        "CXR": "cxr",
        "Other": "other_imaging",
    }

    all_frames = []
    for sheet in xl.sheet_names:
        modality = modality_map.get(sheet, sheet.lower().replace(" ", "_"))
        log.info(f"  Sheet '{sheet}' → modality={modality}")
        sdf = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        sdf = standardize_columns(sdf)
        sdf = clean_research_id(sdf)
        sdf = strip_phi(sdf)

        # For wide sheets with >30 cols, attempt to melt numbered report groups
        if len(sdf.columns) > 30:
            melted = _try_melt_report_groups(sdf)
            if melted is not None and len(melted) > 0:
                melted["modality"] = modality
                all_frames.append(melted)
                log.info(f"    Melted → {len(melted):,} rows")
                continue

        data_cols = [c for c in sdf.columns if c != "research_id"]
        sdf = sdf.dropna(subset=data_cols, how="all")
        sdf["modality"] = modality
        sdf["report_index"] = 1
        if len(sdf):
            all_frames.append(sdf)

    result = pd.concat(all_frames, ignore_index=True)
    log.info(f"  Serial imaging: {len(result):,} total rows")
    return result


def _try_melt_report_groups(df: pd.DataFrame) -> pd.DataFrame | None:
    """Detect columns following prefix_N_suffix pattern and melt to long format."""
    pat = re.compile(r"^(.+?)_(\d+)_(.+)$")
    groups: dict[int, dict[str, str]] = {}
    for col in df.columns:
        if col == "research_id":
            continue
        m = pat.match(col)
        if m:
            prefix, idx_str, suffix = m.groups()
            idx = int(idx_str)
            field = f"{prefix}_{suffix}"
            groups.setdefault(idx, {})[col] = field

    if len(groups) < 2:
        return None

    static = [c for c in df.columns if c == "research_id" or not pat.match(c)]
    frames = []
    for idx in sorted(groups):
        rename = groups[idx]
        avail = [c for c in static + list(rename.keys()) if c in df.columns]
        sub = df[avail].rename(columns=rename).copy()
        sub["report_index"] = idx
        melted = list(rename.values())
        sub = sub.dropna(subset=melted, how="all")
        if len(sub):
            frames.append(sub)

    return pd.concat(frames, ignore_index=True) if frames else None


def process_path_synoptics() -> pd.DataFrame:
    """File 7: Full synoptic pathology — wide format (one row per operation)."""
    log.info("Processing path synoptics (275 cols)...")
    path = resolve_source("synoptic")
    df = pd.read_excel(path, sheet_name="synoptics + Dx merged", engine="openpyxl")
    df = standardize_columns(df)
    df = clean_research_id(df)
    df = strip_phi(df)

    n_dupes = df.duplicated(subset="research_id", keep=False).sum()
    if n_dupes:
        log.info(f"  {n_dupes:,} rows share a research_id (re-operations)")

    return df


def process_clinical_notes() -> pd.DataFrame:
    """File 8: Clinical notes — merge Sheet1 (demographics/summary) + Sheet2 (full notes)."""
    log.info("Processing clinical notes...")
    path = resolve_source("notes")

    s1 = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
    s1 = standardize_columns(s1)
    s1 = clean_research_id(s1)
    s1 = strip_phi(s1)

    s2 = pd.read_excel(path, sheet_name="Sheet2", engine="openpyxl")
    s2 = standardize_columns(s2)
    s2 = clean_research_id(s2)
    s2 = strip_phi(s2)

    if "research_id" in s1.columns and "research_id" in s2.columns:
        shared = set(s1.columns) & set(s2.columns) - {"research_id"}
        if shared:
            s2 = s2.drop(columns=list(shared))
        df = s1.merge(s2, on="research_id", how="outer")
    else:
        df = pd.concat([s1, s2], ignore_index=True)

    log.info(f"  Clinical notes: {len(df):,} rows, {len(df.columns)} cols")
    return df


# ═══════════════════════════════════════════════════════════════════
#  NLP EXTRACTION LAYER
# ═══════════════════════════════════════════════════════════════════

LAB_PATTERNS: dict[str, re.Pattern] = {
    "TSH": re.compile(
        r"\b(?:TSH|thyroid[\s-]*stimulating)\s*(?:level\s*)?(?:=|:|\s+(?:of|was|is|at)\s+)?\s*"
        r"([<>]?\s*\d+\.?\d*)\s*(mIU/L|uIU/mL|mU/L)?", re.I),
    "thyroglobulin": re.compile(
        r"\b(?:thyroglobulin|(?<![a-z])Tg(?![a-z]))\s*(?:level\s*)?(?:=|:|\s+(?:of|was|is)\s+)?\s*"
        r"([<>]?\s*\d+\.?\d*)\s*(ng/mL)?", re.I),
    "anti_thyroglobulin": re.compile(
        r"\b(?:anti[\s-]*thyroglobulin|anti[\s-]*Tg|TgAb)\s*(?:antibod\w*)?\s*"
        r"(?:=|:|\s+(?:of|was|is)\s+)?\s*([<>]?\s*\d+\.?\d*)\s*(IU/mL)?", re.I),
    "calcium": re.compile(
        r"\bcalcium\s*(?:level\s*)?(?:=|:|\s+(?:of|was|is)\s+)?\s*"
        r"(\d+\.?\d*)\s*(mg/dL|mmol/L)?", re.I),
    "PTH": re.compile(
        r"\b(?:PTH|parathyroid[\s-]*hormone|intact[\s-]*PTH)\s*(?:=|:|\s+(?:of|was|is)\s+)?\s*"
        r"(\d+\.?\d*)\s*(pg/mL)?", re.I),
    "vitamin_D": re.compile(
        r"\b(?:vitamin[\s-]*D|25[\s-]*OH[\s-]*D|vit[\s-]*D)\s*(?:=|:|\s+(?:of|was|is)\s+)?\s*"
        r"(\d+\.?\d*)\s*(ng/mL)?", re.I),
}

MEDICATION_PATTERNS: dict[str, re.Pattern] = {
    "levothyroxine": re.compile(
        r"\b(?:levothyroxine|synthroid|levoxyl|l[\s-]*thyroxine)\s*"
        r"(\d+\.?\d*)\s*(mcg|mg|ug)?", re.I),
    "calcium_supplement": re.compile(
        r"\b(?:calcium\s+(?:carbonate|citrate)|caltrate|tums|oscal|citracal)"
        r"(?:\s+(\d+\.?\d*)\s*(mg)?)?", re.I),
    "calcitriol": re.compile(
        r"\b(?:calcitriol|rocaltrol)\s*(?:(\d+\.?\d*)\s*(mcg|mg|ug)?)?", re.I),
}

COMORBIDITY_PATTERNS: dict[str, re.Pattern] = {
    "hypertension":       re.compile(r"\b(?:hypertension|HTN)\b", re.I),
    "diabetes_type2":     re.compile(r"\b(?:type\s*2\s*diabet\w*|DM\s*2|T2DM|NIDDM)\b", re.I),
    "diabetes":           re.compile(r"\b(?:diabet\w+|IDDM)\b", re.I),
    "breast_cancer":      re.compile(r"\bbreast\s+(?:cancer|carcinoma)\b", re.I),
    "lung_cancer":        re.compile(r"\blung\s+(?:cancer|carcinoma)\b", re.I),
    "obesity":            re.compile(r"\bobes\w+\b", re.I),
    "CAD":                re.compile(r"\b(?:coronary\s+artery\s+disease|CAD)\b", re.I),
    "atrial_fibrillation": re.compile(r"\b(?:atrial\s+fibrillat\w+|a[\s-]*fib)\b", re.I),
    "hypothyroidism":     re.compile(r"\bhypothyroid\w+\b", re.I),
    "hyperthyroidism":    re.compile(r"\bhyperthyroid\w+\b", re.I),
    "GERD":               re.compile(r"\b(?:GERD|gastroesophageal\s+reflux)\b", re.I),
    "CKD":                re.compile(r"\b(?:chronic\s+kidney|CKD|renal\s+insufficiency)\b", re.I),
    "depression":         re.compile(r"\b(?:depression|MDD)\b", re.I),
    "asthma":             re.compile(r"\basthma\b", re.I),
    "COPD":               re.compile(r"\b(?:COPD|chronic\s+obstructive)\b", re.I),
}

TREATMENT_PATTERNS: dict[str, re.Pattern] = {
    "RAI":         re.compile(r"\b(?:radioactive\s+iodine|RAI|I[\s-]*131|131[\s-]*I)\b", re.I),
    "EBRT":        re.compile(r"\b(?:external\s+beam|EBRT|radiation\s+therap\w+|XRT)\b", re.I),
    "recurrence":  re.compile(r"\b(?:recurren\w+|persistent\s+disease|structural\s+disease)\b", re.I),
    "reoperation": re.compile(r"\b(?:re[\s-]*operat\w+|completion\s+thyroidectom\w+)\b", re.I),
}


def _extract_events_from_text(
    research_id: int, text: str, source_col: str,
) -> list[dict]:
    """Run all NLP extractors on a single text chunk. Returns list of event dicts."""
    events: list[dict] = []
    if not text or len(text.strip()) < 5:
        return events

    def _ev(etype, esub, val=None, unit=None, date=None, snippet=""):
        return {
            "research_id": research_id,
            "event_type": etype,
            "event_subtype": esub,
            "event_value": val,
            "event_unit": unit,
            "event_date": date,
            "event_text": snippet[:250],
            "source_column": source_col,
        }

    for lab, pat in LAB_PATTERNS.items():
        for m in pat.finditer(text):
            try:
                val = float(re.sub(r"[<>\s]", "", m.group(1)))
            except (ValueError, IndexError):
                val = None
            unit = m.group(2) if m.lastindex and m.lastindex >= 2 else None
            events.append(_ev("lab", lab, val, unit, snippet=m.group(0)))

    for med, pat in MEDICATION_PATTERNS.items():
        for m in pat.finditer(text):
            dose = None
            if m.lastindex and m.lastindex >= 1 and m.group(1):
                try:
                    dose = float(m.group(1))
                except ValueError:
                    pass
            unit = m.group(2) if m.lastindex and m.lastindex >= 2 else None
            events.append(_ev("medication", med, dose, unit, snippet=m.group(0)))

    for cond, pat in COMORBIDITY_PATTERNS.items():
        m_c = pat.search(text)
        if m_c:
            events.append(_ev("comorbidity", cond, snippet=m_c.group(0)))

    for tx, pat in TREATMENT_PATTERNS.items():
        m_t = pat.search(text)
        if m_t:
            ctx = text[max(0, m_t.start() - 80) : m_t.end() + 80]
            d = DATE_RE.search(ctx)
            dt = safe_parse_date(d.group(1)) if d else None
            events.append(_ev("treatment", tx, date=dt, snippet=m_t.group(0)))

    # Follow-up dates near keywords
    fu_pat = re.compile(
        r"(?:follow[\s-]*up|f/u|seen\s+on|visit\s+on|return\s+on)\s*"
        r"(?:on\s+)?(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})", re.I,
    )
    for m_fu in fu_pat.finditer(text):
        dt = safe_parse_date(m_fu.group(1))
        if dt:
            events.append(_ev("follow_up", "follow_up_date", date=dt, snippet=m_fu.group(0)))

    return events


def build_extracted_events(notes_df: pd.DataFrame) -> pd.DataFrame:
    """Run NLP extraction across all note columns for every patient."""
    log.info("Running NLP extraction on clinical notes...")

    note_cols = []
    for c in notes_df.columns:
        if c == "research_id" or notes_df[c].dtype != object:
            continue
        if notes_df[c].notna().any() and notes_df[c].str.len().median() > 50:
            note_cols.append(c)
    if not note_cols:
        note_cols = [c for c in notes_df.columns if c != "research_id" and notes_df[c].dtype == object]

    log.info(f"  Processing {len(note_cols)} text columns across {len(notes_df):,} patients")

    all_events: list[dict] = []
    for _, row in notes_df.iterrows():
        rid = row["research_id"]
        for col in note_cols:
            text = row.get(col)
            if pd.isna(text) or not str(text).strip():
                continue
            evts = _extract_events_from_text(rid, str(text), col)
            all_events.extend(evts)

        if len(all_events) % 50_000 == 0 and all_events:
            log.info(f"    ... {len(all_events):,} events extracted so far")

    events_df = pd.DataFrame(all_events)
    if events_df.empty:
        events_df = pd.DataFrame(columns=[
            "research_id", "event_type", "event_subtype",
            "event_value", "event_unit", "event_date",
            "event_text", "source_column",
        ])

    # Deduplicate comorbidities (one per patient per condition)
    comorb = events_df[events_df["event_type"] == "comorbidity"]
    other = events_df[events_df["event_type"] != "comorbidity"]
    comorb = comorb.drop_duplicates(subset=["research_id", "event_subtype"])
    events_df = pd.concat([other, comorb], ignore_index=True)

    log.info(f"  Extracted {len(events_df):,} total clinical events")
    by_type = events_df.groupby("event_type").size()
    for t, n in by_type.items():
        log.info(f"    {t}: {n:,}")

    return events_df


# ═══════════════════════════════════════════════════════════════════
#  DUCKDB VIEW
# ═══════════════════════════════════════════════════════════════════

ADVANCED_V2_SQL = """
CREATE OR REPLACE VIEW advanced_features_v2 AS
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    -- Tumor pathology
    tp.histology_1_type,
    tp.variant_standardized,
    tp.surgery_type_normalized,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
    tp.tumor_1_extrathyroidal_ext,
    tp.tumor_1_gross_ete,
    -- Complications
    comp.rln_injury_vocal_cord_paralysis,
    comp.vocal_cord_status,
    comp.seroma,
    comp.hematoma,
    comp.hypocalcemia,
    comp.hypoparathyroidism,
    -- Operative
    od.ebl,
    od.skin_to_skin_time,
    -- Benign flags
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hashimoto,
    -- Mutation flags
    tp.braf_mutation_mentioned,
    tp.ras_mutation_mentioned,
    tp.ret_mutation_mentioned,
    tp.tert_mutation_mentioned,
    -- Data availability
    CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
    CASE WHEN od.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_operative_details,
    CASE WHEN cn.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes,
    CASE WHEN ps.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_path_synoptics,
    CASE WHEN mt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_molecular_testing,
    CASE WHEN fh_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_fna_history,
    CASE WHEN unt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_us_tirads,
    mc.has_tumor_pathology,
    mc.has_benign_pathology,
    mc.has_fna_cytology,
    mc.has_ultrasound_reports,
    mc.has_ct_imaging,
    mc.has_mri_imaging,
    mc.has_nuclear_med,
    mc.has_thyroglobulin_labs,
    mc.has_anti_thyroglobulin_labs,
    mc.has_parathyroid
FROM master_cohort mc
LEFT JOIN tumor_pathology tp      ON mc.research_id = tp.research_id
LEFT JOIN benign_pathology bp     ON mc.research_id = bp.research_id
LEFT JOIN complications comp      ON mc.research_id = CAST(comp.research_id AS VARCHAR)
LEFT JOIN operative_details od    ON mc.research_id = CAST(od.research_id AS VARCHAR)
LEFT JOIN clinical_notes cn       ON mc.research_id = CAST(cn.research_id AS VARCHAR)
LEFT JOIN path_synoptics ps       ON mc.research_id = CAST(ps.research_id AS VARCHAR)
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM molecular_testing) mt_agg
    ON mc.research_id = mt_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM fna_history) fh_agg
    ON mc.research_id = fh_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM us_nodules_tirads) unt_agg
    ON mc.research_id = unt_agg.research_id
"""


def build_advanced_features_v2_view(results: dict) -> None:
    """Load new parquets into DuckDB and create the advanced_features_v2 view."""
    db_path = ROOT / "thyroid_master.duckdb"
    if not db_path.exists():
        log.warning("DuckDB not found — skipping view creation. Run 02_build_duckdb_full.py first.")
        return

    try:
        import duckdb
    except ImportError:
        log.warning("duckdb not installed — skipping view creation")
        return

    log.info("Building advanced_features_v2 in DuckDB...")
    con = duckdb.connect(str(db_path))

    new_tables = [
        "complications", "molecular_testing", "operative_details",
        "fna_history", "us_nodules_tirads", "serial_imaging_us",
        "path_synoptics", "clinical_notes", "extracted_clinical_events",
    ]
    for tbl in new_tables:
        pq_path = PROCESSED / f"{tbl}.parquet"
        if pq_path.exists():
            con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{pq_path}')")
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  Loaded {tbl}: {cnt:,} rows")

    try:
        con.execute(ADVANCED_V2_SQL)
        n = con.execute("SELECT COUNT(*) FROM advanced_features_v2").fetchone()[0]
        log.info(f"  Created advanced_features_v2: {n:,} rows")
    except Exception as exc:
        log.error(f"  View creation failed: {exc}")

    con.close()


# ═══════════════════════════════════════════════════════════════════
#  REPORTING
# ═══════════════════════════════════════════════════════════════════

def generate_integration_report(results: dict) -> Path:
    """Write integration_report.csv with per-table stats."""
    log.info("Generating integration report...")
    rows = []
    for name, info in results.items():
        df = info.get("df")
        if df is None or df.empty:
            rows.append({"table": name, "row_count": 0, "col_count": 0})
            continue
        null_rate = df.isnull().mean().mean()
        extra = {}
        if name == "extracted_clinical_events" and "event_type" in df.columns:
            for et in ("lab", "medication", "comorbidity", "treatment", "follow_up"):
                extra[f"n_{et}_events"] = int((df["event_type"] == et).sum())
        rows.append({
            "table": name,
            "row_count": len(df),
            "col_count": len(df.columns),
            "null_rate_pct": round(null_rate * 100, 2),
            "unique_patients": int(df["research_id"].nunique()) if "research_id" in df.columns else 0,
            **extra,
        })

    report_path = ROOT / "integration_report.csv"
    report_df = pd.DataFrame(rows)
    report_df.to_csv(report_path, index=False)
    log.info(f"  Saved {report_path}")
    return report_path


# ═══════════════════════════════════════════════════════════════════
#  DATA DICTIONARY UPDATE
# ═══════════════════════════════════════════════════════════════════

NEW_DICT_ENTRIES = [
    # complications
    ("table", "complications", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "complications", "rln_injury_vocal_cord_paralysis", "VARCHAR", "RLN injury / vocal cord paralysis indicator", "phase6"),
    ("table", "complications", "seroma", "VARCHAR", "Seroma complication flag", "phase6"),
    ("table", "complications", "hematoma", "VARCHAR", "Hematoma complication flag", "phase6"),
    ("table", "complications", "hypocalcemia", "VARCHAR", "Hypocalcemia complication flag", "phase6"),
    ("table", "complications", "hypoparathyroidism", "VARCHAR", "Hypoparathyroidism complication flag", "phase6"),
    ("table", "complications", "vocal_cord_status", "VARCHAR", "Extracted vocal cord status (normal/paresis/paralysis)", "phase6"),
    ("table", "complications", "affected_side", "VARCHAR", "Affected side (left/right/bilateral)", "phase6"),
    ("table", "complications", "laryngoscopy_date", "VARCHAR", "Extracted laryngoscopy date", "phase6"),
    ("table", "complications", "vocal_cord_palsy_detail", "VARCHAR", "Combined palsy detail string", "phase6"),
    ("table", "complications", "_raw_laryngoscopy_note", "VARCHAR", "Raw laryngoscopy note text", "phase6"),
    # molecular_testing
    ("table", "molecular_testing", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "molecular_testing", "test_index", "INT64", "Test slot (1-3)", "phase6"),
    ("table", "molecular_testing", "thyroseq_afirma", "VARCHAR", "Test type (ThyroSeq or Afirma)", "phase6"),
    ("table", "molecular_testing", "date", "VARCHAR", "Test date", "phase6"),
    ("table", "molecular_testing", "result", "VARCHAR", "Test result", "phase6"),
    ("table", "molecular_testing", "mutation", "VARCHAR", "Detected mutation", "phase6"),
    ("table", "molecular_testing", "detailed_findings", "VARCHAR", "Detailed findings text", "phase6"),
    # operative_details
    ("table", "operative_details", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "operative_details", "surg_date", "VARCHAR", "Surgery date", "phase6"),
    ("table", "operative_details", "preop_diagnosis", "VARCHAR", "Preoperative diagnosis", "phase6"),
    ("table", "operative_details", "ebl", "VARCHAR", "Estimated blood loss", "phase6"),
    ("table", "operative_details", "skin_to_skin_time", "VARCHAR", "Skin-to-skin operative time", "phase6"),
    # fna_history
    ("table", "fna_history", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "fna_history", "fna_index", "INT64", "FNA sequence number (1-12)", "phase6"),
    ("table", "fna_history", "date", "VARCHAR", "FNA date (raw)", "phase6"),
    ("table", "fna_history", "fna_date_parsed", "VARCHAR", "FNA date (parsed YYYY-MM-DD)", "phase6"),
    ("table", "fna_history", "bethesda", "VARCHAR", "Bethesda score", "phase6"),
    ("table", "fna_history", "path", "VARCHAR", "Pathology text", "phase6"),
    ("table", "fna_history", "path_extended", "VARCHAR", "Extended pathology text", "phase6"),
    # us_nodules_tirads
    ("table", "us_nodules_tirads", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "us_nodules_tirads", "us_exam_index", "INT64", "US exam timepoint (1-14)", "phase6"),
    ("table", "us_nodules_tirads", "date", "VARCHAR", "US exam date", "phase6"),
    ("table", "us_nodules_tirads", "impression", "VARCHAR", "US impression text", "phase6"),
    # serial_imaging_us
    ("table", "serial_imaging_us", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "serial_imaging_us", "modality", "VARCHAR", "Imaging modality (thyroid_us, ln_us, ct_petct, mri, etc.)", "phase6"),
    ("table", "serial_imaging_us", "report_index", "INT64", "Report sequence number within modality", "phase6"),
    # path_synoptics
    ("table", "path_synoptics", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "path_synoptics", "path_diagnosis_summary", "VARCHAR", "Pathology diagnosis summary", "phase6"),
    ("table", "path_synoptics", "synoptic_diagnosis", "VARCHAR", "Full synoptic diagnosis text", "phase6"),
    ("table", "path_synoptics", "thyroid_procedure", "VARCHAR", "Thyroid surgical procedure", "phase6"),
    ("table", "path_synoptics", "race", "VARCHAR", "Patient race", "phase6"),
    ("table", "path_synoptics", "gender", "VARCHAR", "Patient gender", "phase6"),
    # clinical_notes
    ("table", "clinical_notes", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "clinical_notes", "thyroid_cx_history_summary", "VARCHAR", "Thyroid cancer history/summary note", "phase6"),
    ("table", "clinical_notes", "h_p_1", "VARCHAR", "History & Physical note 1", "phase6"),
    ("table", "clinical_notes", "op_note_1", "VARCHAR", "Operative note 1", "phase6"),
    ("table", "clinical_notes", "dc_sum_1", "VARCHAR", "Discharge summary 1", "phase6"),
    ("table", "clinical_notes", "last_endocrine_fm_note", "VARCHAR", "Last endocrine/family medicine note", "phase6"),
    # extracted_clinical_events
    ("table", "extracted_clinical_events", "research_id", "INT64", "Patient identifier", "phase6"),
    ("table", "extracted_clinical_events", "event_type", "VARCHAR", "Event category (lab, medication, comorbidity, treatment, follow_up)", "phase6"),
    ("table", "extracted_clinical_events", "event_subtype", "VARCHAR", "Specific event (TSH, levothyroxine, hypertension, RAI, etc.)", "phase6"),
    ("table", "extracted_clinical_events", "event_value", "DOUBLE", "Numeric value if applicable (lab result, dose)", "phase6"),
    ("table", "extracted_clinical_events", "event_unit", "VARCHAR", "Value units (ng/mL, mcg, etc.)", "phase6"),
    ("table", "extracted_clinical_events", "event_date", "VARCHAR", "Event date if extractable", "phase6"),
    ("table", "extracted_clinical_events", "event_text", "VARCHAR", "Raw matched text snippet (max 250 chars)", "phase6"),
    ("table", "extracted_clinical_events", "source_column", "VARCHAR", "Source note column name", "phase6"),
    # advanced_features_v2
    ("view", "advanced_features_v2", "research_id", "VARCHAR", "Patient identifier (master anchor)", "phase6"),
    ("view", "advanced_features_v2", "has_complications", "BOOLEAN", "Patient has complications data", "phase6"),
    ("view", "advanced_features_v2", "has_operative_details", "BOOLEAN", "Patient has operative details", "phase6"),
    ("view", "advanced_features_v2", "has_clinical_notes", "BOOLEAN", "Patient has clinical notes", "phase6"),
    ("view", "advanced_features_v2", "has_path_synoptics", "BOOLEAN", "Patient has synoptic pathology", "phase6"),
    ("view", "advanced_features_v2", "has_molecular_testing", "BOOLEAN", "Patient has molecular test data", "phase6"),
    ("view", "advanced_features_v2", "has_fna_history", "BOOLEAN", "Patient has detailed FNA history", "phase6"),
    ("view", "advanced_features_v2", "has_us_tirads", "BOOLEAN", "Patient has US TIRADS data", "phase6"),
]


def update_data_dictionary(results: dict) -> None:
    """Append Phase 6 entries to data_dictionary.csv and data_dictionary.md."""
    log.info("Updating data dictionary...")

    # CSV
    csv_path = ROOT / "data_dictionary.csv"
    existing_rows = []
    if csv_path.exists():
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            existing_rows = list(reader)

    existing_keys = {
        (r[0], r[1], r[2]) for r in existing_rows[1:] if len(r) >= 3
    }
    new_rows = [
        list(entry) for entry in NEW_DICT_ENTRIES
        if (entry[0], entry[1], entry[2]) not in existing_keys
    ]

    if new_rows:
        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)
        log.info(f"  Appended {len(new_rows)} rows to data_dictionary.csv")

    # Markdown
    md_path = ROOT / "data_dictionary.md"
    md_section = """

---

## Phase 6: Integrated Source Tables (8 New Excel Sources)

### `complications` (table)

Source: `Thyroid all_Complications 12_1_25.xlsx`

Surgical complications with NLP-parsed laryngoscopy notes. Key columns:
`rln_injury_vocal_cord_paralysis`, `seroma`, `hematoma`, `hypocalcemia`,
`hypoparathyroidism`, `vocal_cord_status` (normal/paresis/paralysis),
`affected_side`, `laryngoscopy_date`, `_raw_laryngoscopy_note`.

### `molecular_testing` (table, long format)

Source: `THYROSEQ_AFIRMA_12_5.xlsx`

One row per molecular test per patient (up to 3 tests). Key columns:
`test_index`, `thyroseq_afirma`, `date`, `result`, `mutation`, `detailed_findings`.

### `operative_details` (table)

Source: `Thyroid OP Sheet data.xlsx`

Operative sheet data — BMI, EBL, skin-to-skin time, nerve monitoring,
parathyroid autograft notes, IO tumor appearance.

### `fna_history` (table, long format)

Source: `FNAs 12_5_2025.xlsx`

One row per FNA per patient (up to 12 FNAs). Key columns:
`fna_index`, `date`, `bethesda`, `path`, `path_extended`, `specimen_received`.

### `us_nodules_tirads` (table, long format)

Source: `US Nodules TIRADS 12_1_25.xlsx`

One row per US exam per patient (up to 14 exams). Includes per-nodule
TIRADS scores and nodule descriptions within each exam.

### `serial_imaging_us` (table, long format)

Source: `Imaging_12_1_25.xlsx`

Serial imaging reports across 8 modalities (thyroid_us, ln_us, us_fna,
ct_petct, nuclear_med, mri, cxr, other). Raw report text and impressions.

### `path_synoptics` (table, wide — 275+ cols)

Source: `All Diagnoses & synoptic 12_1_2025.xlsx`

Full AJCC staging, margins, variants, LN details for up to 5 tumors.
Includes synoptic diagnosis text, path diagnosis summary, and benign findings.
Note: contains duplicate research_ids for re-operations.

### `clinical_notes` (table)

Source: `Notes 12_1_25.xlsx`

Combined demographics/summary (Sheet1) + clinical notes (Sheet2).
H&P notes 1-4, OP notes 1-4, discharge summaries 1-4, last endocrine/FM note,
ED notes 1-2. Notes may be truncated at 32,767 characters (Excel limit).

### `extracted_clinical_events` (table, long format)

NLP-extracted events from clinical notes. Event types:
- **lab**: TSH, thyroglobulin, anti-Tg, calcium, PTH, vitamin D (with values and units)
- **medication**: levothyroxine (with dose), calcium supplements, calcitriol
- **comorbidity**: hypertension, diabetes, breast/lung cancer, obesity, CAD, etc.
- **treatment**: RAI, EBRT, recurrence, reoperation (with dates when available)
- **follow_up**: follow-up visit dates

### `advanced_features_v2` (view)

Comprehensive analytic view joining `master_cohort` with all Phase 6 tables
plus existing tumor_pathology and benign_pathology. Includes data availability
flags for every domain.
"""
    if md_path.exists():
        current = md_path.read_text()
        if "Phase 6" not in current:
            md_path.write_text(current + md_section)
            log.info("  Appended Phase 6 section to data_dictionary.md")
    else:
        md_path.write_text(md_section)


# ═══════════════════════════════════════════════════════════════════
#  ETL STUB
# ═══════════════════════════════════════════════════════════════════

ETL_STUB = '''#!/usr/bin/env python3
"""
08_integrate_missing_sources.py — Phase 6 ETL entry point

Wrapper that calls the master integration script.
Fits into the existing 01-07 pipeline sequence.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def etl_missing_sources() -> None:
    """Run the Phase 6 integration of 8 missing high-value Excel sources."""
    script = ROOT / "integrate_missing_sources.py"
    if not script.exists():
        print(f"ERROR: {script} not found")
        sys.exit(1)
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ROOT),
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    etl_missing_sources()
'''


def write_etl_stub() -> None:
    stub_path = ROOT / "scripts" / "08_integrate_missing_sources.py"
    stub_path.write_text(ETL_STUB)
    log.info(f"  Wrote ETL stub: {stub_path}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=" * 72)
    log.info("  THYROID LAKEHOUSE — PHASE 6: INTEGRATE 8 MISSING SOURCES")
    log.info("=" * 72)

    results: dict[str, dict] = {}

    steps = [
        ("complications",     process_complications),
        ("molecular_testing", process_molecular_testing),
        ("operative_details", process_operative_details),
        ("fna_history",       process_fna_history),
        ("us_nodules_tirads", process_us_nodules_tirads),
        ("serial_imaging_us", process_serial_imaging),
        ("path_synoptics",    process_path_synoptics),
        ("clinical_notes",    process_clinical_notes),
    ]

    for name, func in steps:
        try:
            df = func()
            path = save_parquet(df, name)
            results[name] = {"path": path, "rows": len(df), "cols": len(df.columns), "df": df}
        except Exception as exc:
            log.error(f"FAILED: {name} — {exc}", exc_info=True)

    # NLP extraction from clinical notes + complications
    if "clinical_notes" in results:
        try:
            events_df = build_extracted_events(results["clinical_notes"]["df"])
            # Also extract from complications laryngoscopy if available
            if "complications" in results:
                comp_df = results["complications"]["df"]
                laryng_col = [c for c in comp_df.columns if "raw" in c and "laryngoscopy" in c]
                if laryng_col:
                    log.info("  Also extracting events from complications laryngoscopy notes...")
                    extra = []
                    for _, row in comp_df.iterrows():
                        text = row.get(laryng_col[0])
                        if pd.notna(text) and str(text).strip():
                            extra.extend(
                                _extract_events_from_text(row["research_id"], str(text), "laryngoscopy_note")
                            )
                    if extra:
                        extra_df = pd.DataFrame(extra)
                        for col in events_df.columns:
                            if col not in extra_df.columns:
                                extra_df[col] = pd.NA
                        extra_df = extra_df[events_df.columns].astype(events_df.dtypes)
                        events_df = pd.concat([events_df, extra_df], ignore_index=True)

            path = save_parquet(events_df, "extracted_clinical_events")
            results["extracted_clinical_events"] = {
                "path": path, "rows": len(events_df),
                "cols": len(events_df.columns), "df": events_df,
            }
        except Exception as exc:
            log.error(f"FAILED: NLP extraction — {exc}", exc_info=True)

    # Outputs
    generate_integration_report(results)
    update_data_dictionary(results)
    write_etl_stub()
    build_advanced_features_v2_view(results)

    # Summary
    log.info("")
    log.info("=" * 72)
    log.info("  INTEGRATION COMPLETE")
    log.info("=" * 72)
    for name, info in results.items():
        log.info(f"  {name:35s} {info['rows']:>8,} rows x {info['cols']:>3} cols")

    log.info("")
    log.info("  Next steps:")
    log.info("    1. dvc add processed/*.parquet   (version new files)")
    log.info("    2. python scripts/02_build_duckdb_full.py   (rebuild DuckDB)")
    log.info("    3. python scripts/03_research_views.py      (rebuild views)")
    log.info("=" * 72)

    if any(name not in results for name, _ in steps):
        sys.exit(1)


if __name__ == "__main__":
    main()
