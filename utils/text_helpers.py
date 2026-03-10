"""
Shared text-processing helpers for the THYROID_2026 pipeline.

Extracted from integrate_missing_sources.py so that downstream scripts
(profiling, clinical_notes_long build, entity extraction) use the same
normalisation logic.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timedelta

import pandas as pd
from dateutil import parser as dateutil_parser

log = logging.getLogger(__name__)

RESEARCH_ID_ALIASES: set[str] = {
    "research_id",
    "research_id_number",
    "researchid",
    "record_id",
}

PHI_COLUMNS: set[str] = {
    "patient_first_nm",
    "patient_last_nm",
    "patient_id",
    "empi_nbr",
    "euh_mrn",
    "tec_mrn",
    "dob",
    "date_of_birth",
    "surgeon",
    "death",
    "patient_first_name",
    "patient_last_name",
}


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


_LEADING_DATE = re.compile(
    r"^\s*(\d{1,2}/\d{1,2}/\d{2,4})\b"
)
_SERVICE_DATE = re.compile(
    r"(?:date\s+of\s+service|service\s+date|admission\s+date|encounter\s+date"
    r"|date\s+of\s+visit|visit\s+date|procedure\s+date|surgery\s+date"
    r"|operative\s+date|date\s+of\s+procedure)\s*:?\s*"
    r"(\d{1,2}/\d{1,2}/\d{2,4})",
    re.IGNORECASE,
)
_HEADER_DATE = re.compile(
    r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b"
)

NOTE_DATE_SCAN_CHARS = 500


def extract_note_date(note_text: str) -> str | None:
    """Extract the most likely encounter/service date from a clinical note.

    Strategy (in priority order):
      1. Explicit "Date of Service: MM/DD/YYYY" label within the first 500 chars
      2. A date at the very start of the note (common pattern)
      3. First date found in the first 500 chars (heuristic fallback)

    Returns ISO YYYY-MM-DD or None.
    """
    if not note_text or len(note_text.strip()) < 4:
        return None

    header = note_text[:NOTE_DATE_SCAN_CHARS]

    m = _SERVICE_DATE.search(header)
    if m:
        return safe_parse_date(m.group(1))

    m = _LEADING_DATE.match(note_text)
    if m:
        return safe_parse_date(m.group(1))

    m = _HEADER_DATE.search(header)
    if m:
        parsed = safe_parse_date(m.group(1))
        if parsed:
            try:
                dt = datetime.strptime(parsed, "%Y-%m-%d")
                if 1990 <= dt.year <= 2030:
                    return parsed
            except ValueError:
                pass

    return None


_NEARBY_DATE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")


def extract_nearby_date(text: str, match_start: int, match_end: int,
                        window: int = 120) -> str | None:
    """Find the closest date within +-window chars of a regex match span.

    Returns ISO YYYY-MM-DD or None.
    """
    region_start = max(0, match_start - window)
    region_end = min(len(text), match_end + window)
    region = text[region_start:region_end]

    best: str | None = None
    best_dist = window + 1
    for m in _NEARBY_DATE.finditer(region):
        date_abs_start = region_start + m.start()
        dist = min(
            abs(date_abs_start - match_start),
            abs(date_abs_start - match_end),
        )
        if dist < best_dist:
            parsed = safe_parse_date(m.group(1))
            if parsed:
                try:
                    dt = datetime.strptime(parsed, "%Y-%m-%d")
                    if 1990 <= dt.year <= 2030:
                        best = parsed
                        best_dist = dist
                except ValueError:
                    pass
    return best


def to_snake_case(name: str) -> str:
    """Convert an arbitrary column name to snake_case."""
    clean = re.sub(r"[^\w]+", "_", str(name).strip()).lower().strip("_")
    return re.sub(r"_+", "_", clean)


def make_note_row_id(research_id: int | str, source_sheet: str, source_column: str) -> str:
    """Deterministic SHA-1 hash used as a stable row identifier."""
    key = f"{research_id}|{source_sheet}|{source_column}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def save_parquet(df: pd.DataFrame, out_path, *, coerce_object: bool = True) -> None:
    """Write DataFrame to Parquet.  Coerces mixed-type object columns to string."""
    from pathlib import Path

    df = df.copy()
    if coerce_object:
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(
                    lambda x: str(x) if pd.notna(x) and x is not None else None
                )
    out_path = Path(out_path)
    df.to_parquet(out_path, engine="pyarrow", index=False)
    size_mb = out_path.stat().st_size / (1024 * 1024)
    log.info(f"  => {out_path.name}  {len(df):>8,} rows x {len(df.columns):>3} cols  ({size_mb:.2f} MB)")
