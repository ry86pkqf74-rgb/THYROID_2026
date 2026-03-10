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
