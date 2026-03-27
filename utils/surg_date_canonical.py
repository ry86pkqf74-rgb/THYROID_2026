"""
Canonical surgery-date resolution for synoptic / path_synoptics rows.

Excel and local DuckDB may store `surg_date` as VARCHAR with leading whitespace or
US-style M/D/YYYY strings that do not cast with TRY_CAST alone. Episode linkage,
Excel reconciliation, and pathology–surgery joins should use the same resolution
chain so each synoptic row maps to the correct surgical encounter key.

DuckDB: use `surgery_date_canonical_sql("surg_date")` in SELECT lists.
Python/pandas: use `canonical_surgery_date_series(series)`.
"""

from __future__ import annotations

import pandas as pd


def surgery_date_canonical_sql(col: str = "surg_date") -> str:
    """Return DuckDB expression (no trailing alias) for calendar DATE or NULL."""
    c = col.strip()
    v = f"TRIM(CAST({c} AS VARCHAR))"
    return f"""COALESCE(
  TRY_CAST({c} AS DATE),
  TRY_CAST({v} AS DATE),
  TRY_STRPTIME({v}, '%m/%d/%Y')::DATE,
  TRY_STRPTIME({v}, '%m/%d/%y')::DATE,
  TRY_STRPTIME({v}, '%Y-%m-%d')::DATE
)"""


def surgery_date_parse_tier_sql(col: str = "surg_date") -> str:
    """Return DuckDB CASE expression for how canonical date was derived."""
    c = col.strip()
    v = f"TRIM(CAST({c} AS VARCHAR))"
    return f"""CASE
  WHEN TRY_CAST({c} AS DATE) IS NOT NULL THEN 'native_cast'
  WHEN TRY_CAST({v} AS DATE) IS NOT NULL THEN 'trim_cast'
  WHEN TRY_STRPTIME({v}, '%m/%d/%Y') IS NOT NULL THEN 'us_mdy_4digit'
  WHEN TRY_STRPTIME({v}, '%m/%d/%y') IS NOT NULL THEN 'us_mdy_2digit'
  WHEN TRY_STRPTIME({v}, '%Y-%m-%d') IS NOT NULL THEN 'iso_after_trim'
  WHEN LENGTH(COALESCE({v}, '')) = 0 THEN 'empty'
  ELSE 'unresolved'
END"""


def canonical_surgery_date_series(s: pd.Series) -> pd.Series:
    """Pandas equivalent: normalized day (ns UTC removed → date) or NaT."""
    s_str = s.map(
        lambda x: (
            ""
            if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x)
            else str(x).strip()
        )
    )
    # Mixed ISO timestamps + US m/d/y in the same column (synoptic Excel):
    # vectorized parse without format= can yield NaT for m/d/y when batched with
    # YYYY-MM-DD strings (pandas 2.x).
    return pd.to_datetime(
        s_str, errors="coerce", utc=False, format="mixed"
    ).dt.normalize()


def canonical_surgery_date_key(s: pd.Series) -> pd.Series:
    """Return python date or None per row (for merges keys)."""
    dt = canonical_surgery_date_series(s)

    def _d(x: object):
        if pd.isna(x):
            return None
        if hasattr(x, "date"):
            return x.date()
        return None

    return dt.map(_d)
