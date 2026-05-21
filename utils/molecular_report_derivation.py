"""Shared molecular report derivation helpers.

These helpers intentionally prefer explicit report text/header signals over
source-call labels or loose warehouse casts. They are small enough to use from
ingest scripts and parser tests without importing the full pipeline.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

import pandas as pd

_MDY_TOKEN_RX = re.compile(r"\b(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{2,4})\b")


def parse_native_report_date(value: Any) -> date | None:
    """Parse an exact native report date without DuckDB YY/MM/DD transposition.

    DuckDB ``TRY_CAST('12/1/17' AS DATE)`` interprets the token as year 0012,
    which later migrations converted to 2012-01-17. Prefer explicit US MDY
    parsing for slash dates before falling back to pandas for ISO-like values.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None
    token = _MDY_TOKEN_RX.search(text)
    if token:
        month = int(token.group("m"))
        day = int(token.group("d"))
        year_raw = int(token.group("y"))
        if year_raw < 100:
            year = 2000 + year_raw if year_raw <= 26 else 1900 + year_raw
        else:
            year = year_raw
        try:
            return date(year, month, day)
        except ValueError:
            return None
    dt = pd.to_datetime(text, errors="coerce", dayfirst=False)
    if pd.notna(dt):
        return dt.date()
    return None


def derive_overall_result_class(
    *,
    test_result_summary: str | None = None,
    headline_text: str | None = None,
) -> str | None:
    """Derive canonical overall_result_class from summary first, then headline."""
    summary = (test_result_summary or "").upper().replace(" ", "_")
    if summary:
        if "CANCEL" in summary:
            return "cancelled"
        if "CURRENTLY_NEGATIVE" in summary or summary == "NEGATIVE":
            return "negative"
        if summary == "SUSPICIOUS":
            return "suspicious"
        if summary == "POSITIVE":
            return "positive"

    head = (headline_text or "")[:2500].upper()
    if not head:
        return None
    if re.search(r"CANCELLED|CANCELED|NOT PERFORMED|TEST NOT PERFORMED", head):
        return "cancelled"
    if re.search(r"NON[- ]?DIAGNOSTIC|INSUFFICIENT|INADEQUATE|QUANTITY NOT SUFFICIENT", head):
        return "non_diagnostic"
    if re.search(
        r"CURRENTLY[\s_]*NEGATIVE|PERFORMED ANALYSIS WAS NEGATIVE|"
        r"NEGATIVE FOR ALL TESTED|TEST RESULT[\s\S]{0,120}\bNEGATIVE\b",
        head,
    ):
        return "negative"
    if re.search(r"AFIRMA[\s\S]{0,220}SUSPICIOUS|GENOMIC SEQUENCING CLASSIFIER[\s\S]{0,220}SUSPICIOUS|\bSUSPICIOUS\b", head):
        return "suspicious"
    if re.search(
        r"TEST RESULT[\s\S]{0,120}\bPOSITIVE\b|"
        r"PROBABILITY OF CANCER[\s\S]{0,120}\bPOSITIVE\b|"
        r"\bPOSITIVE\b[\s\S]{0,80}(INTERMEDIATE|HIGH|NIFTP|CANCER)|"
        r"FUSION (WAS )?IDENTIFIED|MUTATION (WAS )?IDENTIFIED|"
        r"(\bBRAF\b|\bRAS\b|\bHRAS\b|\bNRAS\b|\bKRAS\b|\bPAX8\b)[\s\S]{0,80}(MUTATION|FUSION)",
        head,
    ):
        return "positive"
    if re.search(r"\bBENIGN\b|NO MUTATIONS? DETECTED|NO GENOMIC ALTERATIONS? DETECTED", head):
        return "negative"
    return None


def derive_platform_from_report_header(
    report_text: str | None,
    *,
    fallback_platform: str | None = None,
    fallback_version: int | str | None = None,
) -> tuple[str | None, int | None]:
    """Derive molecular platform and version from report header/product text."""
    header = (report_text or "")[:2500].upper()
    platform: str | None = None
    if re.search(r"AFIRMA THYROID|AFIRMA GENOMIC|AFIRMA GSC|AFIRMA GEC|VERACYTE|XPRESSION ATLAS|GENOMIC SEQUENCING CLASSIFIER", header):
        platform = "Afirma"
    elif re.search(r"THYROSEQ|UPMC", header):
        platform = "ThyroSeq"
    elif re.search(r"QUEST|MAYO|FOUNDATIONONE|TEMPUS|CARIS", header):
        platform = "Other"
    else:
        platform = fallback_platform

    version: int | None = None
    if platform == "ThyroSeq":
        if re.search(r"THYROSEQ[^\n]{0,30}V\s*3|THYROSEQ\s*V?3|\(THYROSEQ V3\)", header):
            version = 3
        elif re.search(r"THYROSEQ[^\n]{0,30}V\s*2|THYROSEQ\s*V?2|\(THYROSEQ V2\)", header):
            version = 2
    if version is None and fallback_version not in (None, ""):
        try:
            version = int(fallback_version)
        except (TypeError, ValueError):
            version = None
    return platform, version
