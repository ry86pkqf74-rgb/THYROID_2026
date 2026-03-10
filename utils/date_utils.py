"""
Higher-level date resolution utilities for the THYROID_2026 pipeline.

Builds on the low-level parsers in ``utils.text_helpers`` (``safe_parse_date``,
``extract_note_date``, ``extract_nearby_date``) to provide unified date
resolution, confidence scoring, and temporal offset calculation used by the
enriched-entity views and downstream analysis cohorts.

Date-status taxonomy (mirrors enriched_note_entities_* views):
    exact_source_date        – entity_date present, confidence 100
    inferred_day_level_date  – note_date used, confidence 70
    coarse_anchor_date       – surgery/FNA/molecular fallback, confidence 35-60
    unresolved_date          – no usable source, confidence 0
"""

from __future__ import annotations

import logging
from datetime import datetime

from utils.text_helpers import safe_parse_date

log = logging.getLogger(__name__)

DATE_STATUS_CONFIDENCE: dict[str, int] = {
    "exact_source_date": 100,
    "inferred_day_level_date": 70,
    "coarse_anchor_date": 50,
    "unresolved_date": 0,
}

_ANCHOR_CONFIDENCE: dict[str, int] = {
    "surgery": 60,
    "fna": 45,
    "molecular": 35,
}


def classify_date_status(
    native_date: str | None,
    inferred_date: str | None,
    anchor_date: str | None,
) -> str:
    """Apply the 4-level date-status taxonomy.

    Parameters
    ----------
    native_date:
        The entity's own date (entity_date).
    inferred_date:
        Day-level date inferred from the note (note_date).
    anchor_date:
        Coarse anchor (surgery / FNA / molecular fallback).

    Returns
    -------
    One of the four canonical status strings.
    """
    if native_date is not None:
        return "exact_source_date"
    if inferred_date is not None:
        return "inferred_day_level_date"
    if anchor_date is not None:
        return "coarse_anchor_date"
    return "unresolved_date"


def compute_date_confidence(date_status: str, source_count: int = 1) -> int:
    """Return a 0-100 confidence score for a resolved date.

    The base confidence comes from the date-status taxonomy.  When multiple
    independent sources agree (``source_count > 1``), coarse-anchor confidence
    is boosted toward the ceiling of its band (60) but never exceeds it.

    Parameters
    ----------
    date_status:
        One of the four taxonomy values.
    source_count:
        Number of independent sources that corroborate the date (>=1).

    Returns
    -------
    Integer confidence in [0, 100].
    """
    base = DATE_STATUS_CONFIDENCE.get(date_status, 0)
    if date_status == "coarse_anchor_date" and source_count > 1:
        boost = min(5 * (source_count - 1), 10)
        return min(base + boost, 60)
    return base


def resolve_event_date(
    native_dates: list[str | None],
    anchor_dates: dict[str, str | None],
    context: dict[str, str | None] | None = None,
) -> tuple[str | None, str, int]:
    """Pick the best date from available sources.

    Resolution order mirrors the enriched-view logic:
      1. Any non-null native (entity) date  →  ``exact_source_date``
      2. Inferred day-level date from context (``note_date`` key)
         →  ``inferred_day_level_date``
      3. Best anchor (surgery > FNA > molecular)  →  ``coarse_anchor_date``
      4. Nothing available  →  ``unresolved_date``

    Parameters
    ----------
    native_dates:
        Entity-level dates (may contain Nones).
    anchor_dates:
        Mapping of anchor type (``"surgery"``, ``"fna"``, ``"molecular"``)
        to ISO date string or None.
    context:
        Optional dict; ``context["note_date"]`` is used as the inferred
        day-level fallback.

    Returns
    -------
    ``(resolved_date, date_status, confidence)``
    """
    context = context or {}

    for nd in native_dates:
        if nd is not None:
            return nd, "exact_source_date", 100

    inferred = context.get("note_date")
    if inferred is not None:
        return inferred, "inferred_day_level_date", 70

    for anchor_type in ("surgery", "fna", "molecular"):
        ad = anchor_dates.get(anchor_type)
        if ad is not None:
            conf = _ANCHOR_CONFIDENCE.get(anchor_type, 35)
            return ad, "coarse_anchor_date", conf

    return None, "unresolved_date", 0


def compute_temporal_offset(
    event_date: str | None,
    surgery_date: str | None,
) -> int | None:
    """Calculate the number of days between an event and surgery.

    A negative value means the event occurred *before* surgery; positive means
    after.  Returns ``None`` if either date is missing or unparseable.
    """
    if event_date is None or surgery_date is None:
        return None
    try:
        ev = datetime.strptime(event_date, "%Y-%m-%d")
        sx = datetime.strptime(surgery_date, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    return (ev - sx).days


def parse_date_safe(val) -> tuple[str | None, str]:
    """Wrapper around ``safe_parse_date`` that also returns a date status.

    Returns
    -------
    ``(date_str, date_status)`` where *date_status* is
    ``"exact_source_date"`` when parsing succeeds or
    ``"unresolved_date"`` when it does not.
    """
    parsed = safe_parse_date(val)
    if parsed is not None:
        return parsed, "exact_source_date"
    return None, "unresolved_date"


def find_best_anchor(
    research_id: int,
    surgery_dates: list[str | None],
    fna_dates: list[str | None],
    molecular_dates: list[str | None],
) -> tuple[str | None, str | None, int]:
    """Find the best anchor date for a patient.

    Priority: earliest valid surgery date > earliest FNA > earliest molecular.

    Parameters
    ----------
    research_id:
        Patient identifier (used only for logging).
    surgery_dates:
        Candidate surgery dates (ISO strings or Nones).
    fna_dates:
        Candidate FNA dates.
    molecular_dates:
        Candidate molecular-report dates.

    Returns
    -------
    ``(anchor_date, anchor_type, confidence)`` where *anchor_type* is
    ``"surgery"``, ``"fna"``, ``"molecular"``, or ``None`` if no anchor
    could be determined.
    """
    for label, candidates, conf in (
        ("surgery", surgery_dates, 60),
        ("fna", fna_dates, 45),
        ("molecular", molecular_dates, 35),
    ):
        valid: list[str] = []
        for d in candidates:
            parsed = safe_parse_date(d)
            if parsed is not None:
                valid.append(parsed)
        if valid:
            best = min(valid)
            log.debug(
                "research_id=%d  anchor=%s  date=%s  (from %d candidate(s))",
                research_id, label, best, len(valid),
            )
            return best, label, conf

    log.debug("research_id=%d  no anchor date found", research_id)
    return None, None, 0
