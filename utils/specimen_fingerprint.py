"""Canonical specimen fingerprint helpers (must stay aligned with SQL in 138 DDL).

Used for tests and documentation of the exact keying policy. Production materialization
computes the same SHA-256 over normalized pipe-separated fields in DuckDB.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any


def _norm_empty(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("", "nan", "none", "null"):
        return ""
    return s


def _norm_day(v: Any) -> str:
    """Day-grain date as YYYY-MM-DD when parseable, else normalized string or empty."""
    if v is None:
        return ""
    if isinstance(v, float) and str(v).lower() == "nan":
        return ""
    if str(v).strip() in ("", "nan"):
        return ""
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:
            pass
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    # ISO
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return _norm_empty(s)


def specimen_master_fingerprint_input(
    *,
    research_id: Any,
    source_system: str,
    procedure_date_day: Any,
    accession_or_source_id: Any,
    specimen_role: str,
    anatomic_site: Any,
    laterality: Any = "",
    surgery_episode_id: Any,
    encounter_synoptic_row_ix: Any,
) -> str:
    """Return pipe-separated normalized payload (before SHA-256).

    Encounter-level masters use laterality="" (tumor site lives on focus rows).
    """
    rid = _norm_empty(research_id)
    parts = [
        rid,
        _norm_empty(source_system),
        _norm_day(procedure_date_day),
        _norm_empty(accession_or_source_id),
        _norm_empty(specimen_role),
        _norm_empty(anatomic_site),
        _norm_empty(laterality),
        _norm_empty(surgery_episode_id),
        _norm_empty(encounter_synoptic_row_ix),
    ]
    return "|".join(parts)


def specimen_master_fingerprint_sha256(**kwargs: Any) -> str:
    payload = specimen_master_fingerprint_input(**kwargs)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def tumor_focus_fingerprint_input(
    *,
    master_fingerprint_sha256: str,
    synoptic_row_ix: Any,
    tumor_index: Any,
    site_text: Any,
    histologic_type: Any,
) -> str:
    base = _norm_empty(master_fingerprint_sha256)
    parts = [
        base,
        _norm_empty(synoptic_row_ix),
        _norm_empty(tumor_index),
        _norm_empty(site_text),
        _norm_empty(histologic_type),
    ]
    return "|".join(parts)


def tumor_focus_fingerprint_sha256(**kwargs: Any) -> str:
    return hashlib.sha256(tumor_focus_fingerprint_input(**kwargs).encode("utf-8")).hexdigest()
