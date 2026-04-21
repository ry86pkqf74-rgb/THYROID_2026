"""
scripts/_lab_value_normalizer.py
================================

Pure-Python normalization helper for canonical lab tables (Script 347).

Public API
----------
    normalize_lab_value(value_raw: str, lab_test_name: str)
        -> tuple[float | None, bool, str | None]

    Returns: (value_numeric, is_censored, value_correction_note)

The pipeline is uniform across all six analytes:
    thyroglobulin, anti_thyroglobulin, tsh, pth, calcium, vitamin_d

Stages (applied in order):
    2A. String cleanup (unit-suffix strip, lab-flag strip, whitespace collapse).
    2B. Censor detection ('<', '>', 'less than', 'greater than',
        'goal <', 'goal less than'). Sets is_censored + censor threshold.
    2C. Titer format (TgAb only, e.g. '1:25600' -> 25600).
    2D. Numeric parse from the cleaned string (or threshold if censored).
    2E. Per-analyte plausibility correction:
        - in plausible range -> keep
        - out-of-range but in OOR band -> try /10 then /100; first hit wins.
        - beyond NULL threshold -> None, 'nulled_unrecoverable_implausible'.
        - negative (non-censored) -> None, 'nulled_negative'.
        - zero for pth/calcium/vitamin_d -> None, 'nulled_zero_implausible'.
        - censored values bypass plausibility correction.
    2F. Return (value_numeric, is_censored, comma-joined correction notes).

The function has NO database dependency and is fully unit-testable.
Helper logic must NEVER be inlined as SQL CASE statements; the build script
applies it via a DataFrame pass.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Per-analyte plausibility ranges (PLAUSIBLE, OOR_HIGH, NULL_BEYOND).
# Lower bound of OOR is the upper bound of PLAUSIBLE; lower-bound semantics
# below.
# ---------------------------------------------------------------------------

_PLAUSIBLE_RANGES: dict[str, tuple[float, float, float]] = {
    # name              (plausible_max, oor_max,    null_beyond)
    "thyroglobulin":     (10000.0,      1_000_000.0, 1_000_000.0),
    "anti_thyroglobulin": (40000.0,     1_000_000.0, 1_000_000.0),
    "tsh":               (150.0,        15000.0,     15000.0),
    "pth":               (3000.0,       300000.0,    300000.0),
    "calcium":           (20.0,         2000.0,      2000.0),
    "vitamin_d":         (200.0,        20000.0,     20000.0),
}

# Plausible MIN per analyte (inclusive).
_PLAUSIBLE_MIN: dict[str, float] = {
    "thyroglobulin":      0.0,
    "anti_thyroglobulin": 0.0,
    "tsh":                0.0,
    "pth":                0.0,
    "calcium":            4.0,
    "vitamin_d":          0.0,
}

# Analytes where exact zero is biologically meaningful (undetectable).
_ZERO_PERMITTED: set[str] = {"tsh", "thyroglobulin", "anti_thyroglobulin"}


# ---------------------------------------------------------------------------
# 2A — string cleanup
# ---------------------------------------------------------------------------

# Order longer suffixes first so 'mIU/ML' matches before 'IU/mL'.
_UNIT_SUFFIXES: list[str] = [
    " mIU/ML", " mIU/mL", " mIU/ml", " mIU/L",
    " uIU/mL", " uIU/ml", " mcIU/mL",
    " IU/mL", " IU/ML",
    " ng/mL", " ng/ml",
    " pg/mL", " pg/ml",
    " mg/dL", " mg/dl",
    " mEq/L",
    " pmol/L",
]

# Lab-flag suffix patterns (the regex must consume only the trailing flag).
_FLAG_SUFFIXES: list[str] = [" (LL)", " (HH)", " (L)", " (H)"]
# Bare H/L only when preceded by a digit or '.'.
_BARE_FLAG_RE = re.compile(r"([0-9.])\s+([LH])\s*$")


def _string_cleanup(raw: str) -> tuple[str, bool]:
    """Return (cleaned, anything_stripped)."""
    if raw is None:
        return "", False
    s = str(raw).strip()
    stripped_anything = False

    # Strip flag suffixes (parens form).
    for f in _FLAG_SUFFIXES:
        if s.lower().endswith(f.lower()):
            s = s[: -len(f)].rstrip()
            stripped_anything = True
    # Strip bare trailing H or L (only if preceded by digit/.).
    m = _BARE_FLAG_RE.search(s)
    if m:
        s = s[: m.start(2)].rstrip()
        stripped_anything = True

    # Strip unit suffixes (case-insensitive). Loop until no suffix matches.
    while True:
        matched = False
        sl = s.lower()
        for u in _UNIT_SUFFIXES:
            if sl.endswith(u.lower()):
                s = s[: -len(u)].rstrip()
                stripped_anything = True
                matched = True
                break
        if not matched:
            break

    # Collapse internal whitespace.
    s2 = re.sub(r"\s+", " ", s).strip()
    if s2 != s:
        stripped_anything = True
    return s2, stripped_anything


# ---------------------------------------------------------------------------
# 2B — censor detection
# ---------------------------------------------------------------------------

_CENSOR_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\s*goal\s+less\s+than\s+(-?\d+\.?\d*)", re.IGNORECASE),
    re.compile(r"^\s*goal\s*<\s*(-?\d+\.?\d*)",            re.IGNORECASE),
    re.compile(r"^\s*less\s+than\s+(-?\d+\.?\d*)",          re.IGNORECASE),
    re.compile(r"^\s*greater\s+than\s+(-?\d+\.?\d*)",       re.IGNORECASE),
    re.compile(r"^\s*<\s*(-?\d+\.?\d*)"),
    re.compile(r"^\s*>\s*(-?\d+\.?\d*)"),
]


def _detect_censor(s: str) -> tuple[bool, Optional[float]]:
    for rx in _CENSOR_PATTERNS:
        m = rx.match(s)
        if m:
            try:
                return True, float(m.group(1))
            except ValueError:
                return True, None
    return False, None


# ---------------------------------------------------------------------------
# 2C — TgAb titer
# ---------------------------------------------------------------------------

_TITER_RE = re.compile(r"^\s*1\s*:\s*(\d+)\s*$")


def _parse_titer(s: str) -> Optional[float]:
    m = _TITER_RE.match(s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# 2D — numeric parse
# ---------------------------------------------------------------------------

_NUMERIC_RE = re.compile(r"(-?\d+\.?\d*(?:[eE][-+]?\d+)?)")


def _parse_first_number(s: str) -> Optional[float]:
    m = _NUMERIC_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 2E — plausibility correction
# ---------------------------------------------------------------------------

def _in_plausible(name: str, v: float) -> bool:
    pmin = _PLAUSIBLE_MIN[name]
    pmax = _PLAUSIBLE_RANGES[name][0]
    return pmin <= v <= pmax


def _within_oor(name: str, v: float) -> bool:
    pmax = _PLAUSIBLE_RANGES[name][0]
    oor_max = _PLAUSIBLE_RANGES[name][1]
    return pmax < v <= oor_max


def _correct_plausibility(name: str, v: float) -> tuple[Optional[float], Optional[str]]:
    """Return (corrected_value_or_None, note_or_None)."""
    if _in_plausible(name, v):
        return v, None
    if v < 0:
        return None, "nulled_negative"
    # Try /10 and /100 only if the original is in OOR band or above.
    pmax = _PLAUSIBLE_RANGES[name][0]
    if v > pmax:
        for div, label in ((10.0, "divided_by_10"),
                           (100.0, "divided_by_100")):
            cand = v / div
            if _in_plausible(name, cand):
                return cand, label
    # If v is below the analyte's plausible MIN (but not negative or zero),
    # we leave it as unrecoverable.
    return None, "nulled_unrecoverable_implausible"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Map common synonyms used across pipelines to canonical analyte keys.
_CANONICAL_ANALYTE: dict[str, str] = {
    "tg":                 "thyroglobulin",
    "thyroglobulin":      "thyroglobulin",
    "tgab":               "anti_thyroglobulin",
    "anti_thyroglobulin": "anti_thyroglobulin",
    "tg_antibody":        "anti_thyroglobulin",
    "tsh":                "tsh",
    "pth":                "pth",
    "calcium":            "calcium",
    "ca":                 "calcium",
    "vitamin_d":          "vitamin_d",
    "vitd":               "vitamin_d",
    "25_oh_vit_d":        "vitamin_d",
}


def _canonical_key(lab_test_name: Optional[str]) -> Optional[str]:
    if lab_test_name is None:
        return None
    k = str(lab_test_name).strip().lower()
    return _CANONICAL_ANALYTE.get(k)


def normalize_lab_value(
    value_raw: Optional[str],
    lab_test_name: Optional[str],
) -> Tuple[Optional[float], bool, Optional[str]]:
    """Normalize a raw lab string to (value_numeric, is_censored, note).

    Notes are comma-separated tags (in pipeline order). ``None`` if no
    transformation was applied.
    """
    notes: list[str] = []
    name = _canonical_key(lab_test_name)

    if value_raw is None or (isinstance(value_raw, float) and value_raw != value_raw):
        return None, False, "unparseable_string"

    raw_str = str(value_raw)
    if raw_str.strip() == "":
        return None, False, "unparseable_string"

    # 2A
    cleaned, stripped = _string_cleanup(raw_str)
    if stripped:
        notes.append("unit_suffix_stripped")

    # 2B
    is_censored, censor_threshold = _detect_censor(cleaned)

    # 2C
    titer_value: Optional[float] = None
    if not is_censored and name == "anti_thyroglobulin":
        titer_value = _parse_titer(cleaned)
        if titer_value is not None:
            notes.append("titer_denominator_extracted")

    # 2D
    if is_censored:
        parsed = censor_threshold
    elif titer_value is not None:
        parsed = titer_value
    else:
        parsed = _parse_first_number(cleaned)
        if parsed is None:
            notes.append("unparseable_string")
            return None, is_censored, ",".join(notes) or None

    if parsed is None:
        notes.append("unparseable_string")
        return None, is_censored, ",".join(notes) or None

    # 2E
    if is_censored:
        # Censored values: skip plausibility correction. If the analyte is
        # unknown, just return the parsed threshold as-is.
        return parsed, True, ",".join(notes) or None

    if name is None:
        # Unknown analyte: return parsed number with no plausibility check.
        return parsed, False, ",".join(notes) or None

    # Negative (non-censored) -> None.
    if parsed < 0:
        notes.append("nulled_negative")
        return None, False, ",".join(notes)

    # Zero handling.
    if parsed == 0.0:
        if name in _ZERO_PERMITTED:
            return 0.0, False, ",".join(notes) or None
        notes.append("nulled_zero_implausible")
        return None, False, ",".join(notes)

    corrected, corr_note = _correct_plausibility(name, parsed)
    if corr_note is not None:
        notes.append(corr_note)
    if corrected is None:
        return None, False, ",".join(notes) or None
    return corrected, False, ",".join(notes) or None


# ---------------------------------------------------------------------------
# Convenience: unit conversion notes for the build pipeline.
# ---------------------------------------------------------------------------

# Canonical units per analyte.
CANONICAL_UNIT: dict[str, str] = {
    "thyroglobulin":      "ng/mL",
    "anti_thyroglobulin": "IU/mL",
    "tsh":                "mIU/L",
    "pth":                "pg/mL",
    "calcium":            "mg/dL",
    "vitamin_d":          "ng/mL",
}

# Conversions from a recognized source unit to the canonical unit.
# Each entry: (analyte, source_unit_lower) -> (factor, canonical_unit_str).
_UNIT_CONVERSION_FACTORS: dict[tuple[str, str], tuple[float, str]] = {
    # Tg / TgAb / TSH / VitD often expressed in equivalent units; identity
    # mappings make the backfill safe.
    ("thyroglobulin",       "ng/ml"):     (1.0,    "ng/mL"),
    ("thyroglobulin",       "ng/dl"):     (0.01,   "ng/mL"),  # 1 ng/dL = 0.01 ng/mL
    ("anti_thyroglobulin",  "iu/ml"):     (1.0,    "IU/mL"),
    ("anti_thyroglobulin",  "ku/l"):      (1.0,    "IU/mL"),  # 1 kU/L = 1 IU/mL
    ("tsh",                 "miu/l"):     (1.0,    "mIU/L"),
    ("tsh",                 "miu/ml"):    (1000.0, "mIU/L"),  # 1 mIU/mL = 1000 mIU/L
    ("tsh",                 "uiu/ml"):    (1.0,    "mIU/L"),  # 1 uIU/mL == 1 mIU/L
    ("tsh",                 "mciu/ml"):   (1.0,    "mIU/L"),
    ("pth",                 "pg/ml"):     (1.0,    "pg/mL"),
    ("calcium",             "mg/dl"):     (1.0,    "mg/dL"),
    ("vitamin_d",           "ng/ml"):     (1.0,    "ng/mL"),
}


def convert_to_canonical_unit(
    value_numeric: Optional[float],
    source_unit: Optional[str],
    lab_test_name: Optional[str],
) -> Tuple[Optional[float], str, Optional[str]]:
    """Return (converted_value, canonical_unit, conversion_note).

    - If ``source_unit`` is None or matches the canonical unit (case-insensitive),
      no conversion is applied; conversion_note is None.
    - If a known conversion exists, apply the factor and emit a note like
      ``unit_converted_<from>_<to>``.
    - If ``source_unit`` is a non-empty string that we do NOT recognize for
      this analyte, raises ``ValueError`` so the build aborts and surfaces
      the row to manual review.
    """
    name = _canonical_key(lab_test_name)
    if name is None:
        # Unknown analyte: no canonical unit to apply.
        return value_numeric, str(source_unit or ""), None
    canonical = CANONICAL_UNIT[name]
    # Treat NULL / empty / 'nan' / 'none' as "unit unknown" -> no conversion,
    # value retained, canonical unit backfilled (this is the deterministic
    # Emory Core Lab default per analyte).
    if source_unit is None or (
        isinstance(source_unit, float) and source_unit != source_unit
    ):
        return value_numeric, canonical, None
    src = str(source_unit).strip()
    if src == "" or src.lower() in {"nan", "none", "null"}:
        return value_numeric, canonical, None
    if src.lower() == canonical.lower():
        return value_numeric, canonical, None
    key = (name, src.lower())
    if key in _UNIT_CONVERSION_FACTORS:
        factor, target = _UNIT_CONVERSION_FACTORS[key]
        new_v = value_numeric * factor if value_numeric is not None else None
        note = f"unit_converted_{src}_{target}"
        return new_v, target, note
    raise ValueError(
        f"Unrecognized source unit {src!r} for analyte {name!r}; "
        f"manual review required (see studies/lab_consolidation_20260421/"
        f"discordance_review.md)."
    )
