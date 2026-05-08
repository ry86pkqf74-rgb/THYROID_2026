"""
Composition vocabulary normalization for TIRADS scorers.
==========================================================

Logan-approved normalization map (CURSOR_PROMPT_PHASE_E_PATCH_AND_RESUME_20260507
session, 2026-05-08). Maps the Phase A.3 / canonical_us_nodule_v2.composition
vocabulary to ACR-canonical composition classes and ACR composition-points.

Decisions baked in:

- ``anechoic`` → ``cystic``: anechoic content on US is by definition fluid-filled =
  pure cyst. ACR's "Cystic or almost completely cystic" covers this. Standard
  radiologist interpretation; not a stretch.
- ``mixed`` (no qualifier) → ``mixed_cystic_solid``: A bare "mixed" in a US report
  almost always means mixed cystic-and-solid. Flagged via
  ``composition_normalization_warning=True`` so downstream cohort docs can
  disclose the case-mix.
- ``predominantly_cystic`` → ``cystic`` for ACR (matches "almost completely cystic"
  → 0 points). Systems like K-TIRADS that distinguish "partially cystic" must
  branch on the *raw* value, not on this ACR-normalized class.
- ``predominantly_solid`` → ``solid`` for ACR (matches "almost completely solid" →
  2 points). Same caveat.

Both ``anechoic → cystic`` and ``mixed → mixed_cystic_solid`` are reasonable
but defensible decisions; the M085 cohort definition discloses them rather
than burying them in code.

Usage::

    from scripts.lib.composition_normalize import (
        normalize_composition_acr,
        acr_composition_points,
        composition_normalization_warning,
    )

    cls = normalize_composition_acr("mixed cystic and solid")  # → "mixed_cystic_solid"
    pts = acr_composition_points("anechoic")                    # → 0
    warn = composition_normalization_warning("mixed")           # → True
"""

from __future__ import annotations

from typing import Optional

# ---------------------------------------------------------------------------
# Canonical ACR composition classes
# ---------------------------------------------------------------------------

ACR_CYSTIC = "cystic"
ACR_SPONGIFORM = "spongiform"
ACR_MIXED_CYSTIC_SOLID = "mixed_cystic_solid"
ACR_SOLID = "solid"

# ---------------------------------------------------------------------------
# Phase A.3 / canonical_us_nodule_v2 vocabulary → ACR class
# ---------------------------------------------------------------------------

COMPOSITION_NORM_MAP: dict[str, str] = {
    # Cystic family
    "cystic": ACR_CYSTIC,
    "predominantly_cystic": ACR_CYSTIC,
    "anechoic": ACR_CYSTIC,
    # Spongiform
    "spongiform": ACR_SPONGIFORM,
    # Mixed family
    "mixed cystic and solid": ACR_MIXED_CYSTIC_SOLID,
    "mixed_cystic_solid": ACR_MIXED_CYSTIC_SOLID,
    "mixed": ACR_MIXED_CYSTIC_SOLID,
    # Solid family
    "predominantly_solid": ACR_SOLID,
    "solid": ACR_SOLID,
}

# Inputs that warrant a composition_normalization_warning=True flag because
# the raw value was ambiguous and was forced into a canonical class.
WARNING_INPUTS: frozenset[str] = frozenset({"mixed"})

# ACR composition-points lookup keyed by canonical ACR class
ACR_COMPOSITION_POINTS: dict[str, int] = {
    ACR_CYSTIC: 0,
    ACR_SPONGIFORM: 0,
    ACR_MIXED_CYSTIC_SOLID: 1,
    ACR_SOLID: 2,
}


def _coerce(value: Optional[str]) -> Optional[str]:
    """Lower-case and strip; return None for empty / None / whitespace-only."""
    if value is None:
        return None
    s = str(value).strip().lower()
    return s if s else None


def normalize_composition_acr(value: Optional[str]) -> Optional[str]:
    """
    Map a raw canonical_us_nodule_v2.composition value to the ACR canonical
    composition class.

    Returns one of: ``"cystic"``, ``"spongiform"``, ``"mixed_cystic_solid"``,
    ``"solid"``, or ``None`` if the input is null / unrecognized.
    """
    s = _coerce(value)
    if s is None:
        return None
    return COMPOSITION_NORM_MAP.get(s)


def acr_composition_points(value: Optional[str]) -> Optional[int]:
    """
    Return the ACR-2017 composition points (0/1/2) for a raw composition value,
    or ``None`` when the value is null / unrecognized.
    """
    cls = normalize_composition_acr(value)
    if cls is None:
        return None
    return ACR_COMPOSITION_POINTS[cls]


def composition_normalization_warning(value: Optional[str]) -> bool:
    """
    Return True iff the raw input was force-mapped through an ambiguity
    decision (currently: bare ``"mixed"`` → ``"mixed_cystic_solid"``).

    Returned in scorer output as ``composition_normalization_warning`` so
    downstream cohort docs can disclose the case-mix.
    """
    s = _coerce(value)
    if s is None:
        return False
    return s in WARNING_INPUTS


__all__ = [
    "ACR_CYSTIC",
    "ACR_SPONGIFORM",
    "ACR_MIXED_CYSTIC_SOLID",
    "ACR_SOLID",
    "COMPOSITION_NORM_MAP",
    "WARNING_INPUTS",
    "ACR_COMPOSITION_POINTS",
    "normalize_composition_acr",
    "acr_composition_points",
    "composition_normalization_warning",
]
