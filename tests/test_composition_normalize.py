"""
Unit tests for scripts.lib.composition_normalize.

Validates the Logan-approved normalization map (Path A patch, 2026-05-08)
covering every Phase A.3 vocabulary value, ACR composition points, and the
ambiguity warning flag for bare "mixed".
"""

from __future__ import annotations

import pytest

from scripts.lib.composition_normalize import (
    ACR_CYSTIC,
    ACR_MIXED_CYSTIC_SOLID,
    ACR_SOLID,
    ACR_SPONGIFORM,
    COMPOSITION_NORM_MAP,
    acr_composition_points,
    composition_normalization_warning,
    normalize_composition_acr,
)


# ---------------------------------------------------------------------------
# Coverage of every Phase A.3 vocabulary value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected_class"),
    [
        ("cystic", ACR_CYSTIC),
        ("predominantly_cystic", ACR_CYSTIC),
        ("anechoic", ACR_CYSTIC),
        ("spongiform", ACR_SPONGIFORM),
        ("mixed cystic and solid", ACR_MIXED_CYSTIC_SOLID),
        ("mixed_cystic_solid", ACR_MIXED_CYSTIC_SOLID),
        ("mixed", ACR_MIXED_CYSTIC_SOLID),
        ("predominantly_solid", ACR_SOLID),
        ("solid", ACR_SOLID),
    ],
)
def test_normalize_composition_acr_known_inputs(raw, expected_class):
    assert normalize_composition_acr(raw) == expected_class


@pytest.mark.parametrize(
    ("raw", "expected_pts"),
    [
        ("cystic", 0),
        ("predominantly_cystic", 0),
        ("anechoic", 0),
        ("spongiform", 0),
        ("mixed cystic and solid", 1),
        ("mixed_cystic_solid", 1),
        ("mixed", 1),
        ("predominantly_solid", 2),
        ("solid", 2),
    ],
)
def test_acr_composition_points_known_inputs(raw, expected_pts):
    assert acr_composition_points(raw) == expected_pts


# ---------------------------------------------------------------------------
# Null / unrecognized handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [None, "", "   ", "garbage", "PARTIALLY_SOMETHING", "n/a"])
def test_normalize_composition_acr_unknown_returns_none(bad):
    assert normalize_composition_acr(bad) is None


@pytest.mark.parametrize("bad", [None, "", "   ", "garbage"])
def test_acr_composition_points_unknown_returns_none(bad):
    assert acr_composition_points(bad) is None


# ---------------------------------------------------------------------------
# Case + whitespace robustness
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected_class"),
    [
        ("CYSTIC", ACR_CYSTIC),
        ("  Cystic  ", ACR_CYSTIC),
        ("Mixed Cystic And Solid", ACR_MIXED_CYSTIC_SOLID),
        ("SPONGIFORM", ACR_SPONGIFORM),
    ],
)
def test_case_and_whitespace_normalized(raw, expected_class):
    assert normalize_composition_acr(raw) == expected_class


# ---------------------------------------------------------------------------
# Warning flag
# ---------------------------------------------------------------------------


def test_warning_only_for_bare_mixed():
    assert composition_normalization_warning("mixed") is True


@pytest.mark.parametrize(
    "raw",
    [
        "mixed cystic and solid",
        "mixed_cystic_solid",
        "cystic",
        "predominantly_cystic",
        "anechoic",
        "spongiform",
        "predominantly_solid",
        "solid",
        None,
        "",
        "garbage",
    ],
)
def test_warning_false_for_non_ambiguous_inputs(raw):
    assert composition_normalization_warning(raw) is False


# ---------------------------------------------------------------------------
# Map surface integrity
# ---------------------------------------------------------------------------


def test_norm_map_covers_expected_vocabulary():
    expected_keys = {
        "cystic",
        "predominantly_cystic",
        "anechoic",
        "spongiform",
        "mixed cystic and solid",
        "mixed_cystic_solid",
        "mixed",
        "predominantly_solid",
        "solid",
    }
    assert set(COMPOSITION_NORM_MAP.keys()) == expected_keys


def test_norm_map_values_are_canonical_classes():
    valid = {ACR_CYSTIC, ACR_SPONGIFORM, ACR_MIXED_CYSTIC_SOLID, ACR_SOLID}
    assert set(COMPOSITION_NORM_MAP.values()) <= valid
