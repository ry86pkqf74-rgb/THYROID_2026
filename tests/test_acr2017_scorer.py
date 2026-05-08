"""
Unit tests for scripts/418_canonical_us_nodule_tirads_acr2017_v1.py — score_acr_dual.

Covers (≥25 cases):
- Each composition × echogenicity × shape × margin × foci permutation that
  hits a category boundary
- The TR1/TR2 ambiguous case (total = 1) → category=TR2, band_ambiguous=TRUE
- Strict-NULL behavior when foci is NULL (4-core present)
- Imputed default (foci null → 0 pts, but category still set)
- FNA threshold edges at 0.5 / 1.0 / 1.5 / 2.5 cm
- composition_normalization_warning fires only on bare "mixed"
- Negative cases (composition or echogenicity null → both branches NULL)
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Load the digit-prefixed scorer module by path
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "418_canonical_us_nodule_tirads_acr2017_v1.py"
)
spec = importlib.util.spec_from_file_location("acr2017_scorer", SCRIPT_PATH)
acr_mod = importlib.util.module_from_spec(spec)
sys.modules["acr2017_scorer"] = acr_mod
assert spec and spec.loader
spec.loader.exec_module(acr_mod)

score_acr_dual = acr_mod.score_acr_dual


def _row(**kwargs):
    """Construct a row dict with sensible defaults; override fields by kwarg."""
    base = {
        "composition": None,
        "echogenicity": None,
        "shape": None,
        "margins": None,
        "echogenic_foci": None,
        "size_cm_max": None,
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Category boundary cases
# ---------------------------------------------------------------------------


def test_pure_cyst_zero_points_is_TR1():
    r = score_acr_dual(_row(
        composition="cystic", echogenicity="anechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 0.0
    assert r["acr2017_category_strict"] == "TR1"
    assert r["acr2017_band_ambiguous"] is False


def test_total_1_is_TR2_and_band_ambiguous():
    # composition=cystic(0) + echogenicity=hyperechoic(1) + shape=wider(0)
    # + margins=smooth(0) + foci=none(0) = 1
    r = score_acr_dual(_row(
        composition="cystic", echogenicity="hyperechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 1.0
    assert r["acr2017_category_strict"] == "TR2"
    assert r["acr2017_band_ambiguous"] is True


def test_total_2_is_TR2_no_ambiguity():
    # composition=mixed(1) + echogenicity=hyperechoic(1) + shape=wider(0)
    # + margins=smooth(0) + foci=none(0) = 2
    r = score_acr_dual(_row(
        composition="mixed_cystic_solid", echogenicity="hyperechoic",
        shape="wider_than_tall", margins="smooth", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 2.0
    assert r["acr2017_category_strict"] == "TR2"
    assert r["acr2017_band_ambiguous"] is False


def test_total_3_is_TR3():
    # composition=solid(2) + echogenicity=hyperechoic(1) + shape=wider(0)
    # + margins=smooth(0) + foci=none(0) = 3
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hyperechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 3.0
    assert r["acr2017_category_strict"] == "TR3"


def test_total_4_is_TR4():
    # solid(2) + hypoechoic(2) + wider(0) + smooth(0) + none(0) = 4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 4.0
    assert r["acr2017_category_strict"] == "TR4"


def test_total_6_upper_TR4():
    # solid(2) + hypoechoic(2) + wider(0) + lobulated(2) + none(0) = 6
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="lobulated", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 6.0
    assert r["acr2017_category_strict"] == "TR4"


def test_total_7_is_TR5():
    # solid(2) + very_hypo(3) + wider(0) + smooth(0) + macrocalc(1) + ... = need 7
    # solid(2) + very_hypo(3) + wider(0) + lobulated(2) + none(0) = 7
    r = score_acr_dual(_row(
        composition="solid", echogenicity="very_hypoechoic", shape="wider_than_tall",
        margins="lobulated", echogenic_foci='["none"]'
    ))
    assert r["acr2017_total_pts_strict"] == 7.0
    assert r["acr2017_category_strict"] == "TR5"


def test_max_score_TR5():
    # solid(2)+very_hypo(3)+taller(3)+ETE(3)+punctate(3)=14
    r = score_acr_dual(_row(
        composition="solid", echogenicity="very_hypoechoic", shape="taller_than_wide",
        margins="extrathyroidal_extension",
        echogenic_foci='["punctate_echogenic_foci"]'
    ))
    assert r["acr2017_total_pts_strict"] == 14.0
    assert r["acr2017_category_strict"] == "TR5"


# ---------------------------------------------------------------------------
# Strict vs imputed branching
# ---------------------------------------------------------------------------


def test_strict_null_when_foci_null():
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci=None,
    ))
    assert r["acr2017_category_strict"] is None
    assert r["acr2017_total_pts_strict"] is None
    assert r["acr2017_features_complete_strict"] is False
    # Imputed assumes foci=["none"]=0 so total=4 → TR4
    assert r["acr2017_category_imputed"] == "TR4"
    assert r["acr2017_total_pts_imputed"] == 4.0
    assert r["acr2017_features_complete_imputed"] is True


def test_imputed_null_when_4core_incomplete():
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins=None, echogenic_foci='["none"]',
    ))
    assert r["acr2017_category_imputed"] is None
    assert r["acr2017_features_complete_imputed"] is False


def test_both_null_when_composition_null():
    r = score_acr_dual(_row(
        composition=None, echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_category_strict"] is None
    assert r["acr2017_category_imputed"] is None


def test_both_null_when_echogenicity_null():
    r = score_acr_dual(_row(
        composition="solid", echogenicity=None, shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_category_strict"] is None
    assert r["acr2017_category_imputed"] is None


# ---------------------------------------------------------------------------
# Echogenic foci additive scoring
# ---------------------------------------------------------------------------


def test_foci_macro_plus_punctate():
    # solid(2)+hypo(2)+wider(0)+smooth(0)+macro(1)+punctate(3)=8 → TR5
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth",
        echogenic_foci='["macrocalcifications","punctate_echogenic_foci"]',
    ))
    assert r["acr2017_total_pts_strict"] == 8.0
    assert r["acr2017_category_strict"] == "TR5"


def test_foci_comet_tail_zero_points():
    # solid(2)+hypo(2)+wider(0)+smooth(0)+comet(0)=4 → TR4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["large_comet_tail_artifacts"]',
    ))
    assert r["acr2017_total_pts_strict"] == 4.0
    assert r["acr2017_category_strict"] == "TR4"


def test_foci_peripheral_rim():
    # solid(2)+iso(1)+wider(0)+smooth(0)+rim(2)=5 → TR4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="isoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["peripheral_rim_calcifications"]',
    ))
    assert r["acr2017_total_pts_strict"] == 5.0
    assert r["acr2017_category_strict"] == "TR4"


def test_foci_unparseable_string():
    # Garbage foci string should be treated as null → strict NULL, imputed OK
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci="not-json-at-all",
    ))
    assert r["acr2017_category_strict"] is None
    assert r["acr2017_category_imputed"] == "TR4"


# ---------------------------------------------------------------------------
# FNA threshold edges
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("size", "expected"),
    [(0.49, False), (0.99, False), (1.0, True), (1.49, True), (2.5, True), (4.0, True)],
)
def test_fna_TR5_threshold_at_1cm(size, expected):
    # Build a TR5 row
    r = score_acr_dual(_row(
        composition="solid", echogenicity="very_hypoechoic", shape="taller_than_wide",
        margins="lobulated", echogenic_foci='["none"]', size_cm_max=size,
    ))
    assert r["acr2017_category_strict"] == "TR5"
    assert r["acr2017_fna_recommended_strict"] is expected


@pytest.mark.parametrize(
    ("size", "expected"),
    [(0.99, False), (1.49, False), (1.5, True), (2.5, True), (4.0, True)],
)
def test_fna_TR4_threshold_at_1_5cm(size, expected):
    # Build a TR4 row (total=4)
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]', size_cm_max=size,
    ))
    assert r["acr2017_category_strict"] == "TR4"
    assert r["acr2017_fna_recommended_strict"] is expected


@pytest.mark.parametrize(
    ("size", "expected"),
    [(2.4, False), (2.5, True), (3.0, True)],
)
def test_fna_TR3_threshold_at_2_5cm(size, expected):
    # Build a TR3 row (total=3)
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hyperechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]', size_cm_max=size,
    ))
    assert r["acr2017_category_strict"] == "TR3"
    assert r["acr2017_fna_recommended_strict"] is expected


def test_fna_TR1_TR2_never_FNA():
    r = score_acr_dual(_row(
        composition="cystic", echogenicity="anechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]', size_cm_max=10.0,
    ))
    assert r["acr2017_category_strict"] == "TR1"
    assert r["acr2017_fna_recommended_strict"] is False


def test_fna_null_when_size_missing_TR4():
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]', size_cm_max=None,
    ))
    assert r["acr2017_category_strict"] == "TR4"
    assert r["acr2017_fna_recommended_strict"] is None


# ---------------------------------------------------------------------------
# Composition normalization (Phase A.3 vocabulary edge cases)
# ---------------------------------------------------------------------------


def test_anechoic_composition_normalizes_to_cystic_zero_points():
    r = score_acr_dual(_row(
        composition="anechoic", echogenicity="anechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 0.0
    assert r["composition_acr_normalized"] == "cystic"
    assert r["composition_normalization_warning"] is False


def test_mixed_with_spaces_normalizes_to_mixed_cystic_solid():
    r = score_acr_dual(_row(
        composition="mixed cystic and solid", echogenicity="hyperechoic",
        shape="wider_than_tall", margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 1.0
    assert r["composition_acr_normalized"] == "mixed_cystic_solid"
    assert r["composition_normalization_warning"] is False


def test_bare_mixed_warns():
    r = score_acr_dual(_row(
        composition="mixed", echogenicity="hyperechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 1.0
    assert r["composition_acr_normalized"] == "mixed_cystic_solid"
    assert r["composition_normalization_warning"] is True


def test_predominantly_cystic_normalizes_to_cystic():
    r = score_acr_dual(_row(
        composition="predominantly_cystic", echogenicity="hyperechoic",
        shape="wider_than_tall", margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 0.0
    assert r["composition_acr_normalized"] == "cystic"


def test_predominantly_solid_normalizes_to_solid():
    r = score_acr_dual(_row(
        composition="predominantly_solid", echogenicity="hypoechoic",
        shape="wider_than_tall", margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 2.0
    assert r["composition_acr_normalized"] == "solid"


def test_unrecognized_composition_yields_null_pts():
    r = score_acr_dual(_row(
        composition="weird_value", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] is None
    assert r["acr2017_category_strict"] is None
    assert r["acr2017_category_imputed"] is None


# ---------------------------------------------------------------------------
# Margins / shape / echogenicity edges
# ---------------------------------------------------------------------------


def test_taller_than_wide_adds_3():
    # cystic(0)+anechoic(0)+taller(3)+smooth(0)+none(0)=3 → TR3
    r = score_acr_dual(_row(
        composition="cystic", echogenicity="anechoic", shape="taller_than_wide",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_total_pts_strict"] == 3.0
    assert r["acr2017_category_strict"] == "TR3"


def test_extrathyroidal_extension_margins_adds_3():
    # solid(2)+iso(1)+wider(0)+ETE(3)+none(0)=6 → TR4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="isoechoic", shape="wider_than_tall",
        margins="extrathyroidal_extension", echogenic_foci='["none"]',
    ))
    assert r["acr2017_total_pts_strict"] == 6.0
    assert r["acr2017_category_strict"] == "TR4"


def test_microlobulated_margins_2pts():
    # solid(2)+hypo(2)+wider(0)+microlobulated(2)+none(0)=6 → TR4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="microlobulated", echogenic_foci='["none"]',
    ))
    assert r["acr2017_total_pts_strict"] == 6.0
    assert r["acr2017_category_strict"] == "TR4"


def test_ill_defined_margins_zero_points():
    # solid(2)+hypo(2)+wider(0)+ill_defined(0)+none(0)=4 → TR4
    r = score_acr_dual(_row(
        composition="solid", echogenicity="hypoechoic", shape="wider_than_tall",
        margins="ill_defined", echogenic_foci='["none"]',
    ))
    assert r["acr2017_total_pts_strict"] == 4.0
    assert r["acr2017_category_strict"] == "TR4"


def test_spongiform_zero_composition_points():
    r = score_acr_dual(_row(
        composition="spongiform", echogenicity="isoechoic", shape="wider_than_tall",
        margins="smooth", echogenic_foci='["none"]',
    ))
    assert r["acr2017_composition_pts"] == 0.0
    assert r["acr2017_total_pts_strict"] == 1.0
    # composition contributes 0 → total=1 (echo) → TR2 + ambiguous
    assert r["acr2017_category_strict"] == "TR2"
    assert r["acr2017_band_ambiguous"] is True
