"""Unit tests for Kwak 2011 scorer (≥20 cases per Phase B prompt)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "419_canonical_us_nodule_tirads_kwak_v1.py"
)
spec = importlib.util.spec_from_file_location("kwak_scorer", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules["kwak_scorer"] = mod
assert spec and spec.loader
spec.loader.exec_module(mod)
score = mod.score_kwak


def _row(**kw):
    base = {"composition": None, "echogenicity": None, "shape": None,
            "margins": None, "echogenic_foci": None, "size_cm_max": None}
    base.update(kw)
    return base


def test_cystic_zero_features_is_2():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "2"
    assert r["kwak_n_suspicious_features"] == 0


def test_predominantly_cystic_zero_features_is_2():
    r = score(_row(composition="predominantly_cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "2"


def test_anechoic_zero_features_is_2():
    r = score(_row(composition="anechoic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "2"


def test_spongiform_zero_features_is_2():
    r = score(_row(composition="spongiform", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "2"


def test_solid_alone_is_4A_not_3():
    # Per B.2: solid composition IS suspicious feature #1, so a "solid-only"
    # nodule has n_susp=1 → 4A, NOT 3. Category 3 requires n_susp=0 from a
    # non-cystic, non-spongiform composition (e.g. mixed_cystic_solid alone).
    r = score(_row(composition="solid", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "4A"
    assert r["kwak_n_suspicious_features"] == 1


def test_mixed_no_other_features_is_3():
    r = score(_row(composition="mixed_cystic_solid", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "3"
    assert r["kwak_n_suspicious_features"] == 0


def test_mixed_one_feature_4A():
    r = score(_row(composition="mixed_cystic_solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "4A"
    assert r["kwak_n_suspicious_features"] == 1


def test_solid_alone_one_feature_4A():
    # solid composition IS feature #1
    r = score(_row(composition="solid", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "4A"
    assert r["kwak_n_suspicious_features"] == 1


def test_two_features_4B():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] == "4B"
    assert r["kwak_n_suspicious_features"] == 2


def test_three_features_4C():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="taller_than_wide", margins="smooth"))
    assert r["kwak_category"] == "4C"
    assert r["kwak_n_suspicious_features"] == 3


def test_four_features_4C():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="taller_than_wide", margins="irregular"))
    assert r["kwak_category"] == "4C"
    assert r["kwak_n_suspicious_features"] == 4


def test_all_five_features_5():
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="taller_than_wide", margins="irregular",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["kwak_category"] == "5"
    assert r["kwak_n_suspicious_features"] == 5


def test_microcalc_is_feature_4():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    # cystic + microcalc → n=1 → 4A (cystic with microcalc is no longer pure benign-2)
    assert r["kwak_n_suspicious_features"] == 1
    assert r["kwak_category"] == "4A"


def test_microlobulated_counts_as_irregular():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="microlobulated"))
    assert r["kwak_n_suspicious_features"] == 1


def test_lobulated_counts_as_irregular():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="lobulated"))
    assert r["kwak_n_suspicious_features"] == 1


def test_very_hypoechoic_counts_as_hypo():
    r = score(_row(composition="cystic", echogenicity="very_hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_n_suspicious_features"] == 1


def test_predominantly_solid_counts_as_solid():
    r = score(_row(composition="predominantly_solid", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_n_suspicious_features"] == 1


def test_null_when_4core_incomplete_margins():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins=None))
    assert r["kwak_category"] is None
    assert r["kwak_n_suspicious_features"] is None


def test_null_when_4core_incomplete_composition():
    r = score(_row(composition=None, echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["kwak_category"] is None


def test_foci_null_does_not_block_scoring():
    # 4 core present, foci null → microcalc=False → still scoreable
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci=None))
    assert r["kwak_category"] == "4B"  # solid + hypo = 2 features → 4B


def test_fna_recommended_when_4A_size_above_1():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="microlobulated",
                   size_cm_max=1.5))
    assert r["kwak_category"] == "4A"
    assert r["kwak_fna_recommended"] is True


def test_fna_not_recommended_when_4A_size_below_1():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="microlobulated",
                   size_cm_max=0.5))
    assert r["kwak_category"] == "4A"
    assert r["kwak_fna_recommended"] is False


def test_fna_false_for_2_3():
    r = score(_row(composition="cystic", echogenicity="anechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=10.0))
    assert r["kwak_category"] == "2"
    assert r["kwak_fna_recommended"] is False


def test_fna_null_when_size_missing_and_suspicious():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=None))
    assert r["kwak_category"] == "4B"
    assert r["kwak_fna_recommended"] is None


def test_features_json_is_serialized_list():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    import json
    feats = json.loads(r["kwak_features_used_json"])
    assert "solid_composition" in feats
    assert "hypoechogenicity" in feats
