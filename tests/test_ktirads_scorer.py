"""Unit tests for K-TIRADS 2021 scorer (≥25 cases per Phase B prompt)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "426_canonical_us_nodule_tirads_ktirads_v1.py"
)
spec = importlib.util.spec_from_file_location("ktirads_scorer", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules["ktirads_scorer"] = mod
assert spec and spec.loader
spec.loader.exec_module(mod)
score = mod.score_ktirads
derive = mod.derive_composition_class


def _row(**kw):
    base = {"composition": None, "echogenicity": None, "shape": None,
            "margins": None, "echogenic_foci": None, "size_cm_max": None,
            "entirely_calcified": None}
    base.update(kw)
    return base


# Composition class
def test_class_iso_hyper_spongiform():
    assert derive("spongiform", "isoechoic", None) == "iso_hyperechoic_spongiform"
    assert derive("spongiform", "hyperechoic", None) == "iso_hyperechoic_spongiform"


def test_class_partially_cystic_intracystic_foci():
    assert derive("mixed_cystic_solid", "isoechoic",
                  ["large_comet_tail_artifacts"]) == "partially_cystic_intracystic_foci"
    assert derive("predominantly_cystic", "anechoic",
                  ["large_comet_tail_artifacts"]) == "partially_cystic_intracystic_foci"


def test_class_pure_cyst():
    assert derive("cystic", "anechoic", None) == "pure_cyst"
    assert derive("cystic", "anechoic", []) == "pure_cyst"
    assert derive("cystic", "anechoic", ["none"]) == "pure_cyst"
    assert derive("anechoic", "anechoic", None) == "pure_cyst"


def test_class_partially_cystic_iso_hyper_fallback():
    # No comet-tail; not pure cyst
    assert derive("mixed_cystic_solid", "isoechoic", None) == "partially_cystic_iso_hyper"
    assert derive("predominantly_cystic", "hyperechoic", None) == "partially_cystic_iso_hyper"


def test_class_solid_hypoechoic():
    assert derive("solid", "hypoechoic", None) == "solid_hypoechoic"
    assert derive("predominantly_solid", "very_hypoechoic", None) == "solid_hypoechoic"


def test_class_solid_iso_hyper_falls_to_partially_iso_hyper():
    # Per rule 4 fallback: solid + iso/hyper falls to partially_cystic_iso_hyper
    # (not solid_hypoechoic since echo is iso/hyper, not hypo)
    assert derive("solid", "isoechoic", None) == "partially_cystic_iso_hyper"


def test_class_other_solid_anechoic():
    # solid + anechoic doesn't match any rule; falls to "other"
    assert derive("solid", "anechoic", None) == "other"


def test_class_null_inputs():
    assert derive(None, "hypoechoic", None) is None
    assert derive("solid", None, None) is None


# Category mapping
def test_category_2_pure_cyst():
    r = score(_row(composition="cystic", echogenicity="anechoic"))
    assert r["ktirads_category"] == "2"


def test_category_2_spongiform():
    r = score(_row(composition="spongiform", echogenicity="hyperechoic"))
    assert r["ktirads_category"] == "2"


def test_category_2_intracystic_foci():
    r = score(_row(composition="mixed_cystic_solid", echogenicity="isoechoic",
                   echogenic_foci='["large_comet_tail_artifacts"]'))
    assert r["ktirads_category"] == "2"


def test_category_3_partially_cystic_no_features():
    r = score(_row(composition="predominantly_cystic", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ktirads_category"] == "3"


def test_category_4_solid_hypo_no_features():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ktirads_category"] == "4"


def test_category_4_partially_cystic_with_microcalc():
    r = score(_row(composition="predominantly_cystic", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["ktirads_category"] == "4"


def test_category_4_entirely_calcified_overrides():
    # Spongiform-iso/hyper would normally be K-TIRADS 2, but entirely_calcified
    # forces K-TIRADS 4 per the rule
    r = score(_row(composition="spongiform", echogenicity="hyperechoic",
                   entirely_calcified=True))
    assert r["ktirads_category"] == "4"


def test_category_5_solid_hypo_microcalc():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["ktirads_category"] == "5"


def test_category_5_solid_hypo_taller():
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="taller_than_wide", margins="smooth"))
    assert r["ktirads_category"] == "5"


def test_category_5_solid_hypo_irregular():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="irregular"))
    assert r["ktirads_category"] == "5"


def test_category_null_when_composition_missing():
    r = score(_row(composition=None, echogenicity="hypoechoic"))
    assert r["ktirads_category"] is None
    assert r["ktirads_composition_class"] is None


def test_category_null_when_echogenicity_missing():
    r = score(_row(composition="solid", echogenicity=None))
    assert r["ktirads_category"] is None


# FNA thresholds
def test_fna_2_never():
    r = score(_row(composition="cystic", echogenicity="anechoic", size_cm_max=10.0))
    assert r["ktirads_fna_recommended"] is False


def test_fna_3_above_2cm():
    r = score(_row(composition="predominantly_cystic", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=2.5))
    assert r["ktirads_category"] == "3"
    assert r["ktirads_fna_recommended"] is True


def test_fna_3_at_2cm_boundary_is_false():
    r = score(_row(composition="predominantly_cystic", echogenicity="hyperechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=2.0))
    assert r["ktirads_category"] == "3"
    assert r["ktirads_fna_recommended"] is False


def test_fna_4_at_1_5cm():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=1.5))
    assert r["ktirads_category"] == "4"
    assert r["ktirads_fna_recommended"] is True


def test_fna_4_below_1_5cm():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=1.0))
    assert r["ktirads_category"] == "4"
    assert r["ktirads_fna_recommended"] is False


def test_fna_5_above_1cm():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="irregular", size_cm_max=1.5))
    assert r["ktirads_category"] == "5"
    assert r["ktirads_fna_recommended"] is True


def test_fna_5_at_1cm_boundary_is_false():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="irregular", size_cm_max=1.0))
    assert r["ktirads_category"] == "5"
    assert r["ktirads_fna_recommended"] is False


def test_fna_null_when_size_missing():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=None))
    assert r["ktirads_category"] == "4"
    assert r["ktirads_fna_recommended"] is None


# n_suspicious counting
def test_n_suspicious_counts_microcalc_taller_irregular():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="taller_than_wide", margins="irregular",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["ktirads_n_suspicious"] == 3


def test_n_suspicious_zero_when_no_features():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ktirads_n_suspicious"] == 0


def test_entirely_calcified_passes_through():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   entirely_calcified=True))
    assert r["ktirads_entirely_calcified"] is True
    assert r["ktirads_category"] == "4"
