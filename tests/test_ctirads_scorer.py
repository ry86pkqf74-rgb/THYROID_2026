"""Unit tests for C-TIRADS 2020 scorer (≥20 cases per Phase B prompt)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "427_canonical_us_nodule_tirads_ctirads_v1.py"
)
spec = importlib.util.spec_from_file_location("ctirads_scorer", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules["ctirads_scorer"] = mod
assert spec and spec.loader
spec.loader.exec_module(mod)
score = mod.score_ctirads


def _row(**kw):
    base = {"composition": None, "echogenicity": None, "shape": None,
            "margins": None, "echogenic_foci": None, "size_cm_max": None,
            "ete_us_jsonb": None}
    base.update(kw)
    return base


# Score / category boundaries
def test_score_negative_with_comet_tail_only():
    # cystic + iso + wider + smooth + comet → 0 positives - 1 comet = -1 → C-TIRADS 2
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["large_comet_tail_artifacts"]'))
    assert r["ctirads_score"] == -1
    assert r["ctirads_category"] == "2"
    assert r["ctirads_comet_tail_present"] is True


def test_score_zero_no_features():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ctirads_score"] == 0
    assert r["ctirads_category"] == "3"


def test_score_1_solid_alone_4A():
    r = score(_row(composition="solid", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ctirads_score"] == 1
    assert r["ctirads_category"] == "4A"


def test_score_2_solid_microcalc_4B():
    r = score(_row(composition="solid", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["ctirads_score"] == 2
    assert r["ctirads_category"] == "4B"


def test_score_3_4C():
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="wider_than_tall", margins="irregular"))
    assert r["ctirads_score"] == 3
    assert r["ctirads_category"] == "4C"


def test_score_4_4C():
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="taller_than_wide", margins="irregular"))
    assert r["ctirads_score"] == 4
    assert r["ctirads_category"] == "4C"


def test_score_5_max_5():
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="taller_than_wide", margins="irregular",
                   echogenic_foci='["punctate_echogenic_foci"]'))
    assert r["ctirads_score"] == 5
    assert r["ctirads_category"] == "5"


def test_comet_tail_subtracts_one():
    # solid+hypo(not very_hypo so not feature)+wider+irregular+comet
    # = solid(1) + irregular(1) - comet(1) = 1 → 4A
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="irregular",
                   echogenic_foci='["large_comet_tail_artifacts"]'))
    assert r["ctirads_score"] == 1
    assert r["ctirads_comet_tail_present"] is True
    assert r["ctirads_category"] == "4A"


def test_marked_hypo_only_very_hypoechoic():
    # hypoechoic alone does NOT count for C-TIRADS marked-hypo (only very_hypo)
    r = score(_row(composition="cystic", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ctirads_score"] == 0
    assert r["ctirads_category"] == "3"


def test_irregular_margins_count():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="irregular"))
    assert r["ctirads_score"] == 1


def test_microlobulated_counts():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="microlobulated"))
    assert r["ctirads_score"] == 1


def test_lobulated_counts():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="lobulated"))
    assert r["ctirads_score"] == 1


def test_ill_defined_counts_for_ctirads():
    # Note: in ACR ill-defined is 0pts but in C-TIRADS it counts as +1
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="ill_defined"))
    assert r["ctirads_score"] == 1


def test_ete_in_jsonb_counts_as_irregular_margin():
    # Smooth margins but ETE present in jsonb → margins_irreg_or_ete = TRUE
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   ete_us_jsonb='{"presence":"strap_muscle_invasion"}'))
    assert r["ctirads_score"] == 1
    assert r["ctirads_category"] == "4A"


def test_ete_in_jsonb_capsule_loss_counts():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   ete_us_jsonb='{"presence":"capsule_loss"}'))
    assert r["ctirads_score"] == 1


def test_ete_in_jsonb_intact_does_not_count():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   ete_us_jsonb='{"presence":"intact"}'))
    assert r["ctirads_score"] == 0


def test_taller_counts():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="taller_than_wide", margins="smooth"))
    assert r["ctirads_score"] == 1


def test_predominantly_solid_counts_as_solid():
    r = score(_row(composition="predominantly_solid", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth"))
    assert r["ctirads_score"] == 1


def test_null_when_4core_incomplete():
    r = score(_row(composition="solid", echogenicity="hypoechoic",
                   shape="wider_than_tall", margins=None))
    assert r["ctirads_score"] is None
    assert r["ctirads_category"] is None


def test_features_json_serialized():
    import json as _json
    r = score(_row(composition="solid", echogenicity="very_hypoechoic",
                   shape="wider_than_tall", margins="smooth"))
    feats = _json.loads(r["ctirads_features_positive_json"])
    assert "solid_composition" in feats
    assert "marked_hypoechogenicity" in feats


# FNA
def test_fna_2_never():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth",
                   echogenic_foci='["large_comet_tail_artifacts"]', size_cm_max=10.0))
    assert r["ctirads_category"] == "2"
    assert r["ctirads_fna_recommended"] is False


def test_fna_3_never():
    r = score(_row(composition="cystic", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=10.0))
    assert r["ctirads_category"] == "3"
    assert r["ctirads_fna_recommended"] is False


def test_fna_4A_threshold_at_1_5cm():
    base = dict(composition="solid", echogenicity="isoechoic",
                shape="wider_than_tall", margins="smooth")
    assert score(_row(**base, size_cm_max=1.49))["ctirads_fna_recommended"] is False
    assert score(_row(**base, size_cm_max=1.5))["ctirads_fna_recommended"] is True


def test_fna_4B_threshold_at_1cm():
    base = dict(composition="solid", echogenicity="isoechoic",
                shape="wider_than_tall", margins="smooth",
                echogenic_foci='["punctate_echogenic_foci"]')
    r = score(_row(**base, size_cm_max=0.99))
    assert r["ctirads_category"] == "4B"
    assert r["ctirads_fna_recommended"] is False
    assert score(_row(**base, size_cm_max=1.0))["ctirads_fna_recommended"] is True


def test_fna_5_at_1cm():
    base = dict(composition="solid", echogenicity="very_hypoechoic",
                shape="taller_than_wide", margins="irregular",
                echogenic_foci='["punctate_echogenic_foci"]')
    r = score(_row(**base, size_cm_max=1.0))
    assert r["ctirads_category"] == "5"
    assert r["ctirads_fna_recommended"] is True


def test_fna_null_when_size_missing_4A():
    r = score(_row(composition="solid", echogenicity="isoechoic",
                   shape="wider_than_tall", margins="smooth", size_cm_max=None))
    assert r["ctirads_category"] == "4A"
    assert r["ctirads_fna_recommended"] is None
