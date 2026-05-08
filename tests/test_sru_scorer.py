"""Unit tests for SRU 2005 scorer (≥15 cases per Phase B prompt)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "428_canonical_us_nodule_tirads_sru_v1.py"
)
spec = importlib.util.spec_from_file_location("sru_scorer", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules["sru_scorer"] = mod
assert spec and spec.loader
spec.loader.exec_module(mod)
score = mod.score_sru


def _row(**kw):
    base = {"composition": None, "echogenic_foci": None, "size_cm_max": None,
            "interval_growth": None, "has_suspicious_ln_within_60d": None}
    base.update(kw)
    return base


# Rule 1: LN priority overrides everything
def test_ln_priority_wins_over_microcalc():
    r = score(_row(composition="solid", has_suspicious_ln_within_60d=1,
                   echogenic_foci='["punctate_echogenic_foci"]', size_cm_max=2.0))
    assert r["sru_recommendation"] == "lymph_node_priority"
    assert json.loads(r["sru_basis_json"])["rule"] == "lymph_node_priority"


def test_ln_priority_wins_over_no_other_data():
    r = score(_row(has_suspicious_ln_within_60d=1))
    assert r["sru_recommendation"] == "lymph_node_priority"


def test_ln_zero_does_not_trigger_priority():
    r = score(_row(composition="cystic", has_suspicious_ln_within_60d=0))
    # Falls to no_fna
    assert r["sru_recommendation"] == "no_fna"


# Rule 2: fna_strong — microcalc + size >= 1.0
def test_fna_strong_microcalc_at_1cm():
    r = score(_row(composition="solid", echogenic_foci='["punctate_echogenic_foci"]',
                   size_cm_max=1.0))
    assert r["sru_recommendation"] == "fna_strong"


def test_fna_strong_microcalc_above_1cm():
    r = score(_row(composition="solid", echogenic_foci='["punctate_echogenic_foci"]',
                   size_cm_max=2.5))
    assert r["sru_recommendation"] == "fna_strong"


def test_microcalc_below_1cm_does_not_trigger_strong():
    # Falls to fna_consider via solid+coarse path? No, no macrocalc.
    # Falls to fna_consider via solid+ size>=1.5? No, size<1.5.
    # Falls through, no other rule matches → NULL
    r = score(_row(composition="solid", echogenic_foci='["punctate_echogenic_foci"]',
                   size_cm_max=0.8))
    assert r["sru_recommendation"] is None


# Rule 3a: solid or coarse calc + size >= 1.5
def test_fna_consider_solid_at_1_5cm():
    r = score(_row(composition="solid", size_cm_max=1.5))
    assert r["sru_recommendation"] == "fna_consider"
    basis = json.loads(r["sru_basis_json"])
    assert basis["rule"] == "fna_consider_solid_or_coarse"


def test_fna_consider_macrocalc_at_1_5cm():
    r = score(_row(composition="cystic",
                   echogenic_foci='["macrocalcifications"]', size_cm_max=1.5))
    # Macrocalc + cystic + size>=1.5 → fna_consider via solid_or_coarse rule
    assert r["sru_recommendation"] == "fna_consider"


def test_solid_below_1_5cm_does_not_trigger_consider():
    r = score(_row(composition="solid", size_cm_max=1.0))
    # No microcalc, size<1.5 → no rule matches
    assert r["sru_recommendation"] is None


def test_predominantly_solid_counts_as_solid():
    r = score(_row(composition="predominantly_solid", size_cm_max=2.0))
    assert r["sru_recommendation"] == "fna_consider"


# Rule 3b: mixed/predominantly_cystic + size >= 2.0
def test_fna_consider_mixed_at_2cm():
    r = score(_row(composition="mixed_cystic_solid", size_cm_max=2.0))
    assert r["sru_recommendation"] == "fna_consider"
    assert json.loads(r["sru_basis_json"])["rule"] == "fna_consider_mixed_predcystic"


def test_mixed_below_2cm_does_not_trigger():
    r = score(_row(composition="mixed_cystic_solid", size_cm_max=1.8))
    assert r["sru_recommendation"] is None


def test_predominantly_cystic_at_2cm_with_growth_falls_to_growth_rule():
    # Note: 3b requires mixed/predominantly_cystic+size>=2.0
    # predominantly_cystic without growth, no microcalc, size 2.5
    # Per rule order: 3b should fire with predominantly_cystic+size>=2.0
    r = score(_row(composition="predominantly_cystic", size_cm_max=2.5,
                   interval_growth=False))
    assert r["sru_recommendation"] == "fna_consider"


# Rule 3c: interval_growth = TRUE
def test_fna_consider_interval_growth():
    r = score(_row(composition="cystic", interval_growth=True, size_cm_max=0.5))
    # No microcalc, no solid+1.5, no mixed+2.0; growth → consider
    assert r["sru_recommendation"] == "fna_consider"
    assert json.loads(r["sru_basis_json"])["rule"] == "fna_consider_interval_growth"


# Rule 4: no_fna
def test_no_fna_cystic_no_growth():
    r = score(_row(composition="cystic", interval_growth=False))
    assert r["sru_recommendation"] == "no_fna"


def test_no_fna_predominantly_cystic_no_growth_small():
    r = score(_row(composition="predominantly_cystic", size_cm_max=1.0,
                   interval_growth=False))
    # 3b requires size>=2.0; size=1.0 doesn't trigger; falls to no_fna
    assert r["sru_recommendation"] == "no_fna"


def test_anechoic_treated_as_pure_cystic_no_fna():
    r = score(_row(composition="anechoic"))
    assert r["sru_recommendation"] == "no_fna"


# Rule 5: NULL when insufficient data
def test_null_when_no_inputs():
    r = score(_row())
    assert r["sru_recommendation"] is None


def test_null_when_solid_no_size():
    r = score(_row(composition="solid", size_cm_max=None))
    # No microcalc; no size to clear thresholds; no growth; not cystic → NULL
    assert r["sru_recommendation"] is None
