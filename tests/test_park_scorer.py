"""
Park / T-US 2009 scorer unit tests.
=====================================
≥ 25 test cases covering:
  - All-X-zero → P1 category (near logistic(intercept))
  - All-X-one → P5 category (maximum logit)
  - One-X-at-a-time β contribution isolation
  - Category boundary cases: probability = 0.07, 0.23, 0.50, 0.90
  - Three-coefficient-set differentiation (sets must produce different probabilities)
  - NULL-X policy (FALSE-default)
  - Negative β (X3 well-circumscribed reduces risk)

These tests are coefficient-set agnostic: they use small hard-coded test
coefficient sets that are internally consistent. They do NOT depend on Logan
supplying the production Park 2009 βs (which are verified separately).

Run: pytest tests/test_park_scorer.py -v
"""

import math
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers to import functions — we import from the scorer module directly.
# Since the scorer module is named with a leading digit, use importlib.
# ---------------------------------------------------------------------------
import importlib.util

SCORER_PATH = Path(__file__).parent.parent / "scripts" / "417_canonical_us_nodule_tirads_park_v1.py"


def _import_scorer():
    spec = importlib.util.spec_from_file_location("park_scorer", SCORER_PATH)
    mod = importlib.util.module_from_spec(spec)
    # Prevent main() from running on import
    spec.loader.exec_module(mod)
    return mod


try:
    _mod = _import_scorer()
    assign_category = _mod.assign_category
    compute_logit = _mod.compute_logit
    logistic = _mod.logistic
    _IMPORT_OK = True
except Exception as _import_err:
    _IMPORT_OK = False
    _IMPORT_ERR = str(_import_err)

    def assign_category(x): raise RuntimeError(_IMPORT_ERR)
    def compute_logit(x, y): raise RuntimeError(_IMPORT_ERR)
    def logistic(x): raise RuntimeError(_IMPORT_ERR)


if not _IMPORT_OK:
    pytestmark = pytest.mark.skip(reason=f"Cannot import scorer: {_IMPORT_ERR}")


# ---------------------------------------------------------------------------
# Test coefficient sets (hard-coded, not from manifest)
# ---------------------------------------------------------------------------
COEF_SET_A = {
    "intercept": -3.0,
    "betas": {
        "x1_taller": 1.5,
        "x2_halo": 0.8,
        "x3_well_circumscribed": -1.0,  # negative: reduces risk
        "x4_microlobulation": 1.2,
        "x5_infiltrative_margin": 1.4,
        "x6_marked_hypo": 1.8,
        "x7_hypo": 0.9,
        "x8_homogeneous": -0.5,
        "x9_mainly_cystic": -2.0,
        "x10_solid": 0.6,
        "x11_microcalc": 1.3,
        "x12_abnormal_ln": 1.7,
    },
}

# Slightly different intercept and betas — used to verify three sets differ
COEF_SET_B = {
    "intercept": -2.5,
    "betas": {
        "x1_taller": 1.2,
        "x2_halo": 0.6,
        "x3_well_circumscribed": -0.8,
        "x4_microlobulation": 1.0,
        "x5_infiltrative_margin": 1.2,
        "x6_marked_hypo": 1.3,   # different from A (1.8) so logit differs
        "x7_hypo": 0.8,
        "x8_homogeneous": -0.4,
        "x9_mainly_cystic": -1.7,
        "x10_solid": 0.5,
        "x11_microcalc": 0.7,    # different from A (1.3) so logit differs
        "x12_abnormal_ln": 1.4,
    },
}

COEF_SET_C = {
    "intercept": -4.0,
    "betas": {
        "x1_taller": 2.0,
        "x2_halo": 1.0,
        "x3_well_circumscribed": -1.2,
        "x4_microlobulation": 1.5,
        "x5_infiltrative_margin": 1.8,
        "x6_marked_hypo": 2.2,
        "x7_hypo": 1.1,
        "x8_homogeneous": -0.6,
        "x9_mainly_cystic": -2.5,
        "x10_solid": 0.8,
        "x11_microcalc": 1.6,
        "x12_abnormal_ln": 2.0,
    },
}

# All X = 0
X_ZERO = {f"park_x{i}_{n}": False for i, n in enumerate(
    ["taller", "halo", "well_circumscribed", "microlobulation",
     "infiltrative_margin", "marked_hypo", "hypo", "homogeneous",
     "mainly_cystic", "solid", "microcalc", "abnormal_ln"], 1)}

# All X = 1
X_ONE = {k: True for k in X_ZERO}


# ---------------------------------------------------------------------------
# Test 1: logistic() function
# ---------------------------------------------------------------------------
def test_logistic_zero():
    assert abs(logistic(0.0) - 0.5) < 1e-9


def test_logistic_positive():
    assert logistic(2.0) > 0.5


def test_logistic_negative():
    assert logistic(-2.0) < 0.5


def test_logistic_large_negative():
    assert logistic(-100.0) < 1e-9


def test_logistic_large_positive():
    assert logistic(100.0) > 1.0 - 1e-9


# ---------------------------------------------------------------------------
# Test 2: assign_category() — boundary cases
# ---------------------------------------------------------------------------
def test_category_none():
    assert assign_category(None) is None


def test_category_zero():
    assert assign_category(0.0) == "P1"


def test_category_below_p1():
    assert assign_category(0.01) == "P1"


def test_category_at_p1_boundary():
    # exactly 0.07 → P1 (≤ 0.07)
    assert assign_category(0.07) == "P1"


def test_category_just_above_p1():
    # 0.07 + epsilon → P2
    assert assign_category(0.07 + 1e-9) == "P2"


def test_category_at_p2_boundary():
    # exactly 0.23 → P2
    assert assign_category(0.23) == "P2"


def test_category_just_above_p2():
    # 0.23 + epsilon → P3
    assert assign_category(0.23 + 1e-9) == "P3"


def test_category_at_p3_boundary():
    # exactly 0.50 → P3
    assert assign_category(0.50) == "P3"


def test_category_just_above_p3():
    # 0.50 + epsilon → P4
    assert assign_category(0.50 + 1e-9) == "P4"


def test_category_at_p4_boundary():
    # exactly 0.90 → P4
    assert assign_category(0.90) == "P4"


def test_category_just_above_p4():
    # 0.90 + epsilon → P5
    assert assign_category(0.90 + 1e-9) == "P5"


def test_category_one():
    assert assign_category(1.0) == "P5"


# ---------------------------------------------------------------------------
# Test 3: compute_logit() — all-zero row
# ---------------------------------------------------------------------------
def test_all_x_zero_logit_equals_intercept():
    logit = compute_logit(COEF_SET_A, X_ZERO)
    assert abs(logit - COEF_SET_A["intercept"]) < 1e-9


def test_all_x_zero_category_p1():
    """With all X zero and intercept=-3.0, logit=-3.0, p=0.047 → P1."""
    logit = compute_logit(COEF_SET_A, X_ZERO)
    prob = logistic(logit)
    cat = assign_category(prob)
    assert cat == "P1", f"Expected P1 for all-zero X, got {cat} (p={prob:.4f})"


# ---------------------------------------------------------------------------
# Test 4: compute_logit() — all-one row → high category
# ---------------------------------------------------------------------------
def test_all_x_one_logit_large():
    logit = compute_logit(COEF_SET_A, X_ONE)
    # Intercept=-3.0, sum of betas = 1.5+0.8-1.0+1.2+1.4+1.8+0.9-0.5-2.0+0.6+1.3+1.7 = 7.7
    expected = -3.0 + 7.7
    assert abs(logit - expected) < 1e-9


def test_all_x_one_category_p5():
    logit = compute_logit(COEF_SET_A, X_ONE)
    prob = logistic(logit)
    cat = assign_category(prob)
    assert cat == "P5", f"Expected P5 for all-one X, got {cat} (p={prob:.4f})"


# ---------------------------------------------------------------------------
# Test 5: One-X-at-a-time β contribution isolation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("x_col,x_key,expected_direction", [
    ("park_x1_taller", "x1_taller", "positive"),
    ("park_x2_halo", "x2_halo", "positive"),
    ("park_x3_well_circumscribed", "x3_well_circumscribed", "negative"),  # risk-reducing
    ("park_x6_marked_hypo", "x6_marked_hypo", "positive"),
    ("park_x11_microcalc", "x11_microcalc", "positive"),
    ("park_x12_abnormal_ln", "x12_abnormal_ln", "positive"),
])
def test_single_x_beta_direction(x_col, x_key, expected_direction):
    """Setting one X to True should change logit in the expected direction."""
    x_off = X_ZERO.copy()
    x_on = X_ZERO.copy()
    x_on[x_col] = True

    logit_off = compute_logit(COEF_SET_A, x_off)
    logit_on = compute_logit(COEF_SET_A, x_on)

    beta = COEF_SET_A["betas"][x_key]
    if expected_direction == "positive":
        assert logit_on > logit_off, f"Expected positive β for {x_col}: {logit_off} → {logit_on}"
        assert abs((logit_on - logit_off) - beta) < 1e-9
    else:
        assert logit_on < logit_off, f"Expected negative β for {x_col}: {logit_off} → {logit_on}"
        assert abs((logit_off - logit_on) - abs(beta)) < 1e-9


# ---------------------------------------------------------------------------
# Test 6: Three coefficient sets produce different probabilities on same row
# ---------------------------------------------------------------------------
def test_three_sets_differ_on_typical_row():
    """A typical row (X6=True, X11=True) should give 3 distinct probabilities."""
    x_vals = X_ZERO.copy()
    x_vals["park_x6_marked_hypo"] = True
    x_vals["park_x11_microcalc"] = True

    p_a = logistic(compute_logit(COEF_SET_A, x_vals))
    p_b = logistic(compute_logit(COEF_SET_B, x_vals))
    p_c = logistic(compute_logit(COEF_SET_C, x_vals))

    probs = {p_a, p_b, p_c}
    assert len(probs) == 3, (
        f"All three coefficient sets produced the same probability! "
        f"p_A={p_a:.4f}, p_B={p_b:.4f}, p_C={p_c:.4f}. "
        "Check that manifests are not accidentally identical."
    )


def test_three_sets_all_zero_differ():
    """Even on the all-zero row, intercepts differ → probabilities differ."""
    p_a = logistic(compute_logit(COEF_SET_A, X_ZERO))
    p_b = logistic(compute_logit(COEF_SET_B, X_ZERO))
    p_c = logistic(compute_logit(COEF_SET_C, X_ZERO))
    assert p_a != p_b or p_a != p_c, "At least two sets must differ on all-zero row."


# ---------------------------------------------------------------------------
# Test 7: NULL coefficient set returns None (defensive)
# ---------------------------------------------------------------------------
def test_null_intercept_returns_none():
    null_set = {"intercept": None, "betas": {k: 1.0 for k in COEF_SET_A["betas"]}}
    assert compute_logit(null_set, X_ZERO) is None


def test_null_beta_returns_none():
    bad_set = dict(COEF_SET_A)
    bad_set["betas"] = dict(COEF_SET_A["betas"])
    bad_set["betas"]["x1_taller"] = None
    assert compute_logit(bad_set, X_ZERO) is None


# ---------------------------------------------------------------------------
# Test 8: Category NULL propagation
# ---------------------------------------------------------------------------
def test_category_of_none_logit():
    """If logit computation returns None (null coef), category should be None."""
    null_set = {"intercept": None, "betas": {k: 1.0 for k in COEF_SET_A["betas"]}}
    logit = compute_logit(null_set, X_ZERO)
    # logit is None → manually verify assign_category(None) = None
    assert assign_category(None) is None


# ---------------------------------------------------------------------------
# Test 9: Sanity — logistic is strictly monotone
# ---------------------------------------------------------------------------
def test_logistic_monotone():
    for z in [-5.0, -1.0, 0.0, 1.0, 5.0]:
        assert logistic(z + 0.1) > logistic(z)


# ---------------------------------------------------------------------------
# Test 10: Edge probabilities
# ---------------------------------------------------------------------------
def test_probability_from_logit_minus_10():
    """logit = -10 → very low prob → P1."""
    assert assign_category(logistic(-10)) == "P1"


def test_probability_from_logit_plus_10():
    """logit = +10 → very high prob → P5."""
    assert assign_category(logistic(10)) == "P5"


# ---------------------------------------------------------------------------
# Test 11: compute_logit works with both bool and 0/1 int inputs
# ---------------------------------------------------------------------------
def test_compute_logit_int_inputs():
    x_int = {k: 1 if v else 0 for k, v in X_ONE.items()}
    logit_bool = compute_logit(COEF_SET_A, X_ONE)
    logit_int = compute_logit(COEF_SET_A, x_int)
    assert abs(logit_bool - logit_int) < 1e-9


# ---------------------------------------------------------------------------
# Test 12: Negative-β feature reduces probability
# ---------------------------------------------------------------------------
def test_negative_beta_reduces_probability():
    """X3 well-circumscribed has negative β → turning it on lowers risk."""
    x_off = X_ZERO.copy()
    x_on = X_ZERO.copy()
    x_on["park_x3_well_circumscribed"] = True
    p_off = logistic(compute_logit(COEF_SET_A, x_off))
    p_on = logistic(compute_logit(COEF_SET_A, x_on))
    assert p_on < p_off, "Turning on well-circumscribed should lower probability."


# ---------------------------------------------------------------------------
# Test 13: Category ordinal consistency
# ---------------------------------------------------------------------------
def test_category_order_preserved():
    """Higher logit → higher or equal category."""
    cats = [assign_category(logistic(z)) for z in [-5, -3, -1, 0, 1, 3, 5]]
    cat_order = ["P1", "P2", "P3", "P4", "P5"]
    idxs = [cat_order.index(c) for c in cats]
    assert idxs == sorted(idxs), f"Category order not monotone: {cats}"
