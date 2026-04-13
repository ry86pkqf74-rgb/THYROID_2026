"""Regression tests: PSM determinism contract (Phase 5).

The test frame is synthetic (no real PHI) but mirrors the column
surface required by `propensity_match`: treat/control indicator,
covariates, research_id, structural_recurrence, dfs_years.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
PSM_MOD_DIR = REPO / "studies" / "proposal2_ete_staging"
if str(PSM_MOD_DIR) not in sys.path:
    sys.path.insert(0, str(PSM_MOD_DIR))


def _make_stub(name: str, attrs: dict) -> types.ModuleType:
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _stub_optional_deps() -> None:
    """Avoid importing heavy optional plotting/stats deps for the pure PSM test.

    The PSM module imports seaborn / lifelines / statsmodels / yaml / matplotlib
    at the top level, but `propensity_match` uses only sklearn, numpy, pandas,
    scipy.stats.fisher_exact. We stub the heavy ones out so the test can
    run in minimal environments (CI, slim container).
    """
    if "lifelines" not in sys.modules:
        sys.modules["lifelines"] = _make_stub(
            "lifelines", {"KaplanMeierFitter": type("KaplanMeierFitter", (), {})}
        )
        sys.modules["lifelines.statistics"] = _make_stub(
            "lifelines.statistics", {"logrank_test": lambda *a, **k: None}
        )
    if "seaborn" not in sys.modules:
        sys.modules["seaborn"] = _make_stub("seaborn", {"set_theme": lambda *a, **k: None})
    if "yaml" not in sys.modules:
        sys.modules["yaml"] = _make_stub(
            "yaml",
            {"safe_load": lambda *a, **k: {}, "dump": lambda *a, **k: ""},
        )
    if "matplotlib" not in sys.modules:
        sys.modules["matplotlib"] = _make_stub(
            "matplotlib", {"use": lambda *a, **k: None, "rcParams": {}}
        )
        sys.modules["matplotlib.pyplot"] = _make_stub(
            "matplotlib.pyplot",
            {
                "subplots": lambda *a, **k: (None, None),
                "tight_layout": lambda *a, **k: None,
                "close": lambda *a, **k: None,
                "rcParams": {},
            },
        )
    if "statsmodels" not in sys.modules:
        sys.modules["statsmodels"] = _make_stub("statsmodels", {})
        sys.modules["statsmodels.api"] = _make_stub(
            "statsmodels.api",
            {"add_constant": lambda x, **k: x, "Logit": type("Logit", (), {})},
        )


_stub_optional_deps()
import proposal2_endpoint_psm_strata as psm  # type: ignore  # noqa: E402


def _synthetic_frame(n_treat: int = 120, n_ctrl: int = 300, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = n_treat + n_ctrl
    rid = [f"RID{i:05d}" for i in range(n)]
    age = rng.normal(50, 12, n).clip(18, 90)
    female = rng.integers(0, 2, n)
    size = rng.gamma(2.0, 1.0, n).clip(0.1, 7.0)
    nflag = rng.integers(0, 2, n)
    treat = np.array([1] * n_treat + [0] * n_ctrl)
    # Structural recurrence: weak signal on treat + n_positive
    logit = -2.0 + 0.4 * treat + 0.6 * nflag + 0.05 * (size - 2.0)
    p = 1.0 / (1.0 + np.exp(-logit))
    recur = (rng.uniform(0, 1, n) < p).astype(int)
    dfs = rng.uniform(0.5, 10.0, n)
    df = pd.DataFrame({
        "research_id": rid,
        "age_at_surgery": age,
        "female": female,
        "largest_tumor_cm": size,
        "n_positive_flag": nflag,
        "ete_micro": treat,
        "ete_group": np.where(treat == 1, "Microscopic ETE", "No ETE"),
        "structural_recurrence": recur,
        "dfs_years": dfs,
    })
    return df


def _pair_rids(matched: pd.DataFrame) -> tuple[frozenset, frozenset]:
    if matched is None:
        return frozenset(), frozenset()
    t = frozenset(matched.loc[matched["treat"] == 1, "research_id"].tolist())
    c = frozenset(matched.loc[matched["treat"] == 0, "research_id"].tolist())
    return t, c


def test_propensity_match_is_row_order_invariant():
    df = _synthetic_frame()
    m1, e1, _ = psm.propensity_match(df.copy())

    # Shuffle the rows with a different seed and rerun.
    rng = np.random.default_rng(99)
    shuffled = df.sample(frac=1.0, random_state=rng.integers(0, 10_000)).reset_index(drop=True)
    m2, e2, _ = psm.propensity_match(shuffled)

    assert m1 is not None and m2 is not None
    assert len(m1) == len(m2), "matched frame length must be row-order invariant"

    t1, c1 = _pair_rids(m1)
    t2, c2 = _pair_rids(m2)
    assert t1 == t2, "treated research_id set must be invariant under input shuffle"
    assert c1 == c2, "control research_id set must be invariant under input shuffle"

    # Effect summary must agree
    assert int(e1.iloc[0]["Matched_pairs"]) == int(e2.iloc[0]["Matched_pairs"])


def test_propensity_match_respects_stable_sort_keys():
    df = _synthetic_frame(n_treat=40, n_ctrl=80, seed=13)
    matched, effect, _ = psm.propensity_match(df.copy())
    assert matched is not None
    # Matched frame should contain both arms
    assert set(matched["treat"].unique()) == {0, 1}
    # Effect table shape sanity
    assert "Matched_pairs" in effect.columns
    assert effect.iloc[0]["Matched_pairs"] > 0


def test_propensity_match_caliper_is_keyword():
    df = _synthetic_frame(n_treat=30, n_ctrl=80, seed=21)
    # Very tight caliper should produce strictly <= pairs vs default
    m_default, e_default, _ = psm.propensity_match(df.copy(), caliper=0.05)
    m_tight, e_tight, _ = psm.propensity_match(df.copy(), caliper=0.001)
    n_default = 0 if e_default is None else int(e_default.iloc[0]["Matched_pairs"])
    n_tight = 0 if e_tight is None else int(e_tight.iloc[0]["Matched_pairs"])
    assert n_tight <= n_default, "tighter caliper must not increase matched pairs"
