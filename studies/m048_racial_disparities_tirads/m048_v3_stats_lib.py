"""M048 v3 — shared stats helpers: SMD, safe Logit fits, OR extraction, mediation bootstrap."""
from __future__ import annotations

import re
import warnings
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import logit

warnings.filterwarnings("ignore", category=FutureWarning)

PRIMARY_RACES = ("Black", "White", "Asian")


def normalize_tr_category(val: object) -> str | None:
    """Map TR labels ('TR4', '4', 4, 'TR5', …) to canonical 'TR4'..'TR5'."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip().upper()
    m = re.search(r"TR\s*0*(\d+)", s)
    if m:
        return f"TR{m.group(1)}"
    m2 = re.search(r"0*(\d+)", s)
    if m2:
        return f"TR{m2.group(1)}"
    return None


def smd_continuous(x: pd.Series, group: pd.Series, ref: str = "White") -> dict[str, float]:
    """Standardized mean difference vs reference for numeric x within primary races."""
    out: dict[str, float] = {}
    sub = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "g": group}).dropna()
    ref_m = sub.loc[sub["g"] == ref, "x"]
    if len(ref_m) < 2:
        return {r: float("nan") for r in PRIMARY_RACES if r != ref}
    ref_mu, ref_sd = float(ref_m.mean()), float(ref_m.std(ddof=0)) or 1e-9
    for r in PRIMARY_RACES:
        if r == ref:
            continue
        rr = sub.loc[sub["g"] == r, "x"]
        if len(rr) < 2:
            out[r] = float("nan")
            continue
        out[r] = (float(rr.mean()) - ref_mu) / ref_sd
    return out


def smd_binary(binary_col: pd.Series, group: pd.Series, ref: str = "White") -> dict[str, float]:
    """SMD for binary 0/1 vs ref using pooled SD approximation for binary."""
    out: dict[str, float] = {}
    sub = pd.DataFrame({"b": binary_col.astype(float), "g": group}).dropna()
    p_ref = float(sub.loc[sub["g"] == ref, "b"].mean()) if (sub["g"] == ref).any() else float("nan")
    for r in PRIMARY_RACES:
        if r == ref:
            continue
        pr = sub.loc[sub["g"] == r, "b"]
        if len(pr) == 0:
            out[r] = float("nan")
            continue
        p_r = float(pr.mean())
        sqrt_pooled = np.sqrt(max(1e-9, (p_ref * (1 - p_ref) + p_r * (1 - p_r)) / 2))
        out[r] = (p_r - p_ref) / sqrt_pooled if sqrt_pooled > 0 else float("nan")
    return out


def fit_logit(
    formula: str,
    df: pd.DataFrame,
    cluster_col: str | None = None,
) -> Any:
    """Fit logistic; cluster-robust SE if cluster_col set."""
    use = df.dropna(subset=["is_malignant"]).copy()
    try:
        if cluster_col and cluster_col in use.columns:
            res = logit(formula, data=use).fit(
                disp=False,
                method="lbfgs",
                maxiter=200,
                cov_type="cluster",
                cov_kwds={"groups": use[cluster_col].astype(str)},
            )
        else:
            res = logit(formula, data=use).fit(disp=False, method="lbfgs", maxiter=200)
    except Exception:
        try:
            res = logit(formula, data=use).fit(disp=False, method="bfgs", maxiter=500)
        except Exception as e2:
            raise RuntimeError(f"logit failed: {e2}") from e2
    return res


def fit_logit_regularized(formula: str, df: pd.DataFrame, alpha: float = 0.05) -> Any:
    """Penalized / Firth-like fallback via statsmodels fit_regularized."""
    use = df.dropna(subset=["is_malignant"]).copy()
    return logit(formula, data=use).fit_regularized(disp=False, alpha=alpha, maxiter=300)


def race_or_table(
    result: Any,
    race_levels: tuple[str, ...] = ("Black", "Asian"),
    ref: str = "White",
) -> pd.DataFrame:
    """Extract OR + 95% CI + p for Treatment(ref) race dummies from statsmodels results."""
    rows = []
    params = result.params
    pvals = getattr(result, "pvalues", None)
    conf_int = result.conf_int()
    for lv in race_levels:
        name = f"C(race_strat, Treatment('{ref}'))[T.{lv}]"
        if name not in params.index:
            for alt in params.index:
                if f"[T.{lv}]" in alt and "race_strat" in alt:
                    name = alt
                    break
        if name not in params.index:
            rows.append({"race_level": lv, "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "p": np.nan, "param": ""})
            continue
        beta = float(params[name])
        lo, hi = float(conf_int.loc[name, 0]), float(conf_int.loc[name, 1])
        p = float(pvals[name]) if pvals is not None and name in pvals.index else np.nan
        rows.append({
            "race_level": lv,
            "or": np.exp(beta),
            "ci_lo": np.exp(lo),
            "ci_hi": np.exp(hi),
            "p": p,
            "param": name,
        })
    return pd.DataFrame(rows)


def prepare_v3_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean dtypes for v3 regression."""
    df = raw.copy()
    df["race_strat"] = df["race_strat"].astype(str)
    df = df[df["race_strat"].isin(PRIMARY_RACES)].copy()
    df["is_malignant"] = df["is_malignant"].apply(lambda v: 1 if v in (True, "true", "True", 1, "1") else 0).astype(int)
    for c in [
        "had_any_genetics", "had_any_nm", "has_clt", "has_mng", "has_graves", "has_niftp", "has_ftump",
        "had_any_fna", "had_repeat_fna",
    ]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: 1 if v in (True, "true", "True", 1, "1") else 0)
    if "bethesda_bucket" in df.columns:
        df["bethesda_bucket"] = df["bethesda_bucket"].fillna("missing").astype(str)
    if "nodule_burden_cat" in df.columns:
        df["nodule_burden_cat"] = df["nodule_burden_cat"].fillna("unknown").astype(str)
    if "surg_procedure_type" in df.columns:
        df["surg_procedure_type"] = df["surg_procedure_type"].fillna("unknown").astype(str)
    if "sex" in df.columns:
        df["sex"] = df["sex"].fillna("unknown").astype(str)
    for c in ["max_tr_int", "age_at_surgery", "surg_year", "n_fnas_total", "n_nodules_total", "days_us_to_surg_approx"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "days_us_to_surg_approx" in df.columns:
        # Bug D: clip negative values (US-after-surgery coding errors) to 0 and
        # convert to years so the variable lives on roughly the same numerical
        # scale as the other regressors. The unscaled day-count (range -10582
        # to 8019) caused lbfgs to overflow the linear predictor and fall back
        # to all-zero coefficients, collapsing race ORs to 1.000.
        days = pd.to_numeric(df["days_us_to_surg_approx"], errors="coerce").clip(lower=0) / 365.0
        df["days_us_to_surg_approx"] = days.fillna(days.median())
    if "surg_year" in df.columns:
        # Bug D: centre surg_year so it does not contribute a 2000+ baseline
        # term to the linear predictor. The Patsy expression `surg_year` in
        # the formulas now picks up this centred copy directly.
        sy = pd.to_numeric(df["surg_year"], errors="coerce")
        df["surg_year"] = sy - sy.median()
    return df


MEDIATORS: list[tuple[str, str]] = [
    ("n_nodules_total", "continuous"),
    ("had_any_genetics", "binary"),
    ("had_any_nm", "binary"),
    ("has_clt", "binary"),
    ("has_mng", "binary"),
    ("n_fnas_total", "continuous"),
    ("days_us_to_surg_approx", "continuous"),
]


def bootstrap_mediation_product(
    df: pd.DataFrame,
    mediator: str,
    med_type: str,
    race_col: str,
    y_col: str,
    controls_formula_tail: str,
    n_boot: int = 1000,
    seed: int = 42,
    race_target: str = "Black",
) -> dict[str, float]:
    """
    Approximate indirect effect for race_target vs White using product-of-coefficients.
    a_path: mediator ~ C(race_strat) + controls
    b_path: y ~ C(race_strat) + mediator + controls
    IE ≈ a_<race_target> * b_med.
    Returns dict including race_target for caller stacking.
    """
    rng = np.random.default_rng(seed)
    rids = df["research_id"].astype(str).values
    u_rid = np.unique(rids)
    coefs = []
    a_key = f"C({race_col}, Treatment('White'))[T.{race_target}]"

    def one_fit(bs_df: pd.DataFrame) -> float:
        try:
            if med_type == "binary":
                ma = f"{mediator} ~ C({race_col}, Treatment('White')) + {controls_formula_tail}"
                ra = logit(ma, data=bs_df).fit(disp=False, method="lbfgs", maxiter=150)
                a_race = float(ra.params.get(a_key, np.nan))
            else:
                ma = f"{mediator} ~ C({race_col}, Treatment('White')) + {controls_formula_tail}"
                ra = sm.OLS.from_formula(ma, data=bs_df).fit()
                a_race = float(ra.params.get(a_key, np.nan))
            yf = f"{y_col} ~ C({race_col}, Treatment('White')) + {mediator} + {controls_formula_tail}"
            ry = logit(yf, data=bs_df).fit(disp=False, method="lbfgs", maxiter=150)
            b_med = float(ry.params.get(mediator, np.nan))
            if np.isnan(a_race) or np.isnan(b_med):
                return np.nan
            return a_race * b_med
        except Exception:
            return np.nan

    for _ in range(n_boot):
        pick = rng.choice(u_rid, size=len(u_rid), replace=True)
        mask = df["research_id"].astype(str).isin(pick)
        bs_df = df.loc[mask].copy()
        if len(bs_df) < 50:
            continue
        coefs.append(one_fit(bs_df))
    coefs = [c for c in coefs if c == c]
    if not coefs:
        return {"race_target": race_target, "indirect_mean": np.nan, "ci_lo": np.nan, "ci_hi": np.nan}
    arr = np.array(coefs)
    return {
        "race_target": race_target,
        "indirect_mean": float(np.mean(arr)),
        "ci_lo": float(np.percentile(arr, 2.5)),
        "ci_hi": float(np.percentile(arr, 97.5)),
    }
