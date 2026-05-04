"""_stats.py — Statistical helpers for M038 analyses.

Provides:
  - chi2_or_fisher(a, b, c, d): 2x2 test, returns (test, p)
  - chi2_table(observed): chi-squared on r×c table, returns (chi2, p, df)
  - mannwhitney(x, y): two-sided U-test
  - ttest(x, y): Welch's two-sample t (equal_var=False)
  - rr_ci(a, n1, b, n2): risk ratio + 95% Wald CI; returns (rr, lo, hi)
  - cont_summary(x): {'n','mean','sd','median','q25','q75','min','max','missing'}
  - count_pct(num, den): formatted "n (%)" string
"""
import numpy as np
import pandas as pd
from scipy import stats
import math


def cont_summary(x):
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) == 0:
        return {"n": 0, "mean": None, "sd": None, "median": None,
                "q25": None, "q75": None, "min": None, "max": None, "missing": 0}
    return {
        "n": int(len(x)),
        "mean": float(x.mean()),
        "sd": float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        "median": float(x.median()),
        "q25": float(x.quantile(0.25)),
        "q75": float(x.quantile(0.75)),
        "min": float(x.min()),
        "max": float(x.max()),
        "missing": int(pd.isna(x).sum()),
    }


def fmt_mean_sd(s):
    if s["n"] == 0: return "—"
    return f"{s['mean']:.1f} ± {s['sd']:.1f}"


def fmt_median_iqr(s):
    if s["n"] == 0: return "—"
    return f"{s['median']:.1f} ({s['q25']:.1f}–{s['q75']:.1f})"


def chi2_or_fisher(a, b, c, d):
    """2x2 contingency: [[a,b],[c,d]]. Use Fisher when any expected cell <5
    or when chi2 fails (e.g., a row/column sums to zero)."""
    table = np.array([[a, b], [c, d]])
    if table.sum() == 0:
        return ("none", float("nan"))
    # Empty row/col → chi2 undefined; fall through to Fisher
    if (table.sum(axis=0) == 0).any() or (table.sum(axis=1) == 0).any():
        try:
            odds, p_f = stats.fisher_exact(table)
            return ("Fisher", float(p_f))
        except Exception:
            return ("none", float("nan"))
    try:
        chi2, p, dof, expected = stats.chi2_contingency(table, correction=False)
    except Exception:
        try:
            odds, p_f = stats.fisher_exact(table)
            return ("Fisher", float(p_f))
        except Exception:
            return ("none", float("nan"))
    if (expected < 5).any():
        try:
            odds, p_f = stats.fisher_exact(table)
            return ("Fisher", float(p_f))
        except Exception:
            return ("Chi2", float(p))
    return ("Chi2", float(p))


def chi2_table(observed):
    obs = np.array(observed)
    if obs.sum() == 0 or obs.shape[0] < 2 or obs.shape[1] < 2:
        return (None, float("nan"), None)
    try:
        chi2, p, dof, expected = stats.chi2_contingency(obs, correction=False)
        return (float(chi2), float(p), int(dof))
    except Exception:
        return (None, float("nan"), None)


def mannwhitney(x, y):
    x = pd.to_numeric(x, errors="coerce").dropna().values
    y = pd.to_numeric(y, errors="coerce").dropna().values
    if len(x) < 2 or len(y) < 2:
        return ("MWU", float("nan"))
    try:
        u, p = stats.mannwhitneyu(x, y, alternative="two-sided")
        return ("MWU", float(p))
    except Exception:
        return ("MWU", float("nan"))


def ttest(x, y):
    x = pd.to_numeric(x, errors="coerce").dropna().values
    y = pd.to_numeric(y, errors="coerce").dropna().values
    if len(x) < 2 or len(y) < 2:
        return ("t", float("nan"))
    try:
        t, p = stats.ttest_ind(x, y, equal_var=False)
        return ("t", float(p))
    except Exception:
        return ("t", float("nan"))


def rr_ci(a, n1, b, n2):
    """Risk ratio (massive vs non-massive) with Wald 95% CI on log RR.
    a = events in massive, n1 = total massive, b = events in non-massive, n2 = total non-massive.
    """
    if n1 == 0 or n2 == 0 or b == 0 or a == 0:
        # Fall back to point estimate without CI when zero counts
        if n1 == 0 or n2 == 0:
            return (float("nan"), float("nan"), float("nan"))
        if a == 0 and b == 0:
            return (float("nan"), float("nan"), float("nan"))
        # 0.5 continuity correction (Haldane-Anscombe)
        a_, b_ = a + 0.5, b + 0.5
        n1_, n2_ = n1 + 1.0, n2 + 1.0
        rr = (a_ / n1_) / (b_ / n2_)
        se = math.sqrt(1.0/a_ - 1.0/n1_ + 1.0/b_ - 1.0/n2_)
        lo = math.exp(math.log(rr) - 1.96*se)
        hi = math.exp(math.log(rr) + 1.96*se)
        return (rr, lo, hi)
    p1 = a / n1
    p2 = b / n2
    rr = p1 / p2
    se = math.sqrt((1.0 - p1) / a + (1.0 - p2) / b)
    lo = math.exp(math.log(rr) - 1.96 * se)
    hi = math.exp(math.log(rr) + 1.96 * se)
    return (float(rr), float(lo), float(hi))


def fmt_p(p):
    if p is None or (isinstance(p, float) and (math.isnan(p))):
        return "—"
    if p < 0.001: return "<0.001"
    return f"{p:.3f}"


def fmt_rr_ci(rr, lo, hi):
    if any(map(lambda v: v is None or (isinstance(v, float) and math.isnan(v)), [rr, lo, hi])):
        return "—"
    return f"{rr:.2f} ({lo:.2f}–{hi:.2f})"


def fmt_pct(num, den):
    if den == 0: return "—"
    return f"{num} ({100*num/den:.1f}%)"


def count_pct(num, den):
    return fmt_pct(int(num), int(den))


def truthy_count(series, true_values=("yes","y","true","1")):
    """Count rows where a string-valued column matches truthy strings (case-insensitive)."""
    s = series.astype("string").str.strip().str.lower()
    return int(s.isin(true_values).sum())
