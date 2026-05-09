#!/usr/bin/env python3
"""
M088 — annual incidence trends and segmented (joinpoint-style) regression.

Pre-specified breakpoints: 2008, 2015, 2017, 2022 (per analysis_plan_v1.md §5).

Output:
  - tables/joinpoint_apc.csv : annual percent change (APC) per segment, per entity
  - figures/figure_2_incidence_trends.png : annual incidence with WHO breakpoints
"""

from __future__ import annotations
import os
import csv
import math
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT       = Path(__file__).resolve().parent.parent
ANALYSIS   = ROOT / "analysis"
OUTPUT     = ANALYSIS / "output"
TABLES     = ROOT / "tables"
FIGURES    = ROOT / "figures"
TABLES.mkdir(exist_ok=True)
FIGURES.mkdir(exist_ok=True)

BREAKPOINTS = [2008, 2015, 2017, 2022]
YEAR_MIN    = 1990
YEAR_MAX    = 2025

# ---------------------------------------------------------------------------- #
# Load annual incidence

def load_incidence():
    rows = []
    with open(OUTPUT / "incidence_annual.csv") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "year":    int(r["surgery_year"]),
                "primary": r["diagnosis_primary"],
                "variant": r["variant"],
                "n":       int(r["n"]),
            })
    return rows

# ---------------------------------------------------------------------------- #
# Build per-entity annual time series, with 0-fill for years with no cases

def build_series(rows, key_fn, entity_label):
    """Aggregate rows under key_fn; return (years, counts) full 1990-2025."""
    by_year = defaultdict(int)
    for r in rows:
        if key_fn(r):
            by_year[r["year"]] += r["n"]
    years  = list(range(YEAR_MIN, YEAR_MAX + 1))
    counts = [by_year.get(y, 0) for y in years]
    return entity_label, years, counts

ENTITY_KEYS = [
    ("Follicular adenoma",                        lambda r: r["primary"] == "follicular_adenoma"),
    ("Hurthle cell / Oncocytic adenoma",          lambda r: r["primary"] == "hurthle_cell_adenoma"),
    ("FTC, all variants",                         lambda r: r["primary"] == "FTC"),
    ("  FTC, minimally invasive",                 lambda r: r["primary"] == "FTC" and r["variant"] == "minimally_invasive"),
    ("  FTC, oncocytic / Hurthle variant",        lambda r: r["primary"] == "FTC" and r["variant"] == "oncocytic_warthin"),
    ("  FTC, widely invasive",                    lambda r: r["primary"] == "FTC" and r["variant"] == "widely_invasive"),
    ("NIFTP",                                     lambda r: r["primary"] == "NIFTP"),
    ("FT-UMP",                                    lambda r: r["primary"] == "FTUMP"),
    ("Atypical FA",                               lambda r: r["primary"] == "atypical_follicular_adenoma"),
    ("DHGTC",                                     lambda r: r["primary"] == "DHGTC"),
]

# ---------------------------------------------------------------------------- #
# Segmented log-linear regression with fixed breakpoints
# log(n+1) = a + b1*x + b2*max(x-bp1,0) + b3*max(x-bp2,0) + ...
# APC for segment k = (exp(slope_k) - 1) * 100, where slope_k is sum of relevant betas

def segmented_log_linear(years, counts, breakpoints):
    """Fit segmented log-linear regression with fixed knots; return APC per segment."""
    x = np.array(years, dtype=float)
    y = np.log(np.array(counts, dtype=float) + 1.0)  # +1 to handle zeros
    # Design: intercept, x, and one hinge per breakpoint
    cols = [np.ones_like(x), x]
    for bp in breakpoints:
        cols.append(np.maximum(x - bp, 0.0))
    X = np.vstack(cols).T
    # OLS via lstsq
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    # Recover slope per segment
    # segment 0 (before bp1): slope = beta[1]
    # segment k+1 (between bp_k and bp_{k+1}): slope = beta[1] + sum(beta[2..k+2])
    seg_bounds = [(YEAR_MIN, breakpoints[0])]
    for i, bp in enumerate(breakpoints):
        nxt = breakpoints[i+1] if i+1 < len(breakpoints) else YEAR_MAX
        seg_bounds.append((bp, nxt))
    slopes = []
    s = beta[1]
    slopes.append(s)
    for i in range(len(breakpoints)):
        s = s + beta[2 + i]
        slopes.append(s)
    apcs = [(math.exp(sl) - 1) * 100 for sl in slopes]
    # Compute residuals + standard errors per segment via subset OLS for rough CI
    # (a true joinpoint CI is more involved; we report per-segment SE on the cumulative slope)
    n  = len(x)
    yhat = X @ beta
    resid = y - yhat
    sigma2 = float(np.sum(resid**2) / max(1, n - X.shape[1]))
    XtX_inv = np.linalg.pinv(X.T @ X)
    var_beta = sigma2 * XtX_inv
    # Per-segment slope variance using delta method: slope_k = beta[1] + sum(beta[2..k+1])
    se_slopes = []
    for k in range(len(slopes)):
        idx = [1] + [2 + j for j in range(k)]
        v = 0.0
        for i in idx:
            for j in idx:
                v += var_beta[i, j]
        se_slopes.append(math.sqrt(max(v, 0.0)))
    z = 1.96
    apc_ci = []
    for sl, se in zip(slopes, se_slopes):
        lo = (math.exp(sl - z*se) - 1) * 100
        hi = (math.exp(sl + z*se) - 1) * 100
        apc_ci.append((lo, hi))
    return list(zip(seg_bounds, slopes, apcs, apc_ci, se_slopes))

# ---------------------------------------------------------------------------- #
# Run analysis

def main():
    rows = load_incidence()

    # Compute APC per entity per segment
    apc_rows = []
    for label, key in ENTITY_KEYS:
        _, years, counts = build_series(rows, key, label)
        if sum(counts) < 10:
            # too sparse for stable APC
            continue
        segments = segmented_log_linear(years, counts, BREAKPOINTS)
        for (yr_lo, yr_hi), slope, apc, (apc_lo, apc_hi), se in segments:
            apc_rows.append({
                "entity":      label.strip(),
                "segment":     f"{yr_lo}-{yr_hi}",
                "slope_log":   round(slope, 4),
                "apc_pct":     round(apc, 1),
                "apc_lo":      round(apc_lo, 1),
                "apc_hi":      round(apc_hi, 1),
                "se":          round(se, 4),
            })

    # Write APC table
    out_csv = TABLES / "joinpoint_apc.csv"
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(apc_rows[0].keys()))
        w.writeheader()
        for r in apc_rows:
            w.writerow(r)
    print(f"Wrote {out_csv} ({len(apc_rows)} rows)")

    # ----- Figure 2 ---------------------------------------------------------- #
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), sharex=True)
    axes = axes.flatten()
    panels = [
        ("Follicular adenoma",                 lambda r: r["primary"] == "follicular_adenoma"),
        ("FTC variants",                       None),  # multi-line panel
        ("NIFTP / FT-UMP / Atypical FA",       None),  # multi-line panel
        ("Oncocytic family (HCA + onco-FTC)",  None),  # composite
    ]
    # Panel 1: FA
    _, yrs, cnts = build_series(rows, lambda r: r["primary"] == "follicular_adenoma", "")
    axes[0].plot(yrs, cnts, marker="o", color="#1f77b4", lw=1.6)
    axes[0].set_title("Follicular adenoma — annual count")
    # Panel 2: FTC variants
    for nm, k, color in [
        ("MI-FTC",        lambda r: r["primary"]=="FTC" and r["variant"]=="minimally_invasive", "#d62728"),
        ("Oncocytic FTC", lambda r: r["primary"]=="FTC" and r["variant"]=="oncocytic_warthin",  "#9467bd"),
        ("WI-FTC",        lambda r: r["primary"]=="FTC" and r["variant"]=="widely_invasive",    "#ff7f0e"),
        ("FTC, NOS",      lambda r: r["primary"]=="FTC" and r["variant"]=="(null)",             "#2ca02c"),
    ]:
        _, yrs, cnts = build_series(rows, k, "")
        axes[1].plot(yrs, cnts, marker="o", color=color, lw=1.4, label=nm)
    axes[1].legend(loc="upper left", fontsize=8)
    axes[1].set_title("FTC variants — annual count")
    # Panel 3: borderline / new entities
    for nm, k, color in [
        ("NIFTP",        lambda r: r["primary"]=="NIFTP",                       "#17becf"),
        ("FT-UMP",       lambda r: r["primary"]=="FTUMP",                       "#e377c2"),
        ("Atypical FA",  lambda r: r["primary"]=="atypical_follicular_adenoma", "#bcbd22"),
    ]:
        _, yrs, cnts = build_series(rows, k, "")
        axes[2].plot(yrs, cnts, marker="o", color=color, lw=1.4, label=nm)
    axes[2].legend(loc="upper left", fontsize=8)
    axes[2].set_title("Borderline / new 2017+ entities")
    # Panel 4: oncocytic family composite
    _, yrs, hca = build_series(rows, lambda r: r["primary"]=="hurthle_cell_adenoma",                       "")
    _, _,   onc = build_series(rows, lambda r: r["primary"]=="FTC" and r["variant"]=="oncocytic_warthin", "")
    composite = [a + b for a, b in zip(hca, onc)]
    axes[3].plot(yrs, composite, marker="o", color="#8c564b", lw=1.6, label="HCA + onco-FTC")
    axes[3].plot(yrs, hca,        marker=".", color="#1f77b4", lw=0.9, label="HCA")
    axes[3].plot(yrs, onc,        marker=".", color="#9467bd", lw=0.9, label="Onco-FTC")
    axes[3].legend(loc="upper left", fontsize=8)
    axes[3].set_title("Oncocytic family (2022 WHO)")

    # Overlay WHO breakpoints
    for ax in axes:
        for bp, lbl in zip(BREAKPOINTS, ["Bethesda 2008", "ATA 2015", "NIFTP 2017", "WHO 5th 2022"]):
            ax.axvline(bp, color="gray", ls="--", lw=0.8, alpha=0.7)
            ax.text(bp + 0.1, ax.get_ylim()[1] * 0.95 if hasattr(ax, "get_ylim") else 100,
                    lbl, rotation=90, fontsize=7, color="gray", va="top")
        ax.set_xlim(YEAR_MIN, YEAR_MAX)
        ax.set_ylabel("n")
    axes[2].set_xlabel("Year of surgery")
    axes[3].set_xlabel("Year of surgery")
    fig.suptitle("M088 Figure 2 — Annual incidence with WHO/ATA breakpoint overlay", fontsize=12)
    fig.tight_layout()
    out_png = FIGURES / "figure_2_incidence_trends.png"
    fig.savefig(out_png, dpi=150)
    print(f"Wrote {out_png}")

    # Summary print
    print("\nSummary APC table:")
    for r in apc_rows:
        if "FTC" in r["entity"] or "FA" in r["entity"] or r["entity"] in ("NIFTP", "FT-UMP", "DHGTC"):
            print(f"  {r['entity']:<40s} {r['segment']:<12s} APC={r['apc_pct']:+6.1f}% [{r['apc_lo']:+6.1f},{r['apc_hi']:+6.1f}]")

if __name__ == "__main__":
    main()
