#!/usr/bin/env python3
"""Generate 06_figures/ for M037 (300 DPI PNG + CSV sidecars)."""

from __future__ import annotations

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIG = os.path.join(PKG, "06_figures")
OUT_ANALYSIS = os.path.join(PKG, "08_analysis_outputs")
PARQUET = os.path.join(OUT_ANALYSIS, "m037_analytic_spine.parquet")

# Wong-like palette
C1, C2, C3 = "#E69F00", "#56B4E9", "#009E73"


def main():
    if not os.path.isfile(PARQUET):
        raise SystemExit(f"Missing {PARQUET}; run build_m037_tables.py first.")
    df = pd.read_parquet(PARQUET)
    os.makedirs(FIG, exist_ok=True)

    # Fig 2 — LN+ rate by tumor size bucket (1 cm bins up to 6+)
    df["size_bin"] = pd.cut(
        df["tumor_size_cm"],
        bins=[0, 1, 2, 3, 4, 5, 6, np.inf],
        labels=["≤1", "1–2", "2–3", "3–4", "4–5", "5–6", ">6"],
        right=True,
    )
    g = df.groupby("size_bin", observed=True).agg(rate=("y_ln_pos", "mean"), n=("research_id", "count")).reset_index()
    g.to_csv(os.path.join(FIG, "m037_fig2_ln_rate_by_size_data.csv"), index=False)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(g))
    ax.bar(x, 100.0 * g["rate"], color=C1, edgecolor="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in g["size_bin"]], rotation=0)
    ax.set_ylabel("LN-positive (N1+) %")
    ax.set_xlabel("Tumor size (cm), pathologic max")
    ax.set_title("M037 — LN+ prevalence by tumor size bucket")
    for i, r in g.iterrows():
        ax.text(i, 100.0 * r["rate"] + 0.8, f'n={int(r["n"])}', ha="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "m037_fig2_ln_rate_by_size.png"), dpi=300)
    plt.close(fig)

    # Fig 3 — Forest plot (primary model coeffs from CSV if present)
    coef_path = os.path.join(OUT_ANALYSIS, "m037_forest_primary.csv")
    if os.path.isfile(coef_path):
        fp = pd.read_csv(coef_path)
        if "or" not in fp.columns and "log_or" in fp.columns:
            fp["or"] = np.exp(fp["log_or"])
            fp["ci_lo"] = np.exp(fp["log_or"] - 1.96 * fp["se"])
            fp["ci_hi"] = np.exp(fp["log_or"] + 1.96 * fp["se"])
    else:
        from build_m037_tables import add_derived, connect, pull_spine  # noqa: E402

        con = connect()
        raw = pull_spine(con)
        con.close()
        mdf = add_derived(raw)
        sub = mdf[["age_at_surgery", "tumor_size_cm", "sex_male", "fhx", "y_ln_pos"]].dropna()
        import statsmodels.api as sm

        X = sm.add_constant(sub[["sex_male", "fhx", "age_at_surgery", "tumor_size_cm"]])
        model = sm.Logit(sub["y_ln_pos"], X).fit(disp=False, maxiter=200)
        rows = []
        for name in model.params.index:
            if name == "const":
                continue
            coef = float(model.params[name])
            se = float(model.bse[name])
            rows.append({"term": name, "log_or": coef, "se": se})
        fp = pd.DataFrame(rows)
        fp["or"] = np.exp(fp["log_or"])
        fp["ci_lo"] = np.exp(fp["log_or"] - 1.96 * fp["se"])
        fp["ci_hi"] = np.exp(fp["log_or"] + 1.96 * fp["se"])
        fp.to_csv(coef_path, index=False)

    labels = {"sex_male": "Male sex", "fhx": "Family hx thyroid (NLP)", "age_at_surgery": "Age (per year)", "tumor_size_cm": "Tumor size (per cm)"}
    fp = fp[fp["term"] != "const"] if "term" in fp.columns else fp
    fp = fp.copy()
    fp["label"] = fp["term"].map(labels).fillna(fp["term"])
    fp = fp.iloc[::-1]
    y = np.arange(len(fp))
    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.errorbar(fp["or"], y, xerr=[fp["or"] - fp["ci_lo"], fp["ci_hi"] - fp["or"]], fmt="o", color=C2, ecolor="gray", capsize=3)
    ax.axvline(1.0, color="gray", linestyle="--", linewidth=1)
    ax.set_yticks(y)
    ax.set_yticklabels(fp["label"])
    ax.set_xlabel("Odds ratio (95% CI)")
    ax.set_title("M037 — Multivariable predictors of N1+ (primary model)")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "m037_fig3_forest_primary.png"), dpi=300)
    fp.to_csv(os.path.join(FIG, "m037_fig3_forest_primary_data.csv"), index=False)
    plt.close(fig)

    # Fig 1 — cohort flow (counts from analytic spine + CPM rule summary)
    snap = {
        "step": [
            "Malignant with LN examined>0 OR LN+ flag (M037 view)",
            "With join to CPM (analytic spine)",
            "With non-missing age+tumor for primary model subset",
        ],
        "n": [len(df), len(df), df[["age_at_surgery", "tumor_size_cm"]].dropna().shape[0]],
    }
    pd.DataFrame(snap).to_csv(os.path.join(FIG, "m037_fig1_flow_data.csv"), index=False)
    fig, ax = plt.subplots(figsize=(7, 2.5))
    ax.axis("off")
    y0 = 0.85
    for i, (s, n) in enumerate(zip(snap["step"], snap["n"], strict=True)):
        ax.text(0.05, y0 - i * 0.28, f"{i+1}. {s}", fontsize=11, va="top")
        ax.text(0.92, y0 - i * 0.28, f"n = {n:,}", fontsize=11, ha="right", fontweight="bold")
    ax.set_title("M037 — Cohort assembly (schematic)")
    fig.savefig(os.path.join(FIG, "m037_fig1_cohort_flow.png"), dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Fig 4 — LN+ rate by nodal harvest (examined nodes) quartile proxy (no time-to-event field)
    ex = df["ln_total_examined"].fillna(0)
    df["exam_quartile"] = pd.qcut(ex, q=4, duplicates="drop")
    q = df.groupby("exam_quartile", observed=True).agg(rate=("y_ln_pos", "mean"), n=("research_id", "count")).reset_index()
    q.to_csv(os.path.join(FIG, "m037_fig4_ln_rate_by_examined_quartile_data.csv"), index=False)
    fig, ax = plt.subplots(figsize=(7, 4))
    x = np.arange(len(q))
    ax.bar(x, 100.0 * q["rate"], color=C3, edgecolor="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(v) for v in q["exam_quartile"]], rotation=15, ha="right")
    ax.set_ylabel("LN+ %")
    ax.set_xlabel("LN examined (quartiles)")
    ax.set_title("M037 — LN+ rate by nodal harvest intensity (quartiles)")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "m037_fig4_ln_rate_by_ln_examined_quartiles.png"), dpi=300)
    plt.close(fig)

    print(f"Figures written under {FIG}")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(__file__))
    main()
