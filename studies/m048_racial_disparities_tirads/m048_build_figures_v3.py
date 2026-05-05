#!/usr/bin/env python3
"""
M048 v3 figures (6–13): 300 DPI PNG + vector PDF under M048_submission_package/figures/v3/.
Reads CSVs from studies/m048_racial_disparities_tirads/v3/.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
V3_DIR = os.path.join(STUDY_DIR, "v3")
FIG_DIR = os.path.join(REPO_ROOT, "M048_submission_package", "figures", "v3")

RACE_COLORS = {
    "Black": "#1f4e79",
    "White": "#7a7a7a",
    "Asian": "#c55a11",
}
PRIMARY_RACES = ["Black", "White", "Asian"]
DPI = 300
VERSION = "M048-v3"
TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
FOOTER = f"{VERSION} | {TS}"


def save_fig(fig, basename: str) -> None:
    os.makedirs(FIG_DIR, exist_ok=True)
    for ext in ("png", "pdf"):
        out = os.path.join(FIG_DIR, f"{basename}.{ext}")
        fig.savefig(out, dpi=DPI if ext == "png" else None, bbox_inches="tight")
        print(f"  saved {os.path.basename(out)}")
    plt.close(fig)


def add_footer(fig, text: str = FOOTER) -> None:
    fig.text(0.99, 0.01, text, ha="right", va="bottom", fontsize=6, color="#888888", transform=fig.transFigure)


def load(name: str) -> pd.DataFrame:
    p = os.path.join(V3_DIR, name)
    if not os.path.isfile(p):
        print(f"  [WARN] missing {name}")
        return pd.DataFrame()
    return pd.read_csv(p)


def figure_6_forest_ful() -> None:
    df = load("m048_v3_full_model_OR.csv")
    if df.empty:
        return
    mask = df["param"].astype(str).str.contains("race_strat", na=False) & df["param"].astype(str).str.contains(
        r"\[T\.(Black|Asian)\]", regex=True, na=False
    )
    sub = df.loc[mask].copy()
    if sub.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 3.5))
    y = np.arange(len(sub))
    lbls = []
    for p in sub["param"].astype(str):
        for lv in PRIMARY_RACES:
            if f"[T.{lv}]" in p:
                lbls.append(f"{lv} vs White")
                break
        else:
            lbls.append(p[:32])
    ax.axvline(1.0, color="#333", lw=0.8, ls="--")
    for i, (_, r) in enumerate(sub.iterrows()):
        lv = PRIMARY_RACES[0]
        for cand in PRIMARY_RACES:
            if cand in str(r["param"]):
                lv = cand
                break
        c = RACE_COLORS.get(lv, "#333333")
        ax.plot([r["ci_lo"], r["ci_hi"]], [i, i], color=c, lw=3, solid_capstyle="round")
        ax.plot(r["or"], i, "o", color=c, ms=8)
    ax.set_yticks(y)
    ax.set_yticklabels(lbls)
    ax.set_xlabel("Adjusted OR (95% CI)")
    ax.set_title("Figure 6 — Model F race effects (full adjustment)")
    ax.invert_yaxis()
    add_footer(fig)
    save_fig(fig, "Figure_6_Adjusted_OR_Forest")


def figure_7_cascade() -> None:
    df = load("m048_v3_attenuation_cascade.csv")
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    order = ["m0_race_only", "m1_tr", "m2_burden", "m3_genetics_nm", "m4_background", "m5_fna_path", "m6_full"]
    for race, color in [("Black", RACE_COLORS["Black"]), ("Asian", RACE_COLORS["Asian"])]:
        xs, ys = [], []
        for j, step in enumerate(order):
            row = df[(df["model_step"] == step) & (df["race_level"] == race)]
            if row.empty or not np.isfinite(row.iloc[0].get("or", np.nan)):
                continue
            ys.append(float(row.iloc[0]["or"]))
            xs.append(j)
        if xs:
            ax.plot(xs, ys, "o-", color=color, label=race, lw=2, ms=7)
    ax.axhline(1.0, color="#999", ls="--", lw=0.8)
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels([o.replace("m", "M").replace("_", " ") for o in order], rotation=25, ha="right")
    ax.set_ylabel("OR vs White")
    ax.set_title("Figure 7 — Attenuation cascade (race OR by model step)")
    ax.legend()
    add_footer(fig)
    save_fig(fig, "Figure_7_Attenuation_Cascade")


def figure_8_interaction() -> None:
    df = load("m048_v3_interaction_race_x_tr.csv")
    if df.empty:
        return
    sub = df[df["param"].astype(str).str.contains(":", na=False)].copy()
    fig, ax = plt.subplots(figsize=(8, 4))
    y = np.arange(len(sub))
    ax.axvline(0, color="#333", lw=0.8)
    for i, (_, r) in enumerate(sub.iterrows()):
        ax.barh(i, r["coef"], color="#4a6fa5", height=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels(sub["param"].astype(str).str.slice(0, 40))
    ax.set_xlabel("Interaction coefficient (log-odds)")
    ax.set_title("Figure 8 — Race × max TR (Model I)")
    ax.invert_yaxis()
    add_footer(fig)
    save_fig(fig, "Figure_8_Race_x_TR_Interaction")


def figure_9_mediation() -> None:
    df = load("m048_v3_mediation.csv")
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    df = df.sort_values("indirect_mean")
    y = np.arange(len(df))
    err_lo = (df["indirect_mean"] - df["ci_lo"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    err_hi = (df["ci_hi"] - df["indirect_mean"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    ax.barh(y, df["indirect_mean"].values, xerr=[err_lo.values, err_hi.values], color="#5c7a99", capsize=3)
    ax.set_yticks(y)
    ax.set_yticklabels(df["mediator"])
    ax.axvline(0, color="#333", lw=0.8)
    ax.set_xlabel("Indirect effect (Black vs White, product-of-coefs bootstrap)")
    ax.set_title("Figure 9 — Mediation (univariate mediators)")
    add_footer(fig)
    save_fig(fig, "Figure_9_Mediation_Diagram")


def figure_10_love() -> None:
    df = load("m048_v3_covariate_balance.csv")
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, max(5, len(df) * 0.12)))
    for race, c in [("Black", RACE_COLORS["Black"]), ("Asian", RACE_COLORS["Asian"])]:
        sub = df[df["race_strat"] == race]
        ax.scatter(sub["smd"], sub["variable"], color=c, alpha=0.75, label=race, s=40)
    ax.axvline(0, color="#333", lw=0.8)
    ax.axvline(0.1, color="#c44", ls=":", lw=0.8)
    ax.axvline(-0.1, color="#c44", ls=":", lw=0.8)
    ax.set_xlabel("SMD vs White")
    ax.set_title("Figure 10 — Covariate balance (Love plot)")
    ax.legend()
    add_footer(fig)
    save_fig(fig, "Figure_10_Covariate_Balance_Love")


def figure_11_quadrant() -> None:
    df = load("m048_v3_disparity_direction_table.csv")
    if df.empty:
        return
    sub = df[df["tr_category"].isin(["TR4", "TR5"])].copy()
    sz = pd.to_numeric(sub["mean_tumor_size_cm"], errors="coerce")
    z = (sz - sz.mean()) / (sz.std(ddof=0) or 1.0)
    fig, ax = plt.subplots(figsize=(8, 6))
    for race in PRIMARY_RACES:
        m = sub["race_strat"] == race
        if not np.any(m.values):
            continue
        rom = pd.to_numeric(sub.loc[m, "rom_pct"], errors="coerce")
        zz = z.loc[m].to_numpy(dtype=float)
        ncell = pd.to_numeric(sub.loc[m, "n_malignant_cell"], errors="coerce").fillna(0)
        sizes = np.clip(np.sqrt(ncell.values) * 10, 30, 220)
        ax.scatter(
            rom,
            zz,
            c=RACE_COLORS.get(race, "#333"),
            s=sizes,
            alpha=0.8,
            label=race,
            edgecolors="white",
            linewidths=0.5,
        )
        for (_, row), xi, yi in zip(sub.loc[m].iterrows(), rom.values, zz):
            ax.annotate(
                str(row["tr_category"]),
                (float(xi), float(yi)),
                fontsize=8,
                xytext=(4, 3),
                textcoords="offset points",
                alpha=0.75,
            )
    ax.axhline(0, color="#999", ls="--", lw=0.6)
    ax.set_xlabel("ROM % (M025 patient grain)")
    ax.set_ylabel("Mean tumor size (Z within TR4–5 cells)")
    ax.set_title("Figure 11 — Disparity-direction quadrant")
    ax.legend()
    add_footer(fig)
    save_fig(fig, "Figure_11_Disparity_Direction_Quadrant")


def figure_12_bethesda() -> None:
    df = load("m048_v3_bethesda_stratified_TR_ROM.csv")
    if df.empty:
        return
    pivot = df.pivot_table(index="bethesda_bucket", columns="race_level", values="or", aggfunc="first")
    mat = np.nan_to_num(pivot.values.astype(float), nan=1.0)
    fig, ax = plt.subplots(figsize=(9, 5))
    im = ax.imshow(mat, aspect="auto", cmap="RdBu_r", vmin=0.3, vmax=3.0)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index.astype(str))
    ax.set_title("Figure 12 — Bethesda-stratified race OR (TR slope model)")
    fig.colorbar(im, ax=ax, label="OR vs White")
    add_footer(fig)
    save_fig(fig, "Figure_12_Bethesda_Stratified_TR_ROM")


def figure_12b_bethesda_rom_heatmap() -> None:
    """ROM% heatmap faceted by race (x = TR category, y = Bethesda bucket)."""
    df = load("m048_v3_bethesda_x_race_x_tr_rom.csv")
    if df.empty:
        return
    races = [r for r in PRIMARY_RACES if r in df["race_strat"].unique()]
    if not races:
        return
    fig, axes = plt.subplots(1, len(races), figsize=(5 * len(races), 5), sharey=True)
    if len(races) == 1:
        axes = [axes]
    tr_order = sorted(df["tr_category"].dropna().astype(str).unique())
    beth_order = sorted(df["bethesda_bucket"].dropna().astype(str).unique())
    for ax, race in zip(axes, races):
        sub = df[df["race_strat"] == race].copy()
        mat = pd.DataFrame(index=beth_order, columns=tr_order, dtype=float)
        for _, row in sub.iterrows():
            b = str(row["bethesda_bucket"])
            t = str(row["tr_category"])
            v = pd.to_numeric(row.get("rom_pct", np.nan), errors="coerce")
            if b in mat.index and t in mat.columns:
                mat.loc[b, t] = v
        mat_arr = mat.values.astype(float)
        im = ax.imshow(mat_arr, aspect="auto", cmap="YlOrRd", vmin=0, vmax=100)
        ax.set_xticks(range(len(tr_order)))
        ax.set_xticklabels(tr_order, rotation=30, ha="right")
        ax.set_yticks(range(len(beth_order)))
        ax.set_yticklabels(beth_order if ax is axes[0] else [])
        ax.set_title(race, color=RACE_COLORS.get(race, "#333"))
        for i in range(len(beth_order)):
            for j in range(len(tr_order)):
                val = mat_arr[i, j]
                if np.isfinite(val):
                    n_cell = sub[
                        (sub["bethesda_bucket"].astype(str) == beth_order[i])
                        & (sub["tr_category"].astype(str) == tr_order[j])
                    ]["n"]
                    n_val = int(n_cell.iloc[0]) if len(n_cell) else 0
                    txt = f"{val:.0f}%\n(n={n_val})" if n_val >= 10 else f"({n_val})"
                    ax.text(j, i, txt, ha="center", va="center", fontsize=7,
                            color="white" if val > 60 else "black")
    fig.colorbar(im, ax=axes[-1], label="ROM %", shrink=0.8)
    fig.suptitle("Figure 12b — Bethesda × race × TR ROM % (cell-level)", fontsize=11)
    add_footer(fig)
    save_fig(fig, "Figure_12b_Bethesda_x_Race_x_TR_ROM")


def figure_13_fna_pattern() -> None:
    df = load("m048_v3_fna_pattern_by_race.csv")
    if df.empty:
        return
    df = df[df["race_strat"].isin(PRIMARY_RACES)].copy()
    subplot_specs = [
        ("pct_with_fna", "% with any FNA", "Percentage (%)"),
        ("mean_fnas_per_patient", "Mean FNAs / patient", "Count"),
        ("pct_repeat_fna_among_biopsied", "% repeat FNA (among biopsied)", "Percentage (%)"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    width = 0.25
    x = np.arange(len(PRIMARY_RACES))
    for ax, (metric, subtitle, ylabel) in zip(axes, subplot_specs):
        vals = []
        colors = []
        for race in PRIMARY_RACES:
            row = df[df["race_strat"] == race]
            v = float(row.iloc[0].get(metric, 0) or 0) if not row.empty else 0.0
            vals.append(v)
            colors.append(RACE_COLORS[race])
        bars = ax.bar(x, vals, width * 2, color=colors)
        ax.set_xticks(x)
        ax.set_xticklabels(PRIMARY_RACES, rotation=15, ha="right")
        ax.set_ylabel(ylabel)
        ax.set_title(subtitle, fontsize=9)
        if "pct" in metric:
            ax.set_ylim(0, 100)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{val:.1f}", ha="center", va="bottom", fontsize=8)
    fig.suptitle("Figure 13 — FNA pattern by race", fontsize=11)
    # Single legend for the whole figure
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=RACE_COLORS[r], label=r) for r in PRIMARY_RACES]
    fig.legend(handles=handles, loc="upper right", fontsize=8, bbox_to_anchor=(0.99, 0.95))
    add_footer(fig)
    fig.tight_layout(rect=[0, 0, 0.88, 1])
    save_fig(fig, "Figure_13_FNA_Pattern_by_Race")


def main() -> None:
    if not os.path.isdir(V3_DIR):
        print(f"Missing {V3_DIR}; run m048_run_analysis_v3.py first.")
        sys.exit(1)
    figure_6_forest_ful()
    figure_7_cascade()
    figure_8_interaction()
    figure_9_mediation()
    figure_10_love()
    figure_11_quadrant()
    figure_12_bethesda()
    figure_12b_bethesda_rom_heatmap()
    figure_13_fna_pattern()
    meta = {"figures_dir": FIG_DIR, "generated_utc": TS, "inputs": V3_DIR}
    with open(os.path.join(FIG_DIR, "figure_build_meta_v3.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[DONE] figures -> {FIG_DIR}")


if __name__ == "__main__":
    main()
