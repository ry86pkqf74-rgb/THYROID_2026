#!/usr/bin/env python3
"""
M048 — Figure builder.

Produces 7 figures (300 dpi PNG + vector PDF) in M048_submission_package/figures/.

Race color encoding (consistent across all figures):
  Black:   #1f4e79
  White:   #7a7a7a
  Asian:   #c55a11
  Other:   #9c9c9c
  Unknown: #cfcfcf

SCOPE: Data visualisation ONLY. No narrative annotations.
QA: All figures include version + timestamp footer.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
FIG_DIR = os.path.join(REPO_ROOT, "M048_submission_package", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

RACE_COLORS = {
    "Black": "#1f4e79",
    "White": "#7a7a7a",
    "Asian": "#c55a11",
    "Other": "#9c9c9c",
    "Unknown": "#cfcfcf",
    "POOLED": "#333333",
}
PRIMARY_RACES = ["Black", "White", "Asian"]
TR_LEVELS = ["TR1", "TR2", "TR3", "TR4", "TR5"]
DPI = 300
VERSION = "M048-v1"
TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
FOOTER = f"{VERSION} | {TS}"


def save_fig(fig, basename: str) -> None:
    for ext in ("png", "pdf"):
        out = os.path.join(FIG_DIR, f"{basename}.{ext}")
        fig.savefig(out, dpi=DPI if ext == "png" else None, bbox_inches="tight")
        print(f"  Saved: {os.path.basename(out)}")
    plt.close(fig)


def add_footer(ax, text: str = FOOTER) -> None:
    ax.figure.text(0.99, 0.01, text, ha="right", va="bottom",
                   fontsize=6, color="#aaaaaa", transform=ax.figure.transFigure)


def load_csvs() -> dict[str, pd.DataFrame]:
    files = {
        "rom": "m048_rom_by_race_x_tr.csv",
        "auc": "m048_auc_by_race.csv",
        "threshold": "m048_threshold_metrics.csv",
        "inflation": "m048_inflation_by_race.csv",
        "feature": "m048_feature_distribution.csv",
        "bethesda": "m048_bethesda_x_race_x_tr.csv",
        "qa_gates": "m048_qa_gates.csv",
    }
    dfs = {}
    for k, fname in files.items():
        path = os.path.join(STUDY_DIR, fname)
        if os.path.exists(path):
            dfs[k] = pd.read_csv(path)
        else:
            print(f"  [WARN] Missing CSV: {fname}")
            dfs[k] = pd.DataFrame()
    return dfs


# ============================================================================
# Figure 1 — Cohort Flow by Race
# ============================================================================
def figure_1_cohort_flow(dfs: dict[str, pd.DataFrame]) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axis("off")

    # Pull counts from qa_gates if available
    gates = dfs.get("qa_gates", pd.DataFrame())
    def get_n(gate_name: str, default: str = "—") -> str:
        if gates.empty:
            return default
        row = gates[gates["gate"] == gate_name]
        return str(row.iloc[0]["actual"]) if not row.empty else default

    rom = dfs.get("rom", pd.DataFrame())

    boxes = []
    if not rom.empty:
        pat_rows = rom[rom["grain"] == "patient"]
        nod_rows = rom[rom["grain"] == "nodule_strict"]
        for race in PRIMARY_RACES:
            p_n = pat_rows[pat_rows["race_strat"] == race]["n_total"].sum()
            n_n = nod_rows[nod_rows["race_strat"] == race]["n_total"].sum()
            boxes.append((race, int(p_n), int(n_n)))

    # Simple flow diagram as text boxes
    y_top = 0.90
    col_x = [0.15, 0.42, 0.68]
    col_labels = ["Race stratum", "Patient n\n(max TR≠NULL)", "Strict-eligible nodules"]
    for cx, lbl in zip(col_x, col_labels):
        ax.text(cx, y_top + 0.04, lbl, ha="center", va="bottom",
                fontsize=10, fontweight="bold", transform=ax.transAxes)

    for i, (race, p_n, n_n) in enumerate(boxes):
        y = y_top - i * 0.22
        color = RACE_COLORS.get(race, "#333333")
        rect = mpatches.FancyBboxPatch(
            (col_x[0] - 0.10, y - 0.08), 0.22, 0.14,
            boxstyle="round,pad=0.02", linewidth=1.5,
            edgecolor=color, facecolor=color + "22",
            transform=ax.transAxes, clip_on=False,
        )
        ax.add_patch(rect)
        ax.text(col_x[0], y, race, ha="center", va="center",
                fontsize=11, fontweight="bold", color=color, transform=ax.transAxes)
        ax.text(col_x[1], y, f"n={p_n:,}", ha="center", va="center",
                fontsize=11, transform=ax.transAxes)
        ax.text(col_x[2], y, f"n={n_n:,}", ha="center", va="center",
                fontsize=11, transform=ax.transAxes)

    ax.set_title("Figure 1 — M048 Cohort Flow by Race", fontsize=13, fontweight="bold", pad=12)
    add_footer(ax)
    save_fig(fig, "Figure_1_Cohort_Flow_by_Race")


# ============================================================================
# Figure 2 — ROC curves by Race (patient grain)
# ============================================================================
def figure_2_roc_by_race(dfs: dict[str, pd.DataFrame]) -> None:
    df_auc = dfs.get("auc", pd.DataFrame())
    df_rom = dfs.get("rom", pd.DataFrame())
    if df_auc.empty or df_rom.empty:
        print("  [SKIP] Figure 2 — missing data")
        return

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot([0, 1], [0, 1], "k--", lw=0.8, alpha=0.5, label="Chance (AUC=0.5)")

    pat_rom = df_rom[(df_rom["grain"] == "patient") & (df_rom["race_strat"].isin(PRIMARY_RACES))]

    for race in PRIMARY_RACES:
        color = RACE_COLORS[race]
        r_auc = df_auc[(df_auc["grain"] == "patient") & (df_auc["race_strat"] == race)]
        if r_auc.empty:
            continue
        auc_val = float(r_auc.iloc[0]["auc"])
        ci_lo = float(r_auc.iloc[0]["auc_ci_lo_95"])
        ci_hi = float(r_auc.iloc[0]["auc_ci_hi_95"])

        # Build empirical ROC from ROM table (TR1–TR5 thresholds)
        sub = pat_rom[pat_rom["race_strat"] == race].copy()
        if sub.empty:
            continue
        sub["tr_int"] = sub["tr_category"].str.extract(r"(\d+)").astype(float)
        sub = sub.sort_values("tr_int")

        # Compute sens / 1-spec at each threshold (TR≥k)
        total_pos = sub["n_malignant"].sum()
        total_neg = (sub["n_total"] - sub["n_malignant"]).sum()
        if total_pos == 0 or total_neg == 0:
            continue
        tpr_pts = [0.0]
        fpr_pts = [0.0]
        for thr in range(5, 0, -1):
            above = sub[sub["tr_int"] >= thr]
            tp = above["n_malignant"].sum()
            fp = (above["n_total"] - above["n_malignant"]).sum()
            tpr_pts.append(tp / total_pos)
            fpr_pts.append(fp / total_neg)
        tpr_pts.append(1.0)
        fpr_pts.append(1.0)

        label = f"{race} (AUC={auc_val:.3f} [{ci_lo:.3f}–{ci_hi:.3f}])"
        ax.plot(fpr_pts, tpr_pts, "-o", color=color, label=label, linewidth=2, markersize=5)

    ax.set_xlabel("1 – Specificity (FPR)", fontsize=12)
    ax.set_ylabel("Sensitivity (TPR)", fontsize=12)
    ax.set_title("Figure 2 — ROC by Race (Patient Grain, Bootstrap 95% CI)", fontsize=12)
    ax.legend(fontsize=9, loc="lower right")
    ax.grid(True, linestyle=":", alpha=0.4)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    add_footer(ax)
    save_fig(fig, "Figure_2_ROC_by_Race")


# ============================================================================
# Figure 3 — ROM by Race × TR (Patient grain)
# Figure 3b — ROM by Race × TR (Nodule strict grain)
# ============================================================================
def figure_3_rom(dfs: dict[str, pd.DataFrame], grain: str, fig_num: str, title_suffix: str) -> None:
    df = dfs.get("rom", pd.DataFrame())
    if df.empty:
        print(f"  [SKIP] Figure {fig_num} — no data")
        return
    df = df[df["grain"] == grain].copy()
    df["tr_int"] = df["tr_category"].str.extract(r"(\d+)").astype(float)
    df = df[df["tr_int"].notna() & df["race_strat"].isin(PRIMARY_RACES)]

    tr_vals = sorted(df["tr_int"].unique())
    x = np.arange(len(tr_vals))
    width = 0.22
    offsets = [-width, 0, width]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    for i, race in enumerate(PRIMARY_RACES):
        color = RACE_COLORS[race]
        sub = df[df["race_strat"] == race].sort_values("tr_int")
        roms, los, his = [], [], []
        for tv in tr_vals:
            row = sub[sub["tr_int"] == tv]
            if row.empty:
                roms.append(0.0); los.append(0.0); his.append(0.0)
            else:
                r = row.iloc[0]
                roms.append(float(r.get("rom_pct", 0) or 0))
                lo = float(r.get("rom_lo_95", 0) or 0)
                hi = float(r.get("rom_hi_95", 0) or 0)
                rom_pt = roms[-1]
                los.append(rom_pt - lo)
                his.append(hi - rom_pt)
        roms = np.array(roms)
        err = np.array([los, his])
        bars = ax.bar(x + offsets[i], roms, width, label=race,
                      color=color, alpha=0.85, edgecolor="white")
        ax.errorbar(x + offsets[i], roms, yerr=err, fmt="none",
                    ecolor="black", capsize=3, linewidth=1.0)

    ax.set_xticks(x)
    ax.set_xticklabels([f"TR{int(t)}" for t in tr_vals], fontsize=11)
    ax.set_ylabel("Risk of Malignancy (%)", fontsize=12)
    ax.set_title(f"Figure {fig_num} — ROM by Race × TI-RADS ({title_suffix})\nError bars: Wilson 95% CI",
                 fontsize=12)
    ax.legend(title="Race", fontsize=10)
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    ax.set_ylim(0, min(100, df["rom_pct"].max() * 1.25 + 5) if not df.empty else 80)
    add_footer(ax)
    save_fig(fig, f"Figure_{fig_num}_ROM_by_Race_{'Patient' if grain == 'patient' else 'Nodule'}")


# ============================================================================
# Figure 4 — Inflation Forest Plot by Race × TR
# ============================================================================
def figure_4_inflation(dfs: dict[str, pd.DataFrame]) -> None:
    df = dfs.get("inflation", pd.DataFrame())
    if df.empty:
        print("  [SKIP] Figure 4 — no inflation data")
        return
    df = df[df["race_strat"].isin(PRIMARY_RACES)].copy()

    labels = []
    # Rebuild properly
    rows_for_plot = []
    for race in PRIMARY_RACES:
        for tr in ["TR4", "TR5"]:
            sub = df[(df["race_strat"] == race) & (df["tr_category"] == tr)]
            if not sub.empty:
                r = sub.iloc[0]
                rows_for_plot.append({
                    "label": f"{race} | {tr}",
                    "race": race,
                    "tr": tr,
                    "inflation_pp": float(r.get("inflation_pp", np.nan) or np.nan),
                    "patient_rom": float(r.get("patient_rom_pct", np.nan) or np.nan),
                    "nodule_rom": float(r.get("nodule_rom_pct", np.nan) or np.nan),
                })

    if not rows_for_plot:
        print("  [SKIP] Figure 4 — empty after filtering")
        return

    df_plot = pd.DataFrame(rows_for_plot)
    y_pos = np.arange(len(df_plot))[::-1]

    fig, ax = plt.subplots(figsize=(9, max(4, 0.6 * len(df_plot) + 1.5)))
    ax.axvline(0, color="gray", linestyle="--", linewidth=0.8)

    for i, (_, row) in enumerate(df_plot.iterrows()):
        color = RACE_COLORS.get(row["race"], "#333")
        marker = "s" if row["tr"] == "TR4" else "D"
        val = row["inflation_pp"]
        if not np.isnan(val):
            ax.plot(val, y_pos[i], marker=marker, color=color, markersize=9, zorder=3)
            ax.text(val + 0.5, y_pos[i], f"{val:+.1f} pp", va="center", fontsize=8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(df_plot["label"].tolist(), fontsize=10)
    ax.set_xlabel("Patient ROM − Nodule ROM (percentage points)", fontsize=11)
    ax.set_title("Figure 4 — Patient–Nodule ROM Inflation by Race × TI-RADS", fontsize=12)
    # Legend
    handles = [
        mpatches.Patch(color=RACE_COLORS[r], label=r) for r in PRIMARY_RACES
    ] + [
        plt.Line2D([0], [0], marker="s", color="gray", linestyle="None", markersize=8, label="TR4"),
        plt.Line2D([0], [0], marker="D", color="gray", linestyle="None", markersize=8, label="TR5"),
    ]
    ax.legend(handles=handles, fontsize=9, loc="lower right")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    add_footer(ax)
    save_fig(fig, "Figure_4_Inflation_by_Race")


# ============================================================================
# Figure 5 — Feature Score Distribution (5 small multiples)
# ============================================================================
def figure_5_features(dfs: dict[str, pd.DataFrame]) -> None:
    df = dfs.get("feature", pd.DataFrame())
    if df.empty:
        print("  [SKIP] Figure 5 — no feature data")
        return
    df = df[df["race_strat"].isin(PRIMARY_RACES)].copy()
    features = ["composition", "echogenicity", "shape", "margin", "foci"]
    feature_labels = {
        "composition": "Composition",
        "echogenicity": "Echogenicity",
        "shape": "Shape",
        "margin": "Margin",
        "foci": "Echogenic Foci",
    }

    fig, axes = plt.subplots(1, 5, figsize=(16, 5), sharey=False)
    for ax, feat in zip(axes, features):
        sub = df[df["feature"] == feat].copy()
        scores = sorted(sub["score"].dropna().unique())
        x = np.arange(len(scores))
        width = 0.22
        offsets = [-width, 0, width]
        for i, race in enumerate(PRIMARY_RACES):
            r_sub = sub[sub["race_strat"] == race]
            # Compute proportion within race
            race_total = r_sub["n"].sum()
            pcts = []
            for sc in scores:
                row = r_sub[r_sub["score"] == sc]
                n = int(row["n"].sum()) if not row.empty else 0
                pcts.append(100.0 * n / race_total if race_total > 0 else 0.0)
            ax.bar(x + offsets[i], pcts, width, label=race,
                   color=RACE_COLORS[race], alpha=0.85, edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels([str(int(s) if s == int(s) else s) for s in scores], fontsize=8)
        ax.set_title(feature_labels.get(feat, feat), fontsize=10, fontweight="bold")
        ax.set_ylabel("% within race" if feat == "composition" else "", fontsize=9)
        ax.grid(axis="y", linestyle=":", alpha=0.4)
        ax.set_ylim(0, 100)

    handles = [mpatches.Patch(color=RACE_COLORS[r], label=r) for r in PRIMARY_RACES]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=10,
               title="Race", bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Figure 5 — ACR TI-RADS Feature Score Distribution by Race\n"
                 "(Strict-eligible nodules; % within race stratum)",
                 fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0.06, 1, 0.95])
    axes[0].figure.text(0.99, 0.005, FOOTER, ha="right", va="bottom", fontsize=6, color="#aaaaaa")
    save_fig(fig, "Figure_5_Feature_Distribution")


# ============================================================================
# Figure S1 — Bethesda × Race × TR (Supplementary Heatmap)
# ============================================================================
def figure_s1_bethesda(dfs: dict[str, pd.DataFrame]) -> None:
    df = dfs.get("bethesda", pd.DataFrame())
    if df.empty or "bethesda_bucket" not in df.columns:
        print("  [SKIP] Figure S1 — no bethesda data or missing column")
        return

    df = df[df["race_strat"].isin(PRIMARY_RACES)].copy()
    fig, axes = plt.subplots(1, 3, figsize=(16, 6), sharey=True)

    bethesda_order = ["I", "II", "III", "IV", "V", "VI"]
    # Filter to known Bethesda buckets
    df["beth_str"] = df["bethesda_bucket"].apply(lambda x: str(x) if pd.notna(x) else "Unknown")
    known_beth = [b for b in bethesda_order if b in df["beth_str"].unique()]
    tr_order = [1, 2, 3, 4, 5]

    for ax, race in zip(axes, PRIMARY_RACES):
        sub = df[df["race_strat"] == race].copy()
        sub["tr_int"] = pd.to_numeric(sub["tr_category"], errors="coerce")

        pivot = pd.DataFrame(index=known_beth or df["beth_str"].unique(),
                             columns=tr_order, data=0.0)
        for _, row in sub.iterrows():
            b = str(row.get("bethesda_bucket", "Unknown"))
            t = row.get("tr_int", np.nan)
            if pd.notna(t) and b in pivot.index:
                pivot.loc[b, int(t)] = float(pivot.loc[b, int(t)]) + float(row.get("n", 0))

        # Normalize by TR total
        pivot_norm = pivot.div(pivot.sum(axis=0).replace(0, np.nan), axis=1) * 100

        color = RACE_COLORS[race]
        im = ax.imshow(pivot_norm.values.astype(float), aspect="auto",
                       cmap=plt.cm.YlOrRd, vmin=0, vmax=100)
        ax.set_xticks(range(len(tr_order)))
        ax.set_xticklabels([f"TR{t}" for t in tr_order], fontsize=10)
        ax.set_yticks(range(len(pivot_norm.index)))
        ax.set_yticklabels(list(pivot_norm.index), fontsize=10)
        ax.set_title(f"{race}", fontsize=12, fontweight="bold", color=color)
        ax.set_xlabel("Max TI-RADS Category", fontsize=10)
        if ax == axes[0]:
            ax.set_ylabel("Bethesda Category", fontsize=10)
        # Annotate cells
        for r_i in range(pivot_norm.shape[0]):
            for c_i in range(pivot_norm.shape[1]):
                val = pivot_norm.iloc[r_i, c_i]
                if not np.isnan(val) and val > 0:
                    ax.text(c_i, r_i, f"{val:.0f}%", ha="center", va="center",
                            fontsize=7, color="black" if val < 60 else "white")

    fig.colorbar(im, ax=axes[-1], fraction=0.046, pad=0.04, label="% within TR (column-normalized)")
    fig.suptitle("Figure S1 — Bethesda × Race × Max TI-RADS Category\n(Column-normalized; % of patients at each TR)",
                 fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.text(0.99, 0.01, FOOTER, ha="right", va="bottom", fontsize=6, color="#aaaaaa")
    save_fig(fig, "Figure_S1_Bethesda_x_Race_x_TR")


# ============================================================================
# Main
# ============================================================================
def main():
    print("=" * 70)
    print("M048 Figure Builder")
    print("=" * 70)
    print(f"Output dir: {FIG_DIR}")
    print(f"DPI: {DPI}")
    print()

    dfs = load_csvs()

    print("[FIG 1] Cohort Flow by Race ...")
    figure_1_cohort_flow(dfs)

    print("[FIG 2] ROC by Race ...")
    figure_2_roc_by_race(dfs)

    print("[FIG 3] ROM by Race × TR (Patient) ...")
    figure_3_rom(dfs, "patient", "3", "Patient Grain")

    print("[FIG 3b] ROM by Race × TR (Nodule strict) ...")
    figure_3_rom(dfs, "nodule_strict", "3b", "Nodule Strict Grain")

    print("[FIG 4] Inflation Forest ...")
    figure_4_inflation(dfs)

    print("[FIG 5] Feature Distribution (5 small multiples) ...")
    figure_5_features(dfs)

    print("[FIG S1] Bethesda × Race × TR Heatmap ...")
    figure_s1_bethesda(dfs)

    print(f"\n[DONE] All figures written to {FIG_DIR}")


if __name__ == "__main__":
    main()
