#!/usr/bin/env python3
"""
34_publication_figures.py — Generate publication-ready figures from analytic model outputs.

Outputs to studies/manuscript_draft/figures/:
  - fig_psm_balance.png       — Love plot (SMD before/after matching)
  - fig_psm_sensitivity.png   — Forest plot of HR across caliper levels
  - fig_km_ete_matched.png    — KM curves for ETE+/ETE- in matched cohort
  - fig_subgroup_forest.png   — Forest plot of event rates by subgroup
  - fig_stage_distribution.png — AJCC stage bar chart

Usage:
    .venv/bin/python scripts/34_publication_figures.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

np.random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "studies" / "analytic_models"
FIG_DIR = ROOT / "studies" / "manuscript_draft" / "figures"


plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "axes.grid.axis": "x",
    "grid.alpha": 0.3,
    "figure.dpi": 150,
})


def fig_psm_balance():
    """Love plot showing SMD for each confounder."""
    path = INPUT_DIR / "psm_balance.csv"
    if not path.exists():
        print("  [SKIP] psm_balance.csv not found")
        return
    df = pd.read_csv(path)

    fig, ax = plt.subplots(figsize=(7, max(3, len(df) * 0.7 + 1)))
    y_pos = range(len(df))

    colors = ["#2196F3" if b else "#F44336" for b in df["Balanced"]]
    ax.barh(y_pos, df["SMD"], color=colors, height=0.5, edgecolor="none")
    ax.axvline(x=0.1, color="#888", linestyle="--", linewidth=1, label="SMD = 0.1 threshold")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df["Variable"], fontsize=10)
    ax.set_xlabel("Standardized Mean Difference (SMD)")
    ax.set_title("Covariate Balance After Propensity-Score Matching", fontsize=13)
    ax.invert_yaxis()

    balanced_patch = mpatches.Patch(color="#2196F3", label="Balanced (SMD < 0.1)")
    imbalanced_patch = mpatches.Patch(color="#F44336", label="Imbalanced (SMD ≥ 0.1)")
    ax.legend(handles=[balanced_patch, imbalanced_patch], fontsize=9, loc="lower right")

    for i, smd in enumerate(df["SMD"]):
        ax.text(smd + 0.003, i, f"{smd:.3f}", va="center", fontsize=9, color="#333")

    plt.tight_layout()
    out = FIG_DIR / "fig_psm_balance.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out.name}")


def fig_psm_sensitivity():
    """Forest plot of HR across caliper levels."""
    path = INPUT_DIR / "psm_sensitivity.csv"
    if not path.exists():
        print("  [SKIP] psm_sensitivity.csv not found")
        return
    df = pd.read_csv(path)

    fig, ax = plt.subplots(figsize=(8, max(3, len(df) * 0.7 + 1.5)))
    y_pos = range(len(df))
    labels = [f"{r['Caliper_mult']:.2f}×SD (n={r['Matched_pairs']:,})" for _, r in df.iterrows()]

    ax.errorbar(df["HR"], y_pos,
                xerr=[df["HR"] - df["CI_lower"], df["CI_upper"] - df["HR"]],
                fmt="o", color="#1565C0", ecolor="#90CAF9", elinewidth=2,
                capsize=4, markersize=7, markeredgecolor="#0D47A1")
    ax.axvline(x=1.0, color="#888", linestyle="--", linewidth=1, label="HR = 1.0 (null)")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Hazard Ratio (95% CI)")
    ax.set_title("PSM Sensitivity Analysis: ETE Effect Across Caliper Levels", fontsize=13)
    ax.invert_yaxis()

    for i, r in df.iterrows():
        sig = "★" if r["p_value"] < 0.05 else ""
        ax.text(r["CI_upper"] + 0.05, i,
                f"HR={r['HR']:.2f}, p={r['p_value']:.3f} {sig}",
                va="center", fontsize=9, color="#333")

    ax.legend(fontsize=9, loc="upper right")
    plt.tight_layout()
    out = FIG_DIR / "fig_psm_sensitivity.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out.name}")


def fig_km_ete_matched():
    """KM bar chart for matched ETE+/ETE- at 1, 3, 5 years."""
    path = INPUT_DIR / "psm_km_summary.csv"
    if not path.exists():
        print("  [SKIP] psm_km_summary.csv not found")
        return
    df = pd.read_csv(path)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(3)
    width = 0.35
    timepoints = ["1yr", "3yr", "5yr"]
    labels_x = ["1 Year", "3 Years", "5 Years"]

    colors = {"ETE+": "#E53935", "ETE-": "#1E88E5"}
    for i, (_, row) in enumerate(df.iterrows()):
        vals = [row.get(t, 0) * 100 for t in timepoints]
        offset = -width / 2 if i == 0 else width / 2
        bars = ax.bar(x + offset, vals, width, label=f"{row['Group']} (n={row['N']:,})",
                      color=colors.get(row["Group"], "#999"), edgecolor="none")
        for bar, v in zip(bars, vals):
            if v and v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                        f"{v:.1f}%", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(labels_x)
    ax.set_ylabel("Recurrence-Free Survival (%)")
    ax.set_ylim(90, 101)
    ax.set_title("Matched Cohort: Recurrence-Free Survival by ETE Status", fontsize=13)
    ax.legend(fontsize=10)
    plt.tight_layout()
    out = FIG_DIR / "fig_km_ete_matched.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out.name}")


def fig_subgroup_forest():
    """Forest plot of event rates by subgroup."""
    path = INPUT_DIR / "subgroup_event_rates.csv"
    if not path.exists():
        print("  [SKIP] subgroup_event_rates.csv not found")
        return
    df = pd.read_csv(path)
    df = df[df["N"] >= 10].copy()

    fig, ax = plt.subplots(figsize=(8, max(4, len(df) * 0.45 + 1)))
    y_pos = range(len(df))

    ax.barh(y_pos, df["Event_rate_pct"], color="#26A69A", height=0.5, edgecolor="none")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df["Subgroup"], fontsize=10)
    ax.set_xlabel("Event Rate (%)")
    ax.set_title("Recurrence Event Rates by Subgroup", fontsize=13)
    ax.invert_yaxis()

    for i, (_, r) in enumerate(df.iterrows()):
        ax.text(r["Event_rate_pct"] + 0.05, i,
                f"{r['Event_rate_pct']:.2f}% ({r['Events']}/{r['N']})",
                va="center", fontsize=9, color="#333")

    plt.tight_layout()
    out = FIG_DIR / "fig_subgroup_forest.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out.name}")


def fig_stage_distribution():
    """AJCC stage distribution from KM summary."""
    path = INPUT_DIR / "km_summary.csv"
    if not path.exists():
        print("  [SKIP] km_summary.csv not found")
        return
    df = pd.read_csv(path)
    stages = df[df["Stratifier"].isin(["AJCC 8th Stage", "Overall Stage"])].copy()
    if stages.empty:
        print("  [SKIP] No stage data in km_summary.csv")
        return

    colors = ["#2196F3", "#FF9800", "#F44336", "#9C27B0", "#880E4F", "#37474F"]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(stages["Stratum"], stages["N"],
                  color=colors[:len(stages)], edgecolor="none")
    for bar, n in zip(bars, stages["N"]):
        pct = 100 * n / stages["N"].sum()
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + stages["N"].max() * 0.02,
                f"{n:,}\n({pct:.1f}%)", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Stage")
    ax.set_ylabel("Number of Patients")
    ax.set_title(f"AJCC Stage Distribution (N={stages['N'].sum():,})", fontsize=13)
    plt.tight_layout()
    out = FIG_DIR / "fig_stage_distribution.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {out.name}")


def main() -> int:
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print("  Publication Figures Generator")
    print(f"  Input:  {INPUT_DIR.relative_to(ROOT)}")
    print(f"  Output: {FIG_DIR.relative_to(ROOT)}")
    print("=" * 68)

    fig_psm_balance()
    fig_psm_sensitivity()
    fig_km_ete_matched()
    fig_subgroup_forest()
    fig_stage_distribution()

    print()
    print("=" * 68)
    for f in sorted(FIG_DIR.iterdir()):
        if f.is_file():
            sz_kb = f.stat().st_size / 1024
            print(f"  ✓  {f.name:<40} {sz_kb:>7.1f} KB")
    print("=" * 68)
    print()
    print("  Figures ready for manuscript.")
    print("  Insert PNGs into Word/PowerPoint or reference in LaTeX.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
