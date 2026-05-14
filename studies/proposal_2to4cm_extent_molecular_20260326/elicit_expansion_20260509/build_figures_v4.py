#!/usr/bin/env python3
"""build_figures_v4.py — Figures 2–4 for EXT2-4 v4 (any preop US 2–4 cm cohort).

Reads:
  tables/table3_v4_diagnostic_performance_actual_reported_call.csv
  tables/table3_v4_rom_pct_descriptive_stats.csv
Figure 1 is produced by build_elicit_expansion_v4.py.
Figure 4 reuses manuscript-cohort era rows (same as v2/v3 surgical denominator).
Outputs *_v4.png/pdf under figures/.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

ROOT = Path(__file__).resolve().parent
FIG = ROOT / "figures"
TABLE3 = ROOT / "tables" / "table3_v4_diagnostic_performance_actual_reported_call.csv"
ROMCSV = ROOT / "tables" / "table3_v4_rom_pct_descriptive_stats.csv"
FIG.mkdir(exist_ok=True)


def parse_pct_ci(s: str) -> tuple[float | None, float | None, float | None]:
    """Parse '80.0% [49.0–94.3]' -> pt, lo, hi as percentages."""
    if not s or s.strip() == "—":
        return None, None, None
    m = re.match(r"([\d.]+)%\s*\[([\d.]+)[–\-]([\d.]+)\]", s.strip())
    if not m:
        return None, None, None
    return float(m.group(1)), float(m.group(2)), float(m.group(3))


def load_forest_rows() -> list[tuple]:
    forest: list[tuple] = []
    with TABLE3.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            if not row["Histology rule"].startswith("Strict"):
                continue
            if row["Bethesda"] not in ("B3+B4",):
                continue
            if row["Size band"] not in ("all_sizes", "2to4cm"):
                continue
            plat = row["Platform"]
            band = row["Size band"]
            n = int(row["n_2x2"])
            sens = parse_pct_ci(row["Sensitivity (95% CI)"])
            spec = parse_pct_ci(row["Specificity (95% CI)"])
            ppv = parse_pct_ci(row["PPV (95% CI)"])
            npv = parse_pct_ci(row["NPV (95% CI)"])

            slab = "all sizes" if band == "all_sizes" else "index nodule strata 2–4 cm"
            forest.append(
                (
                    f"{plat} — B3+B4 {slab} (n={n})",
                    sens[0],
                    sens[1],
                    sens[2],
                    spec[0],
                    spec[1],
                    spec[2],
                    ppv[0],
                    ppv[1],
                    ppv[2],
                    npv[0],
                    npv[1],
                    npv[2],
                    n,
                )
            )
    # deterministic order: Afirma all, Afirma 2–4, ThyroSeq all, ThyroSeq 2–4
    order = []
    for plat in ["Afirma", "ThyroSeq"]:
        for band in ["all_sizes", "2to4cm"]:
            for r in forest:
                if plat in r[0] and (("all sizes" in r[0]) == (band == "all_sizes")):
                    order.append(r)
                    break
    return order


forest_rows = load_forest_rows()
metrics = ["Sensitivity", "Specificity", "PPV", "NPV"]
colors = {"Afirma": "#c0392b", "ThyroSeq": "#2c3e50"}

fig, axes = plt.subplots(1, 4, figsize=(15, 5), dpi=200, sharey=True)
y_pos = np.arange(len(forest_rows))
labels = [r[0] for r in forest_rows]

for j, metric in enumerate(metrics):
    ax = axes[j]
    for i, r in enumerate(forest_rows):
        idx = 1 + j * 3
        pt, lo, hi = r[idx], r[idx + 1], r[idx + 2]
        platform = "Afirma" if "Afirma" in r[0] else "ThyroSeq"
        if pt is None:
            ax.plot([], [])
            continue
        ax.plot([lo, hi], [i, i], color=colors[platform], lw=2)
        ax.plot(pt, i, "o", color=colors[platform], markersize=8)
        ax.text(pt, i + 0.18, f"{pt:.1f}%", ha="center", fontsize=8, color=colors[platform])
    ax.set_xlim(-5, 105)
    ax.set_xlabel(f"{metric} (%)")
    ax.axvline(50, color="grey", linestyle=":", alpha=0.4)
    ax.set_yticks(y_pos)
    ax.grid(axis="x", alpha=0.2)
axes[0].set_yticklabels(labels, fontsize=9)
axes[0].invert_yaxis()
fig.suptitle(
    "Figure 2 (v4). Diagnostic performance — surgical Bethesda III/IV in US-defined 2–4 cm cohort\n"
    "(Wilson 95% CIs; strict NIFTP=benign; index nodule 2–4 cm = patient-grain imaging_nodule_size_cm strata)",
    fontsize=11,
)
plt.tight_layout()
for ext in ("png", "pdf"):
    fig.savefig(FIG / f"fig2_forest_diagnostic_performance_v4.{ext}", dpi=200, bbox_inches="tight")
plt.close(fig)

# --- ROM (ThyroSeq only) ---
rom_rows: list[tuple] = []
with ROMCSV.open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if row["Platform"] != "ThyroSeq":
            continue
        call = row["Reported call"]
        hist = row["Histology class"]
        if hist.lower() == "niftp":
            continue  # omit single NIFTP from six-panel clutter; still in CSV
        n = int(row["n"])
        nrom = row["n with numeric ROM%"]
        nrom_i = int(nrom) if nrom.isdigit() else 0
        med_field = row["Median ROM% [IQR]"]
        mean_v = row["Mean ROM%"]
        if mean_v.startswith("n/a") or med_field.startswith("n/a"):
            continue
        m_med = re.search(r"([\d.]+).*?\[([\d.]+).*?([\d.]+)\]", med_field)
        mean_f = float(mean_v) if mean_v else None
        if not m_med:
            continue
        med = float(m_med.group(1))
        p25 = float(m_med.group(2))
        p75 = float(m_med.group(3))
        rom_rows.append((call, hist, nrom_i, mean_f if mean_f else med, med, p25, p75))

ordered: list[tuple] = []
for c in ["negative", "intermediate", "positive"]:
    for h in ["benign", "malignant"]:
        mt = next((x for x in rom_rows if x[0] == c and x[1] == h), None)
        if mt:
            ordered.append(mt)

fig, ax = plt.subplots(figsize=(10, 6), dpi=200)
xs = []
for i, r in enumerate(ordered):
    call, histo, n, mean, med, p25, p75 = r
    color = "#27ae60" if histo == "benign" else "#c0392b"
    ax.add_patch(
        plt.Rectangle((i - 0.3, p25), 0.6, p75 - p25, facecolor=color, alpha=0.25, edgecolor=color)
    )
    ax.plot([i - 0.3, i + 0.3], [med, med], color=color, lw=2)
    ax.text(i, 105, f"n={n}", ha="center", fontsize=8)
    xs.append(f"{call}\n{histo}")
ax.set_xticks(range(len(ordered)))
ax.set_xticklabels(xs, fontsize=9)
ax.set_ylabel("ThyroSeq numeric ROM% (rom_percent_point)")
ax.set_ylim(0, 110)
ax.set_title(
    "Figure 3 (v4). ThyroSeq numeric ROM% distribution by reported call × histology\n"
    "(v4 Bethesda III/IV molecular-tested subset)",
    fontsize=11,
)
ax.grid(axis="y", alpha=0.2)
ax.legend(
    [
        mpatches.Patch(color="#27ae60", alpha=0.5),
        mpatches.Patch(color="#c0392b", alpha=0.5),
    ],
    ["Benign on final pathology", "Malignant on final pathology"],
    loc="upper left",
)
plt.tight_layout()
for ext in ("png", "pdf"):
    fig.savefig(FIG / f"fig3_rom_pct_distribution_v4.{ext}", dpi=200, bbox_inches="tight")
plt.close(fig)

# -------- Figure 4 (unchanged era rows vs v3) --------
era_rows = [
    (1999, 102, 56, 46, 0, 0, 0, 2),
    (2000, 148, 77, 71, 0, 0, 0, 13),
    (2001, 141, 69, 72, 0, 0, 0, 10),
    (2002, 162, 77, 85, 0, 0, 0, 4),
    (2003, 124, 47, 77, 0, 0, 0, 8),
    (2004, 156, 65, 91, 0, 0, 0, 3),
    (2005, 153, 73, 80, 0, 0, 0, 12),
    (2006, 188, 76, 112, 1, 0, 1, 25),
    (2007, 219, 103, 116, 0, 1, 1, 18),
    (2008, 259, 122, 137, 0, 0, 0, 21),
    (2009, 298, 135, 163, 0, 1, 1, 28),
    (2010, 274, 135, 139, 0, 0, 0, 29),
    (2011, 296, 149, 147, 0, 0, 0, 30),
    (2012, 314, 176, 138, 0, 1, 1, 26),
    (2013, 455, 280, 175, 2, 2, 4, 43),
    (2014, 467, 279, 188, 0, 1, 1, 32),
    (2015, 482, 297, 185, 22, 3, 25, 49),
    (2016, 434, 273, 161, 28, 2, 30, 60),
    (2017, 596, 338, 258, 23, 16, 39, 104),
    (2018, 568, 317, 251, 13, 7, 20, 79),
    (2019, 774, 437, 337, 30, 66, 96, 105),
    (2020, 619, 350, 269, 20, 62, 82, 68),
    (2021, 645, 377, 268, 46, 57, 103, 85),
    (2022, 493, 251, 242, 39, 54, 93, 79),
]

years = np.array([r[0] for r in era_rows])
total_rate = np.array([100 * r[2] / r[1] for r in era_rows])
named_rate = np.array([100 * r[6] / r[1] for r in era_rows])
afirma_rate = np.array([100 * r[4] / r[1] for r in era_rows])
thyroseq_rate = np.array([100 * r[5] / r[1] for r in era_rows])
b34_rate = np.array([100 * r[7] / r[1] for r in era_rows])

fig, axes = plt.subplots(2, 1, figsize=(11, 8), dpi=200, sharex=True)
ax1, ax2 = axes

ax1.plot(years, total_rate, "-o", color="#2c3e50", label="Total thyroidectomy rate (% of surgeries)")
ax1.plot(years, b34_rate, "-s", color="#7f8c8d", label="Bethesda III/IV rate (% of surgeries)")
ax1.axvline(2015, color="grey", linestyle="--", alpha=0.5)
ax1.text(2015.1, 95, "2015 ATA guidelines + GSC era", fontsize=8, color="grey")
ax1.set_ylim(0, 100)
ax1.set_ylabel("% of surgical patients")
ax1.legend(loc="lower right", fontsize=9)
ax1.grid(alpha=0.2)
ax1.set_title(
    "Figure 4 (v4 copy). Temporal trends, 1999–2022 (full surgical denominators)\n"
    "A. Surgical extent + Bethesda III/IV rate (same as EXT2-4 v3).",
)

ax2.plot(years, named_rate, "-o", color="#16a085", label="Any named platform (Afirma + ThyroSeq)")
ax2.plot(years, afirma_rate, "-s", color="#c0392b", label="Afirma")
ax2.plot(years, thyroseq_rate, "-^", color="#2980b9", label="ThyroSeq")
ax2.axvline(2015, color="grey", linestyle="--", alpha=0.5)
ax2.set_ylim(0, 25)
ax2.set_ylabel("% of surgical patients")
ax2.set_xlabel("Surgery year")
ax2.legend(loc="upper left", fontsize=9)
ax2.grid(alpha=0.2)
ax2.set_title("B. Molecular platform utilization.")
plt.tight_layout()
for ext in ("png", "pdf"):
    fig.savefig(FIG / f"fig4_era_trends_v4.{ext}", dpi=200, bbox_inches="tight")
plt.close(fig)

print("OK build_figures_v4:", len(forest_rows), "forest rows")
