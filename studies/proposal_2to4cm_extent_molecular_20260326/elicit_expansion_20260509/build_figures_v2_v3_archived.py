#!/usr/bin/env python3
"""build_figures_v2.py — generate Figures 2, 3, 4 for the v2 manuscript package.

Figure 1 (cohort flow) was already produced by build_elicit_expansion.py.
This script adds:
  Figure 2 — forest plot of diagnostic performance (Sens/Spec/PPV/NPV with 95% CIs)
             for Afirma vs ThyroSeq in B3+B4 (all sizes and 2-4 cm subgroup), Strict.
  Figure 3 — ROM% distribution by reported call x histology for ThyroSeq.
  Figure 4 — temporal trend in molecular-platform utilization and total-thyroidectomy
             rate, 1999-2022.

All numbers must reconcile to:
  tables/table3_v2_diagnostic_performance_actual_reported_call.csv
  tables/table3_v2_rom_pct_descriptive_stats.csv
"""
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# FIGURE 2 — forest plot of diagnostic performance metrics
# ---------------------------------------------------------------------------
# Numbers pulled directly from table3_v2 (Strict, NIFTP=benign).
forest_rows = [
    # (label, sens, sens_lo, sens_hi, spec, spec_lo, spec_hi, ppv, ppv_lo, ppv_hi, npv, npv_lo, npv_hi, n)
    # v3 numbers post-mig_325 (2026-05-14 Cursor cleanup of 16 reported_text guard rows).
    # 13 ThyroSeq parser hallucinations superseded; 6 Afirma "other" rows corrected to "negative";
    # 2 rid 5724 + 2 rid 9991 marked non_diagnostic; 1 rid 11156 reclassified to Other.
    # Pre-mig_325 n: Afirma 91, ThyroSeq 226 (all sizes), 31 (2-4cm). Post-mig_325: 90, 222, 30.
    # All Sens/Spec/PPV/NPV shifts within Wilson 95% CIs already reported.
    ("Afirma — B3+B4 all sizes (n=90)", 90.4, 79.4, 95.8, 21.1, 11.1, 36.3,
     61.0, 49.9, 71.2, 61.5, 35.5, 82.3, 90),
    ("Afirma — B3+B4 2–4 cm (n=5)", 75.0, 30.1, 95.4, 0.0, 0.0, 79.3,
     75.0, 30.1, 95.4, 0.0, 0.0, 79.3, 5),
    ("ThyroSeq — B3+B4 all sizes (n=222)", 69.7, 60.5, 77.6, 63.7, 54.5, 72.0,
     65.0, 55.9, 73.0, 68.6, 59.2, 76.7, 222),
    ("ThyroSeq — B3+B4 2–4 cm (n=30)", 86.7, 62.1, 96.3, 73.3, 48.1, 89.1,
     76.5, 52.7, 90.4, 84.6, 57.8, 95.7, 30),
]

metrics = ["Sensitivity", "Specificity", "PPV", "NPV"]
colors = {"Afirma": "#c0392b", "ThyroSeq": "#2c3e50"}

fig, axes = plt.subplots(1, 4, figsize=(15, 5), dpi=200, sharey=True)
y_pos = np.arange(len(forest_rows))
labels = [r[0] for r in forest_rows]

for j, metric in enumerate(metrics):
    ax = axes[j]
    for i, r in enumerate(forest_rows):
        # Pull point + ci based on metric
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
fig.suptitle("Figure 2. Diagnostic performance of Afirma and ThyroSeq in surgical Bethesda III/IV cohort\n"
             "(actual platform-reported call; Wilson 95% CIs; NIFTP coded as benign)",
             fontsize=11)
plt.tight_layout()
fig.savefig(FIG / "fig2_forest_diagnostic_performance.png", dpi=200, bbox_inches="tight")
fig.savefig(FIG / "fig2_forest_diagnostic_performance.pdf", bbox_inches="tight")
plt.close(fig)

# ---------------------------------------------------------------------------
# FIGURE 3 — ROM% distribution by reported call x histology (ThyroSeq only)
# ---------------------------------------------------------------------------
# Pulled from table3_v2_rom_pct_descriptive_stats.csv. We use median + IQR + n.
# Only ThyroSeq rows (Afirma reports binary call only).
rom_rows = [
    # (call, histology, n_with_rom, mean, median, p25, p75)
    # v3 numbers — post-mig_323 (ThyroSeq n grew substantially as dual-platform patients
    # now appear in ThyroSeq arm rather than being blocked by their Afirma record)
    ("negative", "benign",        58, 4.3,  3, 3, 3),
    ("negative", "malignant",     21, 7.0,  3, 3, 3),
    ("intermediate", "benign",    11, 47.2, 50, 40, 50),
    ("intermediate", "malignant", 13, 54.6, 50, 50, 50),
    ("positive", "benign",        35, 66.8, 70, 70, 70),
    ("positive", "malignant",     72, 74.7, 70, 70, 70),
]

fig, ax = plt.subplots(figsize=(10, 6), dpi=200)
xs = []
for i, r in enumerate(rom_rows):
    call, histo, n, mean, med, p25, p75 = r
    color = "#27ae60" if histo == "benign" else "#c0392b"
    # Box: IQR
    ax.add_patch(plt.Rectangle((i - 0.3, p25), 0.6, p75 - p25,
                               facecolor=color, alpha=0.25, edgecolor=color))
    # Median line
    ax.plot([i - 0.3, i + 0.3], [med, med], color=color, lw=2)
    # n label
    ax.text(i, 105, f"n={n}", ha="center", fontsize=8)
    xs.append(f"{call}\n{histo}")
ax.set_xticks(range(len(rom_rows)))
ax.set_xticklabels(xs, fontsize=9)
ax.set_ylabel("ThyroSeq numeric ROM% (rom_percent_point)")
ax.set_ylim(0, 110)
ax.set_title("Figure 3. ThyroSeq numeric ROM% distribution by reported call × histology\n"
             "(Afirma omitted — reports a binary call only, no numeric ROM%)",
             fontsize=11)
ax.grid(axis="y", alpha=0.2)
# Legend
import matplotlib.patches as mpatches
ax.legend([mpatches.Patch(color="#27ae60", alpha=0.5),
           mpatches.Patch(color="#c0392b", alpha=0.5)],
          ["Benign on final pathology", "Malignant on final pathology"],
          loc="upper left")
plt.tight_layout()
fig.savefig(FIG / "fig3_rom_pct_distribution.png", dpi=200, bbox_inches="tight")
fig.savefig(FIG / "fig3_rom_pct_distribution.pdf", bbox_inches="tight")
plt.close(fig)

# ---------------------------------------------------------------------------
# FIGURE 4 — temporal trend: molecular adoption + total-thyroidectomy rate
# ---------------------------------------------------------------------------
# Year-level counts pulled from manuscript_cohort_v1 query 2026-05-09.
era_rows = [
    # (year, n_surgeries, n_total, n_lobe, n_afirma, n_thyroseq, n_named, n_b34)
    (1999, 102, 56,  46,  0, 0, 0, 2),
    (2000, 148, 77,  71,  0, 0, 0, 13),
    (2001, 141, 69,  72,  0, 0, 0, 10),
    (2002, 162, 77,  85,  0, 0, 0, 4),
    (2003, 124, 47,  77,  0, 0, 0, 8),
    (2004, 156, 65,  91,  0, 0, 0, 3),
    (2005, 153, 73,  80,  0, 0, 0, 12),
    (2006, 188, 76, 112,  1, 0, 1, 25),
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
ax1.set_title("Figure 4. Temporal trends, 1999–2022 (n=8,266 surgeries with year ≥1999 and resolved date)\n"
              "A. Surgical extent and indeterminate cytology rate.")

ax2.plot(years, named_rate, "-o", color="#16a085", label="Any named platform (Afirma + ThyroSeq)")
ax2.plot(years, afirma_rate, "-s", color="#c0392b", label="Afirma")
ax2.plot(years, thyroseq_rate, "-^", color="#2980b9", label="ThyroSeq")
ax2.axvline(2015, color="grey", linestyle="--", alpha=0.5)
ax2.set_ylim(0, 25)
ax2.set_ylabel("% of surgical patients")
ax2.set_xlabel("Surgery year")
ax2.legend(loc="upper left", fontsize=9)
ax2.grid(alpha=0.2)
ax2.set_title("B. Molecular platform utilization rate among surgical patients.")
plt.tight_layout()
fig.savefig(FIG / "fig4_era_trends.png", dpi=200, bbox_inches="tight")
fig.savefig(FIG / "fig4_era_trends.pdf", bbox_inches="tight")
plt.close(fig)

print("Figures 2, 3, 4 written to figures/")
