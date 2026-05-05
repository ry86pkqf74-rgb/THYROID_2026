"""
M025 v2 figure builder.
All numbers verified against M025_tables_and_summary (1).xlsx.
Outputs: 300 dpi PNG + vector PDF for each figure.
"""
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

OUT = "/sessions/eloquent-serene-johnson/mnt/THYROID_2026/M025_submission_package/figures"
os.makedirs(OUT, exist_ok=True)

# Style: Thyroid journal expects clean, high-contrast figures.
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titleweight": "bold",
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
})

PALETTE = {
    "patient": "#1f4e79",
    "nodule":  "#c55a11",
    "acr":     "#a9d18e",
    "tp":      "#2e7d32",
    "fp":      "#ef6c00",
    "fn":      "#c62828",
    "tn":      "#1565c0",
    "neutral": "#7f7f7f",
    "youden":  "#d62728",
}


def save(fig, name):
    fig.savefig(os.path.join(OUT, f"{name}.png"))
    fig.savefig(os.path.join(OUT, f"{name}.pdf"))
    plt.close(fig)
    print(f"  saved {name}.png + {name}.pdf")


# -----------------------------------------------------------------------------
# Figure 1 — Cohort flow diagram (CONSORT-style)
# -----------------------------------------------------------------------------
def fig1_cohort_flow():
    fig, ax = plt.subplots(figsize=(11.5, 10.5))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 13)
    ax.axis("off")

    def box(x, y, w, h, text, fc="#eef2f7", ec="#1f4e79",
            fontsize=9, fontweight="normal", text_color="#0a2540"):
        b = FancyBboxPatch((x - w/2, y - h/2), w, h,
                           boxstyle="round,pad=0.04,rounding_size=0.08",
                           lw=1.4, fc=fc, ec=ec)
        ax.add_patch(b)
        ax.text(x, y, text, ha="center", va="center",
                fontsize=fontsize, fontweight=fontweight,
                color=text_color, wrap=True)

    def arrow(x1, y1, x2, y2, color="#333"):
        a = FancyArrowPatch((x1, y1), (x2, y2),
                            arrowstyle="->,head_width=4,head_length=6",
                            lw=1.3, color=color)
        ax.add_patch(a)

    # Title
    ax.text(7.0, 12.6, "Figure 1. Cohort flow diagram",
            ha="center", fontsize=14, fontweight="bold")

    # Layout: left column primary @ x=4.4; right column exclusions @ x=11.2
    LX, RX = 4.4, 11.2
    BOX_W_LEFT = 8.0
    BOX_W_RIGHT = 5.0

    # Top: warehouse
    box(LX, 11.7, BOX_W_LEFT, 0.9,
        "Institutional thyroid surgical warehouse\n"
        "canonical_patient_master, 1994–2025\n"
        "n = 10,871 unique research IDs",
        fc="#dbe8f5", fontweight="bold", fontsize=10)

    # Excluded — top
    box(RX, 11.0, BOX_W_RIGHT, 1.6,
        "Excluded (n = 7,496):\n"
        "• No preoperative ultrasound (n=4,128)\n"
        "• No operative pathology (n=1,953)\n"
        "• No computed ACR 2017 TR category (n=1,415)",
        fc="#fbeaea", ec="#c62828", fontsize=9)

    arrow(LX, 11.2, LX, 10.3)
    arrow(LX + BOX_W_LEFT/2, 11.5, RX - BOX_W_RIGHT/2, 11.2)

    # Patient analytic cohort
    box(LX, 9.7, BOX_W_LEFT, 1.2,
        "Patient analytic cohort  —  PRIMARY analysis\n"
        "n = 3,375 patients\n"
        "1,479 (43.8%) pathology-proven malignant",
        fc=PALETTE["patient"], ec="#0a2540",
        fontsize=11, fontweight="bold", text_color="white")

    arrow(LX, 9.1, LX, 8.2)

    # Strict nodule subset
    box(LX, 7.4, BOX_W_LEFT, 1.6,
        "Strict ACR-eligible nodule subset — SISTER analysis\n"
        "n = 3,687 nodules from 1,668 patients\n"
        "631 (17.1%) path-proven malignant\n"
        "5-feature complete; 99.3% structured inm_v1 source",
        fc=PALETTE["nodule"], ec="#7a3a08",
        fontsize=10, fontweight="bold", text_color="white")

    box(RX, 7.4, BOX_W_RIGHT, 2.0,
        "Excluded from strict subset:\n"
        "• Incomplete 5-feature scoring\n"
        "• Unknown laterality\n"
        "• Multi-nodule attribution flag\n"
        "• Size-outlier quarantine\n"
        "Relaxed-gate cohort retained as\n"
        "Sensitivity Arm S1A (n = 15,309)",
        fc="#fbeaea", ec="#c62828", fontsize=9)

    arrow(LX + BOX_W_LEFT/2, 7.4, RX - BOX_W_RIGHT/2, 7.4)
    arrow(LX, 6.6, LX, 5.7)

    # Sensitivity arms — full-width
    box(7.0, 5.0, 12.0, 1.4,
        "Pre-specified sensitivity arms\n"
        "S1A relaxed gate (n=15,309)  ·  S1B first-US-only  ·  S1C single-nodule (n=782)  ·  S1D unilateral path-only\n"
        "Era split (S2): pre-2017 nodule n=381 / post-2017 n=3,306\n"
        "Match-window sensitivity (S3): 365 / 180 / 90 / 30 days",
        fc="#f4ecf7", ec="#6a1b9a", fontsize=9)

    arrow(LX, 4.3, LX, 3.5)
    arrow(RX, 4.3, RX, 3.5)

    # Final analytic specs
    box(LX, 2.8, BOX_W_LEFT, 1.4,
        "Primary patient-level analysis\n"
        "Predictor: max ACR 2017 TR category (re-scored)\n"
        "Outcome: any thyroid malignancy on operative\n"
        "pathology specimen (WHO 2022)",
        fc="#eef2f7", fontsize=9)

    box(RX, 2.8, BOX_W_RIGHT, 1.4,
        "Sister nodule-level analysis\n"
        "Predictor: per-nodule ACR 2017 TR\n"
        "Outcome: same-side malignant tumor\n"
        "within 365 days of index US",
        fc="#fff3e8", ec="#7a3a08", fontsize=9)

    arrow(LX, 2.1, LX, 1.3)
    arrow(RX, 2.1, RX, 1.3)

    # Headline results
    box(LX, 0.7, BOX_W_LEFT, 0.9,
        "Patient AUC = 0.648  [95% CI 0.630–0.667]\n"
        "Youden-optimal threshold:  TR ≥ TR4   (J = 0.271)",
        fc=PALETTE["patient"], ec="#0a2540",
        fontsize=10, fontweight="bold", text_color="white")

    box(RX, 0.7, BOX_W_RIGHT, 0.9,
        "Nodule AUC = 0.640\n"
        "TR4 ROM 18.7%   ·   TR5 ROM 26.1%",
        fc=PALETTE["nodule"], ec="#7a3a08",
        fontsize=10, fontweight="bold", text_color="white")

    save(fig, "Figure_1_Cohort_Flow")


# -----------------------------------------------------------------------------
# Figure 2 — Patient-level ROC (operating-point reconstruction)
# -----------------------------------------------------------------------------
def fig2_roc():
    # Operating points from Table 2 (patient grain)
    # Order from (1,1) at TR>=TR1 down to (0,0) at TR>TR5
    points = [
        ("TR≥TR1", 1.000, 1.000),                      # all positive
        ("TR≥TR3", 0.8702, 1 - 0.2358),                # 0.8702, 0.7642
        ("TR≥TR4", 0.7126, 1 - 0.5585),                # 0.7126, 0.4415
        ("TR≥TR5", 0.5551, 1 - 0.6951),                # 0.5551, 0.3049
        ("TR>TR5", 0.000, 0.000),
    ]
    # Sort by FPR ascending
    pts = sorted(points, key=lambda p: (p[2], p[1]))
    fpr = np.array([p[2] for p in pts])
    tpr = np.array([p[1] for p in pts])

    fig, ax = plt.subplots(figsize=(6.0, 6.0))
    # Diagonal
    ax.plot([0, 1], [0, 1], color="#999", lw=1, ls="--", label="No discrimination")
    # Smooth piecewise-linear ROC through operating points
    ax.plot(fpr, tpr, color=PALETTE["patient"], lw=2.4,
            marker="o", markersize=6, label="ACR TI-RADS (patient grain)")
    # Annotate operating points (skip 0,0 and 1,1)
    annotate = {
        "TR≥TR3": (1 - 0.2358, 0.8702, "right"),
        "TR≥TR4": (1 - 0.5585, 0.7126, "left"),
        "TR≥TR5": (1 - 0.6951, 0.5551, "left"),
    }
    for lbl, (x, y, ha) in annotate.items():
        ax.scatter([x], [y], s=60, color=PALETTE["patient"], zorder=5)
        offset = (10, 10) if ha == "left" else (-10, -10)
        ax.annotate(lbl, (x, y), xytext=offset, textcoords="offset points",
                    fontsize=9, ha=("left" if ha == "left" else "right"))

    # Highlight Youden-optimal
    yx, yy = 1 - 0.5585, 0.7126
    ax.scatter([yx], [yy], s=160, facecolor="none",
               edgecolor=PALETTE["youden"], lw=2.2, zorder=6,
               label="Youden-optimal (TR≥TR4, J=0.271)")

    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.set_xlabel("1 − Specificity (false-positive rate)")
    ax.set_ylabel("Sensitivity (true-positive rate)")
    ax.set_title("Figure 2. Patient-level ROC for ACR TI-RADS\n"
                 "AUC = 0.648 (95% CI 0.630–0.667); n=3,375; 1,479 malignant")
    ax.legend(loc="lower right", frameon=True)
    ax.grid(True, ls=":", alpha=0.4)

    save(fig, "Figure_2_ROC_Patient")


# -----------------------------------------------------------------------------
# Figure 3 — Patient-level ROM by TR with ACR-expected bands
# -----------------------------------------------------------------------------
def fig3_patient_rom():
    cats = ["TR1", "TR2", "TR3", "TR4", "TR5"]
    rom = [28.24, 32.11, 27.57, 47.36, 58.68]
    lo  = [23.71, 27.07, 24.67, 42.98, 56.08]
    hi  = [33.24, 37.60, 30.68, 51.77, 61.24]

    # ACR 2017 expected bands
    acr_low  = [0,  0,  0,  5,  20]
    acr_high = [2,  2,  5, 20, 60]   # cap TR5 at 60 for visualization

    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    x = np.arange(len(cats))

    # ACR bands (shaded behind bars)
    for i, c in enumerate(cats):
        ax.fill_between([i - 0.45, i + 0.45], acr_low[i], acr_high[i],
                        color=PALETTE["acr"], alpha=0.55, zorder=1)

    err_lo = [r - l for r, l in zip(rom, lo)]
    err_hi = [h - r for h, r in zip(hi, rom)]
    ax.bar(x, rom, color=PALETTE["patient"], width=0.7, zorder=3,
           edgecolor="white", linewidth=1)
    ax.errorbar(x, rom, yerr=[err_lo, err_hi], fmt="none",
                ecolor="#0a2540", capsize=4, lw=1.3, zorder=4)

    for i, r in enumerate(rom):
        ax.text(i, r + 2, f"{r:.1f}%", ha="center",
                fontsize=10, fontweight="bold", color="#0a2540")

    ax.set_xticks(x)
    ax.set_xticklabels(cats)
    ax.set_ylim(0, 70)
    ax.set_ylabel("Risk of malignancy (%)")
    ax.set_xlabel("ACR TI-RADS category (re-scored, max per patient)")
    ax.set_title("Figure 3. Patient-level ROM by TI-RADS category\n"
                 "Wilson 95% CI; ACR 2017 expected bands shaded green")

    band_handle = mpatches.Patch(color=PALETTE["acr"], alpha=0.55,
                                 label="ACR 2017 expected band")
    bar_handle  = mpatches.Patch(color=PALETTE["patient"], label="Observed patient ROM")
    ax.legend(handles=[bar_handle, band_handle], loc="upper left", frameon=True)

    ax.grid(True, axis="y", ls=":", alpha=0.4)
    save(fig, "Figure_3_Patient_ROM")


# -----------------------------------------------------------------------------
# Figure 3b — Patient vs Nodule ROM paired bars with ACR bands
# -----------------------------------------------------------------------------
def fig3b_paired():
    cats = ["TR2", "TR3", "TR4", "TR5"]
    pat  = [32.11, 27.57, 47.36, 58.68]
    nod  = [12.90,  9.13, 18.72, 26.11]
    pat_lo = [27.07, 24.67, 42.98, 56.08]
    pat_hi = [37.60, 30.68, 51.77, 61.24]
    nod_lo = [ 5.13,  7.80, 16.26, 23.74]
    nod_hi = [28.85, 10.67, 21.47, 28.62]
    inflation = [pat[i] - nod[i] for i in range(len(cats))]

    acr_low  = [ 0,  0,  5, 20]
    acr_high = [ 2,  5, 20, 60]

    fig, ax = plt.subplots(figsize=(9.0, 5.8))
    x = np.arange(len(cats))
    w = 0.36

    for i in range(len(cats)):
        ax.fill_between([i - 0.5, i + 0.5], acr_low[i], acr_high[i],
                        color=PALETTE["acr"], alpha=0.55, zorder=1)

    pat_err = [[p - l for p, l in zip(pat, pat_lo)], [h - p for h, p in zip(pat_hi, pat)]]
    nod_err = [[n - l for n, l in zip(nod, nod_lo)], [h - n for h, n in zip(nod_hi, nod)]]

    ax.bar(x - w/2, pat, width=w, color=PALETTE["patient"], label="Patient grain",
           edgecolor="white", lw=1, zorder=3)
    ax.errorbar(x - w/2, pat, yerr=pat_err, fmt="none", ecolor="#0a2540",
                capsize=3, lw=1.1, zorder=4)
    ax.bar(x + w/2, nod, width=w, color=PALETTE["nodule"], label="Nodule grain (strict)",
           edgecolor="white", lw=1, zorder=3)
    ax.errorbar(x + w/2, nod, yerr=nod_err, fmt="none", ecolor="#7a3a08",
                capsize=3, lw=1.1, zorder=4)

    # Inflation annotations (TR3-TR5)
    for i in range(len(cats)):
        ax.annotate("", xy=(i + w/2, nod[i] + 0.5),
                    xytext=(i - w/2, pat[i] - 0.5),
                    arrowprops=dict(arrowstyle="-", color="#333", lw=0.8, ls=":"))
        midy = (pat[i] + nod[i]) / 2
        ax.text(i, midy, f"+{inflation[i]:.1f} pp",
                ha="center", va="center",
                fontsize=9, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#333", lw=0.7))

    ax.set_xticks(x)
    ax.set_xticklabels(cats)
    ax.set_ylim(0, 70)
    ax.set_xlabel("ACR TI-RADS category")
    ax.set_ylabel("Risk of malignancy (%)")
    ax.set_title("Figure 3b. Patient- vs nodule-grain ROM with ACR-expected bands\n"
                 "Per-nodule TR4 (18.7%) and TR5 (26.1%) recover within-band calibration")

    band_handle = mpatches.Patch(color=PALETTE["acr"], alpha=0.55,
                                 label="ACR 2017 expected band")
    handles, labels = ax.get_legend_handles_labels()
    handles.append(band_handle); labels.append("ACR 2017 expected band")
    ax.legend(handles, labels, loc="upper left", frameon=True)

    ax.grid(True, axis="y", ls=":", alpha=0.4)
    save(fig, "Figure_3b_Patient_vs_Nodule")


# -----------------------------------------------------------------------------
# Figure 4 — Confusion matrix at TR>=TR4 + FNA compliance stack
# -----------------------------------------------------------------------------
def fig4_confusion_and_fna():
    # Confusion matrix (patient grain, threshold TR>=TR4)
    tp, fp, fn, tn = 1054, 837, 425, 1059

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 5.4),
                             gridspec_kw={"width_ratios": [1.05, 1.4]})

    # Confusion matrix as 2x2 colored cells
    ax = axes[0]
    cells = np.array([[tp, fn], [fp, tn]])
    labels = np.array([
        [f"TP\n{tp:,}", f"FN\n{fn:,}"],
        [f"FP\n{fp:,}", f"TN\n{tn:,}"]
    ])
    fcs = np.array([
        [PALETTE["tp"], PALETTE["fn"]],
        [PALETTE["fp"], PALETTE["tn"]]
    ])

    for i in range(2):
        for j in range(2):
            ax.add_patch(plt.Rectangle((j, 1 - i), 1, 1, color=fcs[i, j], alpha=0.85))
            ax.text(j + 0.5, 1.5 - i, labels[i, j], ha="center", va="center",
                    color="white", fontsize=14, fontweight="bold")

    ax.set_xlim(0, 2); ax.set_ylim(0, 2)
    ax.set_xticks([0.5, 1.5]); ax.set_yticks([0.5, 1.5])
    ax.set_xticklabels(["Malignant", "Benign"], fontsize=10)
    ax.set_yticklabels(["TR<TR4", "TR≥TR4"], fontsize=10)
    ax.set_xlabel("Pathology truth", fontsize=10, fontweight="bold")
    ax.set_ylabel("Predicted (patient max TR)", fontsize=10, fontweight="bold")
    ax.set_aspect("equal")
    ax.set_title("Confusion matrix at TR≥TR4 (Youden-optimal)\n"
                 "Sens 71.3%; Spec 55.9%; PPV 55.7%; NPV 71.4%")
    for s in ax.spines.values():
        s.set_visible(False)

    # FNA compliance stacked
    ax2 = axes[1]
    # Two stacks: FNAs performed (n=3,375 had US, of which 2,380 had FNA)
    # Left: FNAs performed → "ACR-warranted" vs "Unnecessary by ACR"
    # Right: All malignancies (n=1,479) → "Above ACR FNA threshold" vs "Below threshold"
    fna_total = 3375  # patient cohort denominator
    unnecessary = 1553
    warranted = fna_total - unnecessary
    cancers_total = 1479
    below_thr = 472
    above_thr = cancers_total - below_thr

    bars = ["FNA-eligibility audit\n(all 3,375 patients)",
            "Cancers vs ACR FNA threshold\n(all 1,479 malignant)"]
    seg1 = [warranted, above_thr]
    seg2 = [unnecessary, below_thr]

    x = np.arange(len(bars))
    ax2.bar(x, seg1, color=PALETTE["tp"], edgecolor="white",
            label="ACR-warranted FNA / cancer above threshold")
    ax2.bar(x, seg2, bottom=seg1, color=PALETTE["fn"], edgecolor="white",
            label="Unnecessary FNA / cancer below threshold")

    for i in range(2):
        ax2.text(i, seg1[i] / 2, f"{seg1[i]:,}", ha="center", va="center",
                 color="white", fontsize=11, fontweight="bold")
        ax2.text(i, seg1[i] + seg2[i] / 2, f"{seg2[i]:,}", ha="center", va="center",
                 color="white", fontsize=11, fontweight="bold")

    ax2.set_xticks(x)
    ax2.set_xticklabels(bars, fontsize=9)
    ax2.set_ylabel("Patients (n)")
    ax2.set_title("ACR 2017 FNA-eligibility audit\n"
                  "1,553 unnecessary FNAs flagged; 472 cancers below threshold")
    ax2.legend(loc="upper right", fontsize=8.5)
    ax2.grid(True, axis="y", ls=":", alpha=0.4)

    fig.suptitle("Figure 4. Diagnostic confusion at TR≥TR4 and ACR FNA-eligibility audit",
                 fontsize=12, fontweight="bold", y=1.02)
    save(fig, "Figure_4_Confusion_and_FNA")


# -----------------------------------------------------------------------------
# Figure 5 — Subgroup forest plot (per-TR ROM at TR>=TR4 and AUC by stratum)
# AUC is computed closed-form from the joint TR x malignancy distribution.
# -----------------------------------------------------------------------------
def closed_form_auc_from_ordinal(counts):
    """
    counts: list of (n_total, n_pos) for ordered categories TR1..TR5.
    Returns AUC for Mann-Whitney rank version of an ordinal predictor with ties.
    """
    n_pos_total = sum(p for _, p in counts)
    n_neg_total = sum(n - p for n, p in counts)
    if n_pos_total == 0 or n_neg_total == 0:
        return float("nan")
    # Sum over pairs: positive in cat i, negative in cat j
    auc_num = 0.0
    for i, (ni, pi) in enumerate(counts):
        for j, (nj, pj) in enumerate(counts):
            ni_neg = nj - pj
            if i > j:
                auc_num += pi * ni_neg
            elif i == j:
                auc_num += 0.5 * pi * ni_neg
    return auc_num / (n_pos_total * n_neg_total)


def fig5_forest():
    # Build subgroup AUC + ROM (TR>=TR4) from Subgroup sheets
    # Sex
    sex_data = {
        "Female": [(282, 75), (253, 73), (684, 179), (397, 178), (1075, 600)],
        "Male":   [( 58, 21), ( 46, 23), (161,  54), ( 95,  55), ( 324, 221)],
    }
    age_data = {
        "<40":   [( 76, 29), ( 57, 24), (162,  66), ( 84, 48), (278, 218)],
        "40–54": [(113, 23), ( 92, 27), (252,  67), (157, 75), (423, 242)],
        "55–69": [(107, 29), (111, 31), (296,  59), (172, 80), (458, 235)],
        "≥70":   [( 44, 15), ( 39, 14), (135,  41), ( 79, 30), (240, 126)],
    }
    # Era split (patient grain)
    era_data = {
        "Pre-2017":  [(69, 17), (71, 24), (105, 26), (52, 21), (125, 81)],
        "Post-2017": [(271, 79), (228, 72), (740, 207), (440, 212), (1274, 740)],
    }
    # Histology category (patient grain) using primary path-cat aggregates
    # We know: differentiated: TR1 87/87, TR2 93/93, TR3 222/222, TR4 222/222, TR5 755/755
    # but those are conditional on path being malignant (denominators=cases)
    # Replace with overall hist groups: Differentiated CA / Medullary / etc.
    # Skip histology stratum AUC — no benign cells present per group

    fig, ax = plt.subplots(figsize=(8.6, 7.2))

    rows = []
    # Overall
    overall_counts = [(340, 96), (299, 96), (845, 233), (492, 233), (1399, 821)]
    rows.append(("Overall (n=3,375)", closed_form_auc_from_ordinal(overall_counts), "#000"))
    rows.append(("", None, None))
    rows.append(("Sex", None, None))
    for k, v in sex_data.items():
        n_tot = sum(n for n, _ in v); n_mal = sum(p for _, p in v)
        auc = closed_form_auc_from_ordinal(v)
        rows.append((f"  {k} (n={n_tot:,}; {n_mal} mal)", auc, "#1f4e79"))
    rows.append(("", None, None))
    rows.append(("Age band", None, None))
    for k, v in age_data.items():
        n_tot = sum(n for n, _ in v); n_mal = sum(p for _, p in v)
        auc = closed_form_auc_from_ordinal(v)
        rows.append((f"  {k} (n={n_tot:,}; {n_mal} mal)", auc, "#0b6e4f"))
    rows.append(("", None, None))
    rows.append(("Surgery era", None, None))
    for k, v in era_data.items():
        n_tot = sum(n for n, _ in v); n_mal = sum(p for _, p in v)
        auc = closed_form_auc_from_ordinal(v)
        rows.append((f"  {k} (n={n_tot:,}; {n_mal} mal)", auc, "#6a1b9a"))

    rows = list(reversed(rows))  # so 'Overall' plots at top
    yvals = list(range(len(rows)))

    ax.axvline(0.5, color="#888", ls="--", lw=1)
    ax.axvline(0.648, color=PALETTE["patient"], ls=":", lw=1.4,
               label="Cohort AUC 0.648")

    for y, (lbl, auc, col) in zip(yvals, rows):
        if auc is None:
            ax.text(0.41, y, lbl, va="center", fontsize=10, fontweight="bold")
            continue
        ax.scatter([auc], [y], s=72, color=col, zorder=4, edgecolor="white", lw=0.7)
        ax.text(auc + 0.012, y, f"{auc:.3f}", va="center", fontsize=8.5, color=col)
        ax.text(0.41, y, lbl, va="center", fontsize=9)

    ax.set_yticks([])
    ax.set_xlim(0.40, 0.80)
    ax.set_xlabel("AUC (closed-form rank Mann–Whitney equivalent)")
    ax.set_title("Figure 5. Subgroup forest plot — AUC by demographic stratum\n"
                 "Discrimination is preserved (modest) across sex, age, and era")
    ax.legend(loc="lower right", frameon=True)
    ax.grid(True, axis="x", ls=":", alpha=0.4)

    save(fig, "Figure_5_Subgroup_Forest")


# -----------------------------------------------------------------------------
# Supplementary Figure S1 — Bethesda × TR heatmap (strict-eligible nodules)
# -----------------------------------------------------------------------------
def figS1_bethesda_heatmap():
    bethesda = ["I (nondiagnostic)", "II (benign)", "III (AUS)",
                "IV (FN/SFN)", "V (suspicious)", "VI (malignant)", "Missing"]
    cats = ["TR2", "TR3", "TR4", "TR5"]
    M = np.array([
        [ 1,    9,    6,   10],   # I
        [ 0,   84,   29,   45],   # II
        [ 2,   31,   25,   26],   # III
        [ 0,   23,   14,   19],   # IV
        [ 0,    6,    6,   23],   # V
        [ 3,   23,   33,   77],   # VI
        [25, 1379,  747, 1041],   # missing
    ])

    fig, ax = plt.subplots(figsize=(7.8, 6.0))
    im = ax.imshow(M, aspect="auto", cmap="Oranges")
    ax.set_xticks(range(len(cats))); ax.set_xticklabels(cats)
    ax.set_yticks(range(len(bethesda))); ax.set_yticklabels(bethesda)
    for i in range(len(bethesda)):
        for j in range(len(cats)):
            v = M[i, j]
            color = "white" if v > 600 else "#222"
            ax.text(j, i, f"{int(v)}", ha="center", va="center",
                    fontsize=9, color=color)
    ax.set_xlabel("ACR TI-RADS category (per nodule, re-scored)")
    ax.set_ylabel("Bethesda category (bridged ≤30 d to nodule)")
    ax.set_title("Supplementary Figure S1. Bethesda × TI-RADS at strict-eligible nodule level\n"
                 "n=3,687 nodules; 495 (13.4%) with bridged Bethesda")

    cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
    cbar.set_label("Nodule count")

    save(fig, "Figure_S1_Bethesda_x_TR")


if __name__ == "__main__":
    print("Building figures...")
    fig1_cohort_flow()
    fig2_roc()
    fig3_patient_rom()
    fig3b_paired()
    fig4_confusion_and_fna()
    fig5_forest()
    figS1_bethesda_heatmap()
    print("Done.")
