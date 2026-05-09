#!/usr/bin/env python3
"""
M088 — Extension figures (Figures S1, S2, S3, S4):

  S1: Boxplots of age and dominant tumor size by 2022 WHO family/label
  S2: Heatmap of capsular qualifiers vs reclassification flags by historical group
  S3: Era-stratified Sankey (3-panel: <2017, 2017-2022, post-2022)
  S4: Cumulative annual incidence of borderline entities (FT-UMP, atypical FA, MI-FTC, NIFTP)
"""
from __future__ import annotations
import csv
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.path import Path as MplPath
import matplotlib.patches as mpatches

ROOT     = Path(__file__).resolve().parent.parent
PER      = ROOT / "deliverables" / "m088_per_research_id.csv"
INCID    = ROOT / "analysis" / "output" / "incidence_annual.csv"
FIGS     = ROOT / "figures"
FIGS.mkdir(exist_ok=True)

df = pd.read_csv(PER)
for c in ["age_yr","dom_tumor_size_cm","surgery_year"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.sort_values(["research_id","historical_group"]).drop_duplicates("research_id", keep="first")

# ----- Figure S1: age + size boxplots ------------------------------------- #
fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

# Panel A: age by 2022 WHO family
groups = ["Oncocytic", "Conventional follicular"]
data = [df[df.who2022_family==g].age_yr.dropna() for g in groups]
bp = axes[0].boxplot(data, labels=groups, patch_artist=True, widths=0.6, medianprops={"color":"black"})
for patch, color in zip(bp['boxes'], ["#e58fb8", "#7fb1d3"]):
    patch.set_facecolor(color); patch.set_alpha(0.85)
axes[0].set_ylabel("Age at surgery (years)")
axes[0].set_title("Age by 2022 WHO family\n(Welch t-test p = 4×10⁻⁶)")
axes[0].grid(axis="y", lw=0.4, alpha=0.4)
for i, d in enumerate(data, 1):
    axes[0].text(i, d.median(), f"  n={len(d)}\n  med={d.median():.1f}",
                 va="center", ha="left", fontsize=8.5)

# Panel B: dominant size by historical group (where available)
order = ["NIFTP","follicular_adenoma","FT-UMP / atypical-FA","FTC_minimally_invasive","FTC_oncocytic_warthin","FTC_NOS","FTC_widely_invasive","DHGTC"]
display = {"NIFTP":"NIFTP","follicular_adenoma":"FA","FT-UMP / atypical-FA":"FT-UMP/atypical-FA","FTC_minimally_invasive":"MI-FTC","FTC_oncocytic_warthin":"Onco-FTC","FTC_NOS":"FTC, NOS","FTC_widely_invasive":"WI-FTC","DHGTC":"DHGTC"}
size_data = []
labels = []
ns = []
for g in order:
    if g == "FT-UMP / atypical-FA":
        sub = df[df.historical_group.isin(["FTUMP","atypical_follicular_adenoma"])]
    else:
        sub = df[df.historical_group==g]
    sz = sub.dom_tumor_size_cm.dropna()
    if len(sz) >= 5:
        size_data.append(sz)
        labels.append(display[g])
        ns.append(len(sz))
bp = axes[1].boxplot(size_data, labels=labels, patch_artist=True, widths=0.6, medianprops={"color":"black"})
for patch in bp['boxes']:
    patch.set_facecolor("#bcdfb8"); patch.set_alpha(0.85)
axes[1].set_ylabel("Dominant tumor size (cm)")
axes[1].set_title("Dominant tumor size by historical group\n(boxes show median + IQR; whiskers 1.5×IQR)")
axes[1].grid(axis="y", lw=0.4, alpha=0.4)
axes[1].tick_params(axis='x', rotation=30)
for tick in axes[1].get_xticklabels():
    tick.set_horizontalalignment('right')
for i, n in enumerate(ns, 1):
    axes[1].text(i, axes[1].get_ylim()[0]+0.3, f"n={n}", ha="center", fontsize=8)

fig.suptitle("M088 Figure S1 — Age (A) and dominant tumor size (B) by group", fontsize=13)
fig.tight_layout()
out = FIGS/"figure_s1_age_size_boxplots.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"Wrote {out}")

# ----- Figure S2: invasion qualifier heatmap ------------------------------ #
inv = pd.read_csv(ROOT/"analysis"/"output"/"extension"/"table_s4_invasion_validation.csv")
inv = inv[inv.n >= 10]  # entities with adequate n
groups = list(inv.historical_group)
metrics = ["pct_strict_eq", "pct_broad_eq", "pct_vascular_doc", "pct_vascular_quant_ge1"]
metric_labels = ["Strict equivocal capsular", "Broad equivocal capsular",
                 "Any vascular documented", "Vascular quantify≥1"]
M = inv[metrics].values  # rows=groups, cols=metrics
fig, ax = plt.subplots(figsize=(10, 0.55*len(groups)+2))
im = ax.imshow(M, cmap="YlOrRd", aspect="auto", vmin=0, vmax=max(M.max(),1))
ax.set_xticks(range(len(metrics)))
ax.set_xticklabels(metric_labels, rotation=20, ha="right")
ax.set_yticks(range(len(groups)))
ax.set_yticklabels(groups)
for i in range(M.shape[0]):
    for j in range(M.shape[1]):
        v = M[i,j]
        ax.text(j, i, f"{v:.1f}", ha="center", va="center",
                color="white" if v > M.max()/2 else "black", fontsize=8.5)
ax.set_title("M088 Figure S2 — Capsular and vascular invasion field rates by historical group (%)")
fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02, label="% of patients")
fig.tight_layout()
out = FIGS/"figure_s2_invasion_validation.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"Wrote {out}")

# ----- Figure S3: era-stratified Sankey (3 panels) ------------------------ #
def era_panel(ax, era_label, era_filter):
    sub = df[era_filter].copy()
    flows = []
    pairs = [
        ("Follicular adenoma", "Follicular adenoma", "follicular_adenoma", lambda r: r.who2022_label_tierA=="Follicular adenoma", "#1f77b4"),
        ("Hurthle cell adenoma", "Oncocytic adenoma", "hurthle_cell_adenoma", None, "#ff7f0e"),
        ("FTC oncocytic", "Oncocytic carcinoma", "FTC_oncocytic_warthin", None, "#9467bd"),
        ("FTC, MI", "FTC, MI", "FTC_minimally_invasive", None, "#d62728"),
        ("FTC, NOS", "FTC, NOS", "FTC_NOS", None, "#2ca02c"),
        ("FTC, WI", "FTC, WI", "FTC_widely_invasive", None, "#a55a00"),
        ("FT-UMP", "FT-UMP", "FTUMP", None, "#e377c2"),
        ("Atypical FA", "FT-UMP (from atypical FA)", "atypical_follicular_adenoma", None, "#bcbd22"),
        ("NIFTP", "NIFTP", "NIFTP", None, "#17becf"),
        ("HCC", "Oncocytic carcinoma", "HCC", None, "#8c564b"),
        ("DHGTC", "DHGTC", "DHGTC", None, "#7f7f7f"),
    ]
    for src_lab, tgt_lab, grp, _, color in pairs:
        n = (sub.historical_group == grp).sum()
        if n > 0:
            flows.append((src_lab, tgt_lab, int(n), color))
    if not flows:
        ax.text(0.5,0.5,"no cases", transform=ax.transAxes, ha="center")
        ax.set_title(era_label); ax.axis("off")
        return

    seen_l, seen_r = [], []
    for s,t,_,_ in flows:
        if s not in seen_l: seen_l.append(s)
        if t not in seen_r: seen_r.append(t)

    def positions(nodes, side):
        totals = defaultdict(int)
        for s,t,n,_ in flows:
            totals[s if side=="left" else t] += n
        ord_ = sorted(nodes, key=lambda nm: -totals[nm])
        pos = {}; running=0; gap=2
        for nm in ord_:
            pos[nm] = (running, running+totals[nm])
            running += totals[nm]+gap
        return pos, running

    Lpos, Lspan = positions(seen_l, "left")
    Rpos, Rspan = positions(seen_r, "right")

    ax.set_xlim(0, 100); ax.set_ylim(0, max(Lspan, Rspan)+2)
    ax.invert_yaxis(); ax.axis("off")
    for nm,(lo,hi) in Lpos.items():
        ax.add_patch(Rectangle((4, lo), 2, hi-lo, fc="#444", ec="black", lw=0.4))
        ax.text(3.5, (lo+hi)/2, f"{nm} ({hi-lo:.0f})", ha="right", va="center", fontsize=7.5)
    for nm,(lo,hi) in Rpos.items():
        ax.add_patch(Rectangle((94, lo), 2, hi-lo, fc="#444", ec="black", lw=0.4))
        ax.text(96.5, (lo+hi)/2, f"{nm} ({hi-lo:.0f})", ha="left", va="center", fontsize=7.5)

    Loff = {nm: lo for nm,(lo,_) in Lpos.items()}
    Roff = {nm: lo for nm,(lo,_) in Rpos.items()}
    for s,t,n,color in flows:
        lo_l, lo_r = Loff[s], Roff[t]
        hi_l, hi_r = lo_l+n, lo_r+n
        Loff[s], Roff[t] = hi_l, hi_r
        x_l, x_r = 6, 94
        verts = [(x_l, lo_l), ((x_l+x_r)/2, lo_l), ((x_l+x_r)/2, lo_r), (x_r, lo_r),
                 (x_r, hi_r), ((x_l+x_r)/2, hi_r), ((x_l+x_r)/2, hi_l), (x_l, hi_l), (x_l, lo_l)]
        codes = [MplPath.MOVETO, MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4,
                 MplPath.LINETO, MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4, MplPath.CLOSEPOLY]
        ax.add_patch(mpatches.PathPatch(MplPath(verts, codes), fc=color, ec="none", alpha=0.55))
    ax.set_title(era_label, fontsize=11)

fig, axes = plt.subplots(3, 1, figsize=(13, 18))
era_panel(axes[0], "Pre-2017 (NIFTP introduction)",  df.surgery_year < 2017)
era_panel(axes[1], "2017–2022 (post-NIFTP, pre-WHO 5th)",  (df.surgery_year >= 2017) & (df.surgery_year <= 2022))
era_panel(axes[2], "Post-2022 (WHO 5th era)",  df.surgery_year > 2022)
fig.suptitle("M088 Figure S3 — Era-stratified Sankey: historical → 2022 WHO Tier A label", fontsize=13)
fig.tight_layout()
out = FIGS/"figure_s3_era_sankey.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"Wrote {out}")

# ----- Figure S4: cumulative incidence of borderline entities ------------- #
inc = pd.read_csv(INCID)
inc.surgery_year = pd.to_numeric(inc.surgery_year, errors="coerce")
inc = inc.dropna(subset=["surgery_year"])
inc.surgery_year = inc.surgery_year.astype(int)
borderline_specs = [
    ("FT-UMP", lambda r: r.diagnosis_primary == "FTUMP", "#e377c2"),
    ("Atypical FA", lambda r: r.diagnosis_primary == "atypical_follicular_adenoma", "#bcbd22"),
    ("MI-FTC", lambda r: r.diagnosis_primary == "FTC" and r.variant == "minimally_invasive", "#d62728"),
    ("NIFTP", lambda r: r.diagnosis_primary == "NIFTP", "#17becf"),
    ("Onco-FTC", lambda r: r.diagnosis_primary == "FTC" and r.variant == "oncocytic_warthin", "#9467bd"),
]
years = list(range(1990, 2026))
fig, ax = plt.subplots(figsize=(12, 6))
for label, fn, color in borderline_specs:
    cum = []
    s = 0
    for y in years:
        rows = inc[(inc.surgery_year==y) & inc.apply(lambda r: fn(r), axis=1)]
        s += rows.n.sum() if len(rows)>0 else 0
        cum.append(s)
    ax.plot(years, cum, marker="o", lw=1.5, label=label, color=color, markersize=3)
for bp, lbl in zip([2008, 2015, 2017, 2022], ["Bethesda 2008", "ATA 2015", "NIFTP 2017", "WHO 5th 2022"]):
    ax.axvline(bp, ls="--", color="gray", alpha=0.5, lw=0.8)
    ax.text(bp+0.1, ax.get_ylim()[1]*0.02, lbl, rotation=90, fontsize=8, color="gray", va="bottom")
ax.set_xlabel("Year of surgery")
ax.set_ylabel("Cumulative cases (1990 onward)")
ax.set_title("M088 Figure S4 — Cumulative incidence of borderline / new-2017+ entities (1990–2025)")
ax.legend(loc="upper left", fontsize=10)
ax.grid(lw=0.4, alpha=0.4)
ax.set_xlim(1990, 2025)
fig.tight_layout()
out = FIGS/"figure_s4_cumulative_borderline.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"Wrote {out}")

print("\nAll extension figures done.")
