"""Build M038 figures.

Figure 1 — Composite massive-goiter flag composition (Venn diagram of 3 components).
Figure 2 — Era-stratified massive-flag prevalence (1999-2025 line plot, with cohort-size bars).
Figure 3 — Strict-definition complication rates by arm (forest-style strip).
Figure 4 — Component coverage by era (CT/MRI documentation expansion narrative).

PNG @ 300 DPI + underlying CSV. Saved to M038_submission_package_v1_0/06_figures/.
"""
import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

OUT_DIR = "/Users/loganglosser/THYROID_2026/M038_submission_package_v1_0/06_figures"
os.makedirs(OUT_DIR, exist_ok=True)

# ============================================================
# Figure 1 — 3-circle Venn (drawn manually; matplotlib-venn may not be installed)
# Numbers from §3.1 (audit-validated): weight-only 898; substernal-only 145; airway-only 429;
# W∩S only 18; W∩A only 127; S∩A only 498; all-three 386.
# ============================================================
# Region counts (per inclusion-exclusion):
#   Weight only      = 898
#   Substernal only  = 145
#   Airway only      = 429
#   W∩S only         = |W∩S| - |W∩S∩A| = 404 - 386 = 18
#   W∩A only         = |W∩A| - |W∩S∩A| = 513 - 386 = 127
#   S∩A only         = |S∩A| - |W∩S∩A| = 884 - 386 = 498
#   All three        = 386
# Sum = 898+145+429+18+127+498+386 = 2,501 ✓

regions = {
    "weight_only": 898,
    "substernal_only": 145,
    "airway_only": 429,
    "weight_substernal_only": 18,
    "weight_airway_only": 127,
    "substernal_airway_only": 498,
    "all_three": 386,
}

fig, ax = plt.subplots(figsize=(7.5, 7.5), dpi=300)
ax.set_xlim(0, 10); ax.set_ylim(0, 10); ax.set_aspect("equal"); ax.axis("off")

# 3 circles
from matplotlib.patches import Circle
c1 = Circle((3.7, 6.0), 2.6, alpha=0.40, color="#5B9BD5", linewidth=1.2)
c2 = Circle((6.3, 6.0), 2.6, alpha=0.40, color="#ED7D31", linewidth=1.2)
c3 = Circle((5.0, 3.7), 2.6, alpha=0.40, color="#70AD47", linewidth=1.2)
ax.add_patch(c1); ax.add_patch(c2); ax.add_patch(c3)

# Set labels (outside circles, top)
ax.text(2.0, 9.0, "Weight ≥ 100 g\n(n=1,429)", ha="center", fontsize=11, color="#1F4E78", weight="bold")
ax.text(8.0, 9.0, "Substernal\n(CT or MRI)\n(n=1,047)", ha="center", fontsize=11, color="#9C480F", weight="bold")
ax.text(5.0, 0.5, "Airway compromise (CT)\n(n=1,440)", ha="center", fontsize=11, color="#385723", weight="bold")

# Region count labels (inside)
ax.text(2.6, 6.6, f"898", ha="center", fontsize=14, weight="bold")  # weight only
ax.text(2.6, 6.1, "weight only", ha="center", fontsize=8, style="italic")
ax.text(7.4, 6.6, f"145", ha="center", fontsize=14, weight="bold")  # substernal only
ax.text(7.4, 6.1, "substernal only", ha="center", fontsize=8, style="italic")
ax.text(5.0, 2.0, f"429", ha="center", fontsize=14, weight="bold")  # airway only
ax.text(5.0, 1.5, "airway only", ha="center", fontsize=8, style="italic")
ax.text(5.0, 7.2, f"18", ha="center", fontsize=12, weight="bold")  # W∩S only
ax.text(5.0, 6.85, "W∩S", ha="center", fontsize=8, style="italic")
ax.text(3.4, 4.6, f"127", ha="center", fontsize=12, weight="bold")  # W∩A only
ax.text(3.4, 4.25, "W∩A", ha="center", fontsize=8, style="italic")
ax.text(6.6, 4.6, f"498", ha="center", fontsize=12, weight="bold")  # S∩A only
ax.text(6.6, 4.25, "S∩A", ha="center", fontsize=8, style="italic")
ax.text(5.0, 5.4, f"386", ha="center", fontsize=14, weight="bold", color="#7030A0")  # all three
ax.text(5.0, 5.05, "all three", ha="center", fontsize=8, style="italic", color="#7030A0")

ax.set_title("Figure 1. Composite massive-goiter flag composition\n(n = 2,501 of 10,871 patients; 23.0%)",
             fontsize=13, weight="bold", pad=10)

# Footer
ax.text(5.0, -0.5, "Inclusion-exclusion check: 898+145+429+18+127+498+386 = 2,501 ✓",
        ha="center", fontsize=9, style="italic", color="#404040")

plt.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "fig1_composite_flag_venn.png"), dpi=300, bbox_inches="tight", facecolor="white")
plt.close(fig)

# Figure 1 underlying CSV
pd.DataFrame([
    {"region": "weight_only", "n": 898},
    {"region": "substernal_only", "n": 145},
    {"region": "airway_only", "n": 429},
    {"region": "weight_substernal_only", "n": 18},
    {"region": "weight_airway_only", "n": 127},
    {"region": "substernal_airway_only", "n": 498},
    {"region": "all_three", "n": 386},
    {"region": "TOTAL", "n": 2501},
]).to_csv(os.path.join(OUT_DIR, "fig1_composite_flag_venn.csv"), index=False)

# ============================================================
# Figure 2 — Era-stratified massive-flag prevalence
# ============================================================
era_data = pd.DataFrame([
    {"era": "1999–2004", "total": 903, "massive": 110, "pct": 110/903*100},
    {"era": "2005–2009", "total": 1191, "massive": 142, "pct": 142/1191*100},
    {"era": "2010–2014", "total": 1885, "massive": 240, "pct": 240/1885*100},
    {"era": "2015–2019", "total": 2935, "massive": 731, "pct": 731/2935*100},
    {"era": "2020–2025", "total": 1817, "massive": 517, "pct": 517/1817*100},
])

fig, ax1 = plt.subplots(figsize=(9, 5.5), dpi=300)
x = np.arange(len(era_data))
ax1.bar(x, era_data["total"], color="#D9E1F2", edgecolor="#5B9BD5", label="Total surgical n")
ax1.bar(x, era_data["massive"], color="#5B9BD5", edgecolor="#1F4E78", label="Massive (composite)")
ax1.set_xticks(x); ax1.set_xticklabels(era_data["era"], fontsize=10)
ax1.set_ylabel("Patients (n)", fontsize=11)
ax1.set_xlabel("Surgical era", fontsize=11)
ax1.set_ylim(0, era_data["total"].max() * 1.15)
for i, (tot, mas) in enumerate(zip(era_data["total"], era_data["massive"])):
    ax1.text(i, tot + 80, f"{tot:,}", ha="center", fontsize=8, color="#404040")
    ax1.text(i, mas + 30, f"{mas:,}", ha="center", fontsize=8, color="white", weight="bold")

ax2 = ax1.twinx()
ax2.plot(x, era_data["pct"], "o-", color="#C00000", linewidth=2.0, markersize=9, label="% Massive")
for i, p in enumerate(era_data["pct"]):
    ax2.annotate(f"{p:.1f}%", (i, p), textcoords="offset points", xytext=(0, 12),
                 ha="center", fontsize=10, weight="bold", color="#C00000")
ax2.set_ylabel("% Massive of era cohort", fontsize=11, color="#C00000")
ax2.set_ylim(0, 35)
ax2.tick_params(axis="y", labelcolor="#C00000")

# Combined legend
h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=9, framealpha=0.92)

ax1.set_title("Figure 2. Era-stratified prevalence of composite massive-goiter flag\n(Emory University, 1999–2025; n = 8,731 with known surgical date of 10,871 total)",
              fontsize=12, weight="bold", pad=10)
ax1.text(2.0, -era_data["total"].max() * 0.10,
         "Note: 2,140 additional patients (19.7%) have no recorded surgical date and are excluded from this view.\n"
         "The post-2015 prevalence rise is partially attributable to expanded CT/MRI documentation\n"
         "and the institutional NLP airway pipeline rollout (see Supp S2 component-coverage tab).",
         ha="left", fontsize=8, style="italic", color="#595959")

plt.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "fig2_era_prevalence.png"), dpi=300, bbox_inches="tight", facecolor="white")
plt.close(fig)
era_data.to_csv(os.path.join(OUT_DIR, "fig2_era_prevalence.csv"), index=False)

# ============================================================
# Figure 3 — Strict-definition complication rates by arm (descriptive bar chart)
# ============================================================
comps = pd.DataFrame([
    {"label": "Any confirmed", "M_pct": 132/2501*100, "NM_pct": 268/8370*100, "rr": (132/2501)/(268/8370)},
    {"label": "Hypopara — transient (<6mo)", "M_pct": 83/2501*100, "NM_pct": 197/8370*100, "rr": (83/2501)/(197/8370)},
    {"label": "Hypopara — permanent (>6mo)", "M_pct": 4/2501*100, "NM_pct": 12/8370*100, "rr": (4/2501)/(12/8370)},
    {"label": "Hematoma", "M_pct": 23/2501*100, "NM_pct": 45/8370*100, "rr": (23/2501)/(45/8370)},
    {"label": "Seroma", "M_pct": 12/2501*100, "NM_pct": 27/8370*100, "rr": (12/2501)/(27/8370)},
    {"label": "RLN injury", "M_pct": 14/2501*100, "NM_pct": 7/8370*100, "rr": (14/2501)/(7/8370)},
    {"label": "VC paralysis", "M_pct": 19/2501*100, "NM_pct": 4/8370*100, "rr": (19/2501)/(4/8370)},
    {"label": "Chyle leak", "M_pct": 2/2501*100, "NM_pct": 1/8370*100, "rr": (2/2501)/(1/8370)},
    {"label": "Hypocalcemia (postop)", "M_pct": 1/2501*100, "NM_pct": 8/8370*100, "rr": (1/2501)/(8/8370)},
    {"label": "Mortality (all-cause)", "M_pct": 59/2501*100, "NM_pct": 133/8370*100, "rr": (59/2501)/(133/8370)},
])
comps = comps.sort_values("M_pct", ascending=True).reset_index(drop=True)

fig, ax = plt.subplots(figsize=(9, 6.5), dpi=300)
y = np.arange(len(comps))
h = 0.36
ax.barh(y - h/2, comps["NM_pct"], height=h, color="#A6A6A6", edgecolor="#595959", label="Non-massive (n=8,370)")
ax.barh(y + h/2, comps["M_pct"], height=h, color="#5B9BD5", edgecolor="#1F4E78", label="Massive (n=2,501)")
for i, row in comps.iterrows():
    ax.text(max(row["M_pct"], row["NM_pct"]) + 0.07, i,
            f"RR ≈ {row['rr']:.2f}" if pd.notna(row["rr"]) and not np.isinf(row["rr"]) else "",
            va="center", fontsize=8.5, color="#404040")
ax.set_yticks(y); ax.set_yticklabels(comps["label"], fontsize=10)
ax.set_xlabel("Strict-definition rate (% of arm)", fontsize=11)
ax.set_title("Figure 3. Strict-definition perioperative complication rates,\nmassive vs non-massive arms (per standing rule, post-mig_252 + mig_255)",
             fontsize=12, weight="bold", pad=10)
ax.legend(loc="lower right", fontsize=9)
ax.set_xlim(0, max(comps["M_pct"].max(), comps["NM_pct"].max()) * 1.45)
ax.grid(axis="x", linestyle=":", color="#CCCCCC", alpha=0.7)
ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)

plt.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "fig3_complications_bar.png"), dpi=300, bbox_inches="tight", facecolor="white")
plt.close(fig)
comps.to_csv(os.path.join(OUT_DIR, "fig3_complications_bar.csv"), index=False)

# ============================================================
# Figure 4 — Component coverage by era (drives era-rise narrative)
# ============================================================
cov = pd.DataFrame([
    {"era":"1999–2004","arm_n":903,"gland":110+673,"ct_subst":1+0,"mri_subst":0,"ct_airway":1+0},
    {"era":"2005–2009","arm_n":1191,"gland":141+918,"ct_subst":0+1,"mri_subst":0+1,"ct_airway":1+2},
    {"era":"2010–2014","arm_n":1885,"gland":234+1505,"ct_subst":25+17,"mri_subst":8+6,"ct_airway":39+71},
    {"era":"2015–2019","arm_n":2935,"gland":657+1886,"ct_subst":406+153,"mri_subst":72+46,"ct_airway":497+388},
    {"era":"2020–2025","arm_n":1817,"gland":421+988,"ct_subst":331+128,"mri_subst":65+59,"ct_airway":404+378},
])
for c in ["gland","ct_subst","mri_subst","ct_airway"]:
    cov[c+"_pct"] = cov[c] / cov["arm_n"] * 100

fig, ax = plt.subplots(figsize=(9, 5.5), dpi=300)
x = np.arange(len(cov))
ax.plot(x, cov["gland_pct"], "o-", color="#1F4E78", linewidth=2, markersize=8, label="Gland weight (path synoptic)")
ax.plot(x, cov["ct_subst_pct"], "s-", color="#C00000", linewidth=2, markersize=8, label="CT substernal")
ax.plot(x, cov["mri_subst_pct"], "^-", color="#7030A0", linewidth=2, markersize=8, label="MRI substernal")
ax.plot(x, cov["ct_airway_pct"], "D-", color="#385723", linewidth=2, markersize=8, label="CT airway compromise")
ax.set_xticks(x); ax.set_xticklabels(cov["era"], fontsize=10)
ax.set_xlabel("Surgical era", fontsize=11)
ax.set_ylabel("Coverage (% with non-NULL value)", fontsize=11)
ax.set_ylim(0, 100)
ax.set_title("Figure 4. Composite-flag source-column coverage by era\n(Documentation-expansion driver of post-2015 massive-prevalence rise)",
             fontsize=12, weight="bold", pad=10)
ax.legend(loc="center left", fontsize=9, framealpha=0.92)
ax.grid(axis="y", linestyle=":", color="#CCCCCC", alpha=0.7)
ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
ax.text(0.02, -22, "Note: cohort-wide, both arms combined. Coverage = fraction of arm with non-NULL value (TRUE or FALSE).\n"
                   "Pre-2010 CT/MRI documentation is essentially absent (<3%); the institutional NLP airway pipeline "
                   "roll-out (post-2015) drives the airway-component flag.",
        fontsize=8, style="italic", color="#595959")
plt.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "fig4_component_coverage.png"), dpi=300, bbox_inches="tight", facecolor="white")
plt.close(fig)
cov.to_csv(os.path.join(OUT_DIR, "fig4_component_coverage.csv"), index=False)

print("All figures saved to:", OUT_DIR)
for f in sorted(os.listdir(OUT_DIR)):
    print(f"  {f}")
