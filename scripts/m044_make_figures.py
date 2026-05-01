#!/usr/bin/env python3
"""
M044 — Build figures 1-7 PNG + underlying CSV for the manuscript.

Figures:
  1. Cohort flow diagram (4,128 → strict-DTC → analytic primary).
  2. ETE group distribution.
  3. Path-proven recurrence rate by ETE group with 95% CI (Wilson).
  4. Path-proven /100 person-years by ETE group (full analytic cohort; aligns with Table 2).
  5. Forest plot from strict-DTC + no-RAI primary logistic (already produced by
     m044_ete_fit_models.py — referenced here for completeness).
  6. Kaplan-Meier path-proven recurrence-free survival by ETE group, strict-DTC,
     surgery-date-known subset.
  7. No/negative ETE explanatory panel: tumor size, lateral-LN, ≥2 surgeries,
     median days-to-2nd by recurred-vs-not.

Inputs:  data/m044/analytic_file_v1.parquet (Cursor/Cowork-built)
         data/m044/m044_cox_primary_summary.csv
         figures/m044_forest_primary_data.csv
Outputs: figures/m044_fig{1..7}_*.png and figures/m044_fig{1..7}_*.csv
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
from lifelines import KaplanMeierFitter
import warnings
warnings.filterwarnings("ignore")

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "m044"
FIG = REPO / "figures"
FIG.mkdir(parents=True, exist_ok=True)

# ETE colors and order
ORDER = ["No/negative ETE", "Microscopic ETE", "Gross ETE"]
COL = {"No/negative ETE": "#999999", "Microscopic ETE": "#1F77B4", "Gross ETE": "#D62728"}
EXC = {'MTC','metastatic MTC','recurrent MTC','MTC/PTC mixed composite',
       'anaplastic carcinoma','metastatic anaplastic carcinoma','metastatic PTC/anaplastic carcinoma',
       'NIFTP','FTUMP','follicular adenoma','atypical follicular adenoma','Atypical hurthle cell neoplasm',
       'NUT carcinoma','adenoid cystic carcinoma'}


def wilson_ci(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    if n == 0: return (0.0, 0.0)
    z = stats.norm.ppf(1 - alpha/2)
    p = k/n
    denom = 1 + z**2/n
    centre = (p + z**2/(2*n)) / denom
    half = z*np.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return (max(0, centre - half), min(1, centre + half))


def poisson_ci(events: int, py: float, alpha: float = 0.05) -> tuple[float, float]:
    if py == 0: return (0.0, 0.0)
    if events == 0:
        lo = 0.0
        hi = stats.chi2.ppf(1-alpha/2, 2*(events+1))/2/py
    else:
        lo = stats.chi2.ppf(alpha/2, 2*events)/2/py
        hi = stats.chi2.ppf(1-alpha/2, 2*(events+1))/2/py
    return (lo*100, hi*100)  # per 100 PY


def fig1_cohort_flow(df: pd.DataFrame):
    """Figure 1 - cohort flow diagram."""
    n_total = len(df)
    excluded_n = int(df['histology_final'].isin(EXC).sum())
    strict = df[~df['histology_final'].isin(EXC)]
    n_strict = len(strict)
    primary = strict[strict['ete_group'].isin(ORDER)]
    n_prim = len(primary)
    excl_other_ete = n_strict - n_prim
    cox_subset = primary[(primary['surg_first_date'].notna()) & (primary['followup_years'] > 0)]
    n_cox = len(cox_subset)

    fig, ax = plt.subplots(figsize=(8, 9))
    ax.set_xlim(0, 10); ax.set_ylim(0, 12); ax.axis('off')

    boxes = [
        (3.5, 10.5, 6.5, 11.5, f"THYROID_2026 cohort\nn = {n_total}", "#E8E8E8"),
        (3.5, 8.0,  6.5, 9.0,  f"Strict-DTC subset\nn = {n_strict}\n(excluded {excluded_n} non-DTC / borderline)", "#D4E6F1"),
        (3.5, 5.5,  6.5, 6.5,  f"Primary 3-level analytic cohort\nn = {n_prim}\n(excluded {excl_other_ete} present-ungraded / missing)", "#A2D9CE"),
        (3.5, 3.0,  6.5, 4.0,  f"Cox subset (surgery-date known + FU > 0)\nn = {n_cox}", "#F9E79F"),
    ]
    for x1,y1,x2,y2,text,color in boxes:
        ax.add_patch(mpatches.FancyBboxPatch((x1, y1), x2-x1, y2-y1,
                                            boxstyle="round,pad=0.1", linewidth=1.5,
                                            facecolor=color, edgecolor="black"))
        ax.text((x1+x2)/2, (y1+y2)/2, text, ha='center', va='center', fontsize=10)

    arrows = [(5, 10.5, 5, 9.05), (5, 8.0, 5, 6.55), (5, 5.5, 5, 4.05)]
    for x1,y1,x2,y2 in arrows:
        ax.annotate('', xy=(x2,y2), xytext=(x1,y1),
                    arrowprops=dict(arrowstyle='->', lw=1.5))

    ax.set_title("Figure 1. M044 cohort flow diagram (strict-DTC primary analysis)",
                 fontsize=12, pad=10)

    # Side-annotations (excluded counts)
    ax.text(7.0, 9.75, f"→ Exclude non-DTC / borderline\n   (n = {excluded_n})", fontsize=9, color='#7F8C8D')
    ax.text(7.0, 7.25, f"→ Exclude present-ungraded / missing\n   (n = {excl_other_ete})", fontsize=9, color='#7F8C8D')
    ax.text(7.0, 4.75, f"→ Restrict to known dates + FU > 0\n   (excluded {n_prim - n_cox})", fontsize=9, color='#7F8C8D')

    plt.tight_layout()
    out_png = FIG / "m044_fig1_cohort_flow.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)

    pd.DataFrame([
        ("Total cohort", n_total),
        ("Excluded non-DTC / borderline", excluded_n),
        ("Strict-DTC subset", n_strict),
        ("Excluded present-ungraded / missing", excl_other_ete),
        ("Primary 3-level analytic", n_prim),
        ("Cox subset (surg-date known + FU>0)", n_cox),
    ], columns=["step", "n"]).to_csv(FIG / "m044_fig1_cohort_flow_data.csv", index=False)
    print(f"  fig1 -> {out_png}")


def fig2_ete_distribution(df: pd.DataFrame):
    """Figure 2 - ETE group distribution (full cohort + strict-DTC overlay)."""
    full = df['ete_group'].value_counts().reindex(ORDER + ["Present ungraded", "Missing/other"]).fillna(0).astype(int)
    strict = df[~df['histology_final'].isin(EXC)]['ete_group'].value_counts().reindex(ORDER + ["Present ungraded", "Missing/other"]).fillna(0).astype(int)

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(full.index))
    ax.bar(x - 0.2, full.values, 0.4, label=f"Full cohort (n={full.sum()})", color="#7F7F7F", alpha=0.85)
    ax.bar(x + 0.2, strict.values, 0.4, label=f"Strict-DTC (n={strict.sum()})", color="#1F77B4", alpha=0.95)
    ax.set_xticks(x); ax.set_xticklabels(full.index, rotation=20, ha='right')
    ax.set_ylabel("Patients (n)")
    ax.set_title("Figure 2. ETE group distribution: full cohort vs strict-DTC subset")
    for i,(f,s) in enumerate(zip(full.values, strict.values)):
        ax.text(i-0.2, f+30, str(f), ha='center', fontsize=8)
        ax.text(i+0.2, s+30, str(s), ha='center', fontsize=8)
    ax.legend()
    plt.tight_layout()
    out_png = FIG / "m044_fig2_ete_distribution.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)

    pd.DataFrame({"ete_group": full.index, "full_cohort_n": full.values, "strict_dtc_n": strict.values}).to_csv(
        FIG / "m044_fig2_ete_distribution_data.csv", index=False)
    print(f"  fig2 -> {out_png}")


def fig3_pp_rate(df: pd.DataFrame):
    """Figure 3 - Path-proven recurrence rate by ETE group with 95% Wilson CI (strict-DTC)."""
    strict = df[~df['histology_final'].isin(EXC)]
    rows = []
    for g in ORDER:
        sub = strict[strict['ete_group']==g]
        n = len(sub); k = int(sub['recurrence_path_proven'].sum())
        rate = k/n if n else 0
        lo, hi = wilson_ci(k, n)
        rows.append({"ete_group": g, "n": n, "events": k, "rate": rate*100,
                     "ci_low": lo*100, "ci_high": hi*100})
    df_p = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(df_p["ete_group"], df_p["rate"], color=[COL[g] for g in df_p["ete_group"]], alpha=0.9)
    err_low = df_p['rate'] - df_p['ci_low']
    err_high = df_p['ci_high'] - df_p['rate']
    ax.errorbar(df_p['ete_group'], df_p['rate'],
                yerr=[err_low, err_high], fmt='none', color='black', capsize=5, linewidth=1.5)
    for i, r in df_p.iterrows():
        ax.text(i, r['ci_high']+0.5, f"{r['rate']:.1f}%\n(n={r['n']}, k={r['events']})",
                ha='center', fontsize=9)
    ax.set_ylabel("Path-proven recurrence rate (%)")
    ax.set_title("Figure 3. Path-proven recurrence rate by ETE group (strict-DTC; 95% Wilson CI)")
    ax.set_ylim(0, max(df_p['ci_high'])*1.4)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    out_png = FIG / "m044_fig3_pp_rate.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)
    df_p.to_csv(FIG / "m044_fig3_pp_rate_data.csv", index=False)
    print(f"  fig3 -> {out_png}")


def fig4_pp_per_100py(df: pd.DataFrame):
    """Figure 4 — path-proven /100 PY for primary three ETE levels.

    Cohort matches M044 Table 2 (full analytic file / cohort view). Numerator and
    denominator include only rows with follow-up years > 0.
    """
    fu = pd.to_numeric(df["followup_years"], errors="coerce").fillna(0.0)
    work = df.assign(_fu=fu)
    pos = work[work["_fu"] > 0]
    rows = []
    for g in ORDER:
        sub = pos[pos["ete_group"] == g]
        py = float(sub["_fu"].sum())
        events = int(sub["recurrence_path_proven"].fillna(False).astype(bool).sum())
        rate = (events / py * 100) if py else 0
        lo, hi = poisson_ci(events, py)
        rows.append({"ete_group": g, "n": len(sub), "events": events, "person_years": round(py,1),
                     "rate_per_100py": rate, "ci_low": lo, "ci_high": hi})
    df_p = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.bar(df_p['ete_group'], df_p['rate_per_100py'], color=[COL[g] for g in df_p['ete_group']], alpha=0.9)
    err_low = df_p['rate_per_100py'] - df_p['ci_low']
    err_high = df_p['ci_high'] - df_p['rate_per_100py']
    ax.errorbar(df_p['ete_group'], df_p['rate_per_100py'],
                yerr=[err_low, err_high], fmt='none', color='black', capsize=5, linewidth=1.5)
    for i, r in df_p.iterrows():
        ax.text(i, r['ci_high']+0.05, f"{r['rate_per_100py']:.2f}\n(events={r['events']}, PY={r['person_years']:.0f})",
                ha='center', fontsize=9)
    ax.set_ylabel("Path-proven recurrence rate (per 100 person-years)")
    ax.set_title(
        "Figure 4. Path-proven recurrence per 100 PY by ETE group "
        "(full M044 cohort; FU>0 numerator & denominator)"
    )
    ax.set_ylim(0, max(df_p['ci_high'])*1.4)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    out_png = FIG / "m044_fig4_pp_per_100py.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)
    df_p.to_csv(FIG / "m044_fig4_pp_per_100py_data.csv", index=False)
    print(f"  fig4 -> {out_png}")


def fig5_forest_already_exists():
    """Figure 5 - Forest plot already produced by m044_ete_fit_models.py."""
    src = FIG / "m044_forest_primary.png"
    if src.exists():
        # Re-link: copy to fig5 name
        dst = FIG / "m044_fig5_forest_primary.png"
        dst.write_bytes(src.read_bytes())
        src_csv = FIG / "m044_forest_primary_data.csv"
        if src_csv.exists():
            (FIG / "m044_fig5_forest_primary_data.csv").write_bytes(src_csv.read_bytes())
        print(f"  fig5 -> {dst} (copied from forest_primary)")
    else:
        print("  fig5 -> SKIP (m044_forest_primary.png not found)")


def fig6_km(df: pd.DataFrame):
    """Figure 6 - Kaplan-Meier path-proven RFS by ETE group (strict-DTC, surg-date known, FU>0)."""
    strict = df[~df['histology_final'].isin(EXC)]
    sub = strict[(strict['surg_first_date'].notna()) & (strict['followup_years'] > 0)
                 & (strict['ete_group'].isin(ORDER))].copy()
    fig, ax = plt.subplots(figsize=(8, 6))
    rows_export = []
    for g in ORDER:
        d = sub[sub['ete_group']==g]
        if len(d) == 0: continue
        kmf = KaplanMeierFitter()
        T = d['followup_years'].values
        E = d['recurrence_path_proven'].astype(int).values
        kmf.fit(T, event_observed=E, label=f"{g} (n={len(d)}, e={int(E.sum())})")
        kmf.plot_survival_function(ax=ax, ci_show=False, color=COL[g], linewidth=2)
        # Event-table snapshot at 1, 3, 5 years
        for tp in [1, 3, 5, 10]:
            try:
                surv = float(kmf.predict(tp))
                rows_export.append({"ete_group": g, "year": tp, "rfs_pct": surv*100, "n_at_risk": int((T>=tp).sum())})
            except Exception: pass
    ax.set_xlabel("Years from first surgery")
    ax.set_ylabel("Path-proven recurrence-free survival")
    ax.set_title("Figure 6. KM path-proven recurrence-free survival by ETE group\n(strict-DTC, surgery-date known, FU>0)")
    ax.set_ylim(0.5, 1.02)
    ax.set_xlim(0, min(15, sub['followup_years'].max()))
    ax.grid(alpha=0.3)
    plt.tight_layout()
    out_png = FIG / "m044_fig6_km_pp.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)
    pd.DataFrame(rows_export).to_csv(FIG / "m044_fig6_km_pp_data.csv", index=False)
    print(f"  fig6 -> {out_png}")


def fig7_noneg_panel(df: pd.DataFrame):
    """Figure 7 - No/negative ETE explanatory panel."""
    sub = df[df['ete_group']=='No/negative ETE'].copy()
    sub['recurred'] = sub['recurrence_status_final'].isin(['path_proven', 'imaging_only_unconfirmed'])
    rec = sub[sub['recurred']]; nor = sub[~sub['recurred']]

    fig, axes = plt.subplots(2, 2, figsize=(11, 8))

    # Panel A: tumor size
    axes[0,0].boxplot([nor['tumor_size_cm'].dropna(), rec['tumor_size_cm'].dropna()],
                      labels=[f"No recurrence (n={len(nor)})", f"Recurred (n={len(rec)})"],
                      patch_artist=True, boxprops=dict(facecolor='#A2D9CE'))
    axes[0,0].set_ylabel("Tumor size (cm)")
    axes[0,0].set_title("A. Tumor size by recurrence status")

    # Panel B: lateral-LN positivity
    lat_nor = (nor.get('lateral_pos_flag', pd.Series([0]*len(nor))).fillna(0).sum() / len(nor) * 100) if len(nor) else 0
    lat_rec = (rec.get('lateral_pos_flag', pd.Series([0]*len(rec))).fillna(0).sum() / len(rec) * 100) if len(rec) else 0
    axes[0,1].bar(['No recurrence','Recurred'], [lat_nor, lat_rec], color=['#A2D9CE','#E59866'])
    axes[0,1].set_ylabel("Lateral-LN positive (%)")
    axes[0,1].set_title("B. Lateral nodal positivity")
    for i,v in enumerate([lat_nor, lat_rec]):
        axes[0,1].text(i, v+1, f"{v:.1f}%", ha='center')

    # Panel C: ≥2 surgeries
    nor['has_2nd'] = (nor['n_surgeries'].fillna(1) >= 2).astype(int)
    rec['has_2nd'] = (rec['n_surgeries'].fillna(1) >= 2).astype(int)
    s2_nor = nor['has_2nd'].sum()/len(nor)*100 if len(nor) else 0
    s2_rec = rec['has_2nd'].sum()/len(rec)*100 if len(rec) else 0
    axes[1,0].bar(['No recurrence','Recurred'], [s2_nor, s2_rec], color=['#A2D9CE','#E59866'])
    axes[1,0].set_ylabel("Patients with ≥2 surgeries (%)")
    axes[1,0].set_title("C. Reoperative pathway")
    for i,v in enumerate([s2_nor, s2_rec]):
        axes[1,0].text(i, v+1, f"{v:.1f}%", ha='center')

    # Panel D: median days to 2nd
    d2_nor = nor['days_to_2nd'].dropna()
    d2_rec = rec['days_to_2nd'].dropna()
    if len(d2_nor) and len(d2_rec):
        axes[1,1].boxplot([d2_nor, d2_rec], labels=[f"No recurrence (n={len(d2_nor)})", f"Recurred (n={len(d2_rec)})"],
                          patch_artist=True, boxprops=dict(facecolor='#A2D9CE'))
        axes[1,1].set_ylabel("Days first → second surgery")
        axes[1,1].set_title("D. Days between surgeries")
    else:
        axes[1,1].text(0.5, 0.5, "Insufficient data", ha='center', va='center', transform=axes[1,1].transAxes)

    fig.suptitle("Figure 7. No/negative ETE subgroup explanatory panel", fontsize=13)
    plt.tight_layout()
    out_png = FIG / "m044_fig7_noneg_panel.png"
    fig.savefig(out_png, dpi=300, bbox_inches='tight')
    plt.close(fig)

    # CSV summary
    pd.DataFrame([
        ("Tumor size mean (cm)", round(nor['tumor_size_cm'].mean(),2), round(rec['tumor_size_cm'].mean(),2)),
        ("Tumor size median (cm)", round(nor['tumor_size_cm'].median(),2), round(rec['tumor_size_cm'].median(),2)),
        ("Lateral LN+ rate (%)", round(lat_nor,1), round(lat_rec,1)),
        (">=2 surgeries (%)", round(s2_nor,1), round(s2_rec,1)),
        ("Median days to 2nd surgery", int(d2_nor.median()) if len(d2_nor) else None,
                                       int(d2_rec.median()) if len(d2_rec) else None),
        ("n", len(nor), len(rec)),
    ], columns=["metric", "no_recurrence", "recurred"]).to_csv(FIG / "m044_fig7_noneg_panel_data.csv", index=False)
    print(f"  fig7 -> {out_png}")


def main():
    pq = DATA / "analytic_file_v1.parquet"
    df = pd.read_parquet(pq)
    print(f"Loaded {pq.name}: {df.shape}")
    fig1_cohort_flow(df)
    fig2_ete_distribution(df)
    fig3_pp_rate(df)
    fig4_pp_per_100py(df)
    fig5_forest_already_exists()
    fig6_km(df)
    fig7_noneg_panel(df)
    print("Done.")


if __name__ == "__main__":
    main()
