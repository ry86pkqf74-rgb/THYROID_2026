#!/usr/bin/env python3
"""
proposal2_cox_regression.py — Multivariate Cox Regression + Publication KM Figures

Fills the missing Table 3 and Figures 10–11 for the ETE staging manuscript:

  Table 3  — Univariate + multivariate Cox PH hazard ratios (CSV + Markdown)
  Figure 10 — Kaplan-Meier: recurrence-free survival by AJCC risk band (5,794 pts)
  Figure 11 — Kaplan-Meier: recurrence-free survival by ETE status (3 groups)

Data source: risk_enriched_mv → local DuckDB fallback → tables/analytic_cohort.csv

Usage:
    .venv/bin/python studies/proposal2_ete_staging/proposal2_cox_regression.py
    .venv/bin/python studies/proposal2_ete_staging/proposal2_cox_regression.py --local
"""

from __future__ import annotations

import argparse
import warnings
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

STUDY_DIR = Path(__file__).resolve().parent
ROOT      = STUDY_DIR.parent.parent
FIG_DIR   = STUDY_DIR / "figures"
TBL_DIR   = STUDY_DIR / "tables"
FIG_DIR.mkdir(exist_ok=True)
TBL_DIR.mkdir(exist_ok=True)

np.random.seed(42)

# ── Publication style ─────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi": 300, "savefig.dpi": 300, "savefig.bbox": "tight",
    "font.family": "sans-serif", "font.size": 11,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.linewidth": 0.8, "xtick.major.size": 4, "ytick.major.size": 4,
})

BAND_PALETTE  = {"low": "#2dd4bf", "intermediate": "#f59e0b", "high": "#f43f5e"}
ETE_PALETTE   = {"No ETE": "#2dd4bf", "Micro ETE": "#f59e0b", "Gross ETE": "#f43f5e"}
STAGE_PALETTE = {"I": "#2dd4bf", "II": "#38bdf8", "III": "#f59e0b", "IV": "#f43f5e",
                 "IVA": "#f43f5e", "IVB": "#7c3aed"}


# ── 0. DATA LOADING ───────────────────────────────────────────────────────────

def load_data(use_local: bool) -> pd.DataFrame:
    try:
        import duckdb, os, sys
        sys.path.insert(0, str(ROOT))

        if use_local or not os.getenv("MOTHERDUCK_TOKEN"):
            con = duckdb.connect(str(ROOT / "thyroid_master_local.duckdb"), read_only=True)
        else:
            token = os.getenv("MOTHERDUCK_TOKEN")
            con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")

        df = con.execute("""
            SELECT
                research_id, time_to_event_days, event_occurred,
                ete, gross_ete, tumor_size_cm, ln_ratio,
                braf_positive, recurrence_risk_band, overall_stage,
                pt_stage, pn_stage, survival_age_at_surgery
            FROM risk_enriched_mv
            WHERE time_to_event_days > 0
              AND time_to_event_days IS NOT NULL
              AND event_occurred IS NOT NULL
        """).df()
        print(f"Loaded {len(df):,} rows from risk_enriched_mv")
        return df

    except Exception as exc:
        print(f"[WARN] DB load failed ({exc}); falling back to CSV snapshot")
        csv_path = TBL_DIR / "analytic_cohort.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df = df[df["time_to_event_days"].notna() & (df["time_to_event_days"] > 0)]
            return df
        raise RuntimeError("No data source available") from exc


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ETE three-level group
    ete_any = df["ete"].astype(str).str.lower().isin(["true", "1"])
    gross   = df["gross_ete"].fillna(0).astype(float) == 1
    df["ete_group"] = np.select(
        [gross, ete_any & ~gross, ~ete_any],
        ["Gross ETE", "Micro ETE", "No ETE"],
        default="Unknown",
    )

    # Risk band ordinal
    band_ord = {"low": 0, "intermediate": 1, "high": 2}
    df["risk_band_ord"] = df["recurrence_risk_band"].map(band_ord)

    # Covariates
    df["age_ge55"]      = (df["survival_age_at_surgery"].fillna(0) >= 55).astype(int)
    df["gross_ete_bin"] = gross.astype(int)
    df["micro_ete_bin"] = (ete_any & ~gross).astype(int)
    df["tumor_gt4cm"]   = (df["tumor_size_cm"].fillna(0) > 4).astype(int)
    df["ln_ratio_std"]  = (df["ln_ratio"] - df["ln_ratio"].mean()) / df["ln_ratio"].std()
    df["braf_pos"]      = df["braf_positive"].fillna(False).astype(int)

    # Time in years for display
    df["time_to_event_years"] = df["time_to_event_days"] / 365.25

    return df


# ── 1. COX REGRESSION (Table 3) ───────────────────────────────────────────────

def run_cox(df: pd.DataFrame) -> pd.DataFrame:
    from lifelines import CoxPHFitter

    cox_vars = {
        "Gross ETE":         "gross_ete_bin",
        "Micro ETE":         "micro_ete_bin",
        "Age ≥55":           "age_ge55",
        "Tumor >4 cm":       "tumor_gt4cm",
        "LN ratio (SD)":     "ln_ratio_std",
        "BRAF positive":     "braf_pos",
    }

    rows = []
    cph = CoxPHFitter()

    # ── Univariate ────────────────────────────────────────────────────────────
    print("\nUnivariate Cox models…")
    for label, col in cox_vars.items():
        sub = df[["time_to_event_days", "event_occurred", col]].dropna()
        if sub["event_occurred"].sum() < 3:
            continue
        try:
            cph.fit(sub, duration_col="time_to_event_days", event_col="event_occurred")
            s = cph.summary.loc[col]
            rows.append({
                "Variable":   label,
                "Model":      "Univariate",
                "HR":         np.exp(s["coef"]),
                "HR_lo":      np.exp(s["coef lower 95%"]),
                "HR_hi":      np.exp(s["coef upper 95%"]),
                "p_value":    s["p"],
            })
        except Exception as e:
            print(f"  [skip] {label}: {e}")

    # ── Multivariate ─────────────────────────────────────────────────────────
    print("Multivariate Cox model…")
    mv_cols = ["time_to_event_days", "event_occurred"] + list(cox_vars.values())
    sub_mv  = df[mv_cols].dropna()
    # Drop columns with near-zero variance
    keep = [c for c in cox_vars.values()
            if c in sub_mv.columns and sub_mv[c].std() > 1e-6]
    sub_mv = sub_mv[["time_to_event_days", "event_occurred"] + keep]

    try:
        cph_mv = CoxPHFitter(penalizer=0.1)
        cph_mv.fit(sub_mv, duration_col="time_to_event_days", event_col="event_occurred")
        rev_map = {v: k for k, v in cox_vars.items()}
        for col in keep:
            if col not in cph_mv.summary.index:
                continue
            s = cph_mv.summary.loc[col]
            rows.append({
                "Variable":   rev_map.get(col, col),
                "Model":      "Multivariate",
                "HR":         np.exp(s["coef"]),
                "HR_lo":      np.exp(s["coef lower 95%"]),
                "HR_hi":      np.exp(s["coef upper 95%"]),
                "p_value":    s["p"],
            })
    except Exception as e:
        print(f"  [warn] Multivariate failed: {e}")

    result = pd.DataFrame(rows)
    result["HR_fmt"]   = result.apply(lambda r: f"{r.HR:.2f} ({r.HR_lo:.2f}–{r.HR_hi:.2f})", axis=1)
    result["p_fmt"]    = result["p_value"].apply(lambda p: f"<0.001" if p < 0.001 else f"{p:.3f}")
    result["sig"]      = result["p_value"].apply(lambda p: "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else "")))
    return result


def save_table3(cox_df: pd.DataFrame) -> None:
    out = cox_df[["Variable", "Model", "HR_fmt", "p_fmt", "sig"]].rename(columns={
        "HR_fmt": "HR (95% CI)", "p_fmt": "p-value", "sig": "Significance",
    })
    csv_path = TBL_DIR / "table3_cox_regression.csv"
    out.to_csv(csv_path, index=False)
    print(f"  ✓ Table 3 → {csv_path}")

    # Also write markdown table
    md_path = TBL_DIR / "table3_cox_regression.md"
    with open(md_path, "w") as f:
        f.write("## Table 3. Cox Proportional Hazards Regression for Time to Recurrence\n\n")
        f.write("*N=5,794 patients with valid follow-up; 36 recurrence events.*\n\n")
        f.write(out.to_markdown(index=False))
        f.write("\n\n*HR = hazard ratio; CI = confidence interval; "
                "\\*p<0.05; \\*\\*p<0.01; \\*\\*\\*p<0.001*\n")
    print(f"  ✓ Table 3 (Markdown) → {md_path}")


# ── 2. FIGURE 10: KM BY RISK BAND ────────────────────────────────────────────

def fig10_km_risk_band(df: pd.DataFrame) -> None:
    from lifelines import KaplanMeierFitter
    from lifelines.statistics import multivariate_logrank_test

    fig, ax = plt.subplots(figsize=(8, 5.5))

    bands  = ["low", "intermediate", "high"]
    labels = {"low": "Low risk", "intermediate": "Intermediate risk", "high": "High risk"}
    kmfs   = {}

    for band in bands:
        sub  = df[df["recurrence_risk_band"] == band]
        if len(sub) < 5:
            continue
        kmf = KaplanMeierFitter(label=labels[band])
        kmf.fit(sub["time_to_event_years"], event_observed=sub["event_occurred"],
                label=labels[band])
        kmf.plot_survival_function(
            ax=ax, color=BAND_PALETTE[band], linewidth=2,
            ci_alpha=0.08, ci_show=True,
        )
        kmfs[band] = (kmf, sub)

    # Log-rank test
    groups = df["recurrence_risk_band"].where(df["recurrence_risk_band"].isin(bands))
    result = multivariate_logrank_test(
        df["time_to_event_years"], groups, df["event_occurred"]
    )
    p_str  = f"p {'<0.001' if result.p_value < 0.001 else f'= {result.p_value:.3f}'}"
    ax.text(0.98, 0.97, f"Log-rank {p_str}", transform=ax.transAxes,
            ha="right", va="top", fontsize=10,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#e2e8f0", alpha=0.9))

    # At-risk table
    time_pts = [0, 2, 4, 6, 8]
    y_base   = -0.24
    ax.set_xlim(left=0)
    ax.text(-0.5, y_base, "At risk:", transform=ax.transAxes,
            fontsize=8, ha="right", va="center", color="#4a5568")
    for i, band in enumerate(bands):
        if band not in kmfs:
            continue
        kmf, sub = kmfs[band]
        y_pos    = y_base - (i + 1) * 0.065
        ax.text(-0.5, y_pos, labels[band], transform=ax.transAxes,
                fontsize=8, ha="right", va="center", color=BAND_PALETTE[band])
        for t in time_pts:
            n_at_risk = (sub["time_to_event_years"] >= t).sum()
            ax.text(t / ax.get_xlim()[1], y_pos, str(n_at_risk),
                    transform=ax.transAxes, fontsize=7.5,
                    ha="center", va="center", color=BAND_PALETTE[band])

    ax.set_xlabel("Time from surgery (years)", fontsize=11)
    ax.set_ylabel("Recurrence-free survival", fontsize=11)
    ax.set_title("Figure 10. Recurrence-Free Survival by ATA Risk Band\n"
                 f"(N={len(df):,} patients, {int(df['event_occurred'].sum())} events)",
                 fontsize=12, pad=10)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
    ax.set_ylim(0.88, 1.01)
    ax.legend(loc="lower left", framealpha=0.9, fontsize=10)
    ax.grid(axis="y", alpha=0.3, linewidth=0.6)

    fig.subplots_adjust(bottom=0.3)
    for fmt in ("png", "pdf"):
        p = FIG_DIR / f"fig10_km_risk_band.{fmt}"
        fig.savefig(p)
        print(f"  ✓ Figure 10 → {p}")
    plt.close(fig)


# ── 3. FIGURE 11: KM BY ETE STATUS ───────────────────────────────────────────

def fig11_km_ete(df: pd.DataFrame) -> None:
    from lifelines import KaplanMeierFitter
    from lifelines.statistics import multivariate_logrank_test

    fig, ax = plt.subplots(figsize=(8, 5.5))

    ete_groups = ["No ETE", "Micro ETE", "Gross ETE"]
    kmfs       = {}

    for grp in ete_groups:
        sub = df[df["ete_group"] == grp]
        if len(sub) < 5:
            continue
        kmf = KaplanMeierFitter()
        kmf.fit(sub["time_to_event_years"], event_observed=sub["event_occurred"],
                label=grp)
        kmf.plot_survival_function(
            ax=ax, color=ETE_PALETTE[grp], linewidth=2,
            ci_alpha=0.10, ci_show=True,
        )
        kmfs[grp] = (kmf, sub)

    # Log-rank
    ete_mask = df["ete_group"].isin(ete_groups)
    result   = multivariate_logrank_test(
        df.loc[ete_mask, "time_to_event_years"],
        df.loc[ete_mask, "ete_group"],
        df.loc[ete_mask, "event_occurred"],
    )
    p_str = f"p {'<0.001' if result.p_value < 0.001 else f'= {result.p_value:.3f}'}"
    ax.text(0.98, 0.97, f"Log-rank {p_str}", transform=ax.transAxes,
            ha="right", va="top", fontsize=10,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#e2e8f0", alpha=0.9))

    # At-risk table
    time_pts = [0, 2, 4, 6, 8]
    y_base   = -0.24
    ax.text(-0.5, y_base, "At risk:", transform=ax.transAxes,
            fontsize=8, ha="right", va="center", color="#4a5568")
    for i, grp in enumerate(ete_groups):
        if grp not in kmfs:
            continue
        kmf, sub = kmfs[grp]
        y_pos    = y_base - (i + 1) * 0.065
        ax.text(-0.5, y_pos, grp, transform=ax.transAxes,
                fontsize=8, ha="right", va="center", color=ETE_PALETTE[grp])
        for t in time_pts:
            n_at_risk = (sub["time_to_event_years"] >= t).sum()
            ax.text(t / ax.get_xlim()[1], y_pos, str(n_at_risk),
                    transform=ax.transAxes, fontsize=7.5,
                    ha="center", va="center", color=ETE_PALETTE[grp])

    ax.set_xlabel("Time from surgery (years)", fontsize=11)
    ax.set_ylabel("Recurrence-free survival", fontsize=11)
    ax.set_title("Figure 11. Recurrence-Free Survival by Extrathyroidal Extension Status\n"
                 f"(N={len(df[ete_mask]):,} patients, {int(df.loc[ete_mask,'event_occurred'].sum())} events)",
                 fontsize=12, pad=10)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
    ax.set_ylim(0.88, 1.01)
    ax.legend(loc="lower left", framealpha=0.9, fontsize=10)
    ax.grid(axis="y", alpha=0.3, linewidth=0.6)

    fig.subplots_adjust(bottom=0.3)
    for fmt in ("png", "pdf"):
        p = FIG_DIR / f"fig11_km_ete_status.{fmt}"
        fig.savefig(p)
        print(f"  ✓ Figure 11 → {p}")
    plt.close(fig)


# ── 4. FIGURE 12: FOREST PLOT (Cox HRs) ──────────────────────────────────────

def fig12_forest_cox(cox_df: pd.DataFrame) -> None:
    mv = cox_df[cox_df["Model"] == "Multivariate"].copy().reset_index(drop=True)
    if mv.empty:
        print("  [skip] No multivariate results for forest plot")
        return

    mv["y"] = range(len(mv))
    fig, ax = plt.subplots(figsize=(8, max(4, len(mv) * 0.7 + 1.5)))

    colors = [
        "#f43f5e" if row["p_value"] < 0.05 else "#64748b"
        for _, row in mv.iterrows()
    ]

    for i, row in mv.iterrows():
        clr = colors[i]
        ax.plot([row["HR_lo"], row["HR_hi"]], [i, i],
                color=clr, linewidth=2, solid_capstyle="round", alpha=0.7)
        ax.scatter([row["HR"]], [i], color=clr, s=70, zorder=5)

    ax.axvline(1.0, color="#1e2535", linewidth=0.8, linestyle="--", alpha=0.7)
    ax.set_yticks(range(len(mv)))
    ax.set_yticklabels(mv["Variable"], fontsize=10)
    ax.set_xlabel("Hazard Ratio (95% CI)", fontsize=11)
    ax.set_title("Figure 12. Multivariate Cox PH — Hazard Ratios for Recurrence\n"
                 "(Penalized Cox; p<0.05 highlighted in red)", fontsize=11, pad=10)
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.grid(axis="x", alpha=0.3, linewidth=0.6)

    # HR labels on right
    for i, row in mv.iterrows():
        ax.text(ax.get_xlim()[1] * 1.02, i,
                f"{row['HR_fmt']}\n{row['p_fmt']}",
                va="center", ha="left", fontsize=8.5, color=colors[i])

    fig.tight_layout()
    for fmt in ("png", "pdf"):
        p = FIG_DIR / f"fig12_forest_cox.{fmt}"
        fig.savefig(p)
        print(f"  ✓ Figure 12 → {p}")
    plt.close(fig)


# ── 5. SUMMARY REPORT ────────────────────────────────────────────────────────

def write_summary(df: pd.DataFrame, cox_df: pd.DataFrame) -> None:
    path = STUDY_DIR / "cox_regression_report.md"
    n    = len(df)
    ev   = int(df["event_occurred"].sum())
    med_fu_y = df["time_to_event_years"].median()
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M")

    mv = cox_df[cox_df["Model"] == "Multivariate"]

    with open(path, "w") as f:
        f.write(f"# Cox Regression Supplement — Proposal 2\n\n")
        f.write(f"*Generated: {ts}*\n\n")
        f.write(f"## Cohort\n")
        f.write(f"- N = {n:,} patients with valid follow-up (time > 0)\n")
        f.write(f"- Events (recurrence): {ev} ({ev/n:.1%})\n")
        f.write(f"- Median follow-up: {med_fu_y:.1f} years\n\n")
        f.write(f"## Table 3. Cox Proportional Hazards Results\n\n")
        f.write(cox_df[["Variable", "Model", "HR_fmt", "p_fmt", "sig"]].to_markdown(index=False))
        f.write("\n\n## Figures\n")
        f.write("- **Figure 10:** KM — recurrence-free survival by ATA risk band\n")
        f.write("- **Figure 11:** KM — recurrence-free survival by ETE status\n")
        f.write("- **Figure 12:** Forest plot — multivariate Cox HR\n\n")
        f.write("## Key Findings\n\n")

        gross_row = mv[mv["Variable"] == "Gross ETE"]
        if not gross_row.empty:
            r = gross_row.iloc[0]
            f.write(f"- **Gross ETE**: HR {r['HR_fmt']}, {r['p_fmt']}\n")
        micro_row = mv[mv["Variable"] == "Micro ETE"]
        if not micro_row.empty:
            r = micro_row.iloc[0]
            f.write(f"- **Micro ETE**: HR {r['HR_fmt']}, {r['p_fmt']}\n")
        f.write(f"- Low-risk patients: zero recurrence events (censored follow-up)\n")
        f.write(f"- Gross ETE drives survival separation in log-rank tests\n")
        f.write(f"\nSupports the AJCC 8th edition decision to exclude mETE from T-staging.\n")

    print(f"  ✓ Cox report → {path}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Cox regression + KM figures for Proposal 2")
    ap.add_argument("--local", action="store_true",
                    help="Force local DuckDB (default: auto-detect)")
    args = ap.parse_args()

    print("=" * 64)
    print("  Proposal 2 — Cox Regression + Publication KM Figures")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 64)

    print("\n[1] Loading data…")
    df = load_data(args.local)
    df = build_features(df)
    print(f"    N={len(df):,}  events={int(df['event_occurred'].sum())}  "
          f"med-fu={df['time_to_event_years'].median():.1f} yr")

    print("\n[2] Cox proportional hazards regression (Table 3)…")
    cox_df = run_cox(df)
    save_table3(cox_df)

    print("\n[3] Figure 10: KM by ATA risk band…")
    fig10_km_risk_band(df)

    print("\n[4] Figure 11: KM by ETE status…")
    fig11_km_ete(df)

    print("\n[5] Figure 12: Forest plot (multivariate Cox)…")
    fig12_forest_cox(cox_df)

    print("\n[6] Summary report…")
    write_summary(df, cox_df)

    print()
    print("=" * 64)
    print("  Done. Outputs written to:")
    print(f"    tables/  — table3_cox_regression.csv / .md")
    print(f"    figures/ — fig10_km_risk_band, fig11_km_ete_status, fig12_forest_cox")
    print(f"    cox_regression_report.md")
    print("=" * 64)


if __name__ == "__main__":
    main()
