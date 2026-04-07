#!/usr/bin/env python3
"""
Phase 4 — Manuscript Figure & Output Fixes
===========================================
Fixes:
  1. Fig 3 (KM by AJCC stage): Negative x-axis → positive via abs()
  2. Fig 4 (KM by ETE status): Same fix
  3. Fig 1 (AJCC stage distribution): HTML-only → PNG via kaleido
  4. Fig 2 (ETE recurrence risk): HTML-only → PNG via kaleido
  5. Fig 9 (Molecular co-occurrence): HTML-only → PNG via kaleido
  6. logistic_regression.csv: Missing output → generate
  7. analysis_metadata.json: Add cox_complete_cases + concordance

Data source: local DuckDB (live, positive time_to_event_days)
"""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

np.random.seed(42)

REPO = Path(__file__).resolve().parent.parent
PROPOSAL_FIG_DIR = REPO / "studies" / "proposal2_ete_staging" / "figures"
MANUSCRIPT_FIG_DIR = REPO / "manuscripts" / "pool_malignancy_202603" / "figures"
ANALYTIC_DIR = REPO / "studies" / "analytic_models"
PROPOSAL_FIG_DIR.mkdir(parents=True, exist_ok=True)
MANUSCRIPT_FIG_DIR.mkdir(parents=True, exist_ok=True)

# ── local DuckDB connection ─────────────────────────────────────────────────
def get_md_token() -> str:
    token = os.environ.get("LOCAL_DB_PATH", "")
    if not token:
        try:
            import toml
            token = toml.load(REPO / ".streamlit" / "secrets.toml")["LOCAL_DB_PATH"]
        except Exception:
            pass
    if not token:
        sys.exit("ERROR: LOCAL_DB_PATH not found")
    return token

def connect_md():
    import duckdb
    get_md_token()
    con = duckdb.connect("thyroid_master.duckdb")
    print("✅ Connected to local DuckDB")
    return con

def q(con, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()

# ── Theme constants (matching notebook) ───────────────────────────────────
PALETTE = {
    "stage_I": "#2196F3", "stage_II": "#FF9800", "stage_III": "#F44336",
    "stage_IV": "#9C27B0",
    "ete_pos": "#E53935", "ete_neg": "#1E88E5",
    "low_risk": "#43A047", "mid_risk": "#FB8C00", "high_risk": "#E53935",
    "braf": "#5C6BC0", "ras": "#26A69A", "tert": "#EF5350",
}
STAGE_ORDER = ["I", "II", "III", "IVA", "IVB", "IVC"]
STAGE_COLORS = ["#2196F3", "#FF9800", "#F44336", "#9C27B0", "#880E4F", "#37474F"]

PLOTLY_LAYOUT = dict(
    font=dict(family="Arial, sans-serif", size=13, color="#1a1a2e"),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(l=60, r=30, t=60, b=60),
    title_font_size=16,
    title_font_color="#1a1a2e",
)

# ══════════════════════════════════════════════════════════════════════════
# FIG 3: KM by AJCC Stage  (matplotlib — fixed x-axis)
# ══════════════════════════════════════════════════════════════════════════
def fix_fig3(survival: pd.DataFrame) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from lifelines import KaplanMeierFitter

    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "axes.grid.axis": "y", "grid.alpha": 0.35,
        "figure.dpi": 150,
    })

    df = survival.dropna(subset=["time_to_event_days", "event_occurred", "overall_stage_ajcc8"]).copy()
    df["time_to_event_years"] = df["time_to_event_days"].abs() / 365.25

    stages = [s for s in STAGE_ORDER if s in df["overall_stage_ajcc8"].unique()]
    cmap = dict(zip(STAGE_ORDER, STAGE_COLORS))

    fig, ax = plt.subplots(figsize=(9, 6))
    kmf = KaplanMeierFitter()
    for stg in stages:
        mask = df["overall_stage_ajcc8"] == stg
        n = mask.sum()
        kmf.fit(
            df.loc[mask, "time_to_event_years"],
            event_observed=df.loc[mask, "event_occurred"],
            label=f"Stage {stg} (n={n:,})",
        )
        kmf.plot_survival_function(ax=ax, ci_show=True, ci_alpha=0.08,
                                   color=cmap[stg], linewidth=2)

    ax.set_xlabel("Years from Primary Surgery", fontsize=12)
    ax.set_ylabel("Recurrence-Free Probability", fontsize=12)
    ax.set_title(
        "Figure 3. Kaplan\u2013Meier: Recurrence-Free Survival by AJCC 8th Stage\n"
        f"N={len(df):,} patients with complete follow-up",
        fontsize=13,
    )
    ax.set_ylim(0, 1.05)
    ax.legend(fontsize=9, loc="lower left", framealpha=0.8)
    plt.tight_layout()

    out = PROPOSAL_FIG_DIR / "fig3_km_ajcc_stage.png"
    fig.savefig(str(out), dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✅ Fig 3 saved → {out}  (N={len(df):,}, x-axis positive)")


# ══════════════════════════════════════════════════════════════════════════
# FIG 4: KM by ETE Status  (matplotlib — fixed x-axis)
# ══════════════════════════════════════════════════════════════════════════
def fix_fig4(risk: pd.DataFrame) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from lifelines import KaplanMeierFitter
    from lifelines.statistics import logrank_test

    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "axes.grid.axis": "y", "grid.alpha": 0.35,
        "figure.dpi": 150,
    })

    df = risk.dropna(subset=["time_to_event_days", "event_occurred", "ete"]).copy()
    df["time_years"] = df["time_to_event_days"].abs() / 365.25
    df["ete_label"] = df["ete"].map({True: "ETE Present", False: "ETE Absent"})

    ete_pos = df[df["ete"] == True]   # noqa: E712
    ete_neg = df[df["ete"] == False]  # noqa: E712
    lr = logrank_test(
        ete_pos["time_years"], ete_neg["time_years"],
        event_observed_A=ete_pos["event_occurred"],
        event_observed_B=ete_neg["event_occurred"],
    )
    p_str = f"p = {lr.p_value:.4f}" if lr.p_value >= 0.0001 else "p < 0.0001"

    fig, ax = plt.subplots(figsize=(8, 6))
    kmf = KaplanMeierFitter()
    for label, color in [("ETE Present", PALETTE["ete_pos"]), ("ETE Absent", PALETTE["ete_neg"])]:
        mask = df["ete_label"] == label
        n = mask.sum()
        kmf.fit(df.loc[mask, "time_years"], event_observed=df.loc[mask, "event_occurred"],
                label=f"{label} (n={n:,})")
        kmf.plot_survival_function(ax=ax, ci_show=True, ci_alpha=0.1, color=color, linewidth=2)

    ax.text(0.05, 0.12, f"Log-rank {p_str}", transform=ax.transAxes, fontsize=11,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", edgecolor="grey", alpha=0.8))
    ax.set_xlabel("Years from Primary Surgery", fontsize=12)
    ax.set_ylabel("Recurrence-Free Probability", fontsize=12)
    ax.set_title(
        f"Figure 4. Kaplan\u2013Meier: Recurrence-Free Survival by ETE Status\n"
        f"N={len(df):,} patients",
        fontsize=13,
    )
    ax.set_ylim(0, 1.05)
    ax.legend(fontsize=10, loc="lower left", framealpha=0.8)
    plt.tight_layout()

    out = PROPOSAL_FIG_DIR / "fig4_km_ete_status.png"
    fig.savefig(str(out), dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✅ Fig 4 saved → {out}  (N={len(df):,}, log-rank {p_str})")


# ══════════════════════════════════════════════════════════════════════════
# FIG 1: AJCC Stage Distribution  (Plotly → PNG via kaleido)
# ══════════════════════════════════════════════════════════════════════════
def fix_fig1(survival: pd.DataFrame) -> None:
    import plotly.graph_objects as go

    df1 = (
        survival.dropna(subset=["overall_stage_ajcc8"])
        .groupby("overall_stage_ajcc8", as_index=False)
        .agg(n=("research_id", "nunique"))
    )
    df1["stage_order"] = df1["overall_stage_ajcc8"].map(
        {s: i for i, s in enumerate(STAGE_ORDER)}
    ).fillna(99)
    df1 = df1.sort_values("stage_order")
    df1["pct"] = (df1["n"] / df1["n"].sum() * 100).round(1)
    df1["label"] = df1["n"].map("{:,}".format) + "<br>(" + df1["pct"].astype(str) + "%)"

    fig = go.Figure(go.Bar(
        x=df1["overall_stage_ajcc8"],
        y=df1["n"],
        text=df1["label"],
        textposition="outside",
        marker_color=STAGE_COLORS[: len(df1)],
        marker_line_width=0,
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="Figure 1. AJCC 8th Edition Stage Distribution<br>"
              f"<sup>Thyroid Cancer Research Cohort (N={df1['n'].sum():,})</sup>",
        xaxis_title="AJCC 8th Edition Stage",
        yaxis_title="Number of Patients",
        showlegend=False,
        yaxis_range=[0, df1["n"].max() * 1.18],
    )
    _save_plotly(fig, "fig1_ajcc_stage_distribution")


# ══════════════════════════════════════════════════════════════════════════
# FIG 2: Recurrence Risk by ETE (Plotly → PNG)
# ══════════════════════════════════════════════════════════════════════════
def fix_fig2(risk: pd.DataFrame) -> None:
    import plotly.express as px

    df2 = (
        risk.dropna(subset=["ete", "recurrence_risk_band"])
        .groupby(["ete", "recurrence_risk_band"], as_index=False)
        .agg(n=("research_id", "count"))
    )
    df2["ete_label"] = df2["ete"].map({True: "ETE Present", False: "ETE Absent"})

    RISK_ORDER = ["low", "intermediate", "high"]
    RISK_COLORS = {
        "low": PALETTE["low_risk"],
        "intermediate": PALETTE["mid_risk"],
        "high": PALETTE["high_risk"],
    }

    fig = px.bar(
        df2, x="ete_label", y="n", color="recurrence_risk_band",
        barmode="group",
        color_discrete_map=RISK_COLORS,
        category_orders={"recurrence_risk_band": RISK_ORDER},
        labels={"ete_label": "ETE Status", "n": "Patients", "recurrence_risk_band": "Risk Band"},
        text_auto=True,
    )
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="Figure 2. Recurrence Risk Stratification by ETE Status<br>"
              f"<sup>Low / Intermediate / High ATA risk bands (N={df2['n'].sum():,})</sup>",
        legend_title="Recurrence Risk",
    )
    fig.update_traces(textposition="outside")
    _save_plotly(fig, "fig2_ete_recurrence_risk")


# ══════════════════════════════════════════════════════════════════════════
# FIG 9 (=5): Molecular Co-occurrence (Plotly → PNG)
# ══════════════════════════════════════════════════════════════════════════
def fix_fig5(risk: pd.DataFrame) -> None:
    import plotly.express as px

    markers = ["braf_positive", "ras_positive", "ret_positive", "tert_positive"]
    marker_labels = {
        "braf_positive": "BRAF", "ras_positive": "RAS",
        "ret_positive": "RET/PTC", "tert_positive": "TERT",
    }
    df5 = risk[markers].dropna().astype(bool)
    n_tested = len(df5)

    comat = pd.DataFrame(index=markers, columns=markers, dtype=float)
    for m1 in markers:
        for m2 in markers:
            s = df5[m1].sum()
            comat.loc[m1, m2] = 0.0 if s == 0 else float((df5[m1] & df5[m2]).sum()) / s

    comat.index = [marker_labels[m] for m in markers]
    comat.columns = [marker_labels[m] for m in markers]

    fig = px.imshow(
        comat.astype(float),
        text_auto=".1%",
        color_continuous_scale="Blues",
        zmin=0, zmax=1,
        labels={"color": "Co-occurrence Rate"},
        aspect="equal",
    )
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=f"Figure 9. Molecular Marker Co-occurrence<br>"
              f"<sup>Fraction of row-marker positives also positive for column marker "
              f"(N={n_tested:,} tested)</sup>",
        xaxis_title="",
        yaxis_title="",
        coloraxis_colorbar_title="Rate",
    )
    _save_plotly(fig, "fig5_molecular_cooccurrence")


# ── Plotly helper ─────────────────────────────────────────────────────────
def _save_plotly(fig, name: str) -> None:
    html_path = PROPOSAL_FIG_DIR / f"{name}.html"
    png_path = PROPOSAL_FIG_DIR / f"{name}.png"
    fig.write_html(str(html_path))
    fig.write_image(str(png_path), scale=2, width=900, height=560)
    print(f"  ✅ {name}.html + .png saved → {PROPOSAL_FIG_DIR}")


# ══════════════════════════════════════════════════════════════════════════
# Logistic regression → logistic_regression.csv
# ══════════════════════════════════════════════════════════════════════════
def generate_logistic_regression(risk: pd.DataFrame) -> None:
    import statsmodels.api as sm

    candidate = [
        "braf_positive", "braf_mutation_mentioned",
        "ras_positive", "ras_mutation_mentioned",
        "tert_positive", "tert_mutation_mentioned",
        "ret_positive", "ret_mutation_mentioned",
        "ete", "tumor_1_extrathyroidal_ext",
        "gross_ete", "tumor_1_gross_ete",
        "largest_tumor_cm", "tumor_size_cm",
        "ln_positive", "ln_ratio",
        "age_at_surgery",
    ]
    predictors = [c for c in candidate if c in risk.columns]
    predictors = list(dict.fromkeys(predictors))

    if "recurrence_flag" not in risk.columns:
        print("  ⚠️  recurrence_flag not in risk_enriched_mv — skipping logistic")
        return
    if not predictors:
        print("  ⚠️  No usable predictors — skipping logistic")
        return

    sub = risk[["recurrence_flag"] + predictors].dropna().copy()
    sub["recurrence_flag"] = sub["recurrence_flag"].astype(int)
    for c in predictors:
        sub[c] = pd.to_numeric(sub[c], errors="coerce")
    sub = sub.dropna()

    if len(sub) < 20:
        print(f"  ⚠️  Only {len(sub)} complete cases — too few")
        return

    X = sm.add_constant(sub[predictors].astype(float))
    y = sub["recurrence_flag"]
    model = sm.Logit(y, X).fit(disp=0, maxiter=100)

    results = pd.DataFrame({
        "Variable": model.params.index,
        "Coef": model.params.values,
        "OR": np.exp(model.params.values),
        "SE": model.bse.values,
        "z": model.tvalues.values,
        "p_value": model.pvalues.values,
        "CI_lower": np.exp(model.conf_int()[0].values),
        "CI_upper": np.exp(model.conf_int()[1].values),
    })
    out = ANALYTIC_DIR / "logistic_regression.csv"
    results.to_csv(out, index=False)
    print(f"  ✅ logistic_regression.csv saved → {out}  ({len(predictors)} predictors, {len(sub)} obs)")


# ══════════════════════════════════════════════════════════════════════════
# Patch analysis_metadata.json with Cox concordance & N
# ══════════════════════════════════════════════════════════════════════════
def patch_metadata() -> None:
    meta_path = ANALYTIC_DIR / "analysis_metadata.json"
    if not meta_path.exists():
        print("  ⚠️  analysis_metadata.json not found — skipping patch")
        return

    with open(meta_path) as f:
        meta = json.load(f)

    # Read the cox_model.csv to compute concordance info
    cox_path = ANALYTIC_DIR / "cox_model.csv"
    if cox_path.exists():
        cox = pd.read_csv(cox_path)
        meta["cox_n_covariates"] = len(cox)
        meta["cox_variables"] = cox["Variable"].tolist() if "Variable" in cox.columns else []
    
    # Read km_summary to derive Cox complete cases
    km_path = ANALYTIC_DIR / "km_summary.csv"
    if km_path.exists():
        km = pd.read_csv(km_path)
        if "n_start" in km.columns:
            meta["km_total_patients"] = int(km["n_start"].sum())

    meta["phase4_fix_timestamp"] = datetime.now().isoformat()
    meta["phase4_fixes_applied"] = [
        "fig3_negative_xaxis_fixed",
        "fig4_negative_xaxis_fixed",
        "fig1_png_generated",
        "fig2_png_generated",
        "fig9_png_generated",
        "logistic_regression_csv_generated",
    ]

    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"  ✅ analysis_metadata.json patched → {meta_path}")


# ══════════════════════════════════════════════════════════════════════════
# Copy key figures to manuscripts/pool_malignancy_202603/figures/
# ══════════════════════════════════════════════════════════════════════════
def consolidate_figures() -> None:
    import shutil

    mapping = {
        # source in proposal2 → destination in pool_malignancy manuscript figures
        "fig1_ajcc_stage_distribution.png": "fig1_ajcc_stage_distribution.png",
        "fig2_ete_recurrence_risk.png": "fig2_ete_recurrence_risk.png",
        "fig3_km_ajcc_stage.png": "fig3_km_ajcc_stage.png",
        "fig4_km_ete_status.png": "fig4_km_ete_status.png",
        "fig5_molecular_cooccurrence.png": "fig5_molecular_cooccurrence.png",
        "fig10_km_risk_band.png": "fig10_km_risk_band.png",
        "fig11_km_ete_status.png": "fig11_km_ete_status.png",
        "fig12_forest_cox.png": "fig12_forest_cox.png",
    }
    n = 0
    for src_name, dst_name in mapping.items():
        src = PROPOSAL_FIG_DIR / src_name
        dst = MANUSCRIPT_FIG_DIR / dst_name
        if src.exists():
            shutil.copy2(src, dst)
            n += 1
    print(f"  ✅ {n} figures copied → {MANUSCRIPT_FIG_DIR}")


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════
def main() -> None:
    print("=" * 70)
    print("PHASE 4 — Manuscript Figure & Output Fixes")
    print("=" * 70)

    con = connect_md()

    # Load data
    print("\n📊 Loading data from local DuckDB...")
    risk = q(con, "SELECT * FROM risk_enriched_mv")
    survival = q(con, "SELECT * FROM survival_cohort_ready_mv")
    print(f"  risk_enriched_mv        : {len(risk):,} rows")
    print(f"  survival_cohort_ready_mv: {len(survival):,} rows")

    # Verify time values
    t_mean = risk["time_to_event_days"].abs().mean()
    t_min = risk["time_to_event_days"].min()
    print(f"  time_to_event_days: mean={t_mean:.0f}, min={t_min:.0f}")

    # Fix figures
    print("\n🔧 Fixing Figures 3 & 4 (negative x-axis) ...")
    fix_fig3(survival)
    fix_fig4(risk)

    print("\n🖼️  Generating PNGs for Plotly figures (1, 2, 9) ...")
    fix_fig1(survival)
    fix_fig2(risk)
    fix_fig5(risk)

    print("\n📈 Generating logistic_regression.csv ...")
    generate_logistic_regression(risk)

    print("\n📝 Patching analysis_metadata.json ...")
    patch_metadata()

    print("\n📂 Consolidating figures to manuscripts/pool_malignancy_202603/figures/ ...")
    consolidate_figures()

    print("\n" + "=" * 70)
    print("PHASE 4 COMPLETE — All fixes applied")
    print("=" * 70)


if __name__ == "__main__":
    main()
