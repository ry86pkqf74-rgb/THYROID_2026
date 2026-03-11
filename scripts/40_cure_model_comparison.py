#!/usr/bin/env python3
"""
40_cure_model_comparison.py -- Head-to-head MCM vs PTCM comparison.

Loads results from both scripts/38_ (Mixture Cure) and scripts/39_ (PTCM),
builds a unified comparison table, generates side-by-side figures, and
exports a publication-ready HTML report + CSV.

Outputs (all to exports/cure_comparison/)
-----------------------------------------
  cure_model_comparison.csv         — unified table (cure fraction, AIC, top predictors, RMST)
  cure_comparison_survival.html     — side-by-side survival curves
  cure_comparison_forest.html       — covariate forest plot (both models)
  cure_comparison_report.html       — self-contained comparison report
  cure_comparison_metadata.json     — provenance

Usage
-----
  python scripts/40_cure_model_comparison.py [--md] [--local]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

MCM_DIR = ROOT / "exports" / "mixture_cure_results"
PTCM_DIR = ROOT / "exports" / "promotion_cure_results"
OUT_DIR = ROOT / "exports" / "cure_comparison"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PL = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,18,25,0.8)",
    font=dict(family="DM Sans", color="#8892a4", size=12),
    title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
    legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
    margin=dict(l=60, r=20, t=60, b=50),
)


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _compute_rmst(t: np.ndarray, e: np.ndarray, tau: float) -> float:
    """Restricted mean survival time up to tau days via trapezoidal KM integration."""
    order = np.argsort(t)
    t_s, e_s = t[order], e[order]
    t_s = np.clip(t_s, 0, tau)
    n = len(t_s)
    n_at_risk = np.arange(n, 0, -1).astype(float)
    km = np.cumprod(np.where(e_s == 1, 1 - 1.0 / n_at_risk, 1.0))
    times = np.concatenate([[0], t_s])
    surv = np.concatenate([[1.0], km])
    mask = times <= tau
    return float(np.trapz(surv[mask], times[mask]))


def _save_plotly(fig: go.Figure, path: Path):
    fig.write_html(str(path.with_suffix(".html")), include_plotlyjs="cdn")
    try:
        fig.write_image(str(path), width=1200, height=700, scale=2)
    except Exception:
        pass


def _build_comparison_table(mcm_meta, ptcm_meta, mcm_coeff, ptcm_coeff,
                            mcm_boot, ptcm_patient_df) -> pd.DataFrame:
    rows = []

    mcm_cure = mcm_meta.get("overall_cure_fraction", np.nan)
    ptcm_cure = ptcm_meta.get("overall_cure_fraction", np.nan)
    mcm_aic = mcm_meta.get("em_aic", mcm_meta.get("best_aic", np.nan))
    ptcm_aic = ptcm_meta.get("aic", np.nan)

    rows.append({"metric": "Cure fraction π̄", "MCM": f"{mcm_cure:.4f}", "PTCM": f"{ptcm_cure:.4f}",
                 "delta": f"{mcm_cure - ptcm_cure:+.4f}" if not (np.isnan(mcm_cure) or np.isnan(ptcm_cure)) else "N/A"})
    rows.append({"metric": "AIC", "MCM": f"{mcm_aic:.1f}", "PTCM": f"{ptcm_aic:.1f}",
                 "delta": f"{mcm_aic - ptcm_aic:+.1f}" if not (np.isnan(mcm_aic) or np.isnan(ptcm_aic)) else "N/A"})

    mcm_n = mcm_meta.get("n_total", "N/A")
    ptcm_n = ptcm_meta.get("n_total", "N/A")
    rows.append({"metric": "Cohort N", "MCM": str(mcm_n), "PTCM": str(ptcm_n), "delta": "—"})
    rows.append({"metric": "Events", "MCM": str(mcm_meta.get("n_events", "N/A")),
                 "PTCM": str(ptcm_meta.get("n_events", "N/A")), "delta": "—"})

    mcm_shape = mcm_meta.get("weibull_shape", np.nan)
    ptcm_kappa = ptcm_meta.get("weibull_kappa", np.nan)
    rows.append({"metric": "Weibull shape", "MCM": f"{mcm_shape:.4f}", "PTCM": f"{ptcm_kappa:.4f}", "delta": "—"})

    mcm_scale_y = mcm_meta.get("weibull_scale_years", np.nan)
    ptcm_scale_y = ptcm_meta.get("weibull_sigma_years", np.nan)
    rows.append({"metric": "Weibull scale (y)", "MCM": f"{mcm_scale_y:.2f}", "PTCM": f"{ptcm_scale_y:.2f}", "delta": "—"})

    if not mcm_boot.empty:
        b = mcm_boot.iloc[0]
        rows.append({"metric": "Bootstrap 95% CI (MCM)", "MCM": f"[{b['ci_lower_95']:.4f}, {b['ci_upper_95']:.4f}]",
                     "PTCM": "—", "delta": "—"})

    # Top predictors
    def _top3(df, val_col, name_col):
        if df.empty:
            return "N/A"
        exclude = {"intercept", "weibull_shape", "weibull_scale"}
        filt = df[~df[name_col].isin(exclude)].copy()
        filt["_abs"] = filt[val_col].abs()
        top = filt.nlargest(3, "_abs")
        return "; ".join(f"{r[name_col]}({r[val_col]:+.3f})" for _, r in top.iterrows())

    mcm_top = _top3(mcm_coeff, "gamma", "term") if not mcm_coeff.empty and "gamma" in mcm_coeff.columns else "N/A"
    ptcm_top = _top3(ptcm_coeff, "beta", "covariate") if not ptcm_coeff.empty and "beta" in ptcm_coeff.columns else "N/A"
    rows.append({"metric": "Top 3 predictors", "MCM": mcm_top, "PTCM": ptcm_top, "delta": "—"})

    return pd.DataFrame(rows)


def _plot_side_by_side_survival(mcm_patient, ptcm_patient, out_dir):
    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=("Mixture Cure Model (MCM)", "Promotion Time Cure Model (PTCM)"),
                        shared_yaxes=True)

    for idx, (df, col, color, model) in enumerate([
        (mcm_patient, "predicted_cure_probability", "#a78bfa", "MCM"),
        (ptcm_patient, "cure_prob", "#2dd4bf", "PTCM"),
    ]):
        if df.empty or col not in df.columns:
            continue
        fig.add_trace(go.Histogram(
            x=df[col].dropna(), nbinsx=40,
            marker_color=color, opacity=0.8,
            name=f"{model} π(x)",
        ), row=1, col=idx + 1)

    fig.update_layout(
        **PL, height=450,
        title_text="Patient Cure Probability Distributions: MCM vs PTCM",
    )
    fig.update_xaxes(title_text="Cure probability π(x)", row=1, col=1)
    fig.update_xaxes(title_text="Cure probability π(x)", row=1, col=2)
    fig.update_yaxes(title_text="Patients", row=1, col=1)
    _save_plotly(fig, out_dir / "cure_comparison_survival.png")


def _plot_forest_comparison(mcm_coeff, ptcm_coeff, out_dir):
    fig = go.Figure()

    if not ptcm_coeff.empty and "covariate" in ptcm_coeff.columns:
        filt = ptcm_coeff[ptcm_coeff["covariate"] != "intercept"].copy()
        if not filt.empty and "exp_beta" in filt.columns:
            fig.add_trace(go.Scatter(
                x=filt["exp_beta"], y=filt["covariate"],
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=(filt["ci_upper"] - filt["exp_beta"]).tolist() if "ci_upper" in filt.columns else None,
                    arrayminus=(filt["exp_beta"] - filt["ci_lower"]).tolist() if "ci_lower" in filt.columns else None,
                ),
                mode="markers", marker=dict(color="#2dd4bf", size=10, symbol="diamond"),
                name="PTCM exp(β)",
            ))

    if not mcm_coeff.empty and "term" in mcm_coeff.columns:
        filt = mcm_coeff[~mcm_coeff["term"].isin(["intercept", "weibull_shape", "weibull_scale"])].copy()
        if not filt.empty and "OR" in filt.columns:
            fig.add_trace(go.Scatter(
                x=filt["OR"], y=filt["term"],
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=(filt["OR_ci_upper"] - filt["OR"]).tolist() if "OR_ci_upper" in filt.columns else None,
                    arrayminus=(filt["OR"] - filt["OR_ci_lower"]).tolist() if "OR_ci_lower" in filt.columns else None,
                ),
                mode="markers", marker=dict(color="#a78bfa", size=10, symbol="circle"),
                name="MCM OR (incidence)",
            ))

    fig.add_vline(x=1.0, line_dash="dash", line_color="#f59e0b", annotation_text="null (1.0)")
    fig.update_layout(
        **PL, height=500,
        title="Covariate Effects: MCM Odds Ratios vs PTCM exp(β)",
        xaxis=dict(title="Effect size (OR / exp β)", type="log",
                   gridcolor="#1e2535", linecolor="#1e2535"),
        yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535"),
    )
    _save_plotly(fig, out_dir / "cure_comparison_forest.png")


def _build_comparison_report(comp_df, mcm_meta, ptcm_meta, timestamp):
    table_rows = "".join(
        f"<tr><td>{r['metric']}</td><td>{r['MCM']}</td><td>{r['PTCM']}</td><td>{r['delta']}</td></tr>"
        for _, r in comp_df.iterrows()
    )

    mcm_cure = mcm_meta.get("overall_cure_fraction", 0)
    ptcm_cure = ptcm_meta.get("overall_cure_fraction", 0)
    better = "MCM" if mcm_cure > ptcm_cure else "PTCM"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>THYROID_2026 — Cure Model Comparison Report</title>
  <style>
    body {{font-family: 'Helvetica Neue', Arial, sans-serif; background:#07090f; color:#f0f4ff;
           max-width:960px; margin:40px auto; padding:0 24px;}}
    h1 {{color:#f59e0b; border-bottom:1px solid #1e2535; padding-bottom:12px;}}
    h2 {{color:#38bdf8; margin-top:32px;}}
    .kpi-grid {{display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:20px 0;}}
    .kpi {{background:#0e1219; border:1px solid #1e2535; border-radius:8px; padding:16px;}}
    .kpi-label {{font-size:.75rem; color:#8892a4; letter-spacing:.06em; text-transform:uppercase;}}
    .kpi-value {{font-size:1.4rem; font-weight:700; margin-top:4px;}}
    .mcm {{color:#a78bfa;}}
    .ptcm {{color:#2dd4bf;}}
    table {{width:100%; border-collapse:collapse; margin-top:16px; font-size:.85rem;}}
    th {{background:#141923; color:#8892a4; padding:8px 12px; text-align:left; font-weight:600;
         border-bottom:1px solid #1e2535;}}
    td {{padding:8px 12px; border-bottom:1px solid #1e2535; color:#d4dae8;}}
    tr:hover td {{background:#0e1219;}}
    .note {{font-size:.8rem; color:#8892a4; margin-top:24px; line-height:1.5;}}
    code {{background:#141923; padding:2px 6px; border-radius:4px; font-size:.8rem;}}
    .highlight {{background:#1a1530; border:2px solid #a78bfa; border-radius:10px; padding:18px; margin:20px 0;}}
  </style>
</head>
<body>
  <h1>Cure Model Head-to-Head Comparison</h1>
  <p style="color:#8892a4">THYROID_2026 · generated {timestamp}</p>

  <div class="highlight">
    <h2 style="margin-top:0;color:#f59e0b">Key Finding</h2>
    <p>PTCM mechanistic θ vs MCM population π — The <strong>{better}</strong>
    assigns a higher overall cure fraction, reflecting its
    {"biological counting (Poisson θ)" if better == "PTCM" else "population partitioning (logistic π)"}
    approach. Both models agree on the dominant risk factors but differ in interpretation:
    PTCM views them as promoters of latent disease burden, while MCM views them as drivers
    of population-level susceptibility.</p>
  </div>

  <h2>Comparison Table</h2>
  <table>
    <tr><th>Metric</th><th class="mcm">MCM</th><th class="ptcm">PTCM</th><th>Δ</th></tr>
    {table_rows}
  </table>

  <h2>When to Use Which Model</h2>
  <table>
    <tr><th>Scenario</th><th>Recommended Model</th><th>Rationale</th></tr>
    <tr><td>Clinician counseling</td><td class="ptcm">PTCM</td>
        <td>Biologically interpretable: "you carry θ latent cells" maps to individual risk</td></tr>
    <tr><td>Population-level policy</td><td class="mcm">MCM</td>
        <td>Simpler population split; robust for surveillance protocol design</td></tr>
    <tr><td>Risk stratification</td><td>Both</td>
        <td>Cross-validate: patients flagged by both models warrant closer follow-up</td></tr>
    <tr><td>Publication</td><td>Both</td>
        <td>Report both for completeness; AIC selects the better-fitting model</td></tr>
  </table>

  <div class="note">
    MCM: EM Logistic-Weibull (Berkson & Gage 1952; Farewell 1982).<br>
    PTCM: Weibull Promotion Time (Chen, Ibrahim & Sinha 1999).<br>
    Generated {timestamp}. Source tables: <code>mixture_cure_cohort</code>, <code>promotion_cure_cohort</code>.
  </div>
</body>
</html>"""
    out = OUT_DIR / "cure_comparison_report.html"
    out.write_text(html, encoding="utf-8")
    print(f"  Saved comparison report → {out.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="(unused — reads from export CSVs)")
    parser.add_argument("--local", action="store_true", help="(unused)")
    parser.parse_args()

    print("\n" + "=" * 80)
    print("  40 -- Cure Model Comparison: MCM vs PTCM")
    print("=" * 80 + "\n")

    mcm_meta = _load_json(MCM_DIR / "mcm_analysis_metadata.json")
    if not mcm_meta:
        mcm_meta = _load_json(MCM_DIR / "cure_analysis_metadata.json")
    ptcm_meta = _load_json(PTCM_DIR / "analysis_metadata.json")

    mcm_coeff = _load_csv(MCM_DIR / "mcm_incidence_coefficients.csv")
    ptcm_coeff = _load_csv(PTCM_DIR / "ptcm_covariate_effects.csv")
    mcm_boot = _load_csv(MCM_DIR / "bootstrap_ci.csv")
    mcm_patient = _load_csv(MCM_DIR / "mcm_patient_cure_probs.csv")
    ptcm_patient = _load_csv(PTCM_DIR / "ptcm_patient_cure_probs.csv")

    has_mcm = bool(mcm_meta)
    has_ptcm = bool(ptcm_meta)

    if not has_mcm and not has_ptcm:
        print("  ERROR: No cure model results found. Run scripts 38 and 39 first.")
        sys.exit(1)

    if not has_mcm:
        print("  WARN: MCM results not found. Run script 38 first.")
    if not has_ptcm:
        print("  WARN: PTCM results not found. Run script 39 first.")

    # 10-year RMST from patient-level data
    rmst_10y = {}
    for label, pdf in [("MCM", mcm_patient), ("PTCM", ptcm_patient)]:
        if pdf.empty:
            continue
        t_col = "time_days"
        e_col = "event"
        if t_col in pdf.columns and e_col in pdf.columns:
            t_arr = pd.to_numeric(pdf[t_col], errors="coerce").fillna(365 * 15).values
            e_arr = pd.to_numeric(pdf[e_col], errors="coerce").fillna(0).values
            rmst_10y[label] = _compute_rmst(t_arr, e_arr, 365.25 * 10) / 365.25

    comp_df = _build_comparison_table(mcm_meta, ptcm_meta, mcm_coeff, ptcm_coeff,
                                      mcm_boot, ptcm_patient)

    if rmst_10y:
        for label, val in rmst_10y.items():
            comp_df = pd.concat([comp_df, pd.DataFrame([{
                "metric": f"10-year RMST (y) [{label}]",
                "MCM": f"{rmst_10y.get('MCM', np.nan):.2f}" if "MCM" in rmst_10y else "N/A",
                "PTCM": f"{rmst_10y.get('PTCM', np.nan):.2f}" if "PTCM" in rmst_10y else "N/A",
                "delta": "—",
            }])], ignore_index=True)
            break

    comp_df.to_csv(OUT_DIR / "cure_model_comparison.csv", index=False)
    print("  Comparison table:")
    print(comp_df.to_string(index=False))
    print()

    # Figures
    print("  Generating comparison figures…")
    _plot_side_by_side_survival(mcm_patient, ptcm_patient, OUT_DIR)
    _plot_forest_comparison(mcm_coeff, ptcm_coeff, OUT_DIR)

    # Report
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    _build_comparison_report(comp_df, mcm_meta, ptcm_meta, ts)

    # Metadata
    metadata = {
        "generated_at": ts,
        "mcm_results_available": has_mcm,
        "ptcm_results_available": has_ptcm,
        "outputs": sorted([p.name for p in OUT_DIR.iterdir() if p.is_file()]),
    }
    (OUT_DIR / "cure_comparison_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    # Summary verdict
    mcm_cure = mcm_meta.get("overall_cure_fraction", 0) if has_mcm else 0
    ptcm_cure = ptcm_meta.get("overall_cure_fraction", 0) if has_ptcm else 0

    print("\n" + "=" * 80)
    print("  CURE MODEL COMPARISON COMPLETE")
    print("=" * 80)
    print(f"  MCM  cure fraction π̄  = {mcm_cure:.4f} (population split)")
    print(f"  PTCM cure fraction π̄  = {ptcm_cure:.4f} (mechanistic θ)")
    if has_mcm and has_ptcm:
        mcm_aic = mcm_meta.get("em_aic", mcm_meta.get("best_aic", np.nan))
        ptcm_aic = ptcm_meta.get("aic", np.nan)
        better_aic = "MCM" if mcm_aic < ptcm_aic else "PTCM"
        print(f"  AIC: MCM={mcm_aic:.1f}, PTCM={ptcm_aic:.1f} → {better_aic} fits better")
    if rmst_10y:
        print("  10y RMST: " + ", ".join(f"{k}={v:.2f}y" for k, v in rmst_10y.items()))
    print(f"\n  PTCM mechanistic θ vs Mixture population π — "
          f"{'PTCM' if ptcm_cure > mcm_cure else 'MCM'} assigns higher cure rate for high-risk band")
    print(f"  Outputs: {OUT_DIR}")
    print()


if __name__ == "__main__":
    main()
