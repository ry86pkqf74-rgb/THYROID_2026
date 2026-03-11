#!/usr/bin/env python3
"""
38_mixture_cure_models.py -- Mixture Cure Model (MCM) for recurrence endpoint.

Population-level counterpart to the PTCM (script 39_). Uses lifelines
MixtureCureFitter (Weibull latency + logistic incidence) plus a custom EM
algorithm that accommodates full covariates (AJCC8, ETE, mutations, risk band).

Model specification
-------------------
  S(t|x) = π(x) + (1 − π(x)) · S_u(t)

  where
    π(x) = logistic(xᵀγ)    — cure/incidence probability
    S_u(t) = Weibull survival — latency among susceptible (uncured) subjects

  π(x) gives the *population split* between cured and uncured; S_u(t) governs
  the time-to-event distribution among the uncured sub-population.

Outputs (all to exports/mixture_cure_results/)
----------------------------------------------
  mcm_summary.csv                    — overall model metrics
  mcm_incidence_coefficients.csv     — logistic regression γ coefficients (ORs)
  mcm_patient_cure_probs.csv         — per-patient π(x), susceptible prob
  mcm_weibull_curve.png / .html      — Weibull + KM overlay
  mcm_cure_by_stratum.png / .html    — cure probability by AJCC stage and ETE
  mcm_report.html                    — self-contained population report
  cure_fraction_summary.csv          — overall + bootstrap CI
  model_comparison.csv               — Weibull vs LogLogistic AIC comparison
  stratified_cure_fractions.csv      — per-subgroup cure fractions
  em_regression_coefficients.csv     — EM coefficients (incidence + latency)
  patient_cure_probabilities.csv     — patient-level cure predictions (EM)
  bootstrap_ci.csv                   — bootstrap confidence interval
  cure_analysis_metadata.json        — provenance metadata

Usage
-----
  python scripts/38_mixture_cure_models.py [--md] [--local] [--dry-run] [--boot 300]

Requires: numpy, scipy, pandas, plotly, lifelines (all in requirements.txt)
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from lifelines import LogLogisticFitter, MixtureCureFitter, WeibullFitter
from scipy.optimize import minimize
from scipy.stats import norm as scipy_norm

warnings.filterwarnings("ignore", category=RuntimeWarning)

ROOT = Path(__file__).resolve().parent.parent
LOCAL_DB = ROOT / "thyroid_master.duckdb"
OUT_DIR = ROOT / "exports" / "mixture_cure_results"
LEGACY_OUT_DIR = ROOT / "exports" / "cure_results"
DATABASE = "thyroid_research_2026"

sys.path.insert(0, str(ROOT))

log = logging.getLogger("mixture_cure")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

np.random.seed(42)

COVARIATES = [
    "intercept",
    "age_z",
    "ajcc_2",
    "ajcc_3",
    "ajcc_4",
    "ete_microscopic",
    "ete_gross",
    "braf_pos",
    "tert_pos",
    "high_risk_band",
]

MAX_TIME = 365 * 15
SEED = 42


def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def _get_con(use_md: bool, use_local: bool):
    import duckdb
    if use_local:
        return duckdb.connect(str(LOCAL_DB)), False
    if use_md:
        import os
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            try:
                import toml
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml")).get(
                    "MOTHERDUCK_TOKEN", ""
                )
            except Exception:
                pass
        if not token:
            print("  ERROR: MOTHERDUCK_TOKEN not set. Use --local for local DuckDB.")
            sys.exit(1)
        con = duckdb.connect(f"md:?motherduck_token={token}")
        con.execute("USE thyroid_research_2026;")
        return con, True
    return duckdb.connect(str(LOCAL_DB)), False


def _load_cohort(con, is_md: bool, dry_run: bool) -> pd.DataFrame:
    for tbl in ["mixture_cure_cohort", "promotion_cure_cohort", "cure_cohort"]:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
            limit = "LIMIT 500" if dry_run else ""
            df = con.execute(f"SELECT * FROM {tbl} {limit}").fetchdf()
            log.info("Loaded %s rows from %s", f"{len(df):,}", tbl)
            return df
        except Exception:
            continue
    log.error("No cure cohort table found. Run script 26 first.")
    return pd.DataFrame()


# ── Feature engineering ───────────────────────────────────────────────────

def _build_design_matrix(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    age_mean = df["age_at_diagnosis"].mean()
    age_std = max(float(df["age_at_diagnosis"].std()), 1.0)

    def _ajcc(row, stage: int) -> int:
        try:
            s = str(row.get("ajcc_stage_8", "")).strip()
            return int(s.upper().replace("STAGE", "").strip() == str(stage) or s == str(stage))
        except Exception:
            return 0

    X = np.column_stack([
        np.ones(len(df)),
        (df["age_at_diagnosis"].fillna(age_mean) - age_mean) / age_std,
        df.apply(lambda r: _ajcc(r, 2), axis=1).astype(float),
        df.apply(lambda r: _ajcc(r, 3), axis=1).astype(float),
        df.apply(lambda r: _ajcc(r, 4), axis=1).astype(float),
        (df["ete_type"].fillna("none").str.lower() == "microscopic").astype(float),
        (df["ete_type"].fillna("none").str.lower() == "gross").astype(float),
        df["braf_status"].fillna(False).astype(float),
        df["tert_status"].fillna(False).astype(float),
        (df["recurrence_risk_band"].fillna("").str.lower() == "high").astype(float),
    ])

    t = df["time_days"].fillna(MAX_TIME).clip(lower=1.0).values.astype(float)
    e = df["event"].fillna(False).astype(float).values
    return X, t, e, age_mean, age_std


# ── Weibull helpers ───────────────────────────────────────────────────────

def weibull_survival(t: np.ndarray, shape: float, scale: float) -> np.ndarray:
    t = np.clip(t, 1e-8, None)
    return np.exp(-((t / max(scale, 1e-8)) ** max(shape, 1e-8)))


def weibull_density(t: np.ndarray, shape: float, scale: float) -> np.ndarray:
    t = np.clip(t, 1e-8, None)
    shape = max(shape, 1e-8)
    scale = max(scale, 1e-8)
    return (shape / scale) * ((t / scale) ** (shape - 1.0)) * np.exp(-((t / scale) ** shape))


# ── Logistic regression for soft targets ──────────────────────────────────

def _fit_logistic_soft(
    x: np.ndarray, y_soft: np.ndarray, l2_penalty: float = 1e-2, max_iter: int = 400,
) -> tuple[np.ndarray, float]:
    n, p = x.shape
    x_aug = np.column_stack([np.ones(n), x])

    def objective(beta: np.ndarray) -> float:
        z = x_aug @ beta
        q = 1.0 / (1.0 + np.exp(-np.clip(z, -25, 25)))
        eps = 1e-9
        ll = np.sum(y_soft * np.log(q + eps) + (1.0 - y_soft) * np.log(1.0 - q + eps))
        reg = 0.5 * l2_penalty * np.sum(beta[1:] ** 2)
        return -(ll - reg)

    res = minimize(objective, x0=np.zeros(x_aug.shape[1]), method="L-BFGS-B",
                   options={"maxiter": max_iter})
    return res.x, float(res.fun)


def _fit_weighted_weibull(
    t: np.ndarray, event: np.ndarray, weights: np.ndarray, max_iter: int = 400,
) -> tuple[float, float, float]:
    t = np.clip(t.astype(float), 1e-8, None)
    event = event.astype(float)
    weights = np.clip(weights.astype(float), 0.0, 1.0)

    def objective(theta: np.ndarray) -> float:
        log_shape, log_scale = theta
        shape, scale = np.exp(log_shape), np.exp(log_scale)
        s = weibull_survival(t, shape, scale)
        f = np.clip(weibull_density(t, shape, scale), 1e-12, None)
        eps = 1e-12
        ll = np.sum(weights * (event * np.log(f + eps) + (1.0 - event) * np.log(s + eps)))
        return -ll

    init = np.array([np.log(1.2), np.log(max(np.median(t), 365.0))])
    res = minimize(objective, x0=init, method="L-BFGS-B", options={"maxiter": max_iter})
    return float(np.exp(res.x[0])), float(np.exp(res.x[1])), float(res.fun)


# ── EM Mixture Cure (logistic incidence + Weibull latency) ────────────────

@dataclass
class EMResult:
    intercept: float
    coefficients: np.ndarray
    feature_names: list[str]
    shape: float
    scale: float
    log_likelihood: float
    iterations: int
    converged: bool
    cure_probability: np.ndarray
    susceptible_probability: np.ndarray
    posterior_uncured: np.ndarray
    gamma_full: np.ndarray


def _build_em_design(df: pd.DataFrame, covariates: list[str]) -> tuple[np.ndarray, list[str]]:
    x_df = pd.get_dummies(df[covariates], drop_first=True, dummy_na=False)
    x_df = x_df.replace({True: 1.0, False: 0.0}).fillna(0.0).astype(float)
    return x_df.to_numpy(), x_df.columns.tolist()


def fit_em_mixture_cure(
    df: pd.DataFrame, covariates: list[str], max_iter: int = 120, tol: float = 1e-5,
) -> EMResult:
    t = df["time_days"].to_numpy(dtype=float)
    event = df["event"].astype(int).to_numpy(dtype=float)
    x, feature_names = _build_em_design(df, covariates)
    n = len(df)

    beta = np.zeros(x.shape[1] + 1)
    shape, scale = 1.2, max(np.median(t), 1.0)
    last_ll = -np.inf
    converged = False
    posterior_uncured = np.clip(event, 0.0, 1.0)
    it = 0

    for it in range(1, max_iter + 1):
        z = np.column_stack([np.ones(n), x]) @ beta
        q = 1.0 / (1.0 + np.exp(-np.clip(z, -25, 25)))
        cure_prob = 1.0 - q

        s_u = weibull_survival(t, shape, scale)
        posterior_uncured = np.where(
            event > 0.5, 1.0,
            (q * s_u) / np.clip(cure_prob + q * s_u, 1e-12, None),
        )

        beta, _ = _fit_logistic_soft(x, posterior_uncured)
        shape, scale, _ = _fit_weighted_weibull(t, event, posterior_uncured)

        z_new = np.column_stack([np.ones(n), x]) @ beta
        q_new = 1.0 / (1.0 + np.exp(-np.clip(z_new, -25, 25)))
        cure_new = 1.0 - q_new
        s_u_new = weibull_survival(t, shape, scale)
        f_u_new = np.clip(weibull_density(t, shape, scale), 1e-12, None)
        surv_mix = np.clip(cure_new + q_new * s_u_new, 1e-12, None)
        dens_mix = np.clip(q_new * f_u_new, 1e-12, None)
        ll = float(np.sum(event * np.log(dens_mix) + (1.0 - event) * np.log(surv_mix)))

        if abs(ll - last_ll) < tol:
            converged = True
            last_ll = ll
            break
        last_ll = ll

    z_final = np.column_stack([np.ones(n), x]) @ beta
    q_final = 1.0 / (1.0 + np.exp(-np.clip(z_final, -25, 25)))
    cure_final = 1.0 - q_final
    return EMResult(
        intercept=float(beta[0]),
        coefficients=beta[1:].copy(),
        feature_names=feature_names,
        shape=float(shape),
        scale=float(scale),
        log_likelihood=float(last_ll),
        iterations=it,
        converged=converged,
        cure_probability=cure_final,
        susceptible_probability=q_final,
        posterior_uncured=posterior_uncured,
        gamma_full=beta.copy(),
    )


# ── Incidence coefficients (logistic ORs) ────────────────────────────────

def _compute_incidence_table(em: EMResult, X_design: np.ndarray, n_boot: int, seed: int,
                             df: pd.DataFrame, covariates: list[str]) -> pd.DataFrame:
    names = ["intercept"] + em.feature_names
    gamma = em.gamma_full
    n_gamma = len(gamma)

    boot_gammas = []
    rng = np.random.default_rng(seed)
    n = len(df)
    for i in range(n_boot):
        idx = rng.integers(0, n, size=n)
        try:
            em_b = fit_em_mixture_cure(df.iloc[idx].reset_index(drop=True), covariates, max_iter=60)
            if len(em_b.gamma_full) == n_gamma:
                boot_gammas.append(em_b.gamma_full)
        except Exception:
            pass
        if (i + 1) % 50 == 0:
            print(f"    incidence bootstrap {i + 1}/{n_boot}…", flush=True)

    if boot_gammas:
        se = np.std(np.array(boot_gammas), axis=0)
    else:
        se = np.full(n_gamma, np.nan)

    rows = []
    for i, name in enumerate(names):
        g = float(gamma[i])
        s = float(se[i]) if not np.isnan(se[i]) else np.nan
        z = g / s if s > 0 else np.nan
        p = float(2 * (1 - scipy_norm.cdf(abs(z)))) if not np.isnan(z) else np.nan
        ci_lo = g - 1.96 * s if not np.isnan(s) else np.nan
        ci_hi = g + 1.96 * s if not np.isnan(s) else np.nan
        rows.append({
            "term": name,
            "gamma": g,
            "se": s,
            "z": z,
            "p_value": p,
            "OR": np.exp(g),
            "OR_ci_lower": np.exp(ci_lo) if not np.isnan(ci_lo) else np.nan,
            "OR_ci_upper": np.exp(ci_hi) if not np.isnan(ci_hi) else np.nan,
        })
    return pd.DataFrame(rows)


# ── Bootstrap cure fraction ───────────────────────────────────────────────

def _bootstrap_cure_fraction(df: pd.DataFrame, n_bootstrap: int) -> pd.DataFrame:
    vals: list[float] = []
    n = len(df)
    t = df["time_days"].to_numpy(dtype=float)
    e = df["event"].astype(bool).to_numpy()

    for i in range(n_bootstrap):
        idx = np.random.choice(n, n, replace=True)
        try:
            mcf = MixtureCureFitter(base_fitter=WeibullFitter())
            mcf.fit(t[idx], event_observed=e[idx], label=f"bootstrap_{i}")
            vals.append(float(mcf.cured_fraction_))
        except Exception:
            pass
        if (i + 1) % 50 == 0:
            print(f"    cure fraction bootstrap {i + 1}/{n_bootstrap}…", flush=True)

    arr = np.array(vals) if vals else np.array([np.nan])
    return pd.DataFrame({
        "metric": ["cure_fraction"],
        "mean": [float(np.nanmean(arr))],
        "std": [float(np.nanstd(arr, ddof=1))],
        "ci_lower_95": [float(np.nanquantile(arr, 0.025))],
        "ci_upper_95": [float(np.nanquantile(arr, 0.975))],
        "n_bootstrap": [int(n_bootstrap)],
    })


# ── Export helpers ────────────────────────────────────────────────────────

def _save_plotly(fig: go.Figure, path: Path) -> None:
    fig.write_html(str(path.with_suffix(".html")), include_plotlyjs="cdn")
    try:
        fig.write_image(str(path), width=1200, height=700, scale=2)
        log.info("Saved PNG → %s", path.name)
    except Exception as exc:
        log.warning("PNG export failed (%s). Install kaleido.", exc)


PL = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,18,25,0.8)",
    font=dict(family="DM Sans", color="#8892a4", size=12),
    title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
    xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", title="Years from surgery"),
    yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535"),
    legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
    margin=dict(l=60, r=20, t=60, b=50),
)

COLORS = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399"]


def _plot_mixture_vs_km(t, e, overall_cure, em, out_dir):
    order = np.argsort(t)
    t_ord, e_ord = t[order], e[order]
    n_at_risk = np.arange(len(t), 0, -1)
    km_s = np.cumprod(np.where(e_ord == 1, 1 - 1.0 / n_at_risk, 1.0))

    t_grid = np.linspace(1, MAX_TIME, 500)
    s_u = weibull_survival(t_grid, em.shape, em.scale)
    mean_cure = float(np.mean(em.cure_probability))
    s_mix = mean_cure + (1 - mean_cure) * s_u

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=t_ord / 365.25, y=km_s, mode="lines",
        name="Kaplan-Meier (empirical)",
        line=dict(color="#38bdf8", width=2, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=t_grid / 365.25, y=s_mix, mode="lines",
        name="MCM — Weibull mixture (mean π)",
        line=dict(color="#a78bfa", width=2.5),
    ))
    fig.add_hline(
        y=mean_cure, line_dash="dot", line_color="#f59e0b",
        annotation_text=f"Cure plateau π̄={mean_cure:.1%}",
        annotation_font_color="#f59e0b",
    )
    fig.update_layout(**PL, title="Mixture Cure Model vs Kaplan-Meier: Recurrence-Free Survival",
                      yaxis=dict(title="Survival / cure probability", range=[0, 1.05],
                                 gridcolor="#1e2535", linecolor="#1e2535"))
    _save_plotly(fig, out_dir / "mcm_weibull_curve.png")


def _plot_cure_by_stratum(df, em, out_dir):
    df = df.copy()
    df["cure_prob"] = em.cure_probability

    fig = go.Figure()
    for i, (strat, col, order) in enumerate([
        ("AJCC Stage", "ajcc_stage_8", None),
        ("ETE Type", "ete_type", ["none", "microscopic", "gross"]),
    ]):
        if col not in df.columns:
            continue
        grp = df.groupby(col)["cure_prob"].mean().reset_index()
        if order:
            grp[col] = pd.Categorical(grp[col], categories=order, ordered=True)
            grp = grp.sort_values(col)
        fig.add_trace(go.Bar(
            x=grp[col].astype(str), y=grp["cure_prob"],
            name=strat, marker_color=COLORS[i + 2], visible=(i == 0),
        ))
    fig.update_layout(
        **PL, title="MCM: Mean Cure Probability π(x) by Stratum",
        yaxis=dict(title="Mean cure probability", range=[0, 1],
                   gridcolor="#1e2535", linecolor="#1e2535"),
        updatemenus=[dict(
            type="buttons", direction="left", x=0.01, y=1.12, showactive=True,
            buttons=[
                dict(label="AJCC Stage", method="update",
                     args=[{"visible": [True, False]},
                           {"title": "MCM: Cure Probability by AJCC Stage"}]),
                dict(label="ETE Type", method="update",
                     args=[{"visible": [False, True]},
                           {"title": "MCM: Cure Probability by ETE Type"}]),
            ],
        )],
    )
    _save_plotly(fig, out_dir / "mcm_cure_by_stratum.png")


# ── HTML report ───────────────────────────────────────────────────────────

def _build_html_report(summary, incidence_df, export_dir, timestamp):
    rows_html = "".join(
        f"<tr><td>{r['term']}</td>"
        f"<td style='text-align:right'>{r['gamma']:.4f}</td>"
        f"<td style='text-align:right'>{r['se']:.4f}</td>"
        f"<td style='text-align:right'>{r['OR']:.3f}</td>"
        f"<td style='text-align:right'>{r.get('OR_ci_lower', float('nan')):.3f}</td>"
        f"<td style='text-align:right'>{r.get('OR_ci_upper', float('nan')):.3f}</td></tr>"
        for _, r in incidence_df.iterrows()
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>THYROID_2026 — Mixture Cure Model Report</title>
  <style>
    body {{font-family: 'Helvetica Neue', Arial, sans-serif; background:#07090f; color:#f0f4ff;
           max-width:900px; margin:40px auto; padding:0 24px;}}
    h1 {{color:#a78bfa; border-bottom:1px solid #1e2535; padding-bottom:12px;}}
    h2 {{color:#38bdf8; margin-top:32px;}}
    .kpi-grid {{display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:20px 0;}}
    .kpi {{background:#0e1219; border:1px solid #1e2535; border-radius:8px; padding:16px;}}
    .kpi-label {{font-size:.75rem; color:#8892a4; letter-spacing:.06em; text-transform:uppercase;}}
    .kpi-value {{font-size:1.4rem; font-weight:700; color:#a78bfa; margin-top:4px;}}
    table {{width:100%; border-collapse:collapse; margin-top:16px; font-size:.85rem;}}
    th {{background:#141923; color:#8892a4; padding:8px 12px; text-align:left;
         font-weight:600; border-bottom:1px solid #1e2535;}}
    td {{padding:8px 12px; border-bottom:1px solid #1e2535; color:#d4dae8;}}
    tr:hover td {{background:#0e1219;}}
    .note {{font-size:.8rem; color:#8892a4; margin-top:24px; line-height:1.5;}}
    code {{background:#141923; padding:2px 6px; border-radius:4px; font-size:.8rem;}}
  </style>
</head>
<body>
  <h1>Mixture Cure Model (MCM)</h1>
  <p style="color:#8892a4">THYROID_2026 · generated {timestamp}</p>

  <h2>Population Rationale</h2>
  <p>The Mixture Cure Model partitions the patient population into two latent groups:
  a <strong>cured</strong> fraction π(x) who will never experience recurrence, and a
  <strong>susceptible</strong> fraction 1−π(x) whose time to recurrence follows a Weibull
  distribution (shape={summary.get("weibull_shape", "?"):.3f},
  scale={summary.get("weibull_scale_years", "?"):.2f} y).
  The logistic incidence model identifies which covariates drive the probability of
  being in the susceptible (uncured) group.</p>

  <h2>Model KPIs</h2>
  <div class="kpi-grid">
    <div class="kpi"><div class="kpi-label">N (cohort)</div>
      <div class="kpi-value">{summary.get("n_total", 0):,}</div></div>
    <div class="kpi"><div class="kpi-label">Events</div>
      <div class="kpi-value">{summary.get("n_events", 0):,} ({summary.get("event_rate_pct", 0):.1f}%)</div></div>
    <div class="kpi"><div class="kpi-label">Overall cure fraction π̄</div>
      <div class="kpi-value">{summary.get("overall_cure_fraction", 0):.1%}</div></div>
    <div class="kpi"><div class="kpi-label">Univariate cure (Weibull)</div>
      <div class="kpi-value">{summary.get("univariate_cure_weibull", 0):.1%}</div></div>
    <div class="kpi"><div class="kpi-label">AIC (best)</div>
      <div class="kpi-value">{summary.get("best_aic", 0):.1f}</div></div>
    <div class="kpi"><div class="kpi-label">EM converged</div>
      <div class="kpi-value">{"Yes" if summary.get("em_converged") else "No"} ({summary.get("em_iterations", 0)} iter)</div></div>
  </div>

  <h2>Incidence Model — Logistic Odds Ratios</h2>
  <p style="color:#8892a4">OR &gt; 1 → higher odds of being in the uncured group</p>
  <table>
    <tr><th>Term</th><th>γ</th><th>SE</th><th>OR</th><th>95% CI lower</th><th>95% CI upper</th></tr>
    {rows_html}
  </table>

  <h2>Interpretation</h2>
  <p>Unlike the PTCM (which models the <em>number</em> of latent cancer cells), the MCM
  treats cure as a <strong>binary population split</strong>. Covariates with OR &gt; 1 increase
  the probability that a patient belongs to the susceptible (uncured) group.
  The Weibull latency component governs <em>when</em> events occur among the uncured.</p>

  <div class="note">
    Model: EM Logistic-Weibull Mixture Cure (Berkson & Gage 1952; Farewell 1982).
    Bootstrap SE from {summary.get("n_boot", 0)} resamples (seed=42).
    Source table: <code>mixture_cure_cohort</code>.
    Analysis date: {timestamp}.
  </div>
</body>
</html>"""
    out = export_dir / "mcm_report.html"
    out.write_text(html, encoding="utf-8")
    log.info("Saved HTML report → %s", out.name)
    return out


# ── Prediction API (reusable from dashboard) ─────────────────────────────

def predict_mixture_cure(
    patient_features: dict,
    gamma: np.ndarray | None = None,
    feature_names: list[str] | None = None,
    age_mean: float = 50.0,
    age_std: float = 15.0,
    weibull_shape: float = 1.0,
    weibull_scale: float = 3650.0,
) -> dict:
    if gamma is None:
        gamma, feature_names, weibull_shape, weibull_scale = _load_fitted_mcm()
        if gamma is None:
            return {"error": "No fitted MCM params available"}

    age = float(patient_features.get("age_at_diagnosis", 45))
    stage = str(patient_features.get("ajcc_stage_8", "I")).strip().upper()
    ete = str(patient_features.get("ete_type", "none")).strip().lower()
    braf = bool(patient_features.get("braf_status", False))
    tert = bool(patient_features.get("tert_status", False))
    risk = str(patient_features.get("recurrence_risk_band", "low")).strip().lower()

    x = np.array([
        1.0,
        (age - age_mean) / max(age_std, 1.0),
        float(stage in ("II", "2")),
        float(stage in ("III", "3")),
        float(stage in ("IV", "IVA", "IVB", "IVC", "4")),
        float(ete == "microscopic"),
        float(ete == "gross"),
        float(braf),
        float(tert),
        float(risk == "high"),
    ])

    n_gamma = len(gamma)
    n_x = len(x)
    if n_gamma > n_x:
        gamma = gamma[:n_x]
    elif n_gamma < n_x:
        x = x[:n_gamma]

    z = float(x @ gamma)
    susceptible = 1.0 / (1.0 + np.exp(-z))
    cure = 1.0 - susceptible

    if cure > 0.85:
        tier = "very_high"
    elif cure > 0.70:
        tier = "high"
    elif cure > 0.50:
        tier = "moderate"
    else:
        tier = "low"

    cond = []
    for yr in [1, 3, 5, 10, 15]:
        t_d = yr * 365.25
        s_u = float(weibull_survival(np.array([t_d]), weibull_shape, weibull_scale)[0])
        s_total = cure + susceptible * s_u
        cond.append({"year": yr, "survival": round(s_total, 4)})

    return {
        "cure_probability": round(cure, 4),
        "susceptible_probability": round(susceptible, 4),
        "cure_tier": tier,
        "conditional_survival": cond,
    }


def _load_fitted_mcm():
    meta_path = OUT_DIR / "mcm_analysis_metadata.json"
    coeff_path = OUT_DIR / "mcm_incidence_coefficients.csv"
    if not meta_path.exists() or not coeff_path.exists():
        return None, None, None, None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        cdf = pd.read_csv(coeff_path)
        gamma = cdf["gamma"].values
        names = cdf["term"].tolist()
        shape = meta.get("weibull_shape", 1.0)
        scale = meta.get("weibull_scale_days", 3650.0)
        return gamma, names, shape, scale
    except Exception:
        return None, None, None, None


# ── Main ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LEGACY_OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 80)
    print("  38 -- Mixture Cure Model (MCM)")
    print("=" * 80 + "\n")

    if args.dry_run:
        print("  [DRY RUN] Using 500-row sample; results are illustrative only.\n")

    con, is_md = _get_con(args.md, args.local)
    df = _load_cohort(con, is_md, args.dry_run)
    con.close()

    if df.empty:
        print("  ERROR: No data. Run `python scripts/26_motherduck_materialize_v2.py --md` first.")
        return 1

    df["time_days"] = pd.to_numeric(df["time_days"], errors="coerce").fillna(MAX_TIME).clip(lower=1)
    df["event"] = df["event"].astype(bool)
    df = df.dropna(subset=["time_days"])

    n_total = len(df)
    n_events = int(df["event"].sum())
    event_rate = n_events / n_total
    t = df["time_days"].to_numpy(dtype=float)
    e = df["event"].to_numpy(dtype=bool)

    print(f"  N = {n_total:,} | Events = {n_events:,} | Event rate = {event_rate:.2%}")
    print(f"  Median follow-up: {df['time_days'].median() / 365.25:.1f} y\n")

    # 1) Overall univariate cure model
    print("  Fitting univariate Weibull MixtureCureFitter…")
    overall = MixtureCureFitter(base_fitter=WeibullFitter())
    overall.fit(t, event_observed=e, label="overall")
    overall_cure = float(overall.cured_fraction_)
    print(f"  Univariate cure fraction: {overall_cure:.4f}\n")

    # 2) Model comparison (Weibull vs LogLogistic)
    model_rows = []
    for label, base in [("weibull", WeibullFitter()), ("loglogistic", LogLogisticFitter())]:
        m = MixtureCureFitter(base_fitter=base)
        m.fit(t, event_observed=e, label=label)
        k = len(m.params_)
        aic = (2 * k) - (2 * float(m.log_likelihood_))
        model_rows.append({
            "model": label, "log_likelihood": float(m.log_likelihood_),
            "n_params": int(k), "aic": float(aic),
            "cured_fraction": float(m.cured_fraction_),
        })
    model_cmp = pd.DataFrame(model_rows).sort_values("aic")
    best_aic = float(model_cmp.iloc[0]["aic"])
    print("  Model comparison:")
    print(model_cmp.to_string(index=False))
    print()

    # 3) Stratified cure fractions
    strat_cols = ["braf_status", "ete_type", "ajcc_stage_8", "recurrence_risk_band"]
    strat_rows: list[dict] = []
    for col in strat_cols:
        if col not in df.columns:
            continue
        for val, sub in df.groupby(col, dropna=False):
            if len(sub) < 30:
                continue
            t_sub = sub["time_days"].to_numpy(dtype=float)
            e_sub = sub["event"].to_numpy(dtype=bool)
            if np.sum(e_sub) < 3:
                continue
            try:
                fit = MixtureCureFitter(base_fitter=WeibullFitter())
                fit.fit(t_sub, event_observed=e_sub, label=f"{col}={val}")
                strat_rows.append({
                    "group_variable": col, "group_value": str(val),
                    "n": int(len(sub)), "events": int(np.sum(e_sub)),
                    "observed_event_rate": float(np.mean(e_sub)),
                    "cure_fraction": float(fit.cured_fraction_),
                })
            except Exception:
                pass
    strat_df = pd.DataFrame(strat_rows)
    if not strat_df.empty:
        print("  Stratified cure fractions:")
        print(strat_df.to_string(index=False))
        print()

    # 4) EM regression (incidence + latency) with full covariates
    covariates = ["ajcc_stage_8", "ete_type", "braf_status", "tert_status", "recurrence_risk_band"]
    for c in covariates:
        if c not in df.columns:
            df[c] = np.nan
    print("  Fitting EM logistic-Weibull cure model…")
    em = fit_em_mixture_cure(df, covariates=covariates)
    print(f"  EM converged: {em.converged} in {em.iterations} iterations")
    print(f"  EM mean cure probability: {np.mean(em.cure_probability):.4f}")
    print(f"  Weibull shape={em.shape:.4f}, scale={em.scale / 365.25:.2f} y\n")

    # 5) Incidence coefficient table with bootstrap CI
    n_boot = min(args.boot, 100)
    print(f"  Computing incidence coefficients (bootstrap n={n_boot})…")
    X_design, _ = _build_em_design(df, covariates)
    incidence_df = _compute_incidence_table(em, X_design, n_boot, SEED, df, covariates)
    print("\n  Incidence ORs (sorted by OR desc):")
    _inc_sorted = incidence_df.sort_values("OR", ascending=False)
    for _, row in _inc_sorted.iterrows():
        sig = "**" if (not np.isnan(row.get("p_value", np.nan)) and row["p_value"] < 0.05) else "  "
        print(f"  {sig} {row['term']:<30} γ={row['gamma']:+.3f}  "
              f"OR={row['OR']:.3f}  "
              f"95%CI=({row.get('OR_ci_lower', np.nan):.3f}, {row.get('OR_ci_upper', np.nan):.3f})")
    print()

    # 6) Bootstrap cure fraction CI
    boot_n = min(args.boot, 300)
    print(f"  Bootstrap cure fraction CI (n={boot_n})…")
    boot_df = _bootstrap_cure_fraction(df, boot_n)
    print(f"  Cure fraction: {boot_df.iloc[0]['mean']:.4f} "
          f"[{boot_df.iloc[0]['ci_lower_95']:.4f}, {boot_df.iloc[0]['ci_upper_95']:.4f}]\n")

    # 7) Patient-level predictions
    pred_df = pd.DataFrame({
        "research_id": df["research_id"].astype("int64", errors="ignore"),
        "time_days": df["time_days"],
        "event": df["event"].astype(int),
        "predicted_cure_probability": em.cure_probability,
        "predicted_susceptible_probability": em.susceptible_probability,
        "posterior_uncured_probability": em.posterior_uncured,
    })
    pred_df["cure_tier"] = pd.cut(
        em.cure_probability,
        bins=[0, 0.50, 0.70, 0.85, 1.001],
        labels=["low (<50%)", "moderate (50-70%)", "high (70-85%)", "very_high (>85%)"],
        right=False,
    )

    # 8) EM regression coefficients (legacy format)
    coef_df = pd.DataFrame({
        "term": ["intercept"] + em.feature_names + ["weibull_shape", "weibull_scale"],
        "estimate": [em.intercept] + em.coefficients.tolist() + [em.shape, em.scale],
    })

    # 9) Summary
    overall_em_cure = float(np.mean(em.cure_probability))
    n_params_em = len(em.gamma_full) + 2
    aic_em = -2 * em.log_likelihood + 2 * n_params_em

    summary = {
        "generated_at": datetime.now().isoformat(),
        "n_total": n_total,
        "n_events": n_events,
        "event_rate_pct": round(event_rate * 100, 2),
        "univariate_cure_weibull": round(overall_cure, 4),
        "overall_cure_fraction": round(overall_em_cure, 4),
        "weibull_shape": round(em.shape, 4),
        "weibull_scale_days": round(em.scale, 2),
        "weibull_scale_years": round(em.scale / 365.25, 4),
        "em_log_likelihood": round(em.log_likelihood, 4),
        "em_converged": em.converged,
        "em_iterations": em.iterations,
        "best_aic": round(best_aic, 2),
        "em_aic": round(aic_em, 2),
        "n_params_em": n_params_em,
        "n_boot": args.boot,
        "dry_run": args.dry_run,
        "git_sha": get_git_sha(),
    }

    # 10) Save all outputs
    for out in [OUT_DIR, LEGACY_OUT_DIR]:
        pd.DataFrame([summary]).to_csv(out / "mcm_summary.csv", index=False)
        incidence_df.to_csv(out / "mcm_incidence_coefficients.csv", index=False)
        pred_df.to_csv(out / "mcm_patient_cure_probs.csv", index=False)
        coef_df.to_csv(out / "em_regression_coefficients.csv", index=False)
        boot_df.to_csv(out / "bootstrap_ci.csv", index=False)
        model_cmp.to_csv(out / "model_comparison.csv", index=False)
        strat_df.to_csv(out / "stratified_cure_fractions.csv", index=False)

    # Legacy-compatible exports
    summary_rows = [
        {"metric": "overall_cure_fraction_univariate_weibull", "value": overall_cure},
        {"metric": "overall_observed_event_rate", "value": float(df["event"].mean())},
        {"metric": "overall_crude_cure_rate", "value": float(1.0 - df["event"].mean())},
        {"metric": "em_mean_predicted_cure_probability", "value": overall_em_cure},
        {"metric": "em_log_likelihood", "value": float(em.log_likelihood)},
        {"metric": "em_converged", "value": bool(em.converged)},
        {"metric": "em_iterations", "value": int(em.iterations)},
    ]
    for out in [OUT_DIR, LEGACY_OUT_DIR]:
        pd.DataFrame(summary_rows).to_csv(out / "cure_fraction_summary.csv", index=False)
        pred_df.to_csv(out / "patient_cure_probabilities.csv", index=False)

    log.info("Saved CSVs to %s and %s", OUT_DIR.name, LEGACY_OUT_DIR.name)

    # 11) Metadata JSON
    metadata = {
        **summary,
        "database": "local" if args.local else DATABASE,
        "input_table": "mixture_cure_cohort",
        "outputs": sorted([p.name for p in OUT_DIR.iterdir() if p.is_file()]),
    }
    for out in [OUT_DIR, LEGACY_OUT_DIR]:
        (out / "mcm_analysis_metadata.json").write_text(
            json.dumps(metadata, indent=2, default=str), encoding="utf-8"
        )
        (out / "cure_analysis_metadata.json").write_text(
            json.dumps(metadata, indent=2, default=str), encoding="utf-8"
        )

    # 12) Plots
    print("  Generating plots…")
    _plot_mixture_vs_km(t.astype(float), e.astype(float), overall_cure, em, OUT_DIR)
    _plot_cure_by_stratum(df, em, OUT_DIR)

    if not strat_df.empty:
        fig_forest = px.scatter(
            strat_df.sort_values("cure_fraction", ascending=False),
            x="cure_fraction", y="group_value", color="group_variable", size="n",
            title="Stratified Cure Fraction Summary",
        )
        _save_plotly(fig_forest, OUT_DIR / "stratified_cure_fraction_forest.png")

    surv_df = overall.survival_function_.reset_index()
    xcol, ycol = surv_df.columns[0], surv_df.columns[1]
    fig_surv = px.line(surv_df, x=xcol, y=ycol, title="Overall Mixture Cure Survival Curve")
    fig_surv.update_layout(xaxis_title="Days", yaxis_title="Survival probability")
    _save_plotly(fig_surv, OUT_DIR / "overall_mixture_cure_survival.png")

    # 13) HTML report
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    _build_html_report(summary, incidence_df, OUT_DIR, ts)

    # 14) PTCM comparison summary
    ptcm_summary_path = ROOT / "exports" / "promotion_cure_results" / "ptcm_summary.csv"
    if ptcm_summary_path.exists():
        ptcm = pd.read_csv(ptcm_summary_path).iloc[0]
        print("\n  ── MCM vs PTCM Comparison ──")
        print(f"  MCM  cure fraction π̄ = {overall_em_cure:.4f} (population split)")
        print(f"  PTCM cure fraction π̄ = {float(ptcm.get('overall_cure_fraction', 0)):.4f} (mechanistic θ)")
        print(f"  MCM  AIC = {aic_em:.1f}")
        print(f"  PTCM AIC = {float(ptcm.get('aic', 0)):.1f}")
    else:
        print("\n  PTCM results not found — run script 39 for comparison.")

    # 15) Final summary
    print("\n" + "=" * 80)
    print("  MIXTURE CURE MODEL COMPLETE")
    print("=" * 80)
    print(f"  Cohort              : {n_total:,} patients, {n_events:,} events ({event_rate:.1%})")
    print(f"  Univariate cure     : {overall_cure:.4f} (Weibull MCF)")
    print(f"  EM cure fraction π̄  : {overall_em_cure:.4f}")
    print(f"  Weibull latency     : shape={em.shape:.4f}, scale={em.scale / 365.25:.2f} y")
    print(f"  Best AIC            : {best_aic:.1f}")
    print(f"  EM AIC              : {aic_em:.1f}")
    print(f"  Outputs             : {OUT_DIR}")
    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true", help="Read from MotherDuck")
    ap.add_argument("--local", action="store_true", help="Force local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Use 500-row sample only")
    ap.add_argument("--boot", type=int, default=300,
                    help="Bootstrap resamples (default 300)")
    args = ap.parse_args()
    try:
        rc = run(args)
        raise SystemExit(rc)
    except Exception as exc:
        log.exception("Mixture cure script failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
