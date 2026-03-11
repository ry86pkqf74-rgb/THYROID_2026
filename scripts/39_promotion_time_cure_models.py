#!/usr/bin/env python3
"""
39_promotion_time_cure_models.py -- Promotion Time Cure Model (PTCM)

Mechanistic complement to the mixture cure model. Implements a Weibull
Promotion Time Cure Model (Chen, Ibrahim & Sinha, 1999) via maximum
likelihood estimation using only scipy + numpy (no extra packages).

Model specification
-------------------
  S(t|x) = exp(-θ(x) · F₀(t))

  where
    θ(x) = exp(xᵀβ)          — log-linear promotion intensity (Poisson mean)
    F₀(t) = 1 - exp(-(t/σ)ᵏ) — Weibull CDF (baseline promotion time)

  Cure probability: π(x) = P(cured | x) = lim_{t→∞} S(t|x) = exp(-θ(x))

Log-likelihood contributions
----------------------------
  Event    :  log(θ(x)) + log(f₀(t)) - θ(x)·F₀(t)
  Censored :  -θ(x)·F₀(t)

Parameters fitted by scipy.optimize.minimize (L-BFGS-B):
  [log(κ), log(σ), β₀, β₁, …, β_p]

Outputs (all to exports/promotion_cure_results/)
-------------------------------------------------
  ptcm_summary.csv            — overall model metrics
  ptcm_covariate_effects.csv  — β coefficients, SE, 95% CI, exp(β)
  ptcm_patient_cure_probs.csv — per-patient π(x), θ(x), cure tier
  ptcm_weibull_curve.png      — Weibull baseline + KM comparison
  ptcm_cure_by_stratum.png    — cure probability by AJCC stage and ETE
  ptcm_report.html            — biological interpretation report

Usage
-----
  python scripts/39_promotion_time_cure_models.py [--md] [--local] [--dry-run] [--boot 100]

Requires: numpy, scipy, pandas, plotly (all in requirements.txt)
Optional: kaleido (for PNG export — pip install kaleido)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import norm as scipy_norm

warnings.filterwarnings("ignore", category=RuntimeWarning)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

EXPORT_DIR = ROOT / "exports" / "promotion_cure_results"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── Constants ────────────────────────────────────────────────────────────────

COVARIATES = [
    "intercept",
    "age_z",           # age standardised
    "ajcc_2",          # stage II vs I (ref)
    "ajcc_3",          # stage III vs I
    "ajcc_4",          # stage IV vs I
    "ete_microscopic", # vs none (ref)
    "ete_gross",       # vs none (ref)
    "braf_pos",
    "tert_pos",
    "high_risk_band",  # recurrence_risk_band == 'high'
]

N_BOOT   = 100   # cold-start bootstrap; ~7s/boot × 100 = ~12 min; use --boot 300 for publication
SEED     = 42
MAX_TIME = 365 * 15   # 15-year administrative censor cap

# ── DuckDB connection helper ──────────────────────────────────────────────────

def _get_con(use_md: bool, use_local: bool):
    import duckdb
    if use_local:
        db_path = ROOT / "thyroid_master.duckdb"
        return duckdb.connect(str(db_path)), False
    if use_md:
        # Load token from env or secrets.toml
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
            print("  ERROR: MOTHERDUCK_TOKEN not set.  Use --local for local DuckDB.")
            sys.exit(1)
        con = duckdb.connect(f"md:?motherduck_token={token}")
        con.execute("USE thyroid_research_2026;")
        return con, True
    # Default: local DuckDB
    db_path = ROOT / "thyroid_master.duckdb"
    return duckdb.connect(str(db_path)), False


def _load_cohort(con, is_md: bool, dry_run: bool) -> pd.DataFrame:
    tbl = "promotion_cure_cohort"
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
    except Exception:
        print(f"  WARN: {tbl} not found. Run script 26 first.")
        return pd.DataFrame()

    limit = "LIMIT 500" if dry_run else ""
    df = con.execute(f"SELECT * FROM {tbl} {limit}").fetchdf()
    print(f"  Loaded {len(df):,} rows from {tbl}")
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

def _build_design_matrix(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (X, t, event) arrays for the PTCM MLE."""
    age_mean = df["age_at_diagnosis"].mean()
    age_std  = max(float(df["age_at_diagnosis"].std()), 1.0)

    def _ajcc(row, stage: int) -> int:
        try:
            s = str(row.get("ajcc_stage_8", "")).strip()
            return int(s.upper().replace("STAGE", "").strip() == str(stage) or
                       s == str(stage))
        except Exception:
            return 0

    X = np.column_stack([
        np.ones(len(df)),                                         # intercept
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
    return X, t, e


# ── Weibull PTCM likelihood ───────────────────────────────────────────────────

def _weibull_f0_F0(t: np.ndarray, log_kappa: float, log_sigma: float):
    """Return Weibull density f₀ and CDF F₀ at times t."""
    kappa = np.exp(log_kappa)
    sigma = np.exp(log_sigma)
    u = (t / sigma) ** kappa
    F0 = 1.0 - np.exp(-u)
    f0 = (kappa / sigma) * (t / sigma) ** (kappa - 1.0) * np.exp(-u)
    return f0, F0


def _neg_loglik(params: np.ndarray, X: np.ndarray, t: np.ndarray, e: np.ndarray) -> float:
    log_kappa, log_sigma = params[0], params[1]
    beta = params[2:]
    theta = np.exp(X @ beta)             # promotion intensity
    f0, F0 = _weibull_f0_F0(t, log_kappa, log_sigma)

    # Guard against numerical issues
    f0  = np.clip(f0,  1e-300, None)
    F0  = np.clip(F0,  0.0,    1.0 - 1e-12)
    theta = np.clip(theta, 1e-300, 1e6)

    ll_event    = np.log(theta) + np.log(f0) - theta * F0
    ll_censored = -theta * F0
    ll = np.where(e == 1, ll_event, ll_censored)
    return -np.sum(ll)


def _fit_ptcm(X: np.ndarray, t: np.ndarray, e: np.ndarray) -> tuple[np.ndarray, float]:
    """MLE fit via cold start. Returns (params, neg_loglik_at_minimum).

    Always cold-starts to avoid local-minima traps on the near-flat
    likelihood surface produced by low event rates (~0.3%).
    """
    n_cov = X.shape[1]
    p0 = np.zeros(2 + n_cov)
    p0[0] = np.log(1.2)   # log_kappa — slightly right-skewed baseline
    p0[1] = np.log(np.median(t[e == 1]) if e.sum() > 0 else np.median(t))

    res = minimize(
        _neg_loglik, p0, args=(X, t, e),
        method="L-BFGS-B",
        options={"maxiter": 2000, "ftol": 1e-10, "gtol": 1e-7},
    )
    return res.x, res.fun


def _bootstrap_ci(
    X: np.ndarray, t: np.ndarray, e: np.ndarray,
    params_mle: np.ndarray, n_boot: int, seed: int
) -> np.ndarray:
    """Return bootstrap SE for each parameter (shape: n_params).

    Cold-start is required; warm-start collapses all bootstrap samples
    to the same local minimum on low-event-rate data (SE → 0, invalid CIs).
    """
    rng   = np.random.default_rng(seed)
    n     = len(t)
    boots = []
    for i in range(n_boot):
        idx = rng.integers(0, n, size=n)
        try:
            p_b, _ = _fit_ptcm(X[idx], t[idx], e[idx])
            boots.append(p_b)
        except Exception:
            pass
        if (i + 1) % 10 == 0:
            print(f"    bootstrap {i + 1}/{n_boot}…", flush=True)
    if not boots:
        return np.full(len(params_mle), np.nan)
    return np.std(np.array(boots), axis=0)


# ── Export helpers ────────────────────────────────────────────────────────────

def _save_plotly_png(fig, path: Path) -> None:
    try:
        fig.write_image(str(path), width=1200, height=700, scale=2)
        print(f"  Saved PNG → {path.name}")
    except Exception as e:
        print(f"  WARN: PNG export failed ({e}). Install kaleido: pip install kaleido")
        # Fallback: save interactive HTML
        html_path = path.with_suffix(".html")
        fig.write_html(str(html_path))
        print(f"  Saved HTML fallback → {html_path.name}")


def _build_html_report(
    summary: dict,
    coeff_df: pd.DataFrame,
    export_dir: Path,
    timestamp: str,
) -> Path:
    rows = "".join(
        f"<tr><td>{r['covariate']}</td>"
        f"<td style='text-align:right'>{r['beta']:.4f}</td>"
        f"<td style='text-align:right'>{r['se']:.4f}</td>"
        f"<td style='text-align:right'>{r['exp_beta']:.3f}</td>"
        f"<td style='text-align:right'>{r['ci_lower']:.3f}</td>"
        f"<td style='text-align:right'>{r['ci_upper']:.3f}</td></tr>"
        for _, r in coeff_df.iterrows()
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>THYROID_2026 — Promotion Time Cure Model Report</title>
  <style>
    body {{font-family: 'Helvetica Neue', Arial, sans-serif; background:#07090f; color:#f0f4ff;
           max-width:900px; margin:40px auto; padding:0 24px;}}
    h1 {{color:#2dd4bf; border-bottom:1px solid #1e2535; padding-bottom:12px;}}
    h2 {{color:#38bdf8; margin-top:32px;}}
    .kpi-grid {{display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:20px 0;}}
    .kpi {{background:#0e1219; border:1px solid #1e2535; border-radius:8px; padding:16px;}}
    .kpi-label {{font-size:.75rem; color:#8892a4; letter-spacing:.06em; text-transform:uppercase;}}
    .kpi-value {{font-size:1.4rem; font-weight:700; color:#2dd4bf; margin-top:4px;}}
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
  <h1>Promotion Time Cure Model (PTCM)</h1>
  <p style="color:#8892a4">THYROID_2026 · generated {timestamp}</p>

  <h2>Biological Rationale</h2>
  <p>The Promotion Time Cure Model interprets thyroid cancer recurrence through a
  biological lens: a latent number of cancer cells capable of promoting recurrence
  follows a Poisson distribution with mean <strong>θ(x) = exp(xᵀβ)</strong>.
  If the Poisson count is zero the patient is operationally cured. The probability
  of cure is <strong>π(x) = exp(−θ(x))</strong>. The baseline promotion-time
  distribution is Weibull (shape κ = {summary.get("weibull_kappa", "?"):.3f},
  scale σ = {summary.get("weibull_sigma_years", "?"):.2f} y).</p>

  <h2>Model KPIs</h2>
  <div class="kpi-grid">
    <div class="kpi">
      <div class="kpi-label">N (cohort)</div>
      <div class="kpi-value">{summary.get("n_total", 0):,}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Observed events</div>
      <div class="kpi-value">{summary.get("n_events", 0):,} ({summary.get("event_rate_pct", 0):.1f}%)</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Overall cure fraction π̄</div>
      <div class="kpi-value">{summary.get("overall_cure_fraction", 0):.1%}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">10-year plateau rate</div>
      <div class="kpi-value">{summary.get("plateau_10y_rate", 0):.1%}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">AIC</div>
      <div class="kpi-value">{summary.get("aic", 0):.1f}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Weibull κ (shape)</div>
      <div class="kpi-value">{summary.get("weibull_kappa", 0):.3f}</div>
    </div>
  </div>

  <h2>Covariate Effects on Promotion Intensity θ(x)</h2>
  <p style="color:#8892a4">exp(β) &gt; 1 → higher promotion intensity → lower cure probability</p>
  <table>
    <tr>
      <th>Covariate</th><th>β</th><th>SE</th>
      <th>exp(β)</th><th>95% CI lower</th><th>95% CI upper</th>
    </tr>
    {rows}
  </table>

  <h2>Interpretation</h2>
  <p>Covariates with <code>exp(β) &gt; 1</code> increase promotion intensity, meaning
  those patients carry more latent disease burden and are less likely to be cured.
  Conversely, <code>exp(β) &lt; 1</code> is protective. The Weibull shape parameter
  κ = {summary.get("weibull_kappa", 1):.3f}
  {"suggests a <strong>decreasing hazard</strong> of promotion over time (early high-risk window)." if summary.get("weibull_kappa", 1) < 1 else
   "suggests an <strong>increasing hazard</strong> of promotion over time." if summary.get("weibull_kappa", 1) > 1 else
   "is consistent with a constant promotion hazard (exponential)."}
  </p>

  <div class="note">
    Model: Weibull Promotion Time Cure Model (Chen, Ibrahim &amp; Sinha 1999).
    Bootstrap SE from {summary.get("n_boot", 0)} resamples (seed=42).
    Source table: <code>promotion_cure_cohort</code>.
    Analysis date: {timestamp}.
  </div>
</body>
</html>"""
    out = export_dir / "ptcm_report.html"
    out.write_text(html, encoding="utf-8")
    print(f"  Saved HTML report → {out.name}")
    return out


# ── Plotting ──────────────────────────────────────────────────────────────────

def _plot_weibull_vs_km(
    t: np.ndarray, e: np.ndarray,
    params: np.ndarray,
    X: np.ndarray,
    export_dir: Path,
) -> None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        print("  WARN: plotly not available, skipping charts.")
        return

    log_kappa, log_sigma = params[0], params[1]
    beta = params[2:]
    theta_mean = np.exp(X @ beta).mean()

    t_grid = np.linspace(1, MAX_TIME, 500)
    _, F0_grid = _weibull_f0_F0(t_grid, log_kappa, log_sigma)
    S_ptcm = np.exp(-theta_mean * F0_grid)

    # KM estimate (basic, no lifelines needed)
    order = np.argsort(t)
    t_ord, e_ord = t[order], e[order]
    n_at_risk = np.arange(len(t), 0, -1)
    km_s  = np.cumprod(np.where(e_ord == 1, 1 - 1.0 / n_at_risk, 1.0))

    PL = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(14,18,25,0.8)",
        font=dict(family="DM Sans", color="#8892a4", size=12),
        title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
        xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", title="Years from surgery"),
        yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", title="Survival / cure probability",
                   range=[0, 1.05]),
        legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
        margin=dict(l=60, r=20, t=60, b=50),
    )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=t_ord / 365.25, y=km_s, mode="lines",
        name="Kaplan-Meier (empirical)",
        line=dict(color="#38bdf8", width=2, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=t_grid / 365.25, y=S_ptcm, mode="lines",
        name="PTCM — Weibull (mean θ)",
        line=dict(color="#2dd4bf", width=2.5),
    ))
    cure_frac = float(np.exp(-theta_mean))
    fig.add_hline(
        y=cure_frac, line_dash="dot", line_color="#f59e0b",
        annotation_text=f"Cure plateau π̄={cure_frac:.1%}",
        annotation_font_color="#f59e0b",
    )
    fig.update_layout(**PL, title="PTCM Weibull vs Kaplan-Meier: Recurrence-Free Survival")
    _save_plotly_png(fig, export_dir / "ptcm_weibull_curve.png")
    fig.write_html(str(export_dir / "ptcm_weibull_curve.html"))


def _plot_cure_by_stratum(
    df: pd.DataFrame, params: np.ndarray, X: np.ndarray, export_dir: Path
) -> None:
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    beta = params[2:]
    theta = np.exp(X @ beta)
    df = df.copy()
    df["cure_prob"] = np.exp(-theta)
    df["theta"]     = theta

    PL = dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,18,25,0.8)",
        font=dict(family="DM Sans", color="#8892a4", size=12),
        title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
        legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
        margin=dict(l=60, r=20, t=60, b=50),
    )
    COLORS = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399"]

    fig = go.Figure()
    for i, (strat, col, order) in enumerate([
        ("AJCC Stage",  "ajcc_stage_8",  None),
        ("ETE Type",    "ete_type",      ["none", "microscopic", "gross"]),
    ]):
        grp = df.groupby(col)["cure_prob"].mean().reset_index()
        if order:
            grp[col] = pd.Categorical(grp[col], categories=order, ordered=True)
            grp = grp.sort_values(col)
        fig.add_trace(go.Bar(
            x=grp[col].astype(str),
            y=grp["cure_prob"],
            name=strat,
            marker_color=COLORS[i],
            visible=(i == 0),
        ))

    fig.update_layout(
        **PL,
        title="PTCM: Mean Cure Probability π(x) by Stratum",
        yaxis=dict(title="Mean cure probability", range=[0, 1]),
        updatemenus=[dict(
            type="buttons", direction="left",
            x=0.01, y=1.12, showactive=True,
            buttons=[
                dict(label="AJCC Stage", method="update",
                     args=[{"visible": [True, False]},
                           {"title": "PTCM: Cure Probability by AJCC Stage"}]),
                dict(label="ETE Type", method="update",
                     args=[{"visible": [False, True]},
                           {"title": "PTCM: Cure Probability by ETE Type"}]),
            ],
        )],
    )
    _save_plotly_png(fig, export_dir / "ptcm_cure_by_stratum.png")
    fig.write_html(str(export_dir / "ptcm_cure_by_stratum.html"))


# ── Prediction API (reusable from other modules) ────────────────────────────

def predict_cure_probability(
    patient_features: dict,
    params: np.ndarray | None = None,
    age_mean: float = 50.0,
    age_std: float = 15.0,
) -> dict:
    """Predict cure probability for a single patient.

    Parameters
    ----------
    patient_features : dict
        Keys: age_at_diagnosis, ajcc_stage_8, ete_type, braf_status,
        tert_status, recurrence_risk_band.
    params : ndarray, optional
        Fitted PTCM params [log_kappa, log_sigma, beta_0, ..., beta_p].
        If None, loads from CSV exports.
    age_mean, age_std : float
        Z-scoring parameters from training cohort.

    Returns
    -------
    dict with cure_probability, theta, cure_tier, conditional_survival
    """
    if params is None:
        params = load_fitted_params()
        if params is None:
            return {"error": "No fitted PTCM params available"}

    log_kappa, log_sigma = params[0], params[1]
    beta = params[2:]

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
    theta = float(np.exp(x @ beta))
    cure = float(np.exp(-theta))

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
        t_d = np.array([yr * 365.25])
        _, F0 = _weibull_f0_F0(t_d, log_kappa, log_sigma)
        s = float(np.exp(-theta * F0)[0])
        cond.append({"year": yr, "survival": round(s, 4), "risk_pct": round((1 - s) * 100, 2)})

    return {
        "cure_probability": round(cure, 4),
        "theta": round(theta, 4),
        "cure_tier": tier,
        "conditional_survival": cond,
    }


def load_fitted_params() -> np.ndarray | None:
    """Load fitted PTCM parameters from CSV exports."""
    meta_path = EXPORT_DIR / "analysis_metadata.json"
    coeff_path = EXPORT_DIR / "ptcm_covariate_effects.csv"
    if not meta_path.exists() or not coeff_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        cdf = pd.read_csv(coeff_path)
        cov_order = {c: i for i, c in enumerate(COVARIATES)}
        cdf["_order"] = cdf["covariate"].map(cov_order)
        cdf = cdf.sort_values("_order").dropna(subset=["_order"])
        betas = cdf["beta"].values
        kappa = meta.get("weibull_kappa", 1.0)
        sigma_years = meta.get("weibull_sigma_years", 5.0)
        return np.concatenate([
            [np.log(max(kappa, 1e-6)), np.log(max(sigma_years * 365.25, 1.0))],
            betas,
        ])
    except Exception:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md",       action="store_true", help="Read from MotherDuck")
    parser.add_argument("--local",    action="store_true", help="Force local DuckDB")
    parser.add_argument("--dry-run",  action="store_true", help="Use 500-row sample only")
    parser.add_argument("--boot",     type=int, default=N_BOOT,
                        help=f"Bootstrap resamples (default {N_BOOT})")
    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("  39 -- Promotion Time Cure Model (PTCM)")
    print("=" * 80 + "\n")

    if args.dry_run:
        print("  [DRY RUN] Using 500-row sample; results are illustrative only.\n")

    # ── 1. Load data ──────────────────────────────────────────────────────
    con, is_md = _get_con(args.md, args.local)
    df = _load_cohort(con, is_md, args.dry_run)
    con.close()

    if df.empty:
        print("  ERROR: No data. Run `python scripts/26_motherduck_materialize_v2.py --md` first.")
        sys.exit(1)

    n_total  = len(df)
    n_events = int(df["event"].sum())
    event_rate = n_events / n_total

    print(f"  N = {n_total:,} | Events = {n_events:,} | Event rate = {event_rate:.2%}")
    print(f"  Median follow-up: {df['time_days'].median() / 365.25:.1f} y")
    print()

    # ── 2. Design matrix ──────────────────────────────────────────────────
    X, t, e = _build_design_matrix(df)
    print(f"  Design matrix: {X.shape[0]:,} × {X.shape[1]} (covariates: {COVARIATES})\n")

    # ── 3. MLE fit ────────────────────────────────────────────────────────
    print("  Fitting Weibull PTCM (L-BFGS-B)…")
    t0 = time.time()
    params, nll = _fit_ptcm(X, t, e)
    elapsed = time.time() - t0
    print(f"  Converged in {elapsed:.1f}s | −log L = {nll:.2f}")

    log_kappa, log_sigma = params[0], params[1]
    kappa = np.exp(log_kappa)
    sigma = np.exp(log_sigma) / 365.25   # convert days → years
    n_params = len(params)
    aic = 2 * nll + 2 * n_params

    beta = params[2:]
    theta_all = np.exp(X @ beta)
    cure_probs = np.exp(-theta_all)
    overall_cure = float(cure_probs.mean())
    plateau_10y  = float(((~df["event"].fillna(False)) & (df["time_days"] > 365 * 10)).mean())

    print(f"\n  Weibull shape κ = {kappa:.4f}, scale σ = {sigma:.2f} years")
    print(f"  Overall cure fraction π̄ = {overall_cure:.3f} ({overall_cure:.1%})")
    print(f"  10-year plateau rate      = {plateau_10y:.3f} ({plateau_10y:.1%})")
    print(f"  AIC = {aic:.1f}\n")

    # ── 4. Bootstrap SE ───────────────────────────────────────────────────
    n_boot = args.boot
    print(f"  Bootstrapping (n={n_boot}, seed={SEED})…")
    t0 = time.time()
    boot_se = _bootstrap_ci(X, t, e, params, n_boot, SEED)
    print(f"  Done in {time.time() - t0:.1f}s\n")

    # ── 5. Coefficient table ──────────────────────────────────────────────
    coeff_rows = []
    for i, cov in enumerate(COVARIATES):
        idx   = i + 2   # skip log_kappa, log_sigma
        b     = float(params[idx])
        se    = float(boot_se[idx]) if not np.isnan(boot_se[idx]) else np.nan
        z     = b / se if se > 0 else np.nan
        p     = float(2 * (1 - scipy_norm.cdf(abs(z)))) if not np.isnan(z) else np.nan
        ci_lo = b - 1.96 * se if not np.isnan(se) else np.nan
        ci_hi = b + 1.96 * se if not np.isnan(se) else np.nan
        coeff_rows.append({
            "covariate": cov, "beta": b, "se": se, "z": z, "p_value": p,
            "exp_beta": np.exp(b), "ci_lower": np.exp(ci_lo), "ci_upper": np.exp(ci_hi),
        })

    coeff_df = pd.DataFrame(coeff_rows)
    coeff_df = coeff_df.sort_values("exp_beta", ascending=False)

    print("  Top predictors of promotion intensity (sorted by exp(β) desc):")
    for _, row in coeff_df.iterrows():
        sig = "**" if (not np.isnan(row["p_value"]) and row["p_value"] < 0.05) else "  "
        print(f"  {sig} {row['covariate']:<22} β={row['beta']:+.3f}  "
              f"exp(β)={row['exp_beta']:.3f}  "
              f"95%CI=({row['ci_lower']:.3f}, {row['ci_upper']:.3f})"
              + (f"  p={row['p_value']:.3f}" if not np.isnan(row["p_value"]) else ""))
    print()

    # ── 6. Patient-level cure probabilities ───────────────────────────────
    df_out = df[["research_id", "age_at_diagnosis", "ajcc_stage_8",
                 "ete_type", "braf_status", "tert_status",
                 "recurrence_risk_band", "event", "time_days"]].copy()
    df_out["theta"]     = theta_all
    df_out["cure_prob"] = cure_probs
    df_out["cure_tier"] = pd.cut(
        cure_probs,
        bins=[0, 0.50, 0.70, 0.85, 1.001],
        labels=["low (<50%)", "moderate (50-70%)", "high (70-85%)", "very_high (>85%)"],
        right=False,
    )

    # ── 7. Summary dict ───────────────────────────────────────────────────
    summary = {
        "generated_at":        datetime.now().isoformat(),
        "n_total":             n_total,
        "n_events":            n_events,
        "event_rate_pct":      round(event_rate * 100, 2),
        "weibull_kappa":       round(float(kappa), 4),
        "weibull_sigma_years": round(float(sigma), 4),
        "overall_cure_fraction": round(overall_cure, 4),
        "plateau_10y_rate":    round(plateau_10y, 4),
        "aic":                 round(aic, 2),
        "neg_loglik":          round(float(nll), 4),
        "n_params":            n_params,
        "n_boot":              n_boot,
        "dry_run":             args.dry_run,
    }

    # ── 8. Save CSVs ─────────────────────────────────────────────────────
    summary_path = EXPORT_DIR / "ptcm_summary.csv"
    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    print(f"  Saved {summary_path.name}")

    coeff_path = EXPORT_DIR / "ptcm_covariate_effects.csv"
    coeff_df.to_csv(coeff_path, index=False)
    print(f"  Saved {coeff_path.name}")

    cure_path = EXPORT_DIR / "ptcm_patient_cure_probs.csv"
    df_out.to_csv(cure_path, index=False)
    print(f"  Saved {cure_path.name} ({len(df_out):,} rows)")

    meta_path = EXPORT_DIR / "analysis_metadata.json"
    meta_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"  Saved {meta_path.name}\n")

    # ── 9. Plots ──────────────────────────────────────────────────────────
    print("  Generating plots…")
    _plot_weibull_vs_km(t, e, params, X, EXPORT_DIR)
    _plot_cure_by_stratum(df, params, X, EXPORT_DIR)

    # ── 10. HTML report ───────────────────────────────────────────────────
    _build_html_report(summary, coeff_df, EXPORT_DIR, datetime.now().strftime("%Y-%m-%d %H:%M"))

    # ── 11. Print summary ─────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("  PTCM COMPLETE")
    print("=" * 80)
    print(f"  Cohort           : {n_total:,} patients, {n_events:,} events ({event_rate:.1%})")
    print(f"  Weibull baseline : κ={kappa:.4f}, σ={sigma:.2f} y")
    print(f"  Overall cure π̄   : {overall_cure:.3f} ({overall_cure:.1%})")
    print(f"  10-year plateau  : {plateau_10y:.3f} ({plateau_10y:.1%})")
    print(f"  AIC              : {aic:.1f}")
    print(f"  Outputs          : {EXPORT_DIR}")
    if not args.dry_run:
        top = coeff_df[coeff_df["covariate"] != "intercept"].head(3)
        print("  Top 3 promoters  :")
        for _, row in top.iterrows():
            print(f"    {row['covariate']}: exp(β)={row['exp_beta']:.3f}")
    print()


if __name__ == "__main__":
    main()
