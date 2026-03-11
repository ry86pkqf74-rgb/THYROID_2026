#!/usr/bin/env python3
"""
38_mixture_cure_models.py -- Mixture cure modeling for recurrence endpoint.

Builds publication-ready cure-model outputs from `cure_cohort`:
  1) Univariate MixtureCureFitter
  2) Stratified MixtureCureFitter summaries
  3) Custom EM logistic-Weibull regression (incidence + latency)
  4) Bootstrap CI for overall cure fraction
  5) Exported tables, figures, and metadata in exports/cure_results/
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from lifelines import LogLogisticFitter, MixtureCureFitter, WeibullFitter
from scipy.optimize import minimize

ROOT = Path(__file__).resolve().parent.parent
LOCAL_DB = ROOT / "thyroid_master.duckdb"
OUT_DIR = ROOT / "exports" / "cure_results"
DATABASE = "thyroid_research_2026"

sys.path.insert(0, str(ROOT))
from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

log = logging.getLogger("mixture_cure")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

np.random.seed(42)


def qual(table: str, catalog: str | None) -> str:
    """Fully-qualify table name when a catalog is available."""
    return f"{catalog}.{table}" if catalog else table


def cached_sqdf(con, sql: str, key: str = "query") -> pd.DataFrame:
    """Script-level SQL helper with timing and consistent logging."""
    t0 = time.perf_counter()
    df = con.execute(sql).fetchdf()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    log.info("%s loaded in %.0f ms (%s rows)", key, elapsed_ms, len(df))
    return df


def get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def build_design_matrix(df: pd.DataFrame, cols: list[str]) -> tuple[np.ndarray, list[str]]:
    """One-hot encode selected covariates and return matrix + feature names."""
    x_df = pd.get_dummies(df[cols], drop_first=True, dummy_na=False)
    x_df = x_df.replace({True: 1.0, False: 0.0}).fillna(0.0).astype(float)
    x = x_df.to_numpy()
    feature_names = x_df.columns.tolist()
    return x, feature_names


def weibull_survival(t: np.ndarray, shape: float, scale: float) -> np.ndarray:
    t = np.clip(t, 1e-8, None)
    shape = max(shape, 1e-8)
    scale = max(scale, 1e-8)
    return np.exp(-((t / scale) ** shape))


def weibull_density(t: np.ndarray, shape: float, scale: float) -> np.ndarray:
    t = np.clip(t, 1e-8, None)
    shape = max(shape, 1e-8)
    scale = max(scale, 1e-8)
    return (shape / scale) * ((t / scale) ** (shape - 1.0)) * np.exp(-((t / scale) ** shape))


def fit_logistic_soft(
    x: np.ndarray,
    y_soft: np.ndarray,
    l2_penalty: float = 1e-2,
    max_iter: int = 400,
) -> tuple[np.ndarray, float]:
    """Fit logistic regression for soft targets via direct optimization."""

    n, p = x.shape
    x_aug = np.column_stack([np.ones(n), x])

    def objective(beta: np.ndarray) -> float:
        z = x_aug @ beta
        q = 1.0 / (1.0 + np.exp(-np.clip(z, -25, 25)))
        eps = 1e-9
        ll = np.sum(y_soft * np.log(q + eps) + (1.0 - y_soft) * np.log(1.0 - q + eps))
        reg = 0.5 * l2_penalty * np.sum(beta[1:] ** 2)
        return -(ll - reg)

    res = minimize(
        objective,
        x0=np.zeros(x_aug.shape[1]),
        method="L-BFGS-B",
        options={"maxiter": max_iter},
    )
    beta = res.x
    nll = float(res.fun)
    return beta, nll


def fit_weighted_weibull(
    t: np.ndarray,
    event: np.ndarray,
    weights: np.ndarray,
    max_iter: int = 400,
) -> tuple[float, float, float]:
    """Weighted Weibull MLE for latent uncured subgroup."""

    t = np.clip(t.astype(float), 1e-8, None)
    event = event.astype(float)
    weights = np.clip(weights.astype(float), 0.0, 1.0)

    def objective(theta: np.ndarray) -> float:
        log_shape, log_scale = theta
        shape = np.exp(log_shape)
        scale = np.exp(log_scale)
        s = weibull_survival(t, shape, scale)
        f = np.clip(weibull_density(t, shape, scale), 1e-12, None)
        eps = 1e-12
        ll = np.sum(weights * (event * np.log(f + eps) + (1.0 - event) * np.log(s + eps)))
        return -ll

    init = np.array([np.log(1.2), np.log(np.median(t) if np.median(t) > 0 else 365.0)])
    res = minimize(objective, x0=init, method="L-BFGS-B", options={"maxiter": max_iter})
    shape = float(np.exp(res.x[0]))
    scale = float(np.exp(res.x[1]))
    nll = float(res.fun)
    return shape, scale, nll


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


def fit_em_logistic_weibull(
    df: pd.DataFrame,
    covariates: list[str],
    max_iter: int = 120,
    tol: float = 1e-5,
) -> EMResult:
    """EM wrapper for logistic incidence + Weibull latency cure model."""

    t = df["time_days"].to_numpy(dtype=float)
    event = df["event"].astype(int).to_numpy(dtype=float)
    x, feature_names = build_design_matrix(df, covariates)
    n = len(df)

    beta = np.zeros(x.shape[1] + 1)
    shape = 1.2
    scale = max(np.median(t), 1.0)
    last_ll = -np.inf
    converged = False
    posterior_uncured = np.clip(event, 0.0, 1.0)

    for it in range(1, max_iter + 1):
        z = np.column_stack([np.ones(n), x]) @ beta
        q = 1.0 / (1.0 + np.exp(-np.clip(z, -25, 25)))  # susceptible probability
        cure_prob = 1.0 - q

        s_u = weibull_survival(t, shape, scale)
        posterior_uncured = np.where(
            event > 0.5,
            1.0,
            (q * s_u) / np.clip(cure_prob + q * s_u, 1e-12, None),
        )

        beta, _ = fit_logistic_soft(x, posterior_uncured)
        shape, scale, _ = fit_weighted_weibull(t, event, posterior_uncured)

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
    )


def bootstrap_cure_fraction(
    df: pd.DataFrame,
    n_bootstrap: int,
    base_fitter=None,
) -> pd.DataFrame:
    """Bootstrap overall cure fraction from univariate mixture cure model."""

    if base_fitter is None:
        base_fitter = WeibullFitter()

    vals: list[float] = []
    n = len(df)
    t = df["time_days"].to_numpy(dtype=float)
    e = df["event"].astype(bool).to_numpy()

    for i in range(n_bootstrap):
        idx = np.random.choice(n, n, replace=True)
        mcf = MixtureCureFitter(base_fitter=base_fitter.__class__())
        mcf.fit(t[idx], event_observed=e[idx], label=f"bootstrap_{i}")
        vals.append(float(mcf.cured_fraction_))

    arr = np.array(vals)
    return pd.DataFrame(
        {
            "metric": ["cure_fraction"],
            "mean": [float(arr.mean())],
            "std": [float(arr.std(ddof=1))],
            "ci_lower_95": [float(np.quantile(arr, 0.025))],
            "ci_upper_95": [float(np.quantile(arr, 0.975))],
            "n_bootstrap": [int(n_bootstrap)],
        }
    )


def save_plotly_figure(fig: go.Figure, out_png: Path, out_html: Path) -> None:
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    try:
        fig.write_image(str(out_png), width=1200, height=700, scale=2)
    except Exception as exc:
        log.warning("Could not export PNG %s: %s", out_png.name, exc)


def connect(args: argparse.Namespace):
    if args.local:
        return None, __import__("duckdb").connect(str(LOCAL_DB))
    cfg = MotherDuckConfig(database=DATABASE)
    con = MotherDuckClient(cfg).connect_rw()
    try:
        con.execute(f"USE {DATABASE};")
    except Exception:
        pass
    return DATABASE, con


def run(args: argparse.Namespace) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    catalog, con = connect(args)
    table_name = qual("cure_cohort", catalog)
    df = cached_sqdf(con, f"SELECT * FROM {table_name}", key="cure_cohort")
    if df.empty:
        raise RuntimeError("cure_cohort is empty or unavailable. Run script 26 first.")

    df["time_days"] = pd.to_numeric(df["time_days"], errors="coerce").fillna(365 * 15).clip(lower=1)
    df["event"] = df["event"].astype(bool)
    df = df.dropna(subset=["time_days"])

    t = df["time_days"].to_numpy(dtype=float)
    e = df["event"].to_numpy(dtype=bool)

    # 1) Overall univariate cure model
    overall = MixtureCureFitter(base_fitter=WeibullFitter())
    overall.fit(t, event_observed=e, label="overall")
    overall_cure = float(overall.cured_fraction_)

    # 2) Model comparison
    model_rows = []
    for label, base in [("weibull", WeibullFitter()), ("loglogistic", LogLogisticFitter())]:
        m = MixtureCureFitter(base_fitter=base)
        m.fit(t, event_observed=e, label=label)
        k = len(m.params_)
        aic = (2 * k) - (2 * float(m.log_likelihood_))
        model_rows.append(
            {
                "model": label,
                "log_likelihood": float(m.log_likelihood_),
                "n_params": int(k),
                "aic": float(aic),
                "cured_fraction": float(m.cured_fraction_),
            }
        )
    model_cmp = pd.DataFrame(model_rows).sort_values("aic", ascending=True)

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
            fit = MixtureCureFitter(base_fitter=WeibullFitter())
            fit.fit(t_sub, event_observed=e_sub, label=f"{col}={val}")
            strat_rows.append(
                {
                    "group_variable": col,
                    "group_value": str(val),
                    "n": int(len(sub)),
                    "events": int(np.sum(e_sub)),
                    "observed_event_rate": float(np.mean(e_sub)),
                    "cure_fraction": float(fit.cured_fraction_),
                    "log_likelihood": float(fit.log_likelihood_),
                }
            )
    strat_df = pd.DataFrame(strat_rows)

    # 4) EM regression (incidence + latency)
    covariates = ["ajcc_stage_8", "ete_type", "braf_status", "tert_status", "ln_positive"]
    for c in covariates:
        if c not in df.columns:
            df[c] = np.nan
    em = fit_em_logistic_weibull(df, covariates=covariates)

    coef_df = pd.DataFrame(
        {
            "term": ["intercept"] + em.feature_names + ["weibull_shape", "weibull_scale"],
            "estimate": [em.intercept] + em.coefficients.tolist() + [em.shape, em.scale],
        }
    )

    pred_df = pd.DataFrame(
        {
            "research_id": df["research_id"].astype("int64", errors="ignore"),
            "time_days": df["time_days"],
            "event": df["event"].astype(int),
            "predicted_cure_probability": em.cure_probability,
            "predicted_susceptible_probability": em.susceptible_probability,
            "posterior_uncured_probability": em.posterior_uncured,
        }
    )

    # 5) Bootstrap CI
    boot_df = bootstrap_cure_fraction(df, n_bootstrap=args.bootstrap_n, base_fitter=WeibullFitter())

    # 6) Summary table
    summary_rows = [
        {
            "metric": "overall_cure_fraction_univariate_weibull",
            "value": overall_cure,
        },
        {
            "metric": "overall_observed_event_rate",
            "value": float(df["event"].mean()),
        },
        {
            "metric": "overall_crude_cure_rate",
            "value": float(1.0 - df["event"].mean()),
        },
        {
            "metric": "em_mean_predicted_cure_probability",
            "value": float(np.mean(em.cure_probability)),
        },
        {
            "metric": "em_log_likelihood",
            "value": float(em.log_likelihood),
        },
        {
            "metric": "em_converged",
            "value": bool(em.converged),
        },
        {
            "metric": "em_iterations",
            "value": int(em.iterations),
        },
    ]
    summary_df = pd.DataFrame(summary_rows)

    # 7) Figures
    surv_df = overall.survival_function_.reset_index()
    xcol = surv_df.columns[0]
    ycol = surv_df.columns[1]
    fig_surv = px.line(surv_df, x=xcol, y=ycol, title="Overall Mixture Cure Survival Curve")
    fig_surv.update_layout(xaxis_title="Days", yaxis_title="Survival probability")
    save_plotly_figure(
        fig_surv,
        OUT_DIR / "overall_mixture_cure_survival.png",
        OUT_DIR / "overall_mixture_cure_survival.html",
    )

    if not strat_df.empty:
        fig_forest = px.scatter(
            strat_df.sort_values("cure_fraction", ascending=False),
            x="cure_fraction",
            y="group_value",
            color="group_variable",
            size="n",
            title="Stratified Cure Fraction Summary",
            labels={"group_value": "Group", "cure_fraction": "Estimated cure fraction"},
        )
        save_plotly_figure(
            fig_forest,
            OUT_DIR / "stratified_cure_fraction_forest.png",
            OUT_DIR / "stratified_cure_fraction_forest.html",
        )

    # 8) Exports
    summary_df.to_csv(OUT_DIR / "cure_fraction_summary.csv", index=False)
    pred_df.to_csv(OUT_DIR / "patient_cure_probabilities.csv", index=False)
    coef_df.to_csv(OUT_DIR / "em_regression_coefficients.csv", index=False)
    boot_df.to_csv(OUT_DIR / "bootstrap_ci.csv", index=False)
    model_cmp.to_csv(OUT_DIR / "model_comparison.csv", index=False)
    strat_df.to_csv(OUT_DIR / "stratified_cure_fractions.csv", index=False)

    html_report = f"""
<html><head><title>Cure Analysis Report</title></head><body>
<h1>Mixture Cure Analysis</h1>
<p>Rows: {len(df):,}</p>
<p>Observed event rate: {df["event"].mean():.4f}</p>
<p>Overall cure fraction (univariate Weibull): {overall_cure:.4f}</p>
<p>EM mean predicted cure probability: {np.mean(em.cure_probability):.4f}</p>
<p>EM converged: {em.converged} in {em.iterations} iterations</p>
</body></html>
"""
    (OUT_DIR / "cure_analysis_report.html").write_text(html_report, encoding="utf-8")

    metadata = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "git_sha": get_git_sha(),
        "database": "local" if args.local else DATABASE,
        "input_table": table_name,
        "n_rows": int(len(df)),
        "n_events": int(df["event"].sum()),
        "bootstrap_n": int(args.bootstrap_n),
        "em_converged": bool(em.converged),
        "outputs": sorted([p.name for p in OUT_DIR.iterdir() if p.is_file()]),
    }
    with open(OUT_DIR / "cure_analysis_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print("\n=== MIXTURE CURE MODEL RESULTS ===")
    print(f"Rows: {len(df):,}")
    print(f"Observed event rate: {df['event'].mean():.4f}")
    print(f"Overall cure fraction (Weibull): {overall_cure:.4f}")
    print(f"EM mean predicted cure probability: {np.mean(em.cure_probability):.4f}")
    print("Top model by AIC:")
    print(model_cmp.head(1).to_string(index=False))
    print(f"\nOutputs written to: {OUT_DIR}")

    if not args.dry_run:
        print("\nMIXTURE CURE MODEL MODULE COMPLETE — run python scripts/38_mixture_cure_models.py then push")

    con.close()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--local", action="store_true", help="Use local DuckDB file")
    ap.add_argument("--dry-run", action="store_true", help="Run analysis without completion banner")
    ap.add_argument(
        "--bootstrap-n",
        type=int,
        default=500,
        help="Bootstrap iterations for cure-fraction CI (default: 500)",
    )
    args = ap.parse_args()
    try:
        rc = run(args)
        raise SystemExit(rc)
    except Exception as exc:
        log.exception("Mixture cure script failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
