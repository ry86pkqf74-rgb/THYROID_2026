#!/usr/bin/env python3
"""
38_advanced_survival_analysis.py — Publication-grade advanced survival models

Reads from `survival_cohort_enriched` (built by script 26) and runs:
  1. Kaplan-Meier stratified by ETE / BRAF / stage / histology
  2. Multivariable Cox PH with Schoenfeld proportional-hazards test
  3. Restricted Mean Survival Time (RMST) at 5 and 10 years
  4. Cumulative Incidence Functions (Aalen-Johansen)
  5. Propensity-score matched survival (ETE gross vs none)
  6. Random Survival Forest + SHAP importances
  7. (Optional) DeepSurv neural survival model

Outputs publication-ready figures (PNG) and tables (CSV) to
  /exports/survival_results/

Dependencies (pip install):
  lifelines scikit-survival shap matplotlib plotly kaleido

Usage:
  .venv/bin/python scripts/38_advanced_survival_analysis.py          # local DB
  .venv/bin/python scripts/38_advanced_survival_analysis.py --md     # MotherDuck
  .venv/bin/python scripts/38_advanced_survival_analysis.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "exports" / "survival_results"
SEED = 42
np.random.seed(SEED)


# ── Connection ───────────────────────────────────────────────────────────

def _get_connection(args):
    if args.local or os.getenv("USE_LOCAL_DUCKDB", "").lower() in ("1", "true", "yes"):
        import duckdb
        local_path = os.getenv("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        print(f"  Connecting to local DuckDB: {local_path}")
        return duckdb.connect(local_path)
    from motherduck_client import MotherDuckClient, MotherDuckConfig
    cfg = MotherDuckConfig(database="thyroid_research_2026")
    print("  Connecting to MotherDuck …")
    return MotherDuckClient(cfg).connect_rw()


def _load_cohort(con) -> pd.DataFrame:
    """Load survival_cohort_enriched, falling back to on-the-fly build."""
    for tbl in ("survival_cohort_enriched", "md_survival_cohort_enriched"):
        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            if not df.empty:
                print(f"  Loaded {tbl}: {len(df):,} rows, {df.columns.tolist()[:8]}…")
                return df
        except Exception:
            continue
    print("  WARN: survival_cohort_enriched not found — run script 26 first")
    sys.exit(1)


def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


# ── Phase 1: Kaplan-Meier ────────────────────────────────────────────────

def phase1_kaplan_meier(df: pd.DataFrame, out: Path) -> None:
    section("Phase 1 — Stratified Kaplan-Meier")
    from lifelines import KaplanMeierFitter
    import plotly.graph_objects as go

    COLORS = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399", "#fb923c"]
    T = df["time_days"] / 365.25
    E = df["event"].astype(bool)

    strats = {
        "ETE Type": "ete_type",
        "BRAF Status": "braf_status",
        "AJCC Stage": "ajcc_stage_8",
        "Histology": "histology",
    }
    summary_rows = []

    for title, col in strats.items():
        if col not in df.columns or df[col].isna().all():
            print(f"  SKIP {title} — column missing or all null")
            continue
        fig = go.Figure()
        groups = sorted(df[col].dropna().unique(), key=str)
        for i, grp in enumerate(groups):
            mask = df[col] == grp
            if mask.sum() < 10:
                continue
            kmf = KaplanMeierFitter()
            kmf.fit(T[mask], event_observed=E[mask], label=str(grp))
            sf = kmf.survival_function_
            ci = kmf.confidence_interval_survival_function_
            color = COLORS[i % len(COLORS)]
            fig.add_trace(go.Scatter(
                x=sf.index, y=sf.iloc[:, 0], mode="lines",
                name=f"{grp} (n={mask.sum()})",
                line=dict(color=color, width=2),
            ))
            fig.add_trace(go.Scatter(
                x=ci.index.tolist() + ci.index.tolist()[::-1],
                y=ci.iloc[:, 0].tolist() + ci.iloc[:, 1].tolist()[::-1],
                fill="toself", fillcolor=color.replace(")", ",0.1)").replace("rgb", "rgba"),
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
            median = kmf.median_survival_time_
            summary_rows.append(dict(
                stratifier=title, group=str(grp), n=int(mask.sum()),
                events=int(E[mask].sum()),
                median_years=round(float(median), 2) if np.isfinite(median) else None,
                surv_5y=round(float(kmf.predict(5)), 3),
                surv_10y=round(float(kmf.predict(10)), 3),
            ))
        fig.update_layout(
            title=f"KM: Recurrence-Free Survival by {title}",
            xaxis_title="Years from Surgery", yaxis_title="Survival Probability",
            yaxis_range=[0, 1.05], height=500, template="plotly_dark",
            legend=dict(bgcolor="rgba(14,18,25,0.8)"),
        )
        fname = f"km_{col}.png"
        fig.write_image(str(out / fname), scale=3)
        print(f"  Saved {fname}")

    pd.DataFrame(summary_rows).to_csv(out / "km_summary.csv", index=False)
    print(f"  Saved km_summary.csv ({len(summary_rows)} strata)")


# ── Phase 2: Cox PH + Schoenfeld ────────────────────────────────────────

def phase2_cox_ph(df: pd.DataFrame, out: Path) -> None:
    section("Phase 2 — Multivariable Cox PH")
    from lifelines import CoxPHFitter

    model_df = df[["time_days", "event", "age_at_diagnosis", "braf_status",
                    "tert_status", "ln_positive"]].copy()
    model_df["time_years"] = model_df.pop("time_days") / 365.25
    for col in ["braf_status", "tert_status"]:
        model_df[col] = model_df[col].astype(int)
    model_df = model_df.dropna()

    if len(model_df) < 50:
        print("  SKIP: too few complete cases for Cox model")
        return

    # Add ETE dummies
    if "ete_type" in df.columns:
        ete = pd.get_dummies(df.loc[model_df.index, "ete_type"],
                             prefix="ete", drop_first=True, dtype=int)
        model_df = pd.concat([model_df, ete], axis=1)

    # Add stage dummies (top 4 stages)
    if "ajcc_stage_8" in df.columns:
        stage = df.loc[model_df.index, "ajcc_stage_8"]
        top4 = stage.value_counts().head(4).index.tolist()
        stage_filt = stage.where(stage.isin(top4), other="Other")
        dummies = pd.get_dummies(stage_filt, prefix="stage", drop_first=True, dtype=int)
        model_df = pd.concat([model_df, dummies], axis=1)

    model_df = model_df.dropna()

    cph = CoxPHFitter(penalizer=0.01)
    cph.fit(model_df, duration_col="time_years", event_col="event")
    cph.print_summary()

    summary = cph.summary.copy()
    summary.to_csv(out / "cox_model.csv")
    print(f"  Concordance: {cph.concordance_index_:.3f}")
    print("  Saved cox_model.csv")

    # Schoenfeld test
    try:
        results = cph.check_assumptions(model_df, p_value_threshold=0.05,
                                        show_plots=False)
        with open(out / "schoenfeld_test.txt", "w") as f:
            if results:
                for item in results:
                    f.write(str(item) + "\n")
            else:
                f.write("Proportional hazards assumption satisfied for all covariates.\n")
        print("  Saved schoenfeld_test.txt")
    except Exception as e:
        print(f"  Schoenfeld test note: {e}")

    return cph


# ── Phase 3: RMST ───────────────────────────────────────────────────────

def phase3_rmst(df: pd.DataFrame, out: Path) -> None:
    section("Phase 3 — Restricted Mean Survival Time")
    from lifelines import KaplanMeierFitter
    from lifelines.utils import restricted_mean_survival_time

    T = df["time_days"] / 365.25
    E = df["event"].astype(bool)

    results = []
    tau_values = [5, 10]

    if "ete_type" not in df.columns:
        print("  SKIP: ete_type not available")
        return

    groups = df["ete_type"].dropna().unique()
    for tau in tau_values:
        for grp in sorted(groups, key=str):
            mask = df["ete_type"] == grp
            if mask.sum() < 10:
                continue
            kmf = KaplanMeierFitter()
            kmf.fit(T[mask], event_observed=E[mask])
            try:
                rmst = restricted_mean_survival_time(kmf, t=tau)
            except Exception:
                rmst = np.nan
            results.append(dict(
                group=str(grp), tau_years=tau, n=int(mask.sum()),
                rmst_years=round(float(rmst), 3) if np.isfinite(rmst) else None,
            ))

    # Pairwise RMST differences
    for tau in tau_values:
        tau_rows = [r for r in results if r["tau_years"] == tau]
        ref = next((r for r in tau_rows if r["group"] == "none"), None)
        if ref and ref["rmst_years"]:
            for r in tau_rows:
                if r["group"] != "none" and r["rmst_years"]:
                    r["rmst_diff_vs_none"] = round(r["rmst_years"] - ref["rmst_years"], 3)

    rmst_df = pd.DataFrame(results)
    rmst_df.to_csv(out / "rmst_table.csv", index=False)
    print("  Saved rmst_table.csv")
    print(rmst_df.to_string(index=False))


# ── Phase 4: Cumulative Incidence ────────────────────────────────────────

def phase4_cumulative_incidence(df: pd.DataFrame, out: Path) -> None:
    section("Phase 4 — Cumulative Incidence Functions")
    import plotly.graph_objects as go

    try:
        from lifelines import AalenJohansenFitter
        has_aj = True
    except ImportError:
        has_aj = False

    T = df["time_days"] / 365.25
    E = df["event_type"]
    COLORS = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b"]

    if not has_aj:
        from lifelines import KaplanMeierFitter
        print("  AalenJohansenFitter not available; using 1 - KM as CIF proxy")
        fig = go.Figure()
        if "ete_type" in df.columns:
            for i, grp in enumerate(sorted(df["ete_type"].dropna().unique(), key=str)):
                mask = df["ete_type"] == grp
                if mask.sum() < 10:
                    continue
                kmf = KaplanMeierFitter()
                kmf.fit(T[mask], event_observed=df.loc[mask, "event"].astype(bool))
                sf = kmf.survival_function_
                fig.add_trace(go.Scatter(
                    x=sf.index, y=1 - sf.iloc[:, 0], mode="lines",
                    name=f"{grp} (n={mask.sum()})",
                    line=dict(color=COLORS[i % len(COLORS)], width=2),
                ))
        fig.update_layout(
            title="Cumulative Incidence of Recurrence by ETE Type",
            xaxis_title="Years", yaxis_title="Cumulative Incidence",
            yaxis_range=[0, 0.5], height=500, template="plotly_dark",
        )
        fig.write_image(str(out / "cif_ete.png"), scale=3)
        print("  Saved cif_ete.png (1 - KM proxy)")
        return

    fig = go.Figure()
    if "ete_type" in df.columns:
        for i, grp in enumerate(sorted(df["ete_type"].dropna().unique(), key=str)):
            mask = df["ete_type"] == grp
            if mask.sum() < 10:
                continue
            aj = AalenJohansenFitter(calculate_variance=True)
            aj.fit(T[mask], E[mask], event_of_interest=1)
            cif = aj.cumulative_density_
            fig.add_trace(go.Scatter(
                x=cif.index, y=cif.iloc[:, 0], mode="lines",
                name=f"{grp} (n={mask.sum()})",
                line=dict(color=COLORS[i % len(COLORS)], width=2),
            ))
    fig.update_layout(
        title="Cumulative Incidence of Recurrence (Aalen-Johansen)",
        xaxis_title="Years", yaxis_title="Cumulative Incidence",
        yaxis_range=[0, 0.5], height=500, template="plotly_dark",
    )
    fig.write_image(str(out / "cif_ete.png"), scale=3)
    print("  Saved cif_ete.png")


# ── Phase 5: PSM-matched survival ───────────────────────────────────────

def phase5_psm(df: pd.DataFrame, out: Path) -> None:
    section("Phase 5 — Propensity Score Matched Survival (ETE)")
    from lifelines import CoxPHFitter, KaplanMeierFitter
    import plotly.graph_objects as go

    if "ete_type" not in df.columns:
        print("  SKIP: ete_type not available")
        return

    df_psm = df[df["ete_type"].isin(["gross", "none"])].copy()
    df_psm["treatment"] = (df_psm["ete_type"] == "gross").astype(int)
    confounders = ["age_at_diagnosis", "ln_positive", "braf_status", "tert_status"]
    for c in confounders:
        if c in df_psm.columns:
            df_psm[c] = pd.to_numeric(df_psm[c], errors="coerce")
    df_psm = df_psm.dropna(subset=["treatment", "time_days", "event"] + confounders)

    if len(df_psm) < 100:
        print(f"  SKIP: only {len(df_psm)} rows for PSM")
        return

    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        print("  SKIP: scikit-learn not installed")
        return

    X = df_psm[confounders].values
    y = df_psm["treatment"].values
    scaler = StandardScaler()
    X_s = scaler.fit_transform(X)
    lr = LogisticRegression(random_state=SEED, max_iter=1000)
    lr.fit(X_s, y)
    ps = lr.predict_proba(X_s)[:, 1]
    df_psm["ps"] = ps

    treated = df_psm[df_psm["treatment"] == 1].copy()
    control = df_psm[df_psm["treatment"] == 0].copy()
    caliper = 0.25 * np.std(ps)

    matched_t, matched_c = [], []
    used_control = set()
    for idx, row in treated.iterrows():
        candidates = control[~control.index.isin(used_control)]
        if candidates.empty:
            break
        dists = np.abs(candidates["ps"].values - row["ps"])
        best_idx = candidates.index[np.argmin(dists)]
        if dists.min() <= caliper:
            matched_t.append(idx)
            matched_c.append(best_idx)
            used_control.add(best_idx)

    n_pairs = len(matched_t)
    print(f"  Matched pairs: {n_pairs}")
    if n_pairs < 30:
        print("  SKIP: too few matched pairs")
        return

    df_matched = pd.concat([
        df_psm.loc[matched_t],
        df_psm.loc[matched_c],
    ])

    # Balance check (SMD)
    balance_rows = []
    for c in confounders:
        t_vals = df_matched.loc[df_matched["treatment"] == 1, c]
        c_vals = df_matched.loc[df_matched["treatment"] == 0, c]
        pooled_sd = np.sqrt((t_vals.var() + c_vals.var()) / 2)
        smd = abs(t_vals.mean() - c_vals.mean()) / pooled_sd if pooled_sd > 0 else 0
        balance_rows.append(dict(confounder=c, smd=round(smd, 4),
                                 mean_treated=round(t_vals.mean(), 3),
                                 mean_control=round(c_vals.mean(), 3)))
    pd.DataFrame(balance_rows).to_csv(out / "psm_balance.csv", index=False)
    print("  Saved psm_balance.csv")

    # Cox in matched cohort
    matched_model = df_matched[["time_days", "event", "treatment"]].copy()
    matched_model["time_years"] = matched_model.pop("time_days") / 365.25
    cph = CoxPHFitter()
    cph.fit(matched_model, duration_col="time_years", event_col="event")
    hr = float(np.exp(cph.params_["treatment"]))
    ci_lo = float(np.exp(cph.confidence_intervals_.iloc[0, 0]))
    ci_hi = float(np.exp(cph.confidence_intervals_.iloc[0, 1]))
    p_val = float(cph.summary["p"]["treatment"])
    print(f"  HR = {hr:.3f} ({ci_lo:.3f}–{ci_hi:.3f}), p = {p_val:.4f}")

    psm_result = dict(
        n_pairs=n_pairs, hr=round(hr, 3),
        ci_lower=round(ci_lo, 3), ci_upper=round(ci_hi, 3),
        p_value=round(p_val, 4), concordance=round(cph.concordance_index_, 3),
        caliper=round(caliper, 4),
    )
    with open(out / "psm_result.json", "w") as f:
        json.dump(psm_result, f, indent=2)
    print("  Saved psm_result.json")

    # KM of matched cohort
    fig = go.Figure()
    for label, val, color in [("Gross ETE", 1, "#f43f5e"), ("No ETE", 0, "#2dd4bf")]:
        sub = df_matched[df_matched["treatment"] == val]
        kmf = KaplanMeierFitter()
        kmf.fit(sub["time_days"] / 365.25, event_observed=sub["event"].astype(bool))
        sf = kmf.survival_function_
        fig.add_trace(go.Scatter(
            x=sf.index, y=sf.iloc[:, 0], mode="lines",
            name=f"{label} (n={len(sub)})", line=dict(color=color, width=2),
        ))
    fig.update_layout(
        title=f"PSM-Matched KM (n={n_pairs} pairs) — HR={hr:.2f}, p={p_val:.3f}",
        xaxis_title="Years", yaxis_title="Survival Probability",
        yaxis_range=[0, 1.05], height=500, template="plotly_dark",
    )
    fig.write_image(str(out / "psm_km.png"), scale=3)
    print("  Saved psm_km.png")


# ── Phase 6: Random Survival Forest + SHAP ──────────────────────────────

def phase6_rsf(df: pd.DataFrame, out: Path) -> None:
    section("Phase 6 — Random Survival Forest + SHAP")
    try:
        from sksurv.ensemble import RandomSurvivalForest
        from sksurv.metrics import concordance_index_censored
    except ImportError:
        print("  SKIP: scikit-survival not installed (pip install scikit-survival)")
        return

    feature_cols = ["age_at_diagnosis", "braf_status", "tert_status",
                    "ras_status", "ln_positive", "tumor_size_cm"]
    if "ete_type" in df.columns:
        ete_dummies = pd.get_dummies(df["ete_type"], prefix="ete", drop_first=True, dtype=int)
        X = pd.concat([df[feature_cols], ete_dummies], axis=1)
    else:
        X = df[feature_cols].copy()

    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    y_event = df["event"].astype(bool)
    y_time = df["time_days"].astype(float)

    valid = X.notna().all(axis=1) & y_time.notna() & (y_time > 0)
    X = X[valid].reset_index(drop=True)
    y_event = y_event[valid].reset_index(drop=True)
    y_time = y_time[valid].reset_index(drop=True)

    if len(X) < 100:
        print(f"  SKIP: only {len(X)} complete cases")
        return

    y_struct = np.array(
        list(zip(y_event.values, y_time.values)),
        dtype=[("event", bool), ("time", float)],
    )

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_struct, test_size=0.3, random_state=SEED,
    )

    rsf = RandomSurvivalForest(
        n_estimators=200, min_samples_split=10, min_samples_leaf=5,
        max_features="sqrt", n_jobs=-1, random_state=SEED,
    )
    rsf.fit(X_train, y_train)

    c_index = concordance_index_censored(
        y_test["event"], y_test["time"], rsf.predict(X_test),
    )[0]
    print(f"  C-index (test): {c_index:.3f}")

    # Feature importance
    importances = pd.DataFrame({
        "feature": X.columns,
        "importance": rsf.feature_importances_,
    }).sort_values("importance", ascending=False)
    importances.to_csv(out / "rsf_importances.csv", index=False)
    print("  Saved rsf_importances.csv")
    print(importances.to_string(index=False))

    # SHAP
    try:
        import shap
        explainer = shap.Explainer(rsf.predict, X_test, seed=SEED)
        shap_values = explainer(X_test[:200])

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig_shap, ax = plt.subplots(figsize=(8, 6))
        shap.plots.beeswarm(shap_values, show=False)
        fig_shap.tight_layout()
        fig_shap.savefig(out / "shap_beeswarm.png", dpi=300, bbox_inches="tight")
        plt.close(fig_shap)
        print("  Saved shap_beeswarm.png")

        fig_bar, ax2 = plt.subplots(figsize=(8, 5))
        shap.plots.bar(shap_values, show=False)
        fig_bar.tight_layout()
        fig_bar.savefig(out / "shap_bar.png", dpi=300, bbox_inches="tight")
        plt.close(fig_bar)
        print("  Saved shap_bar.png")
    except Exception as e:
        print(f"  SHAP visualization note: {e}")

    # Save RSF metrics
    rsf_meta = dict(
        c_index_test=round(c_index, 4),
        n_train=len(X_train), n_test=len(X_test),
        n_estimators=200, features=X.columns.tolist(),
    )
    with open(out / "rsf_metrics.json", "w") as f:
        json.dump(rsf_meta, f, indent=2)
    print("  Saved rsf_metrics.json")


# ── Phase 7: DeepSurv (optional) ────────────────────────────────────────

def phase7_deepsurv(df: pd.DataFrame, out: Path) -> None:
    section("Phase 7 — DeepSurv (PyTorch)")
    try:
        import torch
        import torch.nn as nn
    except ImportError:
        print("  SKIP: PyTorch not installed (pip install torch)")
        return

    feature_cols = ["age_at_diagnosis", "braf_status", "tert_status",
                    "ras_status", "ln_positive", "tumor_size_cm"]
    X = df[feature_cols].copy()
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    valid = X.notna().all(axis=1) & df["time_days"].notna() & (df["time_days"] > 0)
    X = X[valid].reset_index(drop=True)
    T = (df.loc[valid, "time_days"] / 365.25).reset_index(drop=True).astype(float)
    E = df.loc[valid, "event"].reset_index(drop=True).astype(float)

    if len(X) < 100:
        print(f"  SKIP: only {len(X)} complete cases")
        return

    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split

    X_train, X_test, T_train, T_test, E_train, E_test = train_test_split(
        X.values, T.values, E.values, test_size=0.3, random_state=SEED,
    )
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    class DeepSurv(nn.Module):
        def __init__(self, in_features: int):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(in_features, 64), nn.SELU(), nn.AlphaDropout(0.1),
                nn.Linear(64, 32), nn.SELU(), nn.AlphaDropout(0.1),
                nn.Linear(32, 1),
            )

        def forward(self, x):
            return self.net(x)

    def negative_log_partial_likelihood(risk: torch.Tensor, T: torch.Tensor,
                                        E: torch.Tensor) -> torch.Tensor:
        order = torch.argsort(T, descending=True)
        risk = risk[order].squeeze()
        E = E[order]
        log_cumsum = torch.logcumsumexp(risk, dim=0)
        loss = -torch.sum((risk - log_cumsum) * E) / torch.clamp(E.sum(), min=1)
        return loss

    device = torch.device("cpu")
    model = DeepSurv(X_train_s.shape[1]).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-4)

    X_t = torch.tensor(X_train_s, dtype=torch.float32, device=device)
    T_t = torch.tensor(T_train, dtype=torch.float32, device=device)
    E_t = torch.tensor(E_train, dtype=torch.float32, device=device)

    model.train()
    for epoch in range(300):
        optimizer.zero_grad()
        risk = model(X_t)
        loss = negative_log_partial_likelihood(risk, T_t, E_t)
        loss.backward()
        optimizer.step()
        if (epoch + 1) % 100 == 0:
            print(f"    Epoch {epoch+1}: loss = {loss.item():.4f}")

    model.eval()
    with torch.no_grad():
        X_te = torch.tensor(X_test_s, dtype=torch.float32, device=device)
        risk_test = model(X_te).squeeze().cpu().numpy()

    from lifelines.utils import concordance_index
    c_idx = concordance_index(T_test, -risk_test, E_test)
    print(f"  DeepSurv C-index (test): {c_idx:.3f}")

    ds_meta = dict(
        c_index_test=round(c_idx, 4),
        n_train=len(X_train), n_test=len(X_test),
        epochs=300, features=feature_cols,
    )
    with open(out / "deepsurv_metrics.json", "w") as f:
        json.dump(ds_meta, f, indent=2)
    print("  Saved deepsurv_metrics.json")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Advanced survival analysis — publication outputs"
    )
    parser.add_argument("--md", action="store_true",
                        help="Read from MotherDuck instead of local DuckDB")
    parser.add_argument("--local", action="store_true",
                        help="Force local DuckDB")
    parser.add_argument("--dry-run", action="store_true",
                        help="Load data and report shape only")
    args = parser.parse_args()

    if not args.local and not args.md:
        args.local = True

    section("ADVANCED SURVIVAL ANALYSIS — THYROID_2026")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Output: {OUT_DIR}")

    con = _get_connection(args)
    df = _load_cohort(con)

    print(f"\n  Cohort: {len(df):,} patients")
    print(f"  Events: {df['event'].sum():,} ({100*df['event'].mean():.1f}%)")
    print(f"  Median follow-up: {df['time_days'].median()/365.25:.1f} years")
    print(f"  Columns: {df.columns.tolist()}")

    if args.dry_run:
        print("\n  DRY RUN — exiting after data summary.")
        con.close()
        return

    phase1_kaplan_meier(df, OUT_DIR)
    phase2_cox_ph(df, OUT_DIR)
    phase3_rmst(df, OUT_DIR)
    phase4_cumulative_incidence(df, OUT_DIR)
    phase5_psm(df, OUT_DIR)
    phase6_rsf(df, OUT_DIR)
    phase7_deepsurv(df, OUT_DIR)

    # Summary metadata
    meta = dict(
        run_date=datetime.now().isoformat(),
        cohort_n=len(df),
        events=int(df["event"].sum()),
        event_rate_pct=round(100 * df["event"].mean(), 1),
        median_followup_years=round(df["time_days"].median() / 365.25, 1),
        output_dir=str(OUT_DIR),
        columns=df.columns.tolist(),
        script="38_advanced_survival_analysis.py",
    )
    with open(OUT_DIR / "analysis_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    con.close()
    print("\n" + "=" * 72)
    print("  ADVANCED SURVIVAL MODULE COMPLETE — publication-ready")
    print(f"  Results in: {OUT_DIR}")
    print("=" * 72)


if __name__ == "__main__":
    main()
