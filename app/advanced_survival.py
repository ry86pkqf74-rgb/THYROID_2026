"""Advanced Survival tab — interactive KM/CIF, RMST, SHAP, Cox forest plot.

Reads from `survival_cohort_enriched` and `survival_kpis` tables built by
script 26.  Pre-computed export files from script 38 are loaded for SHAP
and RSF importances when available.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "exports" / "survival_results"

PL = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,18,25,0.8)",
    font=dict(family="DM Sans", color="#8892a4", size=12),
    title_font=dict(family="DM Serif Display", color="#f0f4ff", size=15),
    xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535", zerolinecolor="#1e2535"),
    legend=dict(bgcolor="rgba(14,18,25,0.8)", bordercolor="#1e2535", borderwidth=1),
    margin=dict(l=16, r=16, t=36, b=16),
    colorway=["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399", "#fb923c"],
    hoverlabel=dict(bgcolor="#141923", bordercolor="#1e2535", font_color="#f0f4ff"),
)
COLORS = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399", "#fb923c"]


def _sl(t: str) -> str:
    return f'<span class="section-label">{t}</span>'


def _mc(label: str, value: str, delta: str | None = None) -> str:
    d = f'<div class="metric-delta">{delta}</div>' if delta else ""
    return (
        f'<div class="metric-card"><div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>{d}</div>'
    )


def _tbl_exists(con, name: str) -> bool:
    try:
        n = con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name='{name}'"
        ).fetchone()[0]
        return bool(n)
    except Exception:
        return False


def _sqdf(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()


def _load_export(filename: str) -> pd.DataFrame | dict | None:
    p = EXPORT_DIR / filename
    if not p.exists():
        return None
    if filename.endswith(".csv"):
        return pd.read_csv(p)
    if filename.endswith(".json"):
        with open(p) as f:
            return json.load(f)
    return None


# ── Main render ──────────────────────────────────────────────────────────

def render_advanced_survival(con) -> None:
    """Render the Advanced Survival dashboard tab."""

    tbl = "survival_cohort_enriched"
    if not _tbl_exists(con, tbl):
        st.info(
            "Advanced survival data not available. "
            "Run `python scripts/26_motherduck_materialize_v2.py` to build "
            "`survival_cohort_enriched`.",
            icon="📉",
        )
        return

    # ── KPIs ─────────────────────────────────────────────────────────────
    kpi_df = _sqdf(con, "SELECT * FROM survival_kpis") if _tbl_exists(con, "survival_kpis") else pd.DataFrame()
    if not kpi_df.empty:
        r = kpi_df.iloc[0]
        cols = st.columns(5)
        items = [
            ("Cohort N", f"{int(r.get('n') or 0):,}"),
            ("Events", f"{int(r.get('events') or 0):,}"),
            ("Event Rate", f"{float(r.get('event_rate_pct') or 0):.1f}%"),
            ("Mean F/U (yr)", f"{float(r.get('mean_followup_years') or 0):.1f}"),
            ("Median F/U (yr)", f"{float(r.get('median_followup_years') or 0):.1f}"),
        ]
        for c, (label, val) in zip(cols, items):
            with c:
                st.markdown(_mc(label, val), unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # ── Load cohort ──────────────────────────────────────────────────────
    df = _sqdf(con, f"SELECT * FROM {tbl}")
    if df.empty:
        st.warning("Survival cohort is empty.")
        return

    try:
        from lifelines import KaplanMeierFitter
        has_lifelines = True
    except ImportError:
        has_lifelines = False
        st.warning("Install `lifelines` for interactive KM/RMST: `pip install lifelines`")

    # ── Interactive KM ───────────────────────────────────────────────────
    st.markdown(_sl("Kaplan-Meier: Recurrence-Free Survival"), unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    strat_opts = {
        "None": None,
        "ETE Type": "ete_type",
        "BRAF Status": "braf_status",
        "AJCC Stage": "ajcc_stage_8",
        "Histology": "histology",
        "TERT Status": "tert_status",
        "Risk Band": "recurrence_risk_band",
    }
    with c1:
        strat_choice = st.selectbox("Stratify by", list(strat_opts.keys()), key="advsurv_strat")
    with c2:
        max_yr = st.slider("Max follow-up (years)", 1, 20, 15, key="advsurv_yr")

    strat_col = strat_opts[strat_choice]
    T = df["time_days"] / 365.25
    E = df["event"].astype(bool)
    mask_time = T <= max_yr

    fig_km = go.Figure()
    if has_lifelines:
        if strat_col is None or strat_col not in df.columns:
            kmf = KaplanMeierFitter()
            kmf.fit(T[mask_time], event_observed=E[mask_time])
            sf = kmf.survival_function_
            fig_km.add_trace(go.Scatter(
                x=sf.index, y=sf.iloc[:, 0], mode="lines",
                name=f"Overall (n={mask_time.sum()})",
                line=dict(color="#2dd4bf", width=2),
            ))
        else:
            groups = sorted(df.loc[mask_time, strat_col].dropna().unique(), key=str)
            for i, grp in enumerate(groups):
                sub_mask = mask_time & (df[strat_col] == grp)
                if sub_mask.sum() < 5:
                    continue
                kmf = KaplanMeierFitter()
                kmf.fit(T[sub_mask], event_observed=E[sub_mask])
                sf = kmf.survival_function_
                color = COLORS[i % len(COLORS)]
                fig_km.add_trace(go.Scatter(
                    x=sf.index, y=sf.iloc[:, 0], mode="lines",
                    name=f"{grp} (n={sub_mask.sum()})",
                    line=dict(color=color, width=2),
                ))
    fig_km.update_layout(
        **PL, height=500,
        xaxis_title="Years from Surgery", yaxis_title="Event-Free Probability",
        yaxis_range=[0, 1.05],
        title=f"KM: Recurrence-Free Survival{f' by {strat_choice}' if strat_col else ''}",
    )
    st.plotly_chart(fig_km, use_container_width=True)

    # ── Cumulative Incidence ─────────────────────────────────────────────
    st.markdown(_sl("Cumulative Incidence of Recurrence"), unsafe_allow_html=True)
    if has_lifelines and "ete_type" in df.columns:
        fig_cif = go.Figure()
        for i, grp in enumerate(sorted(df["ete_type"].dropna().unique(), key=str)):
            sub = mask_time & (df["ete_type"] == grp)
            if sub.sum() < 5:
                continue
            kmf = KaplanMeierFitter()
            kmf.fit(T[sub], event_observed=E[sub])
            sf = kmf.survival_function_
            fig_cif.add_trace(go.Scatter(
                x=sf.index, y=1 - sf.iloc[:, 0], mode="lines",
                name=f"{grp} (n={sub.sum()})",
                line=dict(color=COLORS[i % len(COLORS)], width=2),
            ))
        fig_cif.update_layout(
            **PL, height=420,
            xaxis_title="Years", yaxis_title="Cumulative Incidence",
            yaxis_range=[0, 0.4],
            title="Cumulative Incidence by ETE Type",
        )
        st.plotly_chart(fig_cif, use_container_width=True)
    else:
        st.caption("ETE type data or lifelines not available for CIF plot.")

    # ── RMST table ───────────────────────────────────────────────────────
    st.markdown(_sl("Restricted Mean Survival Time (RMST)"), unsafe_allow_html=True)
    rmst_df = _load_export("rmst_table.csv")
    if rmst_df is not None and not rmst_df.empty:
        st.dataframe(rmst_df, use_container_width=True, hide_index=True)
    elif has_lifelines and "ete_type" in df.columns:
        from lifelines.utils import restricted_mean_survival_time
        rows = []
        for tau in [5, 10]:
            for grp in sorted(df["ete_type"].dropna().unique(), key=str):
                sub = df["ete_type"] == grp
                if sub.sum() < 10:
                    continue
                kmf = KaplanMeierFitter()
                kmf.fit(T[sub], event_observed=E[sub])
                try:
                    rmst = restricted_mean_survival_time(kmf, t=tau)
                except Exception:
                    rmst = float("nan")
                rows.append(dict(group=grp, tau_years=tau, n=int(sub.sum()),
                                 rmst_years=round(float(rmst), 3)))
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Run `python scripts/38_advanced_survival_analysis.py` for RMST table.")

    # ── Cox model summary ────────────────────────────────────────────────
    st.markdown(_sl("Cox PH Model Summary"), unsafe_allow_html=True)
    cox_df = _load_export("cox_model.csv")
    if cox_df is not None and not cox_df.empty:
        fig_forest = go.Figure()
        cox_df = cox_df.reset_index()
        covar_col = cox_df.columns[0]
        for idx, row in cox_df.iterrows():
            hr = row.get("exp(coef)", None)
            lo = row.get("exp(coef) lower 95%", None)
            hi = row.get("exp(coef) upper 95%", None)
            if hr is None or pd.isna(hr):
                continue
            color = "#f43f5e" if hr > 1 else "#2dd4bf"
            fig_forest.add_trace(go.Scatter(
                x=[lo, hr, hi], y=[row[covar_col]] * 3,
                mode="lines+markers",
                marker=dict(size=[0, 10, 0], color=color),
                line=dict(color=color, width=2),
                name=f"{row[covar_col]}: HR={hr:.2f}",
                showlegend=False,
            ))
        fig_forest.add_vline(x=1.0, line_dash="dash", line_color="#4a5568")
        fig_forest.update_layout(
            **PL, height=max(300, len(cox_df) * 35),
            xaxis_title="Hazard Ratio (95% CI)",
            title="Cox PH — Forest Plot",
        )
        st.plotly_chart(fig_forest, use_container_width=True)

        with st.expander("Full Cox model table"):
            st.dataframe(cox_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Run `python scripts/38_advanced_survival_analysis.py` for Cox results.")

    # ── PSM results ──────────────────────────────────────────────────────
    st.markdown(_sl("Propensity Score Matched Survival (ETE)"), unsafe_allow_html=True)
    psm = _load_export("psm_result.json")
    if psm:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(_mc("Matched Pairs", f"{psm.get('n_pairs', '—'):,}"), unsafe_allow_html=True)
        with c2:
            st.markdown(_mc("HR (gross ETE)", f"{psm.get('hr', '—')}"), unsafe_allow_html=True)
        with c3:
            st.markdown(_mc("95% CI", f"{psm.get('ci_lower', '—')}–{psm.get('ci_upper', '—')}"),
                        unsafe_allow_html=True)
        with c4:
            st.markdown(_mc("p-value", f"{psm.get('p_value', '—')}"), unsafe_allow_html=True)

        balance_df = _load_export("psm_balance.csv")
        if balance_df is not None:
            with st.expander("PSM balance (SMD)"):
                st.dataframe(balance_df, use_container_width=True, hide_index=True)

        psm_img = EXPORT_DIR / "psm_km.png"
        if psm_img.exists():
            st.image(str(psm_img), caption="PSM-Matched Kaplan-Meier", use_container_width=True)
    else:
        st.caption("Run `python scripts/38_advanced_survival_analysis.py` for PSM results.")

    # ── RSF / SHAP ───────────────────────────────────────────────────────
    st.markdown(_sl("Random Survival Forest & SHAP"), unsafe_allow_html=True)
    rsf_metrics = _load_export("rsf_metrics.json")
    if rsf_metrics:
        st.markdown(
            _mc("RSF C-index (test)", f"{rsf_metrics.get('c_index_test', '—')}"),
            unsafe_allow_html=True,
        )
        imp_df = _load_export("rsf_importances.csv")
        if imp_df is not None and not imp_df.empty:
            fig_imp = go.Figure(go.Bar(
                x=imp_df["importance"], y=imp_df["feature"],
                orientation="h", marker_color="#2dd4bf",
            ))
            fig_imp.update_layout(
                **PL, height=max(250, len(imp_df) * 30),
                title="RSF Feature Importance", xaxis_title="Importance",
            )
            st.plotly_chart(fig_imp, use_container_width=True)

        shap_img = EXPORT_DIR / "shap_beeswarm.png"
        if shap_img.exists():
            st.image(str(shap_img), caption="SHAP Beeswarm Plot", use_container_width=True)
    else:
        st.caption(
            "Run `python scripts/38_advanced_survival_analysis.py` for RSF + SHAP. "
            "Requires: `pip install scikit-survival shap`"
        )

    # ── DeepSurv ─────────────────────────────────────────────────────────
    ds_metrics = _load_export("deepsurv_metrics.json")
    if ds_metrics:
        st.markdown(_sl("DeepSurv Neural Survival Model"), unsafe_allow_html=True)
        st.markdown(
            _mc("DeepSurv C-index (test)", f"{ds_metrics.get('c_index_test', '—')}"),
            unsafe_allow_html=True,
        )

    # ── Download ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.caption(
        "Full analysis outputs (figures + tables) available in "
        "`exports/survival_results/`. "
        "Run `python scripts/38_advanced_survival_analysis.py` to regenerate."
    )
