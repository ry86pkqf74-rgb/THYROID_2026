"""Cure Probability dashboard tab."""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from app.helpers import PL, mc, multi_export, qual, sl, sqdf, tbl_exists


def _safe_rate(df: pd.DataFrame) -> float:
    if df.empty:
        return float("nan")
    vals = pd.to_numeric(df["event"], errors="coerce").fillna(0).astype(int)
    return float(vals.mean())


def _cohort_rate(df: pd.DataFrame, col: str, val) -> float:
    if col not in df.columns or df.empty:
        return float("nan")
    sub = df[df[col].astype(str) == str(val)]
    if sub.empty:
        return float("nan")
    return 1.0 - _safe_rate(sub)


def render_cure_probability(con) -> None:
    if not tbl_exists(con, "cure_cohort"):
        st.warning(
            "Cure cohort table is not available yet. "
            "Run `python scripts/26_motherduck_materialize_v2.py --md` first.",
            icon="⚠️",
        )
        return

    st.markdown(sl("Cure Probability"), unsafe_allow_html=True)
    df = sqdf(con, f"SELECT * FROM {qual('cure_cohort')}")
    if df.empty:
        st.info("No rows available in cure_cohort.")
        return

    if "event" not in df.columns:
        st.error("`cure_cohort` is missing `event`; please rebuild script 26 outputs.")
        return

    df["event"] = pd.to_numeric(df["event"], errors="coerce").fillna(0).astype(int)
    df["time_days"] = pd.to_numeric(df.get("time_days", np.nan), errors="coerce").fillna(365 * 15)

    kpi = sqdf(con, f"SELECT * FROM {qual('cure_kpis')} LIMIT 1") if tbl_exists(con, "cure_kpis") else pd.DataFrame()
    if not kpi.empty:
        row = kpi.iloc[0]
        n_total = int(row.get("n_total", len(df)))
        observed_event_rate = float(row.get("observed_event_rate", df["event"].mean()))
        crude_cure_rate = float(row.get("crude_cure_rate", 1.0 - df["event"].mean()))
    else:
        n_total = len(df)
        observed_event_rate = float(df["event"].mean())
        crude_cure_rate = 1.0 - observed_event_rate

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(mc("Cohort Size", f"{n_total:,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Observed Event Rate", f"{observed_event_rate:.1%}"), unsafe_allow_html=True)
    with c3:
        st.markdown(mc("Crude Cure Rate", f"{crude_cure_rate:.1%}"), unsafe_allow_html=True)

    subgroup_rows = []
    for col in ["ajcc_stage_8", "ete_type", "braf_status", "recurrence_risk_band"]:
        if col not in df.columns:
            continue
        g = (
            df.groupby(col, dropna=False)["event"]
            .agg(["count", "mean"])
            .reset_index()
            .rename(columns={"count": "n", "mean": "observed_event_rate"})
        )
        g["group_variable"] = col
        g["group_value"] = g[col].astype(str)
        g["cure_fraction"] = 1.0 - g["observed_event_rate"]
        subgroup_rows.append(g[["group_variable", "group_value", "n", "observed_event_rate", "cure_fraction"]])
    subgroup_df = pd.concat(subgroup_rows, ignore_index=True) if subgroup_rows else pd.DataFrame()

    if not subgroup_df.empty:
        st.markdown(sl("Cure Fraction by Subgroup"), unsafe_allow_html=True)
        fig = px.bar(
            subgroup_df.sort_values("cure_fraction", ascending=False),
            x="cure_fraction",
            y="group_value",
            color="group_variable",
            orientation="h",
            hover_data=["n", "observed_event_rate"],
            labels={"cure_fraction": "Cure fraction", "group_value": "Group"},
        )
        fig.update_layout(**PL, height=520, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(sl("Patient Cure Calculator"), unsafe_allow_html=True)
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        age = st.slider("Age at diagnosis", 18, 90, 50)
        stage = st.selectbox(
            "AJCC 8 stage",
            sorted(df["ajcc_stage_8"].dropna().astype(str).unique().tolist()) if "ajcc_stage_8" in df.columns else ["unknown"],
        )
    with col_b:
        ete = st.selectbox(
            "ETE type",
            sorted(df["ete_type"].dropna().astype(str).unique().tolist()) if "ete_type" in df.columns else ["none"],
        )
        braf = st.selectbox("BRAF status", ["False", "True"])
    with col_c:
        tert = st.selectbox("TERT status", ["False", "True"])
        risk = st.selectbox(
            "Recurrence risk band",
            sorted(df["recurrence_risk_band"].dropna().astype(str).unique().tolist())
            if "recurrence_risk_band" in df.columns
            else ["unknown"],
        )

    base = crude_cure_rate
    stage_adj = _cohort_rate(df, "ajcc_stage_8", stage)
    ete_adj = _cohort_rate(df, "ete_type", ete)
    braf_adj = _cohort_rate(df, "braf_status", braf.lower() == "true")
    tert_adj = _cohort_rate(df, "tert_status", tert.lower() == "true")
    risk_adj = _cohort_rate(df, "recurrence_risk_band", risk)

    vals = [v for v in [base, stage_adj, ete_adj, braf_adj, tert_adj, risk_adj] if pd.notna(v)]
    est = float(np.clip(np.mean(vals), 0.01, 0.99)) if vals else float(np.clip(base, 0.01, 0.99))
    age_penalty = max(0.0, (age - 60) * 0.003)
    est = float(np.clip(est - age_penalty, 0.01, 0.99))

    st.success(f"Estimated cure probability: **{est:.1%}**")
    st.caption(
        "Estimate is data-driven from subgroup empirical cure rates. "
        "For full model-based predictions, run `python scripts/38_mixture_cure_models.py`."
    )

    st.markdown(sl("Comparison Table"), unsafe_allow_html=True)
    comp = (
        df.groupby(["ajcc_stage_8", "ete_type", "braf_status"], dropna=False)["event"]
        .agg(["count", "mean"])
        .reset_index()
        .rename(columns={"count": "n", "mean": "observed_event_rate"})
    )
    comp["cure_fraction"] = 1.0 - comp["observed_event_rate"]
    st.dataframe(comp, use_container_width=True, hide_index=True)
    multi_export(comp, "cure_probability_comparison", "cure_prob")
