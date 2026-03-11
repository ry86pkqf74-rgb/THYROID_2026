"""
Statistical Analysis & Modeling — Dashboard Tab

Provides interactive Table 1 generation, hypothesis testing with
multiple comparison correction, logistic/Cox regression with forest
plots, correlation heatmaps, and diagnostics/export — all wired
to the ThyroidStatisticalAnalyzer core module.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.helpers import PL, COLORS, sl, badge, multi_export, tbl_exists
from utils.statistical_analysis import (
    ThyroidStatisticalAnalyzer,
    THYROID_TABLE1_PRESET,
    THYROID_OUTCOMES,
    THYROID_PREDICTORS,
    THYROID_SURVIVAL,
    HAS_STATSMODELS,
    HAS_SCIPY,
    HAS_LIFELINES,
    HAS_TABLEONE,
    HAS_PINGOUIN,
    HAS_PLOTLY,
)

_DATA_SOURCES = [
    "risk_enriched_mv",
    "advanced_features_v3",
    "survival_cohort_enriched",
    "ptc_cohort",
    "recurrence_risk_cohort",
]

_GROUPBY_OPTIONS = [
    "(None)",
    "sex",
    "braf_positive",
    "tumor_1_extrathyroidal_ext",
    "overall_stage_ajcc8",
    "histology_1_type",
    "recurrence_risk_band",
    "event_occurred",
]

_CORRECTION_METHODS = {
    "None": None,
    "Bonferroni": "bonferroni",
    "FDR (Benjamini-Hochberg)": "fdr_bh",
    "Holm": "holm",
}


# ── Helpers ───────────────────────────────────────────────────────────────

def _lib_badges() -> str:
    libs = [
        ("statsmodels", HAS_STATSMODELS),
        ("scipy", HAS_SCIPY),
        ("lifelines", HAS_LIFELINES),
        ("tableone", HAS_TABLEONE),
        ("pingouin", HAS_PINGOUIN),
    ]
    parts = []
    for name, ok in libs:
        color = "green" if ok else "rose"
        parts.append(badge(name, color))
    return " ".join(parts)


@st.cache_data(ttl=300, show_spinner=False)
def _load_source(_con, view: str) -> pd.DataFrame:
    try:
        return _con.execute(f"SELECT * FROM {view}").fetchdf()
    except Exception:
        return pd.DataFrame()


def _available_sources(con) -> list[str]:
    return [s for s in _DATA_SOURCES if tbl_exists(con, s)]


def _numeric_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != "research_id"]


def _binary_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns
            if c != "research_id" and df[c].nunique(dropna=True) == 2]


def _highlight_pval(val):
    """Styler function: bold + teal for significant p-values."""
    try:
        v = float(val)
        if v < 0.05:
            return f"color: {COLORS['teal']}; font-weight: 700"
    except (ValueError, TypeError):
        pass
    return ""


# ── Sub-tab renderers ─────────────────────────────────────────────────────

def _render_table_one(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available. Run materialization scripts first.")
        return

    c1, c2 = st.columns(2)
    with c1:
        source = st.selectbox("Data source", sources, key="t1_src")
    with c2:
        groupby_opts = ["(None)"] + [
            g for g in _GROUPBY_OPTIONS[1:]
        ]
        groupby = st.selectbox("Group by", groupby_opts, key="t1_grp")

    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    groupby_col = None if groupby == "(None)" else groupby
    if groupby_col and groupby_col not in df.columns:
        st.warning(f"Column `{groupby_col}` not found in `{source}`. Proceeding without grouping.")
        groupby_col = None

    available_cols = sorted(df.columns.tolist())
    preset_cont = [c for c in THYROID_TABLE1_PRESET["continuous"] if c in df.columns]
    preset_cat = [c for c in THYROID_TABLE1_PRESET["categorical"] if c in df.columns]

    with st.expander("Variable selection", expanded=False):
        cont_vars = st.multiselect(
            "Continuous variables",
            [c for c in available_cols if pd.api.types.is_numeric_dtype(df[c])],
            default=preset_cont,
            key="t1_cont",
        )
        cat_vars = st.multiselect(
            "Categorical variables",
            [c for c in available_cols if c not in cont_vars],
            default=preset_cat,
            key="t1_cat",
        )

    if st.button("Generate Table 1", type="primary", key="t1_run"):
        with st.spinner("Building Table 1..."):
            t1_df, meta = analyzer.generate_table_one(
                data=df,
                groupby_col=groupby_col,
                continuous_vars=cont_vars or None,
                categorical_vars=cat_vars or None,
            )

        if "error" in meta:
            st.error(meta["error"])
            return

        st.markdown(sl("Table 1 — Cohort Characteristics"), unsafe_allow_html=True)

        cols_info = st.columns(4)
        cols_info[0].metric("Total N", f"{meta['n_total']:,}")
        if meta.get("nonnormal"):
            cols_info[1].metric("Non-normal vars", len(meta["nonnormal"]))
        cols_info[2].metric("Continuous", len(meta.get("continuous_vars", [])))
        cols_info[3].metric("Categorical", len(meta.get("categorical_vars", [])))

        if HAS_TABLEONE and "tableone_object" in meta:
            styled = t1_df.style.applymap(_highlight_pval, subset=["P-Value"] if "P-Value" in t1_df.columns else [])
            st.dataframe(styled, use_container_width=True, height=600)
        else:
            st.dataframe(t1_df, use_container_width=True, height=600)

        multi_export(t1_df.reset_index() if isinstance(t1_df.index, pd.MultiIndex) else t1_df,
                     "table1", key_sfx="t1")


def _render_hypothesis_testing(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available.")
        return

    source = st.selectbox("Data source", sources, key="ht_src")
    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    c1, c2 = st.columns(2)
    with c1:
        binary_opts = _binary_cols(df) + [
            c for c in df.columns if df[c].nunique(dropna=True) <= 10 and c != "research_id"
        ]
        binary_opts = list(dict.fromkeys(binary_opts))
        target = st.selectbox("Target (grouping) variable", binary_opts, key="ht_target")
    with c2:
        correction_label = st.radio(
            "Multiple comparison correction",
            list(_CORRECTION_METHODS.keys()),
            key="ht_corr",
            horizontal=True,
        )

    preset_features = [p for p in THYROID_PREDICTORS if p in df.columns and p != target]
    features = st.multiselect(
        "Features to test",
        [c for c in df.columns if c != target and c != "research_id"],
        default=preset_features,
        key="ht_feats",
    )

    if st.button("Run Hypothesis Tests", type="primary", key="ht_run"):
        if not features:
            st.warning("Select at least one feature.")
            return

        correction = _CORRECTION_METHODS[correction_label]
        with st.spinner("Running tests..."):
            results = analyzer.run_hypothesis_tests(df, target, features, correction=correction)

        if results.empty or "error" in results.columns:
            st.error(results.get("error", ["Unknown error"])[0] if "error" in results.columns else "No testable features.")
            return

        st.markdown(sl("Hypothesis Test Results"), unsafe_allow_html=True)
        n_sig = int(results["significant"].sum()) if "significant" in results.columns else 0
        ci1, ci2, ci3 = st.columns(3)
        ci1.metric("Tests run", len(results))
        ci2.metric("Significant (p < 0.05)", n_sig)
        ci3.metric("Correction", correction_label)

        display_cols = [c for c in [
            "variable", "test_used", "statistic", "p_value", "p_adjusted",
            "significant", "effect_size", "effect_label", "interpretation"
        ] if c in results.columns]
        styled = results[display_cols].style.applymap(
            _highlight_pval,
            subset=["p_value", "p_adjusted"] if "p_adjusted" in display_cols else ["p_value"],
        )
        st.dataframe(styled, use_container_width=True, height=500)

        multi_export(results, "hypothesis_tests", key_sfx="ht")


def _render_regression(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available.")
        return

    model_type = st.radio(
        "Model type",
        ["Logistic Regression", "Cox Proportional Hazards"],
        key="reg_type",
        horizontal=True,
    )

    source = st.selectbox("Data source", sources, key="reg_src")
    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    if model_type == "Logistic Regression":
        _render_logistic(df, analyzer)
    else:
        _render_cox(df, analyzer)


def _render_logistic(df: pd.DataFrame, analyzer: ThyroidStatisticalAnalyzer) -> None:
    if not HAS_STATSMODELS:
        st.error("statsmodels is required for logistic regression. Install with: `pip install statsmodels`")
        return

    binary = _binary_cols(df)
    outcome_default = next((o for o in THYROID_OUTCOMES if o in binary), binary[0] if binary else None)

    c1, c2 = st.columns(2)
    with c1:
        outcome = st.selectbox(
            "Outcome (binary)",
            binary,
            index=binary.index(outcome_default) if outcome_default in binary else 0,
            key="lr_outcome",
        )
    with c2:
        numeric = _numeric_cols(df)
        preset = [p for p in THYROID_PREDICTORS if p in numeric and p != outcome]
        predictors = st.multiselect("Predictors", numeric, default=preset[:8], key="lr_preds")

    confounders = st.multiselect(
        "Confounders (always adjusted for)",
        [c for c in numeric if c not in predictors and c != outcome],
        key="lr_conf",
    )

    if st.button("Fit Logistic Regression", type="primary", key="lr_run"):
        if not predictors:
            st.warning("Select at least one predictor.")
            return

        with st.spinner("Fitting logistic regression..."):
            result = analyzer.fit_logistic_regression(
                outcome=outcome,
                predictors=predictors,
                confounders=confounders,
                data=df,
            )

        if "error" in result:
            st.error(result["error"])
            return

        _display_logistic_results(result)


def _display_logistic_results(result: dict) -> None:
    st.markdown(sl("Logistic Regression Results"), unsafe_allow_html=True)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("N", f"{result['n_obs']:,}")
    m2.metric("Pseudo R²", f"{result['pseudo_r2']:.4f}")
    m3.metric("AUC", f"{result['auc']:.4f}" if result.get("auc") else "N/A")
    m4.metric("AIC", f"{result['aic']:.1f}")

    for w in result.get("warnings", []):
        st.warning(w, icon="⚠️")

    or_table = result["or_table"]
    st.subheader("Odds Ratios")
    display = or_table[or_table["predictor"] != "const"].copy()
    styled = display.style.applymap(_highlight_pval, subset=["p_value"])
    st.dataframe(styled, use_container_width=True)

    forest_df = display.rename(columns={
        "predictor": "label", "OR": "estimate",
        "CI_lower": "ci_lower", "CI_upper": "ci_upper",
    })
    if not forest_df.empty:
        fig = ThyroidStatisticalAnalyzer.create_forest_plot(
            forest_df, title="Odds Ratios (95% CI)", reference_value=1.0,
        )
        st.plotly_chart(fig, use_container_width=True)

    if not result.get("vif", pd.DataFrame()).empty:
        st.subheader("Variance Inflation Factors")
        st.dataframe(result["vif"], use_container_width=True)

    with st.expander("Interpretation notes"):
        for _, row in display.iterrows():
            if row.get("interpretation"):
                sig_mark = " *" if row["significant"] else ""
                st.markdown(f"- {row['interpretation']}{sig_mark}")

    multi_export(or_table, "logistic_regression", key_sfx="lr")


def _render_cox(df: pd.DataFrame, analyzer: ThyroidStatisticalAnalyzer) -> None:
    if not HAS_LIFELINES:
        st.error("lifelines is required for Cox PH models. Install with: `pip install lifelines`")
        return

    c1, c2 = st.columns(2)
    numeric = _numeric_cols(df)
    with c1:
        time_opts = [c for c in df.columns if "time" in c.lower() or "duration" in c.lower() or "days" in c.lower()]
        if not time_opts:
            time_opts = numeric
        default_time = THYROID_SURVIVAL["time_col"] if THYROID_SURVIVAL["time_col"] in time_opts else (time_opts[0] if time_opts else "")
        time_col = st.selectbox("Time variable", time_opts,
                                index=time_opts.index(default_time) if default_time in time_opts else 0,
                                key="cox_time")
    with c2:
        event_opts = [c for c in df.columns if "event" in c.lower() or "censor" in c.lower() or "status" in c.lower()]
        if not event_opts:
            event_opts = _binary_cols(df)
        default_event = THYROID_SURVIVAL["event_col"] if THYROID_SURVIVAL["event_col"] in event_opts else (event_opts[0] if event_opts else "")
        event_col = st.selectbox("Event variable", event_opts,
                                 index=event_opts.index(default_event) if default_event in event_opts else 0,
                                 key="cox_event")

    pred_options = [c for c in numeric if c != time_col and c != event_col]
    preset = [p for p in THYROID_PREDICTORS if p in pred_options]
    predictors = st.multiselect("Covariates", pred_options, default=preset[:8], key="cox_preds")

    if st.button("Fit Cox PH Model", type="primary", key="cox_run"):
        if not predictors:
            st.warning("Select at least one covariate.")
            return

        with st.spinner("Fitting Cox proportional hazards model..."):
            result = analyzer.fit_cox_ph(
                time_col=time_col,
                event_col=event_col,
                predictors=predictors,
                data=df,
            )

        if "error" in result:
            st.error(result["error"])
            return

        _display_cox_results(result)


def _display_cox_results(result: dict) -> None:
    st.markdown(sl("Cox Proportional Hazards Results"), unsafe_allow_html=True)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("N", f"{result['n_obs']:,}")
    m2.metric("Events", f"{result['n_events']:,}")
    m3.metric("Concordance", f"{result['concordance']:.4f}")
    m4.metric("AIC", f"{result['aic']:.1f}")

    for w in result.get("warnings", []):
        st.warning(w, icon="⚠️")

    hr_table = result["hr_table"]
    st.subheader("Hazard Ratios")
    styled = hr_table.style.applymap(_highlight_pval, subset=["p_value"])
    st.dataframe(styled, use_container_width=True)

    forest_df = hr_table.rename(columns={
        "covariate": "label", "HR": "estimate",
        "CI_lower": "ci_lower", "CI_upper": "ci_upper",
    })
    if not forest_df.empty:
        fig = ThyroidStatisticalAnalyzer.create_forest_plot(
            forest_df, title="Hazard Ratios (95% CI)", reference_value=1.0,
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Interpretation notes"):
        for _, row in hr_table.iterrows():
            if row.get("interpretation"):
                sig_mark = " *" if row["significant"] else ""
                st.markdown(f"- {row['interpretation']}{sig_mark}")

    multi_export(hr_table, "cox_ph_results", key_sfx="cox")


def _render_visualizations(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available.")
        return

    viz_type = st.radio(
        "Visualization",
        ["Correlation Heatmap", "Missing Data", "Distribution Comparison"],
        key="viz_type",
        horizontal=True,
    )

    source = st.selectbox("Data source", sources, key="viz_src")
    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    if viz_type == "Correlation Heatmap":
        _render_corr_heatmap(df, analyzer)
    elif viz_type == "Missing Data":
        _render_missing_viz(df, analyzer)
    else:
        _render_distributions(df)


def _render_corr_heatmap(df: pd.DataFrame, analyzer: ThyroidStatisticalAnalyzer) -> None:
    numeric = _numeric_cols(df)
    preset = [p for p in THYROID_PREDICTORS if p in numeric]
    selected = st.multiselect("Variables", numeric, default=preset[:10], key="corr_vars")

    method = st.radio("Method", ["spearman", "pearson", "kendall"], key="corr_method", horizontal=True)

    if st.button("Compute Correlation Matrix", type="primary", key="corr_run"):
        if len(selected) < 2:
            st.warning("Select at least 2 variables.")
            return

        with st.spinner("Computing correlations..."):
            corr, pval = analyzer.correlation_matrix_with_pvalues(df, selected, method=method)

        if corr.empty:
            st.info("Could not compute correlations.")
            return

        fig = analyzer.create_correlation_heatmap(corr, pval, title=f"{method.title()} Correlation Matrix")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Correlation values"):
            st.dataframe(corr, use_container_width=True)

        multi_export(corr.reset_index(), "correlation_matrix", key_sfx="corr")


def _render_missing_viz(df: pd.DataFrame, analyzer: ThyroidStatisticalAnalyzer) -> None:
    miss = analyzer.missing_data_summary(df)
    miss_nonzero = miss[miss["pct_missing"] > 0]

    st.metric("Columns with missing data", len(miss_nonzero))

    if miss_nonzero.empty:
        st.success("No missing data detected.")
        return

    if HAS_PLOTLY:
        import plotly.graph_objects as go
        top30 = miss_nonzero.head(30)
        fig = go.Figure(go.Bar(
            x=top30["pct_missing"],
            y=top30["column"],
            orientation="h",
            marker_color=[COLORS["rose"] if p > 50 else COLORS["amber"] if p > 20 else COLORS["teal"]
                          for p in top30["pct_missing"]],
        ))
        fig.update_layout(**PL, title="Missing Data (% per column)",
                          xaxis_title="% Missing", height=max(300, 25 * len(top30) + 80))
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(miss_nonzero, use_container_width=True, height=400)
    multi_export(miss, "missing_data", key_sfx="miss")


def _render_distributions(df: pd.DataFrame) -> None:
    if not HAS_PLOTLY:
        st.warning("plotly required for distribution plots.")
        return

    import plotly.graph_objects as go

    numeric = _numeric_cols(df)
    selected = st.selectbox("Variable", numeric, key="dist_var")
    groupby_opts = ["(None)"] + _binary_cols(df)
    groupby = st.selectbox("Group by", groupby_opts, key="dist_grp")

    if selected:
        fig = go.Figure()
        if groupby == "(None)":
            vals = df[selected].dropna()
            fig.add_trace(go.Histogram(x=vals, name=selected, marker_color=COLORS["teal"], opacity=0.8))
        else:
            for gval in sorted(df[groupby].dropna().unique(), key=str):
                vals = df.loc[df[groupby] == gval, selected].dropna()
                fig.add_trace(go.Histogram(x=vals, name=str(gval), opacity=0.7))
            fig.update_layout(barmode="overlay")

        fig.update_layout(**PL, title=f"Distribution: {selected}", xaxis_title=selected, yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)


def _render_diagnostics(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    st.markdown(sl("Library Status"), unsafe_allow_html=True)
    st.markdown(_lib_badges(), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(sl("Available Data Sources"), unsafe_allow_html=True)
    sources = _available_sources(con)
    if sources:
        source_info = []
        for s in sources:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {s}").fetchone()
                source_info.append({"View": s, "Rows": f"{n[0]:,}" if n else "?"})
            except Exception:
                source_info.append({"View": s, "Rows": "error"})
        st.dataframe(pd.DataFrame(source_info), use_container_width=True)
    else:
        st.info("No analytic views found. Run materialization scripts.")

    st.markdown("---")
    st.markdown(sl("Session Info"), unsafe_allow_html=True)
    import importlib
    versions = {}
    for pkg in ["pandas", "numpy", "scipy", "statsmodels", "lifelines", "tableone", "pingouin", "plotly"]:
        try:
            mod = importlib.import_module(pkg)
            versions[pkg] = getattr(mod, "__version__", "?")
        except ImportError:
            versions[pkg] = "not installed"
    st.json(versions)

    st.markdown("---")
    st.caption("Random seed: 42 | All p-values are two-sided unless noted")


# ── Main render function ─────────────────────────────────────────────────

def render_statistical_analysis(con) -> None:
    """Entry point for the Statistical Analysis & Modeling dashboard tab."""
    st.markdown(sl("Statistical Analysis & Modeling"), unsafe_allow_html=True)
    st.caption("Publication-ready inferential statistics, regression modeling, and diagnostics")

    analyzer = ThyroidStatisticalAnalyzer(con)

    tabs = st.tabs([
        "Table 1",
        "Hypothesis Testing",
        "Regression Modeling",
        "Visualizations",
        "Diagnostics & Export",
    ])

    with tabs[0]:
        _render_table_one(con, analyzer)
    with tabs[1]:
        _render_hypothesis_testing(con, analyzer)
    with tabs[2]:
        _render_regression(con, analyzer)
    with tabs[3]:
        _render_visualizations(con, analyzer)
    with tabs[4]:
        _render_diagnostics(con, analyzer)
