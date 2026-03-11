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
    THYROID_NSQIP_OUTCOMES,
    THYROID_NSQIP_PREDICTORS,
    NSQIP_COMPLICATION_COLUMNS,
    LONGITUDINAL_MARKERS,
    ETE_SUBTYPES,
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
    "extracted_clinical_events_v4",
    "longitudinal_lab_view",
]

_LONGITUDINAL_SOURCES = [
    "extracted_clinical_events_v4",
    "longitudinal_lab_view",
]

_COMP_NEG_VALUES = "('','no','none','n/a','na','nan','0','false','neg','negative','absent','normal')"

_NSQIP_COHORT_SQL = f"""
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.has_tumor_pathology,
    tp.histology_1_type,
    tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
    tp.braf_mutation_mentioned,
    ps.tumor_1_extrathyroidal_extension AS tumor_1_extrathyroidal_ext,
    ps.thyroid_procedure AS malignant_surgery_type,
    cp.rln_injury,
    cp.hypocalcemia,
    cp.hypoparathyroidism,
    cp.seroma,
    cp.hematoma
FROM master_cohort mc
INNER JOIN (
    SELECT
        CAST(research_id AS INT) AS research_id,
        MAX(CASE
            WHEN LOWER(CAST(rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR))
                 NOT IN {_COMP_NEG_VALUES}
                 AND rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy IS NOT NULL
            THEN 1 ELSE 0
        END) AS rln_injury,
        MAX(CASE
            WHEN LOWER(CAST(hypocalcemia AS VARCHAR)) NOT IN {_COMP_NEG_VALUES}
                 AND hypocalcemia IS NOT NULL
            THEN 1 ELSE 0
        END) AS hypocalcemia,
        MAX(CASE
            WHEN LOWER(CAST(hypoparathyroidism AS VARCHAR)) NOT IN {_COMP_NEG_VALUES}
                 AND hypoparathyroidism IS NOT NULL
            THEN 1 ELSE 0
        END) AS hypoparathyroidism,
        MAX(CASE
            WHEN LOWER(CAST(seroma AS VARCHAR)) NOT IN {_COMP_NEG_VALUES}
                 AND seroma IS NOT NULL
            THEN 1 ELSE 0
        END) AS seroma,
        MAX(CASE
            WHEN LOWER(CAST(hematoma AS VARCHAR)) NOT IN {_COMP_NEG_VALUES}
                 AND hematoma IS NOT NULL
            THEN 1 ELSE 0
        END) AS hematoma
    FROM complications
    GROUP BY CAST(research_id AS INT)
) cp ON mc.research_id = cp.research_id
LEFT JOIN tumor_pathology tp ON mc.research_id = tp.research_id
LEFT JOIN path_synoptics ps ON mc.research_id = ps.research_id
"""

_NSQIP_COHORT_WRAPPER = f"""
WITH _base AS ({_NSQIP_COHORT_SQL})
SELECT *,
    GREATEST(rln_injury, hypocalcemia, hypoparathyroidism, seroma, hematoma)
    AS any_nsqip_complication
FROM _base
"""

_GROUPBY_OPTIONS = [
    "(None)",
    "sex",
    "braf_positive",
    "tumor_1_extrathyroidal_ext",
    "overall_stage_ajcc8",
    "histology_1_type",
    "recurrence_risk_band",
    "event_occurred",
    "any_nsqip_complication",
    "rln_injury",
    "hypocalcemia",
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


def _available_sources(con) -> list[str]:
    standard = [s for s in _DATA_SOURCES if tbl_exists(con, s)]
    # Add NSQIP cohort if complications table is available
    if tbl_exists(con, "complications") and "nsqip_complication_cohort" not in standard:
        standard.append("nsqip_complication_cohort")
    return standard


@st.cache_data(ttl=300, show_spinner=False)
def _load_nsqip_cohort(_con) -> pd.DataFrame:
    """Build and return the NSQIP complication cohort inline."""
    try:
        return _con.execute(_NSQIP_COHORT_WRAPPER).fetchdf()
    except Exception:
        return pd.DataFrame()


def _load_source(con, view: str) -> pd.DataFrame:
    """Load a data source — handles the virtual nsqip_complication_cohort."""
    if view == "nsqip_complication_cohort":
        return _load_nsqip_cohort(con)
    try:
        return _load_source_cached(con, view)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_source_cached(_con, view: str) -> pd.DataFrame:
    try:
        return _con.execute(f"SELECT * FROM {view}").fetchdf()
    except Exception:
        return pd.DataFrame()


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

        result["outcome_label"] = outcome.replace("_", " ")
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

    snippet = ThyroidStatisticalAnalyzer.format_clinical_snippet(
        result, model_type="OR", outcome_label=result.get("outcome_label", "the outcome")
    )
    if snippet and "No significant" not in snippet:
        st.info(snippet)

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

        result["outcome_label"] = event_col.replace("_", " ")
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

    snippet = ThyroidStatisticalAnalyzer.format_clinical_snippet(
        result, model_type="HR", outcome_label=result.get("outcome_label", "the outcome")
    )
    if snippet and "No significant" not in snippet:
        st.info(snippet)

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


def _render_longitudinal(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    """Sub-tab: Longitudinal Tg/TSH mixed-effects analysis."""
    st.markdown(sl("Longitudinal Biomarker Trajectory Analysis"), unsafe_allow_html=True)
    st.caption(
        "Linear mixed-effects model (random intercept by patient) for repeated "
        "Tg/TSH measurements. Requires `extracted_clinical_events_v4` or "
        "`longitudinal_lab_view`."
    )

    if not HAS_STATSMODELS:
        st.error("statsmodels is required for mixed-effects models. Install with: `pip install statsmodels`")
        return

    c1, c2 = st.columns(2)
    with c1:
        marker_opts = list(LONGITUDINAL_MARKERS.keys())
        marker_labels = {k: v["label"] for k, v in LONGITUDINAL_MARKERS.items()}
        marker = st.selectbox(
            "Biomarker",
            marker_opts,
            format_func=lambda k: marker_labels[k],
            key="long_marker",
        )
    with c2:
        avail_sources = [s for s in _LONGITUDINAL_SOURCES if tbl_exists(con, s)]
        if not avail_sources:
            st.warning(
                "No longitudinal data source available. "
                "Ensure `extracted_clinical_events_v4` or `longitudinal_lab_view` is materialized."
            )
            return
        src = st.selectbox("Data source", avail_sources, key="long_src")

    if st.button("Run Longitudinal Analysis", type="primary", key="long_run"):
        with st.spinner(f"Fitting mixed-effects model for {marker_labels[marker]}…"):
            res = analyzer.longitudinal_summary(marker=marker, view=src)

        if "error" in res:
            st.error(res["error"])
            return

        for w in res.get("warnings", []):
            st.warning(w, icon="⚠️")

        # KPI row
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Patients (≥2 obs)", f"{res['n_patients']:,}")
        k2.metric("Total observations", f"{res['n_obs']:,}")
        slope_disp = f"{res['slope']:+.4f}/yr"
        k3.metric(
            f"{'log' if res['log_transform'] else ''} slope (β/yr)",
            slope_disp,
        )
        p_disp = f"{res['p_value']:.3f}" if res.get("p_value") else "N/A"
        k4.metric("p-value (slope)", p_disp)

        # Clinical note
        st.info(res["clinical_note"])

        st.markdown(sl("Model Summary"), unsafe_allow_html=True)
        st.code(res["model_summary"], language="text")

        # Per-patient slope distribution
        pp = res.get("per_patient_summary", pd.DataFrame())
        if not pp.empty and HAS_PLOTLY:
            import plotly.graph_objects as go
            from app.helpers import PL as _PL  # local import to avoid circular

            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=pp["slope_per_day"].dropna() * 365.25,
                nbinsx=40,
                marker_color="#2dd4bf",
                opacity=0.8,
                name="Annual slope per patient",
            ))
            fig_hist.update_layout(
                **_PL,
                title=f"Per-Patient {marker_labels[marker]} Slope Distribution (per year)",
                xaxis_title="Slope (per year, log scale if applicable)",
                yaxis_title="Patients",
                height=340,
            )
            st.plotly_chart(fig_hist, use_container_width=True)

            pct_rising = float(pp["rising"].mean() * 100)
            c_rising, c_falling = st.columns(2)
            c_rising.metric("Rising trajectory", f"{pct_rising:.1f}%")
            c_falling.metric("Stable/falling trajectory", f"{100 - pct_rising:.1f}%")

        st.markdown(sl("Per-Patient Summary"), unsafe_allow_html=True)
        if not pp.empty:
            pp_display = pp.copy()
            pp_display["slope_annual"] = (pp_display["slope_per_day"] * 365.25).round(4)
            st.dataframe(
                pp_display[["research_id", "n_obs", "slope_annual",
                             "first_value", "last_value", "rising"]],
                use_container_width=True,
                height=400,
            )
            multi_export(pp_display, f"longitudinal_{marker}", key_sfx=f"long_{marker}")


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


def _render_publication_export(con, analyzer: ThyroidStatisticalAnalyzer) -> None:
    """Sub-tab: Publication-ready export bundle with LaTeX snippets."""
    st.markdown(sl("Publication Export"), unsafe_allow_html=True)
    st.caption(
        "Export Table 1, model results, and LaTeX-ready snippets for manuscript submission."
    )

    sources = _available_sources(con)
    if not sources:
        st.info("No analytic views available. Run materialization scripts first.")
        return

    c1, c2 = st.columns(2)
    with c1:
        source = st.selectbox("Primary data source", sources, key="pub_src")
    with c2:
        groupby_opts = ["(None)"] + [g for g in _GROUPBY_OPTIONS[1:]]
        groupby = st.selectbox("Table 1 group by", groupby_opts, key="pub_grp")

    df = _load_source(con, source)

    col_meta = []
    try:
        n_row = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
        col_meta.append(f"Rows: {n_row[0]:,}")
    except Exception:
        pass

    if col_meta:
        st.caption(" | ".join(col_meta))

    run_col1, run_col2 = st.columns(2)
    run_t1 = run_col1.button("Generate Table 1 for export", key="pub_t1_run")
    run_snippet = run_col2.button("Generate clinical snippet text", key="pub_snip_run")

    if run_t1 and not df.empty:
        groupby_col = None if groupby == "(None)" else groupby
        if groupby_col and groupby_col not in df.columns:
            groupby_col = None

        with st.spinner("Building Table 1…"):
            t1_df, meta = analyzer.generate_table_one(
                data=df, groupby_col=groupby_col
            )

        if "error" in meta:
            st.error(meta["error"])
        else:
            st.markdown(sl("Table 1 — Cohort Characteristics"), unsafe_allow_html=True)

            smd_note = " SMD included." if meta.get("smd_computed") else ""
            st.caption(
                f"N={meta['n_total']:,} | {len(meta.get('continuous_vars',[]))} continuous, "
                f"{len(meta.get('categorical_vars',[]))} categorical.{smd_note}"
            )

            st.dataframe(t1_df, use_container_width=True, height=500)

            export_df = t1_df.reset_index() if hasattr(t1_df.index, "names") and len(t1_df.index.names) > 1 else t1_df
            multi_export(export_df, "pub_table1", key_sfx="pub_t1")

            st.markdown("**LaTeX-ready column notes:**")
            st.code(
                "% Values are median [IQR] for non-normal continuous variables and n (%) for categorical.\n"
                "% p-values from Mann-Whitney U / chi-squared / Fisher exact as appropriate.\n"
                f"% N={meta['n_total']:,} patients; missing data excluded per variable.\n"
                + (f"% Standardized mean differences (SMD) included for balance assessment.\n"
                   if meta.get("smd_computed") else ""),
                language="latex",
            )

    if run_snippet and not df.empty:
        st.markdown(sl("Clinical Snippet — Significant Associations"), unsafe_allow_html=True)
        st.caption(
            "Run regression models first, then use this panel to format their "
            "results for copy-paste into manuscript methods/results sections."
        )

        snippet_type = st.radio(
            "Model type", ["Cox (HR)", "Logistic (OR)"],
            key="pub_snip_type", horizontal=True
        )

        st.markdown("**Draft snippet (paste into manuscript):**")

        if snippet_type == "Cox (HR)":
            template = (
                "In multivariable Cox proportional hazards analysis, "
                "[PREDICTOR] was independently associated with "
                "[OUTCOME] (HR=[X.XX], 95% CI [X.XX–X.XX], p=[X.XXX]). "
                "The model demonstrated good discrimination (concordance=[X.XXX]). "
                "Results were robust across sensitivity analyses."
            )
        else:
            template = (
                "In multivariable logistic regression, [PREDICTOR] was "
                "independently associated with [OUTCOME] "
                "(OR=[X.XX], 95% CI [X.XX–X.XX], p=[X.XXX]). "
                "The model fit was adequate (pseudo-R²=[X.XXX], AUC=[X.XXX])."
            )

        st.text_area(
            "Manuscript snippet template (edit values from your model):",
            value=template,
            height=160,
            key="pub_snip_text",
        )

        st.markdown(sl("Model Interpretation Guide"), unsafe_allow_html=True)
        interp_rows = []
        for var, ctx in {
            "BRAF V600E": "associated with RAI refractoriness and aggressive PTC",
            "TERT promoter mutation": "marks dedifferentiation; highest risk when co-occurring with BRAF",
            "Gross ETE": "upstages disease (AJCC T3b/T4); consider RAI and EBRT",
            "LN ratio >0.3": "predicts structural recurrence; intensify surveillance",
            "Age ≥55": "AJCC 8th Ed staging threshold; carries distinct prognosis",
            "AJCC Stage III/IV": "guides RAI dosing and systemic therapy decisions",
        }.items():
            interp_rows.append({"Predictor": var, "Clinical context": ctx})
        st.dataframe(
            pd.DataFrame(interp_rows),
            use_container_width=True,
            hide_index=True,
            height=260,
        )

    st.markdown("---")
    st.markdown(sl("Export Checklist"), unsafe_allow_html=True)
    checks = [
        ("Table 1 (demographics + SMD)", "Generate above and download CSV"),
        ("Hypothesis test battery (FDR-corrected)", "Run from Hypothesis Testing tab"),
        ("Logistic regression OR table", "Run from Regression Modeling tab"),
        ("Cox PH HR table + forest plot", "Run from Regression Modeling tab"),
        ("Longitudinal Tg trajectory", "Run from Longitudinal Analysis tab"),
        ("Missing data summary", "Run from Visualizations tab → Missing Data"),
    ]
    for item, how in checks:
        st.markdown(f"- **{item}** — *{how}*")


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
        "Longitudinal Analysis",
        "Visualizations",
        "Diagnostics",
        "Publication Export",
    ])

    with tabs[0]:
        _render_table_one(con, analyzer)
    with tabs[1]:
        _render_hypothesis_testing(con, analyzer)
    with tabs[2]:
        _render_regression(con, analyzer)
    with tabs[3]:
        _render_longitudinal(con, analyzer)
    with tabs[4]:
        _render_visualizations(con, analyzer)
    with tabs[5]:
        _render_diagnostics(con, analyzer)
    with tabs[6]:
        _render_publication_export(con, analyzer)
