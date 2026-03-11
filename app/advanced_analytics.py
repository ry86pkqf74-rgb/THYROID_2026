"""
Advanced Analytics & AI — Dashboard Tab

Competing-risks survival (CIF), stratified longitudinal trajectories,
ML nomograms with SHAP explainability, interactive risk calculator,
and one-click manuscript report export.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.helpers import PL, COLORS, sl, badge, multi_export, tbl_exists
from utils.advanced_analytics import (
    ThyroidAdvancedAnalyzer,
    LONGITUDINAL_STRATIFIERS,
    HAS_LIFELINES,
    HAS_STATSMODELS,
    HAS_XGB,
    HAS_SKLEARN,
    HAS_SHAP,
    HAS_DOCX,
    HAS_PLOTLY,
)
from utils.statistical_analysis import LONGITUDINAL_MARKERS, THYROID_PREDICTORS

_DATA_SOURCES = [
    "risk_enriched_mv",
    "advanced_features_v3",
    "survival_cohort_enriched",
    "ptc_cohort",
    "recurrence_risk_cohort",
]

_GROUPBY_OPTIONS = [
    "(None)", "sex", "braf_positive", "overall_stage_ajcc8",
    "histology_1_type", "recurrence_risk_band",
]


def _lib_badges() -> str:
    libs = [
        ("lifelines", HAS_LIFELINES),
        ("statsmodels", HAS_STATSMODELS),
        ("xgboost", HAS_XGB),
        ("scikit-learn", HAS_SKLEARN),
        ("SHAP", HAS_SHAP),
        ("python-docx", HAS_DOCX),
    ]
    return " ".join(badge(n, "green" if ok else "rose") for n, ok in libs)


def _available_sources(con) -> list[str]:
    return [s for s in _DATA_SOURCES if tbl_exists(con, s)]


@st.cache_data(ttl=300, show_spinner=False)
def _load_source(_con, view: str) -> pd.DataFrame:
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
    try:
        v = float(val)
        if v < 0.05:
            return f"color: {COLORS['teal']}; font-weight: 700"
    except (ValueError, TypeError):
        pass
    return ""


# ── Sub-tab: Competing Risks ─────────────────────────────────────────────

def _render_competing_risks(con, analyzer: ThyroidAdvancedAnalyzer) -> None:
    st.markdown(sl("Competing Risks Analysis (Aalen-Johansen CIF)"), unsafe_allow_html=True)
    st.caption(
        "Cumulative incidence functions accounting for competing events. "
        "Standard KM overestimates event probability when death competes "
        "with recurrence — CIF provides unbiased estimates."
    )

    if not HAS_LIFELINES:
        st.error("lifelines is required for competing-risks models.")
        return

    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available. Run materialization scripts first.")
        return

    source = st.selectbox("Data source", sources, key="cr_src")
    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        time_opts = [c for c in df.columns
                     if any(k in c.lower() for k in ("time", "days", "duration"))]
        if not time_opts:
            time_opts = _numeric_cols(df)
        time_col = st.selectbox("Time variable", time_opts,
                                index=time_opts.index("time_to_event_days")
                                if "time_to_event_days" in time_opts else 0,
                                key="cr_time")
    with c2:
        event_opts = [c for c in df.columns
                      if any(k in c.lower() for k in ("event", "status", "recur"))]
        if not event_opts:
            event_opts = _binary_cols(df)
        event_col = st.selectbox("Primary event", event_opts,
                                 index=event_opts.index("event_occurred")
                                 if "event_occurred" in event_opts else 0,
                                 key="cr_event")
    with c3:
        compete_opts = ["(None)"] + [c for c in df.columns
                                      if any(k in c.lower()
                                             for k in ("death", "mortality", "deceased"))]
        if len(compete_opts) == 1:
            compete_opts.extend(_binary_cols(df)[:5])
        competing_col = st.selectbox("Competing event", compete_opts, key="cr_compete")

    if st.button("Fit Competing Risks Model", type="primary", key="cr_run"):
        comp = None if competing_col == "(None)" else competing_col

        with st.spinner("Fitting Aalen-Johansen CIF..."):
            result = analyzer.fit_competing_risks(
                time_col=time_col,
                event_col=event_col,
                competing_event_col=comp,
                data=df,
            )

        if "error" in result:
            st.error(result["error"])
            return

        for w in result.get("warnings", []):
            st.warning(w, icon="⚠️")

        k1, k2, k3 = st.columns(3)
        k1.metric("N", f"{result['n_obs']:,}")
        k2.metric("Primary Events", f"{result['n_events']:,}")
        k3.metric("Competing Events", f"{result['n_competing']:,}")

        st.info(result.get("clinical_note", ""))

        if result.get("cif_plot"):
            st.plotly_chart(result["cif_plot"], use_container_width=True)

        st.markdown(sl("Landmark CIF Summary"), unsafe_allow_html=True)
        st.dataframe(result["summary_table"], use_container_width=True, hide_index=True)

        multi_export(result["summary_table"], "competing_risks_summary", key_sfx="cr")


# ── Sub-tab: Longitudinal Trajectories ───────────────────────────────────

def _render_longitudinal_trajectories(con, analyzer: ThyroidAdvancedAnalyzer) -> None:
    st.markdown(sl("Stratified Longitudinal Biomarker Trajectories"), unsafe_allow_html=True)
    st.caption(
        "Subgroup-stratified mixed-effects models for repeated Tg/TSH measurements. "
        "Compare slopes across clinical strata (BRAF, stage, risk band)."
    )

    if not HAS_STATSMODELS:
        st.error("statsmodels is required for mixed-effects models.")
        return

    c1, c2 = st.columns(2)
    with c1:
        marker_opts = list(LONGITUDINAL_MARKERS.keys())
        marker_labels = {k: v["label"] for k, v in LONGITUDINAL_MARKERS.items()}
        marker = st.selectbox(
            "Biomarker", marker_opts,
            format_func=lambda k: marker_labels[k],
            key="lt_marker",
        )
    with c2:
        strat_opts = ["(None)"] + LONGITUDINAL_STRATIFIERS
        stratify = st.selectbox("Stratify by", strat_opts, key="lt_strat")

    if st.button("Run Stratified Analysis", type="primary", key="lt_run"):
        strat_col = None if stratify == "(None)" else stratify

        with st.spinner(f"Fitting mixed-effects models for {marker_labels[marker]}..."):
            result = analyzer.fit_stratified_longitudinal(
                marker=marker, stratify_by=strat_col,
            )

        if "error" in result:
            st.error(result["error"])
            return

        for w in result.get("warnings", []):
            st.warning(w, icon="⚠️")

        overall = result.get("overall_result", {})
        if overall:
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Patients (≥2 obs)", f"{overall.get('n_patients', 0):,}")
            k2.metric("Total Observations", f"{overall.get('n_obs', 0):,}")
            k3.metric("Overall Slope (β/yr)", f"{overall.get('slope', 0):+.4f}")
            p = overall.get("p_value")
            k4.metric("p-value", f"{p:.4f}" if p else "N/A")

            if overall.get("clinical_note"):
                st.info(overall["clinical_note"])

        if result.get("trajectory_plot"):
            st.plotly_chart(result["trajectory_plot"], use_container_width=True)

        comp_table = result.get("comparison_table", pd.DataFrame())
        if not comp_table.empty:
            st.markdown(sl("Stratum Comparison"), unsafe_allow_html=True)
            styled = comp_table.style.applymap(
                _highlight_pval,
                subset=["p_value"] if "p_value" in comp_table.columns else [],
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

            multi_export(comp_table, f"longitudinal_{marker}_stratified", key_sfx="lt")


# ── Sub-tab: ML Nomograms & SHAP ────────────────────────────────────────

def _render_ml_nomogram(con, analyzer: ThyroidAdvancedAnalyzer) -> None:
    st.markdown(sl("ML Nomograms & SHAP Explainability"), unsafe_allow_html=True)
    st.caption(
        "Train XGBoost or Random Forest for binary outcome prediction with "
        "stratified cross-validation, SHAP-based feature importance, and "
        "calibration assessment."
    )

    if not (HAS_XGB or HAS_SKLEARN):
        st.error("xgboost or scikit-learn required.")
        return

    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available.")
        return

    c1, c2 = st.columns(2)
    with c1:
        source = st.selectbox("Data source", sources, key="ml_src")
    with c2:
        model_type = st.radio(
            "Model type",
            ["xgboost", "randomforest"],
            format_func=lambda x: "XGBoost" if x == "xgboost" else "Random Forest",
            key="ml_model",
            horizontal=True,
        )

    df = _load_source(con, source)
    if df.empty:
        st.info("Selected source returned no data.")
        return

    binary = _binary_cols(df)
    if not binary:
        st.warning("No binary outcome columns found in this source.")
        return

    preset_outcomes = [o for o in ["event_occurred", "recurrence_flag", "braf_positive"] if o in binary]
    default_outcome = preset_outcomes[0] if preset_outcomes else binary[0]

    outcome = st.selectbox(
        "Outcome (binary)", binary,
        index=binary.index(default_outcome) if default_outcome in binary else 0,
        key="ml_outcome",
    )

    numeric = _numeric_cols(df)
    preset_preds = [p for p in THYROID_PREDICTORS if p in numeric and p != outcome]
    predictors = st.multiselect(
        "Predictors", numeric, default=preset_preds[:10], key="ml_preds",
    )

    n_folds = st.slider("CV folds", 2, 10, 5, key="ml_folds")

    if st.button("Train Model", type="primary", key="ml_run"):
        if len(predictors) < 2:
            st.warning("Select at least 2 predictors.")
            return

        with st.spinner(f"Training {model_type} with {n_folds}-fold CV..."):
            result = analyzer.train_ml_nomogram(
                outcome=outcome, predictors=predictors,
                model_type=model_type, data=df, n_folds=n_folds,
            )

        if "error" in result:
            st.error(result["error"])
            return

        for w in result.get("warnings", []):
            st.warning(w, icon="⚠️")

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("N", f"{result['n_obs']:,}")
        k2.metric("Events", f"{result['n_events']:,}")
        k3.metric("AUC (CV)", f"{result['auc_cv']:.4f}" if result.get("auc_cv") else "N/A")
        k4.metric("Brier Score", f"{result['brier_cv']:.4f}" if result.get("brier_cv") else "N/A")

        # Feature importance
        st.markdown(sl("Feature Importance"), unsafe_allow_html=True)
        imp = result.get("feature_importance", pd.DataFrame())
        if not imp.empty:
            st.dataframe(imp, use_container_width=True, hide_index=True)

        # SHAP summary
        if result.get("shap_summary_plot"):
            st.markdown(sl("SHAP Feature Importance"), unsafe_allow_html=True)
            st.plotly_chart(result["shap_summary_plot"], use_container_width=True)

        # SHAP beeswarm
        if result.get("shap_beeswarm_plot"):
            st.markdown(sl("SHAP Beeswarm — Feature Impact"), unsafe_allow_html=True)
            st.plotly_chart(result["shap_beeswarm_plot"], use_container_width=True)
            st.caption(
                "Each dot = one patient. Color encodes feature value (red=high, blue=low). "
                "X-axis = SHAP contribution to prediction."
            )

        # Calibration
        if result.get("calibration_plot"):
            st.markdown(sl("Calibration"), unsafe_allow_html=True)
            st.plotly_chart(result["calibration_plot"], use_container_width=True)

        # Store model in session state for risk calculator
        st.session_state["_adv_ml_model"] = result.get("model")
        st.session_state["_adv_ml_features"] = result.get("feature_names", [])
        st.session_state["_adv_ml_X"] = result.get("X")
        st.session_state["_adv_ml_outcome"] = outcome

        st.success("Model stored for Interactive Risk Calculator use.")

        multi_export(imp, f"ml_{model_type}_importance", key_sfx="ml_imp")


# ── Sub-tab: Interactive Risk Calculator ─────────────────────────────────

def _render_risk_calculator(con, analyzer: ThyroidAdvancedAnalyzer) -> None:
    st.markdown(sl("Interactive Risk Calculator"), unsafe_allow_html=True)
    st.caption(
        "Compute individualized risk predictions using the trained ML model. "
        "Adjust patient features via sliders to see real-time risk updates "
        "with SHAP-based feature contributions."
    )

    model = st.session_state.get("_adv_ml_model")
    feature_names = st.session_state.get("_adv_ml_features", [])
    X = st.session_state.get("_adv_ml_X")
    outcome = st.session_state.get("_adv_ml_outcome", "event")

    if model is None or not feature_names:
        st.info(
            "No trained model in session. Go to the **ML Nomograms & SHAP** tab "
            "and train a model first — it will be automatically available here."
        )
        return

    st.markdown(f"**Outcome:** `{outcome}` | **Features:** {len(feature_names)}")

    # Compute feature ranges
    ranges = ThyroidAdvancedAnalyzer.compute_feature_ranges(X, feature_names)

    st.markdown("---")
    st.markdown("**Adjust patient features:**")

    feature_values: dict[str, float] = {}
    cols = st.columns(min(3, len(feature_names)))
    for i, feat in enumerate(feature_names):
        r = ranges.get(feat, {"min": 0.0, "max": 1.0, "median": 0.5})
        col = cols[i % len(cols)]
        with col:
            is_binary = r["min"] == 0 and r["max"] == 1 and r["median"] in (0, 1)
            if is_binary:
                val = st.selectbox(
                    feat.replace("_", " ").title(),
                    [0, 1],
                    index=int(r["median"]),
                    key=f"rc_{feat}",
                )
                feature_values[feat] = float(val)
            else:
                val = st.slider(
                    feat.replace("_", " ").title(),
                    float(r["min"]),
                    float(r["max"]),
                    float(r["median"]),
                    key=f"rc_{feat}",
                )
                feature_values[feat] = val

    if st.button("Calculate Risk", type="primary", key="rc_run"):
        with st.spinner("Computing risk..."):
            result = ThyroidAdvancedAnalyzer.predict_individual_risk(
                model, feature_values, feature_names,
            )

        if "error" in result:
            st.error(result["error"])
            return

        risk_pct = result["risk_pct"]
        risk_class = result["risk_class"]

        color = COLORS["green"] if risk_class == "Low" else COLORS["amber"] if risk_class == "Intermediate" else COLORS["rose"]

        st.markdown(
            f'<div style="text-align:center;padding:1.5rem;background:{color}15;'
            f'border:1px solid {color};border-radius:12px;margin:1rem 0">'
            f'<div style="font-size:2.5rem;font-weight:700;color:{color}">'
            f'{risk_pct:.1f}%</div>'
            f'<div style="font-size:1rem;color:{COLORS["text_mid"]}">'
            f'Predicted {outcome.replace("_", " ")} probability — '
            f'<b>{risk_class} Risk</b></div></div>',
            unsafe_allow_html=True,
        )

        contributions = result.get("feature_contributions", {})
        if contributions and HAS_PLOTLY:
            import plotly.graph_objects as go

            sorted_contribs = dict(sorted(contributions.items(), key=lambda x: abs(x[1])))
            feats = list(sorted_contribs.keys())
            vals = list(sorted_contribs.values())
            colors_bar = [COLORS["rose"] if v > 0 else COLORS["teal"] for v in vals]

            fig = go.Figure(go.Bar(
                x=vals, y=feats, orientation="h",
                marker_color=colors_bar,
            ))
            fig.update_layout(
                **PL, title="Feature Contributions (SHAP)",
                xaxis_title="SHAP Value",
                height=max(300, 28 * len(feats) + 80),
                margin=dict(l=150, r=16, t=50, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                "Red bars increase risk; teal bars decrease risk. "
                "SHAP values show each feature's marginal contribution."
            )


# ── Sub-tab: Manuscript Report ───────────────────────────────────────────

def _render_manuscript_report(con, analyzer: ThyroidAdvancedAnalyzer) -> None:
    st.markdown(sl("One-Click Manuscript Report"), unsafe_allow_html=True)
    st.caption(
        "Generate a Word document containing publication-ready tables, "
        "model results, and clinical interpretation text."
    )

    if not HAS_DOCX:
        st.error("python-docx required — `pip install python-docx`")
        return

    sources = _available_sources(con)
    if not sources:
        st.warning("No analytic views available.")
        return

    source = st.selectbox("Data source", sources, key="rpt_src")

    all_sections = ["Table1", "Cox", "Longitudinal", "CompetingRisks", "ML_Nomogram"]
    selected_sections = st.multiselect(
        "Sections to include", all_sections,
        default=["Table1", "Cox", "Longitudinal"],
        key="rpt_sections",
    )

    c1, c2 = st.columns(2)
    with c1:
        title = st.text_input("Report title", "Thyroid Cancer Analytics Report", key="rpt_title")
    with c2:
        author = st.text_input("Author", "THYROID_2026 Research Team", key="rpt_author")

    if st.button("Generate Report", type="primary", key="rpt_run"):
        if not selected_sections:
            st.warning("Select at least one section.")
            return

        df = _load_source(con, source)
        if df.empty:
            st.error("Selected source returned no data.")
            return

        with st.spinner("Generating manuscript report..."):
            result = analyzer.generate_manuscript_report(
                sections=selected_sections,
                data=df,
                title=title,
                author=author,
            )

        if "error" in result:
            st.error(result["error"])
            return

        for w in result.get("warnings", []):
            st.warning(w, icon="⚠️")

        sections_ok = result.get("sections_included", [])
        st.success(f"Report generated with {len(sections_ok)} sections: {', '.join(sections_ok)}")

        st.download_button(
            "Download Word Report (.docx)",
            result["docx_bytes"],
            result["filename"],
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            key="rpt_download",
        )

    st.markdown("---")
    st.markdown(sl("LaTeX Export"), unsafe_allow_html=True)
    st.caption("Paste model result tables into LaTeX manuscripts.")

    if st.button("Generate LaTeX for stored model", key="rpt_latex"):
        model_imp = st.session_state.get("_adv_ml_model")
        if model_imp is None:
            st.info("No trained ML model in session. Train one in the ML Nomograms tab first.")
        else:
            feat_names = st.session_state.get("_adv_ml_features", [])
            try:
                imp = model_imp.feature_importances_
                imp_df = pd.DataFrame({
                    "Feature": feat_names,
                    "Importance": np.round(imp, 4),
                }).sort_values("Importance", ascending=False)
                latex = ThyroidAdvancedAnalyzer.generate_latex_table(
                    imp_df, caption="ML Feature Importance", label="tab:ml_importance",
                )
                st.code(latex, language="latex")
            except Exception as exc:
                st.error(f"LaTeX generation failed: {exc}")


# ── Sub-tab: Diagnostics ─────────────────────────────────────────────────

def _render_adv_diagnostics(con) -> None:
    st.markdown(sl("Advanced Analytics — Library Status"), unsafe_allow_html=True)
    st.markdown(_lib_badges(), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(sl("Available Data Sources"), unsafe_allow_html=True)
    sources = _available_sources(con)
    if sources:
        rows = []
        for s in sources:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {s}").fetchone()
                rows.append({"View": s, "Rows": f"{n[0]:,}" if n else "?"})
            except Exception:
                rows.append({"View": s, "Rows": "error"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No analytic views found. Run materialization scripts.")

    st.markdown("---")
    st.markdown(sl("Package Versions"), unsafe_allow_html=True)
    import importlib
    versions = {}
    for pkg in ["xgboost", "shap", "docx", "jinja2", "lifelines",
                "sklearn", "statsmodels", "plotly", "pandas", "numpy"]:
        try:
            mod = importlib.import_module(pkg)
            versions[pkg] = getattr(mod, "__version__", "?")
        except ImportError:
            versions[pkg] = "not installed"
    st.json(versions)


# ── Main render function ─────────────────────────────────────────────────

def render_advanced_analytics(con) -> None:
    """Entry point for the Advanced Analytics & AI dashboard tab."""
    st.markdown(sl("Advanced Analytics & AI"), unsafe_allow_html=True)
    st.caption(
        "Competing-risks survival, stratified longitudinal models, "
        "explainable ML nomograms with SHAP, interactive risk calculator, "
        "and one-click manuscript report generation"
    )

    analyzer = ThyroidAdvancedAnalyzer(con)

    tabs = st.tabs([
        "Competing Risks",
        "Longitudinal Trajectories",
        "ML Nomograms & SHAP",
        "Interactive Risk Calculator",
        "Manuscript Report",
        "Diagnostics",
    ])

    with tabs[0]:
        _render_competing_risks(con, analyzer)
    with tabs[1]:
        _render_longitudinal_trajectories(con, analyzer)
    with tabs[2]:
        _render_ml_nomogram(con, analyzer)
    with tabs[3]:
        _render_risk_calculator(con, analyzer)
    with tabs[4]:
        _render_manuscript_report(con, analyzer)
    with tabs[5]:
        _render_adv_diagnostics(con)
