"""
Dashboard tab: Predictive Analytics & Nomograms.

Wires interactive controls around ``ThyroidPredictiveAnalyzer`` for:
  - Model Comparison Hub (PTCM vs Cox vs CIF vs RSF)
  - Competing Risks Analysis with stratified CIF curves
  - ML Nomograms with SHAP explainability
  - Personalized Cure Calculator (PTCM-powered)
  - One-Click Manuscript Export
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from app.helpers import (
    COLORS,
    PL,
    mc,
    multi_export,
    sl,
    sqdf,
    tbl_exists,
)

try:
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

ROOT = Path(__file__).resolve().parent.parent


def render_predictive_analytics(con: Any) -> None:
    """Top-level render function for the Predictive Analytics tab."""
    st.markdown(
        '<h2 style="margin-bottom:0">Predictive Analytics & Nomograms</h2>'
        '<p style="color:#8892a4;font-size:.82rem;margin-top:4px">'
        "Integrated workbench: PTCM cure prediction, competing risks, "
        "ML explainability, and model comparison</p>",
        unsafe_allow_html=True,
    )

    try:
        from utils.predictive_analytics import (
            ThyroidPredictiveAnalyzer,
            PREDICTIVE_PRESETS,
            CURE_CALCULATOR_FEATURES,
            CLINICAL_INTERPRETATIONS,
        )
    except ImportError as exc:
        st.error(f"Predictive analytics module not available: {exc}", icon="🚫")
        return

    try:
        analyzer = ThyroidPredictiveAnalyzer(con)
    except Exception as exc:
        st.error(f"Failed to initialize analyzer: {exc}", icon="🚫")
        return

    sub_tabs = st.tabs([
        "🏆 Model Comparison",
        "⚔️ Competing Risks",
        "🧠 ML Nomograms & SHAP",
        "🎯 Personalized Cure Calculator",
        "📄 Manuscript Export",
    ])

    with sub_tabs[0]:
        _render_model_comparison(con, analyzer)
    with sub_tabs[1]:
        _render_competing_risks(con, analyzer)
    with sub_tabs[2]:
        _render_nomograms(con, analyzer)
    with sub_tabs[3]:
        _render_cure_calculator(con, analyzer)
    with sub_tabs[4]:
        _render_manuscript_export(con, analyzer)


# ── Sub-tab 1: Model Comparison Hub ──────────────────────────────────────

def _render_model_comparison(con: Any, analyzer: "ThyroidPredictiveAnalyzer") -> None:
    st.markdown(sl("Model Comparison Hub"), unsafe_allow_html=True)
    st.caption(
        "Side-by-side comparison of KM, Cox PH, Weibull PTCM, and Random "
        "Survival Forest. Uses the same cohort and predictors for fair comparison."
    )

    if st.button("▶ Run Model Comparison", key="pa_run_comparison"):
        with st.spinner("Fitting models (this may take 30-60 seconds)..."):
            result = analyzer.compare_survival_models()

        if "error" in result:
            st.error(result["error"], icon="❌")
            return

        st.session_state["pa_comparison_result"] = result

    result = st.session_state.get("pa_comparison_result")
    if result is None:
        st.info("Click the button above to compare survival models.", icon="💡")
        _render_ptcm_kpi_cards(analyzer)
        return

    comp_df = result.get("comparison_table", pd.DataFrame())
    if not comp_df.empty:
        st.markdown(sl("Results"), unsafe_allow_html=True)
        st.dataframe(
            comp_df.style.format(precision=4, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )

        if result.get("comparison_plot") and HAS_PLOTLY:
            st.plotly_chart(result["comparison_plot"], use_container_width=True)

        multi_export(comp_df, "model_comparison", "pa_comp")

    if result.get("warnings"):
        with st.expander("⚠️ Warnings", expanded=False):
            for w in result["warnings"]:
                st.warning(w, icon="⚠️")


def _render_ptcm_kpi_cards(analyzer: "ThyroidPredictiveAnalyzer") -> None:
    """Show PTCM summary KPIs if available."""
    if not analyzer.ptcm_available:
        return
    s = analyzer._ptcm_summary
    if not s:
        return
    cols = st.columns(4)
    cards = [
        ("PTCM Cohort", f"{s.get('n_total', 0):,}"),
        ("Cure Fraction π̄", f"{s.get('overall_cure_fraction', 0):.1%}"),
        ("AIC", f"{s.get('aic', 0):.1f}"),
        ("10y Plateau", f"{s.get('plateau_10y_rate', 0):.1%}"),
    ]
    for col, (label, value) in zip(cols, cards):
        col.markdown(mc(label, value), unsafe_allow_html=True)


# ── Sub-tab 2: Competing Risks ──────────────────────────────────────────

def _render_competing_risks(con: Any, analyzer: "ThyroidPredictiveAnalyzer") -> None:
    st.markdown(sl("Competing Risks Analysis"), unsafe_allow_html=True)
    st.caption(
        "Aalen-Johansen cumulative incidence functions accounting for "
        "death as a competing risk for recurrence. Includes cause-specific "
        "Cox hazard ratios and stratified CIF curves."
    )

    from utils.predictive_analytics import PREDICTIVE_PRESETS

    c1, c2 = st.columns(2)
    with c1:
        preset_key = st.selectbox(
            "Analysis preset",
            list(PREDICTIVE_PRESETS.keys()),
            format_func=lambda k: PREDICTIVE_PRESETS[k]["label"],
            key="pa_cr_preset",
        )
    with c2:
        strata_col = st.selectbox(
            "Stratify by (optional)",
            [None, "overall_stage_ajcc8", "braf_positive", "recurrence_risk_band", "sex"],
            format_func=lambda x: "— No stratification —" if x is None else x,
            key="pa_cr_strata",
        )

    if st.button("▶ Fit Competing Risks Model", key="pa_run_cr"):
        preset = PREDICTIVE_PRESETS[preset_key]
        with st.spinner("Fitting Aalen-Johansen CIF..."):
            result = analyzer.fit_competing_risks(
                time_col=preset["time_col"],
                event_col=preset["event_col"],
                competing_event_col=preset.get("competing_event_col"),
                predictors=preset.get("predictors"),
                strata_col=strata_col,
            )
        st.session_state["pa_cr_result"] = result

    result = st.session_state.get("pa_cr_result")
    if result is None:
        st.info("Select a preset and click Run to fit competing risks.", icon="💡")
        return

    if "error" in result:
        st.error(result["error"], icon="❌")
        return

    # KPIs
    kpi_cols = st.columns(3)
    kpi_cols[0].markdown(mc("N (cohort)", f"{result['n_obs']:,}"), unsafe_allow_html=True)
    kpi_cols[1].markdown(mc("Primary Events", str(result["n_events"])), unsafe_allow_html=True)
    kpi_cols[2].markdown(mc("Competing Events", str(result["n_competing"])), unsafe_allow_html=True)

    # CIF plot
    if result.get("cif_plot") and HAS_PLOTLY:
        st.plotly_chart(result["cif_plot"], use_container_width=True)

    # Summary table
    summary = result.get("summary_table", pd.DataFrame())
    if not summary.empty:
        st.markdown(sl("Landmark CIF Estimates"), unsafe_allow_html=True)
        st.dataframe(summary, use_container_width=True, hide_index=True)

    # Cause-specific HRs
    cs_hrs = result.get("cause_specific_hrs", {})
    if cs_hrs:
        st.markdown(sl("Cause-Specific Hazard Ratios"), unsafe_allow_html=True)
        for event_type, hr_result in cs_hrs.items():
            with st.expander(f"{event_type.title()} event (C-index: {hr_result.get('concordance', 'N/A')})"):
                hr_table = hr_result.get("hr_table", pd.DataFrame())
                if not hr_table.empty:
                    st.dataframe(
                        hr_table.style.format(precision=4, na_rep="—"),
                        use_container_width=True,
                        hide_index=True,
                    )

    if result.get("warnings"):
        with st.expander("⚠️ Warnings"):
            for w in result["warnings"]:
                st.warning(w, icon="⚠️")


# ── Sub-tab 3: ML Nomograms & SHAP ──────────────────────────────────────

def _render_nomograms(con: Any, analyzer: "ThyroidPredictiveAnalyzer") -> None:
    st.markdown(sl("Explainable ML Nomograms"), unsafe_allow_html=True)
    st.caption(
        "Cross-validated XGBoost or Random Forest with SHAP feature "
        "importance, beeswarm plots, and calibration assessment."
    )

    from utils.predictive_analytics import PREDICTIVE_PRESETS

    c1, c2 = st.columns(2)
    with c1:
        model_type = st.selectbox(
            "ML Algorithm",
            ["xgboost", "random_forest"],
            format_func=lambda x: {"xgboost": "XGBoost (gradient boosted trees)", "random_forest": "Random Forest"}[x],
            key="pa_nom_model",
        )
    with c2:
        outcome = st.selectbox(
            "Outcome",
            ["event_occurred", "any_nsqip_complication"],
            format_func=lambda x: {"event_occurred": "Structural Recurrence", "any_nsqip_complication": "Any NSQIP Complication"}.get(x, x),
            key="pa_nom_outcome",
        )

    if st.button("▶ Train Nomogram", key="pa_run_nom"):
        preset = PREDICTIVE_PRESETS.get("recurrence", {})
        preds = preset.get("predictors", [])[:8]
        with st.spinner("Training model + computing SHAP values..."):
            result = analyzer.train_explainable_nomogram(
                outcome=outcome,
                predictors=preds,
                base_model=model_type,
            )
        if "error" not in result:
            st.session_state["pa_nom_result"] = result
            st.session_state["pa_nom_model_obj"] = result.get("model")
            st.session_state["pa_nom_features"] = result.get("feature_names", [])
        else:
            st.error(result["error"], icon="❌")
            return

    result = st.session_state.get("pa_nom_result")
    if result is None:
        st.info("Select a model and outcome, then click Train.", icon="💡")
        return

    # Performance KPIs
    kpi_cols = st.columns(4)
    kpi_cols[0].markdown(mc("AUC (CV)", f"{result.get('auc_cv', 'N/A')}"), unsafe_allow_html=True)
    kpi_cols[1].markdown(mc("Brier Score", f"{result.get('brier_cv', 'N/A')}"), unsafe_allow_html=True)
    kpi_cols[2].markdown(mc("N", f"{result.get('n_obs', 0):,}"), unsafe_allow_html=True)
    kpi_cols[3].markdown(mc("Event Rate", f"{result.get('event_rate', 0):.1%}"), unsafe_allow_html=True)

    # Feature importance
    fi = result.get("feature_importance", pd.DataFrame())
    if not fi.empty:
        st.markdown(sl("Feature Importance"), unsafe_allow_html=True)
        shap_fig = result.get("shap_summary_plot")
        if shap_fig and HAS_PLOTLY:
            st.plotly_chart(shap_fig, use_container_width=True)
        else:
            st.dataframe(fi, use_container_width=True, hide_index=True)

    # SHAP beeswarm
    bee_fig = result.get("shap_beeswarm_plot")
    if bee_fig and HAS_PLOTLY:
        with st.expander("SHAP Beeswarm Plot (feature interactions)"):
            st.plotly_chart(bee_fig, use_container_width=True)

    # Calibration
    cal_fig = result.get("calibration_plot")
    if cal_fig and HAS_PLOTLY:
        with st.expander("Calibration Plot"):
            st.plotly_chart(cal_fig, use_container_width=True)

    # Individual risk prediction
    model = st.session_state.get("pa_nom_model_obj")
    features = st.session_state.get("pa_nom_features", [])
    if model and features:
        st.markdown(sl("Individual Risk Prediction"), unsafe_allow_html=True)
        with st.expander("Enter patient features for risk estimate"):
            from utils.predictive_analytics import ThyroidPredictiveAnalyzer as TPA
            feat_vals = {}
            cols = st.columns(min(3, len(features)))
            for i, f in enumerate(features):
                with cols[i % len(cols)]:
                    feat_vals[f] = st.number_input(f, value=0.0, key=f"pa_feat_{f}")
            if st.button("Predict Risk", key="pa_predict_risk"):
                pred = TPA.predict_individual_risk(model, feat_vals, features)
                if "error" in pred:
                    st.error(pred["error"])
                else:
                    st.markdown(
                        mc("Predicted Risk", f"{pred['risk_pct']}%", pred["risk_class"]),
                        unsafe_allow_html=True,
                    )
                    if pred.get("feature_contributions"):
                        st.markdown(sl("SHAP Contributions"), unsafe_allow_html=True)
                        contrib_df = pd.DataFrame([
                            {"Feature": k, "SHAP Value": v}
                            for k, v in pred["feature_contributions"].items()
                        ]).sort_values("SHAP Value", key=abs, ascending=False)
                        st.dataframe(contrib_df, use_container_width=True, hide_index=True)

    if result.get("warnings"):
        with st.expander("⚠️ Warnings"):
            for w in result["warnings"]:
                st.warning(w, icon="⚠️")


# ── Sub-tab 4: Personalized Cure Calculator ──────────────────────────────

def _render_cure_calculator(con: Any, analyzer: "ThyroidPredictiveAnalyzer") -> None:
    st.markdown(sl("Personalized Cure Calculator"), unsafe_allow_html=True)
    st.caption(
        "PTCM-powered cure probability estimation. Enter patient characteristics "
        "to compute long-term cure probability with clinical interpretation."
    )

    if not analyzer.ptcm_available:
        st.warning(
            "PTCM model not fitted. Run `python scripts/39_promotion_time_cure_models.py --md` first.",
            icon="⚠️",
        )
        return

    from utils.predictive_analytics import CURE_CALCULATOR_FEATURES, CLINICAL_INTERPRETATIONS

    calc_spec = analyzer.create_interactive_cure_calculator()
    ref = calc_spec.get("reference_population", {})

    # Reference population context
    with st.expander("Reference Population", expanded=False):
        ref_cols = st.columns(3)
        ref_cols[0].metric("Training cohort", f"{ref.get('n_total', 0):,}")
        ref_cols[1].metric("Mean age", f"{ref.get('age_mean', 0):.1f} ± {ref.get('age_std', 0):.1f}")
        ref_cols[2].metric("Population cure rate", f"{ref.get('overall_cure', 0):.1%}")

    st.markdown("---")
    st.markdown("### Enter Patient Characteristics")

    # Input widgets
    patient = {}
    input_cols = st.columns(3)
    for i, (key, spec) in enumerate(CURE_CALCULATOR_FEATURES.items()):
        with input_cols[i % 3]:
            if spec["type"] == "slider":
                patient[key] = st.slider(
                    spec["label"],
                    min_value=spec["min"], max_value=spec["max"],
                    value=spec["default"],
                    key=f"pa_calc_{key}",
                )
            elif spec["type"] == "select":
                patient[key] = st.selectbox(
                    spec["label"],
                    spec["options"],
                    index=spec["options"].index(spec["default"]),
                    key=f"pa_calc_{key}",
                )
            elif spec["type"] == "toggle":
                patient[key] = st.toggle(
                    spec["label"],
                    value=spec["default"],
                    key=f"pa_calc_{key}",
                )

    st.markdown("---")

    if st.button("🎯 Calculate Cure Probability", key="pa_calc_cure", type="primary"):
        with st.spinner("Computing PTCM cure probability..."):
            result = analyzer.predict_individual_cure_probability(patient)

        if "error" in result:
            st.error(result["error"], icon="❌")
            return

        st.session_state["pa_cure_result"] = result

    result = st.session_state.get("pa_cure_result")
    if result is None:
        return

    # Display cure probability prominently
    cure_pct = result["cure_probability_pct"]
    tier = result["cure_tier"]

    tier_colors = {
        "very_high": COLORS["green"],
        "high": COLORS["teal"],
        "moderate": COLORS["amber"],
        "low": COLORS["rose"],
    }
    tier_color = tier_colors.get(tier, COLORS["teal"])

    st.markdown(
        f'<div style="text-align:center;padding:2rem;background:#0e1219;'
        f'border:2px solid {tier_color};border-radius:16px;margin:1rem 0">'
        f'<div style="font-family:var(--font-m,monospace);font-size:.7rem;'
        f'letter-spacing:.15em;text-transform:uppercase;color:{tier_color}">'
        f'PTCM Cure Probability</div>'
        f'<div style="font-family:var(--font-d,serif);font-size:4rem;'
        f'color:{tier_color};line-height:1;margin:0.5rem 0">{cure_pct}%</div>'
        f'<div style="color:#8892a4;font-size:.85rem">'
        f'Tier: {tier.replace("_", " ").title()} | θ = {result["theta"]:.3f}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Clinical interpretation
    interp = result.get("cure_interpretation", "")
    if interp:
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#0a1a20,#0e1219);'
            f'border:1px solid {tier_color}40;border-left:3px solid {tier_color};'
            f'border-radius:10px;padding:1.2rem 1.4rem;margin:0.8rem 0">'
            f'<div style="font-family:var(--font-m,monospace);font-size:.62rem;'
            f'letter-spacing:.15em;text-transform:uppercase;color:{tier_color};'
            f'margin-bottom:8px">Clinical Interpretation</div>'
            f'<div style="color:#d4dae8;font-size:.9rem">{interp}</div></div>',
            unsafe_allow_html=True,
        )

    # Conditional survival table
    cond_surv = result.get("conditional_survival", pd.DataFrame())
    if not cond_surv.empty:
        st.markdown(sl("Conditional Survival by Year"), unsafe_allow_html=True)
        st.dataframe(
            cond_surv.style.format({"survival_probability": "{:.3f}", "recurrence_risk_pct": "{:.1f}%"}),
            use_container_width=True,
            hide_index=True,
        )

    # Feature contributions
    contribs = result.get("feature_contributions", {})
    if contribs:
        st.markdown(sl("Feature Contributions to Promotion Intensity"), unsafe_allow_html=True)
        contrib_df = pd.DataFrame([
            {
                "Feature": k.replace("_", " ").title(),
                "Value": v["feature_value"],
                "Δθ": v["delta_theta"],
                "Direction": v["direction"].replace("_", " "),
                "β": v["beta"],
            }
            for k, v in contribs.items()
        ]).sort_values("Δθ", key=abs, ascending=False)
        st.dataframe(contrib_df, use_container_width=True, hide_index=True)

    # Survival trajectory plot
    traj_fig = analyzer.plot_individual_cure_trajectory(patient)
    if traj_fig and HAS_PLOTLY:
        st.markdown(sl("Personalized Survival Trajectory"), unsafe_allow_html=True)
        st.plotly_chart(traj_fig, use_container_width=True)


# ── Sub-tab 5: Manuscript Export ─────────────────────────────────────────

def _render_manuscript_export(con: Any, analyzer: "ThyroidPredictiveAnalyzer") -> None:
    st.markdown(sl("One-Click Manuscript Report"), unsafe_allow_html=True)
    st.caption(
        "Generate a formatted Word document (.docx) with selected "
        "analysis sections, tables, and clinical interpretations."
    )

    sections = st.multiselect(
        "Sections to include",
        ["PTCM", "CompetingRisks", "Nomogram", "Comparison"],
        default=["PTCM", "CompetingRisks", "Comparison"],
        key="pa_report_sections",
    )

    c1, c2 = st.columns(2)
    with c1:
        title = st.text_input(
            "Report title",
            value="THYROID_2026 — Predictive Analytics Report",
            key="pa_report_title",
        )
    with c2:
        author = st.text_input(
            "Author",
            value="Thyroid Research Team",
            key="pa_report_author",
        )

    if st.button("📄 Generate Manuscript Report", key="pa_gen_report", type="primary"):
        with st.spinner("Generating report (fitting models as needed)..."):
            result = analyzer.generate_manuscript_report(
                sections=sections, title=title, author=author,
            )

        if "error" in result:
            st.error(result["error"], icon="❌")
            return

        out_path = Path(result["path"])
        if out_path.exists():
            st.success(f"Report generated: {out_path.name}", icon="✅")
            with open(out_path, "rb") as f:
                st.download_button(
                    "⬇️ Download Report (.docx)",
                    f.read(),
                    file_name=out_path.name,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key="pa_download_report",
                )
            st.caption(
                f"Sections: {', '.join(result.get('sections_included', []))} | "
                f"Generated: {result.get('generated_at', '')}"
            )
