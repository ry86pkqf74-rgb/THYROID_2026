"""Cohort QC Dashboard tab — aggregate quality/eligibility overview."""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from app.helpers import sqdf, mc, sl, badge, PL, require_view, tbl_exists, qual


def render_cohort_qc(con) -> None:
    if not tbl_exists(con, "streamlit_cohort_qc_summary_v"):
        st.warning(
            "Cohort QC summary table is not yet available. "
            "Run `python scripts/03_research_views.py --md` to create it.",
            icon="⚠️",
        )
        return

    df = sqdf(con, f"SELECT * FROM {qual('streamlit_cohort_qc_summary_v')}")
    if df.empty:
        st.info("QC summary view returned no data.")
        return

    row = df.iloc[0]

    st.markdown(sl("Cohort Quality & Eligibility Overview"), unsafe_allow_html=True)

    # Top-level metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(mc("Total Patients", f"{int(row.get('total_patients', 0)):,}"),
                     unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Histology Eligible", f"{int(row.get('histology_analysis_eligible', 0)):,}",
                        f"{int(row.get('histology_review_needed', 0)):,} need review"),
                     unsafe_allow_html=True)
    with c3:
        st.markdown(mc("Molecular Eligible", f"{int(row.get('molecular_analysis_eligible', 0)):,}",
                        f"of {int(row.get('molecular_total_rows', 0)):,} total"),
                     unsafe_allow_html=True)
    with c4:
        st.markdown(mc("RAI Analyzable", f"{int(row.get('rai_analyzable', 0)):,}",
                        f"{int(row.get('rai_definite_likely', 0)):,} definite/likely"),
                     unsafe_allow_html=True)
    with c5:
        st.markdown(mc("Validation Errors", f"{int(row.get('validation_errors', 0)):,}",
                        f"{int(row.get('validation_patients_affected', 0)):,} patients"),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Review queue metrics
    st.markdown(sl("Manual Review Queue"), unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(mc("Patients in Queue", f"{int(row.get('review_queue_patients', 0)):,}"),
                     unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Critical Priority", f"{int(row.get('review_critical_patients', 0)):,}"),
                     unsafe_allow_html=True)
    with c3:
        st.markdown(mc("High Priority", f"{int(row.get('review_high_patients', 0)):,}"),
                     unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Domain breakdown bar chart
    st.markdown(sl("Issues by Domain"), unsafe_allow_html=True)
    domain_data = {
        "Domain": ["Histology", "Molecular", "RAI", "Timeline", "Validation"],
        "Unresolved": [
            int(row.get("histology_review_needed", 0)),
            int(row.get("molecular_unresolved", 0)),
            int(row.get("rai_unresolved", 0)),
            int(row.get("timeline_unresolved", 0)),
            int(row.get("validation_errors", 0)),
        ],
        "Eligible": [
            int(row.get("histology_analysis_eligible", 0)),
            int(row.get("molecular_analysis_eligible", 0)),
            int(row.get("rai_analyzable", 0)),
            0, 0,
        ],
    }

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=domain_data["Domain"], y=domain_data["Unresolved"],
        name="Needs Review", marker_color="#f43f5e",
    ))
    fig.add_trace(go.Bar(
        x=domain_data["Domain"], y=domain_data["Eligible"],
        name="Analysis Eligible", marker_color="#34d399",
    ))
    fig.update_layout(**PL, barmode="group", height=350,
                      title="Domain Status: Eligible vs Needs Review")
    st.plotly_chart(fig, use_container_width=True)

    # Timeline date quality
    st.markdown(sl("Timeline Date Quality"), unsafe_allow_html=True)
    date_data = {
        "Status": ["Exact Source", "Inferred Day-Level", "Coarse Anchor", "Unresolved"],
        "Count": [
            int(row.get("timeline_exact_date", 0)),
            int(row.get("timeline_inferred_day", 0)),
            int(row.get("timeline_coarse_anchor", 0)),
            int(row.get("timeline_unresolved", 0)),
        ],
    }
    fig2 = go.Figure(go.Bar(
        x=date_data["Status"], y=date_data["Count"],
        marker_color=["#34d399", "#f59e0b", "#38bdf8", "#f43f5e"],
    ))
    fig2.update_layout(**PL, height=300,
                       title="Date Resolution Quality")
    st.plotly_chart(fig2, use_container_width=True)

    # Adjudication progress (if reviewer tables exist)
    try:
        prog = sqdf(con, "SELECT * FROM adjudication_progress_summary_v")
        if not prog.empty:
            st.markdown(sl("Adjudication Progress"), unsafe_allow_html=True)
            for _, p in prog.iterrows():
                domain = p["domain"]
                pct = float(p.get("pct_complete", 0))
                resolved = int(p.get("resolved", 0))
                total = int(p.get("total_queue", 0))
                st.progress(pct / 100.0,
                            text=f"{domain.title()}: {resolved}/{total} resolved ({pct}%)")
    except Exception:
        pass
